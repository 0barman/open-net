//! One redirect policy and response-header lifecycle for every HTTP client.
use super::*;
use crate::api::http::HttpRedirectError;
#[cfg(feature = "http-client-http2")]
use crate::api::http::HttpTransportSecurity;
use http::{header, HeaderMap, StatusCode};

pub(super) struct ResponseHops<'a> {
    pub(super) request_headers: &'a HeaderMap,
    pub(super) body: &'a bytes::Bytes,
    pub(super) body_present: bool,
    pub(super) replayable_body: bool,
    pub(super) method: &'a Method,
    pub(super) request_id: HttpRequestId,
    pub(super) attempt: u32,
    pub(super) initial_generation: u64,
    pub(super) control: &'a CancellationSignal,
    pub(super) budget: StreamBudget,
    #[cfg(feature = "http-client-cookies")]
    pub(super) cookie_context: &'a CookieRequestContext,
}

pub(super) async fn follow_response_hops(
    response: Response,
    hops: ResponseHops<'_>,
    config: &HttpClientConfig,
    client: &Client,
) -> Result<(Response, u64, bool), NetError> {
    run_budget(
        hops.budget,
        hops.control,
        ErrorStage::Response,
        follow(response, &hops, config, client),
    )
    .await
    .map_err(|error| {
        error
            .with_http_request_id(hops.request_id)
            .with_http_attempt(hops.attempt)
    })
}

async fn follow(
    mut response: Response,
    hops: &ResponseHops<'_>,
    config: &HttpClientConfig,
    client: &Client,
) -> Result<(Response, u64, bool), NetError> {
    #[cfg(feature = "http-client-auth")]
    let auth = config.auth();
    #[cfg(feature = "http-client-auth")]
    let mut auth_allowed = auth.is_some();
    #[cfg(not(feature = "http-client-auth"))]
    let auth_allowed = false;
    let generation = hops.initial_generation;
    #[cfg(feature = "http-client-auth")]
    let mut generation = generation;
    #[cfg(not(feature = "http-client-auth"))]
    let _ = hops.attempt;
    let mut headers = config.merge_headers(hops.request_headers)?;
    let mut method = hops.method.clone();
    let mut body_present = hops.body_present;
    let original_url = response.url().clone();
    let policy = config.redirect_policy();
    let security = config.transport_security();
    #[cfg(feature = "http-client-http2")]
    let security = if config.http_version_policy() == HttpVersionPolicy::Http2Only {
        HttpTransportSecurity::HttpsOnly
    } else {
        security
    };
    // Each response, including an unfollowable redirect or the final hop,
    // updates the store exactly once within the original headers budget.
    for followed in 0..=policy.max_redirects() {
        check_stream_start(hops.budget, hops.control)?;
        #[cfg(feature = "http-client-cookies")]
        update_response_cookies(&mut response, config, hops.request_id, hops.control)?;
        check_stream_start(hops.budget, hops.control)?;
        let status = response.status();
        if !policy.is_enabled()
            || !matches!(
                status,
                StatusCode::MOVED_PERMANENTLY
                    | StatusCode::FOUND
                    | StatusCode::SEE_OTHER
                    | StatusCode::TEMPORARY_REDIRECT
                    | StatusCode::PERMANENT_REDIRECT
            )
        {
            return Ok((response, generation, auth_allowed));
        }
        let Some(location) = response.headers().get(header::LOCATION) else {
            return Ok((response, generation, auth_allowed));
        };
        let location = location
            .to_str()
            .map_err(|_| redirect_error(HttpRedirectError::InvalidLocation))?;
        let mut next_url = response
            .url()
            .join(location)
            .map_err(|_| redirect_error(HttpRedirectError::InvalidLocation))?;
        next_url.set_fragment(None);
        // Authorize the complete target before invoking providers, constructing
        // another request, or opening a connection. Replayability alone does not
        // authorize sending the body to a different origin.
        policy
            .check_redirect(&original_url, response.url(), &next_url, followed, security)
            .map_err(redirect_error)?;
        let (next_method, preserve_body) = redirect_method(status, &method);
        if preserve_body && body_present && !hops.replayable_body {
            return Ok((response, generation, auth_allowed));
        }
        method = next_method;
        if !preserve_body {
            body_present = false;
            headers.remove(header::CONTENT_LENGTH);
            headers.remove(header::CONTENT_TYPE);
            headers.remove(header::CONTENT_ENCODING);
            headers.remove(header::TRANSFER_ENCODING);
        }
        let same_origin = response.url().origin() == next_url.origin();
        if !same_origin {
            strip_sensitive_headers(&mut headers);
            #[cfg(feature = "http-client-auth")]
            {
                auth_allowed = false;
            }
        }
        // Keep only caller headers between hops. Providers are re-evaluated
        // into a temporary map for every URL; generated Cookie headers never
        // become explicit headers on later hops.
        let next_headers = headers.clone();
        #[cfg(any(feature = "http-client-auth", feature = "http-client-cookies"))]
        let mut next_headers = next_headers;
        #[cfg(feature = "http-client-auth")]
        if auth_allowed {
            if let Some(auth) = &auth {
                let mut path = next_url.path().to_owned();
                if let Some(query) = next_url.query() {
                    path.push('?');
                    path.push_str(query);
                }
                let origin = next_url.origin().ascii_serialization();
                let mut context = AuthRequestContext::new(
                    &method,
                    &origin,
                    &path,
                    hops.request_id,
                    hops.attempt,
                    &mut next_headers,
                );
                let applied = AssertUnwindSafe(auth.apply(&mut context))
                    .catch_unwind()
                    .await;
                check_stream_start(hops.budget, hops.control)?;
                match applied {
                    Ok(Ok(())) => generation = context.credential_generation(),
                    Ok(Err(_)) | Err(_) => {
                        return Err(NetError::from(ErrorKind::ProviderFailed)
                            .with_stage(ErrorStage::Provider))
                    }
                }
            }
        }
        #[cfg(feature = "http-client-cookies")]
        {
            cookie_hooks::check_cancelled(hops.control, hops.request_id)?;
            let applied = apply_cookie_header(
                &mut next_headers,
                &next_url,
                &method,
                hops.cookie_context,
                config,
                hops.request_id,
            );
            cookie_hooks::check_cancelled(hops.control, hops.request_id)?;
            applied?;
        }
        check_stream_start(hops.budget, hops.control)?;
        let mut builder = client
            .request(method.clone(), next_url)
            .headers(next_headers);
        if body_present {
            builder = builder.body(hops.body.clone());
            if hops.body.is_empty() {
                builder = builder.header(header::CONTENT_LENGTH, "0");
            }
        }
        let request = builder
            .build()
            .map_err(|error| map_reqwest_error(error, ErrorStage::RequestBuild))?;
        drop(response);
        hops.control.record_transport_send()?;
        #[cfg(feature = "quality-doctor")]
        if let Some(probe) = hops.control.quality.get() {
            probe.hop((followed as u64).saturating_add(1));
        }
        response = execute_request(
            client,
            request,
            hops.control,
            (followed as u64).saturating_add(1),
        )
        .await?;
    }
    Err(redirect_error(HttpRedirectError::TooManyRedirects))
}

fn redirect_error(reason: HttpRedirectError) -> NetError {
    NetError::protocol(reason).with_stage(ErrorStage::Receive)
}

/// Match the transport's established method semantics independently of optional
/// providers. In particular, 301/302 preserve PUT/PATCH bodies, while 303 drops
/// every body and retains HEAD as HEAD.
fn redirect_method(status: StatusCode, method: &Method) -> (Method, bool) {
    match status {
        StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND if *method == Method::POST => {
            (Method::GET, false)
        }
        StatusCode::SEE_OTHER => (
            if *method == Method::HEAD {
                Method::HEAD
            } else {
                Method::GET
            },
            false,
        ),
        _ => (method.clone(), true),
    }
}

fn strip_sensitive_headers(headers: &mut HeaderMap) {
    let names: Vec<_> = headers
        .iter()
        .filter(|(_, value)| value.is_sensitive())
        .map(|(name, _)| name.clone())
        .collect();
    for name in names {
        headers.remove(name);
    }
    for name in [
        header::AUTHORIZATION,
        header::COOKIE,
        header::PROXY_AUTHORIZATION,
        header::WWW_AUTHENTICATE,
        header::HOST,
        header::HeaderName::from_static("cookie2"),
    ] {
        headers.remove(name);
    }
}

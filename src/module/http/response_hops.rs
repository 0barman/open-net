//! One redirect and response-header lifecycle for auth and Cookie providers.
use super::*;
use http::{header, HeaderMap, StatusCode};

pub(super) fn uses_worker_redirects(config: &HttpClientConfig) -> bool {
    let enabled = false;
    #[cfg(feature = "http-client-auth")]
    let enabled = enabled || config.auth().is_some();
    #[cfg(feature = "http-client-cookies")]
    let enabled = enabled || config.cookie_store().is_some();
    enabled
}

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
    if !uses_worker_redirects(config) {
        return Ok((response, hops.initial_generation, false));
    }
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
    // Ten followed redirects allow eleven observed responses. Each response,
    // including an unfollowable redirect or the last hop, updates the store once.
    for followed in 0..=10 {
        check_stream_start(hops.budget, hops.control)?;
        #[cfg(feature = "http-client-cookies")]
        update_response_cookies(&mut response, config, hops.request_id, hops.control)?;
        check_stream_start(hops.budget, hops.control)?;
        let status = response.status();
        if !matches!(
            status,
            StatusCode::MOVED_PERMANENTLY
                | StatusCode::FOUND
                | StatusCode::SEE_OTHER
                | StatusCode::TEMPORARY_REDIRECT
                | StatusCode::PERMANENT_REDIRECT
        ) {
            return Ok((response, generation, auth_allowed));
        }
        let Some(location) = response.headers().get(header::LOCATION) else {
            return Ok((response, generation, auth_allowed));
        };
        let location = match location.to_str() {
            Ok(location) => location,
            Err(_) => return Ok((response, generation, auth_allowed)),
        };
        let mut next_url = response
            .url()
            .join(location)
            .map_err(|_| redirect_error())?;
        if !matches!(next_url.scheme(), "http" | "https")
            || next_url.host_str().is_none()
            || !next_url.username().is_empty()
            || next_url.password().is_some()
        {
            return Err(redirect_error());
        }
        next_url.set_fragment(None);
        #[cfg(feature = "http-client-http2")]
        if config.http_version_policy() == HttpVersionPolicy::Http2Only
            && next_url.scheme() != "https"
        {
            return Err(redirect_error());
        }
        let preserve_body = matches!(
            status,
            StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
        );
        if preserve_body && body_present && !hops.replayable_body {
            return Ok((response, generation, auth_allowed));
        }
        if followed == 10 {
            return Err(redirect_error());
        }
        if !preserve_body {
            if !matches!(method, Method::GET | Method::HEAD) {
                method = Method::GET;
            }
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
        let mut next_headers = headers.clone();
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
        response = client
            .execute(request)
            .await
            .map_err(|error| map_reqwest_error(error, ErrorStage::Receive))?;
    }
    Err(redirect_error())
}

fn redirect_error() -> NetError {
    NetError::from(ErrorKind::Protocol).with_stage(ErrorStage::Receive)
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

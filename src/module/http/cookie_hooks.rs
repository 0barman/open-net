//! Bounded, redacted Cookie provider calls at the transport boundary.
use super::*;
use http::{header, HeaderMap};

pub(super) fn check_cancelled(
    control: &CancellationSignal,
    request_id: HttpRequestId,
) -> Result<(), NetError> {
    if control.is_cancelled() {
        Err(NetError::from(ErrorKind::Cancelled)
            .with_stage(ErrorStage::Dispatch)
            .with_http_request_id(request_id))
    } else {
        Ok(())
    }
}

fn provider_failure(request_id: HttpRequestId) -> NetError {
    NetError::from(ErrorKind::ProviderFailed)
        .with_stage(ErrorStage::Provider)
        .with_http_request_id(request_id)
}

fn header_limit(request_id: HttpRequestId) -> NetError {
    NetError::from(ErrorKind::ItemTooLarge)
        .with_stage(ErrorStage::RequestBuild)
        .with_http_request_id(request_id)
}

pub(super) fn apply_cookie_header(
    headers: &mut HeaderMap,
    url: &reqwest::Url,
    method: &Method,
    context: &CookieRequestContext,
    config: &HttpClientConfig,
    request_id: HttpRequestId,
) -> Result<(), NetError> {
    let Some(store) = config.cookie_store() else {
        return Ok(());
    };
    let limits = config.cookie_limits();
    if headers.contains_key(header::COOKIE) {
        let mut bytes = 0_usize;
        for (index, value) in headers.get_all(header::COOKIE).iter().enumerate() {
            bytes = bytes.saturating_add(value.as_bytes().len());
            if index != 0 {
                bytes = bytes.saturating_add(2);
            }
            if bytes > limits.max_cookie_header_bytes {
                return Err(header_limit(request_id));
            }
        }
        for (name, value) in headers.iter_mut() {
            if name == header::COOKIE {
                value.set_sensitive(true);
            }
        }
        return Ok(());
    }
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        store.cookies_for_request(url, method, context)
    }));
    let value = match result {
        Ok(Ok(value)) => value,
        // Keep the documented jar header-limit classification, but never a
        // provider's arbitrary context or source (which may contain secrets).
        Ok(Err(error))
            if error.kind() == ErrorKind::ItemTooLarge
                && error.context().stage == Some(ErrorStage::RequestBuild) =>
        {
            return Err(header_limit(request_id));
        }
        Ok(Err(_)) | Err(_) => return Err(provider_failure(request_id)),
    };
    if let Some(mut value) = value {
        if value.as_bytes().len() > limits.max_cookie_header_bytes {
            return Err(header_limit(request_id));
        }
        value.set_sensitive(true);
        headers.insert(header::COOKIE, value);
    }
    Ok(())
}

pub(super) fn update_response_cookies(
    response: &mut Response,
    config: &HttpClientConfig,
    request_id: HttpRequestId,
    control: &CancellationSignal,
) -> Result<(), NetError> {
    let Some(store) = config.cookie_store() else {
        return Ok(());
    };
    check_cancelled(control, request_id)?;
    let limits = config.cookie_limits();
    for (name, value) in response.headers_mut().iter_mut() {
        if name == header::SET_COOKIE {
            value.set_sensitive(true);
        }
    }
    let mut headers = response.headers().clone();
    headers.remove(header::SET_COOKIE);
    let mut total = 0_usize;
    for value in response.headers().get_all(header::SET_COOKIE) {
        total = total.saturating_add(value.as_bytes().len());
        if total > limits.max_set_cookie_bytes {
            break;
        }
        if !value.is_empty() && value.as_bytes().len() <= limits.max_cookie_bytes {
            headers.append(header::SET_COOKIE, value.clone());
        }
    }
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        store.set_response_cookies(response.url(), &headers)
    }));
    // A store is synchronous and cannot be preempted. Check the lifecycle
    // immediately after it returns, including its failure path.
    check_cancelled(control, request_id)?;
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) | Err(_) => Err(provider_failure(request_id)),
    }
}

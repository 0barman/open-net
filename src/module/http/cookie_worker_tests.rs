#![cfg(feature = "http-client-cookies")]

use super::*;
use crate::api::http::{CookieLimits, CookieStore, CookieStoreResult};
use std::error::Error;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

struct FailingStore;

impl CookieStore for FailingStore {
    fn cookies_for_request(
        &self,
        _url: &reqwest::Url,
        _method: &Method,
        _context: &CookieRequestContext,
    ) -> CookieStoreResult<Option<http::HeaderValue>> {
        Err(NetError::with_source(
            ErrorKind::Io,
            std::io::Error::other("secret-session"),
        ))
    }

    fn set_response_cookies(
        &self,
        _url: &reqwest::Url,
        _headers: &http::HeaderMap,
    ) -> CookieStoreResult<()> {
        Ok(())
    }
}

#[test]
fn cookie_provider_failures_are_classified_without_retaining_sources() -> TestResult {
    let config = HttpClientConfig::new("https://example.com")?.with_cookie_store(FailingStore);
    let result = apply_cookie_header(
        &mut http::HeaderMap::new(),
        &config.base_url,
        &Method::GET,
        &CookieRequestContext::default(),
        &config,
        HttpRequestId(41),
    );
    let error = match result {
        Err(error) => error,
        Ok(()) => return Err(std::io::Error::other("store error was lost").into()),
    };
    check(
        error.kind() == ErrorKind::ProviderFailed,
        "store error was treated as transport failure",
    )?;
    check(
        error.context().stage == Some(ErrorStage::Provider),
        "wrong provider stage",
    )?;
    check(
        error.context().http_request_id == Some(HttpRequestId(41)),
        "request identity was lost",
    )?;
    check(
        error.source().is_none(),
        "provider source retained a cookie secret",
    )
}

#[test]
fn every_explicit_cookie_value_counts_towards_the_header_budget() -> TestResult {
    let limits = CookieLimits {
        max_cookie_header_bytes: 5,
        ..CookieLimits::default()
    };
    let config = HttpClientConfig::new("https://example.com")?
        .with_cookie_store(FailingStore)
        .with_cookie_limits(limits)?;
    let mut headers = http::HeaderMap::new();
    headers.append(http::header::COOKIE, http::HeaderValue::from_static("a=1"));
    headers.append(http::header::COOKIE, http::HeaderValue::from_static("b=2"));
    let result = apply_cookie_header(
        &mut headers,
        &config.base_url,
        &Method::GET,
        &CookieRequestContext::default(),
        &config,
        HttpRequestId(42),
    );
    check(
        matches!(result, Err(error) if error.kind() == ErrorKind::ItemTooLarge),
        "duplicate Cookie fields bypassed header limit",
    )
}

#[test]
fn buffered_result_bridge_retains_the_request_cookie_context() -> TestResult {
    let context =
        CookieRequestContext::initiated_by(reqwest::Url::parse("https://partner.test")?, true);
    let request = HttpRequest::builder()
        .path("/")
        .cookie_context(context.clone())
        .build()?;
    let (sender, _receiver) = tokio::sync::oneshot::channel();
    let bridge = crate::module::http::result_request::ResultRequest::new(request, sender);
    check(
        bridge.cookie_context() == context,
        "buffered bridge replaced caller context with same-site default",
    )
}

#[test]
fn request_debug_never_formats_explicit_cookie_or_body_secrets() -> TestResult {
    let mut request = HttpRequest::builder()
        .path("/private?REQUEST_SECRET")
        .build()?;
    request.headers.insert(
        http::header::COOKIE,
        http::HeaderValue::from_static("session=REQUEST_SECRET"),
    );
    request.body = bytes::Bytes::from_static(b"REQUEST_SECRET");
    check(
        !format!("{request:?}").contains("REQUEST_SECRET"),
        "request Debug leaked explicit Cookie, path, or body",
    )
}

#![cfg(feature = "http-client-middleware")]

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use open_net::api::http::{
    HttpCall, HttpClient, HttpClientConfig, HttpMiddleware, HttpMiddlewareResponse, HttpRequest,
    HttpResponse, HttpTimeouts, Next, TimeoutPhase,
};
use open_net::error::{ErrorKind, ErrorStage};
use open_net::NetError;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message).into())
    }
}

struct SyntheticResponse {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpMiddleware for SyntheticResponse {
    async fn call(&self, call: HttpCall, _next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(HttpMiddlewareResponse::Buffered(HttpResponse::synthetic(
            call.request_id(),
            StatusCode::OK,
            HeaderMap::new(),
            Bytes::new(),
        )))
    }
}

struct DelayedMiddleware {
    before: bool,
}

#[async_trait]
impl HttpMiddleware for DelayedMiddleware {
    async fn call(&self, call: HttpCall, next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        if self.before {
            tokio::time::sleep(Duration::from_secs(20)).await;
        }
        let response = next.run(call).await?;
        if !self.before {
            tokio::time::sleep(Duration::from_secs(20)).await;
        }
        Ok(response)
    }
}

async fn middleware_total_case(before: bool, streaming: bool) -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let config = HttpClientConfig::new("http://127.0.0.1:1")?
        .with_timeouts(HttpTimeouts::new().with_total_timeout(Duration::from_secs(5)))?
        .with_middleware(DelayedMiddleware { before })
        .with_middleware(SyntheticResponse {
            calls: Arc::clone(&calls),
        });
    let client = HttpClient::new(config, "timeout-middleware")?;
    let request = HttpRequest::builder().path("/").build()?;
    let result = if streaming {
        client.stream(request).await.map(|_| ())
    } else {
        client.request(request).await.map(|_| ())
    };
    client.shutdown().await?;
    let error = result
        .err()
        .ok_or_else(|| io::Error::other("late middleware success accepted"))?;
    check(
        error.kind() == ErrorKind::DeadlineExceeded,
        "wrong total timeout kind",
    )?;
    check(
        error.timeout_phase() == Some(TimeoutPhase::Total),
        "middleware lost total phase",
    )?;
    check(
        error.context().stage == Some(ErrorStage::Dispatch),
        "middleware timeout must be Dispatch",
    )?;
    check(
        error.context().http_request_id.is_some(),
        "middleware timeout lost request ID",
    )?;
    check(
        error.context().http_attempt == Some(0),
        "short circuit must not invent an attempt",
    )?;
    check(
        calls.load(Ordering::Acquire) == usize::from(!before),
        "wrong middleware execution boundary",
    )
}

#[tokio::test(start_paused = true)]
async fn total_covers_buffered_before_middleware() -> TestResult {
    middleware_total_case(true, false).await
}

#[tokio::test(start_paused = true)]
async fn total_covers_buffered_after_middleware() -> TestResult {
    middleware_total_case(false, false).await
}

#[tokio::test(start_paused = true)]
async fn total_covers_stream_before_middleware() -> TestResult {
    middleware_total_case(true, true).await
}

#[tokio::test(start_paused = true)]
async fn total_covers_stream_after_middleware() -> TestResult {
    middleware_total_case(false, true).await
}

#[cfg(feature = "http-client-auth")]
struct PendingAuth;

#[cfg(feature = "http-client-auth")]
#[async_trait]
impl open_net::api::http::HttpAuth for PendingAuth {
    async fn apply(
        &self,
        _context: &mut open_net::api::http::AuthRequestContext<'_>,
    ) -> Result<(), NetError> {
        std::future::pending().await
    }
    async fn on_unauthorized(
        &self,
        _: &open_net::api::http::AuthResponseContext<'_>,
    ) -> Result<open_net::api::http::AuthAction, NetError> {
        Ok(open_net::api::http::AuthAction::DoNotRetry)
    }
}

#[cfg(feature = "http-client-auth")]
struct ReplaceTimeouts;

#[cfg(feature = "http-client-auth")]
#[async_trait]
impl HttpMiddleware for ReplaceTimeouts {
    async fn call(
        &self,
        mut call: HttpCall,
        next: Next,
    ) -> Result<HttpMiddlewareResponse, NetError> {
        *call.request_mut() = HttpRequest::builder()
            .path("/")
            .timeouts(
                HttpTimeouts::new()
                    .without_attempt_timeout()
                    .without_total_timeout(),
            )
            .build()?;
        next.run(call).await
    }
}

#[cfg(feature = "http-client-auth")]
#[tokio::test(start_paused = true)]
async fn middleware_cannot_disable_frozen_attempt_during_auth_preparation() -> TestResult {
    let config = HttpClientConfig::new("http://127.0.0.1:1")?
        .with_timeouts(HttpTimeouts::new().with_attempt_timeout(Duration::from_secs(3)))?
        .with_auth(PendingAuth)
        .with_middleware(ReplaceTimeouts);
    let client = HttpClient::new(config, "timeout-middleware-auth")?;
    let request = HttpRequest::builder().path("/").build()?;
    let result = tokio::time::timeout(Duration::from_secs(10), client.stream(request)).await;
    client.shutdown().await?;
    let error = result?
        .err()
        .ok_or_else(|| io::Error::other("pending auth unexpectedly completed"))?;
    check(
        error.timeout_phase() == Some(TimeoutPhase::Attempt),
        "middleware changed frozen attempt budget",
    )?;
    check(
        error.context().stage == Some(ErrorStage::Provider),
        "auth timeout lost Provider stage",
    )?;
    check(
        error.context().http_attempt == Some(1),
        "auth timeout lost attempt number",
    )?;
    check(
        error.context().http_request_id.is_some(),
        "auth timeout lost request ID",
    )
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

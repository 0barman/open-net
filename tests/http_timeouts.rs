#![cfg(feature = "http-client")]

use open_net::api::retry::RetrySetting;

use open_net::api::error::{ErrorKind, ErrorStage};
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpRequest, HttpRequestId, HttpRequestMethod,
    HttpRequestOptions, HttpRequestTrait, HttpResponseResult, HttpTimeouts, TimeoutPhase,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn check(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}
struct Peer(tokio::task::JoinHandle<TestResult>);
impl Drop for Peer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn peer(headers: bool) -> TestResult<(String, oneshot::Sender<()>, Peer)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let (release, wait) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let n = socket.read(&mut buffer).await?;
            if n == 0 {
                return Err(std::io::Error::other("unexpected request EOF").into());
            }
            request.extend_from_slice(&buffer[..n]);
        }
        if headers {
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n")
                .await?;
        }
        let _ = wait.await;
        if !headers {
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n")
                .await?;
        }
        let _ = socket.write_all(b"x").await;
        Ok(())
    });
    Ok((url, release, Peer(task)))
}

#[tokio::test]
async fn headers_and_idle_failures_have_phase_request_and_attempt() -> TestResult {
    for (headers, phase) in [
        (false, TimeoutPhase::Headers),
        (true, TimeoutPhase::IdleRead),
    ] {
        let (url, _release, _peer) = peer(headers).await?;
        let timeouts = HttpTimeouts::new()
            .without_attempt_timeout()
            .with_headers_timeout(Duration::from_millis(150))
            .with_idle_read_timeout(Duration::from_millis(150));
        let client = HttpClient::new(
            HttpClientConfig::new(url)?.with_timeouts(timeouts)?,
            "phase-context",
        )?;
        let result = tokio::time::timeout(Duration::from_secs(3), client.get("/")).await?;
        check(
            matches!(result, Err(ref e) if e.kind() == ErrorKind::TimedOut && e.timeout_phase() == Some(phase)
            && e.context().http_request_id.is_some() && e.context().http_attempt == Some(1)),
            "buffered phase/context missing",
        )?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn unpolled_stream_releases_admission_and_retains_one_error() -> TestResult {
    let (url, _release, _peer) = peer(true).await?;
    let mut config = HttpClientConfig::new(url)?.with_timeouts(
        HttpTimeouts::new()
            .without_attempt_timeout()
            .with_idle_read_timeout(Duration::from_millis(150)),
    )?;
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "unpolled-timeout")?;
    let mut response = client
        .stream(HttpRequest::builder().path("/").build()?)
        .await?;
    let id = response.request_id;
    // Waiting for registry drain, without ever polling the body, is the regression.
    tokio::time::timeout(Duration::from_secs(3), client.drain()).await??;
    check(
        matches!(response.next_chunk().await, Some(Err(ref e)) if e.timeout_phase() == Some(TimeoutPhase::IdleRead)
        && e.context().http_request_id == Some(id) && e.context().http_attempt == Some(1)),
        "unpolled body lost terminal context",
    )?;
    check(
        response.next_chunk().await.is_none(),
        "terminal error not fused",
    )?;
    client.shutdown().await?;
    Ok(())
}

struct Callback(Arc<AtomicUsize>);
#[open_net::async_trait]
impl HttpRequestTrait for Callback {
    fn get_path(&self) -> String {
        "/".to_owned()
    }
    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::GET
    }
    async fn deal_with_response(self: Box<Self>, _: HttpResponseResult) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn admission_timeout_reserves_id_without_callback_and_immediate_stays_full() -> TestResult {
    let (url, release, _peer) = peer(true).await?;
    let mut config =
        HttpClientConfig::new(url)?.with_timeouts(HttpTimeouts::new().without_attempt_timeout())?;
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "admission-phase")?;
    let first = client
        .stream(HttpRequest::builder().path("/").build()?)
        .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let options = HttpRequestOptions::new()
        .with_request_id(HttpRequestId(901))
        .with_timeouts(HttpTimeouts::new().with_admission_timeout(Duration::from_millis(100)))?;
    for _ in 0..2 {
        let result = client
            .send_wait_with_options(Callback(Arc::clone(&calls)), options.clone())
            .await;
        check(
            matches!(result, Err(ref e) if e.error().timeout_phase() == Some(TimeoutPhase::Admission)
            && e.error().context().http_request_id == Some(HttpRequestId(901)) && e.error().context().http_attempt == Some(0)
            && e.error().context().stage == Some(ErrorStage::Admission)),
            "admission phase/ID or reservation release failed",
        )?;
    }
    let immediate = client.send_with_options(Callback(Arc::clone(&calls)), options);
    check(
        matches!(immediate, Err(ref e) if e.error().kind() == ErrorKind::QueueFull),
        "immediate API became waiting",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 0,
        "rejected request invoked callback",
    )?;
    drop(first);
    let _ = release.send(());
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn disabling_attempt_overrides_both_legacy_limits_and_shutdown_reaps_unpolled_body(
) -> TestResult {
    let (url, _release, _peer) = peer(true).await?;
    let config = HttpClientConfig::new(url)?
        .with_timeout(Duration::from_millis(50))?
        .with_timeouts(HttpTimeouts::new().with_attempt_timeout(Duration::from_millis(50)))?;
    let client = HttpClient::new(config, "disabled-attempt")?;
    let request = HttpRequest::builder()
        .path("/")
        .retry_setting(RetrySetting::Disabled)
        .timeouts(
            HttpTimeouts::new()
                .without_attempt_timeout()
                .without_total_timeout(),
        )
        .build()?;
    let mut response = client.stream(request).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut next = Box::pin(response.next_chunk());
    check(
        futures_util::poll!(next.as_mut()).is_pending(),
        "legacy attempt limit remained enabled",
    )?;
    drop(next);
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    check(
        matches!(response.next_chunk().await, Some(Err(ref e)) if e.kind() == ErrorKind::Cancelled),
        "shutdown did not terminate unbounded stream",
    )?;
    check(
        response.next_chunk().await.is_none(),
        "shutdown body not fused",
    )?;
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

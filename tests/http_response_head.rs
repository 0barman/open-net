#![cfg(feature = "http-client")]

use open_net::api::retry::{Backoff, RetrySchedule, RetrySetting};

use http::StatusCode;
use open_net::api::http::{
    HeaderMap, HttpClient, HttpClientConfig, HttpRequestMethod, HttpRequestOptions,
    HttpRequestTrait, HttpResponseResult, HttpRetryPolicy, HttpTimeouts, RetryOn, TimeoutPhase,
};
use open_net::error::{ErrorKind, ErrorStage, NetError};
use std::error::Error;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const LIMIT: Duration = Duration::from_secs(5);
const PARTIAL_OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nX-Attempt: first\r\nSet-Cookie: private-cookie\r\nConnection: close\r\n\r\nx";
const PARTIAL_UNAUTHORIZED: &[u8] = b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 4\r\nX-Attempt: second\r\nSet-Cookie: private-cookie\r\nConnection: close\r\n\r\nx";
const COMPLETE_OK: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-Attempt: final\r\nConnection: close\r\n\r\nok";
const MALFORMED: &[u8] = b"this is not an HTTP response\r\n\r\n";

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

struct Peer {
    url: String,
    written: tokio::sync::mpsc::UnboundedReceiver<()>,
    task: tokio::task::JoinHandle<TestResult>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn peer(responses: Vec<&'static [u8]>, hold_last: bool) -> TestResult<Peer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let (written_tx, written) = tokio::sync::mpsc::unbounded_channel();
    let count = responses.len();
    let task = tokio::spawn(async move {
        for (index, output) in responses.into_iter().enumerate() {
            let (mut socket, _) = listener.accept().await?;
            let mut request = Vec::new();
            while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                let mut buffer = [0; 1024];
                let size = socket.read(&mut buffer).await?;
                check(size != 0, "request ended before headers")?;
                request.extend_from_slice(&buffer[..size]);
            }
            socket.write_all(output).await?;
            let _ = written_tx.send(());
            if hold_last && index + 1 == count {
                std::future::pending::<()>().await;
            }
            socket.shutdown().await?;
        }
        Ok(())
    });
    Ok(Peer { url, written, task })
}

struct Probe {
    done: oneshot::Sender<HttpResponseResult>,
    calls: Arc<AtomicUsize>,
    hooks: Arc<AtomicUsize>,
}
#[open_net::async_trait]
impl HttpRequestTrait for Probe {
    fn get_path(&self) -> String {
        "/head".into()
    }
    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::GET
    }
    fn on_response_headers(&self, _: &HeaderMap) {
        self.hooks.fetch_add(1, Ordering::SeqCst);
    }
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _ = self.done.send(result);
    }
}

async fn request(
    client: &HttpClient,
    options: HttpRequestOptions,
) -> TestResult<(
    HttpResponseResult,
    open_net::api::http::HttpCompletion,
    usize,
)> {
    let calls = Arc::new(AtomicUsize::new(0));
    let hooks = Arc::new(AtomicUsize::new(0));
    let (done, receiver) = oneshot::channel();
    let handle = client
        .send_with_options(
            Probe {
                done,
                calls: Arc::clone(&calls),
                hooks: Arc::clone(&hooks),
            },
            options,
        )
        .map_err(|error| error.into_error())?;
    let result = tokio::time::timeout(LIMIT, receiver).await??;
    let completion = tokio::time::timeout(LIMIT, handle.finished()).await?;
    completion.callback.clone()?;
    check(
        calls.load(Ordering::SeqCst) == 1,
        "terminal callback count changed",
    )?;
    Ok((result, completion, hooks.load(Ordering::SeqCst)))
}

fn body_error(result: HttpResponseResult) -> TestResult<NetError> {
    match result {
        Err(error) => Ok(error),
        Ok(_) => Err(std::io::Error::other("incomplete body became successful").into()),
    }
}

fn check_head(error: &NetError, status: StatusCode, attempt: &str) -> TestResult {
    let head = error
        .http_response_head()
        .ok_or("received response head missing")?;
    check(
        head.status == status && error.http_status() == Some(status),
        "response status lost",
    )?;
    check(
        head.headers.get("x-attempt").and_then(|h| h.to_str().ok()) == Some(attempt),
        "wrong attempt headers",
    )?;
    check(
        !format!("{head:?} {error:?} {error}").contains("private-cookie"),
        "Debug exposed sensitive header",
    )
}

#[tokio::test]
async fn truncated_200_and_401_retain_head_source_and_completion_without_header_hook() -> TestResult
{
    for (wire, status, attempt) in [
        (PARTIAL_OK, StatusCode::OK, "first"),
        (PARTIAL_UNAUTHORIZED, StatusCode::UNAUTHORIZED, "second"),
    ] {
        let server = peer(vec![wire], false).await?;
        let client = HttpClient::new(HttpClientConfig::new(&server.url)?, "truncated-head")?;
        let (result, completion, hooks) = request(&client, HttpRequestOptions::new()).await?;
        let error = body_error(result)?;
        check_head(&error, status, attempt)?;
        check(
            error.kind() == ErrorKind::Io && error.context().stage == Some(ErrorStage::Receive),
            "body error classification changed",
        )?;
        check(
            error.source().is_some(),
            "original body error source missing",
        )?;
        check(
            hooks == 0,
            "error began invoking the success-only header hook",
        )?;
        let cloned = error.clone();
        let completed = completion
            .response
            .err()
            .ok_or("completion reported success")?;
        for copy in [&cloned, &completed] {
            check_head(copy, status, attempt)?;
            check(
                copy.kind() == error.kind()
                    && copy.context().stage == error.context().stage
                    && copy.context().http_request_id == error.context().http_request_id
                    && copy.context().http_attempt == Some(1),
                "clone or completion lost error context",
            )?;
            check(
                matches!((error.source(), copy.source()), (Some(a), Some(b)) if std::ptr::eq(a, b)),
                "original source was replaced",
            )?;
        }
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn buffered_body_timeouts_keep_their_phase_and_received_head() -> TestResult {
    for phase in [
        TimeoutPhase::IdleRead,
        TimeoutPhase::Attempt,
        TimeoutPhase::Total,
    ] {
        let server = peer(vec![PARTIAL_UNAUTHORIZED], true).await?;
        let duration = Duration::from_millis(200);
        let base = HttpTimeouts::new().without_attempt_timeout();
        let timeouts = match phase {
            TimeoutPhase::IdleRead => base.with_idle_read_timeout(duration),
            TimeoutPhase::Attempt => base.with_attempt_timeout(duration),
            _ => base.with_total_timeout(duration),
        };
        let config = HttpClientConfig::new(&server.url)?.with_timeouts(timeouts)?;
        let client = HttpClient::new(config, "body-timeout-head")?;
        let (result, completion, hooks) = request(&client, HttpRequestOptions::new()).await?;
        let error = body_error(result)?;
        check_head(&error, StatusCode::UNAUTHORIZED, "second")?;
        check(
            error.timeout_phase() == Some(phase)
                && error.context().stage == Some(ErrorStage::Receive),
            "body timeout phase or stage changed",
        )?;
        let completed = completion
            .response
            .err()
            .ok_or("timeout completion became success")?;
        check_head(&completed, StatusCode::UNAUTHORIZED, "second")?;
        check(
            completed.timeout_phase() == Some(phase) && hooks == 0,
            "completion phase or header hook changed",
        )?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn complete_responses_still_invoke_header_hook_once() -> TestResult {
    let server = peer(vec![COMPLETE_OK], false).await?;
    let client = HttpClient::new(HttpClientConfig::new(&server.url)?, "complete-head")?;
    let (result, completion, hooks) = request(&client, HttpRequestOptions::new()).await?;
    let response = result?;
    check(
        response.status == StatusCode::OK && response.body.as_ref() == b"ok" && hooks == 1,
        "normal response contract changed",
    )?;
    check(
        completion.response?.status == StatusCode::OK,
        "normal completion changed",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failures_before_headers_do_not_invent_response_metadata() -> TestResult {
    for wire in [MALFORMED, b"".as_slice()] {
        let server = peer(vec![wire], wire.is_empty()).await?;
        let config = HttpClientConfig::new(&server.url)?
            .with_timeouts(HttpTimeouts::new().with_headers_timeout(Duration::from_millis(200)))?;
        let client = HttpClient::new(config, "no-head")?;
        let (result, _, hooks) = request(&client, HttpRequestOptions::new()).await?;
        let error = body_error(result)?;
        check(
            error.http_response_head().is_none() && error.http_status().is_none() && hooks == 0,
            "pre-header failure retained a head",
        )?;
        if wire.is_empty() {
            check(
                error.timeout_phase() == Some(TimeoutPhase::Headers),
                "header timeout classification changed",
            )?;
        }
        client.shutdown().await?;
    }
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let client = HttpClient::new(HttpClientConfig::new(url)?, "connect-no-head")?;
    let (result, _, _) = request(&client, HttpRequestOptions::new()).await?;
    let error = body_error(result)?;
    check(
        error.http_response_head().is_none() && error.context().stage == Some(ErrorStage::Tcp),
        "connection failure invented a head",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn retries_keep_only_the_final_attempt_response_head() -> TestResult {
    for last in [PARTIAL_UNAUTHORIZED, MALFORMED, COMPLETE_OK] {
        let server = peer(vec![PARTIAL_OK, last], false).await?;
        let client = HttpClient::new(HttpClientConfig::new(&server.url)?, "retry-head")?;
        let policy = HttpRetryPolicy::builder()
            .schedule(
                RetrySchedule::builder()
                    .max_attempts(2)
                    .backoff(Backoff::None)
                    .build()?,
            )
            .retry_on(RetryOn::none().with_body_read(true))
            .build()?;
        let options = HttpRequestOptions::new().with_retry_setting(RetrySetting::Policy(policy));
        let (result, completion, hooks) = request(&client, options).await?;
        if last == COMPLETE_OK {
            let response = result?;
            check(
                response.attempts == 2 && response.body.as_ref() == b"ok" && hooks == 1,
                "successful retry retained earlier body failure",
            )?;
        } else {
            let error = body_error(result)?;
            check(
                error.context().http_attempt == Some(2)
                    && hooks == 0
                    && completion.response.is_err(),
                "failed retry changed completion",
            )?;
            if last == PARTIAL_UNAUTHORIZED {
                check_head(&error, StatusCode::UNAUTHORIZED, "second")?;
            } else {
                check(
                    error.http_response_head().is_none() && error.http_status().is_none(),
                    "earlier attempt head leaked to last failure",
                )?;
            }
        }
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn buffered_size_limit_failure_retains_head_without_succeeding() -> TestResult {
    let server = peer(vec![COMPLETE_OK], false).await?;
    let mut config = HttpClientConfig::new(&server.url)?;
    config.max_response_bytes = 1;
    let client = HttpClient::new(config, "size-limit-head")?;
    let (result, _, hooks) = request(&client, HttpRequestOptions::new()).await?;
    let error = body_error(result)?;
    check_head(&error, StatusCode::OK, "final")?;
    check(
        error.kind() == ErrorKind::ItemTooLarge && hooks == 0,
        "size limit classification changed",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn explicit_cancellation_after_headers_stays_cancelled_without_body_metadata() -> TestResult {
    let mut server = peer(vec![PARTIAL_OK], true).await?;
    let client = HttpClient::new(HttpClientConfig::new(&server.url)?, "cancel-head")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let hooks = Arc::new(AtomicUsize::new(0));
    let (done, receiver) = oneshot::channel();
    let handle = client
        .send(Probe {
            done,
            calls: Arc::clone(&calls),
            hooks: Arc::clone(&hooks),
        })
        .map_err(|error| error.into_error())?;
    tokio::time::timeout(LIMIT, server.written.recv())
        .await?
        .ok_or("peer never wrote headers")?;
    handle.cancel();
    let error = body_error(tokio::time::timeout(LIMIT, receiver).await??)?;
    let completion = tokio::time::timeout(LIMIT, handle.finished()).await?;
    check(
        error.kind() == ErrorKind::Cancelled && error.http_response_head().is_none(),
        "cancellation became a body failure",
    )?;
    check(
        completion.response.err().is_some_and(|error| {
            error.kind() == ErrorKind::Cancelled && error.http_response_head().is_none()
        }),
        "cancelled completion gained metadata",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 1 && hooks.load(Ordering::SeqCst) == 0,
        "cancellation callback contract changed",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

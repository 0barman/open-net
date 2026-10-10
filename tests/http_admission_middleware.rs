#![cfg(feature = "http-client-middleware")]

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::poll;
use http::{HeaderMap, StatusCode};
use open_net::api::http::{
    HttpAdmissionPolicy, HttpCall, HttpClient, HttpClientConfig, HttpMiddleware,
    HttpMiddlewareResponse, HttpRequest, HttpResponse, HttpTimeouts, Next, TimeoutPhase,
};
use open_net::error::{ErrorKind, ErrorStage};
use open_net::NetError;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message).into())
    }
}

fn request(path: &str) -> Result<HttpRequest, NetError> {
    HttpRequest::builder().path(path).build()
}

struct GatedSynthetic {
    calls: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

#[async_trait]
impl HttpMiddleware for GatedSynthetic {
    async fn call(&self, call: HttpCall, _next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if call.request().path == "/hold" {
            self.release.notified().await;
        }
        Ok(HttpMiddlewareResponse::Buffered(HttpResponse::synthetic(
            call.request_id(),
            StatusCode::OK,
            HeaderMap::new(),
            Bytes::from_static(b"synthetic"),
        )))
    }
}

#[tokio::test]
async fn default_wait_stays_pending_before_buffered_middleware() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let mut config = HttpClientConfig::new("http://127.0.0.1:1")?.with_middleware(GatedSynthetic {
        calls: Arc::clone(&calls),
        release: Arc::clone(&release),
    });
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "admission-middleware-wait")?;
    let mut first = Box::pin(client.request(request("/hold")?));
    check(
        poll!(first.as_mut()).is_pending(),
        "first request did not reach middleware gate",
    )?;
    check(
        calls.load(Ordering::Acquire) == 1,
        "first middleware was not entered",
    )?;
    let mut second = Box::pin(client.request(request("/second")?));
    let second_pending = poll!(second.as_mut()).is_pending();
    let admitted_calls = calls.load(Ordering::Acquire);
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), first).await??;
    if second_pending {
        tokio::time::timeout(Duration::from_secs(3), second).await??;
    } else {
        drop(second);
    }
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    check(
        second_pending,
        "default Wait returned before capacity was available",
    )?;
    check(
        admitted_calls == 1,
        "waiting request ran middleware before admission",
    )?;
    check(
        calls.load(Ordering::Acquire) == 2,
        "second request did not continue exactly once",
    )
}

struct TestServer {
    url: String,
    hits: Arc<AtomicUsize>,
    task: JoinHandle<TestResult>,
}

impl TestServer {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let hits = Arc::new(AtomicUsize::new(0));
        let task_hits = Arc::clone(&hits);
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await?;
                let mut received = Vec::new();
                let mut buffer = [0_u8; 1024];
                while !received.windows(4).any(|window| window == b"\r\n\r\n") {
                    let count = stream.read(&mut buffer).await?;
                    if count == 0 {
                        return Err(io::Error::other("client closed before request headers").into());
                    }
                    received.extend_from_slice(&buffer[..count]);
                    check(received.len() <= 64 * 1024, "oversized test request")?;
                }
                task_hits.fetch_add(1, Ordering::AcqRel);
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .await?;
                stream.shutdown().await?;
            }
        });
        Ok(Self { url, hits, task })
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::Acquire)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct TransportGate {
    calls: Arc<AtomicUsize>,
    entered: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
}

#[async_trait]
impl HttpMiddleware for TransportGate {
    async fn call(&self, call: HttpCall, next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if call.request().path == "/hold" {
            self.entered
                .send(())
                .map_err(|_| NetError::from(ErrorKind::Internal))?;
            self.release.notified().await;
        }
        next.run(call).await
    }
}

fn gated_client(
    url: &str,
    capacity: usize,
) -> TestResult<(
    HttpClient,
    Arc<AtomicUsize>,
    Arc<Notify>,
    mpsc::UnboundedReceiver<()>,
)> {
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let (entered, receiver) = mpsc::unbounded_channel();
    let mut config = HttpClientConfig::new(url)?.with_middleware(TransportGate {
        calls: Arc::clone(&calls),
        entered,
        release: Arc::clone(&release),
    });
    config.request_queue_capacity = capacity;
    Ok((
        HttpClient::new(config, "admission-middleware-gate")?,
        calls,
        release,
        receiver,
    ))
}

async fn run_request(
    client: &HttpClient,
    request: HttpRequest,
    streaming: bool,
) -> Result<(), NetError> {
    if streaming {
        let mut response = client.stream(request).await?;
        while let Some(chunk) = response.next_chunk().await {
            let _ = chunk?;
        }
        Ok(())
    } else {
        client.request(request).await.map(|_| ())
    }
}

async fn fail_fast_has_no_side_effects(streaming: bool) -> TestResult {
    let server = TestServer::start().await?;
    let (client, calls, _release, _entered) = gated_client(&server.url, 1)?;
    let mut held = Box::pin(client.request(request("/hold")?));
    check(
        poll!(held.as_mut()).is_pending(),
        "holder did not remain pending",
    )?;
    check(
        calls.load(Ordering::Acquire) == 1,
        "holder was not admitted",
    )?;
    let rejected = HttpRequest::builder()
        .path("/rejected")
        .admission_policy(HttpAdmissionPolicy::FailFast)
        .build()?;
    let mut operation = Box::pin(run_request(&client, rejected, streaming));
    let error = match poll!(operation.as_mut()) {
        std::task::Poll::Ready(Err(error)) => error,
        _ => return Err(io::Error::other("FailFast did not reject immediately").into()),
    };
    check(error.kind() == ErrorKind::QueueFull, "wrong FailFast error")?;
    check(
        error.context().http_attempt == Some(0),
        "rejected request acquired a transport attempt",
    )?;
    check(
        calls.load(Ordering::Acquire) == 1,
        "rejected request ran middleware",
    )?;
    check(server.hits() == 0, "rejected request reached transport")?;
    drop(operation);
    drop(held);
    tokio::time::timeout(Duration::from_secs(3), client.drain()).await??;
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    check(
        server.hits() == 0,
        "dropped holder unexpectedly reached transport",
    )
}

#[tokio::test]
async fn buffered_fail_fast_rejects_before_middleware_or_transport() -> TestResult {
    fail_fast_has_no_side_effects(false).await
}

#[tokio::test]
async fn stream_fail_fast_rejects_before_middleware_or_transport() -> TestResult {
    fail_fast_has_no_side_effects(true).await
}

#[tokio::test]
async fn stream_wait_shares_buffered_capacity_and_runs_middleware_once() -> TestResult {
    let server = TestServer::start().await?;
    let (client, calls, release, _entered) = gated_client(&server.url, 1)?;
    let mut held = Box::pin(client.request(request("/hold")?));
    check(
        poll!(held.as_mut()).is_pending(),
        "holder did not remain pending",
    )?;
    let mut waiter = Box::pin(run_request(&client, request("/stream")?, true));
    check(
        poll!(waiter.as_mut()).is_pending(),
        "stream did not wait for shared capacity",
    )?;
    check(
        calls.load(Ordering::Acquire) == 1,
        "stream middleware ran before admission",
    )?;
    check(
        server.hits() == 0,
        "transport started before holder was released",
    )?;
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), held).await??;
    tokio::time::timeout(Duration::from_secs(3), waiter).await??;
    tokio::time::timeout(Duration::from_secs(3), client.drain()).await??;
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    check(
        calls.load(Ordering::Acquire) == 2,
        "middleware ran more than once per request",
    )?;
    check(server.hits() == 2, "transport count mismatch")
}

struct SyntheticOrReject;

#[async_trait]
impl HttpMiddleware for SyntheticOrReject {
    async fn call(&self, call: HttpCall, _next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        if call.request().path == "/reject" {
            return Err(NetError::from(ErrorKind::ProviderFailed));
        }
        Ok(HttpMiddlewareResponse::Buffered(HttpResponse::synthetic(
            call.request_id(),
            StatusCode::OK,
            HeaderMap::new(),
            Bytes::new(),
        )))
    }
}

#[tokio::test]
async fn short_circuit_and_error_release_admission_in_both_modes() -> TestResult {
    let server = TestServer::start().await?;
    let mut config = HttpClientConfig::new(&server.url)?.with_middleware(SyntheticOrReject);
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "admission-middleware-short-circuit")?;
    for streaming in [false, true] {
        for reject in [false, true] {
            let path = if reject { "/reject" } else { "/synthetic" };
            let result = run_request(&client, request(path)?, streaming).await;
            match (reject, streaming, result) {
                (true, _, Err(error)) => check(
                    error.kind() == ErrorKind::ProviderFailed,
                    "middleware error changed",
                )?,
                (false, true, Err(error)) => check(
                    error.kind() == ErrorKind::RoutingModeMismatch,
                    "stream accepted synthetic buffered response",
                )?,
                (false, false, Ok(())) => {}
                _ => return Err(io::Error::other("unexpected middleware outcome").into()),
            }
            tokio::time::timeout(Duration::from_secs(3), client.drain()).await??;
            let successor = HttpRequest::builder()
                .path("/successor")
                .admission_policy(HttpAdmissionPolicy::FailFast)
                .build()?;
            client.request(successor).await?;
        }
    }
    client.shutdown().await?;
    check(
        server.hits() == 0,
        "short-circuit or rejection sent a transport request",
    )
}

struct ReplaceFrozenOptions {
    entered: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpMiddleware for ReplaceFrozenOptions {
    async fn call(
        &self,
        mut call: HttpCall,
        _next: Next,
    ) -> Result<HttpMiddlewareResponse, NetError> {
        self.entered.fetch_add(1, Ordering::AcqRel);
        *call.request_mut() = HttpRequest::builder()
            .path("/replacement")
            .admission_policy(HttpAdmissionPolicy::FailFast)
            .timeouts(
                HttpTimeouts::new()
                    .without_admission_timeout()
                    .without_total_timeout(),
            )
            .build()?;
        std::future::pending().await
    }
}

async fn replacement_cannot_extend_total(streaming: bool) -> TestResult {
    let entered = Arc::new(AtomicUsize::new(0));
    let mut config = HttpClientConfig::new("http://127.0.0.1:1")?
        .with_timeouts(HttpTimeouts::new().with_total_timeout(Duration::from_millis(80)))?
        .with_middleware(ReplaceFrozenOptions {
            entered: Arc::clone(&entered),
        });
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "admission-frozen-total")?;
    let result = run_request(&client, request("/original")?, streaming).await;
    let error = result
        .err()
        .ok_or_else(|| io::Error::other("replacement bypassed total deadline"))?;
    check(
        error.kind() == ErrorKind::DeadlineExceeded,
        "wrong total timeout error",
    )?;
    check(
        error.timeout_phase() == Some(TimeoutPhase::Total),
        "replacement changed timeout phase",
    )?;
    check(
        error.context().stage == Some(ErrorStage::Dispatch),
        "middleware total lost dispatch stage",
    )?;
    check(
        error.context().http_attempt == Some(0),
        "replacement started a transport attempt",
    )?;
    check(
        entered.load(Ordering::Acquire) == 1,
        "middleware entry count mismatch",
    )?;
    tokio::time::timeout(Duration::from_secs(3), client.drain()).await??;
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn buffered_middleware_replacement_cannot_extend_frozen_total() -> TestResult {
    replacement_cannot_extend_total(false).await
}

#[tokio::test]
async fn stream_middleware_replacement_cannot_extend_frozen_total() -> TestResult {
    replacement_cannot_extend_total(true).await
}

#[tokio::test(start_paused = true)]
async fn admission_budget_stops_after_acceptance_before_middleware() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let mut config = HttpClientConfig::new("http://127.0.0.1:1")?
        .with_timeouts(
            HttpTimeouts::new()
                .with_admission_timeout(Duration::from_millis(10))
                .with_total_timeout(Duration::from_secs(1)),
        )?
        .with_middleware(GatedSynthetic {
            calls: Arc::clone(&calls),
            release: Arc::clone(&release),
        });
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "admission-budget-finished")?;
    let mut operation = Box::pin(client.request(request("/hold")?));
    check(
        poll!(operation.as_mut()).is_pending(),
        "middleware gate was not entered",
    )?;
    tokio::time::advance(Duration::from_millis(20)).await;
    check(
        poll!(operation.as_mut()).is_pending(),
        "admission budget leaked into accepted middleware",
    )?;
    release.notify_one();
    operation.await?;
    client.shutdown().await?;
    check(
        calls.load(Ordering::Acquire) == 1,
        "accepted request was admitted twice",
    )
}

async fn accepted_request_survives_admission_close(streaming: bool) -> TestResult {
    let server = TestServer::start().await?;
    let (client, calls, release, _entered) = gated_client(&server.url, 1)?;
    let mut accepted = Box::pin(run_request(&client, request("/hold")?, streaming));
    check(
        poll!(accepted.as_mut()).is_pending(),
        "accepted request did not enter middleware",
    )?;
    check(
        calls.load(Ordering::Acquire) == 1,
        "request was not accepted before closure",
    )?;
    client.request_shutdown();
    let rejected = client.request(request("/after-close")?).await;
    let error = rejected
        .err()
        .ok_or_else(|| io::Error::other("closed client accepted a new request"))?;
    check(
        error.kind() == ErrorKind::QueueClosed,
        "new request failed for wrong reason",
    )?;
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), accepted).await??;
    tokio::time::timeout(Duration::from_secs(3), client.drain()).await??;
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    check(
        calls.load(Ordering::Acquire) == 1,
        "closed admission invoked another middleware",
    )?;
    check(
        server.hits() == 1,
        "accepted request was lost or duplicated during close",
    )
}

#[tokio::test]
async fn accepted_buffered_publishes_reserved_slot_after_request_shutdown() -> TestResult {
    accepted_request_survives_admission_close(false).await
}

#[tokio::test]
async fn accepted_stream_publishes_supervisor_slot_after_request_shutdown() -> TestResult {
    accepted_request_survives_admission_close(true).await
}

async fn full_shutdown_cancels_polled_middleware(streaming: bool) -> TestResult {
    let server = TestServer::start().await?;
    let (client, calls, _release, mut entered) = gated_client(&server.url, 1)?;
    let task_client = client.clone();
    let task =
        tokio::spawn(async move { run_request(&task_client, request("/hold")?, streaming).await });
    tokio::time::timeout(Duration::from_secs(3), entered.recv())
        .await?
        .ok_or_else(|| io::Error::other("middleware entry channel closed"))?;
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    let outcome = tokio::time::timeout(Duration::from_secs(3), task).await??;
    let error = outcome
        .err()
        .ok_or_else(|| io::Error::other("shutdown allowed pending middleware to succeed"))?;
    check(
        error.kind() == ErrorKind::Cancelled,
        "full shutdown did not cancel middleware",
    )?;
    check(
        calls.load(Ordering::Acquire) == 1,
        "unexpected middleware invocation",
    )?;
    check(server.hits() == 0, "cancelled middleware reached transport")
}

#[tokio::test]
async fn full_shutdown_cancels_buffered_middleware_and_releases_slot() -> TestResult {
    full_shutdown_cancels_polled_middleware(false).await
}

#[tokio::test]
async fn full_shutdown_cancels_stream_middleware_and_releases_slot() -> TestResult {
    full_shutdown_cancels_polled_middleware(true).await
}

async fn retained_unpolled_future_delays_shutdown_until_drop(streaming: bool) -> TestResult {
    let server = TestServer::start().await?;
    let (client, calls, _release, _entered) = gated_client(&server.url, 1)?;
    let mut retained = Box::pin(run_request(&client, request("/hold")?, streaming));
    check(
        poll!(retained.as_mut()).is_pending(),
        "retained future was not pending",
    )?;
    check(
        calls.load(Ordering::Acquire) == 1,
        "retained future was not accepted",
    )?;
    let mut shutdown = Box::pin(client.shutdown());
    let remains_waiting = tokio::time::timeout(Duration::from_millis(40), shutdown.as_mut())
        .await
        .is_err();
    drop(retained);
    if remains_waiting {
        tokio::time::timeout(Duration::from_secs(3), shutdown).await??;
    }
    check(
        remains_waiting,
        "shutdown did not wait for the retained accepted reservation",
    )?;
    check(
        server.hits() == 0,
        "retained middleware unexpectedly reached transport",
    )
}

#[tokio::test]
async fn dropping_retained_buffered_future_unblocks_shutdown() -> TestResult {
    retained_unpolled_future_delays_shutdown_until_drop(false).await
}

#[tokio::test]
async fn dropping_retained_stream_opening_future_unblocks_shutdown() -> TestResult {
    retained_unpolled_future_delays_shutdown_until_drop(true).await
}

struct CoordinatedLanes {
    entered: Arc<AtomicUsize>,
    buffered_release: Arc<Notify>,
    buffered_finished: Arc<Notify>,
}

#[async_trait]
impl HttpMiddleware for CoordinatedLanes {
    async fn call(&self, call: HttpCall, next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        self.entered.fetch_add(1, Ordering::AcqRel);
        if call.request().path == "/buffered" {
            self.buffered_release.notified().await;
            let result = next.run(call).await;
            self.buffered_finished.notify_one();
            result
        } else {
            self.buffered_finished.notified().await;
            next.run(call).await
        }
    }
}

#[tokio::test]
async fn closing_worker_drains_buffered_while_stream_holds_supervisor_reservation() -> TestResult {
    let server = TestServer::start().await?;
    let entered = Arc::new(AtomicUsize::new(0));
    let buffered_release = Arc::new(Notify::new());
    let mut config = HttpClientConfig::new(&server.url)?.with_middleware(CoordinatedLanes {
        entered: Arc::clone(&entered),
        buffered_release: Arc::clone(&buffered_release),
        buffered_finished: Arc::new(Notify::new()),
    });
    config.request_queue_capacity = 2;
    let client = HttpClient::new(config, "admission-concurrent-drain")?;
    let mut buffered = Box::pin(run_request(&client, request("/buffered")?, false));
    let mut streaming = Box::pin(run_request(&client, request("/stream")?, true));
    check(
        poll!(buffered.as_mut()).is_pending(),
        "buffered gate was not pending",
    )?;
    check(
        poll!(streaming.as_mut()).is_pending(),
        "stream gate was not pending",
    )?;
    check(
        entered.load(Ordering::Acquire) == 2,
        "both operations were not accepted",
    )?;
    client.request_shutdown();
    buffered_release.notify_one();
    let (buffered_result, streaming_result) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(buffered, streaming)
    })
    .await?;
    buffered_result?;
    streaming_result?;
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    check(
        server.hits() == 2,
        "concurrent lane drain lost an accepted operation",
    )
}

struct NestedRequest {
    client: Arc<Mutex<Option<HttpClient>>>,
    nested_policy: HttpAdmissionPolicy,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpMiddleware for NestedRequest {
    async fn call(&self, call: HttpCall, _next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if call.request().path == "/outer" {
            let client = self
                .client
                .lock()
                .map_err(|_| NetError::from(ErrorKind::Internal))?
                .take()
                .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
            let nested = HttpRequest::builder()
                .path("/nested")
                .admission_policy(self.nested_policy)
                .timeouts(HttpTimeouts::new().with_admission_timeout(Duration::from_millis(50)))
                .build()?;
            client.request(nested).await?;
        }
        Ok(HttpMiddlewareResponse::Buffered(HttpResponse::synthetic(
            call.request_id(),
            StatusCode::OK,
            HeaderMap::new(),
            Bytes::new(),
        )))
    }
}

async fn nested_request_is_bounded(policy: HttpAdmissionPolicy) -> TestResult {
    let holder = Arc::new(Mutex::new(None));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = HttpClientConfig::new("http://127.0.0.1:1")?.with_middleware(NestedRequest {
        client: Arc::clone(&holder),
        nested_policy: policy,
        calls: Arc::clone(&calls),
    });
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "admission-middleware-nested")?;
    *holder
        .lock()
        .map_err(|_| io::Error::other("client holder poisoned"))? = Some(client.clone());
    let result =
        tokio::time::timeout(Duration::from_secs(2), client.request(request("/outer")?)).await?;
    let error = result
        .err()
        .ok_or_else(|| io::Error::other("nested request bypassed outer capacity"))?;
    match policy {
        HttpAdmissionPolicy::Wait => {
            check(
                error.kind() == ErrorKind::TimedOut,
                "nested Wait did not time out",
            )?;
            check(
                error.timeout_phase() == Some(TimeoutPhase::Admission),
                "nested Wait lost admission phase",
            )?;
        }
        HttpAdmissionPolicy::FailFast => check(
            error.kind() == ErrorKind::QueueFull,
            "nested FailFast did not reject",
        )?,
    }
    check(
        calls.load(Ordering::Acquire) == 1,
        "nested request ran middleware while blocked",
    )?;
    client.request(request("/recover")?).await?;
    tokio::time::timeout(Duration::from_secs(3), client.drain()).await??;
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    check(
        calls.load(Ordering::Acquire) == 2,
        "outer failure leaked admission resources",
    )
}

#[tokio::test]
async fn nested_wait_has_bounded_admission_and_releases_outer_resources() -> TestResult {
    nested_request_is_bounded(HttpAdmissionPolicy::Wait).await
}

#[tokio::test]
async fn nested_fail_fast_returns_without_entering_nested_middleware() -> TestResult {
    nested_request_is_bounded(HttpAdmissionPolicy::FailFast).await
}

struct ReplaceAndContinue {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpMiddleware for ReplaceAndContinue {
    async fn call(
        &self,
        mut call: HttpCall,
        next: Next,
    ) -> Result<HttpMiddlewareResponse, NetError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        *call.request_mut() = HttpRequest::builder()
            .path("/replacement")
            .admission_policy(HttpAdmissionPolicy::Wait)
            .timeouts(
                HttpTimeouts::new()
                    .with_admission_timeout(Duration::from_nanos(1))
                    .with_total_timeout(Duration::from_nanos(1)),
            )
            .build()?;
        next.run(call).await
    }
}

async fn replacement_cannot_readmit_or_replace_budgets(streaming: bool) -> TestResult {
    let server = TestServer::start().await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = HttpClientConfig::new(&server.url)?
        .with_admission_policy(HttpAdmissionPolicy::FailFast)
        .with_timeouts(HttpTimeouts::new().with_total_timeout(Duration::from_secs(2)))?
        .with_middleware(ReplaceAndContinue {
            calls: Arc::clone(&calls),
        });
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "admission-replacement-terminal")?;
    tokio::time::timeout(
        Duration::from_secs(3),
        run_request(&client, request("/original")?, streaming),
    )
    .await??;
    tokio::time::timeout(Duration::from_secs(3), client.drain()).await??;
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    check(
        calls.load(Ordering::Acquire) == 1,
        "request replacement caused another admission",
    )?;
    check(
        server.hits() == 1,
        "request replacement lost or duplicated transport",
    )
}

#[tokio::test]
async fn buffered_terminal_reuses_frozen_admission_after_request_replacement() -> TestResult {
    replacement_cannot_readmit_or_replace_budgets(false).await
}

#[tokio::test]
async fn stream_terminal_reuses_frozen_admission_after_request_replacement() -> TestResult {
    replacement_cannot_readmit_or_replace_budgets(true).await
}

async fn cancel_all_removes_waiter_before_middleware(streaming: bool) -> TestResult {
    let server = TestServer::start().await?;
    let (client, calls, _release, _entered) = gated_client(&server.url, 1)?;
    let mut held = Box::pin(client.request(request("/hold")?));
    check(
        poll!(held.as_mut()).is_pending(),
        "holder did not remain pending",
    )?;
    let mut waiter = Box::pin(run_request(&client, request("/waiter")?, streaming));
    check(
        poll!(waiter.as_mut()).is_pending(),
        "waiter was not registered",
    )?;
    client.cancel_all()?;
    let waiter_result = tokio::time::timeout(Duration::from_secs(3), waiter).await?;
    let waiter_error = waiter_result
        .err()
        .ok_or_else(|| io::Error::other("cancelled waiter succeeded"))?;
    check(
        waiter_error.kind() == ErrorKind::Cancelled,
        "wrong waiter cancellation outcome",
    )?;
    check(
        waiter_error.context().http_attempt == Some(0),
        "cancelled waiter started transport",
    )?;
    let held_result = tokio::time::timeout(Duration::from_secs(3), held).await?;
    let held_error = held_result
        .err()
        .ok_or_else(|| io::Error::other("cancelled holder succeeded"))?;
    check(
        held_error.kind() == ErrorKind::Cancelled,
        "wrong holder cancellation outcome",
    )?;
    tokio::time::timeout(Duration::from_secs(3), client.drain()).await??;
    check(
        calls.load(Ordering::Acquire) == 1,
        "cancelled waiter invoked middleware",
    )?;
    check(
        server.hits() == 0,
        "cancelled waiter or holder reached transport",
    )?;
    client
        .request(
            HttpRequest::builder()
                .path("/recover")
                .admission_policy(HttpAdmissionPolicy::FailFast)
                .build()?,
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    check(
        server.hits() == 1,
        "cancellation leaked an admission reservation",
    )
}

#[tokio::test]
async fn cancel_all_removes_buffered_waiter_before_middleware() -> TestResult {
    cancel_all_removes_waiter_before_middleware(false).await
}

#[tokio::test]
async fn cancel_all_removes_stream_waiter_before_middleware() -> TestResult {
    cancel_all_removes_waiter_before_middleware(true).await
}

struct AfterTransportGate {
    entered: mpsc::UnboundedSender<()>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpMiddleware for AfterTransportGate {
    async fn call(&self, call: HttpCall, next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let held = call.request().path == "/hold";
        let response = next.run(call).await?;
        if held {
            self.entered
                .send(())
                .map_err(|_| NetError::from(ErrorKind::Internal))?;
            std::future::pending::<()>().await;
        }
        Ok(response)
    }
}

async fn after_transport_drop_releases_middleware_capacity(streaming: bool) -> TestResult {
    let server = TestServer::start().await?;
    let (entered, mut observed) = mpsc::unbounded_channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = HttpClientConfig::new(&server.url)?.with_middleware(AfterTransportGate {
        entered,
        calls: Arc::clone(&calls),
    });
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "admission-after-transport-drop")?;
    let mut held = Box::pin(run_request(&client, request("/hold")?, streaming));
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            entered = observed.recv() => entered.ok_or_else(|| io::Error::other("after-transport gate closed")),
            _ = held.as_mut() => Err(io::Error::other("middleware did not retain the completed transport")),
        }
    }).await??;
    check(server.hits() == 1, "middleware gate preceded transport")?;
    let mut waiter = Box::pin(client.request(request("/successor")?));
    check(
        poll!(waiter.as_mut()).is_pending(),
        "transport completion released middleware capacity early",
    )?;
    check(
        calls.load(Ordering::Acquire) == 1,
        "waiter entered middleware before the outer future ended",
    )?;
    drop(held);
    tokio::time::timeout(Duration::from_secs(3), waiter).await??;
    tokio::time::timeout(Duration::from_secs(3), client.drain()).await??;
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    check(
        calls.load(Ordering::Acquire) == 2,
        "outer drop did not restore middleware capacity",
    )?;
    check(
        server.hits() == 2,
        "drop cleanup duplicated or lost transport",
    )
}

#[tokio::test]
async fn buffered_middleware_after_transport_retains_capacity_until_drop() -> TestResult {
    after_transport_drop_releases_middleware_capacity(false).await
}

#[tokio::test]
async fn stream_middleware_after_headers_retains_capacity_until_drop() -> TestResult {
    after_transport_drop_releases_middleware_capacity(true).await
}

struct AbandonTerminal;

#[async_trait]
impl HttpMiddleware for AbandonTerminal {
    async fn call(&self, call: HttpCall, next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        if call.request().path == "/abandon" {
            let mut terminal = Box::pin(next.run(call));
            let in_flight = poll!(terminal.as_mut()).is_pending();
            drop(terminal);
            if !in_flight {
                return Err(NetError::from(ErrorKind::Internal));
            }
            return Err(NetError::from(ErrorKind::ProviderFailed));
        }
        Ok(HttpMiddlewareResponse::Buffered(HttpResponse::synthetic(
            call.request_id(),
            StatusCode::OK,
            HeaderMap::new(),
            Bytes::new(),
        )))
    }
}

#[tokio::test]
async fn draining_after_middleware_abandons_terminal_waits_for_worker_cleanup() -> TestResult {
    let server = TestServer::start().await?;
    let mut config = HttpClientConfig::new(&server.url)?.with_middleware(AbandonTerminal);
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "admission-abandoned-terminal")?;
    let mut premature_drain = false;
    // Probe the synchronous drain/admission boundary repeatedly, without sleeping
    // or assuming when the SDK runtime schedules its cancelled callback job.
    for _ in 0..64 {
        let outcome = client.request(request("/abandon")?).await;
        let error = outcome
            .err()
            .ok_or_else(|| io::Error::other("abandoned terminal unexpectedly succeeded"))?;
        check(
            error.kind() == ErrorKind::Cancelled,
            "abandoned terminal did not cancel its operation",
        )?;
        tokio::time::timeout(Duration::from_secs(3), client.drain()).await??;
        let successor = HttpRequest::builder()
            .path("/successor")
            .admission_policy(HttpAdmissionPolicy::FailFast)
            .build()?;
        if let Err(error) = client.request(successor).await {
            check(
                error.kind() == ErrorKind::QueueFull,
                "successor failed for an unrelated reason",
            )?;
            premature_drain = true;
            break;
        }
    }
    tokio::time::timeout(Duration::from_secs(3), client.shutdown()).await??;
    check(
        !premature_drain,
        "drain returned before the abandoned terminal released capacity",
    )
}

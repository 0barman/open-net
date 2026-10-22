#![cfg(feature = "http-client-middleware")]

use open_net::api::retry::{Backoff, RetryEvent, RetrySchedule, RetrySetting, RetryStopReason};

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use open_net::api::http::{
    HttpCall, HttpClient, HttpClientConfig, HttpMiddleware, HttpMiddlewareResponse, HttpRequest,
    HttpResponse, HttpRetryPolicy, HttpStreamResponse, Next,
};
use open_net::NetError;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

type AnyErrorResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type TestResult = AnyErrorResult<()>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message).into())
    }
}

fn observed_policy() -> AnyErrorResult<(HttpRetryPolicy, std::sync::mpsc::Receiver<RetryEvent>)> {
    let (sent, received) = std::sync::mpsc::channel();
    let policy = HttpRetryPolicy::builder().observer(move |event: &RetryEvent| {
        if sent.send(event.clone()).is_err() {
            open_net::log_e!(open_net::LogType::Common; "test.middleware", "error", "observer_receiver_closed");
        }
    }).build()?;
    Ok((policy, received))
}

fn check_terminal_failure(
    error: &NetError,
    events: &std::sync::mpsc::Receiver<RetryEvent>,
) -> TestResult {
    let report = error
        .retry_report()
        .ok_or_else(|| io::Error::other("middleware validation error lost retry report"))?;
    let completed: Vec<_> = events
        .try_iter()
        .filter_map(|event| match event {
            RetryEvent::Completed(report) => Some(report),
            _ => None,
        })
        .collect();
    check(
        completed.len() == 1 && completed.first() == Some(report),
        "middleware observer and delivered error have different terminal reports",
    )?;
    check(
        report.stop_reason.is_some() && report.stop_reason != Some(RetryStopReason::Completed),
        "middleware validation emitted a successful terminal report",
    )?;
    check(
        report.attempts == 0 && report.transport_sends == 0,
        "middleware short-circuit validation invented a transport attempt",
    )
}

struct TestServer {
    url: String,
    hits: Arc<AtomicUsize>,
    join: Option<JoinHandle<Result<String, io::Error>>>,
}

impl TestServer {
    fn start(body: &'static [u8]) -> Result<Self, io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let hits = Arc::new(AtomicUsize::new(0));
        let thread_hits = Arc::clone(&hits);
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept()?;
            thread_hits.fetch_add(1, Ordering::SeqCst);
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            let request = read_request(&mut stream)?;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes())?;
            stream.write_all(body)?;
            stream.flush()?;
            Ok(request)
        });
        Ok(Self {
            url: format!("http://{address}"),
            hits,
            join: Some(join),
        })
    }

    fn finish(mut self) -> AnyErrorResult<String> {
        let join = self
            .join
            .take()
            .ok_or_else(|| io::Error::other("server was already joined"))?;
        join.join()
            .map_err(|_| io::Error::other("server thread panicked"))?
            .map_err(Into::into)
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

fn read_request(stream: &mut TcpStream) -> Result<String, io::Error> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];
    while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.len() > 64 * 1024 {
            return Err(io::Error::other("request headers too large"));
        }
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

struct TraceMiddleware {
    name: &'static str,
    trace: Arc<Mutex<Vec<String>>>,
    add_header: bool,
}

#[async_trait]
impl HttpMiddleware for TraceMiddleware {
    async fn call(
        &self,
        mut call: HttpCall,
        next: Next,
    ) -> Result<HttpMiddlewareResponse, NetError> {
        self.trace
            .lock()
            .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?
            .push(format!("{}:before", self.name));
        if self.add_header {
            call.request_mut()
                .headers
                .insert("x-middleware", HeaderValue::from_static("enabled"));
        }
        let result = next.run(call).await?;
        self.trace
            .lock()
            .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?
            .push(format!("{}:after", self.name));
        Ok(result)
    }
}

struct BufferedShortCircuit;

#[async_trait]
impl HttpMiddleware for BufferedShortCircuit {
    async fn call(&self, call: HttpCall, _next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        Ok(HttpMiddlewareResponse::Buffered(HttpResponse::synthetic(
            call.request_id(),
            StatusCode::IM_A_TEAPOT,
            HeaderMap::new(),
            Bytes::from_static(b"short-circuit"),
        )))
    }
}

struct StreamingShortCircuit;

#[async_trait]
impl HttpMiddleware for StreamingShortCircuit {
    async fn call(&self, call: HttpCall, _next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        let body = futures_util::stream::once(async { Ok(Bytes::from_static(b"invalid")) });
        Ok(HttpMiddlewareResponse::Streaming(HttpStreamResponse::new(
            StatusCode::OK,
            HeaderMap::new(),
            call.request_id(),
            0,
            Box::pin(body),
        )))
    }
}

struct WrongRequestId;

#[async_trait]
impl HttpMiddleware for WrongRequestId {
    async fn call(&self, _call: HttpCall, _next: Next) -> Result<HttpMiddlewareResponse, NetError> {
        Ok(HttpMiddlewareResponse::Buffered(HttpResponse::synthetic(
            open_net::api::http::HttpRequestId(999_999),
            StatusCode::OK,
            HeaderMap::new(),
            Bytes::from_static(b"wrong-id"),
        )))
    }
}

fn request(path: &str) -> Result<HttpRequest, NetError> {
    HttpRequest::builder()
        .method(Method::GET)
        .path(path)
        .build()
}

async fn shutdown(client: HttpClient) -> TestResult {
    client.shutdown_graceful().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn middleware_runs_in_registration_order_and_can_mutate_request() -> TestResult {
    let server = TestServer::start(b"ok")?;
    let trace = Arc::new(Mutex::new(Vec::new()));
    let config = HttpClientConfig::new(&server.url)?
        .with_middleware(TraceMiddleware {
            name: "outer",
            trace: Arc::clone(&trace),
            add_header: true,
        })
        .with_middleware(TraceMiddleware {
            name: "inner",
            trace: Arc::clone(&trace),
            add_header: false,
        });
    let client = HttpClient::new(config, "middleware-order")?;
    let response = client.request(request("/ordered")?).await?;
    check(response.body.as_ref() == b"ok", "response body mismatch")?;
    check(server.hits() == 1, "expected one transport request")?;
    shutdown(client).await?;
    let request_text = server.finish()?;
    check(
        request_text
            .to_ascii_lowercase()
            .contains("x-middleware: enabled"),
        "middleware header was not sent",
    )?;
    let trace = trace
        .lock()
        .map_err(|_| io::Error::other("trace mutex poisoned"))?;
    check(
        trace.as_slice() == ["outer:before", "inner:before", "inner:after", "outer:after"],
        "middleware order mismatch",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_middleware_can_short_circuit_without_transport() -> TestResult {
    let mut config =
        HttpClientConfig::new("http://127.0.0.1:1")?.with_middleware(BufferedShortCircuit);
    config.request_queue_capacity = 1;
    let client = HttpClient::new(config, "middleware-short-circuit")?;
    let response = client.request(request("/cached")?).await?;
    check(
        response.status == StatusCode::IM_A_TEAPOT,
        "short-circuit status mismatch",
    )?;
    check(
        response.body.as_ref() == b"short-circuit",
        "short-circuit body mismatch",
    )?;
    let second = client.request(request("/cached-again")?).await?;
    check(
        second.status == StatusCode::IM_A_TEAPOT,
        "second short-circuit status mismatch",
    )?;
    shutdown(client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_middleware_must_continue_to_transport() -> TestResult {
    let (policy, events) = observed_policy()?;
    let config = HttpClientConfig::new("http://127.0.0.1:1")?
        .with_default_retry_policy(policy)
        .with_middleware(StreamingShortCircuit);
    let client = HttpClient::new(config, "middleware-stream-mode")?;
    let result = client.stream(request("/stream")?).await;
    let error = match result {
        Ok(_) => return Err(io::Error::other("streaming short-circuit was accepted").into()),
        Err(error) => error,
    };
    check(
        error.kind() == open_net::error::ErrorKind::RoutingModeMismatch,
        "wrong streaming mode error",
    )?;
    check_terminal_failure(&error, &events)?;
    shutdown(client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_middleware_cannot_replace_the_sdk_request_id() -> TestResult {
    let (policy, events) = observed_policy()?;
    let config = HttpClientConfig::new("http://127.0.0.1:1")?
        .with_default_retry_policy(policy)
        .with_middleware(WrongRequestId);
    let client = HttpClient::new(config, "middleware-response-id")?;
    let result = client.request(request("/wrong-id")?).await;
    let error = match result {
        Ok(_) => return Err(io::Error::other("wrong response id was accepted").into()),
        Err(error) => error,
    };
    check(
        error.kind() == open_net::error::ErrorKind::Protocol,
        "wrong response id error kind mismatch",
    )?;
    check_terminal_failure(&error, &events)?;
    shutdown(client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_middleware_passes_through_incremental_body() -> TestResult {
    let server = TestServer::start(b"streamed")?;
    let trace = Arc::new(Mutex::new(Vec::new()));
    let config = HttpClientConfig::new(&server.url)?.with_middleware(TraceMiddleware {
        name: "stream",
        trace: Arc::clone(&trace),
        add_header: false,
    });
    let client = HttpClient::new(config, "middleware-stream")?;
    let mut response = client.stream(request("/stream")?).await?;
    let mut body = Vec::new();
    while let Some(chunk) = response.next_chunk().await {
        body.extend_from_slice(chunk?.as_ref());
    }
    check(body.as_slice() == b"streamed", "stream body mismatch")?;
    shutdown(client).await?;
    let _request_text = server.finish()?;
    let trace = trace
        .lock()
        .map_err(|_| io::Error::other("trace mutex poisoned"))?;
    check(
        trace.as_slice() == ["stream:before", "stream:after"],
        "stream middleware order mismatch",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn middleware_error_is_returned_without_transport() -> TestResult {
    struct Reject;

    #[async_trait]
    impl HttpMiddleware for Reject {
        async fn call(
            &self,
            _call: HttpCall,
            _next: Next,
        ) -> Result<HttpMiddlewareResponse, NetError> {
            Err(NetError::from(open_net::error::ErrorKind::ProviderFailed))
        }
    }

    let config = HttpClientConfig::new("http://127.0.0.1:1")?.with_middleware(Reject);
    let client = HttpClient::new(config, "middleware-error")?;
    let result = client.request(request("/reject")?).await;
    let error = match result {
        Ok(_) => return Err(io::Error::other("middleware error was lost").into()),
        Err(error) => error,
    };
    check(
        error.kind() == open_net::error::ErrorKind::ProviderFailed,
        "middleware error kind mismatch",
    )?;
    check(
        error.context().http_request_id.is_some(),
        "middleware error lost request id",
    )?;
    shutdown(client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn middleware_runs_once_while_transport_retries() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let join = thread::spawn(move || -> Result<usize, io::Error> {
        let mut hits = 0_usize;
        for (status, body) in [(500_u16, b"retry".as_slice()), (200_u16, b"ok".as_slice())] {
            let (mut stream, _) = listener.accept()?;
            hits = hits.saturating_add(1);
            let _request = read_request(&mut stream)?;
            let reason = if status == 200 {
                "OK"
            } else {
                "Internal Server Error"
            };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes())?;
            stream.write_all(body)?;
            stream.flush()?;
        }
        Ok(hits)
    });
    let trace = Arc::new(Mutex::new(Vec::new()));
    let config =
        HttpClientConfig::new(format!("http://{address}"))?.with_middleware(TraceMiddleware {
            name: "retry",
            trace: Arc::clone(&trace),
            add_header: false,
        });
    let client = HttpClient::new(config, "middleware-retry")?;
    let request = HttpRequest::builder()
        .method(Method::GET)
        .path("/retry")
        .retry_setting(RetrySetting::Policy(
            HttpRetryPolicy::builder()
                .schedule(
                    RetrySchedule::builder()
                        .max_attempts(2)
                        .backoff(Backoff::exponential(
                            Duration::from_millis(50).min(Duration::from_secs(30)),
                            2.0,
                            Duration::from_secs(30),
                        ))
                        .build()?,
                )
                .retry_on(open_net::api::http::RetryOn::standard())
                .build()?,
        ))
        .build()?;
    let response = client.request(request).await?;
    check(response.body.as_ref() == b"ok", "retry response mismatch")?;
    shutdown(client).await?;
    let hits = join
        .join()
        .map_err(|_| io::Error::other("retry server thread panicked"))??;
    check(hits == 2, "transport retry count mismatch")?;
    let trace = trace
        .lock()
        .map_err(|_| io::Error::other("trace mutex poisoned"))?;
    check(
        trace.as_slice() == ["retry:before", "retry:after"],
        "middleware was invoked once per transport attempt",
    )
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn middleware_replacement_selects_one_schedule_and_observer_for_each_response_mode(
) -> TestResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct ReplaceRequest {
        policy: HttpRetryPolicy,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl HttpMiddleware for ReplaceRequest {
        async fn call(
            &self,
            mut call: HttpCall,
            next: Next,
        ) -> Result<HttpMiddlewareResponse, NetError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *call.request_mut() = HttpRequest::builder()
                .path("/replacement")
                .retry_setting(RetrySetting::Policy(self.policy.clone()))
                .build()?;
            next.run(call).await
        }
    }

    for streaming in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let peer = tokio::spawn(async move {
            let mut hits = 0_u32;
            for status in [503, 200] {
                let (mut socket, _) =
                    tokio::time::timeout(Duration::from_secs(2), listener.accept()).await??;
                let mut input = Vec::new();
                while !input.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut buffer = [0_u8; 1024];
                    let count =
                        tokio::time::timeout(Duration::from_secs(2), socket.read(&mut buffer))
                            .await??;
                    if count == 0 || input.len() >= 64 * 1024 {
                        return Err(
                            io::Error::other("incomplete or oversized request headers").into()
                        );
                    }
                    input.extend_from_slice(&buffer[..count]);
                }
                check(
                    input.starts_with(b"GET /replacement HTTP/1.1\r\n"),
                    "transport did not use middleware request replacement",
                )?;
                let response = format!("HTTP/1.1 {status} Response\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
                socket.write_all(response.as_bytes()).await?;
                socket.shutdown().await?;
                hits = hits
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("peer hit counter overflow"))?;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(hits)
        });
        let (sent, events) = std::sync::mpsc::channel();
        let replacement = HttpRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(3).build()?)
            .retry_on(open_net::api::http::RetryOn::standard())
            .observer(move |event: &RetryEvent| {
                if sent.send(event.clone()).is_err() {
                    open_net::log_e!(open_net::LogType::Common; "test.middleware", "error", "replacement_observer_receiver_closed");
                }
            })
            .build()?;
        let calls = Arc::new(AtomicUsize::new(0));
        let config = HttpClientConfig::new(format!("http://{address}"))?
            .with_default_retry_policy(HttpRetryPolicy::no_retry())
            .with_middleware(ReplaceRequest {
                policy: replacement,
                calls: Arc::clone(&calls),
            });
        let client = HttpClient::new(config, format!("middleware-effective-policy-{streaming}"))?;
        let operation = async {
            let request = HttpRequest::builder().path("/original").build()?;
            if streaming {
                let mut response = client.stream(request).await?;
                check(
                    response.status == StatusCode::OK && response.attempts == 2,
                    "stream did not use replacement attempt budget",
                )?;
                let mut body = Vec::new();
                while let Some(chunk) = response.next_chunk().await {
                    body.extend_from_slice(&chunk?);
                }
                check(body == b"ok", "stream replacement body mismatch")?;
                Ok(None)
            } else {
                let response = client.request(request).await?;
                check(
                    response.status == StatusCode::OK
                        && response.attempts == 2
                        && response.body.as_ref() == b"ok",
                    "buffered request did not use replacement attempt budget",
                )?;
                let report = response
                    .retry_report()
                    .cloned()
                    .ok_or_else(|| io::Error::other("buffered replacement lost terminal report"))?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Some(report))
            }
        };
        let outcome = tokio::time::timeout(Duration::from_secs(3), operation).await;
        client.shutdown().await?;
        let response_report = match outcome {
            Ok(Ok(report)) => report,
            Ok(Err(error)) => {
                peer.abort();
                return Err(error);
            }
            Err(error) => {
                peer.abort();
                return Err(error.into());
            }
        };
        let hits = peer.await??;
        check(
            hits == 2 && calls.load(Ordering::SeqCst) == 1,
            "middleware replacement ran once per attempt or sent the wrong count",
        )?;
        let observed: Vec<_> = events.try_iter().collect();
        let completed: Vec<_> = observed
            .iter()
            .filter_map(|event| match event {
                RetryEvent::Completed(report) => Some(report),
                _ => None,
            })
            .collect();
        check(
            completed.len() == 1,
            "effective observer did not receive exactly one Completed",
        )?;
        let report = completed
            .first()
            .copied()
            .ok_or_else(|| io::Error::other("effective observer missed terminal report"))?;
        check(
            report.attempts == 2
                && report.retry_count == 1
                && report.transport_sends == 2
                && report.stop_reason == Some(RetryStopReason::Completed),
            "effective observer terminal report mismatch",
        )?;
        check(
            response_report.as_ref().is_none_or(|value| value == report),
            "buffered result and effective observer reports differ",
        )?;
        check(
            matches!(
                observed.as_slice(),
                [
                    RetryEvent::AttemptStarted { attempt: 1 },
                    RetryEvent::RetryScheduled { attempt: 1, .. },
                    RetryEvent::AttemptFinished {
                        attempt: 1,
                        final_attempt: false
                    },
                    RetryEvent::AttemptStarted { attempt: 2 },
                    RetryEvent::AttemptFinished {
                        attempt: 2,
                        final_attempt: true
                    },
                    RetryEvent::Completed(_)
                ]
            ),
            "effective observer lost or duplicated attempt events",
        )?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn automatic_post_safety_reports_the_same_stop_for_buffered_and_streaming() -> TestResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    for streaming in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let hits = Arc::new(AtomicUsize::new(0));
        let received = Arc::clone(&hits);
        let peer = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await?;
                let mut input = Vec::new();
                while !input.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut buffer = [0_u8; 1024];
                    let count =
                        tokio::time::timeout(Duration::from_secs(2), socket.read(&mut buffer))
                            .await??;
                    if count == 0 || input.len() >= 64 * 1024 {
                        return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                            io::Error::other("incomplete or oversized POST headers").into(),
                        );
                    }
                    input.extend_from_slice(&buffer[..count]);
                }
                check(
                    input.starts_with(b"POST /safe HTTP/1.1\r\n"),
                    "safety fixture received a different method",
                )?;
                received.fetch_add(1, Ordering::SeqCst);
                socket.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 5\r\nConnection: close\r\n\r\nretry").await?;
                socket.shutdown().await?;
            }
        });
        let (sent, events) = std::sync::mpsc::channel();
        let policy = HttpRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(3).build()?)
            .retry_on(open_net::api::http::RetryOn::none().with_statuses([StatusCode::SERVICE_UNAVAILABLE]))
            .safety(open_net::api::http::RequestSafety::Automatic)
            .observer(move |event: &RetryEvent| {
                if sent.send(event.clone()).is_err() {
                    open_net::log_e!(open_net::LogType::Common; "test.middleware", "error", "safety_observer_receiver_closed");
                }
            })
            .build()?;
        let client = HttpClient::new(
            HttpClientConfig::new(format!("http://{address}"))?,
            format!("post-safety-report-{streaming}"),
        )?;
        let operation = async {
            let request = HttpRequest::builder()
                .method(Method::POST)
                .path("/safe")
                .retry_setting(RetrySetting::Policy(policy))
                .build()?;
            if streaming {
                let mut response = client.stream(request).await?;
                check(
                    response.status == StatusCode::SERVICE_UNAVAILABLE && response.attempts == 1,
                    "stream replayed an unsafe POST",
                )?;
                let mut body = Vec::new();
                while let Some(chunk) = response.next_chunk().await {
                    body.extend_from_slice(&chunk?);
                }
                check(body == b"retry", "unsafe POST stream lost final body")?;
                Ok(None)
            } else {
                let response = client.request(request).await?;
                check(
                    response.status == StatusCode::SERVICE_UNAVAILABLE
                        && response.attempts == 1
                        && response.body.as_ref() == b"retry",
                    "buffered request replayed an unsafe POST",
                )?;
                let report = response
                    .retry_report()
                    .cloned()
                    .ok_or_else(|| io::Error::other("unsafe POST response lost retry report"))?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Some(report))
            }
        };
        let outcome = tokio::time::timeout(Duration::from_secs(3), operation).await;
        client.shutdown().await?;
        peer.abort();
        let response_report = outcome??;
        check(
            hits.load(Ordering::SeqCst) == 1,
            "unsafe POST reached transport more than once",
        )?;
        let completed: Vec<_> = events
            .try_iter()
            .filter_map(|event| match event {
                RetryEvent::Completed(report) => Some(report),
                _ => None,
            })
            .collect();
        check(
            completed.len() == 1,
            "unsafe POST did not complete observation once",
        )?;
        let report = completed
            .first()
            .ok_or_else(|| io::Error::other("unsafe POST report absent"))?;
        check(
            report.attempts == 1
                && report.retry_count == 0
                && report.transport_sends == 1
                && report.stop_reason == Some(RetryStopReason::UnsafeToReplay),
            "response modes disagree on the replay safety stop reason",
        )?;
        check(
            response_report.as_ref().is_none_or(|value| value == report),
            "unsafe POST observer report differs from buffered response",
        )?;
    }
    Ok(())
}

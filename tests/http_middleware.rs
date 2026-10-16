#![cfg(feature = "http-client-middleware")]

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use open_net::api::http::{
    HttpCall, HttpClient, HttpClientConfig, HttpMiddleware, HttpMiddlewareResponse, HttpRequest,
    HttpResponse, HttpStreamResponse, Next, RetryPolicy,
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
    let config =
        HttpClientConfig::new("http://127.0.0.1:1")?.with_middleware(StreamingShortCircuit);
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
    shutdown(client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_middleware_cannot_replace_the_sdk_request_id() -> TestResult {
    let config = HttpClientConfig::new("http://127.0.0.1:1")?.with_middleware(WrongRequestId);
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
        .retry_policy(RetryPolicy::new(1))
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

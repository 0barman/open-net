#![cfg(feature = "http-client")]

use async_trait::async_trait;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use open_net::api::error::ErrorKind;
use open_net::api::http::{
    HttpClientConfig, HttpRequestId, HttpRequestOptions, HttpRequestTrait, HttpResponseResult,
};
use open_net::api::http::{HttpRequest, HttpStreamResponse};
use open_net::OpenNet;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Notify};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[derive(Debug)]
struct CallbackRecord {
    status: Option<StatusCode>,
    request_id: Option<HttpRequestId>,
    error_kind: Option<ErrorKind>,
}

struct CallbackRequest {
    path: String,
    callback_tx: mpsc::UnboundedSender<CallbackRecord>,
    callback_count: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpRequestTrait for CallbackRequest {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        self.callback_count.fetch_add(1, Ordering::SeqCst);
        let record = match result {
            Ok(response) => CallbackRecord {
                status: Some(response.status()),
                request_id: Some(response.request_id()),
                error_kind: None,
            },
            Err(error) => CallbackRecord {
                status: None,
                request_id: None,
                error_kind: Some(error.kind()),
            },
        };
        let _ = self.callback_tx.send(record);
    }
}

struct BlockingCallbackRequest {
    path: String,
    entered_tx: Option<oneshot::Sender<()>>,
    release: Arc<Notify>,
    callback_count: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpRequestTrait for BlockingCallbackRequest {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    async fn deal_with_response(self: Box<Self>, _result: HttpResponseResult) {
        self.callback_count.fetch_add(1, Ordering::SeqCst);
        if let Some(sender) = self.entered_tx {
            let _ = sender.send(());
        }
        self.release.notified().await;
    }
}

#[derive(Clone)]
struct ResponseSpec {
    status: StatusCode,
    content_type: Option<&'static str>,
    chunks: Vec<Vec<u8>>,
    chunk_delay: Duration,
    entered: Option<Arc<AtomicUsize>>,
    ignore_write_errors: bool,
}

impl ResponseSpec {
    fn complete(status: StatusCode, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            content_type: None,
            chunks: vec![body.into()],
            chunk_delay: Duration::ZERO,
            entered: None,
            ignore_write_errors: false,
        }
    }

    fn delayed(
        status: StatusCode,
        body: impl Into<Vec<u8>>,
        delay: Duration,
        entered: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            status,
            content_type: None,
            chunks: vec![body.into()],
            chunk_delay: delay,
            entered: Some(entered),
            ignore_write_errors: true,
        }
    }

    fn stream(chunks: Vec<Vec<u8>>, content_type: Option<&'static str>) -> Self {
        Self {
            status: StatusCode::OK,
            content_type,
            chunks,
            chunk_delay: Duration::from_millis(30),
            entered: None,
            ignore_write_errors: false,
        }
    }
}

struct TestServer {
    url: String,
    join: Option<JoinHandle<Result<(), String>>>,
}

impl TestServer {
    fn start(responses: Vec<ResponseSpec>) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let join = thread::Builder::new()
            .name("http-lifecycle-test-server".to_owned())
            .spawn(move || serve(listener, responses))?;
        Ok(Self {
            url: format!("http://{address}"),
            join: Some(join),
        })
    }

    fn finish(mut self) -> TestResult {
        let join = self
            .join
            .take()
            .ok_or_else(|| std::io::Error::other("test server already joined"))?;
        join.join()
            .map_err(|_| std::io::Error::other("test server thread failed"))??;
        Ok(())
    }
}

fn serve(listener: TcpListener, responses: Vec<ResponseSpec>) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut workers = Vec::with_capacity(responses.len());
    for response in responses {
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err("timed out waiting for request".to_owned());
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(format!("accept failed: {error}")),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|error| format!("set read timeout failed: {error}"))?;
        read_request(&mut stream).map_err(|error| format!("read request failed: {error}"))?;
        let worker = thread::Builder::new()
            .name("http-lifecycle-response".to_owned())
            .spawn(move || write_response(stream, response))
            .map_err(|error| format!("spawn response writer failed: {error}"))?;
        workers.push(worker);
    }
    for worker in workers {
        worker
            .join()
            .map_err(|_| "response writer thread failed".to_owned())??;
    }
    Ok(())
}

fn write_response(mut stream: TcpStream, response: ResponseSpec) -> Result<(), String> {
    let body_len: usize = response.chunks.iter().map(Vec::len).sum();
    let reason = match response.status.canonical_reason() {
        Some(reason) => reason,
        None => "response",
    };
    let content_type = match response.content_type {
        Some(value) => format!("Content-Type: {value}\r\n"),
        None => String::new(),
    };
    let headers = format!(
            "HTTP/1.1 {} {reason}\r\nContent-Length: {body_len}\r\n{content_type}Connection: close\r\n\r\n",
            response.status.as_u16()
        );
    if let Err(error) = stream.write_all(headers.as_bytes()) {
        if response.ignore_write_errors {
            return Ok(());
        }
        return Err(format!("write headers failed: {error}"));
    }
    if let Some(entered) = response.entered {
        entered.fetch_add(1, Ordering::SeqCst);
    }
    for chunk in response.chunks {
        if !response.chunk_delay.is_zero() {
            thread::sleep(response.chunk_delay);
        }
        if let Err(error) = stream.write_all(&chunk) {
            if response.ignore_write_errors {
                break;
            }
            return Err(format!("write body failed: {error}"));
        }
    }
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())
}

fn read_request(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > 64 * 1024 {
            return Err(std::io::Error::other("request headers too large"));
        }
    }
    Ok(())
}

fn config(url: &str) -> Result<HttpClientConfig, open_net::NetError> {
    HttpClientConfig::new(url)
}

async fn callback_with_timeout(
    receiver: &mut mpsc::UnboundedReceiver<CallbackRecord>,
) -> TestResult<CallbackRecord> {
    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .map_err(|_| std::io::Error::other("callback timed out"))?
        .ok_or_else(|| std::io::Error::other("callback channel closed").into())
}

async fn wait_until_count(count: &AtomicUsize, expected: usize) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(5);
    while count.load(Ordering::SeqCst) < expected {
        if Instant::now() >= deadline {
            return Err(std::io::Error::other("server did not receive request").into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

fn request(path: &str) -> Result<HttpRequest, open_net::NetError> {
    HttpRequest::builder()
        .method(Method::GET)
        .path(path)
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_wait_and_options_deliver_one_callback() -> TestResult {
    let server = TestServer::start(vec![
        ResponseSpec::complete(StatusCode::OK, b"ok".to_vec()),
        ResponseSpec::complete(StatusCode::OK, b"ok".to_vec()),
    ])?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-send-wait", config(&server.url)?)
        .await?;
    let (callback_tx, mut callback_rx) = mpsc::unbounded_channel();
    let callback_count = Arc::new(AtomicUsize::new(0));
    let request_id = client
        .send_wait(CallbackRequest {
            path: "/wait".to_owned(),
            callback_tx: callback_tx.clone(),
            callback_count: Arc::clone(&callback_count),
        })
        .await?;
    let first = callback_with_timeout(&mut callback_rx).await?;
    check(
        first.status == Some(StatusCode::OK),
        "send_wait status mismatch",
    )?;
    check(
        first.request_id == Some(request_id),
        "send_wait request id mismatch",
    )?;
    check(
        callback_count.load(Ordering::SeqCst) == 1,
        "send_wait callback count mismatch",
    )?;

    let second_id = client
        .send_wait_with_options(
            CallbackRequest {
                path: "/options".to_owned(),
                callback_tx,
                callback_count: Arc::clone(&callback_count),
            },
            HttpRequestOptions::new(),
        )
        .await?;
    let second = callback_with_timeout(&mut callback_rx).await?;
    check(
        second.status == Some(StatusCode::OK),
        "options status mismatch",
    )?;
    check(
        second.request_id == Some(second_id),
        "options request id mismatch",
    )?;
    check(
        callback_count.load(Ordering::SeqCst) == 2,
        "options callback count mismatch",
    )?;
    client.shutdown_graceful().await?;
    server.finish()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_and_cancel_all_complete_callbacks_once() -> TestResult {
    let entered = Arc::new(AtomicUsize::new(0));
    let server = TestServer::start(vec![
        ResponseSpec::delayed(
            StatusCode::OK,
            b"slow".to_vec(),
            Duration::from_secs(1),
            Arc::clone(&entered),
        ),
        ResponseSpec::delayed(
            StatusCode::OK,
            b"slow".to_vec(),
            Duration::from_secs(1),
            Arc::clone(&entered),
        ),
    ])?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-cancel", config(&server.url)?)
        .await?;
    let (callback_tx, mut callback_rx) = mpsc::unbounded_channel();
    let callback_count = Arc::new(AtomicUsize::new(0));
    let request_id = client
        .send_wait(CallbackRequest {
            path: "/cancel".to_owned(),
            callback_tx: callback_tx.clone(),
            callback_count: Arc::clone(&callback_count),
        })
        .await?;
    let second_id = client
        .send_wait(CallbackRequest {
            path: "/cancel-all".to_owned(),
            callback_tx,
            callback_count: Arc::clone(&callback_count),
        })
        .await?;
    check(
        second_id != request_id,
        "request identifiers must be unique",
    )?;
    wait_until_count(&entered, 2).await?;
    client.cancel(request_id)?;
    client.cancel_all()?;
    let first = callback_with_timeout(&mut callback_rx).await?;
    let second = callback_with_timeout(&mut callback_rx).await?;
    check(
        first.error_kind == Some(ErrorKind::Cancelled)
            && second.error_kind == Some(ErrorKind::Cancelled),
        "cancel must report cancellation",
    )?;
    check(
        first.request_id.is_none() && second.request_id.is_none(),
        "cancelled callbacks must not expose successful request ids",
    )?;
    check(
        callback_count.load(Ordering::SeqCst) == 2,
        "cancel callbacks must run once each",
    )?;
    client.shutdown_graceful().await?;
    server.finish()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_waits_until_callback_finishes() -> TestResult {
    let server = TestServer::start(vec![ResponseSpec::complete(StatusCode::OK, b"ok".to_vec())])?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-drain", config(&server.url)?)
        .await?;
    let (entered_tx, entered_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let callback_count = Arc::new(AtomicUsize::new(0));
    client
        .send_wait(BlockingCallbackRequest {
            path: "/drain".to_owned(),
            entered_tx: Some(entered_tx),
            release: Arc::clone(&release),
            callback_count: Arc::clone(&callback_count),
        })
        .await?;
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .map_err(|_| std::io::Error::other("callback did not start"))?
        .map_err(|_| std::io::Error::other("callback entry signal dropped"))?;
    let drain_client = client.clone();
    let drain = tokio::spawn(async move { drain_client.drain().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    check(
        !drain.is_finished(),
        "drain returned before callback finished",
    )?;
    release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .map_err(|_| std::io::Error::other("drain timed out"))?
        .map_err(|_| std::io::Error::other("drain task failed"))??;
    check(
        callback_count.load(Ordering::SeqCst) == 1,
        "callback did not finish exactly once",
    )?;
    client.shutdown_graceful().await?;
    server.finish()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_shutdown_and_graceful_shutdown_reject_new_work() -> TestResult {
    let net = OpenNet::new()?;
    let config = HttpClientConfig::new("http://127.0.0.1:1")?;
    let client = net
        .create_http_client_with_config("http-shutdown", config)
        .await?;
    client.request_shutdown();
    client.shutdown_graceful().await?;
    let (callback_tx, _callback_rx) = mpsc::unbounded_channel();
    let callback_count = Arc::new(AtomicUsize::new(0));
    let result = client
        .send_wait(CallbackRequest {
            path: "/closed".to_owned(),
            callback_tx,
            callback_count,
        })
        .await;
    let error = match result {
        Ok(_) => return Err(std::io::Error::other("closed client accepted request").into()),
        Err(error) => error,
    };
    check(
        matches!(error.kind(), ErrorKind::QueueClosed | ErrorKind::Closed),
        "closed client returned unexpected error",
    )
}

async fn collect_stream(mut response: HttpStreamResponse) -> TestResult<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), response.next_chunk())
            .await
            .map_err(|_| std::io::Error::other("stream chunk timed out"))?;
        match chunk {
            Some(Ok(chunk)) => body.extend_from_slice(chunk.as_ref()),
            Some(Err(error)) => return Err(error.into()),
            None => break,
        }
    }
    Ok(body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_delivers_chunks_before_eof() -> TestResult {
    let server = TestServer::start(vec![ResponseSpec::stream(
        vec![b"hello".to_vec(), b" ".to_vec(), b"world".to_vec()],
        Some("application/octet-stream"),
    )])?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-stream", config(&server.url)?)
        .await?;
    let response = client.stream(request("/stream")?).await?;
    check(response.status == StatusCode::OK, "stream status mismatch")?;
    check(
        response
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            == Some("application/octet-stream"),
        "stream content type mismatch",
    )?;
    check(response.attempts == 1, "stream attempts mismatch")?;
    let body = collect_stream(response).await?;
    check(body == b"hello world", "stream body mismatch")?;
    client.shutdown_graceful().await?;
    server.finish()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_exposes_headers_and_streamed_events() -> TestResult {
    let server = TestServer::start(vec![ResponseSpec::stream(
        vec![b"data: one\n\n".to_vec(), b"data: two\n\n".to_vec()],
        Some("text/event-stream"),
    )])?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-sse", config(&server.url)?)
        .await?;
    let mut headers = HeaderMap::new();
    headers.insert("accept", HeaderValue::from_static("text/event-stream"));
    let response = client.sse("/events", headers).await?;
    check(response.status == StatusCode::OK, "SSE status mismatch")?;
    check(
        response
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            == Some("text/event-stream"),
        "SSE content type mismatch",
    )?;
    let body = collect_stream(response).await?;
    check(body == b"data: one\n\ndata: two\n\n", "SSE body mismatch")?;
    client.shutdown_graceful().await?;
    server.finish()
}

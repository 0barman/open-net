#![cfg(feature = "http-client")]

use async_trait::async_trait;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use open_net::api::http::{
    HttpClientConfig, HttpRequestId, HttpRequestTrait, HttpResponseResult, RetryPolicy,
};
use open_net::OpenNet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[derive(Debug)]
struct CallbackRecord {
    status: Option<u16>,
    attempts: Option<u32>,
    request_id: Option<HttpRequestId>,
    thread_name: Option<String>,
    error_kind: Option<String>,
}

struct TestRequest {
    path: String,
    headers: HeaderMap,
    retry_policy: RetryPolicy,
    callback_tx: Sender<CallbackRecord>,
}

struct FlexibleRequest {
    path: String,
    method: Method,
    body: String,
    retry_policy: RetryPolicy,
    callback_tx: Sender<CallbackRecord>,
}

#[async_trait]
impl HttpRequestTrait for FlexibleRequest {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    fn get_method(&self) -> Method {
        self.method.clone()
    }

    fn get_req_data(&self) -> String {
        self.body.clone()
    }

    fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy.clone()
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let record = match result {
            Ok(response) => CallbackRecord {
                status: Some(response.status().as_u16()),
                attempts: Some(response.attempts()),
                request_id: Some(response.request_id()),
                thread_name: thread::current().name().map(str::to_owned),
                error_kind: None,
            },
            Err(error) => CallbackRecord {
                status: None,
                attempts: None,
                request_id: None,
                thread_name: thread::current().name().map(str::to_owned),
                error_kind: Some(format!("{:?}", error.kind())),
            },
        };
        let _ = self.callback_tx.send(record);
    }
}

struct BlockingRequest {
    path: String,
    entered_tx: Sender<()>,
    release_rx: Receiver<()>,
    callback_tx: Sender<CallbackRecord>,
}

#[async_trait]
impl HttpRequestTrait for BlockingRequest {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let _ = self.entered_tx.send(());
        let _ = self.release_rx.recv();
        let record = match result {
            Ok(response) => CallbackRecord {
                status: Some(response.status().as_u16()),
                attempts: Some(response.attempts()),
                request_id: Some(response.request_id()),
                thread_name: thread::current().name().map(str::to_owned),
                error_kind: None,
            },
            Err(error) => CallbackRecord {
                status: None,
                attempts: None,
                request_id: None,
                thread_name: thread::current().name().map(str::to_owned),
                error_kind: Some(format!("{:?}", error.kind())),
            },
        };
        let _ = self.callback_tx.send(record);
    }
}

#[async_trait]
impl HttpRequestTrait for TestRequest {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn headers(&self) -> HeaderMap {
        self.headers.clone()
    }

    fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy.clone()
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let record = match result {
            Ok(response) => CallbackRecord {
                status: Some(response.status().as_u16()),
                attempts: Some(response.attempts()),
                request_id: Some(response.request_id()),
                thread_name: thread::current().name().map(str::to_owned),
                error_kind: None,
            },
            Err(error) => CallbackRecord {
                status: None,
                attempts: None,
                request_id: None,
                thread_name: thread::current().name().map(str::to_owned),
                error_kind: Some(format!("{:?}", error.kind())),
            },
        };
        let _ = self.callback_tx.send(record);
    }
}

struct ServerResponse {
    status: StatusCode,
    body: &'static [u8],
}

struct TestServer {
    url: String,
    join: Option<JoinHandle<Result<Vec<String>, String>>>,
}

impl TestServer {
    fn start(responses: Vec<ServerResponse>) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let join = thread::Builder::new()
            .name("http-test-server".to_owned())
            .spawn(move || serve(listener, responses))?;
        Ok(Self {
            url: format!("http://{address}"),
            join: Some(join),
        })
    }

    fn finish(mut self) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let join = self
            .join
            .take()
            .ok_or_else(|| std::io::Error::other("test server already joined"))?;
        let requests = join
            .join()
            .map_err(|_| std::io::Error::other("test server thread panicked"))??;
        if requests.is_empty() {
            return Err(std::io::Error::other("test server did not receive a request").into());
        }
        Ok(requests)
    }
}

fn serve(listener: TcpListener, responses: Vec<ServerResponse>) -> Result<Vec<String>, String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut requests = Vec::with_capacity(responses.len());
    for response in responses {
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err("timed out waiting for an HTTP request".to_owned());
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(format!("accept failed: {error}")),
            }
        };
        if let Err(error) = stream.set_read_timeout(Some(Duration::from_secs(5))) {
            return Err(format!("set read timeout failed: {error}"));
        }
        let request = read_request(&mut stream).map_err(|error| format!("read failed: {error}"))?;
        requests.push(request);
        let reason = match response.status.canonical_reason() {
            Some(reason) => reason,
            None => "response",
        };
        let header = format!(
            "HTTP/1.1 {} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            response.status.as_u16(),
            response.body.len()
        );
        stream
            .write_all(header.as_bytes())
            .and_then(|_| stream.write_all(response.body))
            .map_err(|error| format!("write failed: {error}"))?;
    }
    Ok(requests)
}

fn read_request(stream: &mut TcpStream) -> Result<String, std::io::Error> {
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
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn callback_channel() -> (Sender<CallbackRecord>, Receiver<CallbackRecord>) {
    mpsc::channel()
}

fn start_truncated_server(
    responses: Vec<(StatusCode, usize, &'static [u8])>,
) -> Result<(String, JoinHandle<Result<(), String>>), std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    let join = thread::Builder::new()
        .name("http-truncated-test-server".to_owned())
        .spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            for (status, advertised_len, body) in responses {
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            if Instant::now() >= deadline {
                                return Err("timed out waiting for a request".to_owned());
                            }
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => return Err(format!("accept failed: {error}")),
                    }
                };
                let _ = read_request(&mut stream);
                let reason = match status.canonical_reason() {
                    Some(value) => value,
                    None => "response",
                };
                let header = format!(
                    "HTTP/1.1 {} {reason}\r\nContent-Length: {advertised_len}\r\nConnection: close\r\n\r\n",
                    status.as_u16()
                );
                if let Err(error) = stream.write_all(header.as_bytes()) {
                    return Err(format!("header write failed: {error}"));
                }
                if let Err(error) = stream.write_all(body) {
                    return Err(format!("body write failed: {error}"));
                }
                let _ = stream.shutdown(std::net::Shutdown::Both);
            }
            Ok(())
        })?;
    Ok((format!("http://{address}"), join))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sends_request_on_isolated_lanes_and_merges_common_headers() -> TestResult {
    let server = TestServer::start(vec![ServerResponse {
        status: StatusCode::OK,
        body: br#"{"ok":true}"#,
    }])?;
    let (callback_tx, callback_rx) = callback_channel();
    let mut common_headers = HeaderMap::new();
    common_headers.insert("x-company", HeaderValue::from_static("company-value"));
    let config = HttpClientConfig::new(&server.url)?.with_common_headers(common_headers)?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-isolated", config)
        .await?;
    let caller_thread = thread::current().id();
    let request = TestRequest {
        path: "/health".to_owned(),
        headers: HeaderMap::new(),
        retry_policy: RetryPolicy::no_retry(),
        callback_tx,
    };
    let request_id = client.send(request)?;
    let callback = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| std::io::Error::other(format!("callback not received: {error}")))?;
    check(callback.status == Some(200), "unexpected response status")?;
    check(callback.attempts == Some(1), "unexpected attempt count")?;
    check(
        callback.request_id == Some(request_id),
        "request id was not preserved",
    )?;
    check(
        callback.thread_name.as_deref() == Some("http-isolated-callback"),
        "callback did not run on its dedicated thread",
    )?;
    check(
        thread::current().id() == caller_thread,
        "caller thread changed while sending",
    )?;
    net.destroy_http_client("http-isolated").await?;
    let requests = server.finish()?;
    let common_header_present = match requests.first() {
        Some(request) => request.contains("x-company: company-value"),
        None => false,
    };
    check(common_header_present, "common header was not sent")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_retryable_statuses_and_calls_back_once_after_final_attempt() -> TestResult {
    let server = TestServer::start(vec![
        ServerResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: b"retry-1",
        },
        ServerResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: b"retry-2",
        },
        ServerResponse {
            status: StatusCode::OK,
            body: b"done",
        },
    ])?;
    let (callback_tx, callback_rx) = callback_channel();
    let config = HttpClientConfig::new(&server.url)?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-retry", config)
        .await?;
    let request = TestRequest {
        path: "/retry".to_owned(),
        headers: HeaderMap::new(),
        retry_policy: RetryPolicy::new(2),
        callback_tx,
    };
    client.send(request)?;
    let callback = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| std::io::Error::other(format!("callback not received: {error}")))?;
    check(
        callback.status == Some(200),
        "final response was not delivered",
    )?;
    check(
        callback.attempts == Some(3),
        "retry attempts were not recorded",
    )?;
    check(
        callback.error_kind.is_none(),
        "successful retry unexpectedly returned an error",
    )?;
    check(
        callback_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "callback ran before the final retry",
    )?;
    net.destroy_http_client("http-retry").await?;
    let requests = server.finish()?;
    check(
        requests.len() == 3,
        "retry policy did not create three attempts",
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_invalid_thread_names_and_releases_http_registry_entries() -> TestResult {
    let net = OpenNet::new()?;
    let invalid_config = HttpClientConfig::new("http://127.0.0.1:1")?;
    let invalid = net
        .create_http_client_with_config("bad\0name", invalid_config)
        .await;
    check(invalid.is_err(), "NUL thread names must be rejected")?;

    let config = HttpClientConfig::new("http://127.0.0.1:1")?;
    let client = net
        .create_http_client_with_config("http-registry", config)
        .await?;
    check(
        net.get_http_client("http-registry").is_ok(),
        "ready HTTP client was not registered",
    )?;
    net.destroy_http_client("http-registry").await?;
    check(
        net.get_http_client("http-registry").is_err(),
        "destroyed HTTP client remained registered",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enforces_response_body_limit_without_panicking() -> TestResult {
    let server = TestServer::start(vec![ServerResponse {
        status: StatusCode::OK,
        body: b"0123456789",
    }])?;
    let (callback_tx, callback_rx) = callback_channel();
    let config = HttpClientConfig::new(&server.url)?.with_max_response_bytes(4)?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-body-limit", config)
        .await?;
    client.send(TestRequest {
        path: "/large".to_owned(),
        headers: HeaderMap::new(),
        retry_policy: RetryPolicy::no_retry(),
        callback_tx,
    })?;
    let callback = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| std::io::Error::other(format!("body-limit callback missing: {error}")))?;
    check(
        callback.status.is_none(),
        "oversized response was reported as success",
    )?;
    check(
        callback.error_kind.as_deref() == Some("ItemTooLarge"),
        "oversized response did not return ItemTooLarge",
    )?;
    net.destroy_http_client("http-body-limit").await?;
    let _ = server.finish()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_response_body_disconnect_before_callback() -> TestResult {
    let (url, join) = start_truncated_server(vec![
        (StatusCode::OK, 8, b"bad"),
        (StatusCode::OK, 2, b"ok"),
    ])?;
    let (callback_tx, callback_rx) = callback_channel();
    let config = HttpClientConfig::new(&url)?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-body-retry", config)
        .await?;
    client.send(TestRequest {
        path: "/disconnect".to_owned(),
        headers: HeaderMap::new(),
        retry_policy: RetryPolicy::new(1),
        callback_tx,
    })?;
    let callback = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| std::io::Error::other(format!("disconnect callback missing: {error}")))?;
    if callback.status != Some(200) {
        return Err(std::io::Error::other(format!(
            "body disconnect was not retried: status={:?} error={:?} attempts={:?}",
            callback.status, callback.error_kind, callback.attempts
        ))
        .into());
    }
    check(
        callback.attempts == Some(2),
        "body disconnect retry count was incorrect",
    )?;
    check(
        callback_rx.try_recv().is_err(),
        "body disconnect callback ran more than once",
    )?;
    net.destroy_http_client("http-body-retry").await?;
    join.join()
        .map_err(|_| std::io::Error::other("truncated server thread failed"))??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_retryable_status_calls_back_once_after_budget_exhaustion() -> TestResult {
    let server = TestServer::start(vec![
        ServerResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: b"still down",
        },
        ServerResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: b"still down",
        },
    ])?;
    let (callback_tx, callback_rx) = callback_channel();
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-final-503", HttpClientConfig::new(&server.url)?)
        .await?;
    client.send(TestRequest {
        path: "/final".to_owned(),
        headers: HeaderMap::new(),
        retry_policy: RetryPolicy::new(1),
        callback_tx,
    })?;
    let callback = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| std::io::Error::other(format!("503 callback missing: {error}")))?;
    check(
        callback.status == Some(503),
        "final 503 status was not delivered",
    )?;
    check(
        callback.attempts == Some(2),
        "final 503 attempt count was incorrect",
    )?;
    check(
        callback_rx.try_recv().is_err(),
        "503 callback ran more than once",
    )?;
    net.destroy_http_client("http-final-503").await?;
    let _ = server.finish()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_retryable_400_calls_back_once() -> TestResult {
    let server = TestServer::start(vec![ServerResponse {
        status: StatusCode::BAD_REQUEST,
        body: b"bad request",
    }])?;
    let (callback_tx, callback_rx) = callback_channel();
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-final-400", HttpClientConfig::new(&server.url)?)
        .await?;
    client.send(TestRequest {
        path: "/bad".to_owned(),
        headers: HeaderMap::new(),
        retry_policy: RetryPolicy::new(3),
        callback_tx,
    })?;
    let callback = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| std::io::Error::other(format!("400 callback missing: {error}")))?;
    check(callback.status == Some(400), "400 status was not delivered")?;
    check(callback.attempts == Some(1), "400 was retried unexpectedly")?;
    check(
        callback_rx.try_recv().is_err(),
        "400 callback ran more than once",
    )?;
    net.destroy_http_client("http-final-400").await?;
    let _ = server.finish()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_no_retry_overrides_client_default() -> TestResult {
    let server = TestServer::start(vec![ServerResponse {
        status: StatusCode::SERVICE_UNAVAILABLE,
        body: b"no retry",
    }])?;
    let (callback_tx, callback_rx) = callback_channel();
    let config = HttpClientConfig::new(&server.url)?.with_default_retry_policy(RetryPolicy::new(3));
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-explicit-no-retry", config)
        .await?;
    let request = TestRequest {
        path: "/no-retry".to_owned(),
        headers: HeaderMap::new(),
        retry_policy: RetryPolicy::no_retry(),
        callback_tx,
    };
    let options =
        open_net::api::http::HttpRequestOptions::new().with_retry_policy(RetryPolicy::no_retry());
    client.send_with_options(request, options)?;
    let callback = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| std::io::Error::other(format!("no-retry callback missing: {error}")))?;
    check(
        callback.status == Some(503),
        "explicit no-retry changed status",
    )?;
    check(
        callback.attempts == Some(1),
        "explicit no-retry was ignored",
    )?;
    net.destroy_http_client("http-explicit-no-retry").await?;
    let _ = server.finish()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_retry_requires_explicit_non_idempotent_opt_in() -> TestResult {
    let server = TestServer::start(vec![ServerResponse {
        status: StatusCode::SERVICE_UNAVAILABLE,
        body: b"post",
    }])?;
    let (callback_tx, callback_rx) = callback_channel();
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-post-protection", HttpClientConfig::new(&server.url)?)
        .await?;
    client.send(FlexibleRequest {
        path: "/post".to_owned(),
        method: Method::POST,
        body: "payload".to_owned(),
        retry_policy: RetryPolicy::new(3),
        callback_tx,
    })?;
    let callback = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| std::io::Error::other(format!("POST callback missing: {error}")))?;
    check(
        callback.status == Some(503),
        "POST response was not delivered",
    )?;
    check(callback.attempts == Some(1), "POST retried without opt in")?;
    net.destroy_http_client("http-post-protection").await?;
    let _ = server.finish()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_callback_isolated_from_worker_lanes() -> TestResult {
    let server = TestServer::start(vec![
        ServerResponse {
            status: StatusCode::OK,
            body: b"first",
        },
        ServerResponse {
            status: StatusCode::OK,
            body: b"second",
        },
    ])?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config(
            "http-callback-isolation",
            HttpClientConfig::new(&server.url)?,
        )
        .await?;
    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let (callback_tx, callback_rx) = callback_channel();
    client.send(BlockingRequest {
        path: "/first".to_owned(),
        entered_tx,
        release_rx,
        callback_tx: callback_tx.clone(),
    })?;
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| {
            std::io::Error::other(format!("blocking callback did not start: {error}"))
        })?;
    client.send(TestRequest {
        path: "/second".to_owned(),
        headers: HeaderMap::new(),
        retry_policy: RetryPolicy::no_retry(),
        callback_tx,
    })?;
    let _ = release_tx.send(());
    let first = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| std::io::Error::other(format!("first callback missing: {error}")))?;
    let second = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| std::io::Error::other(format!("second callback missing: {error}")))?;
    if first.status != Some(200) || second.status != Some(200) {
        return Err(std::io::Error::other(format!(
            "both callbacks did not complete: first={:?} second={:?}",
            first, second
        ))
        .into());
    }
    net.destroy_http_client("http-callback-isolation").await?;
    let _ = server.finish()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_rejects_new_requests() -> TestResult {
    let (callback_tx, _callback_rx) = callback_channel();
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config(
            "http-shutdown-reject",
            HttpClientConfig::new("http://127.0.0.1:1")?,
        )
        .await?;
    net.destroy_http_client("http-shutdown-reject").await?;
    let result = client.send(TestRequest {
        path: "/closed".to_owned(),
        headers: HeaderMap::new(),
        retry_policy: RetryPolicy::no_retry(),
        callback_tx,
    });
    check(result.is_err(), "send succeeded after shutdown")?;
    Ok(())
}

#[cfg(feature = "ws-client")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_and_websocket_registries_allow_the_same_name() -> TestResult {
    let net = OpenNet::new()?;
    let http = net
        .create_http_client_with_config(
            "shared-http-ws-name",
            HttpClientConfig::new("http://127.0.0.1:1")?,
        )
        .await?;
    let ws = net.create_ws_client("shared-http-ws-name").await?;
    check(
        net.get_http_client("shared-http-ws-name").is_ok(),
        "HTTP registry did not retain shared name",
    )?;
    check(
        !ws.is_shutdown(),
        "WebSocket worker was unexpectedly shut down",
    )?;
    net.destroy_http_client("shared-http-ws-name").await?;
    net.destroy_ws_client("shared-http-ws-name").await?;
    http.shutdown().await?;
    Ok(())
}

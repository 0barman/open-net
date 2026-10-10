#![cfg(feature = "http-client")]

use async_trait::async_trait;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
#[cfg(feature = "http-client-auth")]
use open_net::api::http::{
    AccessToken, AuthRequestContext, AuthResponseContext, BearerAuth, BearerTokenSource, HttpAuth,
    RefreshReason,
};
use open_net::api::http::{
    HttpClientConfig, HttpRequestId, HttpRequestMethod, HttpRequestTrait, HttpResponseResult,
    RetryPolicy,
};
use open_net::OpenNet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
#[cfg(feature = "http-client-auth")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[cfg(feature = "http-client-auth")]
struct RotatingTokenSource {
    refreshes: AtomicUsize,
}

#[cfg(feature = "http-client-auth")]
#[async_trait]
impl BearerTokenSource for RotatingTokenSource {
    async fn load(&self) -> open_net::Result<AccessToken> {
        AccessToken::new("initial-token", None)
    }

    async fn refresh(&self, reason: RefreshReason) -> open_net::Result<AccessToken> {
        if reason != RefreshReason::Unauthorized {
            return Err(open_net::NetError::from(
                open_net::error::ErrorKind::ProviderFailed,
            ));
        }
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        AccessToken::new("refreshed-token", None)
    }
}

#[cfg(feature = "http-client-auth")]
struct SlowRotatingTokenSource {
    refreshes: AtomicUsize,
}

#[cfg(feature = "http-client-auth")]
struct ErrorAuth;

#[cfg(feature = "http-client-auth")]
#[async_trait]
impl HttpAuth for ErrorAuth {
    async fn apply(&self, _request: &mut AuthRequestContext<'_>) -> open_net::Result<()> {
        Err(open_net::NetError::provider(std::io::Error::other(
            "provider-secret-must-not-escape",
        )))
    }

    async fn on_unauthorized(
        &self,
        _response: &AuthResponseContext<'_>,
    ) -> open_net::Result<open_net::api::http::AuthAction> {
        Ok(open_net::api::http::AuthAction::DoNotRetry)
    }
}

#[cfg(feature = "http-client-auth")]
#[async_trait]
impl BearerTokenSource for SlowRotatingTokenSource {
    async fn load(&self) -> open_net::Result<AccessToken> {
        AccessToken::new("initial-token", None)
    }

    async fn refresh(&self, _reason: RefreshReason) -> open_net::Result<AccessToken> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(30)).await;
        AccessToken::new("refreshed-token", None)
    }
}

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[cfg(feature = "http-client-auth")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthorized_refreshes_once_and_reapplies_current_token() -> TestResult {
    let server = TestServer::start(vec![
        ServerResponse {
            status: StatusCode::UNAUTHORIZED,
            body: b"unauthorized",
        },
        ServerResponse {
            status: StatusCode::OK,
            body: b"ok",
        },
    ])?;
    let source = std::sync::Arc::new(RotatingTokenSource {
        refreshes: AtomicUsize::new(0),
    });
    let config = HttpClientConfig::new(&server.url)?.with_auth(BearerAuth::new(source.clone()));
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-auth-refresh", config)
        .await?;
    let response = client.get("/auth").await?;
    check(
        response.status() == StatusCode::OK,
        "refresh response was not successful",
    )?;
    check(response.attempts() == 2, "401 did not produce one retry")?;
    check(
        source.refreshes.load(Ordering::SeqCst) == 1,
        "refresh source was called more than once",
    )?;
    net.destroy_http_client("http-auth-refresh").await?;
    let requests = server.finish()?;
    check(requests.len() == 2, "unexpected number of requests")?;
    let first = requests
        .first()
        .ok_or_else(|| std::io::Error::other("first request missing"))?;
    let second = requests
        .get(1)
        .ok_or_else(|| std::io::Error::other("second request missing"))?;
    check(
        first.contains("authorization: Bearer initial-token"),
        "initial token missing",
    )?;
    check(
        second.contains("authorization: Bearer refreshed-token"),
        "refreshed token missing",
    )
}

#[cfg(feature = "http-client-auth")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_provider_runs_for_each_transient_retry_attempt() -> TestResult {
    let server = TestServer::start(vec![
        ServerResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: b"retry",
        },
        ServerResponse {
            status: StatusCode::OK,
            body: b"ok",
        },
    ])?;
    let auth = BearerAuth::from_static("static-token")?;
    let config = HttpClientConfig::new(&server.url)?
        .with_default_retry_policy(RetryPolicy::new(1))
        .with_auth(auth);
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-auth-retry", config)
        .await?;
    let response = client.get("/retry-auth").await?;
    check(
        response.status() == StatusCode::OK,
        "retry response was not successful",
    )?;
    check(response.attempts() == 2, "transient retry did not run")?;
    net.destroy_http_client("http-auth-retry").await?;
    let requests = server.finish()?;
    check(requests.len() == 2, "unexpected number of requests")?;
    check(
        requests
            .first()
            .is_some_and(|request| request.contains("authorization: Bearer static-token")),
        "first attempt did not include authentication",
    )?;
    check(
        requests
            .get(1)
            .is_some_and(|request| request.contains("authorization: Bearer static-token")),
        "retry attempt did not include authentication",
    )
}

#[cfg(feature = "http-client-auth")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_unauthorized_requests_share_one_refresh() -> TestResult {
    let mut responses = Vec::new();
    for _ in 0..3 {
        responses.push(ServerResponse {
            status: StatusCode::UNAUTHORIZED,
            body: b"unauthorized",
        });
    }
    for _ in 0..3 {
        responses.push(ServerResponse {
            status: StatusCode::OK,
            body: b"ok",
        });
    }
    let server = TestServer::start(responses)?;
    let source = std::sync::Arc::new(SlowRotatingTokenSource {
        refreshes: AtomicUsize::new(0),
    });
    let config = HttpClientConfig::new(&server.url)?.with_auth(BearerAuth::new(source.clone()));
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-auth-single-flight", config)
        .await?;
    let (first, second, third) =
        tokio::join!(client.get("/one"), client.get("/two"), client.get("/three"),);
    for response in [first?, second?, third?] {
        check(
            response.status() == StatusCode::OK,
            "concurrent refresh failed",
        )?;
        check(
            response.attempts() == 2,
            "unexpected concurrent attempt count",
        )?;
    }
    check(
        source.refreshes.load(Ordering::SeqCst) == 1,
        "concurrent refresh was not single-flight",
    )?;
    net.destroy_http_client("http-auth-single-flight").await?;
    let requests = server.finish()?;
    check(requests.len() == 6, "unexpected concurrent request count")
}

#[cfg(feature = "http-client-auth")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_errors_are_classified_without_retaining_secret_sources() -> TestResult {
    let server = TestServer::start(vec![ServerResponse {
        status: StatusCode::OK,
        body: b"unused",
    }])?;
    let config = HttpClientConfig::new(&server.url)?.with_auth(ErrorAuth);
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-auth-error", config)
        .await?;
    let error = client
        .get("/provider-error")
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("provider error was not returned"))?;
    check(
        error.kind() == open_net::error::ErrorKind::ProviderFailed,
        "provider error classification changed",
    )?;
    check(
        !format!("{error} {error:?}").contains("provider-secret"),
        "provider source leaked into error formatting",
    )?;
    net.destroy_http_client("http-auth-error").await?;
    Ok(())
}

#[derive(Debug)]
struct CallbackRecord {
    status: Option<u16>,
    attempts: Option<u32>,
    body: Option<Vec<u8>>,
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

    fn get_method(&self) -> HttpRequestMethod {
        self.method.clone().into()
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
                body: Some(response.body().to_vec()),
                request_id: Some(response.request_id()),
                thread_name: thread::current().name().map(str::to_owned),
                error_kind: None,
            },
            Err(error) => CallbackRecord {
                status: None,
                attempts: None,
                body: None,
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

    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::GET
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let _ = self.entered_tx.send(());
        let _ = self.release_rx.recv();
        let record = match result {
            Ok(response) => CallbackRecord {
                status: Some(response.status().as_u16()),
                attempts: Some(response.attempts()),
                body: Some(response.body().to_vec()),
                request_id: Some(response.request_id()),
                thread_name: thread::current().name().map(str::to_owned),
                error_kind: None,
            },
            Err(error) => CallbackRecord {
                status: None,
                attempts: None,
                body: None,
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

    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::GET
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
                body: Some(response.body().to_vec()),
                request_id: Some(response.request_id()),
                thread_name: thread::current().name().map(str::to_owned),
                error_kind: None,
            },
            Err(error) => CallbackRecord {
                status: None,
                attempts: None,
                body: None,
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
    stop: Arc<AtomicBool>,
}

impl TestServer {
    fn start(responses: Vec<ServerResponse>) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let join = thread::Builder::new()
            .name("http-test-server".to_owned())
            .spawn(move || serve(listener, responses, thread_stop))?;
        Ok(Self {
            url: format!("http://{address}"),
            join: Some(join),
            stop,
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

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            // Acceptance polls the stop flag; socket reads/writes have deadlines.
            // Assertion failures must not detach a listener or hide live workers.
            let _ = join.join();
        }
    }
}

fn serve(
    listener: TcpListener,
    responses: Vec<ServerResponse>,
    stop: Arc<AtomicBool>,
) -> Result<Vec<String>, String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut requests = Vec::with_capacity(responses.len());
    for (response_index, response) in responses.into_iter().enumerate() {
        let mut stream = loop {
            if stop.load(Ordering::Acquire) {
                return Err(format!("fixture stopped before response {response_index}"));
            }
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
        let request = read_accepted_request(&mut stream)
            .map_err(|error| format!("response {response_index} read failed: {error}"))?;
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
        stream.write_all(header.as_bytes()).map_err(|error| {
            format!(
                "response {response_index} status {} header write failed: {error}",
                response.status
            )
        })?;
        stream.write_all(response.body).map_err(|error| {
            format!(
                "response {response_index} status {} body write failed: {error}",
                response.status
            )
        })?;
    }
    Ok(requests)
}

fn read_accepted_request(stream: &mut TcpStream) -> Result<String, std::io::Error> {
    // Accepted sockets may inherit O_NONBLOCK from the polling listener (macOS).
    // A socket timeout alone does not turn those immediate WouldBlock reads into
    // blocking reads. Configure the mode explicitly on every accepted socket.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    read_request(stream)
}

#[test]
fn accepted_nonblocking_socket_waits_for_delayed_complete_request() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut client = TcpStream::connect(listener.local_addr()?)?;
    let (mut accepted, _) = listener.accept()?;
    // Reproduce the mode inherited from the nonblocking listener on macOS on
    // every platform, without assuming that TCP connect already sent HTTP bytes.
    accepted.set_nonblocking(true)?;
    let mut probe = [0_u8; 1];
    check(
        accepted.peek(&mut probe).unwrap_err().kind() == std::io::ErrorKind::WouldBlock,
        "fixture must begin with an accepted connection and no request bytes",
    )?;
    let request = "GET /delayed-headers HTTP/1.1\r\nHost: localhost\r\n\r\n";
    let writer = thread::spawn(move || {
        // This delay is deliberate peer input. Success requires reading the
        // complete request, never a timer expiring or a task staying pending.
        thread::sleep(Duration::from_millis(100));
        client.write_all(request.as_bytes())
    });
    let received = read_accepted_request(&mut accepted);
    let sent = writer
        .join()
        .map_err(|_| std::io::Error::other("delayed request writer panicked"))?;
    sent?;
    let received = received?;
    check(
        received == request,
        "delayed HTTP request was not read in full",
    )
}

#[test]
fn request_headers_reject_empty_and_truncated_eof() -> TestResult {
    for incomplete in [
        "",
        "GET /partial HTTP/1.1\r\n",
        "GET /partial HTTP/1.1\r\nHost: localhost\r\n",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let mut client = TcpStream::connect(listener.local_addr()?)?;
        let (mut accepted, _) = listener.accept()?;
        client.write_all(incomplete.as_bytes())?;
        client.shutdown(std::net::Shutdown::Write)?;
        let error = read_accepted_request(&mut accepted)
            .expect_err("EOF before the header terminator is not an HTTP request");
        check(
            error.kind() == std::io::ErrorKind::UnexpectedEof,
            "incomplete request must be classified as UnexpectedEof",
        )?;
    }
    Ok(())
}

fn read_request(stream: &mut TcpStream) -> Result<String, std::io::Error> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before the complete HTTP request headers",
            ));
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
                read_accepted_request(&mut stream)
                    .map_err(|error| format!("truncated fixture request read failed: {error}"))?;
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
        callback.request_id == Some(request_id.id()),
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
    let callback = callback_rx.recv_timeout(Duration::from_secs(5));
    // Preserve the callback and fixture diagnostics before asserting, including
    // on failure. Cleanup must not be skipped by an early status assertion.
    let cancellation = callback.is_err().then(|| client.cancel_all());
    let shutdown = tokio::time::timeout(
        Duration::from_secs(5),
        net.destroy_http_client("http-retry"),
    )
    .await;
    let server_result = server.finish();
    let successful_callback = callback.as_ref().is_ok_and(|callback| {
        callback.status == Some(200)
            && callback.attempts == Some(3)
            && callback.body.as_deref() == Some(b"done".as_slice())
            && callback.error_kind.is_none()
    });
    if !successful_callback || !matches!(shutdown, Ok(Ok(()))) || server_result.is_err() {
        return Err(std::io::Error::other(format!(
            "final response was not delivered: callback={callback:?}; cancellation={cancellation:?}; shutdown={shutdown:?}; server={server_result:?}"
        ))
        .into());
    }
    // Worker shutdown has joined the callback lane, so no timing window is
    // necessary to prove that there cannot be a later extra callback.
    check(
        callback_rx.try_recv().is_err(),
        "callback ran before the final retry",
    )?;
    let requests = server_result?;
    check(
        requests.len() == 3
            && requests.iter().all(|request| {
                request.starts_with("GET /retry HTTP/1.1\r\n") && request.ends_with("\r\n\r\n")
            }),
        "retry policy did not create exactly three complete GET /retry requests",
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
    client
        .send(BlockingRequest {
            path: "/first".to_owned(),
            entered_tx,
            release_rx,
            callback_tx: callback_tx.clone(),
        })
        .map_err(open_net::EnqueueError::into_error)?;
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

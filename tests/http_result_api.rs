#![cfg(feature = "http-client")]

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use open_net::api::http::{HttpClient, HttpClientConfig, HttpRequest, HttpStatusPolicy};
use open_net::error::ErrorKind;
use open_net::{NetError, OpenNet};
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn check(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message.into()).into())
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(TEST_TIMEOUT, future)
        .await
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
}

struct StubServer {
    address: SocketAddr,
    task: JoinHandle<Result<(), String>>,
}

impl StubServer {
    async fn finish(mut self) -> TestResult {
        let result = bounded(&mut self.task).await??;
        result.map_err(std::io::Error::other)?;
        Ok(())
    }
}

impl Drop for StubServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum Reply {
    Fixed(StatusCode, &'static str, Vec<u8>),
    EchoRequest,
    EchoBody,
}

async fn read_request(stream: &mut TcpStream) -> Result<(Vec<u8>, usize), String> {
    let mut bytes = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    let header_end = loop {
        let count = stream
            .read(&mut chunk)
            .await
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("client closed before sending request headers".to_owned());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        if bytes.len() > 64 * 1024 {
            return Err("request headers exceeded test limit".to_owned());
        }
    };

    let head = String::from_utf8_lossy(&bytes[..header_end]);
    let mut content_length = 0_usize;
    for line in head.lines() {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value
                    .trim()
                    .parse::<usize>()
                    .map_err(|error| error.to_string())?;
            }
        }
    }
    if content_length > 64 * 1024 {
        return Err("request body exceeded test limit".to_owned());
    }
    let required = header_end.saturating_add(content_length);
    while bytes.len() < required {
        let count = stream
            .read(&mut chunk)
            .await
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("client closed before sending request body".to_owned());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    Ok((bytes, header_end))
}

fn response_bytes(status: StatusCode, content_type: &str, body: &[u8]) -> Vec<u8> {
    let reason = match status.canonical_reason() {
        Some(value) => value,
        None => "Unknown",
    };
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nX-Test-Response: retained\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status.as_u16(), reason, content_type, body.len()
    );
    let mut output = head.into_bytes();
    output.extend_from_slice(body);
    output
}

async fn spawn_server(replies: Vec<Reply>) -> TestResult<StubServer> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        for reply in replies {
            let (mut stream, _) = tokio::time::timeout(TEST_TIMEOUT, listener.accept())
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            let (request, header_end) =
                tokio::time::timeout(TEST_TIMEOUT, read_request(&mut stream))
                    .await
                    .map_err(|error| error.to_string())??;
            let response = match reply {
                Reply::Fixed(status, content_type, body) => {
                    response_bytes(status, content_type, &body)
                }
                Reply::EchoRequest => {
                    response_bytes(StatusCode::OK, "application/octet-stream", &request)
                }
                Reply::EchoBody => response_bytes(
                    StatusCode::OK,
                    "application/octet-stream",
                    &request[header_end..],
                ),
            };
            tokio::time::timeout(TEST_TIMEOUT, stream.write_all(&response))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    });
    Ok(StubServer { address, task })
}

async fn make_client(server: &StubServer, name: &str) -> TestResult<(OpenNet, HttpClient)> {
    let config = HttpClientConfig::new(format!("http://{}", server.address))?
        .with_timeout(Duration::from_secs(2))?;
    make_client_with_config(name, config).await
}

async fn make_client_with_config(
    name: &str,
    config: HttpClientConfig,
) -> TestResult<(OpenNet, HttpClient)> {
    let net = OpenNet::new()?;
    let client = bounded(net.create_http_client_with_config(name, config)).await??;
    Ok((net, client))
}

async fn destroy_client(net: OpenNet, name: &str) -> TestResult {
    bounded(net.destroy_http_client(name)).await??;
    Ok(())
}

fn status_error(result: Result<(), NetError>) -> TestResult<NetError> {
    match result {
        Ok(()) => Err(std::io::Error::other("status policy accepted a mismatched status").into()),
        Err(error) => Ok(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_preserves_binary_body_headers_and_http_status() -> TestResult {
    let body = [0_u8, 1, 2, 0xff, 0x80, 3];
    let server = spawn_server(vec![Reply::Fixed(
        StatusCode::CREATED,
        "application/octet-stream",
        body.to_vec(),
    )])
    .await?;
    let (net, client) = make_client(&server, "result-binary").await?;

    let response = bounded(client.get_bytes("/binary")).await??;
    check(
        response.status == StatusCode::CREATED,
        "HTTP status was not preserved",
    )?;
    check(
        response.body.as_ref() == body,
        "binary response body changed",
    )?;
    check(
        response.content_type() == Some("application/octet-stream"),
        "content type was not exposed",
    )?;
    check(
        response.headers.get("x-test-response") == Some(&HeaderValue::from_static("retained")),
        "response headers were not retained",
    )?;
    check(
        response.attempts == 1,
        "initial request attempt count is incorrect",
    )?;
    response.check_status(HttpStatusPolicy::Any)?;
    response.check_status(HttpStatusPolicy::Success2xx)?;
    let error = status_error(response.check_status(HttpStatusPolicy::Exact(StatusCode::OK)))?;
    check(
        error.kind() == ErrorKind::HttpStatus,
        "status policy did not produce HttpStatus error",
    )?;
    check(
        error.http_status() == Some(StatusCode::CREATED),
        "HTTP status was not retained on NetError",
    )?;

    destroy_client(net, "result-binary").await?;
    server.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_get_and_post_forward_paths_headers_and_body() -> TestResult {
    let server = spawn_server(vec![Reply::EchoRequest, Reply::EchoRequest]).await?;
    let (net, client) = make_client(&server, "result-get-post").await?;
    let mut headers = HeaderMap::new();
    headers.insert("x-test-header", HeaderValue::from_static("header-value"));

    let response = bounded(client.get_with_headers("/get?value=7", headers.clone())).await??;
    let text = response.text()?;
    check(
        text.contains("GET /get?value=7 HTTP/1.1"),
        "GET path or method was not forwarded",
    )?;
    check(
        text.to_ascii_lowercase()
            .contains("x-test-header: header-value"),
        "GET request header was not forwarded",
    )?;

    let post = bounded(client.post_with_headers("/post", headers, b"request-body")).await??;
    let text = post.text()?;
    check(
        text.contains("POST /post HTTP/1.1"),
        "POST path or method was not forwarded",
    )?;
    check(
        text.to_ascii_lowercase()
            .contains("x-test-header: header-value"),
        "POST request header was not forwarded",
    )?;
    check(
        text.ends_with("\r\n\r\nrequest-body"),
        "POST request body was not forwarded",
    )?;

    destroy_client(net, "result-get-post").await?;
    server.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_get_and_post_shortcuts_keep_failure_status_and_body() -> TestResult {
    let server = spawn_server(vec![
        Reply::Fixed(StatusCode::NOT_FOUND, "text/plain", b"missing".to_vec()),
        Reply::Fixed(
            StatusCode::UNAUTHORIZED,
            "application/json",
            br#"{"error":"unauthorized"}"#.to_vec(),
        ),
    ])
    .await?;
    let (net, client) = make_client(&server, "result-shortcuts").await?;

    let get = bounded(client.get("/missing")).await??;
    check(
        get.status == StatusCode::NOT_FOUND && get.body.as_ref() == b"missing",
        "GET lost HTTP failure status or body",
    )?;
    get.check_status(HttpStatusPolicy::Any)?;
    let error = status_error(get.check_status(HttpStatusPolicy::Success2xx))?;
    check(
        error.http_status() == Some(StatusCode::NOT_FOUND),
        "status error lost original status",
    )?;
    let post = bounded(client.post("/login", b"{}")).await??;
    check(
        post.status == StatusCode::UNAUTHORIZED,
        "POST changed HTTP failure into transport failure",
    )?;
    check(
        post.body.as_ref() == br#"{"error":"unauthorized"}"#,
        "POST discarded error response body",
    )?;

    destroy_client(net, "result-shortcuts").await?;
    server.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_request_builder_sends_explicit_method_headers_and_body() -> TestResult {
    let server = spawn_server(vec![Reply::EchoRequest]).await?;
    let (net, client) = make_client(&server, "result-builder").await?;
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    let request = HttpRequest::builder()
        .method(Method::PUT)
        .path("/builder")
        .headers(headers.clone())
        .body(Bytes::from_static(br#"{"value":7}"#))
        .build()?;
    check(
        request.method == Method::PUT,
        "builder method was not retained",
    )?;
    check(request.path == "/builder", "builder path was not retained")?;
    check(
        request.headers == headers,
        "builder headers were not retained",
    )?;
    check(
        request.body.as_ref() == br#"{"value":7}"#,
        "builder body was not retained",
    )?;

    let response = bounded(client.request(request)).await??;
    let text = response.text()?;
    check(
        text.contains("PUT /builder HTTP/1.1"),
        "request lost explicit method or path",
    )?;
    check(
        text.to_ascii_lowercase()
            .contains("content-type: application/json"),
        "request lost content type",
    )?;
    check(
        text.ends_with("\r\n\r\n{\"value\":7}"),
        "request lost JSON body",
    )?;

    destroy_client(net, "result-builder").await?;
    server.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_post_and_request_transmit_raw_binary_without_utf8_conversion() -> TestResult {
    let server = spawn_server(vec![Reply::EchoBody, Reply::EchoBody, Reply::EchoBody]).await?;
    let (net, client) = make_client(&server, "result-binary-post").await?;
    let body = [0_u8, 0xff, 0x80, 0xc3, 0x28, b'\r', b'\n'];

    let post = bounded(client.post("/binary", body)).await??;
    check(
        post.body.as_ref() == body,
        "POST shortcut changed binary bytes",
    )?;
    let post_headers =
        bounded(client.post_with_headers("/binary", HeaderMap::new(), body)).await??;
    check(
        post_headers.body.as_ref() == body,
        "POST with headers changed binary bytes",
    )?;
    let request = HttpRequest::builder()
        .method(Method::POST)
        .path("/binary")
        .headers(HeaderMap::new())
        .body(Bytes::copy_from_slice(&body))
        .build()?;
    let response = bounded(client.request(request)).await??;
    check(
        response.body.as_ref() == body,
        "request changed binary bytes",
    )?;

    destroy_client(net, "result-binary-post").await?;
    server.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_post_empty_body_is_explicitly_transmitted() -> TestResult {
    let server = spawn_server(vec![Reply::EchoRequest]).await?;
    let (net, client) = make_client(&server, "result-empty-post").await?;
    let response = bounded(client.post("/empty", b"")).await??;
    let text = response.text()?.to_ascii_lowercase();
    check(
        text.contains("post /empty http/1.1"),
        "empty POST method or path was not forwarded",
    )?;
    check(
        text.contains("content-length: 0") || text.contains("transfer-encoding: chunked"),
        "empty POST did not carry an explicit body framing",
    )?;
    destroy_client(net, "result-empty-post").await?;
    server.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_shortcuts_do_not_inherit_global_retry_defaults() -> TestResult {
    let server = spawn_server(vec![Reply::Fixed(
        StatusCode::SERVICE_UNAVAILABLE,
        "text/plain",
        b"retry-me".to_vec(),
    )])
    .await?;
    let config = HttpClientConfig::new(format!("http://{}", server.address))?
        .with_timeout(Duration::from_secs(1))?
        .with_default_retry_policy(open_net::api::http::RetryPolicy::new(1));
    let (net, client) = make_client_with_config("result-no-default-retry", config).await?;
    let response = bounded(client.get("/retry")).await??;
    check(
        response.status == StatusCode::SERVICE_UNAVAILABLE,
        "convenience request inherited an unexpected retry",
    )?;
    destroy_client(net, "result-no-default-retry").await?;
    server.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_get_bytes_with_headers_merges_client_and_request_headers() -> TestResult {
    let server = spawn_server(vec![Reply::EchoRequest]).await?;
    let mut common = HeaderMap::new();
    common.insert("x-common", HeaderValue::from_static("common-value"));
    common.insert("x-override", HeaderValue::from_static("old-value"));
    let config = HttpClientConfig::new(format!("http://{}", server.address))?
        .with_common_headers(common)?
        .with_timeout(Duration::from_secs(2))?;
    let (net, client) = make_client_with_config("result-bytes-headers", config).await?;
    let mut headers = HeaderMap::new();
    headers.insert("x-override", HeaderValue::from_static("request-value"));
    let response = bounded(client.get_bytes_with_headers("/bytes", headers)).await??;
    let text = response.text()?.to_ascii_lowercase();
    check(
        text.contains("get /bytes http/1.1"),
        "binary shortcut changed request method",
    )?;
    check(
        text.contains("x-common: common-value"),
        "common header was lost",
    )?;
    check(
        text.contains("x-override: request-value"),
        "request header did not override common header",
    )?;
    check(
        !text.contains("old-value"),
        "overridden header was still transmitted",
    )?;

    destroy_client(net, "result-bytes-headers").await?;
    server.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_rejects_invalid_path_without_sending_network_request() -> TestResult {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let config = HttpClientConfig::new(format!("http://{}", listener.local_addr()?))?;
    let (net, client) = make_client_with_config("result-invalid-path", config).await?;
    let result = bounded(client.get("http://[invalid-host")).await?;
    match result {
        Ok(_) => return Err(std::io::Error::other("invalid URL was accepted").into()),
        Err(error) => check(
            error.kind() == ErrorKind::InvalidInput,
            "invalid path returned wrong error kind",
        )?,
    }
    check(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "invalid request reached the server",
    )?;
    destroy_client(net, "result-invalid-path").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_rejects_response_larger_than_configured_limit() -> TestResult {
    let server = spawn_server(vec![Reply::Fixed(
        StatusCode::OK,
        "application/octet-stream",
        vec![7_u8; 128],
    )])
    .await?;
    let config = HttpClientConfig::new(format!("http://{}", server.address))?
        .with_max_response_bytes(64)?
        .with_timeout(Duration::from_secs(2))?;
    let (net, client) = make_client_with_config("result-body-limit", config).await?;
    match bounded(client.get_bytes("/large")).await? {
        Ok(_) => return Err(std::io::Error::other("oversized response was accepted").into()),
        Err(error) => check(
            error.kind() == ErrorKind::ItemTooLarge,
            "oversized response returned wrong error kind",
        )?,
    }

    destroy_client(net, "result-body-limit").await?;
    server.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_connection_failure_remains_transport_error() -> TestResult {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    drop(listener);
    let config =
        HttpClientConfig::new(format!("http://{address}"))?.with_timeout(Duration::from_secs(2))?;
    let (net, client) = make_client_with_config("result-transport-error", config).await?;
    match bounded(client.get("/unavailable")).await? {
        Ok(_) => {
            return Err(
                std::io::Error::other("connection failure was returned as HTTP response").into(),
            )
        }
        Err(error) => {
            check(
                error.kind() == ErrorKind::Io,
                "connection failure returned wrong error kind",
            )?;
            check(
                error.http_status().is_none(),
                "transport error acquired an HTTP status",
            )?;
        }
    }
    destroy_client(net, "result-transport-error").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_api_client_created_by_factory_is_available_from_registry() -> TestResult {
    let server = spawn_server(vec![Reply::Fixed(
        StatusCode::OK,
        "text/plain",
        b"registered".to_vec(),
    )])
    .await?;
    let (net, _client) = make_client(&server, "result-registry").await?;
    let registered = net.get_http_client("result-registry")?;
    let response = bounded(registered.get("/registered")).await??;
    check(
        response.body.as_ref() == b"registered",
        "factory client was not usable via registry",
    )?;
    destroy_client(net, "result-registry").await?;
    server.finish().await
}

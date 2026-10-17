#![cfg(feature = "http-client-http2")]

use async_trait::async_trait;
use bytes::Bytes;
use http::{header, Method, Response, StatusCode};
use open_net::api::error::{ErrorKind, ErrorStage};
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpProtocol, HttpRequest, HttpRequestMethod, HttpRequestTrait,
    HttpResponseResult, HttpTlsConfig, HttpVersionPolicy,
};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use std::error::Error;
use std::net::{SocketAddr, TcpListener as CleartextListener};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const CA: &[u8] = include_bytes!("../src/test_fixtures/network/ca.pem");
const SERVER: &[u8] = include_bytes!("fixtures/network/server.pem");
const SERVER_KEY: &[u8] = include_bytes!("fixtures/network/server-key.pem");
const REQUEST_BODY: &[u8] = b"private body that must never reach cleartext";
const RESPONSE_BODY: &[u8] = b"secure destination";
const REDIRECTS: [StatusCode; 5] = [
    StatusCode::MOVED_PERMANENTLY,
    StatusCode::FOUND,
    StatusCode::SEE_OTHER,
    StatusCode::TEMPORARY_REDIRECT,
    StatusCode::PERMANENT_REDIRECT,
];

#[derive(Debug)]
struct ObservedRequest {
    method: Method,
    path: String,
    body: Vec<u8>,
}

struct Peer {
    address: SocketAddr,
    task: tokio::task::JoinHandle<TestResult<ObservedRequest>>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Peer {
    async fn finish(mut self) -> TestResult<ObservedRequest> {
        tokio::time::timeout(Duration::from_secs(5), &mut self.task).await??
    }
}

fn tls_acceptor(alpn: &[u8]) -> TestResult<TlsAcceptor> {
    let mut tls =
        rustls::ServerConfig::builder_with_provider(Arc::new(rustls_openssl::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(
                CertificateDer::pem_slice_iter(SERVER).collect::<Result<Vec<_>, _>>()?,
                PrivateKeyDer::from_pem_slice(SERVER_KEY)?,
            )?;
    tls.alpn_protocols = vec![alpn.to_vec()];
    Ok(TlsAcceptor::from(Arc::new(tls)))
}

async fn tls_peer(status: StatusCode, location: Option<String>) -> TestResult<Peer> {
    let acceptor = tls_acceptor(b"h2")?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let stream = acceptor.accept(socket).await?;
        assert_eq!(stream.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
        let mut connection = h2::server::handshake(stream).await?;
        let (request, mut respond) = connection
            .accept()
            .await
            .ok_or_else(|| std::io::Error::other("peer closed before HTTP/2 request"))??;
        let method = request.method().clone();
        let path = request.uri().path().to_owned();
        let mut receive = request.into_body();
        // Reading request DATA requires concurrently polling the h2 connection.
        // Keep that driver alive through the response, then send GOAWAY so the
        // fixture does not depend on the client's connection-pool idle timeout.
        let response = async move {
            let mut body = Vec::new();
            while let Some(data) = receive.data().await {
                let data = data?;
                body.extend_from_slice(&data);
                receive.flow_control().release_capacity(data.len())?;
            }
            let mut builder = Response::builder().status(status);
            if let Some(location) = location {
                builder = builder.header(header::LOCATION, location);
            }
            let mut send = respond.send_response(builder.body(())?, false)?;
            send.send_data(Bytes::from_static(RESPONSE_BODY), true)?;
            Ok::<_, Box<dyn Error + Send + Sync>>(ObservedRequest { method, path, body })
        };
        let drive = async move {
            connection.graceful_shutdown();
            while let Some(incoming) = connection.accept().await {
                match incoming {
                    Err(error)
                        if error.get_io().is_some_and(|error| {
                            matches!(
                                error.kind(),
                                std::io::ErrorKind::BrokenPipe
                                    | std::io::ErrorKind::ConnectionReset
                            )
                        }) =>
                    {
                        break;
                    }
                    Err(error) => return Err(error.into()),
                    Ok(_) => return Err("unexpected additional HTTP/2 request".into()),
                }
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        };
        let (observed, ()) = tokio::try_join!(response, drive)?;
        Ok(observed)
    });
    Ok(Peer { address, task })
}

async fn serve_h1<S>(
    mut stream: S,
    status: StatusCode,
    location: Option<String>,
) -> TestResult<ObservedRequest>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(stream.read_u8().await?);
        if head.len() > 16 * 1024 {
            return Err("HTTP/1 fixture header limit exceeded".into());
        }
    }
    let head = std::str::from_utf8(&head)?;
    let mut lines = head.lines();
    let request_line = lines.next().ok_or("missing request line")?;
    let mut parts = request_line.split_whitespace();
    let method = Method::from_bytes(parts.next().ok_or("missing method")?.as_bytes())?;
    let path = parts.next().ok_or("missing request target")?.to_owned();
    let mut length = 0;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse::<usize>()?;
            }
        }
    }
    if length > 16 * 1024 {
        return Err("HTTP/1 fixture body limit exceeded".into());
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await?;
    let location = location.map_or_else(String::new, |url| format!("Location: {url}\r\n"));
    let head = format!(
        "HTTP/1.1 {} {}\r\n{location}Content-Length: {}\r\nConnection: close\r\n\r\n",
        status.as_u16(),
        status.canonical_reason().unwrap_or("Response"),
        RESPONSE_BODY.len(),
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(RESPONSE_BODY).await?;
    stream.shutdown().await?;
    Ok(ObservedRequest { method, path, body })
}

async fn h1_peer(tls: bool, status: StatusCode, location: Option<String>) -> TestResult<Peer> {
    let acceptor = tls.then(|| tls_acceptor(b"http/1.1")).transpose()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        match acceptor {
            Some(acceptor) => serve_h1(acceptor.accept(socket).await?, status, location).await,
            None => serve_h1(socket, status, location).await,
        }
    });
    Ok(Peer { address, task })
}

#[derive(Clone, Copy, Debug)]
enum Provider {
    None,
    #[cfg(feature = "http-client-auth")]
    Auth,
    #[cfg(feature = "http-client-cookies")]
    Cookies,
    #[cfg(all(feature = "http-client-auth", feature = "http-client-cookies"))]
    AuthAndCookies,
}

fn client_for(peer: &Peer, provider: Provider) -> TestResult<HttpClient> {
    client_for_policy(peer, provider, HttpVersionPolicy::Http2Only)
}

fn client_for_policy(
    peer: &Peer,
    provider: Provider,
    policy: HttpVersionPolicy,
) -> TestResult<HttpClient> {
    let config = HttpClientConfig::new(format!("https://{}", peer.address))?
        .with_tls(HttpTlsConfig::default().with_root_certificates(CA, true)?)
        .with_environment_proxy(false)
        .with_timeout(Duration::from_secs(3))?
        .with_http_version_policy(policy);
    let config = match provider {
        Provider::None => config,
        #[cfg(feature = "http-client-auth")]
        Provider::Auth => config.with_auth(open_net::api::http::BasicAuth::new("user", "secret")?),
        #[cfg(feature = "http-client-cookies")]
        Provider::Cookies => config.with_cookie_store(open_net::api::http::CookieJar::new()),
        #[cfg(all(feature = "http-client-auth", feature = "http-client-cookies"))]
        Provider::AuthAndCookies => config
            .with_auth(open_net::api::http::BasicAuth::new("user", "secret")?)
            .with_cookie_store(open_net::api::http::CookieJar::new()),
    };
    Ok(HttpClient::new(config, "http2-redirect-regression")?)
}

#[derive(Clone, Copy, Debug)]
enum Delivery {
    Buffered,
    Streaming,
    Callback,
    Sse,
}

const DELIVERIES: [Delivery; 3] = [Delivery::Buffered, Delivery::Streaming, Delivery::Callback];

struct CallbackRequest(oneshot::Sender<HttpResponseResult>);

#[async_trait]
impl HttpRequestTrait for CallbackRequest {
    fn get_path(&self) -> String {
        "/source".to_owned()
    }

    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::POST
    }

    fn get_req_body(&self) -> Bytes {
        Bytes::from_static(REQUEST_BODY)
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let _ = self.0.send(result);
    }
}

#[derive(Debug)]
struct DeliveredResponse {
    status: StatusCode,
    protocol: HttpProtocol,
    body: Vec<u8>,
}

async fn deliver(
    client: &HttpClient,
    delivery: Delivery,
) -> TestResult<open_net::Result<DeliveredResponse>> {
    if matches!(delivery, Delivery::Callback) {
        let (sender, receiver) = oneshot::channel();
        let handle = client
            .send(CallbackRequest(sender))
            .map_err(open_net::NetError::from)?;
        let result = tokio::time::timeout(Duration::from_secs(5), receiver).await??;
        let completion = tokio::time::timeout(Duration::from_secs(5), handle.finished()).await?;
        assert!(completion.callback.is_ok());
        if let Err(error) = &result {
            assert_eq!(error.context().http_request_id, Some(handle.id()));
            assert_eq!(
                completion
                    .response
                    .expect_err("callback reported an error")
                    .kind(),
                error.kind()
            );
        }
        return Ok(result.map(|response| DeliveredResponse {
            status: response.status,
            protocol: response.protocol(),
            body: response.body.to_vec(),
        }));
    }
    let request = HttpRequest::builder()
        .method(Method::POST)
        .path("/source")
        .body(Bytes::from_static(REQUEST_BODY))
        .build()?;
    let result = async {
        match delivery {
            Delivery::Buffered => client
                .request(request)
                .await
                .map(|response| DeliveredResponse {
                    status: response.status,
                    protocol: response.protocol(),
                    body: response.body.to_vec(),
                }),
            Delivery::Streaming | Delivery::Sse => {
                let mut response = if matches!(delivery, Delivery::Sse) {
                    client.sse("/source", http::HeaderMap::new()).await?
                } else {
                    client.stream(request).await?
                };
                let status = response.status;
                let protocol = response.info().protocol;
                let mut body = Vec::new();
                while let Some(chunk) = response.next_chunk().await {
                    body.extend_from_slice(&chunk?);
                }
                Ok(DeliveredResponse {
                    status,
                    protocol,
                    body,
                })
            }
            Delivery::Callback => unreachable!("callback delivery handled above"),
        }
    };
    Ok(tokio::time::timeout(Duration::from_secs(5), result).await?)
}

async fn rejects_downgrade(
    status: StatusCode,
    delivery: Delivery,
    provider: Provider,
) -> TestResult {
    // Leave every connection queued in the kernel. Checking after the operation
    // has finished detects even a connect-then-close attempt without a sleep or
    // a race against a separately scheduled accept task.
    let cleartext = CleartextListener::bind("127.0.0.1:0")?;
    cleartext.set_nonblocking(true)?;
    let source = tls_peer(
        status,
        Some(format!("http://{}/destination", cleartext.local_addr()?)),
    )
    .await?;
    let client = client_for(&source, provider)?;
    let result = deliver(&client, delivery).await?;
    client.shutdown().await?;
    let source_request = source.finish().await?;
    let (method, body) = if matches!(delivery, Delivery::Sse) {
        (Method::GET, b"".as_slice())
    } else {
        (Method::POST, REQUEST_BODY)
    };
    assert_eq!(source_request.method, method);
    assert_eq!(source_request.path, "/source");
    assert_eq!(source_request.body, body);
    assert_blocked_downgrade(&cleartext, result, status, delivery, provider);
    Ok(())
}

fn assert_blocked_downgrade(
    cleartext: &CleartextListener,
    result: open_net::Result<DeliveredResponse>,
    status: StatusCode,
    delivery: Delivery,
    provider: Provider,
) {
    match cleartext.accept() {
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
        other => {
            panic!("{status} {delivery:?} {provider:?} reached the cleartext listener: {other:?}")
        }
    }
    let error = result.expect_err("HTTP/2-only must reject an HTTPS-to-HTTP redirect");
    assert_eq!(error.kind(), ErrorKind::Protocol);
    assert_eq!(error.context().stage, Some(ErrorStage::Receive));
    assert!(error.context().http_request_id.is_some());
    assert_eq!(error.context().http_attempt, Some(1));
    assert_eq!(error.timeout_phase(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_rejects_all_cleartext_redirects_without_providers() -> TestResult {
    for status in REDIRECTS {
        rejects_downgrade(status, Delivery::Buffered, Provider::None).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_rejects_all_cleartext_redirects_without_providers() -> TestResult {
    for status in REDIRECTS {
        rejects_downgrade(status, Delivery::Streaming, Provider::None).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_rejects_all_cleartext_redirects_without_providers() -> TestResult {
    for status in REDIRECTS {
        rejects_downgrade(status, Delivery::Callback, Provider::None).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_rejects_all_cleartext_redirects_without_providers() -> TestResult {
    for status in REDIRECTS {
        rejects_downgrade(status, Delivery::Sse, Provider::None).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_cleartext_after_an_accepted_https_redirect() -> TestResult {
    for delivery in DELIVERIES {
        let cleartext = CleartextListener::bind("127.0.0.1:0")?;
        cleartext.set_nonblocking(true)?;
        let intermediate = tls_peer(
            StatusCode::PERMANENT_REDIRECT,
            Some(format!("http://{}/destination", cleartext.local_addr()?)),
        )
        .await?;
        let source = tls_peer(
            StatusCode::TEMPORARY_REDIRECT,
            Some(format!("https://{}/intermediate", intermediate.address)),
        )
        .await?;
        let client = client_for(&source, Provider::None)?;
        let result = deliver(&client, delivery).await?;
        client.shutdown().await?;
        let initial = source.finish().await?;
        let redirected = intermediate.finish().await?;
        assert_eq!(initial.path, "/source");
        assert_eq!(redirected.path, "/intermediate");
        for observed in [initial, redirected] {
            assert_eq!(observed.method, Method::POST);
            assert_eq!(observed.body, REQUEST_BODY);
        }
        assert_blocked_downgrade(
            &cleartext,
            result,
            StatusCode::PERMANENT_REDIRECT,
            delivery,
            Provider::None,
        );
    }
    Ok(())
}

async fn preserves_secure_redirects(provider: Provider) -> TestResult {
    for status in [
        StatusCode::TEMPORARY_REDIRECT,
        StatusCode::PERMANENT_REDIRECT,
    ] {
        for delivery in DELIVERIES {
            let destination = tls_peer(StatusCode::OK, None).await?;
            let source = tls_peer(
                status,
                Some(format!("https://{}/destination", destination.address)),
            )
            .await?;
            let client = client_for(&source, provider)?;
            let result = deliver(&client, delivery).await??;
            assert_eq!(result.status, StatusCode::OK);
            assert_eq!(result.protocol, HttpProtocol::Http2);
            assert_eq!(result.body, RESPONSE_BODY);
            client.shutdown().await?;
            let initial = source.finish().await?;
            let redirected = destination.finish().await?;
            assert_eq!(initial.path, "/source");
            assert_eq!(redirected.path, "/destination");
            for observed in [initial, redirected] {
                assert_eq!(observed.method, Method::POST, "{status} {delivery:?}");
                assert_eq!(observed.body, REQUEST_BODY, "{status} {delivery:?}");
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_redirects_preserve_post_bodies_without_providers() -> TestResult {
    preserves_secure_redirects(Provider::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_and_http1_only_keep_existing_cleartext_redirect_behavior() -> TestResult {
    for policy in [HttpVersionPolicy::Auto, HttpVersionPolicy::Http1Only] {
        for status in [
            StatusCode::TEMPORARY_REDIRECT,
            StatusCode::PERMANENT_REDIRECT,
        ] {
            for delivery in DELIVERIES {
                let destination = h1_peer(false, StatusCode::OK, None).await?;
                let source = h1_peer(
                    true,
                    status,
                    Some(format!("http://{}/destination", destination.address)),
                )
                .await?;
                let client = client_for_policy(&source, Provider::None, policy)?;
                let result = deliver(&client, delivery).await??;
                assert_eq!(result.status, StatusCode::OK);
                assert_eq!(result.protocol, HttpProtocol::Http11);
                assert_eq!(result.body, RESPONSE_BODY);
                client.shutdown().await?;
                let initial = source.finish().await?;
                let redirected = destination.finish().await?;
                assert_eq!(redirected.path, "/destination");
                for observed in [initial, redirected] {
                    assert_eq!(observed.method, Method::POST);
                    assert_eq!(observed.body, REQUEST_BODY);
                }
            }
        }
    }
    Ok(())
}

#[cfg(feature = "http-client-auth")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_auth_rejects_downgrades_and_preserves_secure_redirects() -> TestResult {
    for status in REDIRECTS {
        for delivery in DELIVERIES {
            rejects_downgrade(status, delivery, Provider::Auth).await?;
        }
    }
    preserves_secure_redirects(Provider::Auth).await
}

#[cfg(feature = "http-client-cookies")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_cookie_store_rejects_downgrades_and_preserves_secure_redirects() -> TestResult {
    for status in REDIRECTS {
        for delivery in DELIVERIES {
            rejects_downgrade(status, delivery, Provider::Cookies).await?;
        }
    }
    preserves_secure_redirects(Provider::Cookies).await
}

#[cfg(all(feature = "http-client-auth", feature = "http-client-cookies"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn combined_providers_reject_downgrades_and_preserve_secure_redirects() -> TestResult {
    for status in REDIRECTS {
        for delivery in DELIVERIES {
            rejects_downgrade(status, delivery, Provider::AuthAndCookies).await?;
        }
    }
    preserves_secure_redirects(Provider::AuthAndCookies).await
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

#![cfg(feature = "http-client")]

use open_net::api::retry::{Backoff, RetrySchedule};

use bytes::Bytes;
use http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use open_net::api::error::{ErrorKind, ErrorStage};
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpRedirectError, HttpRequest, HttpRequestMethod,
    HttpRequestOptions, HttpRequestTrait, HttpResponseResult, HttpRetryPolicy, HttpTlsConfig,
    RedirectPolicy, RetryOn, TransportErrorKind,
};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use std::error::Error;
use std::net::{SocketAddr, TcpListener as UnacceptedListener};
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
const PRIVATE_BODY: &[u8] = b"redirect-private-request-body";
const PRIVATE_LOCATION: &str = "redirect-private-path?token=redirect-private-query";
const REDIRECTS: [StatusCode; 5] = [
    StatusCode::MOVED_PERMANENTLY,
    StatusCode::FOUND,
    StatusCode::SEE_OTHER,
    StatusCode::TEMPORARY_REDIRECT,
    StatusCode::PERMANENT_REDIRECT,
];

#[derive(Clone)]
struct Reply {
    status: StatusCode,
    location: Option<String>,
}

impl Reply {
    fn redirect(status: StatusCode, location: impl Into<String>) -> Self {
        Self {
            status,
            location: Some(location.into()),
        }
    }

    fn ok() -> Self {
        Self {
            status: StatusCode::OK,
            location: None,
        }
    }
}

#[derive(Debug)]
struct Observed {
    method: Method,
    target: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

struct Peer {
    address: SocketAddr,
    tls: bool,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<TestResult<Vec<Observed>>>,
}

impl Peer {
    async fn start(tls: bool, replies: Vec<Reply>) -> TestResult<Self> {
        let acceptor = if tls {
            let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls_openssl::default_provider(),
            ))
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(
                CertificateDer::pem_slice_iter(SERVER).collect::<Result<Vec<_>, _>>()?,
                PrivateKeyDer::from_pem_slice(SERVER_KEY)?,
            )?;
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            Some(TlsAcceptor::from(Arc::new(config)))
        } else {
            None
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut observed = Vec::new();
            for reply in replies {
                let (socket, _) = tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => accepted?,
                };
                let request = match &acceptor {
                    Some(acceptor) => serve(acceptor.accept(socket).await?, reply).await?,
                    None => serve(socket, reply).await?,
                };
                observed.push(request);
            }
            Ok(observed)
        });
        Ok(Self {
            address,
            tls,
            stop: Some(stop),
            task,
        })
    }

    fn origin(&self) -> String {
        format!(
            "{}://{}",
            if self.tls { "https" } else { "http" },
            self.address
        )
    }

    async fn finish(mut self) -> TestResult<Vec<Observed>> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        tokio::time::timeout(Duration::from_secs(5), &mut self.task).await??
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve<S>(mut socket: S, reply: Reply) -> TestResult<Observed>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(socket.read_u8().await?);
        if head.len() > 16 * 1024 {
            return Err("fixture request headers exceeded limit".into());
        }
    }
    let mut lines = std::str::from_utf8(&head)?.lines();
    let mut request_line = lines
        .next()
        .ok_or("missing request line")?
        .split_whitespace();
    let method = Method::from_bytes(request_line.next().ok_or("missing method")?.as_bytes())?;
    let target = request_line.next().ok_or("missing target")?.to_owned();
    let mut headers = HeaderMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').ok_or("invalid header")?;
        headers.append(
            http::HeaderName::from_bytes(name.as_bytes())?,
            HeaderValue::from_str(value.trim())?,
        );
    }
    let length = headers
        .get(header::CONTENT_LENGTH)
        .map(|value| -> TestResult<usize> { Ok(value.to_str()?.parse()?) })
        .transpose()?
        .unwrap_or(0);
    if length > 16 * 1024 {
        return Err("fixture request body exceeded limit".into());
    }
    let mut body = vec![0; length];
    socket.read_exact(&mut body).await?;
    let location = reply
        .location
        .map_or_else(String::new, |location| format!("Location: {location}\r\n"));
    let mut response = format!(
        "HTTP/1.1 {} Response\r\n{location}Content-Length: 2\r\nConnection: close\r\n\r\n",
        reply.status.as_u16(),
    )
    .into_bytes();
    if method != Method::HEAD {
        response.extend_from_slice(b"ok");
    }
    // A rejected redirect is dropped immediately after its headers arrive.
    // Send the small response together, and tolerate the peer closing while
    // TLS close-notify is flushed. Both the request and response write must
    // still succeed, and each test separately checks the client's result.
    socket.write_all(&response).await?;
    allow_early_response_close(socket.shutdown().await)?;
    Ok(Observed {
        method,
        target,
        headers,
        body,
    })
}

fn allow_early_response_close(result: std::io::Result<()>) -> TestResult {
    match result {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

#[derive(Clone, Copy, Debug)]
enum Provider {
    None,
    #[cfg(feature = "http-client-auth")]
    Auth,
    #[cfg(feature = "http-client-cookies")]
    Cookies,
    #[cfg(all(feature = "http-client-auth", feature = "http-client-cookies"))]
    Both,
}

const PROVIDERS: &[Provider] = &[
    Provider::None,
    #[cfg(feature = "http-client-auth")]
    Provider::Auth,
    #[cfg(feature = "http-client-cookies")]
    Provider::Cookies,
    #[cfg(all(feature = "http-client-auth", feature = "http-client-cookies"))]
    Provider::Both,
];

fn config(peer: &Peer, provider: Provider) -> TestResult<HttpClientConfig> {
    let config = HttpClientConfig::new(peer.origin())?
        .with_environment_proxy(false)
        .with_timeout(Duration::from_secs(3))?
        .with_tls(HttpTlsConfig::default().with_root_certificates(CA, true)?);
    Ok(match provider {
        Provider::None => config,
        #[cfg(feature = "http-client-auth")]
        Provider::Auth => config.with_auth(open_net::api::http::BasicAuth::new("user", "secret")?),
        #[cfg(feature = "http-client-cookies")]
        Provider::Cookies => config.with_cookie_store(open_net::api::http::CookieJar::new()),
        #[cfg(all(feature = "http-client-auth", feature = "http-client-cookies"))]
        Provider::Both => config
            .with_auth(open_net::api::http::BasicAuth::new("user", "secret")?)
            .with_cookie_store(open_net::api::http::CookieJar::new()),
    })
}

#[derive(Clone, Copy, Debug)]
enum Delivery {
    Buffered,
    Callback,
    Streaming,
    Sse,
}

const DELIVERIES: &[Delivery] = &[
    Delivery::Buffered,
    Delivery::Callback,
    Delivery::Streaming,
    Delivery::Sse,
];

struct Callback(oneshot::Sender<HttpResponseResult>, Method);

#[open_net::async_trait]
impl HttpRequestTrait for Callback {
    fn get_path(&self) -> String {
        "/source".into()
    }

    fn get_method(&self) -> HttpRequestMethod {
        self.1.clone().into()
    }

    fn get_req_body(&self) -> Bytes {
        Bytes::from_static(PRIVATE_BODY)
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let _ = self.0.send(result);
    }
}

fn request(method: Method) -> open_net::Result<HttpRequest> {
    HttpRequest::builder()
        .method(method)
        .path("/source")
        .body(Bytes::from_static(PRIVATE_BODY))
        .build()
}

async fn deliver(
    client: &HttpClient,
    delivery: Delivery,
) -> TestResult<open_net::Result<StatusCode>> {
    let result = async {
        match delivery {
            Delivery::Buffered => Ok(client.request(request(Method::POST)?).await?.status),
            Delivery::Callback => {
                let (sender, receiver) = oneshot::channel();
                let handle = client
                    .send(Callback(sender, Method::POST))
                    .map_err(open_net::NetError::from)?;
                let response = receiver
                    .await
                    .map_err(|_| open_net::NetError::from(ErrorKind::Internal))?;
                assert!(handle.finished().await.callback.is_ok());
                response.map(|response| response.status)
            }
            Delivery::Streaming | Delivery::Sse => {
                let mut response = if matches!(delivery, Delivery::Sse) {
                    client.sse("/source", HeaderMap::new()).await?
                } else {
                    client.stream(request(Method::POST)?).await?
                };
                let status = response.status;
                while let Some(chunk) = response.next_chunk().await {
                    chunk?;
                }
                Ok(status)
            }
        }
    };
    Ok(tokio::time::timeout(Duration::from_secs(5), result).await?)
}

fn assert_no_connection(listener: &UnacceptedListener) {
    match listener.accept() {
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
        result => panic!("forbidden redirect attempted a connection: {result:?}"),
    }
}

fn assert_rejected(error: &open_net::NetError, reason: HttpRedirectError) {
    assert_eq!(error.kind(), ErrorKind::Protocol);
    assert_eq!(error.context().stage, Some(ErrorStage::Receive));
    assert!(error.context().http_request_id.is_some());
    assert_eq!(error.context().http_attempt, Some(1));
    assert_eq!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<HttpRedirectError>()),
        Some(&reason),
    );
    let mut diagnostics = format!("{error} {error:?}");
    let mut source = error.source();
    while let Some(error) = source {
        diagnostics.push_str(&format!(" {error} {error:?}"));
        source = error.source();
    }
    for secret in [
        "redirect-private-path",
        "redirect-private-query",
        "redirect-private-request-body",
        "private-user",
        "private-password",
    ] {
        assert!(
            !diagnostics.contains(secret),
            "policy error exposed private input"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_entry_point_blocks_allowlisted_downgrades_with_any_provider() -> TestResult {
    // This file intentionally requires only http-client. A build without the
    // HTTP/2 feature must enforce the same security policy.
    for &provider in PROVIDERS {
        for status in REDIRECTS {
            for &delivery in DELIVERIES {
                let target = UnacceptedListener::bind("127.0.0.1:0")?;
                target.set_nonblocking(true)?;
                let target_origin = format!("http://{}", target.local_addr()?);
                let source = Peer::start(
                    true,
                    vec![Reply::redirect(
                        status,
                        format!("{target_origin}/{PRIVATE_LOCATION}"),
                    )],
                )
                .await?;
                let client = HttpClient::new(
                    config(&source, provider)?
                        .with_redirect_policy(RedirectPolicy::allow_origins([target_origin])?),
                    "redirect-downgrade",
                )?;
                let error = deliver(&client, delivery)
                    .await?
                    .expect_err("allowlisting an origin must not authorize TLS downgrade");
                assert_rejected(&error, HttpRedirectError::InsecureTarget);
                client.shutdown().await?;
                let observed = source.finish().await?;
                assert_eq!(observed.len(), 1, "{provider:?} {status} {delivery:?}");
                assert_no_connection(&target);
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_same_origin_rejects_cross_origin_before_connecting() -> TestResult {
    for status in REDIRECTS {
        for &delivery in DELIVERIES {
            let target = UnacceptedListener::bind("127.0.0.1:0")?;
            target.set_nonblocking(true)?;
            let source = Peer::start(
                false,
                vec![Reply::redirect(
                    status,
                    format!("http://{}/{PRIVATE_LOCATION}", target.local_addr()?),
                )],
            )
            .await?;
            let client = HttpClient::new(config(&source, Provider::None)?, "redirect-origin")?;
            let error = deliver(&client, delivery)
                .await?
                .expect_err("cross-origin accepted");
            assert_rejected(&error, HttpRedirectError::OriginNotAllowed);
            client.shutdown().await?;
            assert_eq!(source.finish().await?.len(), 1);
            assert_no_connection(&target);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allowlisted_cross_origin_replays_body_and_strips_sensitive_headers() -> TestResult {
    for &provider in PROVIDERS {
        for status in [
            StatusCode::TEMPORARY_REDIRECT,
            StatusCode::PERMANENT_REDIRECT,
        ] {
            let target = Peer::start(false, vec![Reply::ok()]).await?;
            let source = Peer::start(
                false,
                vec![Reply::redirect(
                    status,
                    format!("{}/target", target.origin()),
                )],
            )
            .await?;
            let mut headers = HeaderMap::new();
            headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_static("Bearer explicit"),
            );
            headers.insert(header::COOKIE, HeaderValue::from_static("private=explicit"));
            let mut sensitive = HeaderValue::from_static("private-key");
            sensitive.set_sensitive(true);
            headers.insert("x-api-key", sensitive);
            headers.insert("x-public", HeaderValue::from_static("retained"));
            let client = HttpClient::new(
                config(&source, provider)?
                    .with_common_headers(headers)?
                    .with_redirect_policy(RedirectPolicy::allow_origins([target.origin()])?),
                "redirect-allowlist",
            )?;
            assert_eq!(
                client.request(request(Method::POST)?).await?.status,
                StatusCode::OK
            );
            client.shutdown().await?;
            let initial = source.finish().await?;
            let observed = target.finish().await?;
            assert_eq!(initial.len(), 1);
            assert_eq!(observed.len(), 1);
            assert_eq!(observed[0].method, Method::POST);
            assert_eq!(observed[0].target, "/target");
            assert_eq!(observed[0].body, PRIVATE_BODY);
            assert!(!observed[0].headers.contains_key(header::REFERER));
            for name in [
                header::AUTHORIZATION,
                header::COOKIE,
                http::HeaderName::from_static("x-api-key"),
            ] {
                assert!(!observed[0].headers.contains_key(name));
            }
            assert_eq!(observed[0].headers.get("x-public").unwrap(), "retained");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_redirects_return_original_response_without_connecting() -> TestResult {
    for status in REDIRECTS {
        for &delivery in DELIVERIES {
            let target = UnacceptedListener::bind("127.0.0.1:0")?;
            target.set_nonblocking(true)?;
            let source = Peer::start(
                true,
                vec![Reply::redirect(
                    status,
                    format!("http://{}/target", target.local_addr()?),
                )],
            )
            .await?;
            let client = HttpClient::new(
                config(&source, Provider::None)?.with_redirect_policy(RedirectPolicy::none()),
                "redirect-none",
            )?;
            assert_eq!(deliver(&client, delivery).await??, status);
            client.shutdown().await?;
            assert_eq!(source.finish().await?.len(), 1);
            assert_no_connection(&target);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_location_returns_the_original_redirect_response() -> TestResult {
    for status in REDIRECTS {
        for &delivery in DELIVERIES {
            let source = Peer::start(
                false,
                vec![Reply {
                    status,
                    location: None,
                }],
            )
            .await?;
            let client = HttpClient::new(config(&source, Provider::None)?, "redirect-no-location")?;
            assert_eq!(deliver(&client, delivery).await??, status);
            client.shutdown().await?;
            assert_eq!(source.finish().await?.len(), 1);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_location_is_a_private_structured_error_before_connecting() -> TestResult {
    let target = UnacceptedListener::bind("127.0.0.1:0")?;
    target.set_nonblocking(true)?;
    let address = target.local_addr()?;
    for location in [
        format!("http://[invalid/{PRIVATE_LOCATION}"),
        format!("http://private-user:private-password@{address}/{PRIVATE_LOCATION}"),
        format!("ftp://{address}/{PRIVATE_LOCATION}"),
        format!("http://{address}/\u{0080}/{PRIVATE_LOCATION}"),
    ] {
        for &delivery in DELIVERIES {
            let source = Peer::start(
                false,
                vec![Reply::redirect(StatusCode::FOUND, location.clone())],
            )
            .await?;
            let client = HttpClient::new(
                config(&source, Provider::None)?.with_redirect_policy(
                    RedirectPolicy::allow_origins([format!("http://{address}")])?,
                ),
                "redirect-invalid-location",
            )?;
            let error = deliver(&client, delivery)
                .await?
                .expect_err("an invalid Location must not be followed or silently accepted");
            assert_rejected(&error, HttpRedirectError::InvalidLocation);
            client.shutdown().await?;
            assert_eq!(source.finish().await?.len(), 1);
            assert_no_connection(&target);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_origin_redirect_methods_and_bodies_do_not_depend_on_providers() -> TestResult {
    for &provider in PROVIDERS {
        for status in REDIRECTS {
            for method in [
                Method::POST,
                Method::PUT,
                Method::PATCH,
                Method::GET,
                Method::HEAD,
            ] {
                let source =
                    Peer::start(false, vec![Reply::redirect(status, "/target"), Reply::ok()])
                        .await?;
                let client = HttpClient::new(config(&source, provider)?, "redirect-method")?;
                let request = HttpRequest::builder()
                    .method(method.clone())
                    .path("/source")
                    .header(header::CONTENT_TYPE, "application/private")?
                    .header(header::CONTENT_ENCODING, "private-encoding")?
                    .body(Bytes::from_static(PRIVATE_BODY))
                    .build()?;
                assert_eq!(client.request(request).await?.status, StatusCode::OK);
                client.shutdown().await?;
                let observed = source.finish().await?;
                assert_eq!(observed.len(), 2);
                let drops_body = status == StatusCode::SEE_OTHER
                    || ((status == StatusCode::MOVED_PERMANENTLY || status == StatusCode::FOUND)
                        && method == Method::POST);
                let expected_method = if drops_body && method != Method::HEAD {
                    Method::GET
                } else {
                    method.clone()
                };
                assert_eq!(
                    observed[1].method, expected_method,
                    "{provider:?} {status} {method}"
                );
                if drops_body {
                    assert!(observed[1].body.is_empty());
                    assert!(!observed[1].headers.contains_key(header::CONTENT_TYPE));
                    assert!(!observed[1].headers.contains_key(header::CONTENT_ENCODING));
                } else {
                    assert_eq!(
                        observed[1].body, PRIVATE_BODY,
                        "{provider:?} {status} {method}"
                    );
                    assert_eq!(
                        observed[1].headers.get(header::CONTENT_TYPE).unwrap(),
                        "application/private"
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonreplayable_callback_returns_redirect_without_resending_body() -> TestResult {
    for (status, method) in [
        (StatusCode::TEMPORARY_REDIRECT, Method::POST),
        (StatusCode::PERMANENT_REDIRECT, Method::POST),
        (StatusCode::MOVED_PERMANENTLY, Method::PUT),
        (StatusCode::FOUND, Method::PUT),
        (StatusCode::MOVED_PERMANENTLY, Method::PATCH),
        (StatusCode::FOUND, Method::PATCH),
    ] {
        let source =
            Peer::start(false, vec![Reply::redirect(status, "/target"), Reply::ok()]).await?;
        let client = HttpClient::new(config(&source, Provider::None)?, "redirect-single-use")?;
        let (sender, receiver) = oneshot::channel();
        let handle = client
            .send_with_options(
                Callback(sender, method.clone()),
                HttpRequestOptions::new().with_replayable_body(false),
            )
            .map_err(open_net::NetError::from)?;
        let response = tokio::time::timeout(Duration::from_secs(5), receiver).await???;
        assert_eq!(response.status, status);
        assert!(handle.finished().await.callback.is_ok());
        client.shutdown().await?;
        let observed = source.finish().await?;
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].method, method);
        assert_eq!(observed[0].body, PRIVATE_BODY);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secure_upgrade_does_not_authorize_a_later_downgrade() -> TestResult {
    let target = UnacceptedListener::bind("127.0.0.1:0")?;
    target.set_nonblocking(true)?;
    let target_origin = format!("http://{}", target.local_addr()?);
    let intermediate = Peer::start(
        true,
        vec![Reply::redirect(
            StatusCode::PERMANENT_REDIRECT,
            format!("{target_origin}/target"),
        )],
    )
    .await?;
    let source = Peer::start(
        false,
        vec![Reply::redirect(
            StatusCode::TEMPORARY_REDIRECT,
            format!("{}/intermediate", intermediate.origin()),
        )],
    )
    .await?;
    let client = HttpClient::new(
        config(&source, Provider::None)?.with_redirect_policy(RedirectPolicy::allow_origins([
            intermediate.origin(),
            target_origin,
        ])?),
        "redirect-upgrade-downgrade",
    )?;
    let error = client
        .request(request(Method::POST)?)
        .await
        .expect_err("second hop downgraded");
    assert_rejected(&error, HttpRedirectError::InsecureTarget);
    client.shutdown().await?;
    for observed in [source.finish().await?, intermediate.finish().await?] {
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].method, Method::POST);
        assert_eq!(observed[0].body, PRIVATE_BODY);
    }
    assert_no_connection(&target);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redirect_policy_rejections_are_not_retried_by_permissive_policies() -> TestResult {
    for &delivery in DELIVERIES {
        let target = UnacceptedListener::bind("127.0.0.1:0")?;
        target.set_nonblocking(true)?;
        let reply = Reply::redirect(
            StatusCode::TEMPORARY_REDIRECT,
            format!("http://{}/{PRIVATE_LOCATION}", target.local_addr()?),
        );
        let source = Peer::start(false, vec![reply; 3]).await?;
        let retry = HttpRetryPolicy::builder()
            .schedule(
                RetrySchedule::builder()
                    .max_attempts(3)
                    .backoff(Backoff::None)
                    .build()?,
            )
            .retry_on(RetryOn::none().with_transport([TransportErrorKind::Protocol]))
            .classifier(move |context| {
                if context.outcome.is_ok_and(|_| true) {
                    open_net::api::retry::RetryDecision::Retry
                } else {
                    open_net::api::retry::RetryDecision::Default
                }
            })
            .safety(open_net::api::http::RequestSafety::IdempotentByContract)
            .build()?;
        let client = HttpClient::new(
            config(&source, Provider::None)?.with_default_retry_policy(retry),
            "redirect-no-retry",
        )?;
        let error = deliver(&client, delivery)
            .await?
            .expect_err("origin policy was bypassed");
        assert_rejected(&error, HttpRedirectError::OriginNotAllowed);
        client.shutdown().await?;
        assert_eq!(
            source.finish().await?.len(),
            1,
            "{delivery:?} resent initial body"
        );
        assert_no_connection(&target);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_redirect_limit_counts_only_followed_hops() -> TestResult {
    for final_status in [StatusCode::OK, StatusCode::FOUND] {
        let source = Peer::start(
            false,
            vec![
                Reply::redirect(StatusCode::FOUND, "/first"),
                Reply {
                    status: final_status,
                    location: Some("/second".into()),
                },
                Reply::ok(),
            ],
        )
        .await?;
        let client = HttpClient::new(
            config(&source, Provider::None)?
                .with_redirect_policy(RedirectPolicy::same_origin().with_max_redirects(1)?),
            "redirect-limit",
        )?;
        let result = client.get("/source").await;
        if final_status == StatusCode::OK {
            assert_eq!(result?.status, StatusCode::OK);
        } else {
            assert_rejected(
                &result.expect_err("hop limit ignored"),
                HttpRedirectError::TooManyRedirects,
            );
        }
        client.shutdown().await?;
        assert_eq!(source.finish().await?.len(), 2);
    }
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

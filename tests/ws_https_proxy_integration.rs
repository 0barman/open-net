//! T05: authenticated CONNECT runs inside a separately verified proxy TLS layer.
#![cfg(feature = "ws-client")]

#[path = "support/tls.rs"]
mod tls_support;

use futures::{SinkExt, StreamExt};
use open_net::{
    error::{ErrorKind, ErrorStage},
    network::{
        ClientIdentity, NetworkConfig, ProxyBasicAuth, ProxyConfig, RootCertificateMode, TlsConfig,
    },
    ws::{ConnectOptions, InitialMessages, Message, ReconnectPolicy},
    HeaderMap, HeaderName, HeaderValue, NetError, OpenNet,
};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use std::{error::Error, future::Future, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::Message as WireMessage;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const BUDGET: Duration = Duration::from_secs(5);
// Exact Base64 encoding of the fixture's "proxy-user:proxy-password".
const BASIC_HEADER: &str = "Proxy-Authorization: Basic cHJveHktdXNlcjpwcm94eS1wYXNzd29yZA==";
const TARGET_CA: &[u8] = include_bytes!("../src/test_fixtures/network/ca.pem");
const TARGET_CERT: &[u8] = include_bytes!("fixtures/network/server.pem");
const TARGET_KEY: &[u8] = include_bytes!("fixtures/network/server-key.pem");
const TARGET_CLIENT: &[u8] = include_bytes!("../src/test_fixtures/network/client.pem");
const TARGET_CLIENT_KEY: &[u8] = include_bytes!("../src/test_fixtures/network/client-key.pem");
const PROXY_CA: &[u8] = include_bytes!("support/https_proxy/ca.pem");
const PROXY_CERT: &[u8] = include_bytes!("support/https_proxy/server.pem");
const PROXY_KEY: &[u8] = include_bytes!("support/https_proxy/server-key.pem");
const PROXY_CLIENT_CA: &[u8] = include_bytes!("../src/test_fixtures/network/other-ca.pem");
const PROXY_CLIENT: &[u8] = include_bytes!("fixtures/network/other-client.pem");
const PROXY_CLIENT_KEY: &[u8] = include_bytes!("../src/test_fixtures/network/other-client-key.pem");

async fn bounded<T>(future: impl Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(BUDGET, future).await?)
}

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

struct Peer<T>(tokio::task::JoinHandle<TestResult<T>>);

impl<T> Peer<T> {
    async fn finish(mut self) -> TestResult<T> {
        bounded(&mut self.0).await??
    }
}

impl<T> Drop for Peer<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug, Default)]
struct Observation {
    rejected: bool,
    alpn: Option<Vec<u8>>,
    client_certificate: Option<Vec<u8>>,
    request: String,
    origin_auth: bool,
    proxy_auth: bool,
}

fn server_config(
    cert: &[u8],
    key: &[u8],
    client_ca: Option<&[u8]>,
    version: &'static rustls::SupportedProtocolVersion,
) -> TestResult<rustls::ServerConfig> {
    let provider = Arc::new(rustls_openssl::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[version])?;
    let builder = if let Some(client_ca) = client_ca {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from_pem_slice(client_ca)?)?;
        builder.with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()?,
        )
    } else {
        builder.with_no_client_auth()
    };
    let mut config = builder.with_single_cert(
        CertificateDer::pem_slice_iter(cert).collect::<Result<Vec<_>, _>>()?,
        PrivateKeyDer::from_pem_slice(key)?,
    )?;
    // The proxy must only offer HTTP/1.1 despite our server preferring HTTP/2.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

fn tls_config(ca: &[u8], identity: Option<(&[u8], &[u8])>) -> TestResult<TlsConfig> {
    let mut config = TlsConfig::default()
        .with_crypto_provider(Arc::new(rustls_openssl::default_provider()))
        .with_root_certificates(ca, RootCertificateMode::Replace)?;
    if let Some((cert, key)) = identity {
        config = config.with_client_identity(ClientIdentity::from_pem(cert, key)?);
    }
    Ok(config)
}

fn proxy_config(address: SocketAddr, tls: TlsConfig, auth: bool) -> TestResult<ProxyConfig> {
    Ok(ProxyConfig::https_connect(
        &format!("https://localhost:{}", address.port()),
        if auth {
            Some(ProxyBasicAuth::new("proxy-user", "proxy-password")?)
        } else {
            None
        },
        tls,
    )?)
}

fn options(url: String) -> ConnectOptions {
    let mut options = ConnectOptions::new(url);
    options.reconnect = ReconnectPolicy::Disabled;
    options.handshake_timeout = Duration::from_secs(2);
    options.connect_timeout = Some(Duration::from_secs(3));
    options.initial_messages = InitialMessages::Buffer(Default::default());
    options.headers = HeaderMap::from_iter([
        (
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer target-only-token"),
        ),
        (
            HeaderName::from_static("proxy-authorization"),
            HeaderValue::from_static("must-never-reach-target"),
        ),
    ]);
    options
}

fn has_exact_basic(request: &str) -> bool {
    request
        .lines()
        .filter(|line| line.eq(&BASIC_HEADER))
        .count()
        == 1
        && request
            .lines()
            .filter(|line| {
                line.to_ascii_lowercase()
                    .starts_with("proxy-authorization:")
            })
            .count()
            == 1
}

async fn read_headers(stream: &mut (impl AsyncRead + Unpin)) -> TestResult<String> {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        if request.len() == 16 * 1024 {
            return Err("fixture CONNECT header exceeded its bound".into());
        }
        request.push(stream.read_u8().await?);
    }
    Ok(String::from_utf8(request)?)
}

async fn start_proxy(
    config: rustls::ServerConfig,
    target: Option<SocketAddr>,
    response: &'static [u8],
) -> TestResult<(SocketAddr, Peer<Observation>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let peer = Peer(tokio::spawn(async move {
        bounded(async {
            let (socket, _) = listener.accept().await?;
            let mut stream = match TlsAcceptor::from(Arc::new(config))
                .accept(socket)
                .into_fallible()
                .await
            {
                Ok(stream) => stream,
                Err((_, mut socket)) => {
                    tls_support::close_rejected_connection(&mut socket).await?;
                    return Ok(Observation {
                        rejected: true,
                        ..Observation::default()
                    });
                }
            };
            let connection = stream.get_ref().1;
            let mut observed = Observation {
                alpn: connection.alpn_protocol().map(Vec::from),
                client_certificate: connection
                    .peer_certificates()
                    .and_then(|certificates| certificates.first())
                    .map(|certificate| certificate.as_ref().to_vec()),
                ..Observation::default()
            };
            observed.request = read_headers(&mut stream).await?;
            // One TLS write contains both interim responses and the final head:
            // parsing must preserve bytes buffered beyond each response.
            stream.write_all(response).await?;
            stream.flush().await?;
            if let Some(target) = target {
                let mut target = TcpStream::connect(target).await?;
                let _ = tokio::io::copy_bidirectional(&mut stream, &mut target).await;
            } else {
                let _ = stream.shutdown().await;
                let _ = tokio::io::copy(&mut stream, &mut tokio::io::sink()).await;
            }
            Ok(observed)
        })
        .await?
    }));
    Ok((address, peer))
}

async fn serve_echo(
    stream: impl AsyncRead + AsyncWrite + Unpin,
    mut observation: Observation,
) -> TestResult<Observation> {
    let mut websocket = tokio_tungstenite::accept_hdr_async(
        stream,
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
            observation.origin_auth = request
                .headers()
                .get("authorization")
                .is_some_and(|value| value == "Bearer target-only-token");
            observation.proxy_auth = request.headers().contains_key("proxy-authorization");
            Ok(response)
        },
    )
    .await?;
    while let Some(message) = websocket.next().await {
        match message? {
            WireMessage::Text(text) => websocket.send(WireMessage::Text(text)).await?,
            WireMessage::Close(_) => {
                websocket.flush().await?;
                break;
            }
            _ => {}
        }
    }
    Ok(observation)
}

async fn start_target(
    config: Option<rustls::ServerConfig>,
) -> TestResult<(SocketAddr, Peer<Observation>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let peer = Peer(tokio::spawn(async move {
        bounded(async {
            let (socket, _) = listener.accept().await?;
            if let Some(config) = config {
                let stream = match TlsAcceptor::from(Arc::new(config))
                    .accept(socket)
                    .into_fallible()
                    .await
                {
                    Ok(stream) => stream,
                    Err((_, mut socket)) => {
                        tls_support::close_rejected_connection(&mut socket).await?;
                        return Ok(Observation {
                            rejected: true,
                            ..Observation::default()
                        });
                    }
                };
                let connection = stream.get_ref().1;
                let observation = Observation {
                    alpn: connection.alpn_protocol().map(Vec::from),
                    client_certificate: connection
                        .peer_certificates()
                        .and_then(|certificates| certificates.first())
                        .map(|certificate| certificate.as_ref().to_vec()),
                    ..Observation::default()
                };
                serve_echo(stream, observation).await
            } else {
                serve_echo(socket, Observation::default()).await
            }
        })
        .await?
    }));
    Ok((address, peer))
}

fn check_error(error: &NetError, kind: ErrorKind, stage: ErrorStage) -> TestResult {
    if error.kind() != kind || error.context().stage != Some(stage) {
        return Err(format!("unexpected failure, expected {kind:?}/{stage:?}: {error:?}").into());
    }
    Ok(())
}

async fn connection_error(network: NetworkConfig, url: String) -> TestResult<NetError> {
    let engine = OpenNet::new_with_network_config(network)?;
    let client = bounded(engine.create_ws_client("https-proxy-failure")).await??;
    let result = bounded(client.connect(options(url))).await?;
    bounded(client.shutdown()).await??;
    bounded(engine.destroy_ws_client("https-proxy-failure")).await??;
    result
        .err()
        .ok_or_else(|| "connection unexpectedly succeeded".into())
}

#[tokio::test]
async fn https_connect_supports_ws_wss_and_separates_both_tls_identities() -> TestResult {
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        for target_tls in [false, true] {
            for authenticated in [false, true] {
                let target_config = target_tls
                    .then(|| server_config(TARGET_CERT, TARGET_KEY, Some(TARGET_CA), version))
                    .transpose()?;
                let (target, target_peer) = start_target(target_config).await?;
                let (proxy, proxy_peer) = start_proxy(
                    server_config(PROXY_CERT, PROXY_KEY, Some(PROXY_CLIENT_CA), version)?,
                    Some(target),
                    b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 201 Tunnel Ready\r\nContent-Length: 9999\r\nTransfer-Encoding: chunked\r\n\r\n",
                )
                .await?;
                let network = NetworkConfig::default()
                    .with_tls(tls_config(
                        TARGET_CA,
                        Some((TARGET_CLIENT, TARGET_CLIENT_KEY)),
                    )?)
                    .with_proxy(proxy_config(
                        proxy,
                        tls_config(PROXY_CA, Some((PROXY_CLIENT, PROXY_CLIENT_KEY)))?,
                        authenticated,
                    )?);
                let engine = OpenNet::new_with_network_config(network)?;
                let client = bounded(engine.create_ws_client("https-proxy-roundtrip")).await??;
                let scheme = if target_tls { "wss" } else { "ws" };
                let mut session = bounded(client.connect(options(format!(
                    "{scheme}://{target}/target-only-path?target-only-query"
                ))))
                .await??;
                let mut messages = session.take_messages().ok_or("message receiver missing")?;
                bounded(session.sender().message("nested TLS echo").send()).await??;
                let received = bounded(messages.recv())
                    .await??
                    .ok_or("proxy echo closed before delivering its response")?;
                check(
                    received.message().and_then(Message::as_text) == Some("nested TLS echo"),
                    "nested TLS echo returned an unexpected payload",
                )?;
                bounded(session.close()).await??;
                bounded(client.shutdown()).await??;
                bounded(engine.destroy_ws_client("https-proxy-roundtrip")).await??;
                let proxy_observed = proxy_peer.finish().await?;
                let target_observed = target_peer.finish().await?;
                check(
                    proxy_observed.alpn.as_deref() == Some(b"http/1.1".as_slice()),
                    "proxy TLS negotiated an unexpected ALPN",
                )?;
                check(
                    proxy_observed.client_certificate.as_deref()
                        == Some(CertificateDer::from_pem_slice(PROXY_CLIENT)?.as_ref()),
                    "proxy received an incorrect client identity",
                )?;
                check(
                    proxy_observed
                        .request
                        .starts_with(&format!("CONNECT {target} HTTP/1.1\r\n")),
                    "proxy CONNECT changed the target authority",
                )?;
                if authenticated {
                    check(
                        has_exact_basic(&proxy_observed.request),
                        "proxy CONNECT did not contain exactly the configured Basic credentials",
                    )?;
                } else {
                    check(
                        !proxy_observed
                            .request
                            .to_ascii_lowercase()
                            .contains("proxy-authorization:"),
                        "unauthenticated proxy CONNECT included an authorization header",
                    )?;
                }
                check(
                    !proxy_observed.request.contains("target-only"),
                    "target authorization, path or query reached CONNECT",
                )?;
                check(
                    !proxy_observed.request.contains("must-never-reach-target"),
                    "origin request's proxy authorization reached CONNECT",
                )?;
                check(
                    target_observed.origin_auth,
                    "target authorization was missing",
                )?;
                check(
                    !target_observed.proxy_auth,
                    "proxy authorization reached the target",
                )?;
                check(
                    target_observed.alpn.is_none(),
                    "target TLS ALPN policy changed",
                )?;
                if target_tls {
                    check(
                        target_observed.client_certificate.as_deref()
                            == Some(CertificateDer::from_pem_slice(TARGET_CLIENT)?.as_ref()),
                        "target received an incorrect client identity",
                    )?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn proxy_certificate_failures_never_send_connect_or_basic() -> TestResult {
    for (cert, key, trusted_ca) in [
        (PROXY_CERT, PROXY_KEY, TARGET_CA),
        (
            include_bytes!("fixtures/network/wrong-host.pem").as_slice(),
            include_bytes!("fixtures/network/wrong-host-key.pem").as_slice(),
            TARGET_CA,
        ),
        (
            include_bytes!("fixtures/network/expired.pem").as_slice(),
            include_bytes!("fixtures/network/expired-key.pem").as_slice(),
            TARGET_CA,
        ),
    ] {
        let (proxy, peer) = start_proxy(
            server_config(cert, key, None, &rustls::version::TLS13)?,
            None,
            b"HTTP/1.1 200 Connected\r\n\r\n",
        )
        .await?;
        let network = NetworkConfig::default()
            .with_tls(tls_config(
                TARGET_CA,
                Some((TARGET_CLIENT, TARGET_CLIENT_KEY)),
            )?)
            .with_proxy(proxy_config(proxy, tls_config(trusted_ca, None)?, true)?);
        let error = connection_error(network, "wss://127.0.0.1:9/".to_owned()).await?;
        check_error(&error, ErrorKind::Tls, ErrorStage::ProxyTls)?;
        check(
            error.context().http_status.is_none(),
            "proxy TLS failure exposed an HTTP status",
        )?;
        let observed = peer.finish().await?;
        check(observed.rejected, "invalid proxy certificate was accepted")?;
        check(
            observed.request.is_empty(),
            "CONNECT reached an untrusted proxy",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn proxy_trust_is_not_implicitly_inherited_by_target_tls() -> TestResult {
    // The proxy and target deliberately use the same proxy-signed server cert.
    // Only the proxy trusts this CA; the target must still reject it.
    let (target, target_peer) = start_target(Some(server_config(
        PROXY_CERT,
        PROXY_KEY,
        None,
        &rustls::version::TLS13,
    )?))
    .await?;
    let (proxy, proxy_peer) = start_proxy(
        server_config(PROXY_CERT, PROXY_KEY, None, &rustls::version::TLS13)?,
        Some(target),
        b"HTTP/1.1 200 Connected\r\n\r\n",
    )
    .await?;
    let network = NetworkConfig::default()
        .with_tls(tls_config(TARGET_CA, None)?)
        .with_proxy(proxy_config(proxy, tls_config(PROXY_CA, None)?, true)?);
    let error = connection_error(network, format!("wss://{target}/")).await?;
    check_error(&error, ErrorKind::Tls, ErrorStage::Tls)?;
    check(
        error.context().http_status.is_none(),
        "target TLS failure exposed an HTTP status",
    )?;
    check(
        target_peer.finish().await?.rejected,
        "target inherited proxy trust roots",
    )?;
    check(
        has_exact_basic(&proxy_peer.finish().await?.request),
        "trusted proxy did not receive the configured Basic credentials",
    )?;
    Ok(())
}

#[tokio::test]
async fn tls13_delayed_proxy_client_certificate_rejection_retains_proxy_tls_stage() -> TestResult {
    // TLS 1.3 may finish on the client before the proxy rejects the missing
    // client identity. The alert is then read during CONNECT, not connect().
    let (proxy, peer) = start_proxy(
        server_config(
            PROXY_CERT,
            PROXY_KEY,
            Some(PROXY_CLIENT_CA),
            &rustls::version::TLS13,
        )?,
        None,
        b"HTTP/1.1 200 Connected\r\n\r\n",
    )
    .await?;
    let network = NetworkConfig::default()
        // This valid proxy identity is placed only in the target configuration.
        .with_tls(tls_config(
            TARGET_CA,
            Some((PROXY_CLIENT, PROXY_CLIENT_KEY)),
        )?)
        .with_proxy(proxy_config(proxy, tls_config(PROXY_CA, None)?, true)?);
    let error = connection_error(network, "wss://127.0.0.1:9/".to_owned()).await?;
    check_error(&error, ErrorKind::Tls, ErrorStage::ProxyTls)?;
    let observed = peer.finish().await?;
    check(
        observed.rejected,
        "proxy inherited the target client identity",
    )?;
    check(
        observed.request.is_empty(),
        "CONNECT reached a proxy that rejected mTLS",
    )?;
    Ok(())
}

#[tokio::test]
async fn tls13_delayed_target_client_certificate_rejection_retains_target_tls_stage() -> TestResult
{
    let (target, target_peer) = start_target(Some(server_config(
        TARGET_CERT,
        TARGET_KEY,
        Some(PROXY_CLIENT_CA),
        &rustls::version::TLS13,
    )?))
    .await?;
    let (proxy, proxy_peer) = start_proxy(
        server_config(
            PROXY_CERT,
            PROXY_KEY,
            Some(PROXY_CLIENT_CA),
            &rustls::version::TLS13,
        )?,
        Some(target),
        b"HTTP/1.1 200 Connected\r\n\r\n",
    )
    .await?;
    let network = NetworkConfig::default()
        .with_tls(tls_config(TARGET_CA, None)?)
        .with_proxy(proxy_config(
            proxy,
            tls_config(PROXY_CA, Some((PROXY_CLIENT, PROXY_CLIENT_KEY)))?,
            true,
        )?);
    let error = connection_error(network, format!("wss://{target}/")).await?;
    check_error(&error, ErrorKind::Tls, ErrorStage::Tls)?;
    check(
        target_peer.finish().await?.rejected,
        "target inherited the proxy client identity",
    )?;
    check(
        has_exact_basic(&proxy_peer.finish().await?.request),
        "authenticated proxy did not receive the configured Basic credentials",
    )?;
    Ok(())
}

#[tokio::test]
async fn https_proxy_407_retains_proxy_stage_and_status() -> TestResult {
    let (proxy, peer) = start_proxy(
        server_config(PROXY_CERT, PROXY_KEY, None, &rustls::version::TLS13)?,
        None,
        b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 999999\r\n\r\n",
    )
    .await?;
    let network = NetworkConfig::default()
        .with_tls(tls_config(TARGET_CA, None)?)
        .with_proxy(proxy_config(proxy, tls_config(PROXY_CA, None)?, true)?);
    let error = connection_error(network, "ws://127.0.0.1:9/".to_owned()).await?;
    check_error(&error, ErrorKind::HandshakeRejected, ErrorStage::Proxy)?;
    check(
        error.context().http_status.map(|status| status.as_u16()) == Some(407),
        "proxy rejection lost its HTTP 407 status",
    )?;
    check(
        has_exact_basic(&peer.finish().await?.request),
        "rejecting proxy did not receive the configured Basic credentials",
    )?;
    Ok(())
}

async fn start_stalled_proxy(
    after_tls: bool,
) -> TestResult<(SocketAddr, oneshot::Receiver<()>, Peer<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (entered, observed) = oneshot::channel();
    let config = server_config(PROXY_CERT, PROXY_KEY, None, &rustls::version::TLS13)?;
    let peer = Peer(tokio::spawn(async move {
        bounded(async {
            let (mut socket, _) = listener.accept().await?;
            if after_tls {
                let mut stream = TlsAcceptor::from(Arc::new(config)).accept(socket).await?;
                let request = read_headers(&mut stream).await?;
                check(
                    has_exact_basic(&request),
                    "stalled proxy received incorrect Basic credentials",
                )?;
                let _ = entered.send(());
                let _ = tokio::io::copy(&mut stream, &mut tokio::io::sink()).await;
            } else {
                // Consume ClientHello only; leave TLS handshake pending.
                let _ = socket.read_u8().await?;
                let _ = entered.send(());
                let _ = tokio::io::copy(&mut socket, &mut tokio::io::sink()).await;
            }
            Ok(())
        })
        .await?
    }));
    Ok((address, observed, peer))
}

#[tokio::test]
async fn proxy_tls_and_connect_timeouts_and_cancellation_release_the_socket() -> TestResult {
    for after_tls in [false, true] {
        for cancel in [false, true] {
            let (proxy, entered, peer) = start_stalled_proxy(after_tls).await?;
            let network = NetworkConfig::default()
                .with_tls(tls_config(TARGET_CA, None)?)
                .with_proxy(proxy_config(proxy, tls_config(PROXY_CA, None)?, true)?);
            let engine = OpenNet::new_with_network_config(network)?;
            let client = bounded(engine.create_ws_client("https-proxy-stall")).await??;
            let mut options = options("wss://127.0.0.1:9/".to_owned());
            options.handshake_timeout = if cancel {
                BUDGET
            } else {
                Duration::from_millis(300)
            };
            let session = bounded(client.start_session(options, None)).await??;
            bounded(entered).await??;
            if cancel {
                session.cancel();
                let error = bounded(session.closed())
                    .await?
                    .err()
                    .ok_or("cancelled session succeeded")?;
                check(
                    error.kind() == ErrorKind::Cancelled,
                    "session cancellation changed its error kind",
                )?;
            } else {
                let error = bounded(session.wait_connected())
                    .await?
                    .err()
                    .ok_or("stalled proxy connected")?;
                check_error(
                    &error,
                    ErrorKind::TimedOut,
                    if after_tls {
                        ErrorStage::Proxy
                    } else {
                        ErrorStage::ProxyTls
                    },
                )?;
                let _ = bounded(session.closed()).await?;
            }
            // Observe EOF before client shutdown; cleanup must be performed by
            // the expired/cancelled connection attempt itself.
            peer.finish().await?;
            bounded(client.shutdown()).await??;
            bounded(engine.destroy_ws_client("https-proxy-stall")).await??;
        }
    }
    Ok(())
}

async fn start_stalled_target(
    after_tls: bool,
) -> TestResult<(SocketAddr, oneshot::Receiver<()>, Peer<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (entered, observed) = oneshot::channel();
    let config = server_config(TARGET_CERT, TARGET_KEY, None, &rustls::version::TLS13)?;
    let peer = Peer(tokio::spawn(async move {
        bounded(async {
            let (mut socket, _) = listener.accept().await?;
            if after_tls {
                let mut stream = TlsAcceptor::from(Arc::new(config)).accept(socket).await?;
                let upgrade = read_headers(&mut stream).await?;
                check(
                    upgrade.starts_with("GET / HTTP/1.1\r\n"),
                    "target received an invalid Upgrade request",
                )?;
                check(
                    !upgrade
                        .to_ascii_lowercase()
                        .contains("proxy-authorization:"),
                    "proxy authorization reached the stalled target",
                )?;
                let _ = entered.send(());
                let _ = tokio::io::copy(&mut stream, &mut tokio::io::sink()).await;
            } else {
                // A target ClientHello reached us through the encrypted proxy
                // tunnel. Keep its handshake pending until the client closes.
                check(
                    socket.read_u8().await? == 0x16,
                    "target did not receive a TLS handshake record",
                )?;
                let _ = entered.send(());
                let _ = tokio::io::copy(&mut socket, &mut tokio::io::sink()).await;
            }
            Ok(())
        })
        .await?
    }));
    Ok((address, observed, peer))
}

#[tokio::test]
async fn proxy_tls_connect_and_target_tls_share_the_original_attempt_deadline() -> TestResult {
    let (target, target_entered, target_peer) = start_stalled_target(false).await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = listener.local_addr()?;
    let config = server_config(PROXY_CERT, PROXY_KEY, None, &rustls::version::TLS13)?;
    let proxy_peer = Peer(tokio::spawn(async move {
        bounded(async {
            let (socket, _) = listener.accept().await?;
            // Consume 400ms in the proxy TLS phase and another 400ms in CONNECT.
            // The target TLS phase must only receive the remaining attempt time.
            tokio::time::sleep(Duration::from_millis(400)).await;
            let mut stream = TlsAcceptor::from(Arc::new(config)).accept(socket).await?;
            let request = read_headers(&mut stream).await?;
            check(
                has_exact_basic(&request),
                "delayed proxy received incorrect Basic credentials",
            )?;
            tokio::time::sleep(Duration::from_millis(400)).await;
            stream.write_all(b"HTTP/1.1 200 Connected\r\n\r\n").await?;
            stream.flush().await?;
            let mut target = TcpStream::connect(target).await?;
            let _ = tokio::io::copy_bidirectional(&mut stream, &mut target).await;
            Ok(())
        })
        .await?
    }));
    let network = NetworkConfig::default()
        .with_tls(tls_config(TARGET_CA, None)?)
        .with_proxy(proxy_config(proxy, tls_config(PROXY_CA, None)?, true)?);
    let engine = OpenNet::new_with_network_config(network)?;
    let client = bounded(engine.create_ws_client("https-proxy-deadline")).await??;
    let mut options = options(format!("wss://{target}/"));
    options.handshake_timeout = Duration::from_millis(1200);
    // An outer logical-session timeout must not mask an accidentally restarted
    // handshake timeout at a later transport phase.
    options.connect_timeout = None;
    let started = std::time::Instant::now();
    let session = bounded(client.start_session(options, None)).await??;
    bounded(target_entered).await??;
    let error = bounded(session.wait_connected())
        .await?
        .err()
        .ok_or("stalled target TLS unexpectedly connected")?;
    let elapsed = started.elapsed();
    check_error(&error, ErrorKind::TimedOut, ErrorStage::Tls)?;
    // A timeout reset after even the first delayed phase would take >=1600ms.
    // Leave 300ms scheduling allowance over the original 1200ms deadline.
    check(
        elapsed < Duration::from_millis(1500),
        &format!("a transport phase restarted the original timeout: {elapsed:?}"),
    )?;
    let _ = bounded(session.closed()).await?;
    proxy_peer.finish().await?;
    target_peer.finish().await?;
    bounded(client.shutdown()).await??;
    bounded(engine.destroy_ws_client("https-proxy-deadline")).await??;
    Ok(())
}

#[tokio::test]
async fn cancellation_during_target_tls_or_upgrade_releases_both_proxy_and_target_sockets(
) -> TestResult {
    for after_tls in [false, true] {
        let (target, entered, target_peer) = start_stalled_target(after_tls).await?;
        let (proxy, proxy_peer) = start_proxy(
            server_config(PROXY_CERT, PROXY_KEY, None, &rustls::version::TLS13)?,
            Some(target),
            b"HTTP/1.1 200 Connected\r\n\r\n",
        )
        .await?;
        let network = NetworkConfig::default()
            .with_tls(tls_config(TARGET_CA, None)?)
            .with_proxy(proxy_config(proxy, tls_config(PROXY_CA, None)?, true)?);
        let engine = OpenNet::new_with_network_config(network)?;
        let client = bounded(engine.create_ws_client("https-proxy-target-cancel")).await??;
        let session =
            bounded(client.start_session(options(format!("wss://{target}/")), None)).await??;
        bounded(entered).await??;
        session.cancel();
        let error = bounded(session.closed())
            .await?
            .err()
            .ok_or("cancelled target handshake unexpectedly connected")?;
        check(
            error.kind() == ErrorKind::Cancelled,
            "target handshake cancellation changed its error kind",
        )?;
        // Both fixture tasks must observe closure before shutting down the
        // reusable SDK client, including the established outer proxy TLS layer.
        check(
            has_exact_basic(&proxy_peer.finish().await?.request),
            "cancelled tunnel's proxy received incorrect Basic credentials",
        )?;
        target_peer.finish().await?;
        bounded(client.shutdown()).await??;
        bounded(engine.destroy_ws_client("https-proxy-target-cancel")).await??;
    }
    Ok(())
}

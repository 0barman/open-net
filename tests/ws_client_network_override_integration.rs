#![cfg(feature = "ws-client")]

use futures::StreamExt;
use open_net::{
    ClientIdentity, ConnectionStatus, NetError, NetworkConfig, OpenNet, ProxyConfig,
    ReconnectPolicy, RootCertificateMode, TlsConfig, WebSocketClient, WebSocketClientConfig,
    WebSocketConnectOptions, WebSocketConnectionEventKind, WebSocketConnectionEvents,
    WebSocketContextConnectOptions,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const CA: &[u8] = include_bytes!("fixtures/network/ca.pem");
const SERVER: &[u8] = include_bytes!("fixtures/network/server.pem");
const SERVER_KEY: &[u8] = include_bytes!("fixtures/network/server-key.pem");
const CLIENT: &[u8] = include_bytes!("fixtures/network/client.pem");
const CLIENT_KEY: &[u8] = include_bytes!("fixtures/network/client-key.pem");

async fn bounded<T>(future: impl Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(Duration::from_secs(4), future).await?)
}

fn client_config() -> WebSocketClientConfig {
    WebSocketClientConfig {
        close_timeout: Duration::from_millis(30),
        ..WebSocketClientConfig::default()
    }
}

fn reconnect(retries: usize) -> ReconnectPolicy {
    ReconnectPolicy {
        enabled: retries > 0,
        max_retries: retries,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(1),
        max_elapsed: Some(Duration::from_secs(2)),
        handshake_timeout: Duration::from_secs(1),
    }
}

fn options(retries: usize) -> WebSocketConnectOptions {
    WebSocketConnectOptions {
        reconnect: reconnect(retries),
        ..WebSocketConnectOptions::default()
    }
}

fn proxy_config(address: SocketAddr) -> TestResult<NetworkConfig> {
    Ok(
        NetworkConfig::default().with_proxy(ProxyConfig::http_connect(
            &format!("http://{address}"),
            None,
        )?),
    )
}

fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find_map(|(key, value)| {
            if key.eq_ignore_ascii_case(name) {
                Some(value.trim())
            } else {
                None
            }
        })
}

async fn read_header(socket: &mut TcpStream) -> TestResult<String> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        if bytes.len() >= 16 * 1024 {
            return Err("test HTTP header exceeded its bound".into());
        }
        bytes.push(bounded(socket.read_u8()).await??);
    }
    Ok(String::from_utf8(bytes)?)
}

struct TaskGuard {
    task: JoinHandle<TestResult>,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Origin {
    address: SocketAddr,
    observed: mpsc::UnboundedReceiver<()>,
    close: mpsc::UnboundedSender<()>,
    _guard: TaskGuard,
}

impl Origin {
    async fn start(statuses: Vec<u16>) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (observe, observed) = mpsc::unbounded_channel();
        let (close, mut close_requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut sockets = Vec::new();
            let mut ordinal = 0usize;
            loop {
                let (mut socket, _) = tokio::select! {
                    close = close_requests.recv() => {
                        close.ok_or("test close channel ended")?;
                        sockets.clear();
                        continue;
                    }
                    accepted = listener.accept() => accepted?,
                };
                let request = read_header(&mut socket).await?;
                let status = statuses.get(ordinal).copied().unwrap_or(101);
                ordinal = ordinal.checked_add(1).ok_or("test ordinal overflow")?;
                if status == 101 {
                    let key =
                        header(&request, "sec-websocket-key").ok_or("test Upgrade has no key")?;
                    let response = format!(
                        "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n",
                        derive_accept_key(key.as_bytes())
                    );
                    socket.write_all(response.as_bytes()).await?;
                    sockets.push(socket);
                } else {
                    let response = format!(
                        "HTTP/1.1 {status} Fixture\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    socket.write_all(response.as_bytes()).await?;
                }
                observe.send(())?;
            }
        });
        Ok(Self {
            address,
            observed,
            close,
            _guard: TaskGuard { task },
        })
    }

    fn url(&self) -> String {
        format!("ws://{}/override-fixture", self.address)
    }

    async fn observe(&mut self) -> TestResult {
        bounded(self.observed.recv())
            .await?
            .ok_or("test origin stopped")?;
        Ok(())
    }
}

struct Proxy {
    address: SocketAddr,
    count: Arc<AtomicUsize>,
    _guard: TaskGuard,
}

impl Proxy {
    async fn start(target: SocketAddr) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let count = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::clone(&count);
        let task = tokio::spawn(async move {
            // Dropping this set when the parent is aborted also aborts all tunnels.
            let mut tunnels: JoinSet<TestResult> = JoinSet::new();
            loop {
                tokio::select! {
                    joined = tunnels.join_next(), if !tunnels.is_empty() => {
                        joined.ok_or("test tunnel set ended")???;
                    }
                    connection = listener.accept() => {
                        let (mut inbound, _) = connection?;
                        accepted.fetch_add(1, Ordering::SeqCst);
                        tunnels.spawn(async move {
                            let request = read_header(&mut inbound).await?;
                            if !request.starts_with(&format!("CONNECT {target} HTTP/1.1\r\n")) {
                                return Err("test proxy received an unexpected CONNECT authority".into());
                            }
                            let mut outbound = TcpStream::connect(target).await?;
                            inbound.write_all(b"HTTP/1.1 200 Connected\r\n\r\n").await?;
                            let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                            Ok(())
                        });
                    }
                }
            }
        });
        Ok(Self {
            address,
            count,
            _guard: TaskGuard { task },
        })
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

#[tokio::test]
async fn same_engine_routes_inherited_overridden_and_explicit_default_clients_independently(
) -> TestResult {
    let mut origin = Origin::start(Vec::new()).await?;
    let proxy_a = Proxy::start(origin.address).await?;
    let proxy_b = Proxy::start(origin.address).await?;
    let engine = OpenNet::new_with_network_config(proxy_config(proxy_a.address)?)?;
    let inherited = engine
        .create_ws_client_with_config("route-a", client_config())
        .await?;
    let overridden = engine
        .create_ws_client_with_network_config(
            "route-b",
            client_config(),
            proxy_config(proxy_b.address)?,
        )
        .await?;
    let direct = engine
        .create_ws_client_with_network_config(
            "route-direct",
            client_config(),
            NetworkConfig::default(),
        )
        .await?;
    let url = origin.url();
    let (a, b, direct_result) = bounded(async {
        tokio::join!(
            inherited.connect_with_options(&url, options(0)),
            overridden.connect_with_options(&url, options(0)),
            direct.connect_with_options(&url, options(0)),
        )
    })
    .await?;
    a?;
    b?;
    direct_result?;
    for _ in 0..3 {
        origin.observe().await?;
    }
    let routing = (proxy_a.count(), proxy_b.count());
    inherited.shutdown().await?;
    overridden.shutdown().await?;
    direct.shutdown().await?;
    for name in ["route-a", "route-b", "route-direct"] {
        engine.destroy_ws_client(name).await?;
    }
    if routing != (1, 1) {
        return Err(format!(
            "client routing differed: proxy A {}, proxy B {}; expected one each",
            routing.0, routing.1
        )
        .into());
    }
    Ok(())
}

async fn mtls_origin() -> TestResult<(String, TaskGuard)> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(CA)?)?;
    let server = Arc::new(
        rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::new(roots),
                    provider,
                )
                .build()?,
            )
            .with_single_cert(
                CertificateDer::pem_slice_iter(SERVER).collect::<Result<Vec<_>, _>>()?,
                PrivateKeyDer::from_pem_slice(SERVER_KEY)?,
            )?,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("wss://{}/identity-isolation", listener.local_addr()?);
    let task = tokio::spawn(async move {
        let mut connections: JoinSet<TestResult> = JoinSet::new();
        loop {
            tokio::select! {
                joined = connections.join_next(), if !connections.is_empty() => {
                    joined.ok_or("test TLS connection set ended")???;
                }
                accepted = listener.accept() => {
                    let (socket, _) = accepted?;
                    let server = Arc::clone(&server);
                    connections.spawn(async move {
                        let stream = match tokio_rustls::TlsAcceptor::from(server).accept(socket).await {
                            Ok(stream) => stream,
                            Err(_) => return Ok(()),
                        };
                        let mut ws = match tokio_tungstenite::accept_async(stream).await {
                            Ok(ws) => ws,
                            Err(_) => return Ok(()),
                        };
                        while let Some(message) = ws.next().await {
                            if message.is_err() || message.as_ref().is_ok_and(|message| message.is_close()) {
                                break;
                            }
                        }
                        Ok(())
                    });
                }
            }
        }
    });
    Ok((url, TaskGuard { task }))
}

fn trust_with_identity(ca: &[u8], identity: bool) -> TestResult<NetworkConfig> {
    let mut tls = TlsConfig::default().with_root_certificates(ca, RootCertificateMode::Only)?;
    if identity {
        tls = tls.with_client_identity(ClientIdentity::from_pem(CLIENT, CLIENT_KEY)?);
    }
    Ok(NetworkConfig::default().with_tls(tls))
}

#[tokio::test]
async fn explicit_client_defaults_replace_engine_ca_and_identity_without_cross_client_leaks(
) -> TestResult {
    let (url, _origin) = mtls_origin().await?;
    let engine = OpenNet::new_with_network_config(trust_with_identity(CA, true)?)?;
    let explicit_default = engine
        .create_ws_client_with_network_config(
            "default-trust",
            client_config(),
            NetworkConfig::default(),
        )
        .await?;
    let no_identity = engine
        .create_ws_client_with_network_config(
            "without-identity",
            client_config(),
            trust_with_identity(CA, false)?,
        )
        .await?;
    let other_ca = engine
        .create_ws_client_with_network_config(
            "other-trust",
            client_config(),
            trust_with_identity(include_bytes!("fixtures/network/other-ca.pem"), true)?,
        )
        .await?;
    // Creation after all overrides also detects accidental mutation of engine defaults.
    let inherited = engine
        .create_ws_client_with_config("inherited-identity", client_config())
        .await?;
    let (default_result, identity_result, other_ca_result, inherited_result) = bounded(async {
        tokio::join!(
            explicit_default.connect_with_options(&url, options(0)),
            no_identity.connect_with_options(&url, options(0)),
            other_ca.connect_with_options(&url, options(0)),
            inherited.connect_with_options(&url, options(0)),
        )
    })
    .await?;
    explicit_default.shutdown().await?;
    no_identity.shutdown().await?;
    other_ca.shutdown().await?;
    inherited.shutdown().await?;
    for name in [
        "default-trust",
        "without-identity",
        "other-trust",
        "inherited-identity",
    ] {
        engine.destroy_ws_client(name).await?;
    }
    inherited_result?;
    if default_result != Err(NetError::TlsConnectError)
        || identity_result != Err(NetError::TlsConnectError)
        || other_ca_result != Err(NetError::TlsConnectError)
    {
        return Err(
            "engine trust roots or client identity leaked into replacement client configuration"
                .into(),
        );
    }
    Ok(())
}

#[tokio::test]
async fn client_ca_and_mtls_override_enables_tls_without_changing_engine_defaults() -> TestResult {
    let (url, _origin) = mtls_origin().await?;
    let engine = OpenNet::new()?;
    let overridden = engine
        .create_ws_client_with_network_config(
            "enabled-client-identity",
            client_config(),
            trust_with_identity(CA, true)?,
        )
        .await?;
    let inherited = engine.create_ws_client("unchanged-default-trust").await?;
    let (accepted, rejected) = bounded(async {
        tokio::join!(
            overridden.connect_with_options(&url, options(0)),
            inherited.connect_with_options(&url, options(0)),
        )
    })
    .await?;
    bounded(overridden.shutdown()).await??;
    bounded(inherited.shutdown()).await??;
    engine.destroy_ws_client("enabled-client-identity").await?;
    engine.destroy_ws_client("unchanged-default-trust").await?;
    accepted?;
    if rejected != Err(NetError::TlsConnectError) {
        return Err("client-specific TLS roots mutated the engine defaults".into());
    }
    Ok(())
}

async fn wait_connected(client: &WebSocketClient) -> TestResult {
    bounded(async {
        loop {
            if client.connection_status() == ConnectionStatus::Connected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
}

async fn expect_context_established(events: &mut WebSocketConnectionEvents) -> TestResult {
    bounded(async {
        for _ in 0..8 {
            let event = events
                .recv()
                .await?
                .ok_or("context session ended before connection")?;
            if event.session_context_id() != 73 {
                return Err("test received another context session's event".into());
            }
            if event.kind() == WebSocketConnectionEventKind::Established {
                return Ok(());
            }
            if event.kind() == WebSocketConnectionEventKind::SessionTerminated {
                return Err("context session failed before connection".into());
            }
        }
        Err("context session did not establish within its event bound".into())
    })
    .await?
}

#[tokio::test]
async fn client_proxy_override_survives_retry_reconnect_reuse_and_context_sessions() -> TestResult {
    let mut origin = Origin::start(vec![503, 101]).await?;
    let proxy_a = Proxy::start(origin.address).await?;
    let proxy_b = Proxy::start(origin.address).await?;
    let engine = OpenNet::new_with_network_config(proxy_config(proxy_a.address)?)?;
    let client = engine
        .create_ws_client_with_network_config(
            "stable-proxy-b",
            client_config(),
            proxy_config(proxy_b.address)?,
        )
        .await?;
    let url = origin.url();

    bounded(client.connect_with_options(&url, options(1))).await??;
    origin.observe().await?;
    origin.observe().await?;
    if proxy_b.count() != 2 || proxy_a.count() != 0 {
        return Err("503 retry did not preserve the client proxy override".into());
    }

    origin.close.send(())?;
    origin.observe().await?;
    wait_connected(&client).await?;
    if proxy_b.count() != 3 || proxy_a.count() != 0 {
        return Err("physical reconnect did not preserve the client proxy override".into());
    }
    bounded(client.disconnect()).await??;

    bounded(client.connect_with_options(&url, options(0))).await??;
    origin.observe().await?;
    if proxy_b.count() != 4 || proxy_a.count() != 0 {
        return Err("explicit reconnect did not preserve the client proxy override".into());
    }
    bounded(client.disconnect()).await??;

    let context = WebSocketContextConnectOptions::new(&url, 73)
        .with_headers(Vec::new(), 74)
        .with_reconnect(reconnect(1));
    let mut events = bounded(client.start_connect_with_context(context)).await??;
    expect_context_established(&mut events).await?;
    origin.observe().await?;
    origin.close.send(())?;
    expect_context_established(&mut events).await?;
    origin.observe().await?;
    let final_routing = (proxy_a.count(), proxy_b.count());
    bounded(client.shutdown()).await??;
    engine.destroy_ws_client("stable-proxy-b").await?;
    if final_routing != (0, 6) {
        return Err(
            "context connection or context reconnect lost the client proxy override".into(),
        );
    }
    Ok(())
}

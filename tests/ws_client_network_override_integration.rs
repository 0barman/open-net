#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;
use open_net::ws::ConnectionEventKind;
use session::{session_options, SessionGuard};

#[path = "support/tls.rs"]
mod tls_support;

use futures::StreamExt;
use open_net::network::{
    ClientIdentity, NetworkConfig, ProxyConfig, RootCertificateMode, TlsConfig,
};
use open_net::ws::{ConnectOptions, ReconnectPolicy, WebSocketClientConfig};
use open_net::OpenNet;
use session::ObservedSession;

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
    {
        let mut config = WebSocketClientConfig::default();
        config.close_timeout = Duration::from_millis(30);
        config
    }
}

fn reconnect(retries: usize) -> ReconnectPolicy {
    if retries > 0 {
        ReconnectPolicy::Backoff(open_net::ws::BackoffConfig {
            max_retries: retries,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_elapsed: Some(Duration::from_secs(2)),
        })
    } else {
        ReconnectPolicy::Disabled
    }
}

fn options(url: &str, retries: usize) -> ConnectOptions {
    let mut connect_options = session_options(url, reconnect(retries));
    connect_options.handshake_timeout = Duration::from_secs(1);
    connect_options.connect_timeout = Some(Duration::from_secs(2));
    connect_options
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
            SessionGuard::establish(&inherited, options(&url, 0)),
            SessionGuard::establish(&overridden, options(&url, 0)),
            SessionGuard::establish(&direct, options(&url, 0)),
        )
    })
    .await?;
    let _session_a = a?;
    let _session_b = b?;
    let _session_direct_result = direct_result?;
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
        rustls::ServerConfig::builder_with_provider(provider.clone())
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
                        let stream = match tokio_rustls::TlsAcceptor::from(server)
                            .accept(socket)
                            .into_fallible()
                            .await
                        {
                            Ok(stream) => stream,
                            Err((_, mut socket)) => {
                                tls_support::close_rejected_connection(&mut socket).await?;
                                return Ok(());
                            }
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
    let mut tls = TlsConfig::default().with_root_certificates(ca, RootCertificateMode::Replace)?;
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
            SessionGuard::establish(&explicit_default, options(&url, 0)),
            SessionGuard::establish(&no_identity, options(&url, 0)),
            SessionGuard::establish(&other_ca, options(&url, 0)),
            SessionGuard::establish(&inherited, options(&url, 0)),
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
    inherited_result?.finish().await?;
    if default_result.err().as_ref().map(|error| error.kind())
        != Some(open_net::error::ErrorKind::Tls)
        || identity_result.err().as_ref().map(|error| error.kind())
            != Some(open_net::error::ErrorKind::Tls)
        || other_ca_result.err().as_ref().map(|error| error.kind())
            != Some(open_net::error::ErrorKind::Tls)
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
            SessionGuard::establish(&overridden, options(&url, 0)),
            SessionGuard::establish(&inherited, options(&url, 0)),
        )
    })
    .await?;
    bounded(overridden.shutdown()).await??;
    bounded(inherited.shutdown()).await??;
    engine.destroy_ws_client("enabled-client-identity").await?;
    engine.destroy_ws_client("unchanged-default-trust").await?;
    accepted?.finish().await?;
    if rejected.err().as_ref().map(|error| error.kind()) != Some(open_net::error::ErrorKind::Tls) {
        return Err("client-specific TLS roots mutated the engine defaults".into());
    }
    Ok(())
}

async fn expect_context_established(
    events: &mut ObservedSession,
    sequence: u64,
) -> TestResult<open_net::ws::ConnectionInfo> {
    bounded(async {
        let started = events.recv().await?.ok_or("context omitted Started")?;
        let ConnectionEventKind::AttemptStarted { attempt } = &started.kind else {
            return Err("context omitted Started".into());
        };
        let event = events
            .recv()
            .await?
            .ok_or("context ended before connection")?;
        let ConnectionEventKind::Established { connection } = &event.kind else {
            return Err("context did not establish".into());
        };
        if started.sequence != sequence
            || event.sequence != sequence + 1
            || event.session_id != attempt.session_id
            || event.client_id != attempt.client_id
            || connection.attempt_id != attempt.attempt_id
            || connection.cycle_id != attempt.cycle_id
        {
            return Err("context event sequence or actual attempt identity changed".into());
        }
        Ok(connection.clone())
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

    let _session = bounded(SessionGuard::establish(&client, options(&url, 1))).await??;
    origin.observe().await?;
    origin.observe().await?;
    if proxy_b.count() != 2 || proxy_a.count() != 0 {
        return Err("503 retry did not preserve the client proxy override".into());
    }

    origin.close.send(())?;
    origin.observe().await?;
    bounded(_session.session.wait_connected()).await??;
    if proxy_b.count() != 3 || proxy_a.count() != 0 {
        return Err("physical reconnect did not preserve the client proxy override".into());
    }
    bounded(_session.session.close()).await??;

    let _session = bounded(SessionGuard::establish(&client, options(&url, 0))).await??;
    origin.observe().await?;
    if proxy_b.count() != 4 || proxy_a.count() != 0 {
        return Err("explicit reconnect did not preserve the client proxy override".into());
    }
    bounded(_session.session.close()).await??;

    let context = {
        let mut connect_options = {
            let mut options = {
                let mut options = ConnectOptions::new(&url);
                options.headers = open_net::HeaderMap::new();
                options
            };
            options.reconnect = reconnect(1);
            options
        };
        connect_options.handshake_timeout = Duration::from_secs(1);
        connect_options.connect_timeout = Some(Duration::from_secs(2));
        connect_options
    };
    let mut events = bounded(session::observe(&client, context)).await??;
    let first = expect_context_established(&mut events, 1).await?;
    origin.observe().await?;
    origin.close.send(())?;
    let ended = bounded(events.recv())
        .await??
        .ok_or("reconnect omitted Disconnected")?;
    let ConnectionEventKind::Disconnected { connection, .. } = &ended.kind else {
        return Err("reconnect omitted Disconnected".into());
    };
    if ended.sequence != 3 || connection.connection_id != first.connection_id {
        return Err("reconnect changed original connection identity".into());
    }
    let second = expect_context_established(&mut events, 4).await?;
    if second.session_id != first.session_id
        || second.connection_id == first.connection_id
        || second.cycle_id == first.cycle_id
    {
        return Err("reconnect did not preserve session and renew connection".into());
    }
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

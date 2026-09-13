#![cfg(feature = "ws-client")]

use futures::StreamExt;
use open_net::{
    ClientIdentity, NetError, NetworkConfig, ProxyBasicAuth, ProxyConfig, RootCertificateMode,
    TlsConfig,
};
use open_net::{OpenNet, ReconnectPolicy, WebSocketConnectOptions};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::accept_async;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CA: &[u8] = include_bytes!("fixtures/network/ca.pem");
const SERVER: &[u8] = include_bytes!("fixtures/network/server.pem");
const SERVER_KEY: &[u8] = include_bytes!("fixtures/network/server-key.pem");
const CLIENT: &[u8] = include_bytes!("fixtures/network/client.pem");
const CLIENT_KEY: &[u8] = include_bytes!("fixtures/network/client-key.pem");

struct TaskGuard<T> {
    task: tokio::task::JoinHandle<T>,
}

impl<T> TaskGuard<T> {
    async fn finish(mut self) -> TestResult<T> {
        Ok(tokio::time::timeout(Duration::from_secs(4), &mut self.task).await??)
    }
}

impl<T> Drop for TaskGuard<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Debug)]
struct TlsObservation {
    accepted: bool,
    version: Option<rustls::ProtocolVersion>,
    alpn: Option<Vec<u8>>,
    full_handshake: bool,
    client_certificates: usize,
    origin_auth_received: bool,
    proxy_auth_received: bool,
}

fn tls_server_config(
    cert: &[u8],
    key: &[u8],
    mtls: bool,
    version: &'static rustls::SupportedProtocolVersion,
) -> TestResult<rustls::ServerConfig> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[version])?;
    let builder = if mtls {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from_pem_slice(CA)?)?;
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
    config.alpn_protocols = vec![b"http/1.1".to_vec(), b"h2".to_vec()];
    Ok(config)
}

async fn tls_peer(
    cert: &[u8],
    key: &[u8],
    mtls: bool,
    version: &'static rustls::SupportedProtocolVersion,
) -> TestResult<(std::net::SocketAddr, TaskGuard<TestResult<TlsObservation>>)> {
    let config = tls_server_config(cert, key, mtls, version)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let stream = match TlsAcceptor::from(Arc::new(config)).accept(socket).await {
            Ok(stream) => stream,
            Err(_) => {
                return Ok(TlsObservation {
                    accepted: false,
                    version: None,
                    alpn: None,
                    full_handshake: false,
                    client_certificates: 0,
                    origin_auth_received: false,
                    proxy_auth_received: false,
                });
            }
        };
        let session = &stream.get_ref().1;
        let mut observation = TlsObservation {
            accepted: true,
            version: session.protocol_version(),
            alpn: session.alpn_protocol().map(Vec::from),
            full_handshake: session.handshake_kind() == Some(rustls::HandshakeKind::Full),
            client_certificates: session.peer_certificates().map_or(0, |certs| certs.len()),
            origin_auth_received: false,
            proxy_auth_received: false,
        };
        let mut ws = tokio_tungstenite::accept_hdr_async(
            stream,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                observation.origin_auth_received = request.headers().contains_key("authorization");
                observation.proxy_auth_received =
                    request.headers().contains_key("proxy-authorization");
                Ok(response)
            },
        )
        .await?;
        while let Some(message) = ws.next().await {
            match message {
                Ok(message) if message.is_close() => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }
        Ok(observation)
    });
    Ok((address, TaskGuard { task }))
}

fn one_attempt() -> WebSocketConnectOptions {
    WebSocketConnectOptions {
        reconnect: ReconnectPolicy {
            enabled: false,
            handshake_timeout: Duration::from_secs(2),
            ..ReconnectPolicy::default()
        },
        ..WebSocketConnectOptions::default()
    }
}

fn trusted_config(mtls: bool) -> TestResult<NetworkConfig> {
    let mut tls = TlsConfig::default().with_root_certificates(CA, RootCertificateMode::Only)?;
    if mtls {
        tls = tls.with_client_identity(ClientIdentity::from_pem(CLIENT, CLIENT_KEY)?);
    }
    Ok(NetworkConfig::default().with_tls(tls))
}

#[tokio::test]
async fn custom_ca_and_mtls_connect_with_both_tls_versions_and_no_alpn() -> TestResult {
    for (version, expected) in [
        (&rustls::version::TLS12, rustls::ProtocolVersion::TLSv1_2),
        (&rustls::version::TLS13, rustls::ProtocolVersion::TLSv1_3),
    ] {
        for mtls in [false, true] {
            let (address, peer) = tls_peer(SERVER, SERVER_KEY, mtls, version).await?;
            let engine = OpenNet::new_with_network_config(trusted_config(mtls)?)?;
            let client = engine.create_ws_client("trusted-local-peer").await?;
            let connected = client
                .connect_with_options(&format!("wss://{address}/auth?scope=test"), one_attempt())
                .await;
            client.shutdown().await?;
            engine.destroy_ws_client("trusted-local-peer").await?;
            let observation = peer.finish().await??;
            connected?;
            if !observation.accepted
                || observation.version != Some(expected)
                || observation.alpn.is_some()
                || !observation.full_handshake
                || (mtls && observation.client_certificates == 0)
            {
                return Err(format!("TLS connection contract differed: {observation:?}").into());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn certificate_rejections_are_tls_errors_without_http_status() -> TestResult {
    for (cert, key, config) in [
        (SERVER, SERVER_KEY, NetworkConfig::default()),
        (
            include_bytes!("fixtures/network/wrong-host.pem").as_slice(),
            include_bytes!("fixtures/network/wrong-host-key.pem").as_slice(),
            trusted_config(false)?,
        ),
        (
            include_bytes!("fixtures/network/expired.pem").as_slice(),
            include_bytes!("fixtures/network/expired-key.pem").as_slice(),
            trusted_config(false)?,
        ),
        (
            SERVER,
            SERVER_KEY,
            NetworkConfig::default().with_tls(TlsConfig::default().with_root_certificates(
                include_bytes!("fixtures/network/other-ca.pem"),
                RootCertificateMode::Only,
            )?),
        ),
    ] {
        let (address, peer) = tls_peer(cert, key, false, &rustls::version::TLS13).await?;
        let engine = OpenNet::new_with_network_config(config)?;
        let client = engine.create_ws_client("certificate-rejection").await?;
        let result = client
            .connect_with_options(&format!("wss://{address}/"), one_attempt())
            .await;
        let status = client.last_handshake_http_status();
        client.shutdown().await?;
        engine.destroy_ws_client("certificate-rejection").await?;
        let observation = peer.finish().await??;
        if result != Err(NetError::TlsConnectError) || status.is_some() || observation.accepted {
            return Err(format!(
                "certificate rejection was not a terminal TLS failure: {result:?}, {status:?}"
            )
            .into());
        }
    }
    Ok(())
}

async fn proxy_peer(
    target: Option<std::net::SocketAddr>,
    response: &'static [u8],
) -> TestResult<(std::net::SocketAddr, TaskGuard<TestResult<String>>)> {
    use tokio::io::AsyncWriteExt;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let (mut inbound, _) = listener.accept().await?;
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            if request.len() >= 16 * 1024 {
                return Err("proxy request exceeded test bounds".into());
            }
            request.push(inbound.read_u8().await?);
        }
        inbound.write_all(response).await?;
        if let Some(target) = target {
            let mut outbound = tokio::net::TcpStream::connect(target).await?;
            let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
        }
        Ok(String::from_utf8(request)?)
    });
    Ok((address, TaskGuard { task }))
}

#[tokio::test]
async fn connect_proxy_carries_mtls_and_separates_proxy_credentials() -> TestResult {
    let (target, peer) = tls_peer(SERVER, SERVER_KEY, true, &rustls::version::TLS13).await?;
    let (proxy, tunnel) = proxy_peer(
        Some(target),
        b"HTTP/1.1 201 Tunnel Ready\r\nContent-Length: 9000\r\nTransfer-Encoding: chunked\r\n\r\n",
    )
    .await?;
    let config = trusted_config(true)?.with_proxy(ProxyConfig::http_connect(
        &format!("http://{proxy}"),
        Some(ProxyBasicAuth::new("test-user", "test-password")?),
    )?);
    let engine = OpenNet::new_with_network_config(config)?;
    let client = engine.create_ws_client("proxy-mtls").await?;
    let mut options = one_attempt();
    options.headers = vec![
        ("Authorization".into(), "Bearer source-test-token".into()),
        ("Proxy-Authorization".into(), "must-not-reach-origin".into()),
    ];
    let result = client
        .connect_with_options(
            &format!("wss://{target}/secret-test-path?secret-test-query"),
            options,
        )
        .await;
    client.shutdown().await?;
    engine.destroy_ws_client("proxy-mtls").await?;
    let observed = peer.finish().await??;
    let request = tunnel.finish().await??;
    result?;
    if !observed.accepted
        || observed.client_certificates == 0
        || !observed.origin_auth_received
        || observed.proxy_auth_received
        || !request.starts_with(&format!("CONNECT {target} HTTP/1.1\r\n"))
        || !request.contains("Proxy-Authorization: Basic ")
        || request.contains("source-test-token")
        || request.contains("secret-test")
    {
        return Err("proxy tunnel authentication or routing contract differed".into());
    }
    Ok(())
}

#[tokio::test]
async fn proxy_rejection_never_becomes_origin_handshake_status() -> TestResult {
    let (proxy, tunnel) =
        proxy_peer(None, b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n").await?;
    let engine = OpenNet::new_with_network_config(
        NetworkConfig::default()
            .with_proxy(ProxyConfig::http_connect(&format!("http://{proxy}"), None)?),
    )?;
    let client = engine.create_ws_client("proxy-rejection").await?;
    let result = client
        .connect_with_options("ws://127.0.0.1:9/", one_attempt())
        .await;
    let status = client.last_handshake_http_status();
    client.shutdown().await?;
    engine.destroy_ws_client("proxy-rejection").await?;
    let _ = tunnel.finish().await??;
    if result != Err(NetError::ConnectError) || status.is_some() {
        return Err(
            format!("proxy failure leaked into origin status: {result:?}, {status:?}").into(),
        );
    }
    Ok(())
}

#[test]
fn network_configuration_rejects_invalid_material_before_connecting() -> TestResult {
    use open_net::{ClientIdentity, ProxyBasicAuth, ProxyConfig, RootCertificateMode, TlsConfig};
    for (username, password) in [
        ("bad:name", "secret"),
        ("name", "bad\nsecret"),
        ("非ASCII", "secret"),
    ] {
        if ProxyBasicAuth::new(username, password).is_ok() {
            return Err("invalid proxy credentials were accepted".into());
        }
    }
    for url in [
        "https://localhost:443",
        "socks5://localhost:1080",
        "http://user:secret@localhost:8080",
        "http://localhost:8080/path",
    ] {
        if ProxyConfig::http_connect(url, None).is_ok() {
            return Err("unsupported proxy URL was accepted".into());
        }
    }
    if TlsConfig::default()
        .with_root_certificates([], RootCertificateMode::Only)
        .is_ok()
    {
        return Err("empty exclusive trust roots were accepted".into());
    }
    if TlsConfig::default()
        .with_root_certificates(b"not a certificate", RootCertificateMode::Append)
        .is_ok()
    {
        return Err("invalid CA material was accepted".into());
    }
    let cert = include_bytes!("fixtures/network/client.pem");
    let key = include_bytes!("fixtures/network/client-key.pem");
    ClientIdentity::from_pem(cert, key)?;
    if ClientIdentity::from_pem(cert, include_bytes!("fixtures/network/server-key.pem")).is_ok() {
        return Err("mismatched client key was accepted".into());
    }
    let duplicate = [key.as_slice(), key.as_slice()].concat();
    if ClientIdentity::from_pem(cert, &duplicate).is_ok() {
        return Err("multiple client private keys were accepted".into());
    }
    Ok(())
}

#[test]
fn proxy_urls_and_pem_bundles_are_validated_strictly() -> TestResult {
    for url in [
        "http://localhost:abc",
        "http://localhost:65536",
        "http://localhost:0",
        "http://localhost:80/#ignored-fragment",
        "http://localhost:80?ignored=query",
        "http://localhost:80@other.example",
        "http://[]:8080",
    ] {
        if ProxyConfig::http_connect(url, None).is_ok() {
            return Err("invalid proxy authority or unsupported URL component was accepted".into());
        }
    }
    for bundle in [
        [CA, b"trailing garbage"].concat(),
        [CA, CLIENT_KEY].concat(),
        b"-----BEGIN CERTIFICATE-----\nnot-base64!\n-----END CERTIFICATE-----\n".to_vec(),
    ] {
        if TlsConfig::default()
            .with_root_certificates(&bundle, RootCertificateMode::Append)
            .is_ok()
        {
            return Err("invalid or foreign PEM contents were ignored".into());
        }
    }
    if ClientIdentity::from_pem(CLIENT, [CLIENT_KEY, b"extra key data"].concat()).is_ok() {
        return Err("private key trailing data was ignored".into());
    }
    let rsa_label =
        String::from_utf8(CLIENT_KEY.to_vec())?.replace("PRIVATE KEY", "RSA PRIVATE KEY");
    if ClientIdentity::from_pem(CLIENT, rsa_label).is_ok() {
        return Err("unsupported PEM key format was accepted".into());
    }
    Ok(())
}

#[test]
fn root_certificates_reject_nested_pem_begin_markers() -> TestResult {
    let nested = [b"-----BEGIN CERTIFICATE-----\n".as_slice(), CA].concat();
    if TlsConfig::default()
        .with_root_certificates(nested, RootCertificateMode::Only)
        .is_ok()
    {
        return Err("nested CA PEM begin marker was accepted".into());
    }
    let foreign = [
        b"-----BEGIN CERTIFICATE-----\n".as_slice(),
        CLIENT_KEY,
        b"-----END CERTIFICATE-----\n".as_slice(),
    ]
    .concat();
    if TlsConfig::default()
        .with_root_certificates(foreign, RootCertificateMode::Only)
        .is_ok()
    {
        return Err("foreign nested CA PEM block was accepted".into());
    }
    Ok(())
}

#[test]
fn client_chain_rejects_nested_pem_begin_markers() -> TestResult {
    let nested = [b"-----BEGIN CERTIFICATE-----\n".as_slice(), CLIENT].concat();
    if ClientIdentity::from_pem(nested, CLIENT_KEY).is_ok() {
        return Err("nested client chain PEM begin marker was accepted".into());
    }
    Ok(())
}

#[test]
fn client_key_rejects_nested_pem_begin_markers() -> TestResult {
    let nested = [b"-----BEGIN PRIVATE KEY-----\n".as_slice(), CLIENT_KEY].concat();
    if ClientIdentity::from_pem(CLIENT, nested).is_ok() {
        return Err("nested PKCS8 PEM begin marker was accepted".into());
    }
    Ok(())
}

#[test]
fn config_debug_does_not_reveal_proxy_or_private_key_material() -> TestResult {
    let auth = ProxyBasicAuth::new("fixture-user-marker", "fixture-password-marker")?;
    let proxy = ProxyConfig::http_connect("http://fixture-proxy-marker:8080", Some(auth.clone()))?;
    let identity = ClientIdentity::from_pem(CLIENT, CLIENT_KEY)?;
    let config = NetworkConfig::default().with_proxy(proxy).with_tls(
        TlsConfig::default()
            .with_root_certificates(CA, RootCertificateMode::Only)?
            .with_client_identity(identity.clone()),
    );
    let debug = format!("{config:?} {auth:?} {identity:?}");
    for forbidden in [
        "fixture-user-marker",
        "fixture-password-marker",
        "fixture-proxy-marker",
        "PRIVATE KEY",
        "BEGIN CERTIFICATE",
    ] {
        if debug.contains(forbidden) {
            return Err("Debug exposed credential or certificate material".into());
        }
    }
    Ok(())
}

/// Exercises the real WSS path before any peer certificate is available. A missing
/// crypto provider used to terminate the connection task before ClientHello.
#[tokio::test]
async fn standalone_ws_writes_tls_client_hello_without_global_provider() -> TestResult {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Err("a different test installed a global TLS provider in this process".into());
    }
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let engine = OpenNet::new()?;
    let client = engine.create_ws_client("network-provider-probe").await?;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut record_type = [0u8; 1];
        socket.read_exact(&mut record_type).await?;
        if record_type.first() != Some(&22) {
            return Err("WSS peer did not receive a TLS handshake record".into());
        }
        TestResult::Ok(())
    });
    let connecting = client.clone();
    let attempt = tokio::spawn(async move {
        connecting
            .connect_with_options(
                &format!("wss://{address}/"),
                WebSocketConnectOptions {
                    reconnect: ReconnectPolicy {
                        enabled: false,
                        handshake_timeout: Duration::from_secs(2),
                        ..ReconnectPolicy::default()
                    },
                    ..WebSocketConnectOptions::default()
                },
            )
            .await
    });
    let observed = tokio::time::timeout(Duration::from_secs(3), peer).await;
    attempt.abort();
    let _ = attempt.await;
    client.shutdown().await?;
    engine.destroy_ws_client("network-provider-probe").await?;
    observed???;
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Err("open-net changed the process-global TLS provider".into());
    }
    Ok(())
}

#[tokio::test]
async fn mtls_rejects_missing_and_untrusted_client_certificates() -> TestResult {
    let other = ClientIdentity::from_pem(
        include_bytes!("fixtures/network/other-client.pem"),
        include_bytes!("fixtures/network/other-client-key.pem"),
    )?;
    for config in [
        trusted_config(false)?,
        NetworkConfig::default().with_tls(
            TlsConfig::default()
                .with_root_certificates(CA, RootCertificateMode::Only)?
                .with_client_identity(other),
        ),
    ] {
        let (address, peer) = tls_peer(SERVER, SERVER_KEY, true, &rustls::version::TLS13).await?;
        let engine = OpenNet::new_with_network_config(config)?;
        let client = engine.create_ws_client("mtls-rejection").await?;
        let connected = client
            .connect_with_options(&format!("wss://{address}/"), one_attempt())
            .await;
        client.shutdown().await?;
        engine.destroy_ws_client("mtls-rejection").await?;
        let observed = peer.finish().await??;
        if observed.accepted || connected != Err(NetError::TlsConnectError) {
            return Err(format!(
                "invalid mTLS identity was not a terminal TLS error: {connected:?}"
            )
            .into());
        }
    }
    Ok(())
}

#[tokio::test]
async fn plain_websocket_uses_connect_tunnel_with_source_headers_intact() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let target = listener.local_addr()?;
    let peer = TaskGuard {
        task: tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            let mut source_auth = false;
            let mut proxy_auth = false;
            let mut ws = tokio_tungstenite::accept_hdr_async(
                socket,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    source_auth = request.headers().contains_key("authorization");
                    proxy_auth = request.headers().contains_key("proxy-authorization");
                    Ok(response)
                },
            )
            .await?;
            while let Some(message) = ws.next().await {
                if message.is_err() || message.as_ref().is_ok_and(|message| message.is_close()) {
                    break;
                }
            }
            if !source_auth || proxy_auth {
                return Err("plain tunnel source header isolation failed".into());
            }
            TestResult::Ok(())
        }),
    };
    let (proxy, tunnel) = proxy_peer(Some(target), b"HTTP/1.1 200 Connected\r\n\r\n").await?;
    let engine = OpenNet::new_with_network_config(
        NetworkConfig::default()
            .with_proxy(ProxyConfig::http_connect(&format!("http://{proxy}"), None)?),
    )?;
    let client = engine.create_ws_client("plain-tunnel").await?;
    let mut options = one_attempt();
    options.headers = vec![
        ("Authorization".into(), "Bearer test-plain-token".into()),
        ("Proxy-Authorization".into(), "test-must-not-forward".into()),
    ];
    let connected = client
        .connect_with_options(&format!("ws://{target}/"), options)
        .await;
    client.shutdown().await?;
    engine.destroy_ws_client("plain-tunnel").await?;
    let request = tunnel.finish().await??;
    peer.finish().await??;
    connected?;
    if request.contains("test-plain-token") || request.contains("test-must-not-forward") {
        return Err("source headers leaked into CONNECT".into());
    }
    Ok(())
}

#[tokio::test]
async fn repeated_mtls_connections_do_not_start_resuming_sessions() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = Arc::new(tls_server_config(
        SERVER,
        SERVER_KEY,
        true,
        &rustls::version::TLS13,
    )?);
    let peer = TaskGuard {
        task: tokio::spawn(async move {
            for _ in 0..2 {
                let (socket, _) = listener.accept().await?;
                let stream = TlsAcceptor::from(Arc::clone(&server))
                    .accept(socket)
                    .await?;
                if stream.get_ref().1.handshake_kind() != Some(rustls::HandshakeKind::Full) {
                    return Err("reused network config unexpectedly resumed TLS".into());
                }
                let mut ws = accept_async(stream).await?;
                while let Some(message) = ws.next().await {
                    if message.is_err() || message.as_ref().is_ok_and(|message| message.is_close())
                    {
                        break;
                    }
                }
            }
            TestResult::Ok(())
        }),
    };
    let engine = OpenNet::new_with_network_config(trusted_config(true)?)?;
    let client = engine.create_ws_client("full-handshakes").await?;
    for _ in 0..2 {
        client
            .connect_with_options(&format!("wss://{address}/"), one_attempt())
            .await?;
        client.disconnect().await?;
    }
    client.shutdown().await?;
    engine.destroy_ws_client("full-handshakes").await?;
    peer.finish().await??;
    Ok(())
}

#[tokio::test]
async fn explicit_tls_provider_coexists_with_a_host_provider() -> TestResult {
    const CHILD: &str = "OPEN_NET_PROVIDER_ISOLATED_TEST";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "explicit_tls_provider_coexists_with_a_host_provider",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("HTTP_PROXY", "http://127.0.0.1:1")
            .env("HTTPS_PROXY", "http://127.0.0.1:1")
            .env("ALL_PROXY", "http://127.0.0.1:1")
            .env("NO_PROXY", "")
            .output()?;
        if !output.status.success() {
            return Err("isolated host-provider compatibility test failed".into());
        }
        return Ok(());
    }
    let mut host_provider = rustls::crypto::aws_lc_rs::default_provider();
    host_provider
        .cipher_suites
        .retain(|suite| suite.version() == &rustls::version::TLS13);
    host_provider
        .install_default()
        .map_err(|_| "test host provider was already installed")?;
    let (address, peer) = tls_peer(SERVER, SERVER_KEY, false, &rustls::version::TLS12).await?;
    let engine = OpenNet::new_with_network_config(trusted_config(false)?)?;
    let client = engine.create_ws_client("host-provider-coexistence").await?;
    let connected = client
        .connect_with_options(&format!("wss://{address}/"), one_attempt())
        .await;
    client.shutdown().await?;
    engine
        .destroy_ws_client("host-provider-coexistence")
        .await?;
    let observed = peer.finish().await??;
    connected?;
    if observed.version != Some(rustls::ProtocolVersion::TLSv1_2) {
        return Err("host provider replaced open-net's explicit protocol support".into());
    }
    Ok(())
}

#[tokio::test]
async fn simultaneous_engines_keep_default_and_appended_roots_isolated() -> TestResult {
    let (trusted_address, trusted_peer) =
        tls_peer(SERVER, SERVER_KEY, false, &rustls::version::TLS13).await?;
    let (default_address, default_peer) =
        tls_peer(SERVER, SERVER_KEY, false, &rustls::version::TLS13).await?;
    let configured =
        OpenNet::new_with_network_config(NetworkConfig::default().with_tls(
            TlsConfig::default().with_root_certificates(CA, RootCertificateMode::Append)?,
        ))?;
    let default = OpenNet::new()?;
    let trusted = configured.create_ws_client("isolated-trusted").await?;
    let untrusted = default.create_ws_client("isolated-default").await?;
    let trusted_url = format!("wss://{trusted_address}/");
    let default_url = format!("wss://{default_address}/");
    let (accepted, rejected) = tokio::join!(
        trusted.connect_with_options(&trusted_url, one_attempt()),
        untrusted.connect_with_options(&default_url, one_attempt())
    );
    trusted.shutdown().await?;
    untrusted.shutdown().await?;
    configured.destroy_ws_client("isolated-trusted").await?;
    default.destroy_ws_client("isolated-default").await?;
    let trusted_observation = trusted_peer.finish().await??;
    let default_observation = default_peer.finish().await??;
    accepted?;
    if rejected != Err(NetError::TlsConnectError)
        || !trusted_observation.accepted
        || default_observation.accepted
    {
        return Err("trust roots leaked between engine instances".into());
    }
    Ok(())
}

#[tokio::test]
async fn connect_proxy_mtls_retry_fetches_updated_token_for_second_upgrade() -> TestResult {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncWriteExt;
    use tokio_tungstenite::tungstenite::http::{Response, StatusCode};
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let target = listener.local_addr()?;
    let target_authority = format!("localhost:{}", target.port());
    let expected_origin_authority = target_authority.clone();
    let server = Arc::new(tls_server_config(
        SERVER,
        SERVER_KEY,
        true,
        &rustls::version::TLS13,
    )?);
    let peer = TaskGuard {
        task: tokio::spawn(async move {
            for attempt in 0..2 {
                let (socket, _) = listener.accept().await?;
                let tls = TlsAcceptor::from(Arc::clone(&server))
                    .accept(socket)
                    .await?;
                if tls.get_ref().1.server_name() != Some("localhost") {
                    return Err("proxy TLS retry changed the target SNI".into());
                }
                if tls
                    .get_ref()
                    .1
                    .peer_certificates()
                    .is_none_or(|certs| certs.is_empty())
                {
                    return Err("retry lost the mTLS identity".into());
                }
                let mut correct_token = false;
                let mut correct_host = false;
                let handshake = tokio_tungstenite::accept_hdr_async(
                    tls,
                    |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                     response| {
                        let wanted = if attempt == 0 {
                            "Bearer fixture-T1"
                        } else {
                            "Bearer fixture-T2"
                        };
                        correct_token = request
                            .headers()
                            .get("authorization")
                            .and_then(|header| header.to_str().ok())
                            == Some(wanted);
                        correct_host = request
                            .headers()
                            .get("host")
                            .and_then(|header| header.to_str().ok())
                            == Some(expected_origin_authority.as_str());
                        if attempt == 0 || !correct_token || !correct_host {
                            let mut rejected = Response::new(Some("fixture retry".to_owned()));
                            *rejected.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                            Err(rejected)
                        } else {
                            Ok(response)
                        }
                    },
                )
                .await;
                if !correct_token {
                    return Err("proxy TLS retry used an incorrect token".into());
                }
                if !correct_host {
                    return Err("proxy TLS retry changed the target Upgrade Host".into());
                }
                if attempt == 0 {
                    if handshake.is_ok() {
                        return Err("first Upgrade did not return the fixture 503".into());
                    }
                } else {
                    let mut ws = handshake?;
                    while let Some(message) = ws.next().await {
                        if message.is_err()
                            || message.as_ref().is_ok_and(|message| message.is_close())
                        {
                            break;
                        }
                    }
                }
            }
            TestResult::Ok(())
        }),
    };
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = proxy_listener.local_addr()?;
    let expected_proxy_authority = target_authority.clone();
    let tunnel = TaskGuard {
        task: tokio::spawn(async move {
            for _ in 0..2 {
                let (mut inbound, _) = proxy_listener.accept().await?;
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    if request.len() >= 16 * 1024 {
                        return Err("test proxy request exceeded its bound".into());
                    }
                    request.push(inbound.read_u8().await?);
                }
                let headers = String::from_utf8(request)?;
                if headers.contains("Bearer") {
                    return Err("source token reached the proxy CONNECT request".into());
                }
                if !headers.starts_with(&format!("CONNECT {expected_proxy_authority} HTTP/1.1\r\n"))
                    || !headers.contains(&format!("\r\nHost: {expected_proxy_authority}\r\n"))
                {
                    return Err("proxy retry changed the CONNECT target authority".into());
                }
                let mut outbound = tokio::net::TcpStream::connect(target).await?;
                inbound.write_all(b"HTTP/1.1 200 Connected\r\n\r\n").await?;
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
            }
            TestResult::Ok(())
        }),
    };
    let engine = OpenNet::new_with_network_config(trusted_config(true)?.with_proxy(
        ProxyConfig::http_connect(
            &format!("http://{proxy}"),
            Some(ProxyBasicAuth::new("fixture", "fixture")?),
        )?,
    ))?;
    let client = engine.create_ws_client("proxy-mtls-refresh").await?;
    let invocations = Arc::new(AtomicUsize::new(0));
    let provider_invocations = Arc::clone(&invocations);
    let mut options = one_attempt();
    options.reconnect.enabled = true;
    options.reconnect.max_retries = 1;
    options.reconnect.initial_delay = Duration::from_millis(1);
    options.reconnect.max_delay = Duration::from_millis(1);
    options.header_provider = Some(Arc::new(move || {
        let value = match provider_invocations.fetch_add(1, Ordering::SeqCst) {
            0 => "Bearer fixture-T1",
            1 => "Bearer fixture-T2",
            _ => return Err(NetError::ConfigError),
        };
        Ok(vec![("Authorization".to_owned(), value.to_owned())])
    }));
    let connected = tokio::time::timeout(
        Duration::from_secs(4),
        client.connect_with_options(&format!("wss://{target_authority}/"), options),
    )
    .await;
    client.shutdown().await?;
    engine.destroy_ws_client("proxy-mtls-refresh").await?;
    connected??;
    peer.finish().await??;
    tunnel.finish().await??;
    if invocations.load(Ordering::SeqCst) != 2 {
        return Err("retry did not fetch exactly one credential snapshot per attempt".into());
    }
    Ok(())
}

use futures_util::StreamExt;
use open_net::api::http::{HttpClient, HttpClientConfig, HttpTlsConfig};
use open_net::error::ErrorKind;
use open_net::network::{NetworkConfig, RootCertificateMode, TlsConfig};
use open_net::ws::ReconnectPolicy;
use open_net::OpenNet;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::LazyConfigAcceptor;

#[path = "../../../support/session.rs"]
mod session;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CA: &[u8] = include_bytes!("../../network/ca.pem");
const SERVER: &[u8] = include_bytes!("../../network/server.pem");
const SERVER_KEY: &[u8] = include_bytes!("../../network/server-key.pem");
const STEP_TIMEOUT: Duration = Duration::from_secs(5);

struct Peer {
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<TestResult>,
}

impl Peer {
    async fn finish(mut self) -> TestResult {
        tokio::time::timeout(STEP_TIMEOUT, &mut self.task).await???;
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A real loopback peer with an explicit, local provider. It checks ClientHello
/// and the negotiated session; no client-side config inspection substitutes for
/// handshake evidence. HTTP peers prefer h2, exposing accidental host-feature
/// leakage into the SDK's HTTP/1.1-only policy.
async fn tls_peer(
    http: bool,
    version: &'static rustls::SupportedProtocolVersion,
    expect_provider_rejection: bool,
) -> TestResult<Peer> {
    let mut config =
        rustls::ServerConfig::builder_with_provider(Arc::new(rustls_openssl::default_provider()))
            .with_protocol_versions(&[version])?
            .with_no_client_auth()
            .with_single_cert(
                CertificateDer::pem_slice_iter(SERVER).collect::<Result<Vec<_>, _>>()?,
                PrivateKeyDer::from_pem_slice(SERVER_KEY)?,
            )?;
    if http {
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    }
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let start = LazyConfigAcceptor::new(rustls::server::Acceptor::default(), socket).await?;
        let offered: Vec<Vec<u8>> = start
            .client_hello()
            .alpn()
            .into_iter()
            .flatten()
            .map(<[u8]>::to_vec)
            .collect();
        let expected_alpn: Vec<Vec<u8>> = if http {
            vec![b"http/1.1".to_vec()]
        } else {
            Vec::new()
        };
        if offered != expected_alpn {
            return Err(format!("unexpected ClientHello ALPN: {offered:?}").into());
        }
        let accepted = start.into_stream(Arc::new(config)).await;
        if expect_provider_rejection {
            let error = accepted
                .err()
                .ok_or("restricted provider accepted TLS 1.2")?;
            let tls_error = error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<rustls::Error>());
            if !matches!(
                tls_error,
                Some(rustls::Error::PeerIncompatible(
                    rustls::PeerIncompatible::NoCipherSuitesInCommon
                        | rustls::PeerIncompatible::Tls12NotOffered
                        | rustls::PeerIncompatible::Tls12NotOfferedOrEnabled
                ))
            ) {
                return Err(
                    format!("expected TLS 1.2 provider restriction, received {error:?}").into(),
                );
            }
            println!("handshake-rejected:TLSv1_2:host-restriction:{tls_error:?}");
            return Ok(());
        }
        let mut stream = accepted?;
        let connection = stream.get_ref().1;
        if connection.protocol_version() != Some(version.version) {
            return Err("peer negotiated the wrong TLS protocol version".into());
        }
        let expected_negotiated = http.then_some(b"http/1.1".as_slice());
        if connection.alpn_protocol() != expected_negotiated {
            return Err("peer negotiated an unexpected ALPN protocol".into());
        }
        println!(
            "handshake:{:?}:alpn={}",
            version.version,
            if http { "http/1.1" } else { "none" }
        );
        if http {
            let mut headers = Vec::new();
            while !headers.ends_with(b"\r\n\r\n") {
                if headers.len() >= 8192 {
                    return Err("HTTP request headers exceeded fixture limit".into());
                }
                headers.push(stream.read_u8().await?);
            }
            if !headers.starts_with(b"GET /provider-probe HTTP/1.1\r\n") {
                return Err(format!("unexpected HTTP request: {headers:?}").into());
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await?;
            stream.shutdown().await?;
        } else {
            let mut socket = tokio_tungstenite::accept_async(stream).await?;
            while let Some(message) = socket.next().await {
                if message?.is_close() {
                    break;
                }
            }
        }
        Ok(())
    });
    Ok(Peer { address, task })
}

/// Exercise the old connect_async default-connector branch before adding the
/// fixture CA for a full handshake. The peer stops after ClientHello: an I/O
/// error is expected, but missing-provider ambiguity remains a genuine panic.
async fn legacy_default_connector() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        if socket.read_u8().await? != 22 {
            return Err("legacy connector did not send a TLS handshake record".into());
        }
        Ok(())
    });
    let peer = Peer { address, task };
    if tokio_tungstenite::connect_async(format!("wss://{address}/"))
        .await
        .is_ok()
    {
        return Err("ClientHello-only peer unexpectedly completed a WSS handshake".into());
    }
    peer.finish().await
}

async fn legacy_wss() -> TestResult {
    legacy_default_connector().await?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(CA)?)?;
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let peer = tls_peer(false, &rustls::version::TLS13, false).await?;
    let (mut socket, _) = tokio_tungstenite::connect_async_tls_with_config(
        format!("wss://{}/", peer.address),
        None,
        false,
        Some(tokio_tungstenite::Connector::Rustls(Arc::new(config))),
    )
    .await?;
    socket.close(None).await?;
    peer.finish().await
}

async fn open_net_wss(
    version: &'static rustls::SupportedProtocolVersion,
    expect_provider_rejection: bool,
    explicit_provider: bool,
) -> TestResult {
    let peer = tls_peer(false, version, expect_provider_rejection).await?;
    let mut tls = TlsConfig::default().with_root_certificates(CA, RootCertificateMode::Replace)?;
    if explicit_provider {
        tls = tls.with_crypto_provider(Arc::new(rustls_openssl::default_provider()));
    }
    let config = NetworkConfig::default().with_tls(tls);
    let engine = OpenNet::new_with_network_config(config)?;
    let client = engine.create_ws_client("mixed-provider-consumer").await?;
    let connected = session::SessionGuard::establish(&client, {
        let mut connect_options = session::session_options(
            format!("wss://{}/", peer.address),
            ReconnectPolicy::Disabled,
        );
        connect_options.handshake_timeout = Duration::from_secs(3);
        connect_options
    })
    .await;
    client.shutdown().await?;
    engine.destroy_ws_client("mixed-provider-consumer").await?;
    if expect_provider_rejection {
        let error = match connected {
            Ok(_) => return Err("SDK WS ignored host TLS 1.3 restriction".into()),
            Err(error) => error,
        };
        if error.kind() != ErrorKind::Tls {
            return Err(format!("expected SDK TLS error, received {error:?}").into());
        }
        println!("sdk-ws-rejected:Tls");
    } else {
        connected?.finish().await?;
    }
    peer.finish().await
}

async fn sdk_https(
    version: &'static rustls::SupportedProtocolVersion,
    expect_provider_rejection: bool,
    explicit_provider: bool,
) -> TestResult {
    let peer = tls_peer(true, version, expect_provider_rejection).await?;
    let mut tls = HttpTlsConfig::default().with_root_certificates(CA, true)?;
    if explicit_provider {
        tls = tls.with_crypto_provider(Arc::new(rustls_openssl::default_provider()));
    }
    let client = HttpClient::new(
        HttpClientConfig::new(format!("https://{}/", peer.address))?
            .with_environment_proxy(false)
            .with_timeout(Duration::from_secs(3))?
            .with_tls(tls),
        "sdk-provider-probe",
    )?;
    let response = client.get("/provider-probe").await;
    client.shutdown().await?;
    if expect_provider_rejection {
        let error = response
            .err()
            .ok_or("SDK HTTP ignored host TLS 1.3 restriction")?;
        if error.kind() != ErrorKind::Tls {
            return Err(format!("expected SDK TLS error, received {error:?}").into());
        }
        println!("sdk-http-rejected:Tls");
    } else {
        let response = response?;
        if response.status().as_u16() != 200 || response.body().as_ref() != b"ok" {
            return Err("SDK HTTPS did not return the expected 200/ok response".into());
        }
        println!("sdk-http-response:200:ok");
    }
    peer.finish().await
}

async fn external_https() -> TestResult {
    let peer = tls_peer(true, &rustls::version::TLS13, false).await?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(CA)?)?;
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls_openssl::default_provider()));
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    let client = reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .no_proxy()
        .http1_only()
        .build()?;
    let body = client
        .get(format!("https://{}/provider-probe", peer.address))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    if body != "ok" {
        return Err(format!("unexpected external HTTPS body: {body:?}").into());
    }
    peer.finish().await
}

fn missing_sdk_provider() -> TestResult {
    if !matches!(OpenNet::new(), Err(error) if error.kind() == ErrorKind::InvalidConfig) {
        return Err("SDK WS did not return typed missing-provider configuration error".into());
    }
    if !matches!(HttpClient::new(HttpClientConfig::new("https://localhost")?, "missing-provider"),
        Err(error) if error.kind() == ErrorKind::InvalidConfig)
    {
        return Err("SDK HTTP did not return typed missing-provider configuration error".into());
    }
    println!("sdk-ws-missing-provider:InvalidConfig");
    println!("sdk-http-missing-provider:InvalidConfig");
    Ok(())
}

async fn run(order: &str) -> TestResult {
    let original = rustls::crypto::CryptoProvider::get_default().cloned();
    for step in order.split(',') {
        tokio::time::timeout(STEP_TIMEOUT, async {
            match step {
                "legacy" => legacy_wss().await,
                "missing-provider" => missing_sdk_provider(),
                "sdk-ws-explicit-tls13" => open_net_wss(&rustls::version::TLS13, false, true).await,
                "sdk-http-explicit-tls13" => sdk_https(&rustls::version::TLS13, false, true).await,
                "open-net" => open_net_wss(&rustls::version::TLS13, false, false).await,
                "sdk-http" => sdk_https(&rustls::version::TLS13, false, false).await,
                "http" => external_https().await,
                "sdk-http-tls12" => sdk_https(&rustls::version::TLS12, false, false).await,
                "sdk-http-reject-tls12" => sdk_https(&rustls::version::TLS12, true, false).await,
                "sdk-ws-tls12" => open_net_wss(&rustls::version::TLS12, false, false).await,
                "sdk-ws-reject-tls12" => open_net_wss(&rustls::version::TLS12, true, false).await,
                "sdk-http-explicit-tls12" => sdk_https(&rustls::version::TLS12, false, true).await,
                "sdk-ws-explicit-tls12" => open_net_wss(&rustls::version::TLS12, false, true).await,
                _ => Err(format!("unknown operation: {step}").into()),
            }
        })
        .await??;
        let unchanged = match (&original, rustls::crypto::CryptoProvider::get_default()) {
            (None, None) => true,
            (Some(before), Some(after)) => Arc::ptr_eq(before, after),
            _ => false,
        };
        if !unchanged {
            return Err(format!("{step} changed the process-default TLS provider").into());
        }
        println!("provider-unchanged:{step}");
        println!("completed:{step}");
    }
    Ok(())
}

fn main() -> TestResult {
    let mut arguments = std::env::args().skip(1);
    let provider = arguments
        .next()
        .ok_or("missing provider: openssl/custom/none")?;
    let order = arguments
        .next()
        .ok_or("missing comma-separated operation order")?;
    if arguments.next().is_some() {
        return Err("unexpected extra argument".into());
    }
    println!("requested-provider:{provider}");
    // Host initialization, before any runtime or TLS client. Filtering cipher
    // suites supplies an observable restriction that distinguishes honoring the
    // host's actual provider from merely preserving its global Arc pointer.
    let host_provider = match provider.as_str() {
        "openssl" => Some(rustls_openssl::default_provider()),
        "custom" => {
            let mut provider = rustls_openssl::default_provider();
            provider
                .cipher_suites
                .retain(|suite| suite.tls13().is_some());
            if provider.cipher_suites.is_empty() {
                return Err("custom fixture provider has no TLS 1.3 cipher suites".into());
            }
            Some(provider)
        }
        "none" => None,
        _ => return Err(format!("unknown provider: {provider}").into()),
    };
    if let Some(host_provider) = host_provider {
        host_provider
            .install_default()
            .map_err(|_| "TLS provider was already initialized before application startup")?;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async { tokio::time::timeout(Duration::from_secs(25), run(&order)).await? })
}

use futures_util::StreamExt;
use open_net::{NetworkConfig, OpenNet, ReconnectPolicy, RootCertificateMode, TlsConfig};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

#[path = "../../../support/session.rs"]
mod session;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CA: &[u8] = include_bytes!("../../network/ca.pem");
const SERVER: &[u8] = include_bytes!("../../network/server.pem");
const SERVER_KEY: &[u8] = include_bytes!("../../network/server-key.pem");

struct Peer {
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<TestResult>,
}

impl Peer {
    async fn finish(mut self) -> TestResult {
        tokio::time::timeout(Duration::from_secs(5), &mut self.task).await???;
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn tls_peer(http: bool) -> TestResult<Peer> {
    // The peer must not install the default that the consumer is testing.
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(
        CertificateDer::pem_slice_iter(SERVER).collect::<Result<Vec<_>, _>>()?,
        PrivateKeyDer::from_pem_slice(SERVER_KEY)?,
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let mut stream = TlsAcceptor::from(Arc::new(config)).accept(socket).await?;
        if http {
            let mut headers = Vec::new();
            while !headers.ends_with(b"\r\n\r\n") {
                if headers.len() >= 8192 {
                    return Err("HTTP request headers exceeded fixture limit".into());
                }
                headers.push(stream.read_u8().await?);
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

/// Exercise the exact old connect_async default-connector branch before adding
/// a fixture CA for the full handshake below. The peer deliberately stops after
/// ClientHello, so an I/O error is expected, but a missing-provider panic is not.
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
    // Same process-default builder used by tokio-tungstenite 0.28's connector;
    // only the root store differs, to permit a deterministic local TLS peer.
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let peer = tls_peer(false).await?;
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

async fn open_net_wss() -> TestResult {
    let peer = tls_peer(false).await?;
    let config = NetworkConfig::default()
        .with_tls(TlsConfig::default().with_root_certificates(CA, RootCertificateMode::Replace)?);
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
    connected?.finish().await?;
    peer.finish().await
}

async fn https() -> TestResult {
    let peer = tls_peer(true).await?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .add_root_certificate(reqwest::Certificate::from_pem(CA)?)
        .build()?;
    let body = client
        .get(format!("https://{}/", peer.address))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    if body != "ok" {
        return Err(format!("unexpected HTTPS body: {body:?}").into());
    }
    peer.finish().await
}

async fn run(order: &str) -> TestResult {
    let original = rustls::crypto::CryptoProvider::get_default().cloned();
    for step in order.split(',') {
        match step {
            "legacy" => legacy_wss().await?,
            "open-net" => open_net_wss().await?,
            "http" => https().await?,
            _ => return Err(format!("unknown operation: {step}").into()),
        }
        let unchanged = match (&original, rustls::crypto::CryptoProvider::get_default()) {
            (None, None) => true,
            (Some(before), Some(after)) => Arc::ptr_eq(before, after),
            _ => false,
        };
        if !unchanged {
            return Err(format!("{step} changed the process-default TLS provider").into());
        }
        println!("completed:{step}");
    }
    Ok(())
}

fn main() -> TestResult {
    let mut arguments = std::env::args().skip(1);
    let provider = arguments
        .next()
        .ok_or("missing provider: ring/aws-lc/none")?;
    let order = arguments
        .next()
        .ok_or("missing comma-separated operation order")?;
    if arguments.next().is_some() {
        return Err("unexpected extra argument".into());
    }
    println!("requested-provider:{provider}");
    // This is application initialization, before the runtime or any TLS client.
    // A library must not choose a process-global provider for its consumer.
    let host_provider = match provider.as_str() {
        "ring" => Some(rustls::crypto::ring::default_provider()),
        "aws-lc" => Some(rustls::crypto::aws_lc_rs::default_provider()),
        "none" => None, // Negative control: leave the legacy default ambiguous.
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
    runtime.block_on(async { tokio::time::timeout(Duration::from_secs(20), run(&order)).await? })
}

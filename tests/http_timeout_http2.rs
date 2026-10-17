#![cfg(feature = "http-client-http2")]

use bytes::Bytes;
use http::Response;
use open_net::api::error::{ErrorKind, ErrorStage};
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpProtocol, HttpRequest, HttpTimeouts, HttpTlsConfig,
    HttpVersionPolicy, TimeoutPhase,
};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const CA: &[u8] = include_bytes!("../src/test_fixtures/network/ca.pem");
const SERVER: &[u8] = include_bytes!("fixtures/network/server.pem");
const SERVER_KEY: &[u8] = include_bytes!("fixtures/network/server-key.pem");

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

struct Peer(tokio::task::JoinHandle<TestResult>);

impl Drop for Peer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn tls_acceptor() -> TestResult<TlsAcceptor> {
    let provider = Arc::new(rustls_openssl::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(
            CertificateDer::pem_slice_iter(SERVER).collect::<Result<Vec<_>, _>>()?,
            PrivateKeyDer::from_pem_slice(SERVER_KEY)?,
        )?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[tokio::test]
async fn unpolled_h2_timeout_resets_only_its_stream_and_preserves_connection() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let acceptor = tls_acceptor()?;
    let requests = Arc::new(AtomicUsize::new(0));
    let server_requests = requests.clone();
    let (reset_tx, reset_rx) = oneshot::channel();
    let peer = Peer(tokio::spawn(async move {
        // Only one TLS connection is served; all three requests must reuse it.
        let (socket, _) = listener.accept().await?;
        let tls = acceptor.accept(socket).await?;
        let mut connection = h2::server::handshake(tls).await?;
        let mut idle: Option<h2::SendStream<Bytes>> = None;
        let mut active: Option<h2::SendStream<Bytes>> = None;
        let mut reset_tx = Some(reset_tx);
        loop {
            tokio::select! {
                incoming = connection.accept() => {
                    let Some(incoming) = incoming else { break };
                    let (request, mut respond) = incoming?;
                    server_requests.fetch_add(1, Ordering::SeqCst);
                    match request.uri().path() {
                        "/idle" => {
                            idle = Some(respond.send_response(Response::new(()), false)?);
                        }
                        "/active" => {
                            let mut send = respond.send_response(Response::new(()), false)?;
                            send.send_data(Bytes::from_static(b"before-"), false)?;
                            active = Some(send);
                        }
                        "/after" => {
                            let mut send = respond.send_response(Response::new(()), false)?;
                            send.send_data(Bytes::from_static(b"reused"), true)?;
                            connection.graceful_shutdown();
                        }
                        _ => return Err(std::io::Error::other("unexpected HTTP/2 path").into()),
                    }
                }
                reset = async {
                    match idle.as_mut() {
                        Some(send) => futures_util::future::poll_fn(|cx| send.poll_reset(cx)).await,
                        None => std::future::pending().await,
                    }
                }, if reset_tx.is_some() => {
                    let reason = reset?;
                    check(reason == h2::Reason::CANCEL, "idle timeout did not reset its HTTP/2 stream")?;
                    idle = None;
                    let mut send = active.take().ok_or_else(|| {
                        std::io::Error::other("idle stream expired before concurrent stream opened")
                    })?;
                    send.send_data(Bytes::from_static(b"after"), true)?;
                    if let Some(sender) = reset_tx.take() {
                        let _ = sender.send(());
                    }
                }
            }
        }
        Ok(())
    }));
    let mut config = HttpClientConfig::new(format!("https://{address}"))?
        .with_environment_proxy(false)
        .with_tls(HttpTlsConfig::default().with_root_certificates(CA, true)?)
        .with_http_version_policy(HttpVersionPolicy::Http2Only)
        .with_connect_timeout(Duration::from_secs(1))?
        .with_timeouts(HttpTimeouts::new().without_attempt_timeout())?;
    config.request_queue_capacity = 2;
    let client = HttpClient::new(config, "h2-stream-timeout-isolation")?;
    let mut idle = client
        .stream(
            HttpRequest::builder()
                .path("/idle")
                .timeouts(HttpTimeouts::new().with_idle_read_timeout(Duration::from_millis(500)))
                .build()?,
        )
        .await?;
    let idle_id = idle.request_id;
    let mut active = client
        .stream(HttpRequest::builder().path("/active").build()?)
        .await?;
    check(
        active.info().protocol == HttpProtocol::Http2,
        "concurrent request did not negotiate HTTP/2",
    )?;
    let first = active
        .next_chunk()
        .await
        .ok_or_else(|| std::io::Error::other("active stream ended before timeout"))??;
    check(first.as_ref() == b"before-", "active first chunk changed")?;
    // The idle handle is intentionally never polled before the server sees RST.
    tokio::time::timeout(Duration::from_secs(5), reset_rx).await??;
    let last = active
        .next_chunk()
        .await
        .ok_or_else(|| std::io::Error::other("active stream was terminated with its sibling"))??;
    check(
        last.as_ref() == b"after",
        "active stream failed after sibling timeout",
    )?;
    check(
        active.next_chunk().await.is_none(),
        "active stream did not finish",
    )?;
    let after = tokio::time::timeout(Duration::from_secs(5), client.get("/after")).await??;
    check(
        after.body.as_ref() == b"reused" && after.protocol() == HttpProtocol::Http2,
        "HTTP/2 connection was not reusable after stream timeout",
    )?;
    tokio::time::timeout(Duration::from_secs(5), client.drain()).await??;
    check(
        matches!(idle.next_chunk().await, Some(Err(ref error))
            if error.kind() == ErrorKind::TimedOut
            && error.timeout_phase() == Some(TimeoutPhase::IdleRead)
            && error.context().http_request_id == Some(idle_id)
            && error.context().http_attempt == Some(1)),
        "unpolled HTTP/2 terminal timeout lost its metadata",
    )?;
    check(
        idle.next_chunk().await.is_none(),
        "idle terminal error was not fused",
    )?;
    check(
        requests.load(Ordering::SeqCst) == 3,
        "requests used an unexpected connection",
    )?;
    client.shutdown().await?;
    drop(peer);
    Ok(())
}

#[tokio::test]
async fn http2_tls_stall_retains_typed_connect_phase() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (entered_tx, entered_rx) = oneshot::channel();
    let peer = Peer(tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut hello = [0_u8; 1024];
        check(
            socket.read(&mut hello).await? > 0,
            "client TLS hello missing",
        )?;
        let _ = entered_tx.send(());
        std::future::pending::<TestResult>().await
    }));
    let config = HttpClientConfig::new(format!("https://{address}"))?
        .with_environment_proxy(false)
        .with_http_version_policy(HttpVersionPolicy::Http2Only)
        .with_connect_timeout(Duration::from_millis(300))?
        .with_timeouts(HttpTimeouts::new().without_attempt_timeout())?;
    let client = HttpClient::new(config, "h2-connect-timeout")?;
    let request = client.get("/");
    tokio::pin!(request);
    tokio::select! {
        entered = entered_rx => entered?,
        result = &mut request => {
            return Err(std::io::Error::other(format!("request ended before TLS stall: {:?}", result.err().map(|error| error.kind()))).into());
        }
    }
    let error = match tokio::time::timeout(Duration::from_secs(5), request).await? {
        Err(error) => error,
        Ok(_) => return Err(std::io::Error::other("stalled HTTP/2 TLS request succeeded").into()),
    };
    check(
        error.kind() == ErrorKind::TimedOut
            && error.timeout_phase() == Some(TimeoutPhase::Connect)
            && error.context().stage == Some(ErrorStage::Write)
            && error.context().http_request_id.is_some()
            && error.context().http_attempt == Some(1),
        "HTTP/2 connection timeout lost typed phase or correlation",
    )?;
    client.shutdown().await?;
    drop(peer);
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

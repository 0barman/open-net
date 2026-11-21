use http::{header, HeaderMap, HeaderName, HeaderValue};
use open_net::api::http::{HttpClientConfig, HttpTlsConfig};
use rustls::crypto::hash::{Context as HashContext, Hash};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use std::io;
use std::net::TcpListener as StdListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const TARGET: &str = "/objects/a%2Fb%20c?part=one&part=two&sig=a%2Bb%3D";
pub const MIB: u64 = 1024 * 1024;
pub const WATCHDOG: Duration = Duration::from_secs(8);

pub fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message).into())
    }
}

fn sha256() -> TestResult<&'static dyn Hash> {
    let suite = rustls_openssl::cipher_suite::TLS13_AES_128_GCM_SHA256
        .tls13()
        .ok_or("missing SHA-256 test suite")?;
    Ok(suite.common.hash_provider)
}

pub fn digest() -> TestResult<Box<dyn HashContext>> {
    Ok(sha256()?.start())
}

pub struct Loopback {
    pub listener: TcpListener,
    backlog: StdListener,
}

impl Loopback {
    pub fn bind() -> TestResult<Self> {
        let backlog = StdListener::bind("127.0.0.1:0")?;
        backlog.set_nonblocking(true)?;
        let listener = TcpListener::from_std(backlog.try_clone()?)?;
        Ok(Self { listener, backlog })
    }

    pub fn config(&self, tls: bool) -> TestResult<HttpClientConfig> {
        let scheme = if tls { "https" } else { "http" };
        let origin = format!("{scheme}://{}", self.listener.local_addr()?);
        let tls = HttpTlsConfig::default()
            .with_crypto_provider(Arc::new(rustls_openssl::default_provider()))
            .with_root_certificates(
                include_bytes!("../../src/test_fixtures/network/ca.pem"),
                true,
            )?;
        Ok(HttpClientConfig::new(origin)?
            .with_environment_proxy(false)
            .with_tls(tls)
            .with_timeout(WATCHDOG)?)
    }

    /// The response closes the first connection. Any replay must open a new one.
    pub fn no_more_connections(&self) -> TestResult {
        match self.backlog.accept() {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(error) => Err(error.into()),
            Ok(_) => Err("unexpected additional HTTP connection".into()),
        }
    }
}

pub struct WireRequest {
    pub start_line: String,
    pub headers: HeaderMap,
    pub length: u64,
    pub digest: Vec<u8>,
}

pub struct PauseAfterHeaders {
    pub ready: oneshot::Sender<()>,
    pub resume: oneshot::Receiver<()>,
    pub received: Arc<AtomicU64>,
}

pub fn tls_acceptor(alpn: &[u8]) -> TestResult<TlsAcceptor> {
    let mut config =
        rustls::ServerConfig::builder_with_provider(Arc::new(rustls_openssl::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(
                CertificateDer::pem_slice_iter(include_bytes!("../fixtures/network/server.pem"))
                    .collect::<Result<Vec<_>, _>>()?,
                PrivateKeyDer::from_pem_slice(include_bytes!(
                    "../fixtures/network/server-key.pem"
                ))?,
            )?;
    config.alpn_protocols = vec![alpn.to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

pub async fn receive(
    peer: &Loopback,
    status: http::StatusCode,
    tls: bool,
    pause: Option<PauseAfterHeaders>,
) -> TestResult<WireRequest> {
    let (socket, _) = peer.listener.accept().await?;
    if tls {
        let stream = tls_acceptor(b"http/1.1")?.accept(socket).await?;
        exchange(stream, status, pause).await
    } else {
        exchange(socket, status, pause).await
    }
}

pub async fn read_head(socket: &mut (impl AsyncRead + Unpin)) -> TestResult<Vec<u8>> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        check(
            head.len() < 16 * 1024,
            "request headers exceed fixture limit",
        )?;
        head.push(socket.read_u8().await?);
    }
    Ok(head)
}

async fn exchange<S>(
    mut socket: S,
    status: http::StatusCode,
    pause: Option<PauseAfterHeaders>,
) -> TestResult<WireRequest>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let head = read_head(&mut socket).await?;
    let mut lines = std::str::from_utf8(&head)?.lines();
    let start_line = lines.next().ok_or("missing request line")?.to_owned();
    let mut headers = HeaderMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').ok_or("invalid request header")?;
        headers.append(
            HeaderName::from_bytes(name.as_bytes())?,
            HeaderValue::from_str(value.trim())?,
        );
    }
    let mut lengths = headers.get_all(header::CONTENT_LENGTH).iter();
    let length: u64 = match lengths.next() {
        Some(value) => value.to_str()?.parse()?,
        None if start_line.starts_with("GET ") => 0,
        None => return Err("missing upload Content-Length".into()),
    };
    check(lengths.next().is_none(), "duplicate Content-Length")?;
    check(
        !headers.contains_key(header::TRANSFER_ENCODING),
        "unexpected Transfer-Encoding",
    )?;
    let receiver_progress = if let Some(pause) = pause {
        pause
            .ready
            .send(())
            .map_err(|_| "missing backpressure observer")?;
        pause.resume.await?;
        Some(pause.received)
    } else {
        None
    };
    let mut hash = digest()?;
    let mut remaining = length;
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut consumed = 0_u64;
    let mut next_pause = 8 * MIB;
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(u64::try_from(buffer.len())?))?;
        let read = socket
            .read(
                buffer
                    .get_mut(..limit)
                    .ok_or("invalid fixture buffer length")?,
            )
            .await?;
        check(read > 0, "unexpected EOF in uploaded body")?;
        hash.update(buffer.get(..read).ok_or("invalid HTTP read length")?);
        remaining -= u64::try_from(read)?;
        consumed += u64::try_from(read)?;
        if let Some(received) = &receiver_progress {
            received.store(consumed, Ordering::Release);
            if consumed >= next_pause {
                next_pause += 8 * MIB;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
    }
    let location = if status.is_redirection() {
        "Location: /replay\r\n"
    } else {
        ""
    };
    let reply = format!(
        "HTTP/1.1 {} Reply\r\n{location}Content-Length: 2\r\nConnection: close\r\n\r\nok",
        status.as_u16()
    );
    socket.write_all(reply.as_bytes()).await?;
    socket.flush().await?;
    Ok(WireRequest {
        start_line,
        headers,
        length,
        digest: hash.finish().as_ref().to_vec(),
    })
}

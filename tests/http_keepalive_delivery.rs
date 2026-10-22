#![cfg(feature = "http-client-http2")]

//! Real TLS/H2 keepalive failures after the peer received an entire empty POST.
//! The raw frame peer stops acknowledging PING after the target request arrives;
//! using h2::server here would acknowledge it automatically. Peer evidence
//! establishes this fixture's delivery only, not every keepalive timeout's progress.

use open_net::api::error::{ErrorKind, NetError};
use open_net::api::http::{
    Http2Config, HttpClient, HttpClientConfig, HttpRequest, HttpRequestMethod, HttpRequestOptions,
    HttpRequestTrait, HttpResponseResult, HttpRetryPolicy, HttpTimeouts, HttpTlsConfig,
    HttpVersionPolicy, RequestSafety, RetryOn, TransportErrorKind,
};
use open_net::api::retry::{Backoff, RetryDecision, RetryEvent, RetrySchedule, RetryStopReason};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::{mpsc, Arc};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const WATCHDOG: Duration = Duration::from_secs(10);
const KEEPALIVE: Duration = Duration::from_millis(50);
const CA: &[u8] = include_bytes!("../src/test_fixtures/network/ca.pem");
const SERVER: &[u8] = include_bytes!("fixtures/network/server.pem");
const SERVER_KEY: &[u8] = include_bytes!("fixtures/network/server-key.pem");
const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[derive(Debug, Default)]
struct Evidence {
    completed_request_streams: Vec<u32>,
    unanswered_pings: usize,
}

struct Peer {
    address: SocketAddr,
    task: tokio::task::JoinHandle<TestResult<Vec<Evidence>>>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Peer {
    /// Accept one real TLS/H2 connection per expected attempt using only the
    /// fixture certificate; retain each connection's request/PING evidence.
    async fn start(attempts: u32, redirect: bool) -> TestResult<Self> {
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls_openssl::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(
            CertificateDer::pem_slice_iter(SERVER).collect::<Result<Vec<_>, _>>()?,
            PrivateKeyDer::from_pem_slice(SERVER_KEY)?,
        )?;
        tls.alpn_protocols = vec![b"h2".to_vec()];
        let acceptor = TlsAcceptor::from(Arc::new(tls));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let mut evidence = Vec::new();
            for _ in 0..attempts {
                let (socket, _) = listener.accept().await?;
                let stream = acceptor.accept(socket).await?;
                check(
                    stream.get_ref().1.alpn_protocol() == Some(b"h2"),
                    "fixture did not negotiate HTTP/2",
                )?;
                evidence.push(serve_connection(stream, redirect).await?);
            }
            Ok(evidence)
        });
        Ok(Self { address, task })
    }

    /// Join the peer after the client closes its timed-out connections. This
    /// watchdog fails a hanging test; it does not create an SDK timeout error.
    async fn finish(mut self) -> TestResult<Vec<Evidence>> {
        tokio::time::timeout(WATCHDOG, &mut self.task).await??
    }
}

async fn write_frame<S: AsyncWrite + Unpin>(
    stream: &mut S,
    kind: u8,
    flags: u8,
    id: u32,
    payload: &[u8],
) -> TestResult {
    let length = u32::try_from(payload.len())?.to_be_bytes();
    let mut header = [0_u8; 9];
    header[..3].copy_from_slice(&length[1..]);
    header[3] = kind;
    header[4] = flags;
    header[5..].copy_from_slice(&id.to_be_bytes());
    stream.write_all(&header).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;
    Ok(())
}

/// Exchange SETTINGS and optionally a 307, then withhold PING ACK only after
/// receiving the complete target request. Keep the socket open so the injected
/// failure is a keepalive timeout; the recorded frames describe this case only.
async fn serve_connection<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    redirect: bool,
) -> TestResult<Evidence> {
    let mut preface = [0_u8; 24];
    stream.read_exact(&mut preface).await?;
    check(&preface == PREFACE, "invalid HTTP/2 connection preface")?;
    write_frame(&mut stream, 4, 0, 0, &[]).await?; // SETTINGS
    let mut evidence = Evidence::default();
    loop {
        let mut header = [0_u8; 9];
        if let Err(error) = stream.read_exact(&mut header).await {
            // The peer holds the connection open until the client has timed
            // out. Closing it after receiving PING would test a reset instead.
            check(
                evidence.unanswered_pings > 0
                    && matches!(
                        error.kind(),
                        std::io::ErrorKind::UnexpectedEof
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::BrokenPipe
                    ),
                "connection ended before the keepalive fixture completed",
            )?;
            return Ok(evidence);
        }
        let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        check(length <= 16_384, "fixture frame exceeded its bounded size")?;
        let kind = header[3];
        let flags = header[4];
        let id = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        let mut payload = vec![0_u8; length];
        stream.read_exact(&mut payload).await?;
        match kind {
            4 => {
                check(id == 0, "SETTINGS used a request stream")?;
                if flags & 1 == 0 {
                    write_frame(&mut stream, 4, 1, 0, &[]).await?;
                }
            }
            1 => {
                // The small empty POST has one complete header block and no
                // DATA. END_HEADERS|END_STREAM is wire evidence of its end.
                check(flags == 5 && !payload.is_empty(), "incomplete empty POST")?;
                let ordinal = evidence.completed_request_streams.len();
                check(
                    id == 1 + 2 * u32::try_from(ordinal)?,
                    "unexpected request stream order",
                )?;
                evidence.completed_request_streams.push(id);
                if redirect && ordinal == 0 {
                    // HPACK literals: indexed name :status (8), value 307;
                    // indexed name location (46), value /blackhole. No dynamic
                    // table mutation is needed. END_STREAM closes the response.
                    let response = b"\x08\x03307\x0f\x1f\x0a/blackhole";
                    write_frame(&mut stream, 1, 5, id, response).await?;
                }
            }
            6 => {
                check(id == 0 && flags == 0 && length == 8, "unexpected PING")?;
                if evidence.completed_request_streams.len() == if redirect { 2 } else { 1 } {
                    evidence.unanswered_pings += 1;
                    // Deliberately do not send PING ACK or any target response.
                } else {
                    // A busy executor may allow a keepalive interval to elapse
                    // during the redirect hop. Only inject the failure after
                    // receiving the target request, regardless of scheduling.
                    write_frame(&mut stream, 6, 1, 0, &payload).await?;
                }
            }
            3 | 7 | 8 => {} // Client RST_STREAM, GOAWAY, or WINDOW_UPDATE.
            _ => return Err(std::io::Error::other("unexpected HTTP/2 frame").into()),
        }
    }
}

/// Verify the retained typed transport cause without parsing error text. This
/// proves a hyper timeout, while peer frames establish this fixture's progress.
fn hyper_timeout(error: &NetError) -> bool {
    let mut source = error.source();
    for _ in 0..32 {
        let Some(current) = source else { break };
        if current
            .downcast_ref::<hyper::Error>()
            .is_some_and(hyper::Error::is_timeout)
        {
            return true;
        }
        source = current.source();
    }
    false
}

#[derive(Clone, Copy, Debug)]
enum Entry {
    Buffered,
    Streaming,
    Redirect,
    NonReplayableCallback,
}

struct Callback(oneshot::Sender<HttpResponseResult>);

#[async_trait::async_trait]
impl HttpRequestTrait for Callback {
    fn get_path(&self) -> String {
        "/blackhole".to_owned()
    }

    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::POST
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let _ = self.0.send(result);
    }
}

/// Execute a real empty POST and compare public errors, retry reports, observer
/// events, and peer frames. SDK request deadlines are disabled; the outer
/// watchdog only bounds the test and cannot satisfy the error assertions.
async fn run_case(
    entry: Entry,
    reasons: RetryOn,
    safety: RequestSafety,
    attempts: u32,
    stop_reason: RetryStopReason,
) -> TestResult {
    let redirect = matches!(entry, Entry::Redirect);
    let peer = Peer::start(attempts, redirect).await?;
    let (events_tx, events_rx) = mpsc::channel();
    let policy = HttpRetryPolicy::builder()
        .schedule(
            RetrySchedule::builder()
                .max_attempts(3)
                .backoff(Backoff::None)
                .build()?,
        )
        .retry_on(reasons)
        .safety(safety)
        // Do not force retry or manufacture a transport classification.
        .classifier(|_| RetryDecision::Default)
        .observer(move |event: &RetryEvent| {
            let _ = events_tx.send(event.clone());
        })
        .build()?;
    let config = HttpClientConfig::new(format!("https://{}", peer.address))?
        .with_environment_proxy(false)
        .with_tls(HttpTlsConfig::default().with_root_certificates(CA, true)?)
        .with_http_version_policy(HttpVersionPolicy::Http2Only)
        .with_http2(Http2Config::new().keep_alive(KEEPALIVE, KEEPALIVE, false))?
        .with_connect_timeout(Duration::from_secs(3))?
        .with_timeouts(
            HttpTimeouts::new()
                .without_attempt_timeout()
                .without_headers_timeout()
                .without_idle_read_timeout()
                .without_total_timeout(),
        )?
        .with_default_retry_policy(policy);
    let client = HttpClient::new(config, "h2-keepalive-delivery")?;
    let request = HttpRequest::builder()
        .method(http::Method::POST)
        .path(if redirect { "/redirect" } else { "/blackhole" })
        .build()?;
    let result = tokio::time::timeout(WATCHDOG, async {
        let outcome = match entry {
            Entry::Streaming => client.stream(request).await.map(|_| ()),
            Entry::NonReplayableCallback => {
                let (sender, receiver) = oneshot::channel();
                client
                    .send_wait_with_options(
                        Callback(sender),
                        HttpRequestOptions::new().with_replayable_body(false),
                    )
                    .await
                    .map_err(|error| error.into_parts().1)?;
                receiver.await?.map(|_| ())
            }
            Entry::Buffered | Entry::Redirect => client.request(request).await.map(|_| ()),
        };
        Ok::<_, Box<dyn Error + Send + Sync>>(outcome)
    })
    .await??;
    let error = result
        .err()
        .ok_or_else(|| std::io::Error::other("silent peer returned a response"))?;
    check(
        error.kind() == ErrorKind::TimedOut,
        "keepalive lost its timeout kind",
    )?;
    check(
        error.context().stage.is_none(),
        "keepalive claimed a known read/write stage",
    )?;
    check(
        error.timeout_phase().is_none(),
        "keepalive claimed an SDK deadline",
    )?;
    check(
        hyper_timeout(&error),
        "failure did not retain a typed hyper timeout",
    )?;
    check(
        error.http_response_head().is_none(),
        "failed target acquired a response head",
    )?;
    let report = error
        .retry_report()
        .ok_or_else(|| std::io::Error::other("missing retry report"))?;
    check(
        report.attempts() == attempts && report.retry_count() == attempts - 1,
        "keepalive retry counts changed",
    )?;
    let sends = u64::from(attempts) * if redirect { 2 } else { 1 };
    check(
        report.transport_sends() == sends,
        "transport send count disagreed with hops",
    )?;
    check(
        report.stop_reason() == Some(stop_reason),
        &format!(
            "{entry:?}: expected {stop_reason:?}, received {:?}",
            report.stop_reason()
        ),
    )?;
    let events: Vec<_> = events_rx.try_iter().collect();
    check(
        events
            .iter()
            .filter(|event| matches!(event, RetryEvent::RetryScheduled { .. }))
            .count()
            == (attempts - 1) as usize,
        "retry observer disagreed with retry report",
    )?;
    let completed: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            RetryEvent::Completed(report) => Some(report),
            _ => None,
        })
        .collect();
    check(
        completed.len() == 1
            && completed[0].attempts() == attempts
            && completed[0].stop_reason() == Some(stop_reason),
        "observer completion disagreed",
    )?;
    client.shutdown().await?;
    let evidence = peer.finish().await?;
    check(
        evidence.len() == attempts as usize,
        "peer connection count disagreed",
    )?;
    for observed in evidence {
        check(
            observed.completed_request_streams == if redirect { vec![1, 3] } else { vec![1] }
                && observed.unanswered_pings > 0,
            "missing complete request then unanswered PING",
        )?;
    }
    Ok(())
}

/// Every execute entry must require explicit DeliveryUnknown selection for a
/// peer-confirmed complete POST whose connection subsequently misses PING ACK.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keepalive_timeout_requires_explicit_delivery_unknown_on_every_execute_entry() -> TestResult
{
    for entry in [Entry::Buffered, Entry::Streaming, Entry::Redirect] {
        for reasons in [
            RetryOn::none(),
            RetryOn::standard(),
            RetryOn::none().with_transport([TransportErrorKind::TimeoutBeforeSend]),
            RetryOn::none().with_transport([TransportErrorKind::ReadReset]),
            RetryOn::none().with_transport([TransportErrorKind::Write]),
        ] {
            run_case(
                entry,
                reasons,
                RequestSafety::IdempotentByContract,
                1,
                RetryStopReason::TimedOut,
            )
            .await?;
        }
        run_case(
            entry,
            RetryOn::none().with_transport([TransportErrorKind::DeliveryUnknown]),
            RequestSafety::IdempotentByContract,
            3,
            RetryStopReason::AttemptsExhausted,
        )
        .await?;
    }
    Ok(())
}

/// Selecting the real failure category must not bypass method safety, Never,
/// or an explicitly non-replayable callback body, even after full wire delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keepalive_delivery_unknown_preserves_replay_safety_and_body_gate() -> TestResult {
    for entry in [Entry::Buffered, Entry::Streaming] {
        for safety in [RequestSafety::Automatic, RequestSafety::Never] {
            run_case(
                entry,
                RetryOn::none().with_transport([TransportErrorKind::DeliveryUnknown]),
                safety,
                1,
                RetryStopReason::UnsafeToReplay,
            )
            .await?;
        }
    }
    run_case(
        Entry::NonReplayableCallback,
        RetryOn::none().with_transport([TransportErrorKind::DeliveryUnknown]),
        RequestSafety::IdempotentByContract,
        1,
        RetryStopReason::UnsafeToReplay,
    )
    .await
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

#![cfg(all(feature = "http-client", feature = "quality-doctor"))]

//! Behavioral comparisons use only controlled loopback peers. A local HTTP
//! proxy verifies proxy request routing; these tests do not emulate a VPN/TUN.

use open_net::api::error::{ErrorKind, ErrorStage};
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpProxyConfig, HttpRequest, HttpTimeouts, TimeoutPhase,
};
use open_net::api::quality::{
    QualityDoctor, QualityDoctorConfig, QualityHttpVersion, QualityLimits, QualityLookup,
    QualityOperationKind, QualityQuery, QualitySubscriptionOptions,
};
use open_net::api::retry::RetrySetting;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
enum Mode {
    Off,
    Unactivated,
    Basic,
    Detailed,
}
const MODES: [Mode; 4] = [Mode::Off, Mode::Unactivated, Mode::Basic, Mode::Detailed];
impl Mode {
    fn config(self) -> QualityDoctorConfig {
        match self {
            Self::Off => QualityDoctorConfig::default(),
            Self::Basic => QualityDoctorConfig::basic(),
            Self::Unactivated | Self::Detailed => QualityDoctorConfig::detailed(),
        }
    }
    fn activated(self) -> bool {
        matches!(self, Self::Basic | Self::Detailed)
    }
}

#[derive(Debug)]
struct Received {
    connection: usize,
    request_line: String,
}
struct Peer {
    address: std::net::SocketAddr,
    accepted: Arc<AtomicUsize>,
    requests: mpsc::UnboundedReceiver<Received>,
    task: tokio::task::JoinHandle<TestResult>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Peer {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        let (send, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (socket, _) = result?;
                        let connection = count.fetch_add(1, Ordering::SeqCst) + 1;
                        let send = send.clone();
                        connections.spawn(serve(socket, connection, send));
                    }
                    Some(result) = connections.join_next(), if !connections.is_empty() => {
                        result??;
                    }
                }
            }
        });
        Ok(Self {
            address,
            accepted,
            requests,
            task,
        })
    }
    async fn next(&mut self) -> TestResult<Received> {
        tokio::time::timeout(WATCHDOG, self.requests.recv())
            .await?
            .ok_or_else(|| "loopback peer stopped before receiving request".into())
    }
}

async fn serve(
    mut socket: TcpStream,
    connection: usize,
    send: mpsc::UnboundedSender<Received>,
) -> TestResult {
    loop {
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            if socket.read(&mut byte).await? == 0 {
                return Ok(());
            }
            request.push(byte[0]);
            if request.len() > 16_384 {
                return Err("oversized fixture request".into());
            }
        }
        let request_line = String::from_utf8(request)?
            .lines()
            .next()
            .ok_or("empty request")?
            .to_owned();
        let stall = request_line.contains("/stall") && !request_line.contains("/redirect-stall");
        let redirect_stall = request_line.contains("/redirect-stall ");
        let redirect = request_line.contains("/redirect ");
        send.send(Received {
            connection,
            request_line,
        })?;
        if stall {
            // Never publish headers. Wait for transport cancellation without
            // changing network policy or injecting a second timeout.
            let mut byte = [0];
            loop {
                match socket.read(&mut byte).await {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                        ) =>
                    {
                        break
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            return Ok(());
        }
        if redirect_stall {
            socket
                .write_all(b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: /stall\r\n\r\n")
                .await?;
        } else if redirect {
            socket
                .write_all(b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: /ok\r\n\r\n")
                .await?;
        } else {
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await?;
        }
    }
}

fn client(peer: &Peer, mode: Mode, proxy: bool) -> TestResult<(HttpClient, Option<QualityDoctor>)> {
    let base = if proxy {
        "http://quality-proxy.invalid".to_owned()
    } else {
        format!("http://{}", peer.address)
    };
    let mut config = HttpClientConfig::new(base)?
        .with_environment_proxy(false)
        .with_connect_timeout(Duration::from_secs(2))?
        .with_standalone_quality_doctor(mode.config());
    if proxy {
        config = config.with_proxy(HttpProxyConfig::http(format!("http://{}", peer.address))?);
    }
    let client = HttpClient::new(config, "quality-behavior")?;
    let doctor = if mode.activated() {
        client.quality_doctor()
    } else {
        None
    };
    Ok((client, doctor))
}
fn request(path: &str) -> TestResult<HttpRequest> {
    Ok(HttpRequest::builder()
        .path(path)
        .retry_setting(RetrySetting::Disabled)
        .build()?)
}

#[tokio::test]
async fn diagnostic_modes_preserve_direct_and_explicit_proxy_connection_reuse() -> TestResult {
    for proxy in [false, true] {
        for mode in MODES {
            let mut peer = Peer::start().await?;
            let (client, doctor) = client(&peer, mode, proxy)?;
            let mut previous_connection = None;
            for _ in 0..3 {
                let response =
                    tokio::time::timeout(WATCHDOG, client.request(request("/ok")?)).await??;
                assert_eq!(response.status, http::StatusCode::OK);
                assert_eq!(response.body.as_ref(), b"ok");
                assert_eq!(response.attempts, 1);
                let received = peer.next().await?;
                assert_eq!(
                    received.request_line,
                    if proxy {
                        "GET http://quality-proxy.invalid/ok HTTP/1.1"
                    } else {
                        "GET /ok HTTP/1.1"
                    }
                );
                if let Some(previous) = previous_connection {
                    assert_eq!(
                        received.connection, previous,
                        "{mode:?}: sequential request was not reused"
                    );
                }
                previous_connection = Some(received.connection);
                if let Some(doctor) = &doctor {
                    let reference = response
                        .quality_ref()
                        .ok_or("active response reference missing")?;
                    let QualityLookup::Available(report) = doctor.lookup(&reference) else {
                        return Err("active response report missing".into());
                    };
                    assert_eq!(report.http_version(), Some(QualityHttpVersion::Http11));
                    assert_eq!(report.http_status(), Some(200));
                    // A version is a response observation, not proof of a selected connection identity.
                    assert_eq!(report.associations().connection(), None);
                } else {
                    assert!(response.quality_ref().is_none());
                }
            }
            assert_eq!(
                peer.accepted.load(Ordering::SeqCst),
                1,
                "{mode:?}: extra connection"
            );
            if let Some(doctor) = &doctor {
                let snapshot = doctor.snapshot(
                    QualityQuery::default().with_kind(QualityOperationKind::Connection),
                )?;
                assert_eq!(
                    snapshot.reports().len(),
                    1,
                    "pool reuse must not invent connector candidates"
                );
            } else if matches!(mode, Mode::Unactivated) {
                let doctor = client
                    .quality_doctor()
                    .ok_or("unactivated doctor missing")?;
                assert!(doctor
                    .snapshot(QualityQuery::default())?
                    .reports()
                    .is_empty());
            }
            tokio::time::timeout(WATCHDOG, client.shutdown()).await??;
        }
    }
    Ok(())
}

#[tokio::test]
async fn diagnostic_modes_keep_slow_headers_timeout_and_cancellation_classification() -> TestResult
{
    for cancel in [false, true] {
        for mode in MODES {
            let mut peer = Peer::start().await?;
            let (client, doctor) = client(&peer, mode, false)?;
            let timeouts = if cancel {
                HttpTimeouts::new()
                    .without_attempt_timeout()
                    .without_headers_timeout()
                    .without_total_timeout()
            } else {
                HttpTimeouts::new()
                    .without_attempt_timeout()
                    .without_total_timeout()
                    .with_headers_timeout(Duration::from_millis(300))
            };
            let stalled = HttpRequest::builder()
                .path("/stall")
                .retry_setting(RetrySetting::Disabled)
                .timeouts(timeouts)
                .build()?;
            let running = client.clone();
            let operation = tokio::spawn(async move { running.request(stalled).await });
            let received = peer.next().await?;
            assert_eq!(received.request_line, "GET /stall HTTP/1.1");
            if cancel {
                client.cancel_all()?;
            }
            let error = tokio::time::timeout(WATCHDOG, operation)
                .await??
                .expect_err("stalled peer cannot return a response");
            assert_eq!(
                error.kind(),
                if cancel {
                    ErrorKind::Cancelled
                } else {
                    ErrorKind::TimedOut
                }
            );
            assert_eq!(
                error.timeout_phase(),
                if cancel {
                    None
                } else {
                    Some(TimeoutPhase::Headers)
                }
            );
            assert_eq!(error.context().http_attempt, Some(1));
            assert!(error.context().http_request_id.is_some());
            if !cancel {
                assert_eq!(error.context().stage, Some(ErrorStage::Response));
            }
            if let Some(doctor) = &doctor {
                let reference = error
                    .quality_ref()
                    .ok_or("active error reference missing")?;
                let QualityLookup::Available(report) = doctor.lookup(&reference) else {
                    return Err("active error report missing".into());
                };
                assert_eq!(report.http_version(), None, "no response head was received");
            } else {
                assert!(error.quality_ref().is_none());
            }
            tokio::time::timeout(WATCHDOG, client.drain()).await??;
            // Following work still obtains admission and a usable connection.
            let response =
                tokio::time::timeout(WATCHDOG, client.request(request("/ok")?)).await??;
            assert_eq!(response.status, http::StatusCode::OK);
            assert_eq!(response.attempts, 1);
            assert_eq!(peer.next().await?.request_line, "GET /ok HTTP/1.1");
            assert!(
                peer.requests.try_recv().is_err(),
                "diagnostics introduced another request"
            );
            tokio::time::timeout(WATCHDOG, client.shutdown()).await??;
        }
    }
    Ok(())
}

#[tokio::test]
async fn saturated_diagnostics_and_snapshot_reads_preserve_cancellation_and_pool_recovery(
) -> TestResult {
    for cancel in [false, true] {
        let mut peer = Peer::start().await?;
        let config = HttpClientConfig::new(format!("http://{}", peer.address))?
            .with_environment_proxy(false)
            .with_standalone_quality_doctor(
                QualityDoctorConfig::detailed().with_limits(
                    QualityLimits::default()
                        .with_ingress_capacity(1)
                        .with_subscription_capacity(1),
                ),
            );
        let client = HttpClient::new(config, "quality-pressure")?;
        let doctor = client.quality_doctor().ok_or("doctor missing")?;
        // Keep the subscriber alive without consuming its updates. Its bounded
        // queue must overflow without applying backpressure to HTTP operations.
        let _slow_subscriber =
            doctor.subscribe(QualitySubscriptionOptions::default().with_capacity(1))?;
        let first = tokio::time::timeout(WATCHDOG, client.request(request("/ok")?)).await??;
        assert_eq!(first.body.as_ref(), b"ok");
        let warm_connection = peer.next().await?.connection;
        // No reader ran during the first request: ingress capacity one ensures
        // real loss, independent of thread scheduling or machine performance.
        let saturated = doctor.snapshot(QualityQuery::default())?;
        assert!(saturated.counters().ingress_queue_full_dropped() > 0);
        assert!(saturated.counters().subscriber_dropped() > 0);
        assert_eq!(saturated.counters().ingress_high_watermark(), 1);

        let timeouts = HttpTimeouts::new()
            .without_attempt_timeout()
            .without_total_timeout();
        let stalled = HttpRequest::builder()
            .path("/stall")
            .retry_setting(RetrySetting::Disabled)
            .timeouts(if cancel {
                timeouts.without_headers_timeout()
            } else {
                timeouts.with_headers_timeout(Duration::from_millis(300))
            })
            .build()?;
        let running = client.clone();
        let operation = tokio::spawn(async move { running.request(stalled).await });
        let received = peer.next().await?;
        assert_eq!(received.request_line, "GET /stall HTTP/1.1");
        assert_eq!(received.connection, warm_connection);

        let reading = doctor.clone();
        let (stop_reading, mut stop) = tokio::sync::oneshot::channel();
        let (reader_ready, ready) = tokio::sync::oneshot::channel();
        let reader = tokio::spawn(async move {
            reading.snapshot(QualityQuery::default())?;
            let _ = reader_ready.send(());
            loop {
                tokio::select! {
                    _ = &mut stop => return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(()),
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        reading.snapshot(QualityQuery::default())?;
                    }
                }
            }
        });
        tokio::time::timeout(WATCHDOG, ready).await??;
        if cancel {
            client.cancel_all()?;
        }
        let error = tokio::time::timeout(WATCHDOG, operation)
            .await??
            .expect_err("a stalled response cannot succeed");
        assert_eq!(
            error.kind(),
            if cancel {
                ErrorKind::Cancelled
            } else {
                ErrorKind::TimedOut
            }
        );
        assert_eq!(
            error.timeout_phase(),
            if cancel {
                None
            } else {
                Some(TimeoutPhase::Headers)
            }
        );
        tokio::time::timeout(WATCHDOG, client.drain()).await??;

        // The unfinished HTTP/1 response cannot return its connection to the
        // pool. New work must obtain a replacement and then reuse that socket.
        let mut recovered_connection = None;
        for _ in 0..2 {
            let response =
                tokio::time::timeout(WATCHDOG, client.request(request("/ok")?)).await??;
            assert_eq!(response.body.as_ref(), b"ok");
            assert_eq!(response.attempts, 1);
            let received = peer.next().await?;
            assert_eq!(received.request_line, "GET /ok HTTP/1.1");
            assert_ne!(received.connection, warm_connection);
            if let Some(previous) = recovered_connection {
                assert_eq!(received.connection, previous);
            }
            recovered_connection = Some(received.connection);
        }
        assert_eq!(peer.accepted.load(Ordering::SeqCst), 2);
        assert!(peer.requests.try_recv().is_err());
        let _ = stop_reading.send(());
        tokio::time::timeout(WATCHDOG, reader).await???;
        tokio::time::timeout(WATCHDOG, client.shutdown()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn successful_response_headers_record_protocol_through_redirects() -> TestResult {
    let mut peer = Peer::start().await?;
    let (client, doctor) = client(&peer, Mode::Detailed, false)?;
    let doctor = doctor.ok_or("doctor missing")?;
    let response = tokio::time::timeout(WATCHDOG, client.request(request("/redirect")?)).await??;
    assert_eq!(response.status, http::StatusCode::OK);
    assert_eq!(peer.next().await?.request_line, "GET /redirect HTTP/1.1");
    assert_eq!(peer.next().await?.request_line, "GET /ok HTTP/1.1");
    let reference = response.quality_ref().ok_or("reference missing")?;
    let QualityLookup::Available(report) = doctor.lookup(&reference) else {
        return Err("report missing".into());
    };
    assert_eq!(report.http_version(), Some(QualityHttpVersion::Http11));
    assert_eq!(
        report
            .phases()
            .iter()
            .filter(|phase| phase.kind() == open_net::quality::PhaseKind::ResponseHeaders)
            .count(),
        2
    );
    tokio::time::timeout(WATCHDOG, client.shutdown()).await??;
    Ok(())
}

#[tokio::test]
#[ignore = "waits for the unchanged 90-second Reqwest pool idle lifetime"]
async fn diagnostic_modes_keep_default_pool_idle_expiry() -> TestResult {
    futures::future::try_join_all(MODES.into_iter().map(|mode| async move {
        let mut peer = Peer::start().await?;
        let (client, _doctor) = client(&peer, mode, false)?;
        let first = tokio::time::timeout(WATCHDOG, client.request(request("/ok")?)).await??;
        assert_eq!(first.body.as_ref(), b"ok");
        let initial = peer.next().await?;
        // The SDK owns a separate worker runtime, so pausing this test's clock
        // cannot advance its pool. Exercise the unchanged 90-second lifetime
        // with real time; run all diagnostic modes concurrently.
        tokio::time::sleep(Duration::from_secs(91)).await;
        let second = tokio::time::timeout(WATCHDOG, client.request(request("/ok")?)).await??;
        assert_eq!(second.body.as_ref(), b"ok");
        assert_ne!(
            peer.next().await?.connection,
            initial.connection,
            "{mode:?}: expired socket was retained"
        );
        assert_eq!(peer.accepted.load(Ordering::SeqCst), 2);
        tokio::time::timeout(WATCHDOG, client.shutdown()).await??;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }))
    .await?;
    Ok(())
}

#[tokio::test]
async fn redirect_head_version_is_retained_when_following_hop_times_out() -> TestResult {
    let mut peer = Peer::start().await?;
    let (client, doctor) = client(&peer, Mode::Detailed, false)?;
    let doctor = doctor.ok_or("doctor missing")?;
    let request = HttpRequest::builder()
        .path("/redirect-stall")
        .retry_setting(RetrySetting::Disabled)
        .timeouts(
            HttpTimeouts::new()
                .without_attempt_timeout()
                .without_total_timeout()
                .with_headers_timeout(Duration::from_millis(500)),
        )
        .build()?;
    let running = client.clone();
    let operation = tokio::spawn(async move { running.request(request).await });
    assert_eq!(
        peer.next().await?.request_line,
        "GET /redirect-stall HTTP/1.1"
    );
    assert_eq!(peer.next().await?.request_line, "GET /stall HTTP/1.1");
    let snapshot =
        doctor.snapshot(QualityQuery::default().with_kind(QualityOperationKind::HttpRequest))?;
    assert_eq!(snapshot.reports().len(), 1);
    let pending = &snapshot.reports()[0];
    assert_eq!(pending.http_version(), Some(QualityHttpVersion::Http11));
    assert!(pending
        .active_phases()
        .any(|phase| phase.kind() == open_net::quality::PhaseKind::ExecuteToHeaders));
    assert_eq!(
        pending
            .phases()
            .iter()
            .filter(|phase| phase.kind() == open_net::quality::PhaseKind::ExecuteToHeaders)
            .count(),
        1
    );
    let error = tokio::time::timeout(WATCHDOG, operation)
        .await??
        .expect_err("second hop must time out");
    assert_eq!(error.timeout_phase(), Some(TimeoutPhase::Headers));
    let reference = error.quality_ref().ok_or("reference missing")?;
    let QualityLookup::Available(report) = doctor.lookup(&reference) else {
        return Err("report missing".into());
    };
    assert_eq!(report.http_version(), Some(QualityHttpVersion::Http11));
    assert_eq!(report.http_status(), None);
    assert_eq!(report.active_phases().count(), 0);
    tokio::time::timeout(WATCHDOG, client.shutdown()).await??;
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

#[cfg(feature = "http-client-http2")]
mod http2 {
    use super::*;
    use open_net::api::http::{HttpProtocol, HttpTlsConfig, HttpVersionPolicy};
    use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
    use tokio::sync::oneshot;

    const CA: &[u8] = include_bytes!("../src/test_fixtures/network/ca.pem");
    const SERVER: &[u8] = include_bytes!("fixtures/network/server.pem");
    const KEY: &[u8] = include_bytes!("fixtures/network/server-key.pem");

    struct H2Peer(tokio::task::JoinHandle<TestResult>);
    impl Drop for H2Peer {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    #[tokio::test]
    async fn activated_diagnostics_preserve_h2_stream_isolation_and_pool_reuse() -> TestResult {
        for mode in [Mode::Off, Mode::Detailed] {
            for cancel in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let address = listener.local_addr()?;
                let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
                    rustls_openssl::default_provider(),
                ))
                .with_safe_default_protocol_versions()?
                .with_no_client_auth()
                .with_single_cert(
                    CertificateDer::pem_slice_iter(SERVER).collect::<Result<Vec<_>, _>>()?,
                    PrivateKeyDer::from_pem_slice(KEY)?,
                )?;
                tls.alpn_protocols = vec![b"h2".to_vec()];
                let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
                let (reset_send, reset_wait) = oneshot::channel();
                let (seen_send, mut seen) = mpsc::unbounded_channel();
                let _peer = H2Peer(tokio::spawn(async move {
                    // Exactly one accepted connection; follow-up requests must
                    // share this connection to reach the fixture.
                    let (socket, _) = listener.accept().await?;
                    let tls = acceptor.accept(socket).await?;
                    let mut connection = h2::server::handshake(tls).await?;
                    let mut held: Option<h2::SendStream<bytes::Bytes>> = None;
                    let mut active: Option<h2::SendStream<bytes::Bytes>> = None;
                    let mut reset_send = Some(reset_send);
                    loop {
                        tokio::select! {
                            next = connection.accept() => {
                                let Some(next) = next else { return Ok(()); };
                                let (request, mut respond) = next?;
                                seen_send.send(request.uri().path().to_owned())?;
                                let mut stream = respond.send_response(http::Response::new(()), false)?;
                                if request.uri().path() == "/hold" {
                                    held = Some(stream);
                                } else if request.uri().path() == "/active" {
                                    stream.send_data(bytes::Bytes::from_static(b"before-"), false)?;
                                    active = Some(stream);
                                } else {
                                    stream.send_data(bytes::Bytes::from_static(b"ok"), true)?;
                                }
                            }
                            reset = async {
                                match held.as_mut() {
                                    Some(stream) => futures_util::future::poll_fn(|cx| stream.poll_reset(cx)).await,
                                    None => std::future::pending().await,
                                }
                            }, if reset_send.is_some() => {
                                assert_eq!(reset?, h2::Reason::CANCEL);
                                held = None;
                                let mut stream = active.take().ok_or("sibling expired before active stream opened")?;
                                stream.send_data(bytes::Bytes::from_static(b"after"), true)?;
                                if let Some(send) = reset_send.take() { let _ = send.send(()); }
                            }
                        }
                    }
                }));
                let config = HttpClientConfig::new(format!("https://{address}"))?
                    .with_environment_proxy(false)
                    .with_tls(HttpTlsConfig::default().with_root_certificates(CA, true)?)
                    .with_http_version_policy(HttpVersionPolicy::Http2Only)
                    .with_timeouts(HttpTimeouts::new().without_attempt_timeout())?
                    .with_standalone_quality_doctor(mode.config());
                let client = HttpClient::new(config, "quality-h2-isolation")?;
                let doctor = if mode.activated() {
                    client.quality_doctor()
                } else {
                    None
                };
                let idle_request = HttpRequest::builder()
                    .path("/hold")
                    .retry_setting(RetrySetting::Disabled)
                    .timeouts(if cancel {
                        HttpTimeouts::new().without_idle_read_timeout()
                    } else {
                        HttpTimeouts::new().with_idle_read_timeout(Duration::from_millis(500))
                    })
                    .build()?;
                let idle = tokio::time::timeout(WATCHDOG, client.stream(idle_request)).await??;
                assert_eq!(idle.info().protocol, HttpProtocol::Http2);
                assert_eq!(
                    tokio::time::timeout(WATCHDOG, seen.recv())
                        .await?
                        .as_deref(),
                    Some("/hold")
                );
                // The held stream has neither delivered body data nor finished.
                let (mut active, active_reference) = tokio::time::timeout(
                    WATCHDOG,
                    client.stream_with_quality_ref(request("/active")?),
                )
                .await??;
                let first = tokio::time::timeout(WATCHDOG, active.next_chunk())
                    .await?
                    .ok_or("active stream ended")??;
                assert_eq!(first.as_ref(), b"before-");
                assert_eq!(
                    tokio::time::timeout(WATCHDOG, seen.recv())
                        .await?
                        .as_deref(),
                    Some("/active")
                );
                if let Some(doctor) = &doctor {
                    let reference = active_reference.ok_or("H2 reference absent")?;
                    let QualityLookup::Available(report) = doctor.lookup(&reference) else {
                        return Err("H2 report absent".into());
                    };
                    assert_eq!(report.http_version(), Some(QualityHttpVersion::Http2));
                }
                if cancel {
                    drop(idle);
                } else {
                    let mut idle = idle;
                    let error = tokio::time::timeout(WATCHDOG, idle.next_chunk())
                        .await?
                        .ok_or("missing idle timeout")?
                        .expect_err("idle body cannot succeed");
                    assert_eq!(error.kind(), ErrorKind::TimedOut);
                    assert_eq!(error.timeout_phase(), Some(TimeoutPhase::IdleRead));
                    assert!(idle.next_chunk().await.is_none());
                }
                tokio::time::timeout(WATCHDOG, reset_wait).await??;
                let last = tokio::time::timeout(WATCHDOG, active.next_chunk())
                    .await?
                    .ok_or("active stream lost with sibling")??;
                assert_eq!(last.as_ref(), b"after");
                assert!(tokio::time::timeout(WATCHDOG, active.next_chunk())
                    .await?
                    .is_none());
                let after =
                    tokio::time::timeout(WATCHDOG, client.request(request("/after")?)).await??;
                assert_eq!(after.body.as_ref(), b"ok");
                assert_eq!(after.protocol(), HttpProtocol::Http2);
                assert_eq!(
                    tokio::time::timeout(WATCHDOG, seen.recv())
                        .await?
                        .as_deref(),
                    Some("/after")
                );
                assert!(seen.try_recv().is_err());
                if let Some(doctor) = &doctor {
                    let snapshot = doctor.snapshot(
                        QualityQuery::default().with_kind(QualityOperationKind::Connection),
                    )?;
                    assert_eq!(snapshot.reports().len(), 1);
                }
                tokio::time::timeout(WATCHDOG, client.shutdown()).await??;
            }
        }
        Ok(())
    }
}

// These variables are only installed on a new test process. Mutating the
// parent test process environment would race every concurrently built client.
const CHILD_BASE: &str = "OPEN_NET_QUALITY_PROXY_CHILD_BASE";
const CHILD_MODE: &str = "OPEN_NET_QUALITY_PROXY_CHILD_DETAILED";

#[tokio::test]
#[ignore = "isolated subprocess helper; invoked by the environment proxy parent test"]
async fn environment_proxy_subprocess_helper() -> TestResult {
    let base = std::env::var(CHILD_BASE)?;
    assert!(base.starts_with("http://127.0.0.1:"));
    let detailed = std::env::var_os(CHILD_MODE).is_some();
    let config = HttpClientConfig::new(base)?
        .with_environment_proxy(true)
        .with_connect_timeout(Duration::from_secs(2))?
        .with_standalone_quality_doctor(if detailed {
            QualityDoctorConfig::detailed()
        } else {
            QualityDoctorConfig::default()
        });
    let client = HttpClient::new(config, "quality-proxy-child")?;
    let _doctor = if detailed {
        client.quality_doctor()
    } else {
        None
    };
    let response = tokio::time::timeout(WATCHDOG, client.request(request("/ok")?)).await??;
    assert_eq!(response.body.as_ref(), b"ok");
    assert_eq!(response.attempts, 1);
    assert_eq!(response.quality_ref().is_some(), detailed);
    tokio::time::timeout(WATCHDOG, client.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn diagnostic_modes_preserve_environment_proxy_and_no_proxy_routes() -> TestResult {
    for bypass in [false, true] {
        for detailed in [false, true] {
            let mut origin = Peer::start().await?;
            let mut proxy = Peer::start().await?;
            let base = format!("http://{}", origin.address);
            let proxy_url = format!("http://{}", proxy.address);
            let mut command = std::process::Command::new(std::env::current_exe()?);
            command
                .arg("--exact")
                .arg("environment_proxy_subprocess_helper")
                .arg("--ignored")
                .arg("--nocapture")
                .env(CHILD_BASE, &base)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            for variable in [
                "HTTP_PROXY",
                "http_proxy",
                "HTTPS_PROXY",
                "https_proxy",
                "ALL_PROXY",
                "all_proxy",
                "NO_PROXY",
                "no_proxy",
                "REQUEST_METHOD",
                CHILD_MODE,
            ] {
                command.env_remove(variable);
            }
            command.env("HTTP_PROXY", proxy_url).env(
                "NO_PROXY",
                if bypass {
                    "127.0.0.1"
                } else {
                    "unmatched.invalid"
                },
            );
            if detailed {
                command.env(CHILD_MODE, "1");
            }
            let output =
                tokio::task::spawn_blocking(move || -> TestResult<std::process::Output> {
                    let mut child = command.spawn()?;
                    let started = std::time::Instant::now();
                    loop {
                        if child.try_wait()?.is_some() {
                            break;
                        }
                        if started.elapsed() > WATCHDOG {
                            let _ = child.kill();
                            let _ = child.wait();
                            return Err("environment proxy subprocess exceeded watchdog".into());
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Ok(child.wait_with_output()?)
                })
                .await??;
            assert!(
                output.status.success(),
                "proxy child failed: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            if bypass {
                assert_eq!(origin.next().await?.request_line, "GET /ok HTTP/1.1");
                assert_eq!(origin.accepted.load(Ordering::SeqCst), 1);
                assert_eq!(proxy.accepted.load(Ordering::SeqCst), 0);
            } else {
                assert_eq!(
                    proxy.next().await?.request_line,
                    format!("GET {base}/ok HTTP/1.1")
                );
                assert_eq!(proxy.accepted.load(Ordering::SeqCst), 1);
                assert_eq!(origin.accepted.load(Ordering::SeqCst), 0);
            }
        }
    }
    Ok(())
}

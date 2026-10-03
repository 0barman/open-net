#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;
use open_net::ws::ConnectionEventKind;
use open_net::ws::{ConnectionEvent, RetryDecision, TerminationReason};
use session::{session_options, SessionGuard};

use open_net::network::{NetworkConfig, ProxyConfig};
use open_net::ws::{ConnectOptions, ConnectionState, ReconnectPolicy, WebSocketClientConfig};
use open_net::{NetError, OpenNet, WebSocketClient};

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

#[track_caller]
fn error(message: impl Into<String>) -> TestError {
    let location = std::panic::Location::caller();
    let message = message.into();
    std::io::Error::other(format!("{location}: {message}")).into()
}

#[track_caller]
fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(error(message))
    }
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .map_err(|e| error(format!("{label}: {e}")))
}

async fn established(events: &mut session::ObservedSession) -> TestResult<ConnectionEvent> {
    let started = bounded("Started", events.recv())
        .await??
        .ok_or("missing Started")?;
    let ConnectionEventKind::AttemptStarted { attempt } = started.kind else {
        return Err(error("first event was not Started"));
    };
    check(
        started.sequence == 1
            && started.client_id == attempt.client_id
            && started.session_id == attempt.session_id,
        "Started identity or sequence changed",
    )?;
    let established = bounded("Established", events.recv())
        .await??
        .ok_or("missing Established")?;
    let ConnectionEventKind::Established { connection } = &established.kind else {
        return Err(error("attempt did not establish"));
    };
    check(
        established.sequence == 2
            && established.client_id == started.client_id
            && established.session_id == started.session_id,
        "Established envelope changed",
    )?;
    check(
        connection.client_id == attempt.client_id
            && connection.session_id == attempt.session_id
            && connection.cycle_id == attempt.cycle_id
            && connection.attempt_id == attempt.attempt_id,
        "Established did not retain its attempt identity",
    )?;
    Ok(established)
}

async fn cancelled(
    events: &mut session::ObservedSession,
    established: &ConnectionEvent,
) -> TestResult {
    let ConnectionEventKind::Established { connection } = &established.kind else {
        return Err(error("cancellation fixture had no connection"));
    };
    let disconnected = bounded("Disconnected", events.recv())
        .await??
        .ok_or("missing Disconnected")?;
    check(
        disconnected.sequence == 3
            && disconnected.client_id == established.client_id
            && disconnected.session_id == established.session_id,
        "Disconnected envelope changed",
    )?;
    let ConnectionEventKind::Disconnected {
        connection: ended,
        end,
    } = disconnected.kind
    else {
        return Err(error("cancellation omitted physical terminal"));
    };
    check(
        ended.connection_id == connection.connection_id
            && end.reason == TerminationReason::Cancelled,
        "cancellation closed another physical connection",
    )?;
    let closed = bounded("Closed", events.recv())
        .await??
        .ok_or("missing Closed")?;
    check(
        closed.sequence == 4
            && closed.client_id == established.client_id
            && closed.session_id == established.session_id,
        "Closed envelope changed",
    )?;
    let ConnectionEventKind::Closed {
        result: Err(failure),
    } = closed.kind
    else {
        return Err(error("cancelled session did not report failure"));
    };
    check(
        failure.kind() == open_net::error::ErrorKind::Cancelled,
        "wrong cancellation classification",
    )?;
    check(
        bounded("stable EOF", events.recv()).await??.is_none(),
        "event followed Closed",
    )?;
    Ok(())
}

fn check_exhaustion_source(failure: &NetError) -> TestResult {
    let cause = std::error::Error::source(failure)
        .and_then(|source| source.downcast_ref::<NetError>())
        .ok_or("exhaustion lost its original NetError source")?;
    check(
        cause.kind() == open_net::error::ErrorKind::HandshakeRejected
            && cause.context().http_status == Some(open_net::StatusCode::SERVICE_UNAVAILABLE)
            && cause.context().stage == Some(open_net::error::ErrorStage::Upgrade)
            && failure.context().http_status == cause.context().http_status,
        "exhaustion did not retain the last HTTP rejection",
    )?;
    Ok(())
}

fn policy(retries: usize) -> ReconnectPolicy {
    ReconnectPolicy::Backoff(open_net::ws::BackoffConfig {
        max_retries: retries,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(1),
        max_elapsed: Some(Duration::from_secs(3)),
    })
}

fn options(url: &str, retries: usize) -> ConnectOptions {
    let mut connect_options = session_options(url, policy(retries));
    connect_options.handshake_timeout = Duration::from_secs(2);
    connect_options
}

fn increment(counter: &AtomicUsize) -> Result<usize, NetError> {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
            value.checked_add(1)
        })
        .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))
}

fn client_config() -> WebSocketClientConfig {
    {
        let mut config = WebSocketClientConfig::default();
        config.close_timeout = Duration::from_millis(20);
        config
    }
}

async fn cleanup(client: &WebSocketClient, peer: &mut Peer) -> TestResult {
    let stopped = bounded("shutdown client", client.shutdown()).await;
    let peer_result = peer.finish().await;
    stopped??;
    peer_result
}

async fn read_header(stream: &mut TcpStream) -> TestResult<String> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        check(
            bytes.len() < 16 * 1024,
            "test HTTP header exceeded its bound",
        )?;
        bytes.push(bounded("read request header", stream.read_u8()).await??);
    }
    Ok(String::from_utf8(bytes)?)
}

struct Peer {
    url: String,
    accepted: Arc<AtomicUsize>,
    stop: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Sender<()>>,
    requests: mpsc::UnboundedReceiver<()>,
    task: JoinHandle<TestResult>,
}

impl Peer {
    async fn start(statuses: Vec<u16>) -> TestResult<Self> {
        Self::start_with_proxy(statuses, false).await
    }

    async fn start_with_proxy(statuses: Vec<u16>, proxy: bool) -> TestResult<Self> {
        Self::start_mode(statuses, proxy, false).await
    }

    async fn start_mode(statuses: Vec<u16>, proxy: bool, hold_first: bool) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&accepted);
        let (stop, mut stopped) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let mut first_release = hold_first.then_some(released);
        let (observed, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let (mut stream, _) = tokio::select! {
                    _ = &mut stopped => return Ok(()),
                    incoming = listener.accept() => incoming?,
                };
                let mut request = read_header(&mut stream).await?;
                let index = increment(&count)?;
                observed.send(())?;
                if let Some(released) = first_release.take() {
                    tokio::select! {
                        _ = &mut stopped => return Ok(()),
                        released = released => released?,
                    }
                }
                let status = match statuses.get(index) {
                    Some(status) => *status,
                    None => 101,
                };
                if proxy && status == 101 {
                    check(
                        request.starts_with("CONNECT "),
                        "proxy received a non-CONNECT request",
                    )?;
                    stream
                        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                        .await?;
                    request = read_header(&mut stream).await?;
                }
                let response = if status == 101 {
                    let key = request
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find_map(|(key, value)| {
                            key.eq_ignore_ascii_case("sec-websocket-key")
                                .then_some(value.trim())
                        })
                        .ok_or_else(|| error("missing WebSocket key"))?;
                    format!(
                        "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n",
                        derive_accept_key(key.as_bytes())
                    )
                } else {
                    format!(
                        "HTTP/1.1 {status} Test Response\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                };
                stream.write_all(response.as_bytes()).await?;
                if status == 101 {
                    held.push(stream);
                }
            }
        });
        Ok(Self {
            url,
            accepted,
            stop: Some(stop),
            release: Some(release),
            requests,
            task,
        })
    }

    fn count(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    async fn observe_request(&mut self) -> TestResult {
        bounded("observe physical handshake", self.requests.recv())
            .await?
            .ok_or_else(|| error("peer observation channel closed"))
    }

    fn release_handshake(&mut self) -> TestResult {
        self.release
            .take()
            .ok_or_else(|| error("handshake gate was already released"))?
            .send(())
            .map_err(|_| error("handshake gate receiver closed"))
    }

    async fn finish(&mut self) -> TestResult {
        if let Some(stop) = self.stop.take() {
            // A finished task may have already closed this receiver. Joining below
            // reports its original error instead of obscuring it with SendError.
            let _ = stop.send(());
        }
        bounded("join local peer", &mut self.task).await???;
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn terminal_http_rejection_blocks_hints_but_allows_explicit_recovery() -> TestResult {
    for status in [401, 403] {
        let mut peer = Peer::start(vec![status]).await?;
        let net = OpenNet::new()?;
        let client = net
            .create_ws_client_with_config("terminal-http-recovery", client_config())
            .await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let provider_calls = Arc::clone(&calls);
        let initial = {
            let mut options = options(&peer.url, 6);
            options.handshake_provider =
                Some(open_net::ws::HandshakeProvider::blocking(move |_| {
                    increment(&provider_calls)?;
                    Ok(open_net::ws::HandshakeHeaders {
                        headers: open_net::HeaderMap::new(),
                        credential_version: Some((1).to_string()),
                    })
                }));
            options
        };
        let outcome: TestResult = async {
            let failed_session = client.start_session(initial, None).await?;
            let failed = bounded("initial rejection", failed_session.wait_connected()).await?;
            check(
                failed.err().as_ref().map(|error| error.kind())
                    == Some(open_net::error::ErrorKind::HandshakeRejected),
                "HTTP error changed",
            )?;
            check(
                failed_session
                    .state()?
                    .last_error
                    .as_ref()
                    .and_then(|error| error.context().http_status)
                    .map(|status| status.as_u16())
                    == Some(status),
                "HTTP snapshot changed",
            )?;
            check(
                failed_session
                    .state()?
                    .last_error
                    .as_ref()
                    .map(|error| error.kind())
                    == Some(open_net::error::ErrorKind::HandshakeRejected),
                "HTTP terminal failure lost its stable error snapshot",
            )?;
            check(peer.count() == 1, "non-retryable HTTP failure was retried")?;
            check(
                bounded("failed session cleanup", failed_session.closed())
                    .await?
                    .is_err(),
                "failed session cleanup changed its terminal result",
            )?;
            for _ in 0..8 {
                failed_session.notify_network_available();
            }
            // A terminated session has no recoverable target. The new owner must
            // establish without a hint reviving the old rejected provider.
            let _session = bounded(
                "explicit recovery after hints",
                SessionGuard::establish(&client, options(&peer.url, 0)),
            )
            .await??;
            check(
                calls.load(Ordering::SeqCst) == 1,
                "hints reinvoked the rejected provider",
            )?;
            check(
                peer.count() == 2,
                "explicit recovery did not use exactly one new socket",
            )?;
            check(
                matches!(
                    _session.session.state()?.state,
                    ConnectionState::Connected(_)
                ),
                "explicit recovery did not establish",
            )?;
            check(
                _session.session.state()?.last_error.is_none(),
                "explicit recovery retained the old failure snapshot",
            )?;
            Ok(())
        }
        .await;
        let cleanup = bounded("shutdown HTTP client", client.shutdown()).await;
        let peer_result = peer.finish().await;
        cleanup??;
        peer_result?;
        outcome?;
    }
    Ok(())
}

#[tokio::test]
async fn local_provider_and_header_failures_do_not_restart_on_hints() -> TestResult {
    for failure in [
        NetError::from(open_net::error::ErrorKind::ProviderFailed),
        NetError::from(open_net::error::ErrorKind::RetryExhausted),
        NetError::from(open_net::error::ErrorKind::InvalidConfig),
    ] {
        let failure_kind = failure.kind();
        let mut peer = Peer::start(Vec::new()).await?;
        let net = OpenNet::new()?;
        let client = net
            .create_ws_client_with_config("local-recovery", client_config())
            .await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let provider_calls = Arc::clone(&calls);
        let initial = {
            let mut options = options(&peer.url, 6);
            options.handshake_provider =
                Some(open_net::ws::HandshakeProvider::blocking(move |_| {
                    if increment(&provider_calls)? == 0 {
                        if failure.kind() == open_net::error::ErrorKind::InvalidConfig {
                            return Ok(open_net::ws::HandshakeHeaders {
                                headers: open_net::HeaderMap::from_iter([(
                                    open_net::HeaderName::from_bytes(("authorization").as_bytes())?,
                                    open_net::HeaderValue::from_str("invalid\r\nvalue")?,
                                )]),
                                credential_version: Some((1).to_string()),
                            });
                        }
                        return Err(failure.clone().into());
                    }
                    Ok(open_net::ws::HandshakeHeaders {
                        headers: open_net::HeaderMap::new(),
                        credential_version: Some((1).to_string()),
                    })
                }));
            options
        };
        let outcome: TestResult = async {
            let failed_session = client.start_session(initial, None).await?;
            let failed = bounded("local terminal failure", failed_session.wait_connected()).await?;
            let failure = failed.err().ok_or("provider/header failure was accepted")?;
            let source = std::error::Error::source(&failure)
                .ok_or("provider/header failure dropped its original source")?;
            let source_matches = if failure_kind == open_net::error::ErrorKind::InvalidConfig {
                source.is::<http::header::InvalidHeaderValue>()
            } else {
                source
                    .downcast_ref::<NetError>()
                    .is_some_and(|source| source.kind() == failure_kind)
            };
            check(
                failure.kind() == open_net::error::ErrorKind::ProviderFailed && source_matches,
                "provider/header failure lost its boundary or original source",
            )?;
            check(peer.count() == 0, "local failure opened a network socket")?;
            check(
                failed_session
                    .state()?
                    .last_error
                    .as_ref()
                    .is_some_and(|error| error.context().http_status.is_none()),
                "local failure inherited an HTTP status",
            )?;
            check(
                failed_session
                    .state()?
                    .last_error
                    .as_ref()
                    .map(NetError::kind)
                    == Some(open_net::error::ErrorKind::ProviderFailed),
                "local failure lost its stable error snapshot",
            )?;
            check(
                bounded("failed session cleanup", failed_session.closed())
                    .await?
                    .is_err(),
                "failed session cleanup changed its terminal result",
            )?;
            for _ in 0..8 {
                failed_session.notify_network_available();
            }
            let _session = bounded(
                "explicit recovery after local failure",
                SessionGuard::establish(&client, options(&peer.url, 0)),
            )
            .await??;
            check(
                calls.load(Ordering::SeqCst) == 1,
                "hint invoked a rejected local provider again",
            )?;
            check(
                peer.count() == 1,
                "local recovery opened an unexpected socket",
            )?;
            Ok(())
        }
        .await;
        cleanup(&client, &mut peer).await?;
        outcome?;
    }
    Ok(())
}

#[tokio::test]
async fn proxy_authentication_rejection_does_not_restart_on_hints() -> TestResult {
    let mut peer = Peer::start_with_proxy(vec![407], true).await?;
    let proxy_url = peer.url.replacen("ws://", "http://", 1);
    let net = OpenNet::new()?;
    let client = net
        .create_ws_client_with_network_config(
            "proxy-auth-recovery",
            client_config(),
            NetworkConfig::default().with_proxy(ProxyConfig::http_connect(&proxy_url, None)?),
        )
        .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::clone(&calls);
    let initial = {
        let mut options = options("ws://origin.invalid/recovery", 6);
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(move |_| {
            increment(&provider_calls)?;
            Ok(open_net::ws::HandshakeHeaders {
                headers: open_net::HeaderMap::new(),
                credential_version: Some((1).to_string()),
            })
        }));
        options
    };
    let target = "ws://origin.invalid/recovery";
    let outcome: TestResult = async {
        let failed_session = client.start_session(initial, None).await?;
        let failed = bounded("proxy auth rejection", failed_session.wait_connected()).await?;
        check(
            failed.err().as_ref().map(|error| error.kind())
                == Some(open_net::error::ErrorKind::HandshakeRejected),
            "proxy auth error changed",
        )?;
        check(
            peer.count() == 1,
            "proxy authentication was retried in the same cycle",
        )?;
        check(
            failed_session
                .state()?
                .last_error
                .as_ref()
                .is_some_and(|error| {
                    error.context().http_status
                        == Some(open_net::StatusCode::PROXY_AUTHENTICATION_REQUIRED)
                        && error.context().stage == Some(open_net::error::ErrorStage::Proxy)
                }),
            "proxy rejection lost its typed Proxy-stage HTTP context",
        )?;
        check(
            bounded("failed session cleanup", failed_session.closed())
                .await?
                .is_err(),
            "failed session cleanup changed its terminal result",
        )?;
        for _ in 0..8 {
            failed_session.notify_network_available();
        }
        let _session = bounded(
            "explicit proxy recovery",
            SessionGuard::establish(&client, options(target, 0)),
        )
        .await??;
        check(
            calls.load(Ordering::SeqCst) == 1,
            "hint retried the rejected proxy target",
        )?;
        check(
            peer.count() == 2,
            "proxy recovery did not use exactly one new tunnel",
        )?;
        Ok(())
    }
    .await;
    cleanup(&client, &mut peer).await?;
    outcome
}

#[tokio::test]
async fn transient_exhaustion_requires_a_new_session_and_hints_preserve_healthy_socket(
) -> TestResult {
    for retries in [0usize, 1, 6] {
        let attempts = retries
            .checked_add(1)
            .ok_or_else(|| error("attempt count overflow"))?;
        let mut peer = Peer::start(vec![503; attempts]).await?;
        let net = OpenNet::new()?;
        let client = net
            .create_ws_client_with_config("transient-recovery", client_config())
            .await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let provider_calls = Arc::clone(&calls);
        let initial = {
            let mut options = options(&peer.url, retries);
            options.handshake_provider =
                Some(open_net::ws::HandshakeProvider::blocking(move |_| {
                    increment(&provider_calls)?;
                    Ok(open_net::ws::HandshakeHeaders {
                        headers: open_net::HeaderMap::new(),
                        credential_version: Some((1).to_string()),
                    })
                }));
            options
        };
        let outcome: TestResult = async {
            let mut old = session::observe(&client, initial).await?;
            let mut failures = 0usize;
            let mut started = None;
            let mut terminal = None;
            let mut sequence = 0u64;
            while let Some(event) = bounded("exhaust attempts", old.recv()).await?? {
                sequence = sequence.checked_add(1).ok_or("event sequence overflow")?;
                check(event.sequence == sequence, "exhaustion event sequence skipped")?;
                check(terminal.is_none(), "event followed Closed")?;
                match &event.kind {
                    ConnectionEventKind::AttemptStarted { attempt } => {
                        check(started.is_none(), "attempt started before previous outcome")?;
                        check(attempt.client_id == event.client_id && attempt.session_id == event.session_id,
                            "Started has a different envelope scope")?;
                        started = Some(attempt.clone());
                    }
                    ConnectionEventKind::AttemptFailed { attempt, error: failure, retry, .. } => {
                        let previous = started.take().ok_or("failure omitted Started")?;
                        check(previous.cycle_id == attempt.cycle_id && previous.attempt_id == attempt.attempt_id,
                            "failure changed attempt identity")?;
                        check(failure.context().http_status == Some(open_net::StatusCode::SERVICE_UNAVAILABLE), "failed attempt lost HTTP status")?;
                        failures = failures.checked_add(1).ok_or("failure count overflow")?;
                        check(matches!(retry, RetryDecision::Stop) == (failures == attempts),
                            "retry decision disagrees with configured budget")?;
                    }
                    ConnectionEventKind::Closed { result: Err(failure) } => {
                        check(started.is_none(), "Closed omitted an attempt outcome")?;
                        check(failure.kind() == open_net::error::ErrorKind::RetryExhausted,
                            "exhaustion lost its terminal classification")?;
                        check_exhaustion_source(failure)?;
                        check(failure.context().session_id == Some(event.session_id),
                            "Closed error lost its session identity")?;
                        terminal = Some(event);
                    }
                    _ => return Err(error("exhausted session unexpectedly established")),
                }
            }
            terminal.ok_or_else(|| error("missing terminal event"))?;
            check(
                failures == attempts && peer.count() == attempts,
                "max_retries did not yield exactly 1 + max_retries handshakes",
            )?;
            check(
                calls.load(Ordering::SeqCst) == attempts,
                "provider count disagreed with handshakes",
            )?;
            check(
                bounded("exhausted session cleanup", old.session.closed()).await?.is_err(),
                "exhausted session cleanup changed its terminal result",
            )?;
            for _ in 0..8 {
                old.session.notify_network_available();
            }
            // A new owner must be admitted after exhaustion; hints only wake
            // active backoff and cannot revive the terminated owner.
            let session = bounded(
                "explicit recovery after exhaustion",
                SessionGuard::establish(&client, options(&peer.url, 0)),
            )
            .await??;
            check(
                peer.count() == attempts + 1,
                "explicit recovery used extra sockets",
            )?;
            check(
                calls.load(Ordering::SeqCst) == attempts,
                "hint revived the terminal provider",
            )?;
            check(
                bounded("terminal stream stays closed", old.recv())
                    .await??
                    .is_none(),
                "hint restarted the terminal event stream",
            )?;
            old.session.cancel();
            for _ in 0..8 {
                session.session.notify_network_available();
            }
            let duplicate = session::observe(&client
                , options(&peer.url, 0))
                .await;
            check(
                matches!(duplicate, Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::SessionAlreadyExists)),
                "healthy owner was replaced",
            )?;
            check(
                peer.count() == attempts + 1,
                "hint opened an extra healthy socket",
            )?;
            check(
                session.session.state()?.last_error.is_none(),
                "explicit recovery retained stale failure snapshots",
            )?;
            session.session.cancel();
            session.finish().await?;
            Ok(())
        }
        .await;
        cleanup(&client, &mut peer).await?;
        outcome?;
    }
    Ok(())
}

#[tokio::test]
async fn context_exhaustion_recovers_explicitly_without_a_network_hint() -> TestResult {
    let mut peer = Peer::start(vec![503, 503]).await?;
    let net = OpenNet::new()?;
    let client = net
        .create_ws_client_with_config("context-explicit-recovery", client_config())
        .await?;
    let outcome: TestResult = async {
        let mut old = bounded(
            "start exhausted context",
            session::observe(&client, {
                let mut connect_options = {
                    let mut options = {
                        let mut options = ConnectOptions::new(&peer.url);
                        options.headers = open_net::HeaderMap::new();
                        options
                    };
                    options.reconnect = policy(1);
                    options
                };
                connect_options.handshake_timeout = Duration::from_secs(2);
                connect_options
            }),
        )
        .await??;
        let mut failed_attempts = 0usize;
        let mut terminal = None;
        let mut started = None;
        let mut sequence = 0u64;
        while let Some(event) = bounded("exhaust old context", old.recv()).await?? {
            sequence = sequence.checked_add(1).ok_or("event sequence overflow")?;
            check(event.sequence == sequence, "old context sequence skipped")?;
            check(terminal.is_none(), "old context emitted after Closed")?;
            match &event.kind {
                ConnectionEventKind::AttemptStarted { attempt } => {
                    check(started.is_none(), "attempt started before previous outcome")?;
                    check(
                        attempt.client_id == event.client_id
                            && attempt.session_id == event.session_id,
                        "Started has a different envelope scope",
                    )?;
                    started = Some(attempt.clone());
                }
                ConnectionEventKind::AttemptFailed {
                    attempt,
                    error: failure,
                    retry,
                    ..
                } => {
                    let previous = started.take().ok_or("failure omitted Started")?;
                    check(
                        previous.cycle_id == attempt.cycle_id
                            && previous.attempt_id == attempt.attempt_id,
                        "failure changed attempt identity",
                    )?;
                    failed_attempts = failed_attempts
                        .checked_add(1)
                        .ok_or("event count overflow")?;
                    check(
                        failure.context().http_status
                            == Some(open_net::StatusCode::SERVICE_UNAVAILABLE),
                        "original transient failure lost",
                    )?;
                    check(
                        matches!(retry, RetryDecision::Stop) == (failed_attempts == 2),
                        "retry decision disagrees with configured budget",
                    )?;
                }
                ConnectionEventKind::Closed {
                    result: Err(failure),
                } => {
                    check(started.is_none(), "Closed omitted an attempt outcome")?;
                    check(
                        failure.kind() == open_net::error::ErrorKind::RetryExhausted,
                        "wrong exhaustion classification",
                    )?;
                    check_exhaustion_source(failure)?;
                    terminal = Some(event);
                }
                _ => return Err(error("unexpected successful event during exhaustion")),
            }
        }
        check(
            failed_attempts == 2,
            "context did not consume exactly two attempts",
        )?;
        let previous = terminal.ok_or_else(|| error("old context omitted its terminal event"))?;
        check(
            bounded("old context cleanup", old.session.closed())
                .await?
                .is_err(),
            "exhausted old context cleanup changed its terminal result",
        )?;
        // No network hint or simulated network edge occurs between the terminal
        // event and this explicit new session.
        let mut current = bounded(
            "start explicit new context",
            session::observe(&client, {
                let mut connect_options = {
                    let mut options = {
                        let mut options = ConnectOptions::new(&peer.url);
                        options.headers = open_net::HeaderMap::new();
                        options
                    };
                    options.reconnect = policy(0);
                    options
                };
                connect_options.handshake_timeout = Duration::from_secs(2);
                connect_options
            }),
        )
        .await??;
        let established = established(&mut current).await?;
        check(
            established.session_id != previous.session_id,
            "new context reused terminal session identity",
        )?;
        check(
            established.client_id == previous.client_id,
            "explicit recovery unexpectedly replaced the client",
        )?;
        old.session.cancel();
        check(
            bounded("old stream remains ended", old.recv())
                .await??
                .is_none(),
            "terminal stream restarted",
        )?;
        drop(old);
        let duplicate = bounded(
            "new context survives old handle",
            SessionGuard::establish(&client, options(&peer.url, 0)),
        )
        .await?;
        check(
            duplicate.err().as_ref().map(|error| error.kind())
                == Some(open_net::error::ErrorKind::SessionAlreadyExists),
            "stale handle cancellation affected new session",
        )?;
        check(
            peer.count() == 3,
            "explicit context recovery opened extra sockets",
        )?;
        current.session.cancel();
        cancelled(&mut current, &established).await?;
        Ok(())
    }
    .await;
    cleanup(&client, &mut peer).await?;
    outcome
}

#[tokio::test]
async fn disabled_reconnect_stops_after_one_attempt_and_ignores_hints() -> TestResult {
    let mut peer = Peer::start(vec![503]).await?;
    let net = OpenNet::new()?;
    let client = net
        .create_ws_client_with_config("disabled-reconnect", client_config())
        .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::clone(&calls);
    let mut initial = {
        let mut options = session_options(&peer.url, ReconnectPolicy::Disabled);
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(move |_| {
            increment(&provider_calls)?;
            Ok(open_net::ws::HandshakeHeaders {
                headers: open_net::HeaderMap::new(),
                credential_version: Some((1).to_string()),
            })
        }));
        options
    };
    initial.handshake_timeout = Duration::from_secs(2);
    initial.connect_timeout = Some(Duration::from_secs(3));
    let outcome: TestResult = async {
        let failed_session = client.start_session(initial, None).await?;
        let failed = bounded(
            "disabled reconnect first failure",
            failed_session.wait_connected(),
        )
        .await?;
        check(
            failed.err().as_ref().map(|error| error.kind())
                == Some(open_net::error::ErrorKind::HandshakeRejected),
            "disabled reconnect error changed",
        )?;
        check(
            peer.count() == 1,
            "disabled reconnect consumed additional attempts",
        )?;
        check(
            bounded("failed session cleanup", failed_session.closed())
                .await?
                .is_err(),
            "failed session cleanup changed its terminal result",
        )?;
        for _ in 0..8 {
            failed_session.notify_network_available();
        }
        let _session = bounded(
            "explicit recovery with reconnect disabled",
            SessionGuard::establish(&client, options(&peer.url, 0)),
        )
        .await??;
        check(
            calls.load(Ordering::SeqCst) == 1,
            "hint restarted a disabled reconnect provider",
        )?;
        check(
            peer.count() == 2,
            "disabled reconnect opened unexpected extra sockets",
        )?;
        Ok(())
    }
    .await;
    cleanup(&client, &mut peer).await?;
    outcome
}

#[tokio::test]
async fn one_hundred_concurrent_context_requests_admit_one_connection_owner() -> TestResult {
    let mut peer = Peer::start_mode(Vec::new(), false, true).await?;
    let net = OpenNet::new()?;
    let client = net
        .create_ws_client_with_config("one-concurrent-owner", client_config())
        .await?;
    let outcome: TestResult = async {
        let mut attempts = JoinSet::new();
        for _ in 0..100u64 {
            let client = client.clone();
            let target = peer.url.clone();
            attempts.spawn(async move {
                session::observe(&client, {
                    let mut connect_options = {
                        let mut options = {
                            let mut options = ConnectOptions::new(target);
                            options.headers = open_net::HeaderMap::new();
                            options
                        };
                        options.reconnect = policy(0);
                        options
                    };
                    connect_options.handshake_timeout = Duration::from_secs(2);
                    connect_options
                })
                .await
            });
        }
        // The server has read a real Upgrade but deliberately withholds 101, so
        // every request below is checked against one in-progress physical dial.
        peer.observe_request().await?;
        let mut owner = None;
        let mut rejected = 0usize;
        while !attempts.is_empty() {
            let result = bounded("concurrent command admission", attempts.join_next())
                .await?
                .ok_or_else(|| error("concurrent task set ended early"))??;
            match result {
                Ok(events) => {
                    check(
                        owner.is_none(),
                        "more than one concurrent session was admitted",
                    )?;
                    owner = Some(events);
                }
                Err(failure)
                    if failure.kind() == open_net::error::ErrorKind::SessionAlreadyExists =>
                {
                    rejected = rejected
                        .checked_add(1)
                        .ok_or_else(|| error("rejection count overflow"))?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        check(
            rejected == 99,
            "concurrent admission did not reject exactly 99 requests",
        )?;
        check(
            peer.count() == 1,
            "concurrent admission opened more than one socket",
        )?;
        let mut owner = owner.ok_or_else(|| error("no concurrent session was admitted"))?;
        peer.release_handshake()?;
        let established = established(&mut owner).await?;
        for _ in 0..8 {
            owner.session.notify_network_available();
        }
        let duplicate = bounded(
            "healthy owner command barrier",
            SessionGuard::establish(&client, options(&peer.url, 0)),
        )
        .await?;
        check(
            duplicate.err().as_ref().map(|error| error.kind())
                == Some(open_net::error::ErrorKind::SessionAlreadyExists),
            "healthy owner was replaced by hints",
        )?;
        check(
            peer.count() == 1,
            "healthy owner was assigned another socket",
        )?;
        check(
            owner.session.cancel(),
            "winning session was already cancelled",
        )?;
        cancelled(&mut owner, &established).await?;
        Ok(())
    }
    .await;
    cleanup(&client, &mut peer).await?;
    outcome
}

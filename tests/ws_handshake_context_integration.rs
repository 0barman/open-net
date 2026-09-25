#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;
use open_net::ws::ConnectionEvent;
use open_net::ws::ConnectionEventKind as Kind;
use session::SessionGuard;

use open_net::error::ErrorStage;
use open_net::ws::{ConnectOptions, ReconnectPolicy, WebSocketClientConfig};
use open_net::{NetError, OpenNet, WebSocketClient};
use session::ObservedSession;

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
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
fn policy(retries: usize) -> ReconnectPolicy {
    if retries > 0 {
        ReconnectPolicy::Backoff(open_net::ws::BackoffConfig {
            max_retries: retries,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_elapsed: Some(Duration::from_secs(3)),
        })
    } else {
        ReconnectPolicy::Disabled
    }
}
async fn make_client(net: &OpenNet, name: &str) -> TestResult<WebSocketClient> {
    Ok(bounded(
        "create client",
        net.create_ws_client_with_config(name, {
            let mut config = WebSocketClientConfig::default();
            config.close_timeout = Duration::from_millis(30);
            config
        }),
    )
    .await??)
}
async fn make_single_provider_client(net: &OpenNet, name: &str) -> TestResult<WebSocketClient> {
    let mut config = WebSocketClientConfig::default();
    config.close_timeout = Duration::from_millis(30);
    config.dispatch.blocking_handshake_jobs = 1;
    Ok(bounded(
        "create one-slot provider client",
        net.create_ws_client_with_config(name, config),
    )
    .await??)
}

fn options(url: &str) -> ConnectOptions {
    let mut connect_options = {
        let mut options = {
            let mut options = ConnectOptions::new(url);
            options.headers = open_net::HeaderMap::new();
            options
        };
        options.reconnect = policy(0);
        options
    };
    connect_options.handshake_timeout = Duration::from_secs(2);
    connect_options.connect_timeout = Some(Duration::from_secs(3));
    connect_options
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
async fn read_upgrade(stream: &mut TcpStream) -> TestResult<String> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        if bytes.len() >= 16 * 1024 {
            return Err(error("oversized test Upgrade request"));
        }
        bytes.push(bounded("read Upgrade byte", stream.read_u8()).await??);
    }
    Ok(String::from_utf8(bytes)?)
}
/// Status zero holds the socket before replying. Guards abort and release all
/// local sockets even when a test returns early.
struct Peer {
    url: String,
    accepted: Arc<AtomicUsize>,
    requests: mpsc::UnboundedReceiver<String>,
    close_connections: mpsc::UnboundedSender<()>,
    task: JoinHandle<TestResult>,
}
impl Peer {
    async fn start(statuses: Vec<u16>) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepted);
        let (tx, requests) = mpsc::unbounded_channel();
        let (close_connections, mut close_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let (mut stream, _) = tokio::select! {
                    request = close_rx.recv() => {
                        request.ok_or_else(|| error("peer control channel closed"))?;
                        held.clear();
                        continue;
                    }
                    accepted = listener.accept() => accepted?,
                };
                let index = counter.fetch_add(1, Ordering::SeqCst);
                let request = read_upgrade(&mut stream).await?;
                let status = statuses.get(index).copied().unwrap_or(101);
                let response = if status == 101 {
                    let key = header(&request, "sec-websocket-key")
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
                tx.send(request)?;
                if status != 0 {
                    stream.write_all(response.as_bytes()).await?;
                }
                if matches!(status, 0 | 101) {
                    held.push(stream);
                }
            }
        });
        Ok(Self {
            url,
            accepted,
            requests,
            close_connections,
            task,
        })
    }
    async fn request(&mut self) -> TestResult<String> {
        bounded("observe Upgrade", self.requests.recv())
            .await?
            .ok_or_else(|| error("peer observation channel closed"))
    }
    fn count(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn next(events: &mut ObservedSession) -> TestResult<ConnectionEvent> {
    bounded("receive event", events.recv())
        .await??
        .ok_or_else(|| error("events ended early"))
}
async fn next_attempt_result(events: &mut ObservedSession) -> TestResult<ConnectionEvent> {
    let started = next(events).await?;
    let Kind::AttemptStarted { attempt } = &started.kind else {
        return Err(error("attempt omitted Started"));
    };
    let event = next(events).await?;
    check(
        Some(event.sequence) == started.sequence.checked_add(1)
            && started.client_id == attempt.client_id
            && started.session_id == attempt.session_id
            && event.client_id == attempt.client_id
            && event.session_id == attempt.session_id,
        "attempt result lost Started sequence or envelope identity",
    )?;
    let (cycle, id) = match &event.kind {
        Kind::Established { connection } => (connection.cycle_id, connection.attempt_id),
        Kind::AttemptFailed { attempt, .. } => (attempt.cycle_id, attempt.attempt_id),
        _ => {
            return Err(error(
                "Started did not have exactly one establishment/failure result",
            ))
        }
    };
    check(
        cycle == attempt.cycle_id && id == attempt.attempt_id,
        "attempt result changed its Started identity",
    )?;
    Ok(event)
}
async fn drain(events: &mut ObservedSession) -> TestResult<Vec<ConnectionEvent>> {
    let mut result = Vec::new();
    let mut sequence: Option<u64> = None;
    let mut terminal = false;
    let mut started = None;
    while let Some(event) = bounded("drain events", events.recv()).await?? {
        check(!terminal, "event appeared after SessionTerminated")?;
        if let Some(previous) = sequence {
            check(
                previous.checked_add(1) == Some(event.sequence),
                "event sequence is not contiguous",
            )?;
        }
        match &event.kind {
            Kind::AttemptStarted { attempt } => {
                check(
                    started
                        .replace((attempt.cycle_id, attempt.attempt_id))
                        .is_none(),
                    "attempt started before prior outcome",
                )?;
                check(
                    event.client_id == attempt.client_id && event.session_id == attempt.session_id,
                    "Started identity changed",
                )?;
            }
            Kind::AttemptFailed { attempt, .. } => {
                check(
                    started.take() == Some((attempt.cycle_id, attempt.attempt_id)),
                    "failure omitted its own Started",
                )?;
            }
            Kind::Established { connection } => {
                check(
                    started.take() == Some((connection.cycle_id, connection.attempt_id)),
                    "Established omitted its own Started",
                )?;
            }
            Kind::Closed { .. } => check(started.is_none(), "Closed discarded a Started outcome")?,
            _ => {}
        }
        sequence = Some(event.sequence);
        terminal = matches!(event.kind, Kind::Closed { .. });
        result.push(event);
        check(result.len() <= 100, "unbounded event production")?;
    }
    check(terminal, "event stream omitted SessionTerminated")?;
    Ok(result)
}
fn count(events: &[ConnectionEvent], predicate: impl Fn(&Kind) -> bool) -> usize {
    events.iter().filter(|event| predicate(&event.kind)).count()
}
#[derive(Clone)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);
impl Gate {
    fn new() -> Self {
        Self(Arc::new((Mutex::new(false), Condvar::new())))
    }
    fn wait(&self) -> Result<(), NetError> {
        let (lock, condition) = self.0.as_ref();
        let mut released = lock
            .lock()
            .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?;
        while !*released {
            released = condition
                .wait(released)
                .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?;
        }
        Ok(())
    }
    fn release(&self) {
        let (lock, condition) = self.0.as_ref();
        match lock.lock() {
            Ok(mut released) => *released = true,
            Err(e) => *e.into_inner() = true,
        }
        condition.notify_all();
    }
}
struct Release(Gate);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn session_cancel_stops_a_blocked_provider_before_its_deadline() -> TestResult {
    let peer = Peer::start(vec![101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "scope-provider-cancel").await?;
    let gate = Gate::new();
    let release = Release(gate.clone());
    let (started, mut starts) = mpsc::unbounded_channel();
    let mut connect_options = {
        let mut options = options(&peer.url);
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(move |_| {
            started
                .send(())
                .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?;
            gate.wait()?;
            Ok(open_net::ws::HandshakeHeaders {
                headers: open_net::HeaderMap::new(),
                credential_version: Some((201).to_string()),
            })
        }));
        options
    };
    connect_options.handshake_timeout = Duration::from_secs(60);
    let mut events = session::observe(&client, connect_options).await?;
    check(
        bounded("provider starts", starts.recv()).await?.is_some(),
        "provider never started",
    )?;
    events.session.cancel();
    let seen = tokio::time::timeout(Duration::from_millis(500), drain(&mut events))
        .await
        .map_err(|e| error(format!("scope did not interrupt provider: {e}")))??;
    check(
        count(&seen, |kind| matches!(kind, Kind::Established { .. })) == 0,
        "cancelled provider established a connection",
    )?;
    check(
        matches!(
            events.session.state()?.state,
            open_net::ws::ConnectionState::Closed(_)
        ),
        "scope did not retire the connection session",
    )?;
    release.0.release();
    check(peer.count() == 0, "cancelled scope reached the peer")?;
    bounded(
        "destroy scoped client",
        net.destroy_ws_client("scope-provider-cancel"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn session_cancel_stops_upgrade_and_cannot_revoke_the_next_session() -> TestResult {
    let mut peer = Peer::start(vec![0, 101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "scope-upgrade-cancel").await?;
    let mut first = session::observe(&client, options(&peer.url)).await?;
    peer.request().await?;
    first.session.cancel();
    let seen = tokio::time::timeout(Duration::from_millis(500), drain(&mut first))
        .await
        .map_err(|e| error(format!("scope did not interrupt Upgrade: {e}")))??;
    check(
        count(&seen, |kind| matches!(kind, Kind::Established { .. })) == 0,
        "retired Upgrade was established",
    )?;
    let mut second = session::observe(&client, options(&peer.url)).await?;
    let established = next_attempt_result(&mut second).await?;
    check(
        matches!(established.kind, Kind::Established { .. }),
        "new scope did not establish",
    )?;
    first.session.cancel();
    check(
        !matches!(
            second.session.state()?.state,
            open_net::ws::ConnectionState::Closed(_)
        ),
        "old session revoked new owner",
    )?;
    check(
        matches!(
            second.session.state()?.state,
            open_net::ws::ConnectionState::Connected(_)
        ),
        "old scope retired new connection",
    )?;
    second.session.cancel();
    drain(&mut second).await?;
    bounded(
        "destroy scoped client",
        net.destroy_ws_client("scope-upgrade-cancel"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn new_context_connection_entry_returns_a_cancellable_event_handle() -> TestResult {
    let mut peer = Peer::start(vec![0]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "context-entry").await?;
    let mut events = bounded(
        "accept session before Upgrade",
        session::observe(&client, {
            let mut options = options(&peer.url);
            options
                .metadata
                .insert("session_context".into(), "7".into());
            options
        }),
    )
    .await??;
    peer.request().await?;
    events.session.cancel();
    let seen = drain(&mut events).await?;
    check(
        count(&seen, |kind| matches!(kind, Kind::AttemptFailed { .. })) == 1,
        "cancelled attempt omitted its result",
    )?;
    check(
        count(&seen, |kind| matches!(kind, Kind::Closed { .. })) == 1,
        "session ended more than once",
    )?;
    check(
        matches!(seen.first(), Some(ConnectionEvent { kind: Kind::AttemptStarted { attempt }, .. })
            if attempt.metadata.get("session_context").map(String::as_str) == Some("7")
                && seen.iter().all(|event| event.session_id == attempt.session_id)),
        "session context changed",
    )?;
    bounded("destroy client", net.destroy_ws_client("context-entry")).await??;
    Ok(())
}

#[tokio::test]
async fn six_headers_dynamic_override_empty_region_and_path_query_reach_peer() -> TestResult {
    let mut peer = Peer::start(vec![101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "complete-headers").await?;
    let url = format!("{}/cloud/path?mode=test", peer.url);
    let headers: open_net::HeaderMap = [
        ("authorization", "Bearer fake-token"),
        ("x-md-global-app-version", "1.2.3"),
        ("x-md-global-language", "zh-CN"),
        ("x-md-global-region", ""),
        ("x-md-region", "SG"),
        ("x-md-global-device-fp", "fake-device"),
    ]
    .into_iter()
    .map(|(k, v)| {
        Ok((
            open_net::HeaderName::from_bytes(k.as_bytes())?,
            open_net::HeaderValue::from_str(v)?,
        ))
    })
    .collect::<Result<_, TestError>>()?;
    let expected = headers.clone();
    let mut events = session::observe(&client, {
        let mut options = {
            let mut options = options(&url);
            options.headers = open_net::HeaderMap::from_iter([(
                open_net::HeaderName::from_bytes(("Authorization").as_bytes())?,
                open_net::HeaderValue::from_str("old")?,
            )]);
            options
        };
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(move |_| {
            Ok(open_net::ws::HandshakeHeaders {
                headers: headers.clone(),
                credential_version: Some((22).to_string()),
            })
        }));
        options
    })
    .await?;
    let request = peer.request().await?;
    check(
        request.starts_with("GET /cloud/path?mode=test HTTP/1.1\r\n"),
        "path/query changed",
    )?;
    for (name, value) in &expected {
        check(
            header(&request, name.as_str()) == Some(value.to_str()?),
            "business header missing or changed",
        )?;
    }
    let established = next_attempt_result(&mut events).await?;
    check(
        matches!(established.kind, Kind::Established { .. }),
        "header handshake did not establish",
    )?;
    check(
        matches!(&established.kind, Kind::Established { connection } if connection.credential_version.as_deref() == Some("22")),
        "headers and context diverged",
    )?;
    events.session.cancel();
    drain(&mut events).await?;
    bounded("destroy client", net.destroy_ws_client("complete-headers")).await??;
    Ok(())
}

#[tokio::test]
async fn local_provider_errors_never_open_network_and_anonymous_connections_remain_valid(
) -> TestResult {
    for failure in [
        NetError::from(open_net::error::ErrorKind::ProviderFailed),
        NetError::from(open_net::error::ErrorKind::InvalidConfig),
    ] {
        let failure_kind = failure.kind();
        let peer = Peer::start(vec![101]).await?;
        let net = OpenNet::new()?;
        let client = make_client(&net, "provider-local-error").await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let provider_calls = Arc::clone(&calls);
        let mut events = session::observe(&client, {
            let mut options = {
                let mut options = options(&peer.url);
                options.reconnect = policy(3);
                options
            };
            options.handshake_provider =
                Some(open_net::ws::HandshakeProvider::blocking(move |_| {
                    provider_calls.fetch_add(1, Ordering::SeqCst);
                    Err(failure.clone().into())
                }));
            options
        })
        .await?;
        let seen = drain(&mut events).await?;
        check(
            peer.count() == 0 && calls.load(Ordering::SeqCst) == 1,
            "local failure reached network or retried",
        )?;
        check(
            count(&seen, |kind| matches!(kind, Kind::AttemptFailed { .. })) == 1,
            "local failure completed incorrectly",
        )?;
        let details = seen
            .iter()
            .find_map(|event| match &event.kind {
                Kind::AttemptFailed { error, .. } => Some(error),
                _ => None,
            })
            .ok_or_else(|| error("missing local failure details"))?;
        check(
            details.kind() == open_net::error::ErrorKind::ProviderFailed
                && std::error::Error::source(details)
                    .and_then(|source| source.downcast_ref::<NetError>())
                    .is_some_and(|source| source.kind() == failure_kind)
                && details
                    .context()
                    .http_status
                    .map(|status| status.as_u16())
                    .is_none(),
            "wrong local failure details",
        )?;
        check(
            seen.iter()
                .filter(|e| matches!(e.kind, Kind::AttemptFailed { .. }))
                .all(|e| {
                    matches!(
                        &e.kind,
                        Kind::AttemptFailed {
                            credential_version: None,
                            ..
                        }
                    )
                }),
            "failed provider used stale context",
        )?;
        bounded(
            "destroy client",
            net.destroy_ws_client("provider-local-error"),
        )
        .await??;
    }
    let mut peer = Peer::start(vec![101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "anonymous-context").await?;
    let mut events = session::observe(&client, options(&peer.url)).await?;
    check(
        header(&peer.request().await?, "authorization").is_none(),
        "anonymous connection gained Authorization",
    )?;
    check(
        matches!(
            next_attempt_result(&mut events).await?.kind,
            Kind::Established { .. }
        ),
        "anonymous connection rejected",
    )?;
    events.session.cancel();
    drain(&mut events).await?;
    bounded("destroy client", net.destroy_ws_client("anonymous-context")).await??;
    Ok(())
}

#[tokio::test]
async fn invalid_typed_header_input_fails_in_provider_without_network_or_retry() -> TestResult {
    for invalid in [
        ("bad header", "value"),
        ("x-good", "value\r\ninjected: yes"),
    ] {
        let peer = Peer::start(vec![101]).await?;
        let net = OpenNet::new()?;
        let client = make_client(&net, "invalid-context-headers").await?;
        let mut events = session::observe(&client, {
            let mut options = {
                let mut options = options(&peer.url);
                options.handshake_provider =
                    Some(open_net::ws::HandshakeProvider::blocking(move |_| {
                        let name = open_net::HeaderName::from_bytes(invalid.0.as_bytes())?;
                        let value = open_net::HeaderValue::from_str(invalid.1)?;
                        Ok(open_net::ws::HandshakeHeaders::new(
                            open_net::HeaderMap::from_iter([(name, value)]),
                        ))
                    }));
                options
            };
            options.reconnect = policy(3);
            options
        })
        .await?;
        let seen = drain(&mut events).await?;
        check(
            peer.count() == 0
                && count(&seen, |kind| matches!(kind, Kind::AttemptFailed { .. })) == 1,
            "invalid header reached network or retried",
        )?;
        check(
            seen.iter()
                .filter_map(|event| match &event.kind {
                    Kind::AttemptFailed { error, .. } => Some(error),
                    _ => None,
                })
                .all(|f| f.context().http_status.is_none()),
            "local error gained HTTP status",
        )?;
        let failure = seen
            .iter()
            .find_map(|event| match &event.kind {
                Kind::AttemptFailed { error, .. } => Some(error),
                _ => None,
            })
            .ok_or("invalid typed header omitted provider failure")?;
        let cause = failure.clone();
        let source = std::error::Error::source(&cause)
            .ok_or("provider dropped the original HTTP parsing error")?;
        check(
            cause.kind() == open_net::error::ErrorKind::ProviderFailed
                && failure.context().stage == Some(ErrorStage::Provider)
                && if invalid.0 == "bad header" {
                    source.is::<http::header::InvalidHeaderName>()
                } else {
                    source.is::<http::header::InvalidHeaderValue>()
                },
            "typed header parse failure lost its boundary or original source",
        )?;
        bounded(
            "destroy client",
            net.destroy_ws_client("invalid-context-headers"),
        )
        .await??;
    }
    Ok(())
}

#[tokio::test]
async fn refreshed_headers_bind_actual_context_to_each_attempt_even_with_delayed_consumption(
) -> TestResult {
    let mut peer = Peer::start(vec![503, 401]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "token-refresh-context").await?;
    let gate = Gate::new();
    let release = Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::clone(&calls);
    let (attempt_tx, mut attempt_rx) = mpsc::unbounded_channel();
    let mut events = session::observe(&client, {
        let mut options = {
            let mut options = {
                let mut options = options(&peer.url);
                options
                    .metadata
                    .insert("session_context".to_owned(), "50".to_owned());
                options
            };
            options.reconnect = policy(2);
            options
        };
        options.handshake_provider =
            Some(open_net::ws::HandshakeProvider::blocking(move |attempt| {
                let call = provider_calls.fetch_add(1, Ordering::SeqCst);
                if attempt.metadata.get("session_context").map(String::as_str) != Some("50")
                    || tokio::runtime::Handle::try_current().is_ok()
                {
                    return Err(NetError::from(open_net::error::ErrorKind::InvalidConfig).into());
                }
                attempt_tx
                    .send(attempt)
                    .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?;
                if call > 0 {
                    gate.wait()?;
                }
                let (token, context) = if call == 0 {
                    ("Bearer fake-T1", 51)
                } else {
                    ("Bearer fake-T2", 52)
                };
                Ok(open_net::ws::HandshakeHeaders {
                    headers: open_net::HeaderMap::from_iter([(
                        open_net::HeaderName::from_bytes(("Authorization").as_bytes())?,
                        open_net::HeaderValue::from_str(token)?,
                    )]),
                    credential_version: Some((context).to_string()),
                })
            }));
        options
    })
    .await?;
    check(
        header(&peer.request().await?, "authorization") == Some("Bearer fake-T1"),
        "wrong first credential",
    )?;
    release.0.release();
    check(
        header(&peer.request().await?, "authorization") == Some("Bearer fake-T2"),
        "second credential did not refresh",
    )?;
    let seen = drain(&mut events).await?;
    let failed: Vec<_> = seen
        .iter()
        .filter(|e| matches!(e.kind, Kind::AttemptFailed { .. }))
        .collect();
    check(
        failed.len() == 2 && calls.load(Ordering::SeqCst) == 2,
        "unexpected retry/provider count",
    )?;
    let first = failed
        .first()
        .ok_or_else(|| error("missing first failure"))?;
    let second = failed
        .get(1)
        .ok_or_else(|| error("missing second failure"))?;
    for event in [first, second] {
        let supplied = bounded("provider attempt metadata", attempt_rx.recv())
            .await?
            .ok_or_else(|| error("missing provider attempt metadata"))?;
        let Kind::AttemptFailed { attempt, .. } = &event.kind else {
            return Err(error("expected per-attempt failure"));
        };
        check(
            event.client_id == supplied.client_id
                && event.session_id == supplied.session_id
                && attempt.cycle_id == supplied.cycle_id
                && attempt.attempt_id == supplied.attempt_id,
            "event metadata does not match the actual provider invocation",
        )?;
    }
    let Kind::AttemptFailed {
        attempt: first_attempt,
        credential_version: first_version,
        error: first_error,
        ..
    } = &first.kind
    else {
        return Err(error("missing first AttemptFailed data"));
    };
    let Kind::AttemptFailed {
        attempt: second_attempt,
        credential_version: second_version,
        error: second_error,
        ..
    } = &second.kind
    else {
        return Err(error("missing second AttemptFailed data"));
    };
    check(
        first_version.as_deref() == Some("51") && second_version.as_deref() == Some("52"),
        "events lost credential revisions",
    )?;
    check(
        first_attempt.attempt_id != second_attempt.attempt_id
            && first_attempt.cycle_id == second_attempt.cycle_id,
        "wrong cycle/attempt identities",
    )?;
    check(
        first_error
            .context()
            .http_status
            .map(|status| status.as_u16())
            == Some(503),
        "first HTTP status changed",
    )?;
    check(
        second_error
            .context()
            .http_status
            .map(|status| status.as_u16())
            == Some(401),
        "second HTTP status changed",
    )?;
    events.session.notify_network_available();
    check(
        bounded("terminal events", events.recv()).await??.is_none(),
        "terminal session produced another event",
    )?;
    check(
        tokio::time::timeout(Duration::from_millis(60), peer.requests.recv())
            .await
            .is_err(),
        "network notification revived a terminal context session",
    )?;
    check(peer.count() == 2, "terminal auth session revived")?;
    bounded(
        "destroy client",
        net.destroy_ws_client("token-refresh-context"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn status_matrix_stops_auth_failures_and_retries_transient_http_failures() -> TestResult {
    for status in [401, 403, 408, 429, 500, 503] {
        let peer = Peer::start(vec![status, status]).await?;
        let net = OpenNet::new()?;
        let client = make_client(&net, "handshake-status-matrix").await?;
        let mut events = session::observe(&client, {
            let mut options = options(&peer.url);
            options.reconnect = policy(1);
            options
        })
        .await?;
        let seen = drain(&mut events).await?;
        let expected = if matches!(status, 401 | 403) { 1 } else { 2 };
        check(
            peer.count() == expected
                && count(&seen, |kind| matches!(kind, Kind::AttemptFailed { .. })) == expected,
            "wrong HTTP retry count",
        )?;
        check(
            count(&seen, |kind| matches!(kind, Kind::Closed { .. })) == 1,
            "HTTP session terminated incorrectly",
        )?;
        check(
            seen.iter()
                .filter(|e| matches!(e.kind, Kind::AttemptFailed { .. }))
                .all(|e| matches!(&e.kind, Kind::AttemptFailed { error, .. } if error.context().http_status.map(|status| status.as_u16()) == Some(status))),
            "HTTP result status lost",
        )?;
        bounded(
            "destroy client",
            net.destroy_ws_client("handshake-status-matrix"),
        )
        .await??;
    }
    Ok(())
}

#[tokio::test]
async fn transient_http_then_local_provider_error_does_not_inherit_http_status() -> TestResult {
    let peer = Peer::start(vec![503]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "no-stale-http-status").await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::clone(&calls);
    let mut events = session::observe(&client, {
        let mut options = {
            let mut options = options(&peer.url);
            options.reconnect = policy(2);
            options
        };
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(move |_| {
            if provider_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(open_net::ws::HandshakeHeaders {
                    headers: open_net::HeaderMap::new(),
                    credential_version: Some((71).to_string()),
                })
            } else {
                Err(NetError::from(open_net::error::ErrorKind::ProviderFailed).into())
            }
        }));
        options
    })
    .await?;
    let seen = drain(&mut events).await?;
    let failures: Vec<_> = seen
        .iter()
        .filter(|e| matches!(e.kind, Kind::AttemptFailed { .. }))
        .collect();
    check(failures.len() == 2, "missing local second attempt")?;
    let last = failures
        .last()
        .and_then(|event| match &event.kind {
            Kind::AttemptFailed { error, .. } => Some(error),
            _ => None,
        })
        .ok_or_else(|| error("missing last failure"))?;
    check(
        last.kind() == open_net::error::ErrorKind::ProviderFailed
            && last
                .context()
                .http_status
                .map(|status| status.as_u16())
                .is_none(),
        "local error inherited HTTP status",
    )?;
    check(peer.count() == 1, "local second attempt opened socket")?;
    bounded(
        "destroy client",
        net.destroy_ws_client("no-stale-http-status"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn event_capacity_backpressure_allows_cancel_and_exactly_one_terminal_event() -> TestResult {
    let mut peer = Peer::start(vec![503, 503]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "bounded-context-events").await?;
    let mut events = session::observe_with(
        &client,
        {
            let mut options = options(&peer.url);
            options.reconnect = policy(20);
            options
        },
        open_net::ws::JournalOptions {
            max_events: 4,
            ..Default::default()
        },
    )
    .await?;
    peer.request().await?;
    check(
        tokio::time::timeout(Duration::from_millis(60), peer.requests.recv())
            .await
            .is_err(),
        "minimum event capacity allowed another handshake before consumption",
    )?;
    events.session.cancel();
    let seen = drain(&mut events).await?;
    check(
        count(&seen, |kind| matches!(kind, Kind::Closed { .. })) == 1,
        "full queue lost or duplicated terminal",
    )?;
    check(
        peer.count() == 1,
        "full queue started another network attempt",
    )?;
    events.session.cancel();
    check(events.recv().await?.is_none(), "repeat cancel added events")?;
    bounded(
        "destroy client",
        net.destroy_ws_client("bounded-context-events"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn blocked_provider_cancel_is_bounded_and_late_snapshot_cannot_open_network() -> TestResult {
    let peer = Peer::start(vec![101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "blocked-context-provider").await?;
    let gate = Gate::new();
    let release = Release(gate.clone());
    let (start_tx, mut start_rx) = mpsc::unbounded_channel();
    let (finish_tx, mut finish_rx) = mpsc::unbounded_channel();
    let mut events = session::observe(&client, {
        let mut options = options(&peer.url);
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(move |_| {
            start_tx
                .send(())
                .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?;
            gate.wait()?;
            finish_tx
                .send(())
                .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?;
            Ok(open_net::ws::HandshakeHeaders {
                headers: open_net::HeaderMap::new(),
                credential_version: Some((91).to_string()),
            })
        }));
        options
    })
    .await?;
    check(
        bounded("provider starts", start_rx.recv()).await?.is_some(),
        "provider did not start",
    )?;
    events.session.cancel();
    let seen = drain(&mut events).await?;
    check(
        count(&seen, |kind| matches!(kind, Kind::Established { .. })) == 0,
        "cancelled provider established",
    )?;
    release.0.release();
    check(
        bounded("provider exits", finish_rx.recv()).await?.is_some(),
        "provider did not exit",
    )?;
    bounded(
        "destroy provider client",
        net.destroy_ws_client("blocked-context-provider"),
    )
    .await??;
    check(peer.count() == 0, "late provider opened network")?;
    Ok(())
}

#[tokio::test]
async fn old_receiver_drop_does_not_cancel_new_session_and_recreation_changes_instance(
) -> TestResult {
    let mut peer = Peer::start(vec![101, 101, 101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "context-instance").await?;
    let mut old = session::observe(&client, options(&peer.url)).await?;
    peer.request().await?;
    let original = next_attempt_result(&mut old).await?;
    old.session.cancel();
    drain(&mut old).await?;
    let mut current = session::observe(&client, options(&peer.url)).await?;
    peer.request().await?;
    let established = next_attempt_result(&mut current).await?;
    check(
        established.session_id != original.session_id,
        "session ID reused",
    )?;
    drop(old);
    check(
        matches!(
            current.session.state()?.state,
            open_net::ws::ConnectionState::Connected(_)
        ),
        "old handle cancelled new session",
    )?;
    current.session.cancel();
    drain(&mut current).await?;
    bounded("destroy client", net.destroy_ws_client("context-instance")).await??;
    let recreated = make_client(&net, "context-instance").await?;
    let mut last = session::observe(&recreated, options(&peer.url)).await?;
    peer.request().await?;
    let final_established = next_attempt_result(&mut last).await?;
    check(
        final_established.client_id != original.client_id,
        "recreated instance ID reused",
    )?;
    drop(last);
    bounded(
        "destroy dropped receiver",
        net.destroy_ws_client("context-instance"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn invalid_context_options_fail_before_session_acceptance() -> TestResult {
    let peer = Peer::start(vec![101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "invalid-context-options").await?;
    for max_events in [0, 2, usize::MAX] {
        let invalid = open_net::ws::JournalOptions {
            max_events,
            ..Default::default()
        };
        check(
            matches!(bounded("invalid journal", session::observe_with(&client, options(&peer.url), invalid)).await?, Err(error) if error.kind() == open_net::error::ErrorKind::InvalidConfig),
            "invalid journal accepted",
        )?;
    }
    let mut invalid = options(&peer.url);
    invalid.handshake_timeout = Duration::MAX;
    check(
        matches!(bounded("invalid options", session::observe(&client, invalid)).await?, Err(error) if error.kind() == open_net::error::ErrorKind::InvalidConfig),
        "invalid options accepted",
    )?;
    check(peer.count() == 0, "invalid options reached TCP")?;
    bounded(
        "destroy client",
        net.destroy_ws_client("invalid-context-options"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn static_headers_keep_the_same_value_on_retry() -> TestResult {
    let mut peer = Peer::start(vec![503, 101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "static-headers").await?;
    let session = bounded(
        "static header session retry",
        SessionGuard::establish(&client, {
            let mut options = {
                let mut options = options(&peer.url);
                options.headers = open_net::HeaderMap::from_iter([(
                    open_net::HeaderName::from_bytes(("authorization").as_bytes())?,
                    open_net::HeaderValue::from_str("Bearer fake-static")?,
                )]);
                options
            };
            options.reconnect = policy(1);
            options
        }),
    )
    .await??;
    for _ in 0..2 {
        check(
            header(&peer.request().await?, "authorization") == Some("Bearer fake-static"),
            "static header changed on retry",
        )?;
    }
    bounded("destroy client", net.destroy_ws_client("static-headers")).await??;
    session.finish().await?;
    Ok(())
}

#[tokio::test]
async fn transient_http_then_upgrade_timeout_does_not_inherit_previous_status() -> TestResult {
    let peer = Peer::start(vec![503, 0]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "http-then-timeout").await?;
    let mut events = session::observe(&client, {
        let mut connect_options = {
            let mut options = options(&peer.url);
            options.reconnect = policy(1);
            options
        };
        connect_options.handshake_timeout = Duration::from_millis(80);
        connect_options
    })
    .await?;
    let seen = drain(&mut events).await?;
    let failed: Vec<_> = seen
        .iter()
        .filter(|event| matches!(event.kind, Kind::AttemptFailed { .. }))
        .collect();
    check(
        failed.len() == 2 && peer.count() == 2,
        "timeout regression did not make two attempts",
    )?;
    let last = failed
        .last()
        .and_then(|event| match &event.kind {
            Kind::AttemptFailed { error, .. } => Some(error),
            _ => None,
        })
        .ok_or_else(|| error("missing timeout details"))?;
    check(
        last.kind() == open_net::error::ErrorKind::TimedOut
            && last
                .context()
                .http_status
                .map(|status| status.as_u16())
                .is_none(),
        "timeout inherited earlier HTTP status",
    )?;
    bounded("destroy client", net.destroy_ws_client("http-then-timeout")).await??;
    Ok(())
}

#[tokio::test]
async fn session_owner_drop_revokes_an_in_progress_handshake_without_a_followup_command(
) -> TestResult {
    let mut peer = Peer::start(vec![0]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "receiver-drop-cancel").await?;
    let events = session::observe(&client, options(&peer.url)).await?;
    peer.request().await?;
    let mut state = events.session.watch_state()?;
    drop(events);
    bounded("Session owner drop reaches worker", async {
        loop {
            if matches!(
                state.current().state,
                open_net::ws::ConnectionState::Closed(_)
            ) {
                return Ok::<(), TestError>(());
            }
            if state.recv().await?.is_none() {
                return Err(error("state ended without Closed"));
            }
        }
    })
    .await??;
    bounded(
        "destroy client",
        net.destroy_ws_client("receiver-drop-cancel"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_with_unconsumed_event_capacity_preserves_terminal_delivery() -> TestResult {
    let mut peer = Peer::start(vec![503]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "shutdown-full-events").await?;
    let mut events = session::observe_with(
        &client,
        {
            let mut options = options(&peer.url);
            options.reconnect = policy(20);
            options
        },
        open_net::ws::JournalOptions {
            max_events: 4,
            ..Default::default()
        },
    )
    .await?;
    peer.request().await?;
    bounded(
        "shutdown full events",
        net.destroy_ws_client("shutdown-full-events"),
    )
    .await??;
    let seen = drain(&mut events).await?;
    check(
        count(&seen, |kind| matches!(kind, Kind::Closed { .. })) == 1,
        "shutdown lost terminal result",
    )?;
    check(
        peer.count() == 1,
        "event queue started another attempt before shutdown",
    )?;
    Ok(())
}

#[tokio::test]
async fn cancelled_provider_retains_its_client_quota_until_the_actual_closure_exits() -> TestResult
{
    let peer = Peer::start(vec![101]).await?;
    let net = OpenNet::new()?;
    let client = make_single_provider_client(&net, "provider-quota").await?;
    let gate = Gate::new();
    let release = Release(gate.clone());
    let (first_tx, mut first_rx) = mpsc::unbounded_channel();
    let (finished_tx, mut finished_rx) = mpsc::unbounded_channel();
    let mut first = session::observe(&client, {
        let mut options = options(&peer.url);
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(move |_| {
            first_tx
                .send(())
                .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?;
            gate.wait()?;
            finished_tx
                .send(())
                .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?;
            Ok(open_net::ws::HandshakeHeaders {
                headers: open_net::HeaderMap::new(),
                credential_version: Some((141).to_string()),
            })
        }));
        options
    })
    .await?;
    check(
        bounded("first provider started", first_rx.recv())
            .await?
            .is_some(),
        "first provider did not start",
    )?;
    first.session.cancel();
    drain(&mut first).await?;
    let (second_tx, mut second_rx) = mpsc::unbounded_channel();
    let replacement = open_net::ws::HandshakeProvider::blocking(move |_| {
        second_tx
            .send(())
            .map_err(|_| error("second provider observer closed"))?;
        Ok(open_net::ws::HandshakeHeaders {
            headers: open_net::HeaderMap::new(),
            credential_version: Some("143".to_owned()),
        })
    });
    let mut rejected = session::observe(&client, {
        let mut options = options(&peer.url);
        options.handshake_provider = Some(replacement.clone());
        options
    })
    .await?;
    let rejected_events = drain(&mut rejected).await?;
    let failure = rejected_events
        .iter()
        .find_map(|event| match &event.kind {
            Kind::AttemptFailed { error, .. } => Some(error),
            _ => None,
        })
        .ok_or("occupied provider slot did not produce a failure")?;
    check(
        failure.kind() == open_net::error::ErrorKind::ResourceExhausted
            && failure.context().stage == Some(ErrorStage::Provider)
            && failure
                .context()
                .http_status
                .map(|status| status.as_u16())
                .is_none()
            && matches!(second_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty))
            && peer.count() == 0,
        "cancelled provider released its slot before the closure exited",
    )?;
    release.0.release();
    check(
        bounded("first provider exits", finished_rx.recv())
            .await?
            .is_some(),
        "first provider did not exit",
    )?;
    // The closure's completion signal precedes the worker's permit destructor.
    // Observe admission itself rather than assuming a scheduler timing delay.
    let mut second = bounded("admit after the actual blocking job retires", async {
        loop {
            let mut events = session::observe(&client
                ,
                    { let mut options = options(&peer.url); options.handshake_provider = Some(replacement.clone()); options },
                )
                .await?;
            let event = next_attempt_result(&mut events).await?;
            if matches!(event.kind, Kind::Established { .. }) {
                check(
                    matches!(&event.kind, Kind::Established { connection } if connection.credential_version.as_deref() == Some("143")),
                    "replacement credential changed",
                )?;
                return Ok::<_, TestError>(events);
            }
            let Kind::AttemptFailed { error: failure, .. } = &event.kind else {
                return Err(error("replacement omitted AttemptFailed details"));
            };
            check(
                matches!(event.kind, Kind::AttemptFailed { .. })
                    && failure.kind() == open_net::error::ErrorKind::ResourceExhausted
                    && failure.context().stage == Some(ErrorStage::Provider),
                "replacement failed for a reason other than the retiring permit",
            )?;
            drain(&mut events).await?;
            tokio::task::yield_now().await;
        }
    })
    .await??;
    check(
        bounded("second provider started", second_rx.recv())
            .await?
            .is_some(),
        "released quota did not admit the replacement provider",
    )?;
    check(
        peer.count() == 1,
        "cancelled first provider opened a stale socket",
    )?;
    second.session.cancel();
    drain(&mut second).await?;
    bounded("destroy client", net.destroy_ws_client("provider-quota")).await??;
    Ok(())
}

#[test]
fn context_option_and_snapshot_debug_do_not_expose_credentials_or_full_urls() -> TestResult {
    const SECRET: &str = "fake-secret-that-must-not-appear-in-debug";
    let config = {
        let mut options = options(&format!("wss://localhost/path?token={SECRET}"));
        options.headers = open_net::HeaderMap::from_iter([(
            open_net::HeaderName::from_bytes(("Authorization").as_bytes())?,
            open_net::HeaderValue::from_str(SECRET)?,
        )]);
        options
    };
    let snapshot = open_net::ws::HandshakeHeaders {
        headers: open_net::HeaderMap::from_iter([(
            open_net::HeaderName::from_bytes(("Authorization").as_bytes())?,
            open_net::HeaderValue::from_str(SECRET)?,
        )]),
        credential_version: Some((151).to_string()),
    };
    check(
        !format!("{config:?}").contains(SECRET),
        "options Debug exposed a secret",
    )?;
    check(
        !format!("{snapshot:?}").contains(SECRET),
        "snapshot Debug exposed a secret",
    )?;
    Ok(())
}

#[tokio::test]
async fn automatic_reconnect_preserves_session_and_refreshes_context_for_a_new_cycle() -> TestResult
{
    let mut peer = Peer::start(vec![101, 101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "context-automatic-reconnect").await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::clone(&calls);
    let mut events = session::observe(&client, {
        let mut options = {
            let mut options = options(&peer.url);
            options.reconnect = policy(2);
            options
        };
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(move |_| {
            let context = match provider_calls.fetch_add(1, Ordering::SeqCst) {
                0 => 161,
                _ => 162,
            };
            Ok(open_net::ws::HandshakeHeaders {
                headers: open_net::HeaderMap::new(),
                credential_version: Some((context).to_string()),
            })
        }));
        options
    })
    .await?;
    peer.request().await?;
    let first = next_attempt_result(&mut events).await?;
    check(
        matches!(first.kind, Kind::Established { .. }),
        "initial session did not establish",
    )?;
    peer.close_connections.send(())?;
    let ended = next(&mut events).await?;
    check(
        matches!(ended.kind, Kind::Disconnected { .. }),
        "physical close omitted its event",
    )?;
    peer.request().await?;
    let second = next_attempt_result(&mut events).await?;
    check(
        matches!(second.kind, Kind::Established { .. }),
        "automatic reconnect did not establish",
    )?;
    let Kind::Established {
        connection: first_connection,
    } = &first.kind
    else {
        return Err(error("first Established payload missing"));
    };
    let Kind::Established {
        connection: second_connection,
    } = &second.kind
    else {
        return Err(error("second Established payload missing"));
    };
    let Kind::Disconnected {
        connection: ended_connection,
        ..
    } = &ended.kind
    else {
        return Err(error("Disconnected payload missing"));
    };
    check(
        ended_connection.connection_id == first_connection.connection_id,
        "disconnect changed physical identity",
    )?;
    check(
        first.client_id == second.client_id && first.session_id == second.session_id,
        "automatic reconnect changed session identity",
    )?;
    check(
        first_connection.cycle_id != second_connection.cycle_id
            && first_connection.connection_id != second_connection.connection_id,
        "automatic reconnect reused old cycle",
    )?;
    check(
        first_connection.credential_version.as_deref() == Some("161")
            && second_connection.credential_version.as_deref() == Some("162"),
        "automatic reconnect did not refresh provider context",
    )?;
    check(
        first.sequence.checked_add(1) == Some(ended.sequence)
            && ended.sequence.checked_add(2) == Some(second.sequence),
        "automatic reconnect event sequence skipped",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 2,
        "provider was not called once per physical attempt",
    )?;
    events.session.cancel();
    drain(&mut events).await?;
    bounded(
        "destroy auto reconnect",
        net.destroy_ws_client("context-automatic-reconnect"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn established_event_allows_immediate_reject_policy_send_without_state_polling() -> TestResult
{
    let mut peer = Peer::start(Vec::new()).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "established-send-ready").await?;
    for _ in 0..16 {
        let mut events = session::observe(&client, options(&peer.url)).await?;
        let established = next_attempt_result(&mut events).await?;
        check(
            matches!(established.kind, Kind::Established { .. }),
            "session did not establish",
        )?;
        // The default disconnected policy is Reject. Do not poll status or
        // yield before this first enqueue: Established must already mean ready.
        let _receipt = events.session.sender().try_enqueue("immediate-enqueue")?;
        bounded(
            "write immediately after Established",
            events.session.sender().send("immediate-write"),
        )
        .await??;
        peer.request().await?;
        events.session.cancel();
        drain(&mut events).await?;
    }
    bounded(
        "destroy send-ready client",
        net.destroy_ws_client("established-send-ready"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn timed_out_provider_and_rejected_quota_sessions_cannot_accumulate_threads() -> TestResult {
    let peer = Peer::start(vec![101]).await?;
    let net = OpenNet::new()?;
    let client = make_single_provider_client(&net, "timed-out-provider-quota").await?;
    let gate = Gate::new();
    let release = Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::clone(&calls);
    let (finish_tx, mut finish_rx) = mpsc::unbounded_channel();
    let provider: open_net::ws::HandshakeProvider =
        open_net::ws::HandshakeProvider::blocking(move |_| {
            provider_calls.fetch_add(1, Ordering::SeqCst);
            gate.wait()?;
            finish_tx
                .send(())
                .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?;
            Ok(open_net::ws::HandshakeHeaders {
                headers: open_net::HeaderMap::new(),
                credential_version: Some((181).to_string()),
            })
        });
    for session in 180..183 {
        let mut events = session::observe(&client, {
            let mut options = {
                let mut connect_options = {
                    let mut options = options(&peer.url);
                    options.reconnect = policy(0);
                    options
                };
                connect_options.handshake_timeout = Duration::from_millis(60);
                connect_options
            };
            options.handshake_provider = Some(provider.clone());
            options
        })
        .await?;
        let seen = drain(&mut events).await?;
        check(
            count(&seen, |kind| matches!(kind, Kind::AttemptFailed { .. })) == 1,
            "provider timeout omitted attempt result",
        )?;
        let failure = seen
            .iter()
            .find_map(|event| match &event.kind {
                Kind::AttemptFailed { error, .. } => Some(error),
                _ => None,
            })
            .ok_or_else(|| error("missing provider timeout details"))?;
        check(
            failure.kind()
                == if session == 180 {
                    open_net::error::ErrorKind::TimedOut
                } else {
                    open_net::error::ErrorKind::ResourceExhausted
                }
                && failure.context().stage == Some(ErrorStage::Provider)
                && failure
                    .context()
                    .http_status
                    .map(|status| status.as_u16())
                    .is_none(),
            "provider deadline or full-quota rejection has wrong error",
        )?;
    }
    check(
        calls.load(Ordering::SeqCst) == 1,
        "timeouts accumulated blocked provider threads",
    )?;
    release.0.release();
    check(
        bounded("timed-out provider exits", finish_rx.recv())
            .await?
            .is_some(),
        "provider did not exit after release",
    )?;
    bounded(
        "destroy quota client",
        net.destroy_ws_client("timed-out-provider-quota"),
    )
    .await??;
    check(
        peer.count() == 0,
        "expired provider result opened stale network",
    )?;
    Ok(())
}

#[tokio::test]
async fn dynamic_provider_refresh_and_local_rejection_use_observed_sessions() -> TestResult {
    let mut peer = Peer::start(vec![503, 101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "dynamic-provider").await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::clone(&calls);
    let session = bounded(
        "dynamic session",
        SessionGuard::establish(&client, {
            let mut options = {
                let mut options = {
                    let mut options = options(&peer.url);
                    options.headers = open_net::HeaderMap::from_iter([(
                        open_net::HeaderName::from_bytes(("Authorization").as_bytes())?,
                        open_net::HeaderValue::from_str("fake-static")?,
                    )]);
                    options
                };
                options.handshake_provider =
                    Some(open_net::ws::HandshakeProvider::blocking(move |_| {
                        if tokio::runtime::Handle::try_current().is_ok() {
                            return Err(NetError::from(
                                open_net::error::ErrorKind::RuntimeUnavailable,
                            )
                            .into());
                        }
                        let token = if provider_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            "fake-dynamic-T1"
                        } else {
                            "fake-dynamic-T2"
                        };
                        Ok(open_net::ws::HandshakeHeaders {
                            headers: open_net::HeaderMap::from_iter([(
                                open_net::HeaderName::from_bytes(("authorization").as_bytes())?,
                                open_net::HeaderValue::from_str(token)?,
                            )]),
                            credential_version: Some((183).to_string()),
                        })
                    }));
                options
            };
            options.reconnect = policy(1);
            options
        }),
    )
    .await??;
    check(
        header(&peer.request().await?, "authorization") == Some("fake-dynamic-T1"),
        "first credential changed",
    )?;
    check(
        header(&peer.request().await?, "authorization") == Some("fake-dynamic-T2"),
        "dynamic provider did not refresh",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 2,
        "provider invocation count changed",
    )?;
    bounded(
        "destroy dynamic client",
        net.destroy_ws_client("dynamic-provider"),
    )
    .await??;
    session.finish().await?;
    let untouched = Peer::start(vec![101]).await?;
    let rejected = make_client(&net, "local-provider").await?;
    let mut events = session::observe(&rejected, {
        let mut options = {
            let mut options = options(&untouched.url);
            options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(|_| {
                Err(NetError::from(open_net::error::ErrorKind::ProviderFailed).into())
            }));
            options
        };
        options.reconnect = policy(3);
        options
    })
    .await?;
    let received = drain(&mut events).await?;
    let terminal = received
        .last()
        .ok_or_else(|| error("provider rejection omitted terminal event"))?;
    check(
        matches!(&terminal.kind, Kind::Closed { result: Err(error) } if error.kind() == open_net::error::ErrorKind::ProviderFailed),
        "provider rejection lost its actual error",
    )?;
    check(
        untouched.count() == 0
            && matches!(&terminal.kind, Kind::Closed { result: Err(error) } if error.context().http_status.is_none()),
        "local rejection reached network or gained HTTP status",
    )?;
    bounded(
        "destroy rejected client",
        net.destroy_ws_client("local-provider"),
    )
    .await??;
    Ok(())
}

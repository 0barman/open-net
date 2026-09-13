#![cfg(feature = "ws-client")]

use open_net::{
    ConnectionStatus, NetError, OpenNet, ReconnectPolicy, WebSocketClient, WebSocketClientConfig,
    WebSocketConnectOptions, WebSocketConnectionEvent, WebSocketConnectionEventKind as Kind,
    WebSocketConnectionEvents, WebSocketContextConnectOptions, WebSocketHandshakeSnapshot,
};
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
fn error(message: impl Into<String>) -> TestError {
    std::io::Error::other(message.into()).into()
}
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
    ReconnectPolicy {
        enabled: retries > 0,
        max_retries: retries,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(1),
        max_elapsed: Some(Duration::from_secs(3)),
        handshake_timeout: Duration::from_secs(2),
    }
}
async fn make_client(net: &OpenNet, name: &str) -> TestResult<WebSocketClient> {
    Ok(bounded(
        "create client",
        net.create_ws_client_with_config(
            name,
            WebSocketClientConfig {
                close_timeout: Duration::from_millis(30),
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??)
}
fn options(url: &str, session: u64, attempt: u64) -> WebSocketContextConnectOptions {
    WebSocketContextConnectOptions::new(url, session)
        .with_headers(Vec::new(), attempt)
        .with_reconnect(policy(0))
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
                    format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n", derive_accept_key(key.as_bytes()))
                } else {
                    format!("HTTP/1.1 {status} Test Response\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
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
async fn next(events: &mut WebSocketConnectionEvents) -> TestResult<WebSocketConnectionEvent> {
    bounded("receive event", events.recv())
        .await??
        .ok_or_else(|| error("events ended early"))
}
async fn drain(
    events: &mut WebSocketConnectionEvents,
) -> TestResult<Vec<WebSocketConnectionEvent>> {
    let mut result = Vec::new();
    let mut sequence: Option<u64> = None;
    let mut terminal = false;
    while let Some(event) = bounded("drain events", events.recv()).await?? {
        check(!terminal, "event appeared after SessionTerminated")?;
        if let Some(previous) = sequence {
            check(
                previous.checked_add(1) == Some(event.sequence()),
                "event sequence is not contiguous",
            )?;
        }
        sequence = Some(event.sequence());
        terminal = event.kind() == Kind::SessionTerminated;
        result.push(event);
        check(result.len() <= 100, "unbounded event production")?;
    }
    check(terminal, "event stream omitted SessionTerminated")?;
    Ok(result)
}
fn count(events: &[WebSocketConnectionEvent], kind: Kind) -> usize {
    events.iter().filter(|event| event.kind() == kind).count()
}
#[derive(Clone)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);
impl Gate {
    fn new() -> Self {
        Self(Arc::new((Mutex::new(false), Condvar::new())))
    }
    fn wait(&self) -> Result<(), NetError> {
        let (lock, condition) = self.0.as_ref();
        let mut released = lock.lock().map_err(|_| NetError::InternalError)?;
        while !*released {
            released = condition
                .wait(released)
                .map_err(|_| NetError::InternalError)?;
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
async fn request_scope_cancel_stops_a_blocked_provider_before_its_deadline() -> TestResult {
    let peer = Peer::start(vec![101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "scope-provider-cancel").await?;
    let scope = open_net::api::wsc::RequestScope::new();
    let gate = Gate::new();
    let release = Release(gate.clone());
    let (started, mut starts) = mpsc::unbounded_channel();
    let mut connect_options = options(&peer.url, 200, 201)
        .with_request_scope(scope.clone())
        .with_header_provider(Arc::new(move |_| {
            started.send(()).map_err(|_| NetError::InternalError)?;
            gate.wait()?;
            Ok(WebSocketHandshakeSnapshot::new(Vec::new(), 201))
        }));
    let mut long_attempt = policy(0);
    long_attempt.handshake_timeout = Duration::from_secs(60);
    connect_options = connect_options.with_reconnect(long_attempt);
    let mut events = client.start_connect_with_context(connect_options).await?;
    check(
        bounded("provider starts", starts.recv()).await?.is_some(),
        "provider never started",
    )?;
    scope.cancel();
    let seen = tokio::time::timeout(Duration::from_millis(500), drain(&mut events))
        .await
        .map_err(|e| error(format!("scope did not interrupt provider: {e}")))??;
    check(
        count(&seen, Kind::Established) == 0,
        "cancelled provider established a connection",
    )?;
    check(
        client.connection_status() == ConnectionStatus::Idle,
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
async fn request_scope_cancel_stops_upgrade_and_cannot_revoke_the_next_scope() -> TestResult {
    let mut peer = Peer::start(vec![0, 101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "scope-upgrade-cancel").await?;
    let first_scope = open_net::api::wsc::RequestScope::new();
    let mut first = client
        .start_connect_with_context(
            options(&peer.url, 202, 203).with_request_scope(first_scope.clone()),
        )
        .await?;
    peer.request().await?;
    first_scope.cancel();
    let seen = tokio::time::timeout(Duration::from_millis(500), drain(&mut first))
        .await
        .map_err(|e| error(format!("scope did not interrupt Upgrade: {e}")))??;
    check(
        count(&seen, Kind::Established) == 0,
        "retired Upgrade was established",
    )?;
    let second_scope = open_net::api::wsc::RequestScope::new();
    let mut second = client
        .start_connect_with_context(
            options(&peer.url, 202, 204).with_request_scope(second_scope.clone()),
        )
        .await?;
    let established = next(&mut second).await?;
    check(
        established.kind() == Kind::Established,
        "new scope did not establish",
    )?;
    first_scope.cancel();
    check(!second_scope.is_cancelled(), "old scope revoked new owner")?;
    check(
        client.connection_status() == ConnectionStatus::Connected,
        "old scope retired new connection",
    )?;
    bounded("cancel new scoped session", second.cancel()).await??;
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
        client.start_connect_with_context(options(&peer.url, 7, 11)),
    )
    .await??;
    peer.request().await?;
    bounded("cancel session", events.cancel()).await??;
    let seen = drain(&mut events).await?;
    check(
        count(&seen, Kind::AttemptFailed) == 1,
        "cancelled attempt omitted its result",
    )?;
    check(
        count(&seen, Kind::SessionTerminated) == 1,
        "session ended more than once",
    )?;
    check(
        seen.iter().all(|e| e.session_context_id() == 7),
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
    let headers: Vec<(String, String)> = [
        ("authorization", "Bearer fake-token"),
        ("x-md-global-app-version", "1.2.3"),
        ("x-md-global-language", "zh-CN"),
        ("x-md-global-region", ""),
        ("x-md-region", "SG"),
        ("x-md-global-device-fp", "fake-device"),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), v.into()))
    .collect();
    let expected = headers.clone();
    let mut events = client
        .start_connect_with_context(
            options(&url, 20, 21)
                .with_headers(vec![("Authorization".into(), "old".into())], 21)
                .with_header_provider(Arc::new(move |_| {
                    Ok(WebSocketHandshakeSnapshot::new(headers.clone(), 22))
                })),
        )
        .await?;
    let request = peer.request().await?;
    check(
        request.starts_with("GET /cloud/path?mode=test HTTP/1.1\r\n"),
        "path/query changed",
    )?;
    for (name, value) in expected {
        check(
            header(&request, &name) == Some(value.as_str()),
            "business header missing or changed",
        )?;
    }
    let established = next(&mut events).await?;
    check(
        established.kind() == Kind::Established,
        "header handshake did not establish",
    )?;
    check(
        established.attempt_context_id() == Some(22),
        "headers and context diverged",
    )?;
    bounded("cancel session", events.cancel()).await??;
    drain(&mut events).await?;
    bounded("destroy client", net.destroy_ws_client("complete-headers")).await??;
    Ok(())
}

#[tokio::test]
async fn local_provider_errors_never_open_network_and_anonymous_connections_remain_valid(
) -> TestResult {
    for failure in [NetError::NotLoggedInError, NetError::ConfigError] {
        let peer = Peer::start(vec![101]).await?;
        let net = OpenNet::new()?;
        let client = make_client(&net, "provider-local-error").await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let provider_calls = Arc::clone(&calls);
        let mut events = client
            .start_connect_with_context(
                options(&peer.url, 30, 31)
                    .with_reconnect(policy(3))
                    .with_header_provider(Arc::new(move |_| {
                        provider_calls.fetch_add(1, Ordering::SeqCst);
                        Err(failure)
                    })),
            )
            .await?;
        let seen = drain(&mut events).await?;
        check(
            peer.count() == 0 && calls.load(Ordering::SeqCst) == 1,
            "local failure reached network or retried",
        )?;
        check(
            count(&seen, Kind::AttemptFailed) == 1,
            "local failure completed incorrectly",
        )?;
        let details = seen
            .iter()
            .find_map(WebSocketConnectionEvent::failure)
            .ok_or_else(|| error("missing local failure details"))?;
        check(
            details.error() == failure && details.http_status().is_none(),
            "wrong local failure details",
        )?;
        check(
            seen.iter()
                .filter(|e| e.kind() == Kind::AttemptFailed)
                .all(|e| e.attempt_context_id().is_none()),
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
    let mut events = client
        .start_connect_with_context(options(&peer.url, 32, 33))
        .await?;
    check(
        header(&peer.request().await?, "authorization").is_none(),
        "anonymous connection gained Authorization",
    )?;
    check(
        next(&mut events).await?.kind() == Kind::Established,
        "anonymous connection rejected",
    )?;
    bounded("cancel session", events.cancel()).await??;
    drain(&mut events).await?;
    bounded("destroy client", net.destroy_ws_client("anonymous-context")).await??;
    Ok(())
}

#[tokio::test]
async fn illegal_headers_fail_locally_without_retry() -> TestResult {
    for invalid in [
        ("bad header", "value"),
        ("x-good", "value\r\ninjected: yes"),
    ] {
        let peer = Peer::start(vec![101]).await?;
        let net = OpenNet::new()?;
        let client = make_client(&net, "invalid-context-headers").await?;
        let mut events = client
            .start_connect_with_context(
                options(&peer.url, 40, 41)
                    .with_headers(vec![(invalid.0.into(), invalid.1.into())], 41)
                    .with_reconnect(policy(3)),
            )
            .await?;
        let seen = drain(&mut events).await?;
        check(
            peer.count() == 0 && count(&seen, Kind::AttemptFailed) == 1,
            "invalid header reached network or retried",
        )?;
        check(
            seen.iter()
                .filter_map(WebSocketConnectionEvent::failure)
                .all(|f| f.http_status().is_none()),
            "local error gained HTTP status",
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
    let mut events = client
        .start_connect_with_context(
            options(&peer.url, 50, 51)
                .with_reconnect(policy(2))
                .with_header_provider(Arc::new(move |attempt| {
                    let call = provider_calls.fetch_add(1, Ordering::SeqCst);
                    if attempt.session_context_id() != 50
                        || tokio::runtime::Handle::try_current().is_ok()
                    {
                        return Err(NetError::ConfigError);
                    }
                    attempt_tx
                        .send(attempt)
                        .map_err(|_| NetError::InternalError)?;
                    if call > 0 {
                        gate.wait()?;
                    }
                    let (token, context) = if call == 0 {
                        ("Bearer fake-T1", 51)
                    } else {
                        ("Bearer fake-T2", 52)
                    };
                    Ok(WebSocketHandshakeSnapshot::new(
                        vec![("Authorization".into(), token.into())],
                        context,
                    ))
                })),
        )
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
        .filter(|e| e.kind() == Kind::AttemptFailed)
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
        check(
            event.client_instance_id() == supplied.client_instance_id()
                && event.session_id() == supplied.session_id()
                && event.cycle_id() == Some(supplied.cycle_id())
                && event.attempt_id() == Some(supplied.attempt_id()),
            "event metadata does not match the actual provider invocation",
        )?;
    }
    check(
        first.attempt_context_id() == Some(51) && second.attempt_context_id() == Some(52),
        "events lost credential revisions",
    )?;
    check(
        first.attempt_id() != second.attempt_id() && first.cycle_id() == second.cycle_id(),
        "wrong cycle/attempt identities",
    )?;
    check(
        first.failure().and_then(|f| f.http_status()) == Some(503),
        "first HTTP status changed",
    )?;
    check(
        second.failure().and_then(|f| f.http_status()) == Some(401),
        "second HTTP status changed",
    )?;
    client.notify_network_available();
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
        let mut events = client
            .start_connect_with_context(options(&peer.url, 60, 61).with_reconnect(policy(1)))
            .await?;
        let seen = drain(&mut events).await?;
        let expected = if matches!(status, 401 | 403) { 1 } else { 2 };
        check(
            peer.count() == expected && count(&seen, Kind::AttemptFailed) == expected,
            "wrong HTTP retry count",
        )?;
        check(
            count(&seen, Kind::SessionTerminated) == 1,
            "HTTP session terminated incorrectly",
        )?;
        check(
            seen.iter()
                .filter(|e| e.kind() == Kind::AttemptFailed)
                .all(|e| e.failure().and_then(|f| f.http_status()) == Some(status)),
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
    let mut events = client
        .start_connect_with_context(
            options(&peer.url, 70, 71)
                .with_reconnect(policy(2))
                .with_header_provider(Arc::new(move |_| {
                    if provider_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        Ok(WebSocketHandshakeSnapshot::new(Vec::new(), 71))
                    } else {
                        Err(NetError::NotLoggedInError)
                    }
                })),
        )
        .await?;
    let seen = drain(&mut events).await?;
    let failures: Vec<_> = seen
        .iter()
        .filter(|e| e.kind() == Kind::AttemptFailed)
        .collect();
    check(failures.len() == 2, "missing local second attempt")?;
    let last = failures
        .last()
        .and_then(|e| e.failure())
        .ok_or_else(|| error("missing last failure"))?;
    check(
        last.error() == NetError::NotLoggedInError && last.http_status().is_none(),
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
    let mut events = client
        .start_connect_with_context(
            options(&peer.url, 80, 81)
                .with_reconnect(policy(20))
                .with_event_capacity(3),
        )
        .await?;
    peer.request().await?;
    check(
        tokio::time::timeout(Duration::from_millis(60), peer.requests.recv())
            .await
            .is_err(),
        "minimum event capacity allowed another handshake before consumption",
    )?;
    bounded("cancel full event queue", events.cancel()).await??;
    let seen = drain(&mut events).await?;
    check(
        count(&seen, Kind::SessionTerminated) == 1,
        "full queue lost or duplicated terminal",
    )?;
    check(
        peer.count() == 1,
        "full queue started another network attempt",
    )?;
    bounded("repeat cancel", events.cancel()).await??;
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
    let mut events = client
        .start_connect_with_context(options(&peer.url, 90, 91).with_header_provider(Arc::new(
            move |_| {
                start_tx.send(()).map_err(|_| NetError::InternalError)?;
                gate.wait()?;
                finish_tx.send(()).map_err(|_| NetError::InternalError)?;
                Ok(WebSocketHandshakeSnapshot::new(Vec::new(), 91))
            },
        )))
        .await?;
    check(
        bounded("provider starts", start_rx.recv()).await?.is_some(),
        "provider did not start",
    )?;
    bounded("cancel provider", events.cancel()).await??;
    let seen = drain(&mut events).await?;
    check(
        count(&seen, Kind::Established) == 0,
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
    let mut old = client
        .start_connect_with_context(options(&peer.url, 100, 101))
        .await?;
    peer.request().await?;
    let original = next(&mut old).await?;
    bounded("cancel session", old.cancel()).await??;
    drain(&mut old).await?;
    let mut current = client
        .start_connect_with_context(options(&peer.url, 102, 103))
        .await?;
    peer.request().await?;
    let established = next(&mut current).await?;
    check(
        established.session_id() != original.session_id(),
        "session ID reused",
    )?;
    drop(old);
    check(
        client.connection_status() == ConnectionStatus::Connected,
        "old handle cancelled new session",
    )?;
    bounded("cancel session", current.cancel()).await??;
    drain(&mut current).await?;
    bounded("destroy client", net.destroy_ws_client("context-instance")).await??;
    let recreated = make_client(&net, "context-instance").await?;
    let mut last = recreated
        .start_connect_with_context(options(&peer.url, 104, 105))
        .await?;
    peer.request().await?;
    let final_established = next(&mut last).await?;
    check(
        final_established.client_instance_id() != original.client_instance_id(),
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
    for invalid in [
        WebSocketContextConnectOptions::new(&peer.url, 1),
        options(&peer.url, 1, 2).with_event_capacity(0),
        options(&peer.url, 1, 2).with_event_capacity(2),
        options(&peer.url, 1, 2).with_event_capacity(usize::MAX),
        options(&peer.url, 1, 2).with_reconnect(ReconnectPolicy {
            handshake_timeout: Duration::MAX,
            ..policy(0)
        }),
    ] {
        check(
            matches!(
                bounded(
                    "invalid options",
                    client.start_connect_with_context(invalid)
                )
                .await?,
                Err(NetError::ConfigError)
            ),
            "invalid options accepted",
        )?;
    }
    check(peer.count() == 0, "invalid options reached TCP")?;
    bounded(
        "destroy client",
        net.destroy_ws_client("invalid-context-options"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn legacy_static_headers_keep_the_same_value_on_retry() -> TestResult {
    let mut peer = Peer::start(vec![503, 101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "legacy-static-headers").await?;
    bounded(
        "legacy static retry",
        client.connect_with_options(
            &peer.url,
            WebSocketConnectOptions {
                headers: vec![("authorization".into(), "Bearer fake-static".into())],
                header_provider: None,
                reconnect: policy(1),
            },
        ),
    )
    .await??;
    for _ in 0..2 {
        check(
            header(&peer.request().await?, "authorization") == Some("Bearer fake-static"),
            "legacy static header changed",
        )?;
    }
    bounded(
        "destroy client",
        net.destroy_ws_client("legacy-static-headers"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn transient_http_then_upgrade_timeout_does_not_inherit_previous_status() -> TestResult {
    let peer = Peer::start(vec![503, 0]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "http-then-timeout").await?;
    let mut events = client
        .start_connect_with_context(
            options(&peer.url, 110, 111).with_reconnect(ReconnectPolicy {
                handshake_timeout: Duration::from_millis(80),
                ..policy(1)
            }),
        )
        .await?;
    let seen = drain(&mut events).await?;
    let failed: Vec<_> = seen
        .iter()
        .filter(|event| event.kind() == Kind::AttemptFailed)
        .collect();
    check(
        failed.len() == 2 && peer.count() == 2,
        "timeout regression did not make two attempts",
    )?;
    let last = failed
        .last()
        .and_then(|event| event.failure())
        .ok_or_else(|| error("missing timeout details"))?;
    check(
        last.error() == NetError::TimeoutError && last.http_status().is_none(),
        "timeout inherited earlier HTTP status",
    )?;
    bounded("destroy client", net.destroy_ws_client("http-then-timeout")).await??;
    Ok(())
}

#[tokio::test]
async fn receiver_drop_revokes_an_in_progress_handshake_without_a_followup_command() -> TestResult {
    let mut peer = Peer::start(vec![0]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "receiver-drop-cancel").await?;
    let events = client
        .start_connect_with_context(options(&peer.url, 120, 121))
        .await?;
    peer.request().await?;
    drop(events);
    bounded("receiver drop reaches worker", async {
        while client.connection_status() != ConnectionStatus::Idle {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    check(
        client.connection_status() == ConnectionStatus::Idle,
        "receiver drop did not stop session",
    )?;
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
    let mut events = client
        .start_connect_with_context(
            options(&peer.url, 130, 131)
                .with_reconnect(policy(20))
                .with_event_capacity(3),
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
        count(&seen, Kind::SessionTerminated) == 1,
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
    let client = make_client(&net, "provider-quota").await?;
    let gate = Gate::new();
    let release = Release(gate.clone());
    let (first_tx, mut first_rx) = mpsc::unbounded_channel();
    let (finished_tx, mut finished_rx) = mpsc::unbounded_channel();
    let mut first = client
        .start_connect_with_context(options(&peer.url, 140, 141).with_header_provider(Arc::new(
            move |_| {
                first_tx.send(()).map_err(|_| NetError::InternalError)?;
                gate.wait()?;
                finished_tx.send(()).map_err(|_| NetError::InternalError)?;
                Ok(WebSocketHandshakeSnapshot::new(Vec::new(), 141))
            },
        )))
        .await?;
    check(
        bounded("first provider started", first_rx.recv())
            .await?
            .is_some(),
        "first provider did not start",
    )?;
    bounded("cancel first provider session", first.cancel()).await??;
    drain(&mut first).await?;
    let (second_tx, mut second_rx) = mpsc::unbounded_channel();
    let mut second = client
        .start_connect_with_context(options(&peer.url, 142, 143).with_header_provider(Arc::new(
            move |_| {
                second_tx.send(()).map_err(|_| NetError::InternalError)?;
                Ok(WebSocketHandshakeSnapshot::new(Vec::new(), 143))
            },
        )))
        .await?;
    check(
        tokio::time::timeout(Duration::from_millis(60), second_rx.recv())
            .await
            .is_err(),
        "cancelled provider released quota before closure exited",
    )?;
    release.0.release();
    check(
        bounded("first provider exits", finished_rx.recv())
            .await?
            .is_some(),
        "first provider did not exit",
    )?;
    check(
        bounded("second provider starts", second_rx.recv())
            .await?
            .is_some(),
        "released quota did not wake next provider",
    )?;
    let established = next(&mut second).await?;
    check(
        established.kind() == Kind::Established && established.attempt_context_id() == Some(143),
        "second provider result is incorrectly attributed",
    )?;
    check(
        peer.count() == 1,
        "cancelled first provider opened a stale socket",
    )?;
    bounded("cancel session", second.cancel()).await??;
    drain(&mut second).await?;
    bounded("destroy client", net.destroy_ws_client("provider-quota")).await??;
    Ok(())
}

#[test]
fn context_option_and_snapshot_debug_do_not_expose_credentials_or_full_urls() -> TestResult {
    const SECRET: &str = "fake-secret-that-must-not-appear-in-debug";
    let config = options(&format!("wss://localhost/path?token={SECRET}"), 150, 151)
        .with_headers(vec![("Authorization".into(), SECRET.into())], 151);
    let snapshot =
        WebSocketHandshakeSnapshot::new(vec![("Authorization".into(), SECRET.into())], 151);
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
    let mut events = client
        .start_connect_with_context(
            options(&peer.url, 160, 161)
                .with_reconnect(policy(2))
                .with_header_provider(Arc::new(move |_| {
                    let context = match provider_calls.fetch_add(1, Ordering::SeqCst) {
                        0 => 161,
                        _ => 162,
                    };
                    Ok(WebSocketHandshakeSnapshot::new(Vec::new(), context))
                })),
        )
        .await?;
    peer.request().await?;
    let first = next(&mut events).await?;
    check(
        first.kind() == Kind::Established,
        "initial session did not establish",
    )?;
    peer.close_connections.send(())?;
    let ended = next(&mut events).await?;
    check(
        ended.kind() == Kind::ConnectionTerminated,
        "physical close omitted its event",
    )?;
    peer.request().await?;
    let second = next(&mut events).await?;
    check(
        second.kind() == Kind::Established,
        "automatic reconnect did not establish",
    )?;
    check(
        first.client_instance_id() == second.client_instance_id()
            && first.session_id() == second.session_id()
            && first.session_context_id() == second.session_context_id(),
        "automatic reconnect changed session identity",
    )?;
    check(
        first.cycle_id() != second.cycle_id(),
        "automatic reconnect reused old cycle",
    )?;
    check(
        first.attempt_context_id() == Some(161) && second.attempt_context_id() == Some(162),
        "automatic reconnect did not refresh provider context",
    )?;
    check(
        first.sequence().checked_add(1) == Some(ended.sequence())
            && ended.sequence().checked_add(1) == Some(second.sequence()),
        "automatic reconnect event sequence skipped",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 2,
        "provider was not called once per physical attempt",
    )?;
    bounded("cancel auto reconnect session", events.cancel()).await??;
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
    for round in 0..16 {
        let mut events = client
            .start_connect_with_context(options(&peer.url, 170, round))
            .await?;
        let established = next(&mut events).await?;
        check(
            established.kind() == Kind::Established,
            "session did not establish",
        )?;
        // The default disconnected policy is Reject. Do not poll status or
        // yield before this first enqueue: Established must already mean ready.
        client.try_send_message(open_net::WsBody::Text("immediate-enqueue".into()))?;
        bounded(
            "write immediately after Established",
            client.send_message(open_net::WsBody::Text("immediate-write".into())),
        )
        .await??;
        peer.request().await?;
        bounded("cancel send-ready session", events.cancel()).await??;
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
async fn timed_out_provider_and_waiting_quota_sessions_cannot_accumulate_threads() -> TestResult {
    let peer = Peer::start(vec![101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "timed-out-provider-quota").await?;
    let gate = Gate::new();
    let release = Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::clone(&calls);
    let (finish_tx, mut finish_rx) = mpsc::unbounded_channel();
    let provider: open_net::WebSocketHandshakeProvider = Arc::new(move |_| {
        provider_calls.fetch_add(1, Ordering::SeqCst);
        gate.wait()?;
        finish_tx.send(()).map_err(|_| NetError::InternalError)?;
        Ok(WebSocketHandshakeSnapshot::new(Vec::new(), 181))
    });
    for session in 180..183 {
        let mut events = client
            .start_connect_with_context(
                options(&peer.url, session, 181)
                    .with_reconnect(ReconnectPolicy {
                        handshake_timeout: Duration::from_millis(60),
                        ..policy(0)
                    })
                    .with_header_provider(Arc::clone(&provider)),
            )
            .await?;
        let seen = drain(&mut events).await?;
        check(
            count(&seen, Kind::AttemptFailed) == 1,
            "provider timeout omitted attempt result",
        )?;
        let failure = seen
            .iter()
            .find_map(WebSocketConnectionEvent::failure)
            .ok_or_else(|| error("missing provider timeout details"))?;
        check(
            failure.error() == NetError::TimeoutError && failure.http_status().is_none(),
            "provider quota timeout has wrong error",
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
async fn legacy_dynamic_provider_refresh_and_local_rejection_keep_their_original_contract(
) -> TestResult {
    let mut peer = Peer::start(vec![503, 101]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "legacy-dynamic-provider").await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::clone(&calls);
    bounded(
        "legacy dynamic connect",
        client.connect_with_options(
            &peer.url,
            WebSocketConnectOptions {
                headers: vec![("Authorization".into(), "fake-static".into())],
                header_provider: Some(Arc::new(move || {
                    if tokio::runtime::Handle::try_current().is_ok() {
                        return Err(NetError::RuntimeError);
                    }
                    let token = if provider_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        "fake-dynamic-T1"
                    } else {
                        "fake-dynamic-T2"
                    };
                    Ok(vec![("authorization".into(), token.into())])
                })),
                reconnect: policy(1),
            },
        ),
    )
    .await??;
    check(
        header(&peer.request().await?, "authorization") == Some("fake-dynamic-T1"),
        "legacy first credential changed",
    )?;
    check(
        header(&peer.request().await?, "authorization") == Some("fake-dynamic-T2"),
        "legacy dynamic provider did not refresh",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 2,
        "legacy provider invocation count changed",
    )?;
    bounded(
        "destroy legacy dynamic client",
        net.destroy_ws_client("legacy-dynamic-provider"),
    )
    .await??;
    let untouched = Peer::start(vec![101]).await?;
    let rejected = make_client(&net, "legacy-local-provider").await?;
    let result = bounded(
        "legacy local provider error",
        rejected.connect_with_options(
            &untouched.url,
            WebSocketConnectOptions {
                headers: Vec::new(),
                header_provider: Some(Arc::new(|| Err(NetError::NotLoggedInError))),
                reconnect: policy(3),
            },
        ),
    )
    .await?;
    check(
        result == Err(NetError::NotLoggedInError),
        "legacy provider rejection error changed",
    )?;
    check(
        untouched.count() == 0 && rejected.last_handshake_http_status().is_none(),
        "legacy local rejection reached network or gained HTTP status",
    )?;
    bounded(
        "destroy rejected legacy client",
        net.destroy_ws_client("legacy-local-provider"),
    )
    .await??;
    Ok(())
}

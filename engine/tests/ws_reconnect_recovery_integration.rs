#![cfg(feature = "ws-client")]

use open_net::{
    ConnectionStatus, NetError, NetworkConfig, OpenNet, ProxyConfig, ReconnectPolicy,
    WebSocketClient, WebSocketClientConfig, WebSocketConnectOptions, WebSocketConnectionEventKind,
    WebSocketContextConnectOptions, WebSocketTerminationReason,
};
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
        enabled: true,
        max_retries: retries,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(1),
        max_elapsed: Some(Duration::from_secs(3)),
        handshake_timeout: Duration::from_secs(2),
    }
}

fn options(retries: usize) -> WebSocketConnectOptions {
    WebSocketConnectOptions {
        reconnect: policy(retries),
        ..WebSocketConnectOptions::default()
    }
}

fn increment(counter: &AtomicUsize) -> Result<usize, NetError> {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
            value.checked_add(1)
        })
        .map_err(|_| NetError::InternalError)
}

fn client_config() -> WebSocketClientConfig {
    WebSocketClientConfig {
        close_timeout: Duration::from_millis(20),
        ..WebSocketClientConfig::default()
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
                    format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n", derive_accept_key(key.as_bytes()))
                } else {
                    format!("HTTP/1.1 {status} Test Response\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
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
async fn legacy_terminal_http_rejection_blocks_hints_but_allows_explicit_recovery() -> TestResult {
    for status in [401, 403] {
        let mut peer = Peer::start(vec![status]).await?;
        let net = OpenNet::new()?;
        let client = net
            .create_ws_client_with_config("legacy-terminal-http-recovery", client_config())
            .await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let provider_calls = Arc::clone(&calls);
        let initial = WebSocketConnectOptions {
            header_provider: Some(Arc::new(move || {
                increment(&provider_calls)?;
                Ok(Vec::new())
            })),
            ..options(6)
        };
        let outcome: TestResult = async {
            let failed = bounded(
                "initial rejection",
                client.connect_with_options(&peer.url, initial),
            )
            .await?;
            check(
                failed == Err(NetError::ConnectError),
                "legacy HTTP error changed",
            )?;
            check(
                client.last_handshake_http_status() == Some(status),
                "HTTP snapshot changed",
            )?;
            check(
                client.last_connection_error() == Some(NetError::ConnectError),
                "HTTP terminal failure lost its stable error snapshot",
            )?;
            check(peer.count() == 1, "non-retryable HTTP failure was retried")?;
            for _ in 0..8 {
                client.notify_network_available();
            }
            // The explicit command follows every hint in the worker FIFO. Its
            // successful handshake is a completion barrier: a stale hint would
            // have occupied the one connection slot and caused ConnectionExists.
            bounded(
                "explicit recovery after hints",
                client.connect_with_options(&peer.url, options(0)),
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
                client.connection_status() == ConnectionStatus::Connected,
                "explicit recovery did not establish",
            )?;
            check(
                client.last_connection_error().is_none(),
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
        NetError::NotLoggedInError,
        NetError::RetryExhausted,
        NetError::ConfigError,
    ] {
        let mut peer = Peer::start(Vec::new()).await?;
        let net = OpenNet::new()?;
        let client = net
            .create_ws_client_with_config("local-recovery", client_config())
            .await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let provider_calls = Arc::clone(&calls);
        let initial = WebSocketConnectOptions {
            header_provider: Some(Arc::new(move || {
                if increment(&provider_calls)? == 0 {
                    if failure == NetError::ConfigError {
                        return Ok(vec![("authorization".into(), "invalid\r\nvalue".into())]);
                    }
                    return Err(failure);
                }
                Ok(Vec::new())
            })),
            ..options(6)
        };
        let outcome: TestResult = async {
            let failed = bounded(
                "local terminal failure",
                client.connect_with_options(&peer.url, initial),
            )
            .await?;
            check(failed == Err(failure), "provider/header error changed")?;
            check(peer.count() == 0, "local failure opened a network socket")?;
            check(
                client.last_handshake_http_status().is_none(),
                "local failure inherited an HTTP status",
            )?;
            check(
                client.last_connection_error() == Some(failure),
                "local failure lost its stable error snapshot",
            )?;
            for _ in 0..8 {
                client.notify_network_available();
            }
            bounded(
                "explicit recovery after local failure",
                client.connect_with_options(&peer.url, options(0)),
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
    let initial = WebSocketConnectOptions {
        header_provider: Some(Arc::new(move || {
            increment(&provider_calls)?;
            Ok(Vec::new())
        })),
        ..options(6)
    };
    let target = "ws://origin.invalid/recovery";
    let outcome: TestResult = async {
        let failed = bounded(
            "proxy auth rejection",
            client.connect_with_options(target, initial),
        )
        .await?;
        check(
            failed == Err(NetError::ConnectError),
            "proxy auth error changed",
        )?;
        check(
            peer.count() == 1,
            "proxy authentication was retried in the same cycle",
        )?;
        check(
            client.last_handshake_http_status().is_none(),
            "proxy status contaminated Upgrade snapshot",
        )?;
        for _ in 0..8 {
            client.notify_network_available();
        }
        bounded(
            "explicit proxy recovery",
            client.connect_with_options(target, options(0)),
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
async fn transient_retry_counts_and_hint_recovery_preserve_the_healthy_socket() -> TestResult {
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
        let initial = WebSocketConnectOptions {
            header_provider: Some(Arc::new(move || {
                increment(&provider_calls)?;
                Ok(Vec::new())
            })),
            ..options(retries)
        };
        let (status_tx, mut status_rx) = mpsc::unbounded_channel();
        client.register_web_socket_client_connect_status_listener(Box::new(move |status| {
            // The receiver may close during client destruction. This observation
            // channel does not affect network work or require its own completion.
            let _ = status_tx.send(status);
        }));
        let outcome: TestResult = async {
            let failed = bounded(
                "exhaust transient attempts",
                client.connect_with_options(&peer.url, initial),
            )
            .await?;
            check(
                failed == Err(NetError::ConnectError),
                "legacy exhaustion error changed",
            )?;
            check(
                peer.count() == attempts,
                "max_retries did not yield exactly 1 + max_retries handshakes",
            )?;
            check(
                calls.load(Ordering::SeqCst) == attempts,
                "provider count disagreed with actual handshakes",
            )?;
            client.notify_network_available();
            bounded("hint-established status", async {
                while let Some(status) = status_rx.recv().await {
                    if status == ConnectionStatus::Connected {
                        return Ok::<(), TestError>(());
                    }
                }
                Err(error("status channel closed before hint recovery"))
            })
            .await??;
            let expected = attempts
                .checked_add(1)
                .ok_or_else(|| error("recovered count overflow"))?;
            for _ in 0..8 {
                client.notify_network_available();
            }
            // The next command is a FIFO barrier after all repeated hints. It
            // must observe the existing healthy connection and must not replace it.
            let duplicate = bounded(
                "healthy-connection command barrier",
                client.connect_with_options(&peer.url, options(0)),
            )
            .await?;
            check(
                duplicate == Err(NetError::ConnectionExists),
                "healthy socket was replaced or lost",
            )?;
            check(
                peer.count() == expected,
                "repeated hint opened another healthy socket",
            )?;
            check(
                calls.load(Ordering::SeqCst) == expected,
                "repeated hint restarted the healthy provider",
            )?;
            check(
                client.last_handshake_http_status().is_none(),
                "successful recovery retained stale HTTP failure",
            )?;
            check(
                client.last_connection_error() == Some(NetError::ConnectError),
                "automatic recovery no longer preserves the last connection failure",
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
async fn context_exhaustion_recovers_explicitly_without_a_network_hint() -> TestResult {
    let mut peer = Peer::start(vec![503, 503]).await?;
    let net = OpenNet::new()?;
    let client = net
        .create_ws_client_with_config("context-explicit-recovery", client_config())
        .await?;
    let outcome: TestResult = async {
        let mut old = bounded(
            "start exhausted context",
            client.start_connect_with_context(
                WebSocketContextConnectOptions::new(&peer.url, 400)
                    .with_headers(Vec::new(), 4000)
                    .with_reconnect(policy(1)),
            ),
        )
        .await??;
        let mut failed_attempts = 0usize;
        let mut terminal = None;
        while let Some(event) = bounded("exhaust old context", old.recv()).await?? {
            check(
                terminal.is_none(),
                "old context emitted after its terminal event",
            )?;
            match event.kind() {
                WebSocketConnectionEventKind::AttemptFailed => {
                    failed_attempts = failed_attempts
                        .checked_add(1)
                        .ok_or_else(|| error("event count overflow"))?;
                    check(
                        event.failure().and_then(|failure| failure.http_status()) == Some(503),
                        "original transient failure lost",
                    )?;
                }
                WebSocketConnectionEventKind::SessionTerminated => {
                    check(
                        event.termination_reason()
                            == Some(WebSocketTerminationReason::RetryExhausted),
                        "wrong exhaustion reason",
                    )?;
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
        // No network hint or simulated network edge occurs between the terminal
        // event and this explicit new session.
        let mut current = bounded(
            "start explicit new context",
            client.start_connect_with_context(
                WebSocketContextConnectOptions::new(&peer.url, 401)
                    .with_headers(Vec::new(), 4010)
                    .with_reconnect(policy(0)),
            ),
        )
        .await??;
        let established = bounded("new context Established", current.recv())
            .await??
            .ok_or_else(|| error("new context closed before Established"))?;
        check(
            established.kind() == WebSocketConnectionEventKind::Established,
            "explicit new session failed",
        )?;
        check(
            established.session_id() != previous.session_id(),
            "new context reused terminal session identity",
        )?;
        check(
            established.client_instance_id() == previous.client_instance_id(),
            "explicit recovery unexpectedly replaced the client",
        )?;
        check(
            established.session_context_id() == 401,
            "new session lost application context",
        )?;
        bounded("cancel stale handle", old.cancel()).await??;
        check(
            bounded("old stream remains ended", old.recv())
                .await??
                .is_none(),
            "terminal stream restarted",
        )?;
        drop(old);
        let duplicate = bounded(
            "new context survives old handle",
            client.connect_with_options(&peer.url, options(0)),
        )
        .await?;
        check(
            duplicate == Err(NetError::ConnectionExists),
            "stale handle cancellation affected new session",
        )?;
        check(
            peer.count() == 3,
            "explicit context recovery opened extra sockets",
        )?;
        bounded("cancel recovered context", current.cancel()).await??;
        let mut terminal_count = 0usize;
        while let Some(event) = bounded("drain recovered context", current.recv()).await?? {
            if event.kind() == WebSocketConnectionEventKind::SessionTerminated {
                terminal_count = terminal_count
                    .checked_add(1)
                    .ok_or_else(|| error("terminal count overflow"))?;
            }
        }
        check(
            terminal_count == 1,
            "recovered context omitted or duplicated terminal event",
        )?;
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
    let initial = WebSocketConnectOptions {
        reconnect: ReconnectPolicy {
            enabled: false,
            ..policy(6)
        },
        header_provider: Some(Arc::new(move || {
            increment(&provider_calls)?;
            Ok(Vec::new())
        })),
        ..WebSocketConnectOptions::default()
    };
    let outcome: TestResult = async {
        let failed = bounded(
            "disabled reconnect first failure",
            client.connect_with_options(&peer.url, initial),
        )
        .await?;
        check(
            failed == Err(NetError::ConnectError),
            "disabled reconnect error changed",
        )?;
        check(
            peer.count() == 1,
            "disabled reconnect consumed additional attempts",
        )?;
        for _ in 0..8 {
            client.notify_network_available();
        }
        bounded(
            "explicit recovery with reconnect disabled",
            client.connect_with_options(&peer.url, options(0)),
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
        for context in 0..100u64 {
            let client = client.clone();
            let target = peer.url.clone();
            attempts.spawn(async move {
                client
                    .start_connect_with_context(
                        WebSocketContextConnectOptions::new(target, context)
                            .with_headers(Vec::new(), context)
                            .with_reconnect(policy(0)),
                    )
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
                Err(NetError::ConnectionExists) => {
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
        let established = bounded("one owner establishes", owner.recv())
            .await??
            .ok_or_else(|| error("winning session ended before Established"))?;
        check(
            established.kind() == WebSocketConnectionEventKind::Established,
            "winning session did not establish",
        )?;
        for _ in 0..8 {
            client.notify_network_available();
        }
        let duplicate = bounded(
            "healthy owner command barrier",
            client.connect_with_options(&peer.url, options(0)),
        )
        .await?;
        check(
            duplicate == Err(NetError::ConnectionExists),
            "healthy owner was replaced by hints",
        )?;
        check(
            peer.count() == 1,
            "healthy owner was assigned another socket",
        )?;
        bounded("cancel winning session", owner.cancel()).await??;
        let mut terminals = 0usize;
        while let Some(event) = bounded("winning session terminal", owner.recv()).await?? {
            if event.kind() == WebSocketConnectionEventKind::SessionTerminated {
                terminals = terminals
                    .checked_add(1)
                    .ok_or_else(|| error("terminal count overflow"))?;
            }
        }
        check(
            terminals == 1,
            "winning session did not terminate exactly once",
        )?;
        Ok(())
    }
    .await;
    cleanup(&client, &mut peer).await?;
    outcome
}

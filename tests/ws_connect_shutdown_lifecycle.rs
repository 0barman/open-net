#![cfg(feature = "ws-client")]

//! Lifecycle regressions that cross real handshake and worker cleanup boundaries.

use open_net::{
    ConnectionStatus, NetError, OpenNet, ReconnectPolicy, WebSocketClient, WebSocketClientConfig,
    WebSocketConnectOptions, WebSocketConnectionEventKind as Kind, WebSocketConnectionEvents,
    WebSocketContextConnectOptions, WebSocketTerminationReason,
};
use std::future::{poll_fn, Future};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .map_err(|error| std::io::Error::other(format!("{label}: {error}")).into())
}

fn policy() -> ReconnectPolicy {
    ReconnectPolicy {
        enabled: false,
        handshake_timeout: Duration::from_secs(30),
        ..ReconnectPolicy::default()
    }
}

fn context_options(url: &str, context: u64) -> WebSocketContextConnectOptions {
    WebSocketContextConnectOptions::new(url, context)
        .with_headers(Vec::new(), context)
        .with_reconnect(policy())
}

async fn client(net: &OpenNet, name: &str, close_timeout: Duration) -> TestResult<WebSocketClient> {
    Ok(bounded(
        "create client",
        net.create_ws_client_with_config(
            name,
            WebSocketClientConfig {
                close_timeout,
                response_dispatch_grace: Duration::from_millis(20),
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??)
}

async fn accept_upgrade(listener: &TcpListener) -> TestResult<(TcpStream, String)> {
    let (mut stream, _) = bounded("accept peer", listener.accept()).await??;
    let request = bounded("read Upgrade request", async {
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            if request.len() >= 16 * 1024 {
                return Err(std::io::Error::other("oversized Upgrade request"));
            }
            request.push(stream.read_u8().await?);
        }
        Ok::<_, std::io::Error>(request)
    })
    .await??;
    let request = String::from_utf8(request)?;
    let key = request
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find_map(|(name, value)| {
            name.eq_ignore_ascii_case("sec-websocket-key")
                .then(|| value.trim())
        })
        .ok_or_else(|| std::io::Error::other("Upgrade request omitted WebSocket key"))?;
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n",
        derive_accept_key(key.as_bytes())
    );
    Ok((stream, response))
}

async fn terminal_events(
    events: &mut WebSocketConnectionEvents,
    context: u64,
    reason: WebSocketTerminationReason,
) -> TestResult {
    let mut kinds = Vec::new();
    while let Some(event) = bounded("receive terminal event", events.recv()).await?? {
        check(
            event.session_context_id() == context,
            "context was replaced",
        )?;
        check(
            event.termination_reason() == Some(reason),
            "incorrect termination reason",
        )?;
        check(
            event
                .failure()
                .is_some_and(|failure| failure.error() == NetError::Cancelled),
            "cancellation error was not retained",
        )?;
        kinds.push(event.kind());
        check(kinds.len() <= 2, "duplicate terminal events")?;
    }
    check(
        kinds == [Kind::AttemptFailed, Kind::SessionTerminated],
        "cancelled Upgrade published success or omitted a terminal event",
    )
}

struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test]
async fn disconnect_cancels_legacy_upgrade_and_late_reply_cannot_replace_new_context() -> TestResult
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let net = OpenNet::new()?;
    let client = client(
        &net,
        "legacy-upgrade-replacement",
        Duration::from_millis(30),
    )
    .await?;
    let connecting_client = client.clone();
    let old_url = url.clone();
    let connecting = tokio::spawn(async move {
        connecting_client
            .connect_with_options(
                &old_url,
                WebSocketConnectOptions {
                    reconnect: policy(),
                    ..WebSocketConnectOptions::default()
                },
            )
            .await
    });
    let _abort_connect = AbortOnDrop(connecting.abort_handle());
    let (mut old_peer, old_response) = accept_upgrade(&listener).await?;

    bounded("disconnect blocked legacy Upgrade", client.disconnect()).await??;
    check(
        bounded("cancelled legacy connect result", connecting).await?? == Err(NetError::Cancelled),
        "legacy connect did not receive cancellation",
    )?;
    check(
        client.connection_status() == ConnectionStatus::Idle,
        "disconnect did not reach Idle",
    )?;

    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        let observation = (
            response.connection_generation(),
            response.message().to_text().map(str::to_owned),
        );
        if received_tx.send(observation).is_err() {
            eprintln!("replacement listener observation receiver closed");
        }
    }));
    let mut events = client
        .start_connect_with_context(context_options(&url, 202))
        .await?;
    let (mut new_peer, response) = accept_upgrade(&listener).await?;
    bounded(
        "reply to replacement Upgrade",
        new_peer.write_all(response.as_bytes()),
    )
    .await??;
    let established = bounded("replacement Established", events.recv())
        .await??
        .ok_or_else(|| std::io::Error::other("replacement session ended before Established"))?;
    check(
        established.kind() == Kind::Established && established.session_context_id() == 202,
        "replacement context was not established",
    )?;

    // The old peer is deliberately released only after the replacement is live.
    // Either the late write is rejected or its data is discarded by the closed socket.
    if let Err(error) = bounded(
        "late old Upgrade reply",
        old_peer.write_all(old_response.as_bytes()),
    )
    .await?
    {
        eprintln!("old peer rejected the late Upgrade as expected: {error}");
    }
    let mut byte = [0u8; 1];
    check(
        matches!(
            bounded("old socket closed", old_peer.read(&mut byte)).await?,
            Ok(0) | Err(_)
        ),
        "cancelled legacy socket remained usable",
    )?;
    bounded("send replacement data", new_peer.write_all(b"\x81\x01B")).await??;
    let (generation, message) = bounded("replacement data callback", received_rx.recv())
        .await?
        .ok_or_else(|| std::io::Error::other("replacement data callback disappeared"))?;
    check(
        Some(generation) == established.cycle_id() && message? == "B",
        "late legacy reply changed the replacement data identity",
    )?;
    check(
        client.connection_status() == ConnectionStatus::Connected,
        "late legacy reply changed replacement status",
    )?;
    bounded(
        "destroy replacement",
        net.destroy_ws_client("legacy-upgrade-replacement"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_during_context_upgrade_preserves_terminal_identity_and_closes_old_handles(
) -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let net = OpenNet::new()?;
    let client = client(&net, "context-upgrade-shutdown", Duration::from_millis(30)).await?;
    let mut events = client
        .start_connect_with_context(context_options(&url, 303))
        .await?;
    let (_peer, _late_response) = accept_upgrade(&listener).await?;

    bounded("shutdown blocked context Upgrade", client.shutdown()).await??;
    terminal_events(&mut events, 303, WebSocketTerminationReason::Shutdown).await?;
    check(
        client.connection_status() == ConnectionStatus::Closed,
        "shutdown did not reach Closed",
    )?;
    check(
        client
            .connect_with_options(
                &url,
                WebSocketConnectOptions {
                    reconnect: policy(),
                    ..WebSocketConnectOptions::default()
                },
            )
            .await
            == Err(NetError::EngineDropped),
        "closed handle accepted a new legacy connection",
    )?;
    check(
        matches!(
            client
                .start_connect_with_context(context_options(&url, 304))
                .await,
            Err(NetError::EngineDropped)
        ),
        "closed handle accepted a new context session",
    )?;
    bounded("cancel already terminated session", events.cancel()).await??;
    bounded("repeat shutdown", client.shutdown()).await??;
    bounded(
        "release shutdown registry entry",
        net.destroy_ws_client("context-upgrade-shutdown"),
    )
    .await??;
    Ok(())
}

#[derive(Clone)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);

impl Gate {
    fn new() -> Self {
        Self(Arc::new((Mutex::new(false), Condvar::new())))
    }

    fn wait(&self) -> Result<(), NetError> {
        let (lock, wake) = self.0.as_ref();
        let mut released = lock.lock().map_err(|_| NetError::InternalError)?;
        while !*released {
            released = wake.wait(released).map_err(|_| NetError::InternalError)?;
        }
        Ok(())
    }

    fn release(&self) {
        let (lock, wake) = self.0.as_ref();
        match lock.lock() {
            Ok(mut released) => *released = true,
            Err(poisoned) => *poisoned.into_inner() = true,
        }
        wake.notify_all();
    }
}

struct ReleaseOnDrop(Gate);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn dropping_destroy_waiter_reserves_name_until_the_blocked_worker_exits() -> TestResult {
    let net = OpenNet::new()?;
    let name = "destroy-waiter-name-reservation";
    let client = client(&net, name, Duration::from_secs(3)).await?;
    let release = Gate::new();
    let _release_on_drop = ReleaseOnDrop(release.clone());
    let closed_gate = release.clone();
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    client.register_web_socket_client_connect_status_listener(Box::new(move |status| {
        if status == ConnectionStatus::Closed {
            if entered_tx.send(()).is_err() {
                eprintln!("Closed callback observation receiver closed");
            }
            if let Err(error) = closed_gate.wait() {
                eprintln!("Closed callback gate failed: {error:?}");
            }
        }
    }));

    let mut destroying = Box::pin(net.destroy_ws_client(name));
    let first_poll = poll_fn(|context| Poll::Ready(destroying.as_mut().poll(context))).await;
    check(
        first_poll.is_pending(),
        "destroy completed before reaching the Closed callback gate",
    )?;
    bounded("worker enters Closed callback", entered_rx.recv())
        .await?
        .ok_or_else(|| std::io::Error::other("Closed callback did not enter its gate"))?;
    drop(destroying);

    check(
        matches!(net.get_ws_client(name), Err(NetError::ConnectionClosing)),
        "dropping destroy released the name before the worker exited",
    )?;
    check(
        matches!(
            bounded("reject duplicate while Closing", net.create_ws_client(name)).await?,
            Err(NetError::ClientAlreadyExists)
        ),
        "a second worker was created before the old worker exited",
    )?;
    release.release();
    bounded("background join releases name", async {
        loop {
            match net.get_ws_client(name) {
                Err(NetError::ClientNotFound) => return Ok::<_, NetError>(()),
                Err(NetError::ConnectionClosing) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
                Ok(_) => return Err(NetError::InternalError),
            }
        }
    })
    .await??;
    let replacement =
        bounded("recreate after background join", net.create_ws_client(name)).await??;
    check(
        replacement.connection_status() == ConnectionStatus::Idle
            && client.connection_status() == ConnectionStatus::Closed,
        "recreation reused the closed instance",
    )?;
    bounded("destroy replacement instance", net.destroy_ws_client(name)).await??;
    Ok(())
}

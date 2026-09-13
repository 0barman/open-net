#![cfg(feature = "ws-client")]

use futures::{SinkExt, StreamExt};
use open_net::{
    ConnectionStatus, NetError, OpenNet, ReconnectPolicy, WebSocketClient, WebSocketClientConfig,
    WebSocketConnectOptions, WebSocketMessage, WsBody,
};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const OPERATION_BUDGET: Duration = Duration::from_secs(4);
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
const PONG_TIMEOUT: Duration = Duration::from_millis(400);
const WRITE_TIMEOUT: Duration = Duration::from_millis(200);
// A full next-ping interval, its write limit and pong limit, plus a bounded
// scheduling allowance for the client's independent OS thread/runtime.
const BLACKHOLE_BUDGET: Duration = Duration::from_secs(2);

async fn bounded<T>(future: impl Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(OPERATION_BUDGET, future).await?)
}

struct FixtureTask {
    stop: CancellationToken,
    task: JoinHandle<TestResult>,
}

impl FixtureTask {
    async fn finish(mut self) -> TestResult {
        self.stop.cancel();
        let joined = bounded(&mut self.task).await?;
        joined??;
        Ok(())
    }
}

impl Drop for FixtureTask {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}

async fn serve_echo(stream: TcpStream, stop: CancellationToken) -> TestResult {
    let mut socket = tokio::select! {
        biased;
        _ = stop.cancelled() => return Ok(()),
        upgraded = bounded(tokio_tungstenite::accept_async(stream)) => upgraded??,
    };
    loop {
        let message = tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            message = socket.next() => message.ok_or("echo stream ended without a Close frame")??,
        };
        match message {
            Message::Text(text) => bounded(socket.send(Message::Text(text))).await??,
            Message::Ping(payload) => bounded(socket.send(Message::Pong(payload))).await??,
            Message::Close(_) => {
                bounded(socket.flush()).await??;
                return Ok(());
            }
            _ => {}
        }
    }
}

async fn start_echo_peer() -> TestResult<(SocketAddr, FixtureTask)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let stop = CancellationToken::new();
    let task_stop = stop.clone();
    let task = tokio::spawn(async move {
        let mut connections: JoinSet<TestResult> = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = task_stop.cancelled() => break,
                completed = connections.join_next(), if !connections.is_empty() => {
                    completed.ok_or("echo connection set ended unexpectedly")???;
                }
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    connections.spawn(serve_echo(stream, task_stop.clone()));
                }
            }
        }
        while let Some(completed) = connections.join_next().await {
            completed??;
        }
        Ok(())
    });
    Ok((address, FixtureTask { stop, task }))
}

async fn forward_until_blackholed(
    mut inbound: TcpStream,
    target: SocketAddr,
    mut blackhole: watch::Receiver<bool>,
    entered: mpsc::Sender<()>,
    stop: CancellationToken,
) -> TestResult {
    let mut outbound = tokio::select! {
        biased;
        _ = stop.cancelled() => return Ok(()),
        connected = bounded(TcpStream::connect(target)) => connected??,
    };
    loop {
        if *blackhole.borrow_and_update() {
            // Neither stream is dropped, shut down, read or written here. The
            // old tunnel stays blackholed even when new tunnels resume forwarding.
            bounded(entered.send(())).await??;
            stop.cancelled().await;
            drop((inbound, outbound));
            return Ok(());
        }
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            changed = blackhole.changed() => changed?,
            forwarded = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {
                forwarded?;
                return Ok(());
            }
        }
    }
}

struct BlackholeRelay {
    address: SocketAddr,
    blackhole: watch::Sender<bool>,
    entered: mpsc::Receiver<()>,
    accepted: Arc<AtomicUsize>,
    task: FixtureTask,
}

impl BlackholeRelay {
    async fn start(target: SocketAddr) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (blackhole, observations) = watch::channel(false);
        let (entered_tx, entered) = mpsc::channel(4);
        let accepted = Arc::new(AtomicUsize::new(0));
        let observed_accepts = Arc::clone(&accepted);
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let task = tokio::spawn(async move {
            let mut tunnels: JoinSet<TestResult> = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = task_stop.cancelled() => break,
                    completed = tunnels.join_next(), if !tunnels.is_empty() => {
                        completed.ok_or("relay tunnel set ended unexpectedly")???;
                    }
                    connection = listener.accept() => {
                        let (inbound, _) = connection?;
                        observed_accepts.fetch_add(1, Ordering::SeqCst);
                        tunnels.spawn(forward_until_blackholed(
                            inbound, target, observations.clone(), entered_tx.clone(), task_stop.clone(),
                        ));
                    }
                }
            }
            while let Some(completed) = tunnels.join_next().await {
                completed??;
            }
            Ok(())
        });
        Ok(Self {
            address,
            blackhole,
            entered,
            accepted,
            task: FixtureTask { stop, task },
        })
    }
}

fn one_attempt() -> WebSocketConnectOptions {
    WebSocketConnectOptions {
        reconnect: ReconnectPolicy {
            enabled: false,
            handshake_timeout: Duration::from_secs(2),
            ..ReconnectPolicy::default()
        },
        ..WebSocketConnectOptions::default()
    }
}

async fn echo_roundtrip(
    client: &WebSocketClient,
    responses: &mut mpsc::Receiver<String>,
    payload: &str,
) -> TestResult {
    bounded(client.send_message(WsBody::Text(payload.to_owned()))).await??;
    let echoed = bounded(responses.recv())
        .await?
        .ok_or("echo callback channel closed")?;
    if echoed != payload {
        return Err(format!("expected echoed {payload:?}, received {echoed:?}").into());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_tcp_blackhole_times_out_without_network_events_and_explicit_connect_recovers(
) -> TestResult {
    let (peer_address, peer) = start_echo_peer().await?;
    let mut relay = BlackholeRelay::start(peer_address).await?;
    // The default Ignore policy supplies no internal network monitor or injected
    // events. All reachability detection in this test comes from actual WS I/O.
    let engine = OpenNet::new()?;
    let name = "real-tcp-blackhole";
    let client = bounded(engine.create_ws_client_with_config(
        name,
        WebSocketClientConfig {
            heartbeat_interval: HEARTBEAT_INTERVAL,
            pong_timeout: PONG_TIMEOUT,
            control_write_timeout: WRITE_TIMEOUT,
            data_frame_write_timeout: WRITE_TIMEOUT,
            close_timeout: Duration::from_millis(300),
            response_dispatch_grace: Duration::ZERO,
            ..WebSocketClientConfig::default()
        },
    ))
    .await??;
    let (response_tx, mut responses) = mpsc::channel(4);
    let callback_failed = Arc::new(AtomicBool::new(false));
    let callback_failure = Arc::clone(&callback_failed);
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        if let WebSocketMessage::Text(text) = response.message() {
            if response_tx.try_send(text.to_string()).is_err() {
                callback_failure.store(true, Ordering::SeqCst);
            }
        }
    }));
    let url = format!("ws://{}/blackhole", relay.address);
    let scenario: TestResult = async {
        bounded(client.connect_with_options(&url, one_attempt())).await??;
        echo_roundtrip(&client, &mut responses, "before-blackhole").await?;
        if client.connection_status() != ConnectionStatus::Connected {
            return Err("the initial echo did not leave a live connection".into());
        }

        relay.blackhole.send_replace(true);
        bounded(relay.entered.recv())
            .await?
            .ok_or("relay ended before retaining its blackholed sockets")?;
        tokio::time::timeout(BLACKHOLE_BUDGET, async {
            while client.connection_status() == ConnectionStatus::Connected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        if client.connection_status() != ConnectionStatus::Disconnected
            || client.last_connection_error() != Some(NetError::SocketRecvTimeout)
        {
            return Err(format!(
                "blackhole must fail through missing Pong: status={:?}, error={:?}",
                client.connection_status(),
                client.last_connection_error(),
            )
            .into());
        }
        if relay.accepted.load(Ordering::SeqCst) != 1 {
            return Err("disabled reconnect unexpectedly created another TCP tunnel".into());
        }

        relay.blackhole.send_replace(false);
        bounded(client.connect_with_options(&url, one_attempt())).await??;
        echo_roundtrip(&client, &mut responses, "after-explicit-reconnect").await?;
        if relay.accepted.load(Ordering::SeqCst) != 2
            || client.connection_status() != ConnectionStatus::Connected
            || client.last_connection_error().is_some()
        {
            return Err("explicit recovery must use a new tunnel and clear the timeout".into());
        }
        Ok(())
    }
    .await;

    // Always collect client cleanup and both supervisor results, even when the
    // scenario failed. Drop guards cover cancellation of the test itself.
    let shutdown = async {
        bounded(client.shutdown()).await??;
        Ok(())
    }
    .await;
    let destroy = async {
        bounded(engine.destroy_ws_client(name)).await??;
        Ok(())
    }
    .await;
    relay.task.stop.cancel();
    peer.stop.cancel();
    let relay_finished = relay.task.finish().await;
    let peer_finished = peer.finish().await;
    let mut failures = Vec::new();
    for (stage, result) in [
        ("scenario", scenario),
        ("client shutdown", shutdown),
        ("client destroy", destroy),
        ("relay", relay_finished),
        ("echo peer", peer_finished),
    ] {
        if let Err(error) = result {
            failures.push(format!("{stage}: {error}"));
        }
    }
    if callback_failed.load(Ordering::SeqCst) {
        failures.push("data callback could not deliver its observation".to_owned());
    }
    if !failures.is_empty() {
        return Err(failures.join("; ").into());
    }
    Ok(())
}

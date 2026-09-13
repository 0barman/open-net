#![cfg(feature = "ws-client")]

//! Exercise callback admission through a real socket and the public connection events.
//! Tiny payloads and callback gates make both overload limits deterministic.

use futures::{SinkExt, StreamExt};
use open_net::{
    ConnectionStatus, NetError, OpenNet, ReconnectPolicy, WebSocketClient, WebSocketClientConfig,
    WebSocketConnectStage, WebSocketConnectionEvent, WebSocketConnectionEventKind as Kind,
    WebSocketConnectionEvents, WebSocketContextConnectOptions, WebSocketTerminationReason,
};
use std::future::Future;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;
type Peer = WebSocketStream<TcpStream>;

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
        .map_err(|cause| error(format!("{label}: {cause}")))
}

#[derive(Clone)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);

impl Gate {
    fn new() -> Self {
        Self(Arc::new((Mutex::new(false), Condvar::new())))
    }

    fn wait(&self) -> TestResult {
        let (lock, changed) = self.0.as_ref();
        let mut released = lock
            .lock()
            .map_err(|cause| error(format!("callback gate lock: {cause}")))?;
        while !*released {
            released = changed
                .wait(released)
                .map_err(|cause| error(format!("callback gate wait: {cause}")))?;
        }
        Ok(())
    }

    fn release(&self) {
        let (lock, changed) = self.0.as_ref();
        match lock.lock() {
            Ok(mut released) => *released = true,
            Err(cause) => {
                eprintln!("callback gate poisoned during cleanup: {cause}");
                *cause.into_inner() = true;
            }
        }
        changed.notify_all();
    }
}

struct ReleaseOnDrop(Gate);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Observation {
    generation: u64,
    payload: Vec<u8>,
}

struct Probe {
    gate: Gate,
    _release: ReleaseOnDrop,
    entered: mpsc::Receiver<Observation>,
    finished: mpsc::Receiver<TestResult<Observation>>,
}

impl Probe {
    fn install(client: &WebSocketClient) -> Self {
        let gate = Gate::new();
        let callback_gate = gate.clone();
        let (entered_tx, entered) = mpsc::channel(8);
        let (finished_tx, finished) = mpsc::channel(8);
        client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
            let result = (|| -> TestResult<Observation> {
                let Message::Binary(payload) = response.message() else {
                    return Err(error("unexpected non-binary callback payload"));
                };
                let observation = Observation {
                    generation: response.connection_generation(),
                    payload: payload.to_vec(),
                };
                entered_tx.try_send(observation.clone())?;
                if payload.first().copied() == Some(b'A') {
                    callback_gate.wait()?;
                }
                Ok(observation)
            })();
            if let Err(cause) = finished_tx.try_send(result) {
                eprintln!("callback result observation failed: {cause}");
            }
        }));
        Self {
            _release: ReleaseOnDrop(gate.clone()),
            gate,
            entered,
            finished,
        }
    }

    async fn entered_payload(&mut self, payload: &[u8]) -> TestResult<u64> {
        let observation = bounded("callback entered", self.entered.recv())
            .await?
            .ok_or_else(|| error("callback entry channel closed"))?;
        check(
            observation.payload == payload,
            &format!("unexpected callback observation: {observation:?}"),
        )?;
        Ok(observation.generation)
    }

    async fn entered(&mut self, generation: u64, payload: &[u8]) -> TestResult {
        check(
            self.entered_payload(payload).await? == generation,
            "callbacks from the same socket used different response generations",
        )
    }

    async fn finished(&mut self, generation: u64, payload: &[u8]) -> TestResult {
        let observation = bounded("callback completed", self.finished.recv())
            .await?
            .ok_or_else(|| error("callback completion channel closed"))??;
        check(
            observation.generation == generation && observation.payload == payload,
            &format!("unexpected completed callback: {observation:?}"),
        )
    }
}

fn config(capacity: usize, bytes: usize) -> WebSocketClientConfig {
    WebSocketClientConfig {
        callback_queue_capacity: capacity,
        callback_queue_max_bytes: bytes,
        data_callback_concurrency: 1,
        response_dispatch_grace: Duration::from_millis(40),
        close_timeout: Duration::from_millis(40),
        heartbeat_interval: Duration::from_secs(60),
        pong_timeout: Duration::from_secs(120),
        ..WebSocketClientConfig::default()
    }
}

async fn connect(
    client: &WebSocketClient,
    listener: &TcpListener,
    reconnect: bool,
) -> TestResult<WebSocketConnectionEvents> {
    let options =
        WebSocketContextConnectOptions::new(format!("ws://{}", listener.local_addr()?), 111)
            .with_headers(Vec::new(), 222)
            .with_reconnect(ReconnectPolicy {
                enabled: reconnect,
                max_retries: 1,
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                max_elapsed: Some(Duration::from_secs(3)),
                handshake_timeout: Duration::from_secs(3),
            });
    Ok(bounded(
        "start context connection",
        client.start_connect_with_context(options),
    )
    .await??)
}

async fn accept(listener: &TcpListener) -> TestResult<Peer> {
    let (stream, _) = bounded("accept callback test peer", listener.accept()).await??;
    Ok(bounded(
        "complete peer handshake",
        tokio_tungstenite::accept_async(stream),
    )
    .await??)
}

async fn next(events: &mut WebSocketConnectionEvents) -> TestResult<WebSocketConnectionEvent> {
    bounded("connection event", events.recv())
        .await??
        .ok_or_else(|| error("connection events ended early"))
}

async fn established(
    events: &mut WebSocketConnectionEvents,
) -> TestResult<WebSocketConnectionEvent> {
    let event = next(events).await?;
    check(event.kind() == Kind::Established, "expected Established")?;
    check(
        event.session_context_id() == 111 && event.attempt_context_id() == Some(222),
        "handshake context was changed",
    )?;
    check(event.failure().is_none(), "Established contained failure")?;
    Ok(event)
}

async fn overflow(
    events: &mut WebSocketConnectionEvents,
    established: WebSocketConnectionEvent,
) -> TestResult<WebSocketConnectionEvent> {
    let event = next(events).await?;
    check(
        event.kind() == Kind::ConnectionTerminated
            && event.cycle_id() == established.cycle_id()
            && event.session_id() == established.session_id()
            && event.client_instance_id() == established.client_instance_id()
            && event.session_context_id() == established.session_context_id()
            && event.sequence()
                == established
                    .sequence()
                    .checked_add(1)
                    .ok_or_else(|| error("sequence overflow"))?,
        "overflow lost its original connection identity or event sequence",
    )?;
    check(
        event.termination_reason() == Some(WebSocketTerminationReason::IoFailure)
            && event.failure().is_some_and(|failure| {
                failure.error() == NetError::CallbackQueueOverflow
                    && failure.stage() == WebSocketConnectStage::WebSocketIo
            }),
        "raw callback overflow did not produce the explicit I/O failure",
    )?;
    Ok(event)
}

async fn session_ended(
    events: &mut WebSocketConnectionEvents,
    previous: WebSocketConnectionEvent,
) -> TestResult {
    let event = next(events).await?;
    check(
        event.kind() == Kind::SessionTerminated
            && event.session_id() == previous.session_id()
            && event.cycle_id() == previous.cycle_id()
            && event.sequence()
                == previous
                    .sequence()
                    .checked_add(1)
                    .ok_or_else(|| error("sequence overflow"))?
            && event
                .failure()
                .is_some_and(|failure| failure.error() == NetError::CallbackQueueOverflow),
        "disabled reconnect omitted or changed the overload terminal event",
    )?;
    check(
        bounded("session event EOF", events.recv())
            .await??
            .is_none(),
        "events appeared after SessionTerminated",
    )
}

async fn send(peer: &mut Peer, payload: &[u8]) -> TestResult {
    bounded(
        "send callback payload",
        peer.send(Message::Binary(payload.to_vec().into())),
    )
    .await??;
    Ok(())
}

/// A returned Pong proves the reader processed every preceding data message.
/// The blocked callback is still holding its bytes while this protocol reply runs.
async fn ping_fence(peer: &mut Peer) -> TestResult {
    let payload = b"callback-budget-fence";
    bounded(
        "send peer Ping",
        peer.send(Message::Ping(payload.to_vec().into())),
    )
    .await??;
    let reply = bounded("receive Pong while data callback is blocked", peer.next())
        .await?
        .ok_or_else(|| error("peer closed before Pong"))??;
    check(
        matches!(reply, Message::Pong(ref pong) if pong.as_ref() == payload),
        "reader did not keep protocol Pong responsive within callback capacity",
    )
}

async fn cleanup(net: &OpenNet, name: &str, probe: &Probe, outcome: TestResult) -> TestResult {
    // This runs after an early Result failure too; Drop provides a second release boundary.
    probe.gate.release();
    let destroyed = bounded(
        "destroy callback overload client",
        net.destroy_ws_client(name),
    )
    .await;
    outcome?;
    destroyed??;
    Ok(())
}

#[tokio::test]
async fn raw_count_overflow_is_observable_and_shutdown_does_not_wait_for_blocked_callback(
) -> TestResult {
    const NAME: &str = "callback-count-overload";
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let net = OpenNet::new()?;
    let client = bounded(
        "create callback count client",
        net.create_ws_client_with_config(NAME, config(1, 16)),
    )
    .await??;
    let mut probe = Probe::install(&client);
    let outcome = async {
        let mut events = connect(&client, &listener, false).await?;
        let mut peer = accept(&listener).await?;
        let opened = established(&mut events).await?;
        send(&mut peer, b"A").await?;
        let generation = probe.entered_payload(b"A").await?;
        send(&mut peer, b"B").await?;
        ping_fence(&mut peer).await?;
        send(&mut peer, b"C").await?;
        let ended = overflow(&mut events, opened).await?;
        session_ended(&mut events, ended).await?;
        check(
            client.connection_status() == ConnectionStatus::Disconnected
                && client.last_connection_error() == Some(NetError::CallbackQueueOverflow),
            "public connection snapshot lost the callback overflow",
        )?;
        bounded(
            "shutdown while first callback stays blocked",
            client.shutdown(),
        )
        .await??;
        check(
            client.connection_status() == ConnectionStatus::Closed,
            "shutdown did not reach Closed",
        )?;
        probe.gate.release();
        probe.finished(generation, b"A").await?;
        Ok(())
    }
    .await;
    cleanup(&net, NAME, &probe, outcome).await
}

#[tokio::test]
async fn raw_cumulative_byte_overflow_preserves_admitted_data_and_protocol_pong() -> TestResult {
    const NAME: &str = "callback-byte-overload";
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let net = OpenNet::new()?;
    let client = bounded(
        "create callback byte client",
        net.create_ws_client_with_config(NAME, config(4, 5)),
    )
    .await??;
    let mut probe = Probe::install(&client);
    let outcome = async {
        let mut events = connect(&client, &listener, false).await?;
        let mut peer = accept(&listener).await?;
        let opened = established(&mut events).await?;
        send(&mut peer, b"AAA").await?;
        let generation = probe.entered_payload(b"AAA").await?;
        send(&mut peer, b"BB").await?;
        ping_fence(&mut peer).await?;
        // Queue capacity is four, and every individual payload is <= five bytes.
        // Only the combined running + queued byte budget rejects this final byte.
        send(&mut peer, b"C").await?;
        let ended = overflow(&mut events, opened).await?;
        session_ended(&mut events, ended).await?;
        check(
            client.last_connection_error() == Some(NetError::CallbackQueueOverflow),
            "byte overflow error was not retained",
        )?;
        probe.gate.release();
        probe.finished(generation, b"AAA").await?;
        probe.entered(generation, b"BB").await?;
        probe.finished(generation, b"BB").await?;
        bounded("shutdown drained byte overflow", client.shutdown()).await??;
        check(
            matches!(
                probe.entered.try_recv(),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
            ),
            "rejected payload was dispatched",
        )?;
        Ok(())
    }
    .await;
    cleanup(&net, NAME, &probe, outcome).await
}

#[tokio::test]
async fn raw_byte_budget_is_shared_across_reconnects_and_recovers_after_callback_return(
) -> TestResult {
    const NAME: &str = "callback-reconnect-budget";
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let net = OpenNet::new()?;
    let client = bounded(
        "create reconnect budget client",
        net.create_ws_client_with_config(NAME, config(4, 6)),
    )
    .await??;
    let mut probe = Probe::install(&client);
    let outcome = async {
        let mut events = connect(&client, &listener, true).await?;
        let mut old_peer = accept(&listener).await?;
        let first = established(&mut events).await?;
        send(&mut old_peer, b"AAAA").await?;
        let old_generation = probe.entered_payload(b"AAAA").await?;
        ping_fence(&mut old_peer).await?;
        drop(old_peer);
        let lost = next(&mut events).await?;
        check(
            lost.kind() == Kind::ConnectionTerminated && lost.cycle_id() == first.cycle_id(),
            "old socket termination lost its generation",
        )?;

        let mut overloaded_peer = accept(&listener).await?;
        let second = established(&mut events).await?;
        check(
            second.session_id() == first.session_id() && second.cycle_id() != first.cycle_id(),
            "automatic reconnect replaced the session or reused its generation",
        )?;
        // The old callback retains four of six bytes. A fresh per-connection
        // budget would incorrectly admit this valid three-byte message.
        send(&mut overloaded_peer, b"BBB").await?;
        let ended = overflow(&mut events, second).await?;
        check(
            ended
                .failure()
                .is_some_and(|failure| failure.error() == NetError::CallbackQueueOverflow),
            "reconnect did not share the original byte budget",
        )?;
        drop(overloaded_peer);
        probe.gate.release();
        probe.finished(old_generation, b"AAAA").await?;

        let mut recovered_peer = accept(&listener).await?;
        let third = established(&mut events).await?;
        check(
            third.session_id() == first.session_id() && third.cycle_id() != second.cycle_id(),
            "budget recovery changed session identity",
        )?;
        // This probe fits even before the completed old callback's waiter is
        // polled. Its entry proves the serial dispatcher reclaimed the old lease.
        send(&mut recovered_peer, b"CC").await?;
        let recovered_generation = probe.entered_payload(b"CC").await?;
        check(
            recovered_generation != old_generation,
            "a new socket reused the old response generation",
        )?;
        send(&mut recovered_peer, b"DDDD").await?;
        probe.finished(recovered_generation, b"CC").await?;
        probe.entered(recovered_generation, b"DDDD").await?;
        probe.finished(recovered_generation, b"DDDD").await?;
        ping_fence(&mut recovered_peer).await?;
        check(
            client.connection_status() == ConnectionStatus::Connected,
            "released callback budget did not recover the live connection",
        )?;
        bounded("cancel recovered session", events.cancel()).await??;
        bounded("shutdown recovered budget client", client.shutdown()).await??;
        Ok(())
    }
    .await;
    cleanup(&net, NAME, &probe, outcome).await
}

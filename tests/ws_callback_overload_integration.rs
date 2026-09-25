#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

// Exercise callback admission through a real socket and the public connection events.
// Tiny payloads and callback gates make both overload limits deterministic.

use futures::{SinkExt, StreamExt};
use open_net::error::ErrorStage;
use open_net::subscription::Subscription;
use open_net::ws::ConnectionEvent;
use open_net::ws::ConnectionEventKind as Kind;
use open_net::ws::TerminationReason;
use open_net::ws::{ConnectOptions, ReconnectPolicy, WebSocketClientConfig};
use open_net::ws::{ConnectionId, ConnectionState, Message as DataMessage, Session};
use open_net::{OpenNet, WebSocketClient};
use session::ObservedSession;

use std::future::Future;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;
type Peer = WebSocketStream<TcpStream>;

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
    generation: ConnectionId,
    payload: Vec<u8>,
}

struct Probe {
    gate: Gate,
    _release: ReleaseOnDrop,
    _subscription: Subscription,
    entered: mpsc::Receiver<Observation>,
    finished: mpsc::Receiver<TestResult<Observation>>,
}

impl Probe {
    fn install(session: &mut Session) -> TestResult<Self> {
        let gate = Gate::new();
        let callback_gate = gate.clone();
        let (entered_tx, entered) = mpsc::channel(8);
        let (finished_tx, finished) = mpsc::channel(8);
        let subscription = session.on_message(move |_context, response| {
            let result = (|| -> TestResult<Observation> {
                let response = response?;
                let Some(DataMessage::Binary(payload)) = response.message() else {
                    return Err(error("unexpected non-binary callback payload"));
                };
                let observation = Observation {
                    generation: response.connection_id(),
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
        })?;
        Ok(Self {
            _release: ReleaseOnDrop(gate.clone()),
            _subscription: subscription,
            gate,
            entered,
            finished,
        })
    }

    async fn entered_payload(&mut self, payload: &[u8]) -> TestResult<ConnectionId> {
        let observation = bounded("callback entered", self.entered.recv())
            .await?
            .ok_or_else(|| error("callback entry channel closed"))?;
        check(
            observation.payload == payload,
            &format!("unexpected callback observation: {observation:?}"),
        )?;
        Ok(observation.generation)
    }

    async fn entered(&mut self, generation: ConnectionId, payload: &[u8]) -> TestResult {
        check(
            self.entered_payload(payload).await? == generation,
            "callbacks from the same socket used different response generations",
        )
    }

    async fn finished(&mut self, generation: ConnectionId, payload: &[u8]) -> TestResult {
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
    {
        let mut config = WebSocketClientConfig::default();
        config.dispatch.incoming.max_items = capacity;
        config.dispatch.incoming.max_bytes = bytes;
        config.dispatch.message_callback_workers = 1;
        config.requests.manual_response_grace = Duration::from_millis(40);
        config.close_timeout = Duration::from_millis(40);
        config.heartbeat = Some(open_net::ws::HeartbeatConfig {
            interval: Duration::from_secs(60),
            pong_timeout: Duration::from_secs(120),
        });
        config
    }
}

async fn connect(
    client: &WebSocketClient,
    listener: &TcpListener,
    reconnect: bool,
) -> TestResult<ObservedSession> {
    let options = {
        let mut connect_options = {
            let mut options = {
                let mut options = ConnectOptions::new(format!("ws://{}", listener.local_addr()?));
                options.headers = open_net::HeaderMap::new();
                options
            };
            options.reconnect = if reconnect {
                ReconnectPolicy::Backoff(open_net::ws::BackoffConfig {
                    max_retries: 1,
                    initial_delay: Duration::from_millis(1),
                    max_delay: Duration::from_millis(1),
                    max_elapsed: Some(Duration::from_secs(3)),
                })
            } else {
                ReconnectPolicy::Disabled
            };
            options
        };
        connect_options.handshake_timeout = Duration::from_secs(3);
        connect_options.connect_timeout = Some(Duration::from_secs(3));
        connect_options
    };
    Ok(bounded(
        "start context connection",
        session::observe(&client, options),
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

async fn next(events: &mut ObservedSession) -> TestResult<ConnectionEvent> {
    bounded("connection event", events.recv())
        .await??
        .ok_or_else(|| error("connection events ended early"))
}

async fn established(events: &mut ObservedSession) -> TestResult<ConnectionEvent> {
    let started = next(events).await?;
    let Kind::AttemptStarted { attempt } = &started.kind else {
        return Err(error("expected Started"));
    };
    let event = next(events).await?;
    let Kind::Established { connection } = &event.kind else {
        return Err(error("expected Established"));
    };
    check(
        event.sequence == started.sequence + 1
            && connection.attempt_id == attempt.attempt_id
            && connection.cycle_id == attempt.cycle_id
            && event.session_id == attempt.session_id
            && connection.credential_version.is_none(),
        "handshake attempt identity or order changed",
    )?;
    Ok(event)
}

async fn overflow(
    events: &mut ObservedSession,
    established: &ConnectionEvent,
) -> TestResult<ConnectionEvent> {
    let Kind::Established {
        connection: original,
    } = &established.kind
    else {
        return Err(error("expected original Established"));
    };
    let event = next(events).await?;
    let Kind::Disconnected { connection, end } = &event.kind else {
        return Err(error("overflow omitted Disconnected"));
    };
    check(
        connection.connection_id == original.connection_id
            && connection.cycle_id == original.cycle_id
            && event.session_id == established.session_id
            && event.client_id == established.client_id
            && event.sequence == established.sequence + 1,
        "overflow lost original connection identity or event sequence",
    )?;
    check(
        end.reason == TerminationReason::IoFailure
            && end.error.as_ref().is_some_and(|failure| {
                failure.kind() == open_net::error::ErrorKind::CallbackOverflow
                    && failure.context().stage == Some(ErrorStage::Receive)
            }),
        "raw callback overflow did not produce the explicit I/O failure",
    )?;
    Ok(event)
}

async fn session_ended(events: &mut ObservedSession, previous: ConnectionEvent) -> TestResult {
    let Kind::Disconnected { connection, .. } = &previous.kind else {
        return Err(error("expected previous Disconnected"));
    };
    let event = next(events).await?;
    check(
        event.session_id == previous.session_id
            && event.sequence == previous.sequence + 1
            && matches!(&event.kind, Kind::Closed { result: Err(failure) }
            if failure.kind() == open_net::error::ErrorKind::CallbackOverflow
                && failure.context().connection_id == Some(connection.connection_id)),
        "disabled reconnect omitted or changed the overload terminal event",
    )?;
    check(
        bounded("session event EOF", events.recv())
            .await??
            .is_none(),
        "events appeared after Closed",
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
/// Data callbacks stay blocked while the reader sends this protocol reply.
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
    let mut events = connect(&client, &listener, false).await?;
    let mut probe = Probe::install(&mut events.session)?;
    let outcome = async {
        let mut peer = accept(&listener).await?;
        let opened = established(&mut events).await?;
        send(&mut peer, b"A").await?;
        let generation = probe.entered_payload(b"A").await?;
        send(&mut peer, b"B").await?;
        ping_fence(&mut peer).await?;
        send(&mut peer, b"C").await?;
        let ended = overflow(&mut events, &opened).await?;
        session_ended(&mut events, ended).await?;
        check(
            matches!(events.session.state()?.state, ConnectionState::Closed(_))
                && events
                    .session
                    .state()?
                    .last_error
                    .as_ref()
                    .map(|error| error.kind())
                    == Some(open_net::error::ErrorKind::CallbackOverflow),
            "public connection snapshot lost the callback overflow",
        )?;
        bounded(
            "shutdown while first callback stays blocked",
            client.shutdown(),
        )
        .await??;
        check(client.is_shutdown(), "shutdown did not reach Closed")?;
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
    let mut events = connect(&client, &listener, false).await?;
    let mut probe = Probe::install(&mut events.session)?;
    let outcome = async {
        let mut peer = accept(&listener).await?;
        let opened = established(&mut events).await?;
        send(&mut peer, b"AAA").await?;
        let generation = probe.entered_payload(b"AAA").await?;
        send(&mut peer, b"BB").await?;
        send(&mut peer, b"CCC").await?;
        ping_fence(&mut peer).await?;
        // Queue capacity is four, and every individual payload is <= five bytes.
        // V2 counts queued bytes; the running callback has already released its quota.
        // The two queued messages exhaust all five bytes and reject one more.
        send(&mut peer, b"D").await?;
        let ended = overflow(&mut events, &opened).await?;
        session_ended(&mut events, ended).await?;
        check(
            events
                .session
                .state()?
                .last_error
                .as_ref()
                .map(|error| error.kind())
                == Some(open_net::error::ErrorKind::CallbackOverflow),
            "byte overflow error was not retained",
        )?;
        probe.gate.release();
        probe.finished(generation, b"AAA").await?;
        probe.entered(generation, b"BB").await?;
        probe.finished(generation, b"BB").await?;
        probe.entered(generation, b"CCC").await?;
        probe.finished(generation, b"CCC").await?;
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
    let mut events = connect(&client, &listener, true).await?;
    let mut probe = Probe::install(&mut events.session)?;
    let outcome = async {
        let mut old_peer = accept(&listener).await?;
        let first = established(&mut events).await?;
        send(&mut old_peer, b"AAAA").await?;
        let old_generation = probe.entered_payload(b"AAAA").await?;
        send(&mut old_peer, b"BBBB").await?;
        ping_fence(&mut old_peer).await?;
        drop(old_peer);
        let lost = next(&mut events).await?;
        check(
            matches!((&lost.kind, &first.kind), (Kind::Disconnected { connection, .. }, Kind::Established { connection: original }) if connection.connection_id == original.connection_id)
                && lost.sequence == first.sequence + 1,
            "old socket termination lost its generation",
        )?;

        let mut overloaded_peer = accept(&listener).await?;
        let second = established(&mut events).await?;
        check(
            second.session_id == first.session_id && second.sequence == lost.sequence + 2
                && matches!((&second.kind, &first.kind), (Kind::Established { connection }, Kind::Established { connection: original }) if connection.cycle_id != original.cycle_id),
            "automatic reconnect replaced the session or reused its generation",
        )?;
        // An older queued message retains four of six bytes across reconnect.
        // A fresh per-connection budget would wrongly admit this three-byte message.
        send(&mut overloaded_peer, b"EEE").await?;
        let ended = overflow(&mut events, &second).await?;
        check(
            matches!(&ended.kind, Kind::Disconnected { end, .. } if end.error.as_ref().is_some_and(|failure| failure.kind() == open_net::error::ErrorKind::CallbackOverflow)),
            "reconnect did not share the original byte budget",
        )?;
        drop(overloaded_peer);
        probe.gate.release();
        probe.finished(old_generation, b"AAAA").await?;
        probe.entered(old_generation, b"BBBB").await?;
        probe.finished(old_generation, b"BBBB").await?;

        let mut recovered_peer = accept(&listener).await?;
        let third = established(&mut events).await?;
        check(
            third.session_id == first.session_id && third.sequence == ended.sequence + 2
                && matches!((&third.kind, &second.kind), (Kind::Established { connection }, Kind::Established { connection: original }) if connection.cycle_id != original.cycle_id),
            "budget recovery changed session identity",
        )?;
        // Draining the older queued message frees the shared quota for this connection.
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
            matches!(events.session.state()?.state, ConnectionState::Connected(_)),
            "released callback budget did not recover the live connection",
        )?;
        events.session.cancel();
        let _ = bounded("cancel recovered session", events.session.closed()).await?;
        bounded("shutdown recovered budget client", client.shutdown()).await??;
        Ok(())
    }
    .await;
    cleanup(&net, NAME, &probe, outcome).await
}

#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt};
use open_net::ws::{
    Message as NetMessage, MessageLane, MessageReceipt, OperationPhase, SendOptions, Session,
    TaskSuccess, TerminationOutcome, WebSocketClient,
};
use open_net::ws::{ReconnectPolicy, WebSocketClientConfig};
use open_net::OpenNet;

use session::{session_options, SessionGuard};
use socket2::SockRef;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

#[track_caller]
fn error(message: impl Into<String>) -> TestError {
    let location = std::panic::Location::caller();
    std::io::Error::other(format!("{location}: {}", message.into())).into()
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
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .map_err(|failure| error(format!("{label}: {failure}")))
}

struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct PeerFrame {
    body: NetMessage,
    owner_bound: bool,
}

struct Peer {
    url: String,
    frames: mpsc::Receiver<PeerFrame>,
    owner: Arc<Mutex<Option<MessageReceipt>>>,
    task: AbortOnDrop<TestResult>,
}

impl Peer {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let (frames_tx, frames) = mpsc::channel(16);
        let owner = Arc::new(Mutex::new(None));
        let peer_owner = Arc::clone(&owner);
        let task = AbortOnDrop(tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = tokio_tungstenite::accept_async(stream).await?;
            while let Some(frame) = socket.next().await {
                let body = match frame? {
                    Message::Text(text) => NetMessage::text(text.to_string()),
                    Message::Binary(bytes) => NetMessage::binary(bytes),
                    Message::Ping(_) => {
                        socket.flush().await?;
                        continue;
                    }
                    Message::Close(_) => {
                        socket.flush().await?;
                        return Ok(());
                    }
                    _ => continue,
                };
                let owner_bound = peer_owner
                    .lock()
                    .map_err(|failure| error(format!("peer owner lock: {failure}")))?
                    .is_some();
                frames_tx.send(PeerFrame { body, owner_bound }).await?;
            }
            Ok(())
        }));
        Ok(Self {
            url,
            frames,
            owner,
            task,
        })
    }

    fn bind(&self, handle: Option<MessageReceipt>) -> TestResult {
        *self
            .owner
            .lock()
            .map_err(|failure| error(format!("bind owner lock: {failure}")))? = handle;
        Ok(())
    }

    async fn frame(&mut self) -> TestResult<PeerFrame> {
        bounded("peer application frame", self.frames.recv())
            .await?
            .ok_or_else(|| error("peer stopped before receiving the expected message"))
    }

    async fn finish(mut self) -> TestResult {
        bounded("join message peer", &mut self.task.0).await???;
        Ok(())
    }
}

async fn client(net: &OpenNet, name: &str) -> TestResult<WebSocketClient> {
    Ok(bounded(
        "create message client",
        net.create_ws_client_with_config(name, {
            let mut config = WebSocketClientConfig::default();
            config.close_timeout = Duration::from_millis(100);
            config.heartbeat = Some(open_net::ws::HeartbeatConfig {
                interval: Duration::from_secs(3600),
                pong_timeout: Duration::from_secs(7200),
            });
            config
        }),
    )
    .await??)
}

async fn connect(client: &WebSocketClient, url: &str) -> TestResult<SessionGuard> {
    Ok(bounded(
        "connect message peer",
        SessionGuard::establish(client, session_options(url, ReconnectPolicy::Disabled)),
    )
    .await??)
}

fn options(lane: MessageLane) -> SendOptions {
    SendOptions {
        lane,
        enqueue_timeout: Some(Duration::from_secs(2)),
        ..Default::default()
    }
}

async fn probe(session: &Session, peer: &mut Peer, text: &str) -> TestResult<PeerFrame> {
    let body = NetMessage::text(text.to_owned());
    let handle = bounded(
        "submit writer barrier",
        session
            .sender()
            .message(body.clone())
            .options(options(MessageLane::Normal))
            .enqueue(),
    )
    .await??;
    bounded("writer barrier completion", handle.written()).await??;
    let received = peer.frame().await?;
    check(
        received.body == body,
        "a cancelled or uncommitted body reached the peer",
    )?;
    Ok(received)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_messages_bind_owner_before_both_lanes_reach_the_peer() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = client(&net, "message-owner-binding").await?;
    let session = connect(&client, &peer.url).await?;
    for lane in [MessageLane::Normal, MessageLane::Urgent] {
        peer.bind(None)?;
        let body = NetMessage::text(format!("prepared-{lane:?}"));
        let prepared = match lane {
            MessageLane::Normal => {
                bounded(
                    "prepare normal message",
                    session
                        .session
                        .sender()
                        .message(body.clone())
                        .options(options(lane))
                        .prepare(),
                )
                .await??
            }
            MessageLane::Urgent => session
                .session
                .sender()
                .message(body.clone())
                .options(options(lane))
                .try_prepare()?,
        };
        let owner = prepared.receipt().clone();
        check(
            owner.state()?.phase == OperationPhase::Prepared,
            "preparation became writable",
        )?;
        check(
            session.session.requests()?.pending_snapshot()?.is_empty(),
            "preparation allocated pending state",
        )?;
        // Real writer/peer progress proves the prepared body stayed hidden. An
        // accidentally published normal entry precedes this FIFO probe; urgent
        // entries would also be selected before it.
        let before_binding = probe(&session.session, &mut peer, "before-owner-binding").await?;
        check(
            !before_binding.owner_bound,
            "probe unexpectedly had a bound owner",
        )?;
        peer.bind(Some(owner.clone()))?;
        let committed = prepared.commit()?;
        bounded("committed message write", committed.written()).await??;
        let received = peer.frame().await?;
        check(received.body == body, "commit changed the message payload")?;
        check(
            received.owner_bound,
            "message reached the peer before owner binding",
        )?;
        bounded("owner observes shared write", owner.written()).await??;
        check(
            matches!(owner.state()?.result, Some(Ok(TaskSuccess::Written))),
            "owner missed successful writing",
        )?;
        check(
            matches!(owner.cancel()?, TerminationOutcome::AlreadyFinished),
            "late cancellation overwrote Written",
        )?;
        check(
            session.session.requests()?.pending_snapshot()?.is_empty(),
            "written message retained pending state",
        )?;
    }
    bounded(
        "destroy owner client",
        net.destroy_ws_client("message-owner-binding"),
    )
    .await??;
    session.finish().await?;
    peer.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_and_dropped_preparations_never_reach_the_peer() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = client(&net, "message-prepared-cancellation").await?;
    let session = connect(&client, &peer.url).await?;
    for lane in [MessageLane::Normal, MessageLane::Urgent] {
        let prepared = session
            .session
            .sender()
            .message(NetMessage::text(
                "explicitly-cancelled-preparation".to_owned(),
            ))
            .options(options(lane))
            .try_prepare()?;
        let retained = prepared.receipt().clone();
        check(
            matches!(
                retained.cancel()?,
                TerminationOutcome::TerminatedBeforeWrite
            ),
            "prepared cancellation did not win with zero delivery",
        )?;
        check(
            matches!(prepared.commit(), Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::Cancelled)),
            "cancelled preparation was committed",
        )?;
        check(
            bounded("explicit preparation cancellation", retained.written())
                .await?
                .as_ref()
                .map_err(|error| error.kind())
                == Err(open_net::error::ErrorKind::Cancelled),
            "retained handle missed explicit cancellation",
        )?;
        let prepared = bounded(
            "prepare dropped message",
            session
                .session
                .sender()
                .message(NetMessage::text("dropped-preparation".to_owned()))
                .options(options(lane))
                .prepare(),
        )
        .await??;
        let retained = prepared.receipt().clone();
        drop(prepared);
        check(
            bounded("dropped preparation cancellation", retained.written())
                .await?
                .as_ref()
                .map_err(|error| error.kind())
                == Err(open_net::error::ErrorKind::Cancelled),
            "handle clone kept a dropped preparation alive",
        )?;
        probe(&session.session, &mut peer, "after-prepared-cancellation").await?;
        check(
            session.session.requests()?.pending_snapshot()?.is_empty(),
            "cancelled message retained pending state",
        )?;
    }
    bounded(
        "destroy cancelled client",
        net.destroy_ws_client("message-prepared-cancellation"),
    )
    .await??;
    session.finish().await?;
    peer.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_submitted_handles_keeps_normal_and_urgent_delivery() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = client(&net, "message-drop-observers").await?;
    let session = connect(&client, &peer.url).await?;
    for lane in [MessageLane::Normal, MessageLane::Urgent] {
        let body = NetMessage::binary(Bytes::from(format!("retained-dispatch-{lane:?}")));
        let handle = match lane {
            MessageLane::Normal => {
                bounded(
                    "submit normal message",
                    session
                        .session
                        .sender()
                        .message(body.clone())
                        .options(options(lane))
                        .enqueue(),
                )
                .await??
            }
            MessageLane::Urgent => session
                .session
                .sender()
                .message(body.clone())
                .options(options(lane))
                .try_enqueue()?,
        };
        let observer = handle.clone();
        let waiting = Box::pin(observer.written());
        drop(waiting);
        drop(observer);
        drop(handle);
        check(
            peer.frame().await?.body == body,
            "dropping observers cancelled submitted data",
        )?;
        probe(&session.session, &mut peer, "after-dropped-observers").await?;
        check(
            session.session.requests()?.pending_snapshot()?.is_empty(),
            "submitted body created pending state",
        )?;
    }
    bounded(
        "destroy observer client",
        net.destroy_ws_client("message-drop-observers"),
    )
    .await??;
    session.finish().await?;
    peer.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn message_cancel_during_tcp_backpressure_retires_both_socket_halves() -> TestResult {
    const BODY_BYTES: usize = 32 * 1024 * 1024;
    const FRAME_BYTES: usize = 32 * 1024;
    const WIRE_BYTES: usize = BODY_BYTES + 8 * (BODY_BYTES / FRAME_BYTES);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let (ready_tx, ready_rx) = oneshot::channel();
    let (started_tx, started_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let mut peer = AbortOnDrop(tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        SockRef::from(&stream).set_recv_buffer_size(4096)?;
        let mut socket = tokio_tungstenite::accept_async(stream).await?;
        ready_tx
            .send(())
            .map_err(|_| error("readiness receiver closed"))?;
        let mut prefix = [0_u8; 64];
        bounded(
            "peer observes first data bytes",
            socket.get_mut().read_exact(&mut prefix),
        )
        .await??;
        check(
            prefix.first().copied() == Some(0x02),
            "message did not start a fragmented binary write",
        )?;
        started_tx
            .send(())
            .map_err(|_| error("first-byte receiver closed"))?;
        bounded("release peer after cancellation", resume_rx).await??;
        // EOF/reset without explicitly disconnecting the client proves that its
        // reader did not keep the retired physical socket alive after writer exit.
        let mut received = prefix.len();
        let mut buffer = [0_u8; 8192];
        loop {
            match socket.get_mut().read(&mut buffer).await {
                Ok(0) => return Ok::<usize, TestError>(received),
                Ok(count) => {
                    received = received
                        .checked_add(count)
                        .ok_or_else(|| error("wire count overflow"))?;
                    check(
                        received < WIRE_BYTES,
                        "complete cancelled payload escaped after retirement",
                    )?;
                }
                Err(failure)
                    if matches!(
                        failure.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    return Ok(received)
                }
                Err(failure) => return Err(failure.into()),
            }
        }
    }));
    let net = OpenNet::new()?;
    let client = bounded(
        "create backpressured message client",
        net.create_ws_client_with_config("message-real-backpressure", {
            let mut config = WebSocketClientConfig::default();
            config.queues.normal.max_bytes = BODY_BYTES;
            config.frames.data_frame_payload_size = Some(FRAME_BYTES);
            config.frames.write_buffer_size = 0;
            config.frames.max_write_buffer_size = BODY_BYTES + 1024;
            config.tcp.send_buffer_size = Some(4096);
            config.frames.data_frame_write_timeout = Duration::from_secs(60);
            config.close_timeout = Duration::from_millis(100);
            config.heartbeat = Some(open_net::ws::HeartbeatConfig {
                interval: Duration::from_secs(3600),
                pong_timeout: Duration::from_secs(7200),
            });
            config
        }),
    )
    .await??;
    let session = connect(&client, &url).await?;
    bounded("peer ready before message", ready_rx).await??;
    let handle = session
        .session
        .sender()
        .message(NetMessage::binary(Bytes::from(vec![0x5a; BODY_BYTES])))
        .options(SendOptions {
            lane: MessageLane::Normal,
            write_timeout: Duration::from_secs(60),
            ..Default::default()
        })
        .try_enqueue()?;
    bounded("partial frame reached peer", started_rx).await??;
    check(
        handle.written().now_or_never().is_none(),
        "socket was not backpressured",
    )?;
    check(
        matches!(handle.cancel()?, TerminationOutcome::DeliveryUnknown),
        "partial message cancellation lost delivery uncertainty",
    )?;
    resume_tx
        .send(())
        .map_err(|_| error("peer ended before cancellation"))?;
    check(
        bounded("cancelled write resolves", handle.written())
            .await?
            .as_ref()
            .map_err(|error| error.kind())
            == Err(open_net::error::ErrorKind::DeliveryUnknown),
        "write result disagrees with in-flight cancellation",
    )?;
    let received = bounded("retired socket reaches EOF or reset", &mut peer.0).await???;
    check(
        (64..WIRE_BYTES).contains(&received),
        "retired socket received an invalid byte count",
    )?;
    check(
        matches!(handle.state()?.result, Some(Err(error)) if error.kind() == open_net::error::ErrorKind::DeliveryUnknown),
        "terminal handle lost the uncertain result",
    )?;
    check(
        session.session.requests()?.pending_snapshot()?.is_empty(),
        "untracked backpressure allocated pending state",
    )?;
    session.finish().await?;
    bounded(
        "destroy backpressure client",
        net.destroy_ws_client("message-real-backpressure"),
    )
    .await??;
    Ok(())
}

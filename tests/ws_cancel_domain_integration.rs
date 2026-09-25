#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt};
use open_net::ws::ConnectionEventKind;
use open_net::ws::{
    CancellationGroup, Message as NetMessage, MessageLane, Request, RequestId, RequestOptions,
    ResponseRouting, SendOptions, Session,
};
use open_net::ws::{ConnectOptions, ReconnectPolicy, WebSocketClientConfig};
use open_net::{NetError, OpenNet};

use socket2::SockRef;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

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

fn rejected<T>(result: Result<T, NetError>, wanted: NetError, context: &str) -> TestResult {
    match result {
        Err(actual) if actual.kind() == wanted.kind() => Ok(()),
        Err(actual) => Err(error(format!(
            "{context}: wanted {wanted:?}, got {actual:?}"
        ))),
        Ok(_) => Err(error(format!("{context}: unexpectedly accepted"))),
    }
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .map_err(|cause| error(format!("{label}: {cause}")))
}

struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Peer {
    url: String,
    frames: mpsc::UnboundedReceiver<Message>,
    _task: AbortOnDrop<TestResult>,
}

impl Peer {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let (frames_tx, frames) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = tokio_tungstenite::accept_async(stream).await?;
            while let Some(frame) = socket.next().await {
                match frame? {
                    Message::Text(text) => frames_tx.send(Message::Text(text))?,
                    Message::Binary(body) => frames_tx.send(Message::Binary(body))?,
                    Message::Ping(_) => socket.flush().await?,
                    Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
            Ok(())
        });
        Ok(Self {
            url,
            frames,
            _task: AbortOnDrop(task),
        })
    }

    async fn text(&mut self, wanted: &str) -> TestResult {
        let frame = bounded("receive peer application message", self.frames.recv())
            .await?
            .ok_or_else(|| error("peer ended before expected application message"))?;
        check(
            matches!(frame, Message::Text(ref text) if text.as_str() == wanted),
            &format!("peer received {frame:?}, wanted {wanted:?}"),
        )
    }
}

fn request(id: &str, text: &str) -> TestResult<Request> {
    Ok(Request::new(RequestId::new(id)?, text.into()))
}
fn options(group: &CancellationGroup, lane: MessageLane) -> SendOptions {
    SendOptions {
        cancellation: Some(group.clone()),
        lane,
        ..Default::default()
    }
}
async fn connect(net: &OpenNet, name: &str, url: &str) -> TestResult<Session> {
    let client = net
        .create_ws_client_with_config(name, {
            let mut c = WebSocketClientConfig::default();
            c.close_timeout = Duration::from_millis(50);
            c.heartbeat = None;
            c
        })
        .await?;
    let mut o = ConnectOptions::new(url);
    o.routing = ResponseRouting::Manual;
    o.reconnect = ReconnectPolicy::Disabled;
    Ok(client.connect(o).await?)
}
#[test]
fn parent_cancellation_is_irreversible_and_child_cancellation_preserves_siblings() -> TestResult {
    let parent = CancellationGroup::new();
    let first = parent.child();
    let second = parent.child();
    let grandchild = first.child();
    first.clone().cancel();
    check(
        first.is_cancelled()
            && grandchild.is_cancelled()
            && !parent.is_cancelled()
            && !second.is_cancelled(),
        "child cancellation crossed branches",
    )?;
    check(
        first.child().is_cancelled(),
        "new child escaped cancelled parent",
    )?;
    let independent = CancellationGroup::new();
    parent.cancel();
    parent.cancel();
    check(
        second.is_cancelled() && parent.child().is_cancelled() && !independent.is_cancelled(),
        "parent cancellation crossed independent groups",
    )
}
#[tokio::test]
async fn cancelling_one_prepared_domain_keeps_the_other_domain_and_connection_writable(
) -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let s = connect(&net, "group-isolation", &peer.url).await?;
    let parent = CancellationGroup::new();
    let a = parent.child();
    let b = parent.child();
    let requests = s.requests()?;
    let first = requests
        .request(request("a", "must-not-send")?)
        .options(RequestOptions {
            send: options(&a, MessageLane::Normal),
            ..Default::default()
        })
        .prepare()
        .await?;
    let second = requests
        .request(request("b", "live-body")?)
        .options(RequestOptions {
            send: options(&b, MessageLane::Normal),
            ..Default::default()
        })
        .prepare()
        .await?;
    a.cancel();
    rejected(
        first.commit(),
        NetError::from(open_net::error::ErrorKind::Cancelled),
        "revoked prepare",
    )?;
    check(!b.is_cancelled(), "sibling revoked")?;
    let receipt = second.commit()?;
    bounded("sibling written", receipt.handle().written()).await??;
    peer.text("live-body").await?;
    parent.cancel();
    check(
        matches!(receipt.response().await, Err(e) if e.kind() == open_net::error::ErrorKind::Cancelled),
        "pending cancellation lost",
    )?;
    s.sender().send("control-after-cancel").await?;
    peer.text("control-after-cancel").await?;
    check(
        requests.pending_snapshot()?.is_empty(),
        "cancel leaked pending",
    )?;
    net.destroy_ws_client("group-isolation").await?;
    Ok(())
}
#[tokio::test]
async fn cancelled_domain_rejects_new_registered_and_bare_message_admission() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let s = connect(&net, "group-reject", &peer.url).await?;
    let group = CancellationGroup::new();
    group.cancel();
    let requests = s.requests()?;
    for wait in [false, true] {
        let builder = requests
            .request(request("rejected", "must-not-send")?)
            .options(RequestOptions {
                send: options(&group, MessageLane::Normal),
                ..Default::default()
            });
        let result = if wait {
            builder.prepare().await
        } else {
            builder.try_prepare()
        };
        rejected(
            result.map_err(|e| e.into_error()),
            NetError::from(open_net::error::ErrorKind::Cancelled),
            "cancelled request admission",
        )?;
        for lane in [MessageLane::Normal, MessageLane::Urgent] {
            let builder = s
                .sender()
                .message("must-not-send")
                .options(options(&group, lane));
            let result = if wait {
                builder.enqueue().await
            } else {
                builder.try_enqueue()
            };
            rejected(
                result.map_err(|e| e.into_error()),
                NetError::from(open_net::error::ErrorKind::Cancelled),
                "cancelled message admission",
            )?;
        }
    }
    s.sender().send("barrier").await?;
    peer.text("barrier").await?;
    check(
        requests.pending_snapshot()?.is_empty(),
        "rejection allocated pending",
    )?;
    net.destroy_ws_client("group-reject").await?;
    Ok(())
}
#[tokio::test]
async fn a_cancel_group_does_not_revive_a_sender_bound_to_a_closed_session() -> TestResult {
    let net = OpenNet::new()?;
    let first_peer = Peer::start().await?;
    let old = connect(&net, "group-old-owner", &first_peer.url).await?;
    let sender = old.sender();
    old.close().await?;
    let client = net.get_ws_client("group-old-owner")?;
    let mut second_peer = Peer::start().await?;
    let mut o = ConnectOptions::new(&second_peer.url);
    o.reconnect = ReconnectPolicy::Disabled;
    let current = client.connect(o).await?;
    check(
        sender
            .message("old-owner")
            .options(options(&CancellationGroup::new(), MessageLane::Normal))
            .try_enqueue()
            .is_err(),
        "fresh group revived stale sender",
    )?;
    current.sender().send("current-owner").await?;
    second_peer.text("current-owner").await?;
    check(old.id() != current.id(), "new owner reused SessionId")?;
    net.destroy_ws_client("group-old-owner").await?;
    Ok(())
}
#[tokio::test]
async fn domain_cancellation_withdraws_normal_and_urgent_prepared_messages_without_pending_entries(
) -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let s = connect(&net, "group-prepared", &peer.url).await?;
    for lane in [MessageLane::Normal, MessageLane::Urgent] {
        let group = CancellationGroup::new();
        let p = s
            .sender()
            .message("must-not-send")
            .options(options(&group, lane))
            .prepare()
            .await?;
        let receipt = p.receipt().clone();
        group.cancel();
        rejected(
            p.commit(),
            NetError::from(open_net::error::ErrorKind::Cancelled),
            "cancelled message commit",
        )?;
        rejected(
            receipt.written().await,
            NetError::from(open_net::error::ErrorKind::Cancelled),
            "cancelled message receipt",
        )?;
    }
    s.sender().send("barrier").await?;
    peer.text("barrier").await?;
    check(
        s.requests()?.pending_snapshot()?.is_empty(),
        "message allocated pending",
    )?;
    net.destroy_ws_client("group-prepared").await?;
    Ok(())
}
#[tokio::test]
async fn domain_cancellation_wakes_capacity_wait_without_revoking_holder() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = net
        .create_ws_client_with_config("group-capacity", {
            let mut c = WebSocketClientConfig::default();
            c.queues.normal.max_items = 1;
            c
        })
        .await?;
    let s = client.connect(&*peer.url).await?;
    let holder = s.sender().message("holder").try_prepare()?;
    let group = CancellationGroup::new();
    let mut waiting = Box::pin(
        s.sender()
            .message("cancelled-waiter")
            .options(options(&group, MessageLane::Normal))
            .prepare(),
    );
    check(
        waiting.as_mut().now_or_never().is_none(),
        "capacity wait did not block",
    )?;
    group.cancel();
    rejected(
        bounded("cancelled waiter", waiting)
            .await?
            .map_err(|e| e.into_error()),
        NetError::from(open_net::error::ErrorKind::Cancelled),
        "capacity cancellation",
    )?;
    holder.commit()?.written().await?;
    peer.text("holder").await?;
    s.sender().send("barrier").await?;
    peer.text("barrier").await?;
    net.destroy_ws_client("group-capacity").await?;
    Ok(())
}
const LARGE_BODY_BYTES: usize = 32 * 1024 * 1024;
const DATA_FRAME_BYTES: usize = 32 * 1024;
const DATA_FRAME_HEADER_BYTES: usize = 8;
const LARGE_WIRE_BYTES: usize =
    LARGE_BODY_BYTES + DATA_FRAME_HEADER_BYTES * (LARGE_BODY_BYTES / DATA_FRAME_BYTES);
const LIVE_SENTINEL: &str = "live domain still owns delivery";

#[tokio::test]
async fn registered_domain_cancel_retires_real_backpressured_transport_but_preserves_session(
) -> TestResult {
    run_real_backpressure(None, true).await
}

#[tokio::test]
async fn normal_message_domain_cancel_retires_real_backpressured_transport() -> TestResult {
    run_real_backpressure(Some(MessageLane::Normal), true).await
}

#[tokio::test]
async fn urgent_message_domain_cancel_retires_real_backpressured_transport() -> TestResult {
    run_real_backpressure(Some(MessageLane::Urgent), true).await
}

#[tokio::test]
async fn queued_normal_and_urgent_domain_cancellation_keeps_unrelated_backpressured_write(
) -> TestResult {
    run_real_backpressure(None, false).await
}

async fn run_real_backpressure(lane: Option<MessageLane>, cancel_in_flight: bool) -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let (ready_tx, ready_rx) = oneshot::channel();
    let (first_bytes_tx, first_bytes_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let (drained_tx, drained_rx) = oneshot::channel();
    let (sentinel_tx, sentinel_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let mut peer = AbortOnDrop(tokio::spawn(async move {
        let (stream, _) = bounded("accept backpressure socket", listener.accept()).await??;
        SockRef::from(&stream).set_recv_buffer_size(4096)?;
        let mut socket = bounded(
            "accept backpressure Upgrade",
            tokio_tungstenite::accept_async(stream),
        )
        .await??;
        ready_tx
            .send(())
            .map_err(|_| error("backpressure readiness receiver closed"))?;
        // No WebSocket read has prefetched bytes. Reading a fixed prefix establishes
        // that a real data frame started while the rest is held behind TCP pressure.
        let mut prefix = [0_u8; 64];
        bounded(
            "observe first data bytes",
            socket.get_mut().read_exact(&mut prefix),
        )
        .await??;
        check(
            prefix.first().copied() == Some(0x02),
            "large body did not begin a fragmented binary message",
        )?;
        check(
            prefix.get(1).copied() == Some(0xfe),
            "large frame is not masked with 16-bit length",
        )?;
        let frame_length: [u8; 2] = prefix
            .get(2..4)
            .ok_or_else(|| error("missing large-frame length"))?
            .try_into()?;
        check(
            usize::from(u16::from_be_bytes(frame_length)) == DATA_FRAME_BYTES,
            "unexpected data fragmentation size",
        )?;
        if cancel_in_flight {
            // The reader may receive Ping while data remains buffered in the shared
            // WebSocket. Its automatic flush must obey the same retirement decision.
            bounded(
                "send concurrent Ping",
                socket.send(Message::Ping(Bytes::from_static(b"gate"))),
            )
            .await??;
        }
        first_bytes_tx
            .send(())
            .map_err(|_| error("first-byte receiver closed"))?;
        bounded("release peer backpressure", resume_rx).await??;
        let received = bounded("drain blocked data", async {
            let mut received = prefix.len();
            let mut buffer = [0_u8; 8192];
            loop {
                // In the surviving case, consume exactly the known fragmented wire
                // length. The next read can then verify that no cancelled text escaped.
                let remaining = LARGE_WIRE_BYTES.saturating_sub(received);
                let limit = if cancel_in_flight {
                    buffer.len()
                } else {
                    remaining.min(buffer.len())
                };
                if limit == 0 {
                    return Ok::<usize, TestError>(received);
                }
                match socket.get_mut().read(&mut buffer[..limit]).await {
                    Ok(0) if cancel_in_flight => return Ok(received),
                    Ok(0) => {
                        return Err(error(
                            "unrelated domain cancellation truncated the live body",
                        ))
                    }
                    Ok(count) => {
                        received = received
                            .checked_add(count)
                            .ok_or_else(|| error("wire byte counter overflow"))?;
                        if cancel_in_flight && received >= LARGE_WIRE_BYTES {
                            return Err(error(
                                "the entire revoked body escaped on the retired transport",
                            ));
                        }
                    }
                    Err(cause)
                        if cancel_in_flight
                            && matches!(
                                cause.kind(),
                                std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::ConnectionAborted
                            ) =>
                    {
                        return Ok(received)
                    }
                    Err(cause) => return Err(cause.into()),
                }
            }
        })
        .await??;
        drained_tx
            .send(received)
            .map_err(|_| error("drain observation receiver closed"))?;
        if cancel_in_flight {
            drop(socket);
            let (stream, _) = bounded("accept replacement socket", listener.accept()).await??;
            socket = bounded(
                "accept replacement Upgrade",
                tokio_tungstenite::accept_async(stream),
            )
            .await??;
        }
        let message = bounded("receive live-domain sentinel", socket.next())
            .await?
            .ok_or_else(|| error("peer ended without live-domain sentinel"))??;
        check(
            matches!(message, Message::Text(ref text) if text.as_str() == LIVE_SENTINEL),
            &format!("cancelled queued message escaped before sentinel: {message:?}"),
        )?;
        sentinel_tx
            .send(())
            .map_err(|_| error("sentinel observation receiver closed"))?;
        bounded("finish backpressure peer", finish_rx).await??;
        Ok::<usize, TestError>(received)
    }));

    let net = OpenNet::new()?;
    let name = format!("domain-real-backpressure-{lane:?}-{cancel_in_flight}");
    let client = bounded(
        "create backpressure client",
        net.create_ws_client_with_config(&name, {
            let mut config = WebSocketClientConfig::default();
            config.queues.normal.max_bytes = LARGE_BODY_BYTES + 8192;
            config.queues.urgent.max_bytes = LARGE_BODY_BYTES + 8192;
            config.frames.data_frame_payload_size = Some(DATA_FRAME_BYTES);
            config.frames.write_buffer_size = 0;
            config.frames.max_write_buffer_size = LARGE_BODY_BYTES + 1024;
            config.tcp.send_buffer_size = Some(4096);
            config.frames.data_frame_write_timeout = Duration::from_secs(60);
            config.close_timeout = Duration::from_millis(50);
            config.heartbeat = Some(open_net::ws::HeartbeatConfig {
                interval: Duration::from_secs(3600),
                pong_timeout: Duration::from_secs(7200),
            });
            config
        }),
    )
    .await??;
    let business = CancellationGroup::new();
    let live = CancellationGroup::new();
    let mut events = bounded(
        "start backpressure session",
        session::observe(&client, {
            let mut options = {
                let mut options = ConnectOptions::new(&url);
                options.headers = open_net::HeaderMap::new();
                options
            };
            options.routing = ResponseRouting::Manual;
            options.reconnect = ReconnectPolicy::Backoff(open_net::ws::BackoffConfig {
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                ..open_net::ws::BackoffConfig::default()
            });
            options
        }),
    )
    .await??;
    let started = bounded("start backpressure attempt", events.recv())
        .await??
        .ok_or_else(|| error("missing Started"))?;
    let ConnectionEventKind::AttemptStarted { attempt } = &started.kind else {
        return Err(error("missing Started"));
    };
    let first_event = bounded("establish backpressure session", events.recv())
        .await??
        .ok_or_else(|| error("missing Established"))?;
    let ConnectionEventKind::Established {
        connection: first_connection,
    } = &first_event.kind
    else {
        return Err(error("missing Established"));
    };
    check(
        started.sequence == 1
            && first_event.sequence == 2
            && first_connection.attempt_id == attempt.attempt_id
            && first_connection.cycle_id == attempt.cycle_id
            && first_connection.session_id == attempt.session_id,
        "backpressure attempt sequence or identity changed",
    )?;
    let mut last_sequence = first_event.sequence;
    bounded("peer is ready for large body", ready_rx).await??;
    let active_domain = if cancel_in_flight { &business } else { &live };
    let mut active_options = options(active_domain, lane.unwrap_or(MessageLane::Normal));
    active_options.write_timeout = Duration::from_secs(60);
    let body = NetMessage::binary(Bytes::from(vec![0x5a; LARGE_BODY_BYTES]));
    let mut writing: Pin<Box<dyn Future<Output = Result<(), NetError>>>> = match lane {
        Some(_) => {
            let receipt = events
                .session
                .sender()
                .message(body)
                .options(active_options)
                .enqueue()
                .await?;
            Box::pin(async move { receipt.written().await })
        }
        None => {
            let receipt = events
                .session
                .requests()?
                .request(Request::new(RequestId::new("large-domain-body")?, body))
                .options(RequestOptions {
                    send: active_options,
                    response_timeout: Duration::from_secs(60),
                    ..Default::default()
                })
                .enqueue()
                .await?;
            Box::pin(async move { receipt.handle().written().await.map(|_| ()) })
        }
    };
    bounded("first data frame entered real socket", first_bytes_rx).await??;
    check(
        writing.as_mut().now_or_never().is_none(),
        "large body finished before the backpressure barrier",
    )?;
    let mut queued_handles = Vec::new();
    if !cancel_in_flight {
        for lane in [MessageLane::Normal, MessageLane::Urgent] {
            queued_handles.push(
                events
                    .session
                    .sender()
                    .message("revoked-try")
                    .options(options(&business, lane))
                    .try_enqueue()?,
            );
            queued_handles.push(
                events
                    .session
                    .sender()
                    .message("revoked-async")
                    .options(options(&business, lane))
                    .enqueue()
                    .await?,
            );
        }
    }
    business.cancel();
    check(
        !matches!(
            events.session.state()?.state,
            open_net::ws::ConnectionState::Closed(_)
        ),
        "delivery cancellation changed connection scope identity",
    )?;
    check(
        !live.is_cancelled(),
        "delivery cancellation revoked an independent domain",
    )?;
    for handle in queued_handles {
        rejected(
            bounded(
                "cancel queued handle without draining peer",
                handle.written(),
            )
            .await?,
            NetError::from(open_net::error::ErrorKind::Cancelled),
            "queued bare message domain cancellation",
        )?;
    }
    resume_tx
        .send(())
        .map_err(|_| error("peer ended before backpressure release"))?;
    let write_result = bounded("resolve large-body write", writing).await?;
    if cancel_in_flight {
        rejected(
            write_result,
            NetError::from(open_net::error::ErrorKind::DeliveryUnknown),
            "cancel active large body",
        )?;
        bounded(
            "automatic reconnect preserves delivery authorization",
            async {
                let lost = events
                    .recv()
                    .await?
                    .ok_or_else(|| error("missing Disconnected"))?;
                check(
                    lost.sequence == 3
                        && matches!(&lost.kind, ConnectionEventKind::Disconnected { connection, .. }
                if connection.connection_id == first_connection.connection_id),
                    "wrong disconnected identity or sequence",
                )?;
                let started = events
                    .recv()
                    .await?
                    .ok_or_else(|| error("missing reconnect Started"))?;
                let ConnectionEventKind::AttemptStarted { attempt } = &started.kind else {
                    return Err(error("missing reconnect Started"));
                };
                let event = events
                    .recv()
                    .await?
                    .ok_or_else(|| error("missing reconnect Established"))?;
                let ConnectionEventKind::Established { connection } = &event.kind else {
                    return Err(error("missing reconnect Established"));
                };
                check(
                    started.sequence == 4
                        && event.sequence == 5
                        && connection.cycle_id == attempt.cycle_id
                        && connection.attempt_id == attempt.attempt_id
                        && connection.cycle_id != first_connection.cycle_id
                        && connection.connection_id != first_connection.connection_id
                        && connection.session_id == first_connection.session_id,
                    "automatic reconnect changed attempt order or original session identity",
                )?;
                last_sequence = event.sequence;
                Ok::<_, TestError>(())
            },
        )
        .await??;
    } else {
        write_result?;
    }
    let received = bounded("observe raw byte count", drained_rx).await??;
    check(
        if cancel_in_flight {
            (64..LARGE_WIRE_BYTES).contains(&received)
        } else {
            received == LARGE_WIRE_BYTES
        },
        "transport delivery did not follow the selected domain cancellation",
    )?;
    rejected(
        events
            .session
            .sender()
            .message("old-domain-after-reconnect")
            .options(options(&business, MessageLane::Normal))
            .try_enqueue()
            .map_err(|e| e.into_error()),
        NetError::from(open_net::error::ErrorKind::Cancelled),
        "cancelled group revived",
    )?;
    bounded(
        "live-domain sentinel",
        events
            .session
            .sender()
            .message(LIVE_SENTINEL)
            .options(options(&live, MessageLane::Normal))
            .send(),
    )
    .await??;
    bounded("peer confirms live-domain sentinel", sentinel_rx).await??;
    finish_tx
        .send(())
        .map_err(|_| error("peer ended before explicit test cleanup"))?;
    let peer_received = bounded("join backpressure peer", &mut peer.0).await???;
    check(
        peer_received == received,
        "peer byte count changed after drain",
    )?;
    bounded("destroy backpressure client", net.destroy_ws_client(&name)).await??;
    bounded("drain backpressure connection events", async {
        let mut terminated = false;
        while let Some(event) = events.recv().await? {
            check(!terminated, "connection event followed SessionTerminated")?;
            check(
                event.sequence == last_sequence + 1,
                "cleanup event sequence skipped",
            )?;
            last_sequence = event.sequence;
            terminated = matches!(event.kind, ConnectionEventKind::Closed { .. });
        }
        check(terminated, "destroy omitted SessionTerminated")
    })
    .await?
}

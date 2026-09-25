#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use open_net::ws::ConnectionEventKind;
use open_net::ws::{
    ConnectionState, DisconnectedPolicy, Message as DataMessage, OperationPhase, Request,
    RequestId, RequestOptions, SendOptions, TerminationOutcome,
};
use open_net::ws::{ReconnectPolicy, WebSocketClientConfig};
use open_net::{EnqueueError, OpenNet};
use session::ObservedSession;

use socket2::SockRef;
use std::future::Future;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

const BODY_BYTES: usize = 32 * 1024 * 1024;
const FRAME_BYTES: usize = 32 * 1024;
const FRAME_HEADER_BYTES: usize = 8;
const REQUEST_WIRE_BYTES: usize = BODY_BYTES + FRAME_HEADER_BYTES * (BODY_BYTES / FRAME_BYTES);
const FRESH_MESSAGE: &str = "new scope remains writable";

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
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .map_err(|cause| error(format!("{label}: {cause}")))
}

struct PeerTask(JoinHandle<TestResult<usize>>);

impl Drop for PeerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn established(events: &mut ObservedSession) -> TestResult<open_net::ws::ConnectionInfo> {
    let started = bounded("receive Started", events.recv())
        .await??
        .ok_or_else(|| error("session ended before Started"))?;
    let ConnectionEventKind::AttemptStarted { attempt } = &started.kind else {
        return Err(error("expected Started"));
    };
    let event = bounded("receive Established", events.recv())
        .await??
        .ok_or_else(|| error("session ended before Established"))?;
    let ConnectionEventKind::Established { connection } = &event.kind else {
        return Err(error("expected Established"));
    };
    check(
        event.sequence == started.sequence + 1
            && connection.attempt_id == attempt.attempt_id
            && connection.cycle_id == attempt.cycle_id
            && event.session_id == attempt.session_id
            && event.client_id == attempt.client_id,
        "attempt identity or order changed",
    )?;
    Ok(connection.clone())
}

async fn terminated(events: &mut ObservedSession) -> TestResult {
    bounded("wait for complete scope cleanup", async {
        let mut seen_terminal = false;
        while let Some(event) = events.recv().await? {
            if seen_terminal {
                return Err(error("event followed SessionTerminated"));
            }
            if matches!(event.kind, ConnectionEventKind::Established { .. }) {
                return Err(error("cancelled scope established another connection"));
            }
            seen_terminal = matches!(event.kind, ConnectionEventKind::Closed { .. });
        }
        check(seen_terminal, "missing SessionTerminated")
    })
    .await?
}

#[tokio::test]
async fn scope_cancel_during_real_socket_backpressure_retires_both_socket_halves() -> TestResult {
    run_scope_backpressure(true).await
}

#[tokio::test]
async fn real_socket_backpressure_without_revocation_delivers_the_complete_request() -> TestResult {
    run_scope_backpressure(false).await
}

async fn run_scope_backpressure(revoke_while_blocked: bool) -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let (ready_tx, ready_rx) = oneshot::channel();
    let (first_bytes_tx, first_bytes_rx) = oneshot::channel();
    let (resume_read_tx, resume_read_rx) = oneshot::channel();
    let (old_stopped_tx, old_stopped_rx) = oneshot::channel();
    let (old_drained_tx, old_drained_rx) = oneshot::channel();
    let (fresh_received_tx, fresh_received_rx) = oneshot::channel();
    let (finish_peer_tx, finish_peer_rx) = oneshot::channel();
    let mut peer = PeerTask(tokio::spawn(async move {
        let (stream, _) = bounded("accept old socket", listener.accept()).await??;
        SockRef::from(&stream).set_recv_buffer_size(4096)?;
        let receive_buffer = SockRef::from(&stream).recv_buffer_size()?;
        let mut socket = bounded(
            "accept old Upgrade",
            tokio_tungstenite::accept_async(stream),
        )
        .await??;
        ready_tx
            .send(())
            .map_err(|_| error("old peer readiness receiver closed"))?;

        // The client commits only after readiness. No WebSocket read has prefetched
        // data, so this reads the masked binary frame directly from the real socket.
        let mut prefix = [0_u8; 64];
        bounded(
            "observe first data-frame bytes",
            socket.get_mut().read_exact(&mut prefix),
        )
        .await??;
        check(
            prefix.first().copied() == Some(0x02),
            &format!(
                "old request did not begin with a non-final binary frame: {:02x?}",
                &prefix[..FRAME_HEADER_BYTES]
            ),
        )?;
        check(
            prefix.get(1).copied() == Some(0xfe),
            &format!(
                "expected masked frame with a 16-bit length: {:02x?}",
                &prefix[..FRAME_HEADER_BYTES]
            ),
        )?;
        let length_bytes: [u8; 2] = prefix
            .get(2..4)
            .ok_or_else(|| error("missing frame length"))?
            .try_into()?;
        check(
            usize::from(u16::from_be_bytes(length_bytes)) == FRAME_BYTES,
            &format!(
                "unexpected first-frame payload size: {}",
                u16::from_be_bytes(length_bytes)
            ),
        )?;
        first_bytes_tx
            .send(receive_buffer)
            .map_err(|_| error("first-byte receiver closed"))?;

        // Resume immediately after cancellation is published, before local tasks
        // have necessarily stopped. The control run resumes without cancellation.
        bounded("release old peer backpressure", resume_read_rx).await??;
        let received = bounded("drain retired socket to EOF or reset", async {
            let mut received = prefix.len();
            let mut buffer = [0_u8; 8192];
            loop {
                match socket.get_mut().read(&mut buffer).await {
                    Ok(0) => return Ok::<usize, TestError>(received),
                    Ok(count) => {
                        received = received
                            .checked_add(count)
                            .ok_or_else(|| error("received byte count overflow"))?;
                        if received >= REQUEST_WIRE_BYTES {
                            return if revoke_while_blocked {
                                Err(error(format!("the complete old request escaped cancellation: received {received} of {REQUEST_WIRE_BYTES} wire bytes")))
                            } else {
                                Ok(received)
                            };
                        }
                    }
                    Err(cause)
                        if matches!(
                            cause.kind(),
                            std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::ConnectionAborted
                        ) =>
                    {
                        return Ok(received);
                    }
                    Err(cause) => return Err(cause.into()),
                }
            }
        })
        .await??;
        // In the control run keep the peer alive until the confirmed write and
        // explicit scope cleanup; peer EOF must not race the successful commit.
        bounded("old client completed scope cleanup", old_stopped_rx).await??;
        drop(socket);
        old_drained_tx
            .send(received)
            .map_err(|_| error("old socket drain receiver closed"))?;

        let (stream, _) = bounded("accept new scope socket", listener.accept()).await??;
        let mut fresh = bounded(
            "accept new scope Upgrade",
            tokio_tungstenite::accept_async(stream),
        )
        .await??;
        let message = bounded("receive new scope message", fresh.next())
            .await?
            .ok_or_else(|| error("new scope socket ended without its message"))??;
        check(
            matches!(message, Message::Text(ref text) if text.as_str() == FRESH_MESSAGE),
            "new scope received stale or incorrect data",
        )?;
        fresh_received_tx
            .send(())
            .map_err(|_| error("new scope observation receiver closed"))?;
        bounded("release final peer socket", finish_peer_rx).await??;
        Ok(received)
    }));

    let net = OpenNet::new()?;
    let client = bounded(
        "create scoped client",
        net.create_ws_client_with_config("scope-real-backpressure", {
            let mut config = WebSocketClientConfig::default();
            config.queues.normal.max_bytes = BODY_BYTES;
            // A single large socket write may be accepted despite a small send
            // buffer. Bounded frames require repeated writes while the peer is
            // stopped, keeping the complete request behind real backpressure.
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
    let mut old_events = bounded(
        "start old scoped connection",
        session::observe(
            &client,
            session::session_options(&url, ReconnectPolicy::Disabled),
        ),
    )
    .await??;
    established(&mut old_events).await?;
    bounded("old peer is ready", ready_rx).await??;

    let prepared = bounded(
        "prepare large old request",
        old_events
            .session
            .requests()?
            .request(Request::new(
                RequestId::new("scope-backpressured-old-owner")?,
                DataMessage::binary(Bytes::from(vec![0x5a; BODY_BYTES])),
            ))
            .options(RequestOptions {
                send: SendOptions {
                    write_timeout: Duration::from_secs(60),
                    ..SendOptions::default()
                },
                ..RequestOptions::default()
            })
            .prepare(),
    )
    .await?
    .map_err(EnqueueError::into_error)?;
    let receipt = prepared.commit()?;
    let mut writing = Box::pin(receipt.handle().written());
    let receive_buffer = bounded(
        "peer observes first frame before cancellation",
        first_bytes_rx,
    )
    .await??;
    if let Some(result) = writing.as_mut().now_or_never() {
        return Err(error(format!(
            "large send completed before the backpressure barrier: {:?}; peer receive buffer={receive_buffer}",
            result.map(|_| ())
        )));
    }
    if revoke_while_blocked {
        old_events.session.cancel();
    }
    resume_read_tx
        .send(())
        .map_err(|_| error("old peer disappeared before socket cleanup"))?;
    let write_result = bounded("resolve backpressured write", writing).await?;
    if revoke_while_blocked {
        check(
            matches!(write_result, Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::DeliveryUnknown)),
            &format!(
                "partial real-socket send did not report DeliveryUnknown: {:?}",
                write_result.as_ref().map(|_| ())
            ),
        )?;
    } else {
        check(
            write_result.is_ok(),
            &format!(
                "unrevoked request did not finish writing: {:?}",
                write_result.as_ref().map(|_| ())
            ),
        )?;
        old_events.session.cancel();
    }
    terminated(&mut old_events).await?;
    old_stopped_tx
        .send(())
        .map_err(|_| error("old peer ended before scope cleanup"))?;
    let received = bounded("peer observes old socket EOF or reset", old_drained_rx).await??;
    check(
        if revoke_while_blocked {
            (64..REQUEST_WIRE_BYTES).contains(&received)
        } else {
            received == REQUEST_WIRE_BYTES
        },
        &format!("old socket delivery did not match scope revocation: revoked={revoke_while_blocked}, received={received}, complete request={REQUEST_WIRE_BYTES} wire bytes"),
    )?;

    let mut fresh_events = bounded(
        "start new scoped connection",
        session::observe(
            &client,
            session::session_options(&url, ReconnectPolicy::Disabled),
        ),
    )
    .await??;
    established(&mut fresh_events).await?;
    bounded(
        "send new scope message",
        fresh_events
            .session
            .sender()
            .send(DataMessage::text(FRESH_MESSAGE)),
    )
    .await??;
    bounded("peer confirms new owner data", fresh_received_rx).await??;
    fresh_events.session.cancel();
    terminated(&mut fresh_events).await?;
    finish_peer_tx
        .send(())
        .map_err(|_| error("fresh peer ended early"))?;
    let final_received = bounded("join peer task", &mut peer.0).await???;
    check(
        final_received == received,
        "peer byte count changed after its cleanup report",
    )?;
    bounded(
        "destroy test client",
        net.destroy_ws_client("scope-real-backpressure"),
    )
    .await??;
    eprintln!("scope backpressure verified: revoked while blocked={revoke_while_blocked}, old wire bytes={received}/{REQUEST_WIRE_BYTES}, body bytes={BODY_BYTES}, frame bytes={FRAME_BYTES}, peer receive buffer={receive_buffer}; new scope delivered");
    Ok(())
}

#[tokio::test]
async fn same_scope_prepared_request_survives_automatic_reconnect_without_new_identity(
) -> TestResult {
    const ORIGINAL_BODY: &[u8] = b"original operation survives its physical connection";
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let (drop_first_tx, drop_first_rx) = oneshot::channel();
    let (second_accepted_tx, second_accepted_rx) = oneshot::channel();
    let (allow_upgrade_tx, allow_upgrade_rx) = oneshot::channel();
    let (original_received_tx, original_received_rx) = oneshot::channel();
    let (followup_received_tx, followup_received_rx) = oneshot::channel();
    let (finish_peer_tx, finish_peer_rx) = oneshot::channel();
    let mut peer = PeerTask(tokio::spawn(async move {
        let (stream, _) = bounded("accept first reconnect socket", listener.accept()).await??;
        let mut first = bounded(
            "accept first reconnect Upgrade",
            tokio_tungstenite::accept_async(stream),
        )
        .await??;
        bounded("release first physical connection", drop_first_rx).await??;
        check(
            first.next().now_or_never().is_none(),
            "prepared request was sent before commit",
        )?;
        drop(first);

        let (stream, _) = bounded("accept automatic reconnect socket", listener.accept()).await??;
        second_accepted_tx
            .send(())
            .map_err(|_| error("reconnect acceptance receiver closed"))?;
        // Keep the second Upgrade pending while the caller commits the old prepared
        // request. This deterministically exercises its WaitForReconnect policy.
        bounded("allow second Upgrade", allow_upgrade_rx).await??;
        let mut second = bounded(
            "accept second reconnect Upgrade",
            tokio_tungstenite::accept_async(stream),
        )
        .await??;
        let original = bounded("receive preserved operation", second.next())
            .await?
            .ok_or_else(|| error("reconnected peer ended without original operation"))??;
        check(
            matches!(original, Message::Binary(ref body) if body.as_ref() == ORIGINAL_BODY),
            "reconnect changed or lost the original request body",
        )?;
        original_received_tx
            .send(())
            .map_err(|_| error("original operation receiver closed"))?;
        let followup = bounded("receive original scope followup", second.next())
            .await?
            .ok_or_else(|| error("reconnected peer ended before scope followup"))??;
        check(
            matches!(followup, Message::Text(ref text) if text.as_str() == FRESH_MESSAGE),
            "original scope could not submit after reconnect",
        )?;
        followup_received_tx
            .send(())
            .map_err(|_| error("followup receiver closed"))?;
        bounded("finish reconnect peer", finish_peer_rx).await??;
        Ok(2)
    }));
    let net = OpenNet::new()?;
    let client = bounded(
        "create reconnect client",
        net.create_ws_client_with_config("scope-prepared-auto-reconnect", {
            let mut config = WebSocketClientConfig::default();
            config.close_timeout = Duration::from_millis(100);
            config
        }),
    )
    .await??;
    let mut events = bounded(
        "start original scope session",
        session::observe(&client, {
            let mut options = session::session_options(
                &url,
                ReconnectPolicy::Backoff(open_net::ws::BackoffConfig {
                    max_retries: 1,
                    initial_delay: Duration::from_millis(1),
                    max_delay: Duration::from_millis(1),
                    max_elapsed: Some(Duration::from_secs(20)),
                }),
            );
            options.handshake_timeout = Duration::from_secs(10);
            options
        }),
    )
    .await??;
    let first_connection = established(&mut events).await?;
    let first_cycle = first_connection.cycle_id;
    let prepared = bounded(
        "prepare before connection loss",
        events
            .session
            .requests()?
            .request(Request::new(
                RequestId::new("same-scope-prepared-reconnect")?,
                DataMessage::binary(Bytes::from_static(ORIGINAL_BODY)),
            ))
            .options(RequestOptions {
                send: SendOptions {
                    disconnected: DisconnectedPolicy::WaitForReconnect,
                    ..SendOptions::default()
                },
                response_timeout: Duration::from_secs(60),
                ..RequestOptions::default()
            })
            .prepare(),
    )
    .await?
    .map_err(EnqueueError::into_error)?;
    let handle = prepared.handle().clone();
    let original_operation = handle.id();
    drop_first_tx
        .send(())
        .map_err(|_| error("first peer disappeared before preparation"))?;
    let ended = bounded("first ConnectionTerminated", events.recv())
        .await??
        .ok_or_else(|| error("session terminated instead of reconnecting"))?;
    check(
        matches!(&ended.kind, ConnectionEventKind::Disconnected { connection, .. }
        if connection.connection_id == first_connection.connection_id && connection.cycle_id == first_cycle)
            && ended.sequence == 3,
        "wrong connection termination identity or order",
    )?;
    bounded("second socket accepted before Upgrade", second_accepted_rx).await??;
    check(
        !matches!(events.session.state()?.state, ConnectionState::Closed(_))
            && handle.state()?.phase == OperationPhase::Prepared,
        "physical disconnect revoked the business scope",
    )?;
    let receipt = prepared.commit()?;
    let mut written = Box::pin(receipt.handle().written());
    check(
        written.as_mut().now_or_never().is_none(),
        "request completed while second Upgrade was gated",
    )?;
    allow_upgrade_tx
        .send(())
        .map_err(|_| error("second peer disappeared before commit"))?;
    let second_connection = established(&mut events).await?;
    check(
        second_connection.cycle_id != first_cycle
            && second_connection.connection_id != first_connection.connection_id,
        "physical connection identity did not change",
    )?;
    check(
        second_connection.session_id == first_connection.session_id
            && second_connection.client_id == first_connection.client_id,
        "reconnect changed original session ownership",
    )?;
    bounded("preserved request write succeeds", written).await??;
    bounded("peer confirms original operation", original_received_rx).await??;
    check(
        handle.id() == original_operation,
        "reconnect replaced registration token",
    )?;
    check(
        handle.cancel()? == TerminationOutcome::TerminatedAfterWrite,
        "original registration no longer owned the reconnected pending request",
    )?;
    check(
        matches!(
            bounded("finish original pending registration", receipt.response()).await?,
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::Cancelled)),
        "original registration did not retain terminal ownership",
    )?;
    bounded(
        "send using the original business scope",
        events
            .session
            .sender()
            .send(DataMessage::text(FRESH_MESSAGE)),
    )
    .await??;
    bounded("peer confirms same-scope followup", followup_received_rx).await??;
    events.session.cancel();
    terminated(&mut events).await?;
    finish_peer_tx
        .send(())
        .map_err(|_| error("reconnect peer finished early"))?;
    check(
        bounded("join reconnect peer", &mut peer.0).await??? == 2,
        "peer omitted one of the scoped operations",
    )?;
    bounded(
        "destroy reconnect client",
        net.destroy_ws_client("scope-prepared-auto-reconnect"),
    )
    .await??;
    Ok(())
}

#![cfg(feature = "ws-client")]

use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use open_net::{
    DisconnectedTaskPolicy, NetError, OpenNet, ReconnectPolicy, RequestScope,
    RequestTerminationOutcome, WSRequestConfig, WSRequestTrait, WebSocketClientConfig,
    WebSocketConnectionEventKind, WebSocketConnectionEvents, WebSocketContextConnectOptions,
    WebSocketRequestOptions, WsBody,
};
use socket2::SockRef;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

const BODY_BYTES: usize = 32 * 1024 * 1024;
const FRESH_MESSAGE: &str = "new scope remains writable";

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
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .map_err(|cause| error(format!("{label}: {cause}")))
}

struct BinaryRequest(&'static str, Bytes);

impl WSRequestTrait for BinaryRequest {
    fn uuid(&self) -> String {
        self.0.to_owned()
    }
    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Binary(self.1.clone()))
    }
}

struct PeerTask(JoinHandle<TestResult<usize>>);

impl Drop for PeerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn established(events: &mut WebSocketConnectionEvents) -> TestResult {
    let event = bounded("receive Established", events.recv())
        .await??
        .ok_or_else(|| error("session ended before Established"))?;
    check(
        event.kind() == WebSocketConnectionEventKind::Established,
        "expected Established",
    )
}

async fn terminated(events: &mut WebSocketConnectionEvents) -> TestResult {
    bounded("wait for complete scope cleanup", async {
        let mut seen_terminal = false;
        while let Some(event) = events.recv().await? {
            if seen_terminal {
                return Err(error("event followed SessionTerminated"));
            }
            if event.kind() == WebSocketConnectionEventKind::Established {
                return Err(error("cancelled scope established another connection"));
            }
            seen_terminal = event.kind() == WebSocketConnectionEventKind::SessionTerminated;
        }
        check(seen_terminal, "missing SessionTerminated")
    })
    .await?
}

#[tokio::test]
async fn scope_cancel_during_real_socket_backpressure_retires_both_socket_halves() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let (ready_tx, ready_rx) = oneshot::channel();
    let (first_bytes_tx, first_bytes_rx) = oneshot::channel();
    let (resume_read_tx, resume_read_rx) = oneshot::channel();
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
            prefix.first().copied() == Some(0x82),
            "old request was not one final binary frame",
        )?;
        check(
            prefix.get(1).copied() == Some(0xff),
            "expected masked frame with a 64-bit length",
        )?;
        let length_bytes: [u8; 8] = prefix
            .get(2..10)
            .ok_or_else(|| error("missing frame length"))?
            .try_into()?;
        check(
            u64::from_be_bytes(length_bytes) == BODY_BYTES as u64,
            "unexpected old payload size",
        )?;
        first_bytes_tx
            .send(receive_buffer)
            .map_err(|_| error("first-byte receiver closed"))?;

        // Stop reading until cancellation and the session cleanup boundary have
        // both been observed. Kernel-accepted bytes may still arrive afterwards.
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
                        if received >= BODY_BYTES + 14 {
                            return Err(error("the complete old request escaped cancellation"));
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
        net.create_ws_client_with_config(
            "scope-real-backpressure",
            WebSocketClientConfig {
                business_queue_max_bytes: BODY_BYTES,
                data_frame_payload_size: None,
                write_buffer_size: 0,
                max_write_buffer_size: BODY_BYTES + 1024,
                tcp_send_buffer_size: Some(4096),
                data_frame_write_timeout: Duration::from_secs(60),
                heartbeat_interval: Duration::from_secs(3600),
                pong_timeout: Duration::from_secs(7200),
                close_timeout: Duration::from_millis(100),
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??;
    let old_scope = RequestScope::new();
    let mut old_events = bounded(
        "start old scoped connection",
        client.start_connect_with_context(
            WebSocketContextConnectOptions::new(&url, 301)
                .with_headers(Vec::new(), 401)
                .with_request_scope(old_scope.clone()),
        ),
    )
    .await??;
    established(&mut old_events).await?;
    bounded("old peer is ready", ready_rx).await??;

    let prepared = bounded(
        "prepare large old request",
        client.prepare_registered(
            Arc::new(BinaryRequest(
                "scope-backpressured-old-owner",
                Bytes::from(vec![0x5a; BODY_BYTES]),
            )),
            WebSocketRequestOptions::new(WSRequestConfig {
                write_timeout: Duration::from_secs(60),
                send_retry_count: 0,
                idempotent: false,
                ..WSRequestConfig::default()
            })
            .with_scope(old_scope.clone()),
        ),
    )
    .await??;
    let receipt = prepared.commit()?;
    let mut writing = Box::pin(receipt.wait_until_written());
    let receive_buffer = bounded(
        "peer observes first frame before cancellation",
        first_bytes_rx,
    )
    .await??;
    check(
        writing.as_mut().now_or_never().is_none(),
        "large send completed before the backpressure barrier",
    )?;
    old_scope.cancel();
    check(
        matches!(
            bounded("cancel backpressured write", writing).await?,
            Err(NetError::DeliveryUnknown)
        ),
        "partial real-socket send did not report DeliveryUnknown",
    )?;
    terminated(&mut old_events).await?;
    resume_read_tx
        .send(())
        .map_err(|_| error("old peer disappeared before socket cleanup"))?;
    let received = bounded("peer observes old socket EOF or reset", old_drained_rx).await??;
    check(
        (64..BODY_BYTES).contains(&received),
        "old socket did not stop after a partial request",
    )?;

    let fresh_scope = RequestScope::new();
    let mut fresh_events = bounded(
        "start new scoped connection",
        client.start_connect_with_context(
            WebSocketContextConnectOptions::new(&url, 302)
                .with_headers(Vec::new(), 402)
                .with_request_scope(fresh_scope.clone()),
        ),
    )
    .await??;
    established(&mut fresh_events).await?;
    bounded(
        "send new scope message",
        client.send_message_with_options(
            WsBody::Text(FRESH_MESSAGE.to_owned()),
            WebSocketRequestOptions::default().with_scope(fresh_scope.clone()),
        ),
    )
    .await??;
    bounded("peer confirms new owner data", fresh_received_rx).await??;
    fresh_scope.cancel();
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
    eprintln!("scope backpressure verified: old wire bytes={received}, body bytes={BODY_BYTES}, peer receive buffer={receive_buffer}; new scope delivered");
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
        net.create_ws_client_with_config(
            "scope-prepared-auto-reconnect",
            WebSocketClientConfig {
                close_timeout: Duration::from_millis(100),
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??;
    let original_scope = RequestScope::new();
    let mut events = bounded(
        "start original scope session",
        client.start_connect_with_context(
            WebSocketContextConnectOptions::new(&url, 501)
                .with_headers(Vec::new(), 601)
                .with_request_scope(original_scope.clone())
                .with_reconnect(ReconnectPolicy {
                    enabled: true,
                    max_retries: 1,
                    initial_delay: Duration::from_millis(1),
                    max_delay: Duration::from_millis(1),
                    max_elapsed: Some(Duration::from_secs(20)),
                    handshake_timeout: Duration::from_secs(10),
                }),
        ),
    )
    .await??;
    let first_event = bounded("first Established", events.recv())
        .await??
        .ok_or_else(|| error("original scope ended before first connection"))?;
    check(
        first_event.kind() == WebSocketConnectionEventKind::Established,
        "first event was not Established",
    )?;
    let first_cycle = first_event
        .cycle_id()
        .ok_or_else(|| error("missing first physical cycle"))?;
    let prepared = bounded(
        "prepare before connection loss",
        client.prepare_registered(
            Arc::new(BinaryRequest(
                "same-scope-prepared-reconnect",
                Bytes::from_static(ORIGINAL_BODY),
            )),
            WebSocketRequestOptions::new(WSRequestConfig {
                disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
                send_retry_count: 0,
                response_timeout: Duration::from_secs(60),
                ..WSRequestConfig::default()
            })
            .with_scope(original_scope.clone()),
        ),
    )
    .await??;
    let registration = prepared.registration().clone();
    let original_registration_token = registration.token();
    drop_first_tx
        .send(())
        .map_err(|_| error("first peer disappeared before preparation"))?;
    let ended = bounded("first ConnectionTerminated", events.recv())
        .await??
        .ok_or_else(|| error("session terminated instead of reconnecting"))?;
    check(
        ended.kind() == WebSocketConnectionEventKind::ConnectionTerminated
            && ended.cycle_id() == Some(first_cycle),
        "wrong connection termination identity",
    )?;
    bounded("second socket accepted before Upgrade", second_accepted_rx).await??;
    check(
        !original_scope.is_cancelled(),
        "physical disconnect revoked the business scope",
    )?;
    let receipt = prepared.commit()?;
    let mut written = Box::pin(receipt.wait_until_written());
    check(
        written.as_mut().now_or_never().is_none(),
        "request completed while second Upgrade was gated",
    )?;
    allow_upgrade_tx
        .send(())
        .map_err(|_| error("second peer disappeared before commit"))?;
    let second_event = bounded("second Established", events.recv())
        .await??
        .ok_or_else(|| error("session ended before reconnection"))?;
    check(
        second_event.kind() == WebSocketConnectionEventKind::Established,
        "reconnect omitted Established",
    )?;
    check(
        second_event
            .cycle_id()
            .is_some_and(|cycle| cycle != first_cycle),
        "physical connection cycle did not change",
    )?;
    check(
        second_event.session_id() == first_event.session_id()
            && second_event.client_instance_id() == first_event.client_instance_id()
            && second_event.session_context_id() == first_event.session_context_id(),
        "reconnect changed original session ownership",
    )?;
    let completion = bounded("preserved request write succeeds", written).await??;
    bounded("peer confirms original operation", original_received_rx).await??;
    check(
        registration.token() == original_registration_token,
        "reconnect replaced registration token",
    )?;
    check(
        matches!(
            registration.cancel()?,
            RequestTerminationOutcome::Terminated {
                error: NetError::Cancelled
            }
        ),
        "original registration no longer owned the reconnected pending request",
    )?;
    check(
        matches!(
            bounded("finish original pending registration", completion.wait()).await?,
            Err(NetError::Cancelled)
        ),
        "original registration did not retain terminal ownership",
    )?;
    bounded(
        "send using the original business scope",
        client.send_message_with_options(
            WsBody::Text(FRESH_MESSAGE.to_owned()),
            WebSocketRequestOptions::default().with_scope(original_scope.clone()),
        ),
    )
    .await??;
    bounded("peer confirms same-scope followup", followup_received_rx).await??;
    original_scope.cancel();
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

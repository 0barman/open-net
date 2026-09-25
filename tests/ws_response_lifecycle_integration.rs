//! Network response completion and application processing are independent owners.
#![cfg(feature = "ws-client")]
#[path = "support/session.rs"]
mod session;
#[path = "support/v2_peer.rs"]
mod v2;
use futures::FutureExt;
use open_net::ws::*;
use open_net::{error::ErrorKind, OpenNet};
use session::{session_options, SessionGuard};
use std::{future::Future, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use v2::{check, Peer, TestResult};
type TestError = Box<dyn std::error::Error + Send + Sync>;
fn error(value: impl Into<String>) -> TestError {
    std::io::Error::other(value.into()).into()
}
async fn bounded<T>(label: &str, f: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .map_err(|e| error(format!("{label}: {e}")))
}
struct AbortOnDrop<T>(JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn resolve(
    session: &Session,
    inbox: &mut MessageReceiver,
    peer: &mut Peer,
    id: &str,
) -> TestResult<(Response, RequestHandle)> {
    let receipt = session
        .requests()?
        .request(v2::request(id)?)
        .options(RequestOptions {
            response_timeout: Duration::from_millis(100),
            ..Default::default()
        })
        .enqueue()
        .await?;
    let handle = receipt.handle().clone();
    peer.next().await?;
    peer.text(format!("final:{id}"))?;
    let incoming = v2::bounded(inbox.recv())
        .await??
        .ok_or("response missing")?;
    check(
        session
            .response_resolver()?
            .resolve(handle.registration(), &incoming)?
            == ResolveOutcome::Resolved,
        "claim failed",
    )?;
    Ok((receipt.response().await?, handle))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_response_outlives_network_deadline_expiry_disconnect_and_shutdown() -> TestResult {
    let (net, client, mut session, mut peer) =
        v2::connected("claim-lifetime", WebSocketClientConfig::default()).await?;
    let mut inbox = session.take_messages().ok_or("inbox")?;
    let mut events = session.subscribe_tasks(TaskEventOptions::default())?;
    let (response, handle) = resolve(&session, &mut inbox, &mut peer, "owned").await?;
    tokio::time::sleep(Duration::from_millis(130)).await;
    check(
        handle.expire()? == TerminationOutcome::AlreadyFinished
            && handle.cancel()? == TerminationOutcome::AlreadyFinished,
        "late terminal rewrote response",
    )?;
    session.close().await?;
    client.shutdown().await?;
    net.destroy_ws_client("claim-lifetime").await?;
    check(
        response.request_id().as_str() == "owned"
            && response.message().as_text() == Some("final:owned"),
        "owned response lost body on shutdown",
    )?;
    let event = v2::bounded(events.recv()).await??.ok_or("terminal event")?;
    check(
        matches!(event.result, Ok(TaskSuccess::ResponseReceived)),
        "network success changed",
    )?;
    check(
        v2::bounded(events.recv()).await??.is_none(),
        "second network terminal",
    )?;
    Ok(())
}
async fn handoff(mode: u8) -> TestResult {
    let (_net, client, mut session, mut peer) =
        v2::connected("business-handoff", WebSocketClientConfig::default()).await?;
    let mut inbox = session.take_messages().ok_or("inbox")?;
    let mut events = session.subscribe_tasks(TaskEventOptions::default())?;
    let (response, handle) = resolve(&session, &mut inbox, &mut peer, "business").await?;
    let (tx, mut rx) = mpsc::channel(1);
    match mode {
        0 => {
            tx.try_send(response.clone())?;
            check(
                matches!(
                    tx.try_send(response.clone()),
                    Err(mpsc::error::TrySendError::Full(_))
                ),
                "application queue not full",
            )?;
            check(rx.recv().await.is_some(), "application lost first response")?;
        }
        1 => {
            drop(rx);
            check(
                matches!(
                    tx.try_send(response.clone()),
                    Err(mpsc::error::TrySendError::Closed(_))
                ),
                "closed application queue accepted response",
            )?;
        }
        _ => {
            let (done, receiver) = oneshot::channel::<Result<Response, &str>>();
            drop(receiver);
            check(
                done.send(Ok(response.clone())).is_err(),
                "closed business completion receiver accepted result",
            )?;
        }
    }
    check(
        session.requests()?.pending_snapshot()?.is_empty(),
        "business handoff restored pending ownership",
    )?;
    check(
        handle.cancel()? == TerminationOutcome::AlreadyFinished,
        "business failure rewrote network success",
    )?;
    client.shutdown().await?;
    let event = v2::bounded(events.recv())
        .await??
        .ok_or("network terminal")?;
    check(
        matches!(event.result, Ok(TaskSuccess::ResponseReceived)),
        "business failure became network error",
    )?;
    check(
        v2::bounded(events.recv()).await??.is_none(),
        "business failure duplicated network terminal",
    )?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_response_processing_queue_full_keeps_network_success_and_business_failure(
) -> TestResult {
    handoff(0).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_response_processing_queue_closed_keeps_network_success_and_business_failure(
) -> TestResult {
    handoff(1).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_response_business_receiver_closing_does_not_restore_network_ownership(
) -> TestResult {
    handoff(2).await
}
struct Protocol;
impl ResponseProtocol for Protocol {
    fn route(&self, incoming: &IncomingMessage) -> Result<ResponseRoute, open_net::BoxError> {
        let Some(text) = incoming.message().and_then(Message::as_text) else {
            return Ok(ResponseRoute::Unmatched);
        };
        let Some((kind, id)) = text.split_once(':') else {
            return Ok(ResponseRoute::Unmatched);
        };
        Ok(match kind {
            "intermediate" => ResponseRoute::Intermediate {
                request_id: RequestId::new(id)?,
            },
            "final" | "rejected" => ResponseRoute::Final {
                request_id: RequestId::new(id)?,
            },
            _ => ResponseRoute::Unmatched,
        })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixture_intermediate_unknown_final_and_rejected_keep_transport_and_business_separate(
) -> TestResult {
    let mut peer = Peer::start().await?;
    let net = OpenNet::new()?;
    let client = net.create_ws_client("protocol-dispositions").await?;
    let mut options = v2::options(peer.url.clone());
    options.routing = ResponseRouting::protocol(Protocol);
    let mut session = client.connect(options).await?;
    let mut inbox = session.take_messages().ok_or("inbox")?;
    let mut events = session.subscribe_tasks(TaskEventOptions::default())?;
    let receipt = session
        .requests()?
        .request(v2::request("history")?)
        .enqueue()
        .await?;
    peer.next().await?;
    for kind in ["intermediate", "unknown", "intermediate"] {
        peer.text(format!("{kind}:history"))?;
        v2::bounded(inbox.recv()).await??.ok_or("packet missing")?;
        check(
            session.requests()?.pending_snapshot()?.len() == 1,
            "nonfinal consumed pending",
        )?;
        check(
            receipt.handle().state()?.result.is_none(),
            "nonfinal ended request",
        )?;
    }
    peer.text("final:history")?;
    check(
        receipt.response().await?.message().as_text() == Some("final:history"),
        "final payload",
    )?;
    peer.text("final:history")?;
    v2::bounded(inbox.recv()).await??;
    let rejected = session
        .requests()?
        .request(v2::request("rejected")?)
        .enqueue()
        .await?;
    peer.next().await?;
    peer.text("rejected:rejected")?;
    let response = rejected.response().await?;
    check(
        response
            .message()
            .as_text()
            .is_some_and(|t| t.starts_with("rejected:")),
        "business rejection lost body",
    )?;
    for _ in 0..2 {
        check(
            matches!(
                v2::bounded(events.recv())
                    .await??
                    .ok_or("event missing")?
                    .result,
                Ok(TaskSuccess::ResponseReceived)
            ),
            "business rejection became transport error",
        )?;
    }
    let timeout = session
        .requests()?
        .request(v2::request("expiry")?)
        .options(RequestOptions {
            response_timeout: Duration::from_millis(100),
            ..Default::default()
        })
        .enqueue()
        .await?;
    peer.next().await?;
    for _ in 0..3 {
        peer.text("intermediate:expiry")?;
        v2::bounded(inbox.recv()).await??;
    }
    check(
        matches!(v2::bounded(timeout.response()).await?,Err(e) if e.kind()==ErrorKind::TimedOut),
        "intermediate extended deadline",
    )?;
    peer.text("final:expiry")?;
    v2::bounded(inbox.recv()).await??;
    check(
        session.requests()?.pending_snapshot()?.is_empty(),
        "late final resurrected pending",
    )?;
    check(
        matches!(v2::bounded(events.recv()).await??.ok_or("timeout event")?.result,Err(e) if e.kind()==ErrorKind::TimedOut),
        "wrong timeout event",
    )?;
    client.shutdown().await?;
    check(
        v2::bounded(events.recv()).await??.is_none(),
        "duplicate/late response emitted terminal",
    )?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saved_registration_cannot_claim_replacement_after_expiry() -> TestResult {
    let (_net, client, mut session, mut peer) =
        v2::connected("stale-registration", WebSocketClientConfig::default()).await?;
    let mut inbox = session.take_messages().ok_or("inbox")?;
    let resolver = session.response_resolver()?;
    let old = session
        .requests()?
        .request(v2::request("reused")?)
        .enqueue()
        .await?;
    let old_handle = old.handle().clone();
    peer.next().await?;
    peer.text("old")?;
    let incoming = v2::bounded(inbox.recv()).await??.ok_or("old response")?;
    old_handle.expire()?;
    check(
        matches!(old.response().await,Err(e) if e.kind()==ErrorKind::TimedOut),
        "expiry changed",
    )?;
    let new = session
        .requests()?
        .request(v2::request("reused")?)
        .enqueue()
        .await?;
    peer.next().await?;
    check(
        resolver.resolve(old_handle.registration(), &incoming)? == ResolveOutcome::StaleOrFinished,
        "old token claimed new registration",
    )?;
    check(
        session.requests()?.pending_snapshot()?.len() == 1,
        "old token removed replacement",
    )?;
    peer.text("new")?;
    let fresh = v2::bounded(inbox.recv()).await??.ok_or("new response")?;
    check(
        resolver.resolve(new.handle().registration(), &fresh)? == ResolveOutcome::Resolved,
        "new token failed",
    )?;
    new.response().await?;
    client.shutdown().await?;
    Ok(())
}
/// The server keeps its reader stopped after witnessing real frame bytes, but sends a
/// Ping plus a text marker in the other direction. Receiving the marker proves that the
/// client reader is active before cancellation of this individual registration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registration_cancel_during_real_backpressure_retires_an_active_ping_reader() -> TestResult
{
    use bytes::Bytes;
    use socket2::SockRef;
    const BODY_BYTES: usize = 32 * 1024 * 1024;
    const FRAME_BYTES: usize = 32 * 1024;
    const WIRE_BYTES: usize = BODY_BYTES + (BODY_BYTES / FRAME_BYTES) * 8;
    const NAME: &str = "registration-active-reader-backpressure";
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let (ready_tx, ready_rx) = oneshot::channel();
    let (prefix_tx, prefix_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let mut peer = AbortOnDrop(tokio::spawn(async move {
        let (stream, _) = bounded("accept backpressure peer", listener.accept()).await??;
        SockRef::from(&stream).set_recv_buffer_size(4096)?;
        let receive_buffer = SockRef::from(&stream).recv_buffer_size()?;
        let mut socket = bounded(
            "upgrade backpressure peer",
            tokio_tungstenite::accept_async(stream),
        )
        .await??;
        ready_tx
            .send(())
            .map_err(|_| error("peer readiness receiver closed"))?;
        let mut prefix = [0_u8; 64];
        bounded(
            "observe actual binary prefix",
            socket.get_mut().read_exact(&mut prefix),
        )
        .await??;
        check(
            prefix.first().copied() == Some(0x02) && prefix.get(1).copied() == Some(0xfe),
            "expected the first non-final masked binary fragment",
        )?;
        let length: [u8; 2] = prefix
            .get(2..4)
            .ok_or_else(|| error("missing frame length"))?
            .try_into()?;
        check(
            usize::from(u16::from_be_bytes(length)) == FRAME_BYTES,
            "wrong backpressured fragment size",
        )?;
        prefix_tx
            .send(receive_buffer)
            .map_err(|_| error("prefix observer closed"))?;
        // Valid, unmasked server frames. Raw output avoids prefetched inbound bytes after
        // taking the binary prefix directly from the underlying socket.
        bounded(
            "send server Ping and reader marker",
            socket
                .get_mut()
                .write_all(b"\x89\x04ping\x81\x0dreader-active"),
        )
        .await??;
        bounded("release backpressured peer", resume_rx).await??;
        let received = bounded("drain cancelled registration socket", async {
            let mut received = prefix.len();
            let mut buffer = [0_u8; 8192];
            loop {
                match socket.get_mut().read(&mut buffer).await {
                    Ok(0) => return Ok::<usize, TestError>(received),
                    Ok(count) => {
                        received = received
                            .checked_add(count)
                            .ok_or_else(|| error("byte count overflow"))?;
                        if received >= WIRE_BYTES {
                            return Err(error(
                                "active reader flushed the complete cancelled request",
                            ));
                        }
                    }
                    Err(failure)
                        if matches!(
                            failure.kind(),
                            std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::ConnectionAborted
                        ) =>
                    {
                        return Ok(received);
                    }
                    Err(failure) => return Err(failure.into()),
                }
            }
        })
        .await??;
        Ok::<usize, TestError>(received)
    }));
    let net = OpenNet::new()?;
    let client = bounded(
        "create individual cancellation client",
        net.create_ws_client_with_config(NAME, {
            let mut config = WebSocketClientConfig::default();
            config.queues.normal.max_bytes = BODY_BYTES;
            // A single large send can be accepted by the OS before the peer
            // consumes it; SO_SNDBUF is not a portable per-call byte cap.
            // Small fragments force repeated socket writes while the peer
            // stops reading. Exact partial-frame retirement (including None)
            // is also covered by the scripted send-chain component tests.
            config.frames.data_frame_payload_size = Some(FRAME_BYTES);
            config.frames.write_buffer_size = 0;
            config.frames.max_write_buffer_size = BODY_BYTES + 1024;
            config.tcp.send_buffer_size = Some(4096);
            config.frames.data_frame_write_timeout = Duration::from_secs(60);
            config.close_timeout = Duration::from_millis(30);
            config.heartbeat = Some(open_net::ws::HeartbeatConfig {
                interval: Duration::from_secs(3600),
                pong_timeout: Duration::from_secs(7200),
            });
            config
        }),
    )
    .await??;
    let mut _session = bounded(
        "connect individual cancellation peer",
        SessionGuard::establish(&client, session_options(&url, ReconnectPolicy::Disabled)),
    )
    .await??;
    let mut inbound = _session.session.take_messages().ok_or("inbox")?;
    bounded("peer handshake complete", ready_rx).await??;
    let prepared = bounded(
        "prepare individual backpressured request",
        _session
            .session
            .requests()?
            .request(open_net::ws::Request::new(
                RequestId::new("registration-large-body")?,
                Message::binary(Bytes::from(vec![0x5a; BODY_BYTES])),
            ))
            .options(RequestOptions {
                send: SendOptions {
                    write_timeout: Duration::from_secs(60),
                    ..Default::default()
                },
                ..Default::default()
            })
            .prepare(),
    )
    .await??;
    let handle = prepared.handle().clone();
    let _receipt = prepared.commit()?;
    let mut written = Box::pin(handle.written());
    let receive_buffer = bounded("peer saw partial frame", prefix_rx).await??;
    let marker = bounded("active reader processed server Ping", inbound.recv())
        .await??
        .ok_or("reader marker")?;
    check(
        marker
            .message()
            .and_then(Message::as_text)
            .ok_or("marker text")?
            == "reader-active",
        "reader marker did not follow Ping",
    )?;
    drop(marker);
    // Check after the reader barrier: waiting for the marker gives the writer
    // time to progress, so an earlier Pending observation is insufficient.
    let premature_write = written
        .as_mut()
        .now_or_never()
        .map(|result| result.map(|_| ()));
    check(
        premature_write.is_none(),
        &format!(
            "write was not backpressured at cancellation barrier: \
             receipt={premature_write:?}, receive_buffer={receive_buffer}, \
             connection_error={:?}",
            _session.session.state()?,
        ),
    )?;
    let cancelled = handle.cancel()?;
    if cancelled != TerminationOutcome::DeliveryUnknown {
        return Err(error(format!(
            "individual writing cancel did not report uncertain delivery: \
             outcome={cancelled:?}, receipt={:?}, connection_error={:?}",
            written
                .as_mut()
                .now_or_never()
                .map(|result| result.map(|_| ())),
            _session.session.state()?,
        )));
    }
    // Release the peer immediately, before awaiting a network shutdown notification. This
    // lets any incorrectly surviving read half compete to flush its shared write buffer.
    resume_tx
        .send(())
        .map_err(|_| error("peer exited before cancellation release"))?;
    let receipt = bounded("cancelled write receipt", written)
        .await?
        .map(|_| ());
    check(
        receipt.as_ref().map_err(|error| error.kind())
            == Err(open_net::error::ErrorKind::DeliveryUnknown),
        &format!("writing receipt lost uncertain delivery: {receipt:?}"),
    )?;
    let received = bounded("join cancellation peer", &mut peer.0).await???;
    check(
        (64..WIRE_BYTES).contains(&received),
        &format!(
            "cancellation did not retire the partial request: {received}/{WIRE_BYTES} wire bytes"
        ),
    )?;
    check(
        _session.session.requests()?.pending_snapshot()?.is_empty(),
        "cancelled individual registration leaked pending",
    )?;
    bounded(
        "destroy individual cancellation client",
        net.destroy_ws_client(NAME),
    )
    .await??;
    eprintln!(
        "individual registration + active Ping reader: {received}/{WIRE_BYTES} wire bytes, {BODY_BYTES} body bytes, {FRAME_BYTES} bytes/fragment, receive_buffer={receive_buffer}; partial socket retired"
    );
    Ok(())
}

//! Exercise a fragmented interruption through the queue-owning writer.

use super::*;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::{DisconnectedPolicy, RequestOptions, SendOptions};
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

const FIRST_ID: &str = "interrupted-continuation";
const FIRST_BODY: &[u8] = b"abcdefghi";
const NEXT_BODY: &[u8] = b"next";
const GENERATION: u64 = 1;
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

struct QueueBoundarySink {
    messages: Arc<Mutex<Vec<Message>>>,
    controls: mpsc::Sender<ControlMessage>,
    completed_flushes: usize,
    entered: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}

impl Sink<Message> for QueueBoundarySink {
    type Error = WsError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), WsError> {
        let this = self.get_mut();
        let first = {
            let mut messages = this.messages.lock().map_err(|error| {
                WsError::Io(std::io::Error::other(format!("record messages: {error}")))
            })?;
            messages.push(message);
            messages.len() == 1
        };
        if first {
            for _ in 0..MAX_READY_CONTROLS_PER_BOUNDARY {
                this.controls
                    .try_send(ControlMessage::FlushAutomatic)
                    .map_err(|error| {
                        WsError::Io(std::io::Error::other(format!("inject controls: {error}")))
                    })?;
            }
        }
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        let this = self.get_mut();
        if this.completed_flushes == MAX_READY_CONTROLS_PER_BOUNDARY {
            if let Some(entered) = this.entered.take() {
                if entered.send(()).is_err() {
                    return Poll::Ready(Err(WsError::Io(std::io::Error::other(
                        "boundary observer closed",
                    ))));
                }
            }
            if let Some(release) = this.release.as_mut() {
                match std::future::Future::poll(Pin::new(release), cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => this.release = None,
                    Poll::Ready(Err(error)) => {
                        return Poll::Ready(Err(WsError::Io(std::io::Error::other(format!(
                            "boundary release: {error}"
                        )))));
                    }
                }
            }
        }
        let Some(completed) = this.completed_flushes.checked_add(1) else {
            return Poll::Ready(Err(WsError::Io(std::io::Error::other(
                "flush counter overflow",
            ))));
        };
        this.completed_flushes = completed;
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }
}

fn verify_only_prefix(messages: &Mutex<Vec<Message>>) -> TestResult {
    let messages = messages
        .lock()
        .map_err(|error| test_error(format!("inspect messages: {error}")))?;
    check_eq!(messages.len(), 1)?;
    check!(matches!(
        messages.first(),
        Some(Message::Frame(frame))
            if frame.header().opcode == OpCode::Data(OpData::Binary)
                && !frame.header().is_final
                && frame.payload() == b"abc"
    ))
}

async fn check_writer_interruption(deadline: bool, urgent_followup: bool) -> TestResult {
    let mut config = crate::ws::WebSocketClientConfig::default();
    config.queues.normal.max_items = 2;
    config.queues.normal.max_bytes =
        FIRST_BODY.len() + if urgent_followup { 0 } else { NEXT_BODY.len() };
    config.queues.urgent.max_items = 2;
    config.queues.urgent.max_bytes = NEXT_BODY.len();
    config.requests.max_pending = 1;
    let runtime = fixture::runtime(config, crate::ws::ResponseRouting::Manual, true).await?;
    runtime.activate_connection(crate::ws::ConnectionId::from_allocated(GENERATION))?;
    let source = runtime.queue.clone();
    let urgent = runtime.urgent_queue.clone();
    let next_queue = if urgent_followup { &urgent } else { &source };
    let pending = runtime.pending.clone();
    let requests = fixture::requests(&runtime);
    let sender = fixture::sender(&runtime);
    let options = SendOptions {
        write_timeout: WRITE_TIMEOUT,
        retry: crate::ws::SendRetryPolicy::Idempotent { max_retries: 2 },
        disconnected: DisconnectedPolicy::WaitForReconnect,
        ..Default::default()
    };
    let first_receipt = requests
        .request(crate::ws::Request::new(
            crate::ws::RequestId::new(FIRST_ID)?,
            crate::ws::Message::binary(Bytes::from_static(FIRST_BODY)),
        ))
        .options(RequestOptions {
            send: options.clone(),
            ..Default::default()
        })
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    let first_handle = first_receipt.handle().clone();
    let (control_tx, control_rx) = mpsc::channel(MAX_READY_CONTROLS_PER_BOUNDARY);
    let (io_event_tx, mut io_event_rx) = mpsc::channel(2);
    let (entered_tx, mut entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let messages = Arc::new(Mutex::new(Vec::new()));
    let sink = QueueBoundarySink {
        messages: messages.clone(),
        controls: control_tx,
        completed_flushes: 0,
        entered: Some(entered_tx),
        release: Some(release_rx),
    };
    let started_at = Instant::now();
    // Keep the future local: failure of any check also drops the writer and its A.
    let mut writer = Box::pin(run_write_loop(
        sink,
        WriteLoopContext {
            cancel_domain_gate: None,
            queue: source.clone(),
            urgent_queue: urgent.clone(),
            control_rx,
            pending_requests: pending.clone(),
            io_event_tx,
            generation: GENERATION,
            cancel: CancellationToken::new(),
            data_frame_payload_size: Some(3),
            control_write_timeout: Duration::from_secs(10),
            data_frame_write_timeout: Duration::from_secs(10),
            heartbeat_config: Some(crate::ws::HeartbeatConfig {
                interval: Duration::from_secs(3600),
                pong_timeout: Duration::from_secs(7200),
            }),
            heartbeat: Arc::new(HeartbeatState::new(GENERATION)),
        },
        CancellationToken::new(),
    ));
    let mut reached = false;
    for _ in 0..64 {
        check!(
            writer.as_mut().now_or_never().is_none(),
            "writer ended before gate"
        )?;
        match entered_rx.try_recv() {
            Ok(()) => {
                reached = true;
                break;
            }
            Err(oneshot::error::TryRecvError::Empty) => {}
            Err(error) => return Err(test_error(format!("gate observer: {error}"))),
        }
    }
    check!(reached, "writer did not reach the control-flush gate")?;
    verify_only_prefix(&messages)?;
    check_eq!(requests.pending_snapshot()?.len(), 1)?;

    // Queue B after A starts so the urgent variant cannot precede A's first frame.
    let next_receipt = sender
        .message(crate::ws::Message::binary(Bytes::from_static(NEXT_BODY)))
        .options(SendOptions {
            lane: if urgent_followup {
                crate::ws::MessageLane::Urgent
            } else {
                crate::ws::MessageLane::Normal
            },
            ..options.clone()
        })
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    check_eq!(
        sender
            .message(crate::ws::Message::binary(Bytes::from_static(FIRST_BODY)))
            .options(options.clone())
            .try_enqueue()
            .err()
            .map(|e| e.error().kind()),
        Some(crate::error::ErrorKind::QueueFull)
    )?;
    release_tx
        .send(())
        .map_err(|_| test_error("release gate receiver closed"))?;
    check!(
        writer.as_mut().now_or_never().is_none(),
        "missing fairness yield"
    )?;
    verify_only_prefix(&messages)?;
    if deadline {
        let expires = started_at
            .checked_add(WRITE_TIMEOUT)
            .ok_or_else(|| test_error("test write deadline overflow"))?;
        tokio::time::advance(expires.saturating_duration_since(Instant::now())).await;
    } else {
        first_handle.cancel()?;
    }
    check!(
        writer.as_mut().now_or_never().is_some(),
        "interrupted prefix must stop the writer before dispatching B"
    )?;
    drop(writer);
    verify_only_prefix(&messages)?;
    check_eq!(
        first_handle.written().await.err().map(|e| e.kind()),
        Some(crate::error::ErrorKind::DeliveryUnknown)
    )?;
    check_eq!(
        first_receipt.response().await.err().map(|e| e.kind()),
        Some(crate::error::ErrorKind::DeliveryUnknown)
    )?;
    check!(requests.pending_snapshot()?.is_empty())?;
    check!(matches!(
        io_event_rx.try_recv(),
        Ok(IoEvent::WriteEnded {
            generation: GENERATION,
            error,
            ..
        }) if matches!(error.kind(), crate::error::ErrorKind::DeliveryUnknown)))?;
    check!(matches!(
        io_event_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ))?;
    check!(next_receipt.state()?.result.is_none())?;
    let capacity = sender
        .message(crate::ws::Message::binary(Bytes::from_static(FIRST_BODY)))
        .options(options)
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    let next = next_queue.try_next().ok_or_else(|| test_error("B lost"))?;
    check_eq!(next.message, Message::Binary(Bytes::from_static(NEXT_BODY)))?;
    check_eq!(next.attempt, 0)?;
    next.complete(Err(NetError::from(crate::error::ErrorKind::Cancelled)));
    check_eq!(
        next_receipt.written().await.err().map(|e| e.kind()),
        Some(crate::error::ErrorKind::Cancelled)
    )?;
    let probe = source
        .try_next()
        .ok_or_else(|| test_error("permit probe missing"))?;
    probe.complete(Err(NetError::from(crate::error::ErrorKind::Cancelled)));
    check!(capacity.written().await.is_err())?;
    check!(source.try_next().is_none())?;
    check!(urgent.try_next().is_none())?;
    // Reuse the same request identity after old cancellation. The old handle may not affect it.
    let replacement = requests
        .request(crate::ws::Request::new(
            crate::ws::RequestId::new(FIRST_ID)?,
            crate::ws::Message::binary(Bytes::from_static(FIRST_BODY)),
        ))
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    check_eq!(
        first_handle.cancel()?,
        crate::ws::TerminationOutcome::AlreadyFinished
    )?;
    check_eq!(requests.pending_snapshot()?.len(), 1)?;
    replacement.handle().cancel()?;
    check!(replacement.response().await.is_err())?;
    check!(requests.pending_snapshot()?.is_empty())?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn continuation_dispatch_cancel_stops_normal_followup_and_releases_capacity() -> TestResult {
    check_writer_interruption(false, false).await
}

#[tokio::test(start_paused = true)]
async fn continuation_dispatch_cancel_stops_urgent_followup_and_releases_capacity() -> TestResult {
    check_writer_interruption(false, true).await
}

#[tokio::test(start_paused = true)]
async fn continuation_deadline_stops_normal_followup_and_releases_capacity() -> TestResult {
    check_writer_interruption(true, false).await
}

#[tokio::test(start_paused = true)]
async fn continuation_deadline_stops_urgent_followup_and_releases_capacity() -> TestResult {
    check_writer_interruption(true, true).await
}

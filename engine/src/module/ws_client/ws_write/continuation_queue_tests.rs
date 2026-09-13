//! Exercise a fragmented interruption through the queue-owning writer.

use super::*;
use crate::api::traits::ws::ws_body::WsBody;
use crate::api::traits::ws::ws_request_config::{DisconnectedTaskPolicy, WSRequestConfig};
use crate::api::traits::ws::ws_request_trait::WSRequestTrait;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::module::ws_client::write::queued_request::DispatchPhase;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

const FIRST_ID: &str = "interrupted-continuation";
const NEXT_ID: &str = "next-complete-message";
const FIRST_BODY: &[u8] = b"abcdefghi";
const NEXT_BODY: &[u8] = b"next";
const GENERATION: u64 = 914;
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

struct TrackedRequest;

impl WSRequestTrait for TrackedRequest {
    fn uuid(&self) -> String {
        FIRST_ID.to_owned()
    }

    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Binary(Bytes::from_static(FIRST_BODY)))
    }
}

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

fn enqueue_untracked(
    queue: &PriorityWriteQueue,
    id: &str,
    body: &'static [u8],
) -> Result<oneshot::Receiver<Result<(), NetError>>, NetError> {
    queue.try_enqueue(
        id.to_owned(),
        None,
        Message::Binary(Bytes::from_static(body)),
        body.len(),
        WSRequestConfig {
            expect_response: false,
            disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
            ..WSRequestConfig::default()
        },
        &CancellationToken::new(),
        CancellationToken::new(),
        DispatchPhase::new(),
        CancellationToken::new(),
    )
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
    let source = PriorityWriteQueue::new(
        2,
        FIRST_BODY.len() + if urgent_followup { 0 } else { NEXT_BODY.len() },
    )?;
    let urgent = PriorityWriteQueue::new(2, NEXT_BODY.len())?;
    let next_queue = if urgent_followup { &urgent } else { &source };
    let pending = PendingRequestView::with_capacity(1);
    // Both terminal causes must suppress retries even when the request qualifies.
    let config = WSRequestConfig {
        write_timeout: WRITE_TIMEOUT,
        idempotent: true,
        send_retry_count: 2,
        disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
        ..WSRequestConfig::default()
    };
    let (token, completion) = pending.reserve(Arc::new(TrackedRequest), &config)?;
    let phase = DispatchPhase::new();
    phase.set_pending_cleanup(pending.clone(), FIRST_ID.to_owned(), token)?;
    let dispatch_cancel = CancellationToken::new();
    let mut first_receipt = source.try_enqueue(
        FIRST_ID.to_owned(),
        Some(token),
        Message::Binary(Bytes::from_static(FIRST_BODY)),
        FIRST_BODY.len(),
        config.clone(),
        &CancellationToken::new(),
        dispatch_cancel.clone(),
        phase,
        CancellationToken::new(),
    )?;
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
            heartbeat_interval: Duration::from_secs(3600),
            pong_timeout: Duration::from_secs(7200),
            response_dispatch_grace: Duration::ZERO,
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
    check_eq!(pending.len(), 1)?;

    // Queue B after A starts so the urgent variant cannot precede A's first frame.
    let mut next_receipt = enqueue_untracked(next_queue, NEXT_ID, NEXT_BODY)?;
    check!(matches!(
        enqueue_untracked(&source, "capacity-while-writing", FIRST_BODY),
        Err(NetError::QueueFull)
    ))?;
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
        dispatch_cancel.cancel();
    }
    check!(
        writer.as_mut().now_or_never().is_some(),
        "interrupted prefix must stop the writer before dispatching B"
    )?;
    drop(writer);
    verify_only_prefix(&messages)?;
    check_eq!(first_receipt.try_recv()?, Err(NetError::DeliveryUnknown))?;
    check_eq!(
        completion.wait().now_or_never(),
        Some(Err(NetError::DeliveryUnknown))
    )?;
    check!(pending.is_empty())?;
    check!(matches!(
        io_event_rx.try_recv(),
        Ok(IoEvent::WriteEnded {
            generation: GENERATION,
            error: NetError::DeliveryUnknown
        })
    ))?;
    check!(matches!(
        io_event_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ))?;
    check!(matches!(
        next_receipt.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ))?;

    // A's task AND byte permits are reusable before B finishes, with no A retry.
    let mut capacity_receipt =
        enqueue_untracked(&source, "capacity-after-interruption", FIRST_BODY)?;
    let next = next_queue
        .try_next()
        .ok_or_else(|| test_error("B was lost"))?;
    check_eq!(next.uuid, NEXT_ID)?;
    check_eq!(next.attempt, 0)?;
    check_eq!(
        &next.message,
        &Message::Binary(Bytes::from_static(NEXT_BODY))
    )?;
    next.complete(Err(NetError::Cancelled));
    check_eq!(next_receipt.try_recv()?, Err(NetError::Cancelled))?;
    let capacity = source
        .try_next()
        .ok_or_else(|| test_error("capacity probe missing"))?;
    check_eq!(capacity.uuid, "capacity-after-interruption")?;
    capacity.complete(Err(NetError::Cancelled));
    check_eq!(capacity_receipt.try_recv()?, Err(NetError::Cancelled))?;
    check!(source.try_next().is_none(), "cancel/deadline requeued A")?;
    check!(urgent.try_next().is_none())?;

    // A late old-token cleanup must not remove a replacement registration.
    let (replacement_token, replacement_completion) =
        pending.reserve(Arc::new(TrackedRequest), &config)?;
    check!(replacement_token != token)?;
    check!(pending
        .remove_if_token(FIRST_ID, token, NetError::DeliveryUnknown)
        .is_none())?;
    check_eq!(pending.len(), 1)?;
    check!(pending
        .remove_if_token(FIRST_ID, replacement_token, NetError::Cancelled)
        .is_some())?;
    check_eq!(
        replacement_completion.wait().now_or_never(),
        Some(Err(NetError::Cancelled))
    )?;
    check!(pending.is_empty())?;

    verify_capacity_limits(
        &source,
        if urgent_followup {
            FIRST_BODY
        } else {
            b"abcdefghijklm"
        },
    )?;
    verify_capacity_limits(&urgent, NEXT_BODY)?;
    Ok(())
}

fn verify_capacity_limits(queue: &PriorityWriteQueue, full_body: &'static [u8]) -> TestResult {
    // Exhaust only task slots, leaving spare byte capacity.
    let mut first = enqueue_untracked(queue, "task-capacity-1", b"x")?;
    let mut second = enqueue_untracked(queue, "task-capacity-2", b"x")?;
    check!(matches!(
        enqueue_untracked(queue, "extra-task", b"x"),
        Err(NetError::QueueFull)
    ))?;
    for request in queue.drain() {
        request.complete(Err(NetError::Cancelled));
    }
    check_eq!(first.try_recv()?, Err(NetError::Cancelled))?;
    check_eq!(second.try_recv()?, Err(NetError::Cancelled))?;

    // Exhaust only byte slots, leaving one free task slot.
    let mut bytes = enqueue_untracked(queue, "byte-capacity", full_body)?;
    check!(matches!(
        enqueue_untracked(queue, "extra-byte", b"x"),
        Err(NetError::QueueFull)
    ))?;
    for request in queue.drain() {
        request.complete(Err(NetError::Cancelled));
    }
    check_eq!(bytes.try_recv()?, Err(NetError::Cancelled))?;
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

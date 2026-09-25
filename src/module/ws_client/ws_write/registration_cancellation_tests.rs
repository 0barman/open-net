use super::*;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::{
    DeliveryEvidence, RequestReceipt, ResponseRouting, TaskEventOptions, TaskEvents,
    TerminationOutcome, WebSocketClientConfig,
};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

async fn dispatch(
    observe: bool,
) -> TestResult<(
    Arc<crate::module::ws_client::session_runtime::SessionRuntime>,
    QueuedRequest,
    RequestReceipt,
    Option<TaskEvents>,
)> {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let events = if observe {
        Some(runtime.tasks.subscribe(TaskEventOptions::default())?)
    } else {
        None
    };
    let receipt = fixture::requests(&runtime)
        .request(fixture::request("writer-race")?)
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    let queued = runtime
        .queue
        .try_next()
        .ok_or_else(|| test_error("queued request missing"))?;
    Ok((runtime, queued, receipt, events))
}
async fn drive<W: Sink<Message, Error = WsError> + Unpin>(
    sink: &mut W,
    request: QueuedRequest,
    runtime: &crate::module::ws_client::session_runtime::SessionRuntime,
) -> RequestAction {
    let (_sender, mut controls) = mpsc::channel(1);
    let period = Duration::from_secs(3600);
    let mut heartbeat = HeartbeatSchedule::from_interval(
        tokio::time::interval_at(Instant::now() + period, period),
        period,
    );
    handle_queued_request(
        sink,
        request,
        &runtime.queue,
        &runtime.pending,
        1,
        None,
        MAX_READY_CONTROLS_PER_BOUNDARY,
        &mut controls,
        Duration::from_secs(1),
        Duration::from_secs(1),
        &mut heartbeat,
        &HeartbeatState::new(1),
        &CancellationToken::new(),
    )
    .await
}
struct GateSink {
    ready: bool,
    flush: Arc<AtomicBool>,
    messages: Arc<AtomicUsize>,
}

impl Sink<Message> for GateSink {
    type Error = WsError;
    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        if self.ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(self: Pin<&mut Self>, _message: Message) -> Result<(), WsError> {
        self.messages.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        if self.flush.load(Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct SocketPolls {
    ready: AtomicUsize,
    sends: AtomicUsize,
    flushes: AtomicUsize,
    closes: AtomicUsize,
    reads: AtomicUsize,
}

impl SocketPolls {
    fn snapshot(&self) -> [usize; 5] {
        [
            self.ready.load(Ordering::Acquire),
            self.sends.load(Ordering::Acquire),
            self.flushes.load(Ordering::Acquire),
            self.closes.load(Ordering::Acquire),
            self.reads.load(Ordering::Acquire),
        ]
    }
}

struct SharedSocketProbe {
    polls: Arc<SocketPolls>,
    writable: Arc<AtomicBool>,
}

impl Sink<Message> for SharedSocketProbe {
    type Error = WsError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        self.polls.ready.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), WsError> {
        self.polls.sends.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        self.polls.flushes.fetch_add(1, Ordering::AcqRel);
        if self.writable.load(Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        self.polls.closes.fetch_add(1, Ordering::AcqRel);
        self.poll_flush(cx)
    }
}

impl futures::Stream for SharedSocketProbe {
    type Item = Result<Message, WsError>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.reads.fetch_add(1, Ordering::AcqRel);
        // Model the automatic Pong flush that a WebSocket read poll can perform.
        self.polls.flushes.fetch_add(1, Ordering::AcqRel);
        Poll::Pending
    }
}

#[tokio::test]
async fn cancellation_or_expiry_before_data_start_preserves_unsent_reason_and_unique_terminal(
) -> TestResult {
    for observe in [false, true] {
        for expire in [false, true] {
            let (runtime, queued, receipt, mut events) = dispatch(observe).await?;
            let handle = receipt.handle().clone();
            let messages = Arc::new(AtomicUsize::new(0));
            let mut sink = GateSink {
                ready: false,
                flush: Arc::new(AtomicBool::new(false)),
                messages: messages.clone(),
            };
            let mut sending = Box::pin(drive(&mut sink, queued, &runtime));
            check!(futures::poll!(sending.as_mut()).is_pending())?;
            let outcome = if expire {
                handle.expire()?
            } else {
                handle.cancel()?
            };
            check_eq!(outcome, TerminationOutcome::TerminatedBeforeWrite)?;
            let action = sending.await;
            check!(matches!(action, RequestAction::Continue))?;
            let kind = if expire {
                crate::error::ErrorKind::TimedOut
            } else {
                crate::error::ErrorKind::Cancelled
            };
            check_eq!(handle.written().await.err().map(|e| e.kind()), Some(kind))?;
            check_eq!(receipt.response().await.err().map(|e| e.kind()), Some(kind))?;
            check_eq!(handle.state()?.delivery, DeliveryEvidence::NotStarted)?;
            check_eq!(messages.load(Ordering::Acquire), 0)?;
            check!(runtime.pending.snapshot()?.is_empty())?;
            if let Some(events) = events.as_mut() {
                check_eq!(
                    events
                        .recv()
                        .await?
                        .ok_or_else(|| test_error("terminal missing"))?
                        .operation_id,
                    handle.id()
                )?;
                check!(events.try_recv().is_err())?;
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn cancellation_after_data_start_retires_socket_and_publishes_unknown_once() -> TestResult {
    for observe in [false, true] {
        let (runtime, queued, receipt, mut events) = dispatch(observe).await?;
        let handle = receipt.handle().clone();
        let retirement = CancellationToken::new();
        queued
            .dispatch_phase
            .bind_write_retirement(retirement.clone());
        let mut sink = GateSink {
            ready: true,
            flush: Arc::new(AtomicBool::new(false)),
            messages: Arc::new(AtomicUsize::new(0)),
        };
        let mut sending = Box::pin(drive(&mut sink, queued, &runtime));
        check!(futures::poll!(sending.as_mut()).is_pending())?;
        check_eq!(handle.cancel()?, TerminationOutcome::DeliveryUnknown)?;
        check!(retirement.is_cancelled())?;
        check!(
            matches!(sending.await, RequestAction::StopWithError(error) if error.kind() == crate::error::ErrorKind::DeliveryUnknown)
        )?;
        check_eq!(
            receipt.response().await.err().map(|e| e.kind()),
            Some(crate::error::ErrorKind::DeliveryUnknown)
        )?;
        check_eq!(handle.state()?.delivery, DeliveryEvidence::Unknown)?;
        check_eq!(handle.cancel()?, TerminationOutcome::AlreadyFinished)?;
        if let Some(events) = events.as_mut() {
            check_eq!(
                events
                    .recv()
                    .await?
                    .ok_or_else(|| test_error("terminal missing"))?
                    .delivery,
                DeliveryEvidence::Unknown
            )?;
            check!(events.try_recv().is_err())?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn response_claim_wins_cancel_and_delayed_write_completion() -> TestResult {
    let (runtime, queued, receipt, _) = dispatch(false).await?;
    let handle = receipt.handle().clone();
    let flush = Arc::new(AtomicBool::new(false));
    let mut sink = GateSink {
        ready: true,
        flush: flush.clone(),
        messages: Arc::new(AtomicUsize::new(0)),
    };
    let mut sending = Box::pin(drive(&mut sink, queued, &runtime));
    check!(futures::poll!(sending.as_mut()).is_pending())?;
    check!(runtime
        .pending
        .complete_response(handle.request_id(), &fixture::incoming(1, "reply"))?)?;
    check_eq!(handle.cancel()?, TerminationOutcome::AlreadyFinished)?;
    flush.store(true, Ordering::Release);
    check!(matches!(sending.await, RequestAction::Continue))?;
    check_eq!(
        handle.written().await?,
        crate::ws::WriteOutcome::ResponseConfirmed
    )?;
    check_eq!(receipt.response().await?.request_id(), handle.request_id())?;
    Ok(())
}
#[tokio::test]
async fn cancellation_after_write_preserves_written_evidence_and_only_ends_response_wait(
) -> TestResult {
    let (runtime, queued, receipt, _) = dispatch(false).await?;
    let handle = receipt.handle().clone();
    let mut sink = GateSink {
        ready: true,
        flush: Arc::new(AtomicBool::new(true)),
        messages: Arc::new(AtomicUsize::new(0)),
    };
    check!(matches!(
        drive(&mut sink, queued, &runtime).await,
        RequestAction::Continue
    ))?;
    check_eq!(handle.written().await?, crate::ws::WriteOutcome::Written)?;
    check_eq!(handle.cancel()?, TerminationOutcome::TerminatedAfterWrite)?;
    check_eq!(
        receipt.response().await.err().map(|e| e.kind()),
        Some(crate::error::ErrorKind::Cancelled)
    )?;
    check_eq!(handle.state()?.delivery, DeliveryEvidence::Written)?;
    Ok(())
}
#[tokio::test]
async fn registration_cancel_freezes_reader_and_controls_before_writer_resumes() -> TestResult {
    use crate::module::ws_client::network_io::{NetworkAwareSink, NetworkAwareStream};
    use futures::StreamExt;
    let (runtime, queued, receipt, _) = dispatch(false).await?;
    let handle = receipt.handle().clone();
    let retirement = CancellationToken::new();
    queued
        .dispatch_phase
        .bind_write_retirement(retirement.clone());
    let polls = Arc::new(SocketPolls::default());
    let writable = Arc::new(AtomicBool::new(false));
    let probe = || SharedSocketProbe {
        polls: polls.clone(),
        writable: writable.clone(),
    };
    let mut sink = NetworkAwareSink::new(probe(), None, 0).with_write_retirement(&retirement);
    let mut reader = NetworkAwareStream::new(probe(), None, 0).with_write_retirement(&retirement);
    let mut sending = Box::pin(drive(&mut sink, queued, &runtime));
    check!(futures::poll!(sending.as_mut()).is_pending())?;
    check!(reader.next().now_or_never().is_none())?;
    let before = polls.snapshot();
    check_eq!(before, [1, 1, 2, 0, 1])?;
    check_eq!(handle.cancel()?, TerminationOutcome::DeliveryUnknown)?;
    writable.store(true, Ordering::Release);
    check!(reader.next().now_or_never().is_none())?;
    check_eq!(polls.snapshot(), before)?;
    check!(matches!(sending.await, RequestAction::StopWithError(_)))?;
    let automatic = handle_control_message(
        &mut sink,
        ControlMessage::FlushAutomatic,
        Duration::from_secs(1),
        &CancellationToken::new(),
    )
    .await;
    check!(matches!(automatic, ControlAction::Failed { .. }))?;
    check_eq!(polls.snapshot(), before)?;
    check!(receipt.response().await.is_err())?;
    Ok(())
}

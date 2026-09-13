use super::*;
use crate::api::wsc::request_registration::{
    RegistrationControl, RequestRegistration, RequestTerminationOutcome,
};
use crate::api::wsc::{
    PendingRequestCompletion, WebSocketTaskEvent, WebSocketTaskEventOptions, WebSocketTaskSource,
};
use crate::module::ws_client::task_observer::{TaskObservation, TaskObserverStore};
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::module::ws_client::write::queued_request::DispatchPhase;
use crate::{WSRequestConfig, WSRequestTrait, WsBody};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::sync::oneshot;

struct Request;

impl WSRequestTrait for Request {
    fn uuid(&self) -> String {
        "registration-cancellation".to_owned()
    }
    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Text("body".to_owned()))
    }
}

struct RegisteredDispatch {
    request: QueuedRequest,
    written: oneshot::Receiver<Result<(), NetError>>,
    completion: PendingRequestCompletion,
    registration: RequestRegistration,
    pending: PendingRequestView,
    queue: Arc<PriorityWriteQueue>,
    events: Option<std::sync::mpsc::Receiver<WebSocketTaskEvent>>,
}

fn registered_dispatch(observe: bool) -> TestResult<RegisteredDispatch> {
    let request: Arc<dyn WSRequestTrait> = Arc::new(Request);
    let mut events = None;
    let observation = if observe {
        let store = TaskObserverStore::new(601);
        let (sender, receiver) = std::sync::mpsc::channel();
        store.register(
            Box::new(move |event| {
                if sender.send(event).is_err() {
                    eprintln!("cancellation event receiver closed");
                }
            }),
            WebSocketTaskEventOptions::new(1, 64),
        )?;
        let listener = store
            .snapshot()?
            .ok_or_else(|| test_error("missing observer"))?;
        events = Some(receiver);
        Some(TaskObservation::new(
            601,
            1,
            Some(request.uuid()),
            WebSocketTaskSource::Request(request.clone()),
            false,
            None,
            listener.try_reserve(4, false)?,
        ))
    } else {
        None
    };
    let phase = DispatchPhase::with_observation(observation.clone());
    let cancel = CancellationToken::new();
    let pending = PendingRequestView::with_capacity(1);
    let queue = PriorityWriteQueue::new(1, 4)?;
    let config = WSRequestConfig::default();
    let (registration, completion) = pending.reserve_snapshot_observed(
        request.uuid(),
        request,
        &config,
        observation,
        None,
        Some(RegistrationControl::new(
            phase.clone(),
            cancel.clone(),
            Arc::downgrade(&queue),
        )),
        None,
    )?;
    phase.set_pending_cleanup(
        pending.clone(),
        registration.request_id().to_owned(),
        registration.raw_token(),
    )?;
    let written = queue.try_enqueue(
        registration.request_id().to_owned(),
        Some(registration.raw_token()),
        Message::Text("body".into()),
        4,
        config,
        &CancellationToken::new(),
        cancel,
        phase,
        CancellationToken::new(),
    )?;
    let request = queue
        .try_next()
        .ok_or_else(|| test_error("queued request missing"))?;
    Ok(RegisteredDispatch {
        request,
        written,
        completion,
        registration,
        pending,
        queue,
        events,
    })
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

async fn drive(
    sink: &mut GateSink,
    request: QueuedRequest,
    queue: &Arc<PriorityWriteQueue>,
    pending: &PendingRequestView,
) -> RequestAction {
    let (_sender, mut controls) = mpsc::channel(1);
    let period = Duration::from_secs(3600);
    let mut heartbeat = tokio::time::interval_at(Instant::now() + period, period);
    handle_queued_request(
        sink,
        request,
        queue,
        pending,
        601,
        None,
        MAX_READY_CONTROLS_PER_BOUNDARY,
        &mut controls,
        Duration::from_secs(1),
        Duration::from_secs(1),
        &mut heartbeat,
        &HeartbeatState::new(601),
        period,
        Duration::from_secs(1),
        &CancellationToken::new(),
    )
    .await
}

#[tokio::test]
async fn registration_cancellation_before_data_start_preserves_unsent_reason_with_or_without_observer(
) -> TestResult {
    for observe in [false, true] {
        for expire in [false, true] {
            let RegisteredDispatch {
                request,
                written,
                completion,
                registration,
                pending,
                queue,
                events,
            } = registered_dispatch(observe)?;
            let messages = Arc::new(AtomicUsize::new(0));
            let mut sink = GateSink {
                ready: false,
                flush: Arc::new(AtomicBool::new(false)),
                messages: messages.clone(),
            };
            let mut sending = Box::pin(drive(&mut sink, request, &queue, &pending));
            check!(futures::poll!(&mut sending).is_pending())?;
            let outcome = if expire {
                registration.expire()?
            } else {
                registration.cancel()?
            };
            let action = sending.await;
            let actual_written = written.await?;
            let actual_pending = completion.wait().await;
            let expected = if expire {
                NetError::TimeoutError
            } else {
                NetError::Cancelled
            };
            check_eq!(
                (outcome, actual_written, actual_pending),
                (
                    RequestTerminationOutcome::Terminated { error: expected },
                    Err(expected),
                    Err(expected)
                ),
                "unsent outcomes differ: observer={observe}, expire={expire}"
            )?;
            check_eq!(action, RequestAction::Continue)?;
            check_eq!(messages.load(Ordering::Acquire), 0)?;
            check!(pending.is_empty())?;
            if let Some(events) = events {
                check_eq!(
                    events.recv_timeout(Duration::from_secs(1))?.result(),
                    Err(expected)
                )?;
                check!(events.try_recv().is_err())?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn registration_expire_after_dequeue_keeps_timeout_without_observer() -> TestResult {
    for observe in [false, true] {
        let RegisteredDispatch {
            request,
            written,
            completion,
            registration,
            pending,
            queue,
            events: _events,
        } = registered_dispatch(observe)?;
        let outcome = registration.expire()?;
        let mut sink = GateSink {
            ready: true,
            flush: Arc::new(AtomicBool::new(true)),
            messages: Arc::new(AtomicUsize::new(0)),
        };
        check_eq!(
            drive(&mut sink, request, &queue, &pending).await,
            RequestAction::Continue
        )?;
        check_eq!(
            outcome,
            RequestTerminationOutcome::Terminated {
                error: NetError::TimeoutError
            }
        )?;
        check_eq!(
            written.await?,
            Err(NetError::TimeoutError),
            "dequeued expiry was downgraded: observer={observe}"
        )?;
        check_eq!(completion.wait().await, Err(NetError::TimeoutError))?;
        check_eq!(sink.messages.load(Ordering::Acquire), 0)?;
    }
    Ok(())
}

#[tokio::test]
async fn registration_cancellation_after_data_start_is_unknown_with_or_without_observer(
) -> TestResult {
    for observe in [false, true] {
        for expire in [false, true] {
            let RegisteredDispatch {
                request,
                written,
                completion,
                registration,
                pending,
                queue,
                events,
            } = registered_dispatch(observe)?;
            let messages = Arc::new(AtomicUsize::new(0));
            let mut sink = GateSink {
                ready: true,
                flush: Arc::new(AtomicBool::new(false)),
                messages: messages.clone(),
            };
            let mut sending = Box::pin(drive(&mut sink, request, &queue, &pending));
            check!(futures::poll!(&mut sending).is_pending())?;
            check_eq!(messages.load(Ordering::Acquire), 1)?;
            let outcome = if expire {
                registration.expire()?
            } else {
                registration.cancel()?
            };
            check_eq!(
                sending.await,
                RequestAction::StopWithError(NetError::DeliveryUnknown)
            )?;
            check_eq!(
                outcome,
                RequestTerminationOutcome::Terminated {
                    error: NetError::DeliveryUnknown
                }
            )?;
            check_eq!(written.await?, Err(NetError::DeliveryUnknown))?;
            check_eq!(completion.wait().await, Err(NetError::DeliveryUnknown))?;
            if let Some(events) = events {
                check_eq!(
                    events.recv_timeout(Duration::from_secs(1))?.result(),
                    Err(NetError::DeliveryUnknown)
                )?;
                check!(events.try_recv().is_err())?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn registration_cancellation_after_response_claim_cannot_override_success() -> TestResult {
    for observe in [false, true] {
        let RegisteredDispatch {
            request,
            written,
            completion,
            registration,
            pending,
            queue,
            events,
        } = registered_dispatch(observe)?;
        let flush = Arc::new(AtomicBool::new(false));
        let mut sink = GateSink {
            ready: true,
            flush: flush.clone(),
            messages: Arc::new(AtomicUsize::new(0)),
        };
        let mut sending = Box::pin(drive(&mut sink, request, &queue, &pending));
        check!(futures::poll!(&mut sending).is_pending())?;
        check!(pending
            .take_request(registration.request_id(), 601)
            .is_some())?;
        check_eq!(
            registration.cancel()?,
            RequestTerminationOutcome::AlreadyClaimedOrFinished
        )?;
        check_eq!(
            registration.expire()?,
            RequestTerminationOutcome::AlreadyClaimedOrFinished
        )?;
        flush.store(true, Ordering::Release);
        check_eq!(sending.await, RequestAction::Continue)?;
        check_eq!(written.await?, Ok(()))?;
        check_eq!(completion.wait().await, Ok(()))?;
        if let Some(events) = events {
            check_eq!(
                events.recv_timeout(Duration::from_secs(1))?.result(),
                Ok(crate::WebSocketTaskSuccess::ResponseClaimed)
            )?;
            check!(events.try_recv().is_err())?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn registration_cancellation_after_write_only_finishes_response_wait() -> TestResult {
    for observe in [false, true] {
        let RegisteredDispatch {
            request,
            written,
            completion,
            registration,
            pending,
            queue,
            events,
        } = registered_dispatch(observe)?;
        let retirement = CancellationToken::new();
        request
            .dispatch_phase
            .bind_write_retirement(retirement.clone());
        let mut sink = GateSink {
            ready: true,
            flush: Arc::new(AtomicBool::new(true)),
            messages: Arc::new(AtomicUsize::new(0)),
        };
        check_eq!(
            drive(&mut sink, request, &queue, &pending).await,
            RequestAction::Continue
        )?;
        check!(
            !retirement.is_cancelled(),
            "successful completion retired a reusable connection"
        )?;
        check_eq!(written.await?, Ok(()))?;
        check_eq!(
            registration.cancel()?,
            RequestTerminationOutcome::Terminated {
                error: NetError::Cancelled
            }
        )?;
        check_eq!(completion.wait().await, Err(NetError::Cancelled))?;
        check_eq!(sink.messages.load(Ordering::Acquire), 1)?;
        if let Some(events) = events {
            let event = events.recv_timeout(Duration::from_secs(1))?;
            check_eq!(event.result(), Err(NetError::Cancelled))?;
            check_eq!(event.delivery(), crate::WebSocketTaskDelivery::Written)?;
        }
    }
    Ok(())
}

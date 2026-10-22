//! Timeout provenance must survive heartbeat work and a not-yet-ready data sink.
use super::*;
use crate::api::retry::{RetrySchedule, RetrySetting, RetryStopReason};
use crate::error::ErrorKind;
use crate::module::ws_client::{
    session_runtime::SessionRuntime,
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::{
    DeliveryEvidence, ResponseRouting, SendOptions, TaskEndCause, TaskEventOptions,
    WebSocketClientConfig, WsReplaySafety, WsRequestOperationOptions, WsRequestRetryOn,
    WsRequestRetryPolicy, WsSendRetryOn, WsSendRetryPolicy, WsTimeoutPhase,
};
use std::task::{Context, Poll};

#[derive(Clone, Copy)]
enum Stall {
    None,
    Ready,
    Flush,
    Ping,
}
struct BudgetSink {
    stall: Stall,
    messages: Vec<Message>,
    recovery_after_data: Option<Arc<HeartbeatState>>,
}
impl BudgetSink {
    fn new(stall: Stall) -> Self {
        Self {
            stall,
            messages: Vec::new(),
            recovery_after_data: None,
        }
    }
}
impl Sink<Message> for BudgetSink {
    type Error = WsError;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        if matches!(self.stall, Stall::Ready) {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), WsError> {
        if !matches!(message, Message::Ping(_)) {
            if let Some(state) = self.recovery_after_data.take() {
                let now = Instant::now();
                let HeartbeatTick::SendProbe(payload) = state.on_tick(now, Duration::ZERO) else {
                    return Err(WsError::Io(std::io::Error::other("probe already active")));
                };
                if !state.mark_sent(&payload, now) {
                    return Err(WsError::Io(std::io::Error::other(
                        "probe was not marked sent",
                    )));
                }
            }
        }
        self.messages.push(message);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        if matches!(self.stall, Stall::Flush)
            || (matches!(self.stall, Stall::Ping)
                && matches!(self.messages.last(), Some(Message::Ping(_))))
        {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn poll_close(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        self.poll_flush(context)
    }
}

async fn runtime() -> TestResult<Arc<SessionRuntime>> {
    let mut config = WebSocketClientConfig::default();
    config.queues.normal.max_items = 1;
    config.queues.normal.max_bytes = 128;
    config.requests.max_pending = 1;
    Ok(fixture::runtime(config, ResponseRouting::Manual, true).await?)
}

async fn drive(
    runtime: &Arc<SessionRuntime>,
    request: QueuedRequest,
    sink: &mut BudgetSink,
    heartbeat: &HeartbeatState,
    frame_timeout: Duration,
) -> TestResult<RequestAction> {
    let (_controls, mut control_rx) = mpsc::channel(1);
    let mut schedule = HeartbeatSchedule::new(None)?;
    let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig {
        pong_recovery_attempts: 1,
        ..Default::default()
    });
    Ok(handle_queued_request(
        sink,
        request,
        &runtime.queue,
        &runtime.pending,
        1,
        Some(1),
        MAX_READY_CONTROLS_PER_BOUNDARY,
        &mut control_rx,
        Duration::from_secs(1),
        frame_timeout,
        &mut schedule,
        heartbeat,
        &mut recovery,
        &CancellationToken::new(),
    )
    .await)
}

async fn check_resources_and_close(runtime: &Arc<SessionRuntime>) -> TestResult {
    // Shutdown drains pending requests and queues, so verify release before it can
    // repair a leak left by timeout, cancellation or retry completion.
    check!(runtime.pending.snapshot()?.is_empty())?;
    let slot = runtime.pending.try_reserve_slot()?;
    drop(slot);
    check!(runtime.queue.try_next().is_none())?;
    let permits = runtime.queue.try_reserve(128)?;
    drop(permits);
    runtime.end(
        NetError::from(ErrorKind::EngineDropped),
        TaskEndCause::Shutdown,
    );
    fixture::bounded(runtime.wait_requests()).await?;
    Ok(())
}

#[tokio::test]
async fn raw_heartbeat_write_timeout_retries_after_a_written_prefix() -> TestResult {
    let runtime = runtime().await?;
    let mut events = runtime.tasks.subscribe(TaskEventOptions::default())?;
    let policy = WsSendRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(2).build()?)
        .retry_on(WsSendRetryOn::none().with_write_timeout(true))
        .safety(WsReplaySafety::IdempotentByContract)
        .build()?;
    let receipt = fixture::sender(&runtime)
        .message("abc")
        .options(SendOptions {
            write_timeout: Duration::from_millis(30),
            retry_setting: RetrySetting::Policy(policy),
            ..Default::default()
        })
        .try_enqueue()
        .map_err(|error| error.into_error())?;
    let request = runtime
        .queue
        .try_next()
        .ok_or_else(|| test_error("raw write missing"))?;
    let heartbeat = Arc::new(HeartbeatState::new(1));
    let mut sink = BudgetSink::new(Stall::Ping);
    sink.recovery_after_data = Some(heartbeat.clone());
    check!(matches!(
        drive(
            &runtime,
            request,
            &mut sink,
            &heartbeat,
            Duration::from_secs(1)
        )
        .await?,
        RequestAction::StopWithError(_)
    ))?;
    check_eq!(sink.messages.len(), 2)?;
    check!(matches!(sink.messages.last(), Some(Message::Ping(_))))?;
    let retry = runtime
        .queue
        .try_next()
        .ok_or_else(|| test_error("Write timeout retry missing"))?;
    check_eq!(
        retry.dispatch_phase.snapshot()?.delivery,
        DeliveryEvidence::Unknown
    )?;
    let mut sink = BudgetSink::new(Stall::None);
    check_eq!(
        drive(
            &runtime,
            retry,
            &mut sink,
            &HeartbeatState::new(2),
            Duration::from_secs(1)
        )
        .await?,
        RequestAction::Continue
    )?;
    fixture::bounded(receipt.written()).await??;
    let report = receipt.retry_report();
    check_eq!(report.attempts, 2)?;
    check_eq!(report.transport_sends, 2)?;
    check_eq!(report.stop_reason, Some(RetryStopReason::Completed))?;
    let event = fixture::bounded(events.recv())
        .await??
        .ok_or_else(|| test_error("terminal event missing"))?;
    check_eq!(event.delivery, DeliveryEvidence::Written)?;
    check!(events.try_recv().is_err())?;
    check_resources_and_close(&runtime).await
}

#[tokio::test]
async fn frame_readiness_timeout_retries_raw_and_releases_queue_capacity() -> TestResult {
    let runtime = runtime().await?;
    let mut events = runtime.tasks.subscribe(TaskEventOptions::default())?;
    let policy = WsSendRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(2).build()?)
        .retry_on(WsSendRetryOn::none().with_write_timeout(true))
        .safety(WsReplaySafety::IdempotentByContract)
        .build()?;
    let receipt = fixture::sender(&runtime)
        .message("abc")
        .options(SendOptions {
            write_timeout: Duration::from_secs(1),
            retry_setting: RetrySetting::Policy(policy),
            ..Default::default()
        })
        .try_enqueue()
        .map_err(|error| error.into_error())?;
    let request = runtime
        .queue
        .try_next()
        .ok_or_else(|| test_error("raw write missing"))?;
    let mut sink = BudgetSink::new(Stall::Ready);
    drive(
        &runtime,
        request,
        &mut sink,
        &HeartbeatState::new(1),
        Duration::from_millis(5),
    )
    .await?;
    check!(sink.messages.is_empty())?;
    let retry = runtime
        .queue
        .try_next()
        .ok_or_else(|| test_error("frame timeout retry missing"))?;
    check_eq!(
        retry.dispatch_phase.snapshot()?.delivery,
        DeliveryEvidence::NotStarted
    )?;
    let mut sink = BudgetSink::new(Stall::None);
    drive(
        &runtime,
        retry,
        &mut sink,
        &HeartbeatState::new(2),
        Duration::from_secs(1),
    )
    .await?;
    fixture::bounded(receipt.written()).await??;
    check_eq!(receipt.retry_report().attempts, 2)?;
    check_eq!(receipt.retry_report().transport_sends, 1)?;
    check!(fixture::bounded(events.recv()).await??.is_some())?;
    check!(events.try_recv().is_err())?;
    check_resources_and_close(&runtime).await
}

#[tokio::test]
async fn frame_readiness_timeout_retries_logical_request_with_fresh_registration() -> TestResult {
    let runtime = runtime().await?;
    let mut events = runtime.tasks.subscribe(TaskEventOptions::default())?;
    let policy = WsRequestRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(2).build()?)
        .retry_on(WsRequestRetryOn::none().with_write_timeout(true))
        .safety(WsReplaySafety::IdempotencyKey)
        .build()?;
    let receipt = fixture::requests(&runtime)
        .request_with_factory("key", |context| {
            Ok(crate::ws::Message::text(context.request_id().as_str()))
        })?
        .options(WsRequestOperationOptions {
            write_timeout: Duration::from_secs(1),
            total_timeout: Some(Duration::from_secs(2)),
            retry_setting: RetrySetting::Policy(policy),
            ..Default::default()
        })
        .enqueue()
        .await
        .map_err(|error| error.into_error())?;
    let mut ids = Vec::new();
    for _ in 0..2 {
        let request = fixture::bounded(runtime.queue.next())
            .await?
            .ok_or_else(|| test_error("logical write missing"))?;
        ids.push(
            request
                .registration
                .as_ref()
                .ok_or_else(|| test_error("registration missing"))?
                .request_id()
                .clone(),
        );
        let mut sink = BudgetSink::new(Stall::Ready);
        drive(
            &runtime,
            request,
            &mut sink,
            &HeartbeatState::new(1),
            Duration::from_millis(5),
        )
        .await?;
        check!(sink.messages.is_empty())?;
    }
    check!(ids.first() != ids.get(1))?;
    let error = fixture::bounded(receipt.response())
        .await?
        .err()
        .ok_or_else(|| test_error("blocked writes unexpectedly succeeded"))?;
    check_eq!(error.kind(), ErrorKind::TimedOut)?;
    check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Write))?;
    let report = error
        .retry_report()
        .ok_or_else(|| test_error("retry report missing"))?;
    check_eq!(report.attempts, 2)?;
    check_eq!(report.transport_sends, 0)?;
    check_eq!(report.stop_reason, Some(RetryStopReason::AttemptsExhausted))?;
    let event = fixture::bounded(events.recv())
        .await??
        .ok_or_else(|| test_error("terminal event missing"))?;
    check_eq!(event.delivery, DeliveryEvidence::NotStarted)?;
    check!(events.try_recv().is_err())?;
    check_resources_and_close(&runtime).await
}

async fn heartbeat_boundary(
    phase: WsTimeoutPhase,
    timeout: Duration,
    control_timeout: Duration,
    cancel: &CancellationToken,
) -> TestResult<RequestWriteFailure> {
    let core = fixture::operation(&SendOptions::default(), false, 88)?;
    core.enqueue()?;
    check!(core.mark_writing(crate::ws::ConnectionId::from_allocated(1)))?;
    let mut sink = BudgetSink::new(Stall::Flush);
    let (_controls, mut controls) = mpsc::channel(1);
    let mut schedule = HeartbeatSchedule::from_interval(
        tokio::time::interval_at(Instant::now(), Duration::from_secs(3600)),
        Duration::from_secs(1),
    );
    let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig::default());
    // Let Tokio register the immediate tick before this helper polls it once.
    tokio::time::sleep(Duration::from_millis(1)).await;
    let budget = TimeoutBudget::new((Instant::now() + timeout).into_std(), phase);
    let failure = send_request_message_with_phase(
        &mut sink,
        Message::text("abc"),
        None,
        MAX_READY_CONTROLS_PER_BOUNDARY,
        budget,
        &mut controls,
        control_timeout,
        Duration::from_secs(1),
        &mut schedule,
        &HeartbeatState::new(1),
        &mut recovery,
        cancel,
        &core.cancel_token(),
        &core,
    )
    .await
    .err()
    .ok_or_else(|| test_error("blocked heartbeat succeeded"))?;
    check!(sink
        .messages
        .iter()
        .all(|message| matches!(message, Message::Ping(_))))?;
    Ok(failure)
}

#[tokio::test]
async fn heartbeat_write_and_attempt_budgets_keep_independent_retry_switches() -> TestResult {
    for phase in [WsTimeoutPhase::Write, WsTimeoutPhase::Attempt] {
        let failure = heartbeat_boundary(
            phase,
            Duration::from_millis(20),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await?;
        check_eq!(failure.request_error.ws_timeout_phase(), Some(phase))?;
        let writes = WsRequestRetryPolicy::builder()
            .retry_on(WsRequestRetryOn::none().with_write_timeout(true))
            .build()?;
        let attempts = WsRequestRetryPolicy::builder()
            .retry_on(WsRequestRetryOn::none().with_attempt_timeout(true))
            .build()?;
        let errors = WsRequestRetryPolicy::builder()
            .retry_on(WsRequestRetryOn::none().with_write_errors(true))
            .build()?;
        check_eq!(
            writes.should_retry_error(&failure.request_error),
            phase == WsTimeoutPhase::Write
        )?;
        check_eq!(
            attempts.should_retry_error(&failure.request_error),
            phase == WsTimeoutPhase::Attempt
        )?;
        check!(!errors.should_retry_error(&failure.request_error))?;
    }
    Ok(())
}

#[tokio::test]
async fn earlier_heartbeat_control_timeout_and_cancellation_keep_their_results() -> TestResult {
    let failure = heartbeat_boundary(
        WsTimeoutPhase::Attempt,
        Duration::from_millis(60),
        Duration::from_millis(5),
        &CancellationToken::new(),
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(70)).await;
    check_eq!(failure.request_error.ws_timeout_phase(), None)?;
    check_eq!(failure.request_error.kind(), ErrorKind::Closed)?;
    let cancel = CancellationToken::new();
    let cancel_write = async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        cancel.cancel();
    };
    let (failure, ()) = tokio::join!(
        heartbeat_boundary(
            WsTimeoutPhase::Attempt,
            Duration::from_millis(50),
            Duration::from_secs(1),
            &cancel
        ),
        cancel_write,
    );
    let failure = failure?;
    check_eq!(failure.request_error.kind(), ErrorKind::Cancelled)?;
    check_eq!(failure.request_error.ws_timeout_phase(), None)?;
    Ok(())
}

#[tokio::test]
async fn normal_retry_and_recovery_flush_keep_the_request_budget() -> TestResult {
    for phase in [WsTimeoutPhase::Write, WsTimeoutPhase::Attempt] {
        for normal_retry in [false, true] {
            let state = HeartbeatState::new(1);
            let mut recovery = HeartbeatRecovery::new(HeartbeatRetryConfig {
                pong_recovery_attempts: 1,
                ping_write_retries: 1,
                ..Default::default()
            });
            let now = Instant::now();
            if normal_retry {
                let HeartbeatTick::SendProbe(payload) = state.on_tick(now, Duration::from_secs(1))
                else {
                    return Err(test_error("normal retry probe missing"));
                };
                recovery.schedule_normal_retry(payload, now)?;
                recovery.normal_retry_deadline = Some(now);
            } else {
                recovery.start();
                recovery.recovery_flush_pending = true;
                recovery.next_deadline = Some(now);
            }
            let budget = TimeoutBudget::new((now + Duration::from_millis(10)).into_std(), phase);
            let mut sink = BudgetSink::new(Stall::Flush);
            let error = within_request_budget(
                budget,
                service_due_heartbeat_work(
                    &mut sink,
                    &state,
                    &mut recovery,
                    Duration::from_secs(1),
                    Some(budget),
                    &CancellationToken::new(),
                ),
            )
            .await
            .err()
            .ok_or_else(|| test_error("stalled heartbeat flush succeeded"))?;
            check_eq!(error.ws_timeout_phase(), Some(phase))?;
            check!(sink.messages.is_empty())?;
            check!(!state.has_outstanding_probe())?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn heartbeat_budget_selection_survives_polling_after_both_deadlines() -> TestResult {
    for request_first in [false, true] {
        let budget = TimeoutBudget::new(
            (Instant::now() + Duration::from_millis(20)).into_std(),
            WsTimeoutPhase::Attempt,
        );
        let timeout = if request_first {
            Duration::from_millis(40)
        } else {
            Duration::from_millis(5)
        };
        let mut sink = BudgetSink::new(Stall::Flush);
        let cancel = CancellationToken::new();
        let future = send_control_frame_outcome(
            &mut sink,
            Message::Ping(Bytes::new()),
            timeout,
            &cancel,
            Some(budget),
        );
        tokio::pin!(future);
        check!(future.as_mut().now_or_never().is_none())?;
        std::thread::sleep(Duration::from_millis(60));
        tokio::time::sleep(Duration::from_millis(1)).await;
        let ControlWriteOutcome::Failed(error) = future.await else {
            return Err(test_error("blocked control write did not fail"));
        };
        check_eq!(
            error.ws_timeout_phase(),
            request_first.then_some(WsTimeoutPhase::Attempt)
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn cancelling_a_pending_readiness_wait_does_not_retry_or_retain_capacity() -> TestResult {
    let runtime = runtime().await?;
    let mut events = runtime.tasks.subscribe(TaskEventOptions::default())?;
    let policy = WsSendRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(2).build()?)
        .retry_on(WsSendRetryOn::none().with_write_timeout(true))
        .safety(WsReplaySafety::IdempotentByContract)
        .build()?;
    let receipt = fixture::sender(&runtime)
        .message("abc")
        .options(SendOptions {
            retry_setting: RetrySetting::Policy(policy),
            ..Default::default()
        })
        .try_enqueue()
        .map_err(|error| error.into_error())?;
    let request = runtime
        .queue
        .try_next()
        .ok_or_else(|| test_error("raw write missing"))?;
    let mut sink = BudgetSink::new(Stall::Ready);
    let heartbeat = HeartbeatState::new(1);
    let cancellation = async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        receipt.cancel()
    };
    let (result, cancelled) = tokio::join!(
        drive(
            &runtime,
            request,
            &mut sink,
            &heartbeat,
            Duration::from_secs(1)
        ),
        cancellation,
    );
    result?;
    cancelled?;
    let error = fixture::bounded(receipt.written())
        .await?
        .err()
        .ok_or_else(|| test_error("cancelled write succeeded"))?;
    check_eq!(error.kind(), ErrorKind::Cancelled)?;
    check!(sink.messages.is_empty())?;
    check_eq!(receipt.retry_report().attempts, 1)?;
    check_eq!(receipt.retry_report().transport_sends, 0)?;
    check_eq!(
        receipt.retry_report().stop_reason,
        Some(RetryStopReason::Cancelled)
    )?;
    let event = fixture::bounded(events.recv())
        .await??
        .ok_or_else(|| test_error("cancellation event missing"))?;
    check_eq!(event.cause, TaskEndCause::Cancelled)?;
    check!(events.try_recv().is_err())?;
    check_resources_and_close(&runtime).await
}

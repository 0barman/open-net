use super::*;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::module::ws_client::v2_test_support;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CallbackGate {
    entered: oneshot::Receiver<()>,
    release: Option<std::sync::mpsc::SyncSender<()>>,
}

impl CallbackGate {
    async fn wait(&mut self) -> TestResult {
        v2_test_support::bounded(&mut self.entered).await??;
        Ok(())
    }

    fn release(&mut self) -> TestResult {
        if let Some(sender) = self.release.take() {
            sender
                .send(())
                .map_err(|_| "callback gate receiver closed")?;
        }
        Ok(())
    }
}

impl Drop for CallbackGate {
    fn drop(&mut self) {
        if let Some(sender) = self.release.take() {
            let _ = sender.try_send(());
        }
    }
}

fn callback_gate() -> (impl Fn() -> Result<()> + Send + Sync, CallbackGate) {
    let (entered_tx, entered) = oneshot::channel();
    let entered_tx = Mutex::new(Some(entered_tx));
    let (release, release_rx) = std::sync::mpsc::sync_channel(1);
    let release_rx = Mutex::new(release_rx);
    let callback = move || {
        if let Some(sender) = entered_tx.lock().map_err(NetError::from_poison)?.take() {
            let _ = sender.send(());
        }
        release_rx
            .lock()
            .map_err(NetError::from_poison)?
            .recv_timeout(Duration::from_secs(3))
            .map_err(|error| NetError::with_source(ErrorKind::Internal, error))?;
        Ok(())
    };
    (
        callback,
        CallbackGate {
            entered,
            release: Some(release),
        },
    )
}

async fn reconnect(runtime: &Arc<SessionRuntime>) -> Result<()> {
    runtime.connection_ended(
        ConnectionId::from_allocated(1),
        NetError::from(ErrorKind::NotConnected),
    )?;
    runtime.lifecycle.connection_terminated_with_details(
        TerminationReason::IoFailure,
        None,
        crate::module::ws_client::io_diagnostics::ConnectionTerminationDetails::default(),
        Some(2),
    )?;
    runtime.lifecycle.begin_cycle(2, true)?;
    let reservation = runtime.lifecycle.reserve_attempt().await?;
    runtime
        .lifecycle
        .begin_attempt(runtime.lifecycle.handshake_attempt(2, 0), reservation)?;
    runtime
        .lifecycle
        .set_attempt_credential_version(2, 0, None)?;
    runtime.lifecycle.prepare_established(2, 0)?;
    runtime.lifecycle.commit_established(2, 0)?;
    runtime.activate_connection(ConnectionId::from_allocated(2))
}

fn check_released(runtime: &SessionRuntime) -> TestResult {
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(
        runtime.pending.try_reserve_slot().is_ok(),
        "pending slot retained"
    )?;
    check!(
        runtime.queue.try_next().is_none(),
        "operation remained queued"
    )?;
    check!(
        runtime.queue.try_reserve(64).is_ok(),
        "queue item or byte capacity retained"
    )?;
    Ok(())
}

fn limited_config() -> WebSocketClientConfig {
    let mut config = WebSocketClientConfig::default();
    config.requests.max_pending = 1;
    config.queues.normal.max_items = 1;
    config.queues.normal.max_bytes = 64;
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_keeps_original_generation_across_factory_and_releases_resources() -> TestResult {
    let runtime = v2_test_support::runtime(limited_config(), ResponseRouting::Manual, true).await?;
    let mut events = runtime.tasks.subscribe(TaskEventOptions {
        max_tasks: 1,
        ..TaskEventOptions::default()
    })?;
    let (block, mut gate) = callback_gate();
    let receipt = v2_test_support::requests(&runtime)
        .request_with_factory("original-generation", move |_| {
            block()?;
            Ok(Message::text("body"))
        })?
        .enqueue()
        .await
        .map_err(EnqueueError::into_error)?;
    let operation_id = receipt.handle().id();
    let handle = receipt.handle().clone();
    gate.wait().await?;
    reconnect(&runtime).await?;
    gate.release()?;
    let result = v2_test_support::bounded(receipt.response()).await;
    if result.is_err() {
        handle.cancel()?;
    }
    let result = result?;
    let error = result.err().ok_or("Reject unexpectedly succeeded")?;
    check_eq!(error.kind(), ErrorKind::NotConnected)?;
    let report = error.retry_report().ok_or("missing retry report")?;
    check_eq!(report.attempts, 1)?;
    check_eq!(report.transport_sends, 0)?;
    let event = v2_test_support::bounded(events.recv())
        .await??
        .ok_or("task stream closed")?;
    check_eq!(event.operation_id, operation_id)?;
    check_eq!(event.cause, TaskEndCause::Disconnected)?;
    check!(
        matches!(events.try_recv(), Err(crate::error::TryReceiveError::Empty)),
        "duplicate terminal event or closed observation"
    )?;
    drop(
        runtime
            .tasks
            .snapshot()?
            .try_reserve(0, MessageLane::Normal)?,
    );
    check_released(&runtime)?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    v2_test_support::bounded(runtime.wait_requests()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_for_reconnect_may_queue_after_factory_generation_changes() -> TestResult {
    let runtime = v2_test_support::runtime(limited_config(), ResponseRouting::Manual, true).await?;
    let (block, mut gate) = callback_gate();
    let receipt = v2_test_support::requests(&runtime)
        .request_with_factory("wait-generation", move |_| {
            block()?;
            Ok(Message::text("body"))
        })?
        .options(WsRequestOperationOptions {
            disconnected: DisconnectedPolicy::WaitForReconnect,
            ..WsRequestOperationOptions::default()
        })
        .enqueue()
        .await
        .map_err(EnqueueError::into_error)?;
    gate.wait().await?;
    reconnect(&runtime).await?;
    gate.release()?;
    let queued = v2_test_support::bounded(async {
        loop {
            if let Some(queued) = runtime.queue.try_next() {
                break queued;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    check!(!queued
        .admission_cancel
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled))?;
    receipt.handle().cancel()?;
    drop(queued);
    let error = receipt
        .response()
        .await
        .err()
        .ok_or("cancel unexpectedly succeeded")?;
    check_eq!(error.kind(), ErrorKind::Cancelled)?;
    check_released(&runtime)?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    v2_test_support::bounded(runtime.wait_requests()).await?;
    Ok(())
}

#[tokio::test]
async fn reject_connection_loss_interrupts_retry_backoff() -> TestResult {
    let runtime = v2_test_support::runtime(limited_config(), ResponseRouting::Manual, true).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded = calls.clone();
    let (backoff_tx, backoff_rx) = oneshot::channel();
    let backoff_tx = Mutex::new(Some(backoff_tx));
    let policy = WsRequestRetryPolicy::builder()
        .schedule(
            crate::api::retry::RetrySchedule::builder()
                .max_attempts(2)
                .backoff(crate::api::retry::Backoff::constant(Duration::from_secs(
                    30,
                )))
                .build()?,
        )
        .retry_on(WsRequestRetryOn::none().with_write_errors(true))
        .safety(WsReplaySafety::IdempotentByContract)
        .observer(move |event: &RetryEvent| {
            if matches!(event, RetryEvent::RetryScheduled { .. }) {
                if let Some(sender) = lock_recover(&backoff_tx).take() {
                    let _ = sender.send(());
                }
            }
        })
        .build()?;
    let receipt = v2_test_support::requests(&runtime)
        .request_with_factory("backoff-generation", move |_| {
            recorded.fetch_add(1, Ordering::Relaxed);
            Ok(Message::text("body"))
        })?
        .retry_setting(RetrySetting::Policy(policy))
        .enqueue()
        .await
        .map_err(EnqueueError::into_error)?;
    let queued = v2_test_support::bounded(async {
        loop {
            if let Some(queued) = runtime.queue.try_next() {
                break queued;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    queued.complete(Err(
        NetError::from(ErrorKind::Io).with_stage(ErrorStage::Write)
    ));
    v2_test_support::bounded(backoff_rx).await??;
    reconnect(&runtime).await?;
    let error = v2_test_support::bounded(receipt.response())
        .await?
        .err()
        .ok_or("disconnected backoff unexpectedly succeeded")?;
    check_eq!(error.kind(), ErrorKind::NotConnected)?;
    check_eq!(calls.load(Ordering::Relaxed), 1)?;
    check_released(&runtime)?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    v2_test_support::bounded(runtime.wait_requests()).await?;
    Ok(())
}

#[tokio::test]
async fn reject_connection_loss_while_waiting_for_queue_releases_registration() -> TestResult {
    let runtime = v2_test_support::runtime(limited_config(), ResponseRouting::Manual, true).await?;
    let occupied = runtime.queue.try_reserve(1)?;
    let receipt = v2_test_support::requests(&runtime)
        .request_with_factory("queue-generation", |_| Ok(Message::text("body")))?
        .enqueue()
        .await
        .map_err(EnqueueError::into_error)?;
    let handle = receipt.handle().clone();
    v2_test_support::bounded(async {
        while handle.current_registration().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    reconnect(&runtime).await?;
    let error = v2_test_support::bounded(receipt.response())
        .await?
        .err()
        .ok_or("disconnected queue wait unexpectedly succeeded")?;
    check_eq!(error.kind(), ErrorKind::NotConnected)?;
    check_eq!(handle.retry_report().transport_sends, 0)?;
    drop(occupied);
    check_released(&runtime)?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    v2_test_support::bounded(runtime.wait_requests()).await?;
    Ok(())
}

#[tokio::test]
async fn dispatch_cannot_renew_a_logical_operations_original_generation() -> TestResult {
    let runtime = v2_test_support::runtime(limited_config(), ResponseRouting::Manual, true).await?;
    let connection_lease = runtime.lease(DisconnectedPolicy::Reject)?;
    reconnect(&runtime).await?;
    let registration_gate = Mutex::new(());
    let registered = AtomicUsize::new(0);
    let check = || Ok(());
    let install = |_: &RequestHandle| {
        registered.fetch_add(1, Ordering::Relaxed);
        Ok(())
    };
    let result = dispatch::prepare_operation_attempt(
        &runtime,
        &v2_test_support::request("original-lease")?,
        &RequestOptions::default(),
        Instant::now(),
        runtime.pending.try_reserve_slot()?,
        dispatch::AttemptRegistration {
            absolute_budget: None,
            connection_lease: &connection_lease,
            gate: &registration_gate,
            check: &check,
            install: &install,
        },
    )
    .await;
    let error = result
        .err()
        .ok_or("dispatch renewed an ended connection lease")?;
    check_eq!(error.kind(), ErrorKind::NotConnected)?;
    check_eq!(registered.load(Ordering::Relaxed), 0)?;
    check_released(&runtime)?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    v2_test_support::bounded(runtime.wait_requests()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_and_total_keep_priority_over_factory_generation_loss() -> TestResult {
    for cancel in [false, true] {
        let runtime =
            v2_test_support::runtime(limited_config(), ResponseRouting::Manual, true).await?;
        let group = CancellationGroup::new();
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .ok_or("clock range")?;
        let (block, mut gate) = callback_gate();
        let receipt = v2_test_support::requests(&runtime)
            .request_with_factory("factory-termination", move |_| {
                block()?;
                Ok(Message::text("body"))
            })?
            .options(WsRequestOperationOptions {
                cancellation: Some(group.clone()),
                deadline: Some(deadline),
                ..WsRequestOperationOptions::default()
            })
            .enqueue()
            .await
            .map_err(EnqueueError::into_error)?;
        gate.wait().await?;
        reconnect(&runtime).await?;
        if cancel {
            group.cancel();
        } else {
            tokio::time::sleep_until(deadline.into()).await;
        }
        gate.release()?;
        let error = v2_test_support::bounded(receipt.response())
            .await?
            .err()
            .ok_or("terminated factory unexpectedly succeeded")?;
        if cancel {
            check_eq!(error.kind(), ErrorKind::Cancelled)?;
            check_eq!(
                error
                    .retry_report()
                    .ok_or("missing cancel report")?
                    .stop_reason,
                Some(RetryStopReason::Cancelled)
            )?;
        } else {
            check_eq!(error.kind(), ErrorKind::TimedOut)?;
            check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Total))?;
        }
        check_released(&runtime)?;
        runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
        v2_test_support::bounded(runtime.wait_requests()).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_success_survives_connection_loss_before_logical_completion() -> TestResult {
    let runtime = v2_test_support::runtime(limited_config(), ResponseRouting::Manual, true).await?;
    let (block, mut gate) = callback_gate();
    let policy = WsRequestRetryPolicy::builder()
        .classifier(move |_| match block() {
            Ok(()) => RetryDecision::Default,
            Err(_) => RetryDecision::Stop,
        })
        .build()?;
    let receipt = v2_test_support::requests(&runtime)
        .request_with_factory("selected-generation", |_| Ok(Message::text("body")))?
        .retry_setting(RetrySetting::Policy(policy))
        .enqueue()
        .await
        .map_err(EnqueueError::into_error)?;
    let handle = receipt.handle().clone();
    let queued = v2_test_support::bounded(async {
        loop {
            if let Some(queued) = runtime.queue.try_next() {
                break queued;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let registration = handle
        .current_registration()
        .ok_or("registration missing")?;
    check!(queued
        .dispatch_phase
        .mark_writing(ConnectionId::from_allocated(1)))?;
    check!(runtime
        .pending
        .bind_connection(&registration, ConnectionId::from_allocated(1))?)?;
    check!(runtime.pending.complete_response(
        registration.request_id(),
        &v2_test_support::incoming(1, "ok")
    )?)?;
    queued.complete(Ok(()));
    gate.wait().await?;
    reconnect(&runtime).await?;
    gate.release()?;
    let response = v2_test_support::bounded(receipt.response()).await??;
    check_eq!(response.connection_id(), ConnectionId::from_allocated(1))?;
    check_eq!(
        handle.retry_report().stop_reason,
        Some(RetryStopReason::Completed)
    )?;
    check_eq!(handle.cancel()?, TerminationOutcome::AlreadyFinished)?;
    check_released(&runtime)?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    v2_test_support::bounded(runtime.wait_requests()).await?;
    Ok(())
}

#[derive(Clone, Copy)]
enum FinalDisconnectOverride {
    None,
    RetryScheduledDisconnect,
    Total,
    OperationCancel,
    GroupCancel,
    ShutdownBeforeDisconnect,
    ShutdownAfterDisconnect,
}

async fn selected_result_at_final_disconnect(
    write_timeout: bool,
    boundary: FinalDisconnectOverride,
) -> TestResult {
    let runtime = v2_test_support::runtime(limited_config(), ResponseRouting::Manual, true).await?;
    let factories = Arc::new(AtomicUsize::new(0));
    let observed_factories = factories.clone();
    let completed = Arc::new(AtomicUsize::new(0));
    let observed_completed = completed.clone();
    let observer_disconnects = Arc::new(AtomicUsize::new(0));
    let observed_disconnects = observer_disconnects.clone();
    let observed_runtime = Arc::downgrade(&runtime);
    let group = CancellationGroup::new();
    let deadline = if matches!(boundary, FinalDisconnectOverride::Total) {
        Some(
            Instant::now()
                .checked_add(Duration::from_secs(1))
                .ok_or("clock range")?,
        )
    } else {
        None
    };
    let (block, mut gate) = callback_gate();
    let policy = WsRequestRetryPolicy::builder()
        .schedule(
            crate::api::retry::RetrySchedule::builder()
                .max_attempts(2)
                .build()?,
        )
        .retry_on(WsRequestRetryOn::none().with_write_timeout(true))
        .safety(WsReplaySafety::IdempotentByContract)
        .classifier(move |_| match block() {
            Ok(()) => RetryDecision::Default,
            Err(_) => RetryDecision::Stop,
        })
        .observer(move |event: &RetryEvent| {
            if matches!(event, RetryEvent::Completed(_)) {
                observed_completed.fetch_add(1, Ordering::SeqCst);
            }
            if matches!(boundary, FinalDisconnectOverride::RetryScheduledDisconnect)
                && matches!(event, RetryEvent::RetryScheduled { .. })
            {
                if let Some(runtime) = observed_runtime.upgrade() {
                    if runtime
                        .connection_ended(
                            ConnectionId::from_allocated(1),
                            NetError::from(ErrorKind::NotConnected),
                        )
                        .is_ok()
                    {
                        observed_disconnects.fetch_add(1, Ordering::SeqCst);
                    }
                    runtime.end(
                        NetError::from(ErrorKind::NotConnected),
                        TaskEndCause::Disconnected,
                    );
                }
            }
        })
        .build()?;
    let receipt = v2_test_support::requests(&runtime)
        .request_with_factory("selected-final-disconnect", move |_| {
            observed_factories.fetch_add(1, Ordering::SeqCst);
            Ok(Message::text("body"))
        })?
        .options(WsRequestOperationOptions {
            deadline,
            cancellation: Some(group.clone()),
            ..WsRequestOperationOptions::default()
        })
        .retry_setting(RetrySetting::Policy(policy))
        .enqueue()
        .await
        .map_err(EnqueueError::into_error)?;
    let handle = receipt.handle().clone();
    let queued = v2_test_support::bounded(async {
        loop {
            if let Some(queued) = runtime.queue.try_next() {
                break queued;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let registration = handle
        .current_registration()
        .ok_or("registration missing")?;
    check!(queued
        .dispatch_phase
        .mark_writing(ConnectionId::from_allocated(1)))?;
    check!(runtime
        .pending
        .bind_connection(&registration, ConnectionId::from_allocated(1))?)?;
    if write_timeout {
        check!(queued.dispatch_phase.start_data_write(false)?)?;
        queued.complete(Err(NetError::from(ErrorKind::TimedOut)
            .with_stage(ErrorStage::Write)
            .with_ws_timeout_phase(WsTimeoutPhase::Write)));
    } else {
        check!(runtime.pending.complete_response(
            registration.request_id(),
            &v2_test_support::incoming(1, "selected"),
        )?)?;
        queued.complete(Ok(()));
    }
    gate.wait().await?;
    // A non-reconnecting worker retires the runtime after physical loss. Keep
    // the runner stopped after consuming its result so this ordering is exact.
    if matches!(boundary, FinalDisconnectOverride::ShutdownBeforeDisconnect) {
        runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    }
    if !matches!(boundary, FinalDisconnectOverride::RetryScheduledDisconnect) {
        runtime.connection_ended(
            ConnectionId::from_allocated(1),
            NetError::from(ErrorKind::NotConnected),
        )?;
        runtime.end(
            NetError::from(ErrorKind::NotConnected),
            TaskEndCause::Disconnected,
        );
    }
    match boundary {
        FinalDisconnectOverride::Total => {
            tokio::time::sleep_until(deadline.ok_or("total deadline missing")?.into()).await;
        }
        FinalDisconnectOverride::OperationCancel => {
            handle.cancel()?;
        }
        FinalDisconnectOverride::GroupCancel => {
            group.cancel();
        }
        FinalDisconnectOverride::ShutdownAfterDisconnect => {
            runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
        }
        FinalDisconnectOverride::None
        | FinalDisconnectOverride::RetryScheduledDisconnect
        | FinalDisconnectOverride::ShutdownBeforeDisconnect => {}
    }
    gate.release()?;
    let result = v2_test_support::bounded(receipt.response()).await?;
    let report = handle.retry_report();
    match boundary {
        FinalDisconnectOverride::None | FinalDisconnectOverride::RetryScheduledDisconnect
            if write_timeout =>
        {
            let error = result
                .err()
                .ok_or("selected Write failure became success")?;
            check_eq!(error.kind(), ErrorKind::DeliveryUnknown)?;
            check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Write))?;
            check_eq!(error.retry_report(), Some(&report))?;
            check!(report.stop_reason != Some(RetryStopReason::Shutdown))?;
        }
        FinalDisconnectOverride::None => {
            let response = result?;
            check_eq!(response.connection_id(), ConnectionId::from_allocated(1))?;
            check_eq!(response.retry_report(), Some(&report))?;
            check_eq!(report.stop_reason, Some(RetryStopReason::Completed))?;
        }
        _ => {
            let error = result
                .err()
                .ok_or("outer boundary did not override selected result")?;
            check_eq!(error.retry_report(), Some(&report))?;
            match boundary {
                FinalDisconnectOverride::Total => {
                    check_eq!(error.kind(), ErrorKind::TimedOut)?;
                    check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Total))?;
                    check_eq!(report.stop_reason, Some(RetryStopReason::TimedOut))?;
                }
                FinalDisconnectOverride::OperationCancel | FinalDisconnectOverride::GroupCancel => {
                    check_eq!(error.kind(), ErrorKind::Cancelled)?;
                    check_eq!(report.stop_reason, Some(RetryStopReason::Cancelled))?;
                }
                _ => {
                    check_eq!(error.kind(), ErrorKind::Closed)?;
                    check_eq!(report.stop_reason, Some(RetryStopReason::Shutdown))?;
                }
            }
        }
    }
    check_eq!(report.attempts, 1)?;
    check_eq!(factories.load(Ordering::SeqCst), 1)?;
    check_eq!(completed.load(Ordering::SeqCst), 1)?;
    if matches!(boundary, FinalDisconnectOverride::RetryScheduledDisconnect) {
        check_eq!(observer_disconnects.load(Ordering::SeqCst), 1)?;
    }
    check_eq!(handle.cancel()?, TerminationOutcome::AlreadyFinished)?;
    v2_test_support::bounded(runtime.wait_requests()).await?;
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(runtime.queue.try_next().is_none())?;
    drop(runtime.queue.try_reserve(64)?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_success_and_write_survive_final_natural_disconnect() -> TestResult {
    for write_timeout in [false, true] {
        selected_result_at_final_disconnect(write_timeout, FinalDisconnectOverride::None).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_disconnect_from_retry_observer_preserves_selected_write() -> TestResult {
    selected_result_at_final_disconnect(true, FinalDisconnectOverride::RetryScheduledDisconnect)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn total_and_cancellation_override_selected_results_after_final_disconnect() -> TestResult {
    for boundary in [
        FinalDisconnectOverride::Total,
        FinalDisconnectOverride::OperationCancel,
        FinalDisconnectOverride::GroupCancel,
    ] {
        for write_timeout in [false, true] {
            selected_result_at_final_disconnect(write_timeout, boundary).await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_shutdown_overrides_selected_results_in_both_disconnect_orders() -> TestResult {
    for boundary in [
        FinalDisconnectOverride::ShutdownBeforeDisconnect,
        FinalDisconnectOverride::ShutdownAfterDisconnect,
    ] {
        for write_timeout in [false, true] {
            selected_result_at_final_disconnect(write_timeout, boundary).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn final_disconnect_without_a_selected_result_finishes_without_retry() -> TestResult {
    let runtime = v2_test_support::runtime(limited_config(), ResponseRouting::Manual, true).await?;
    let factories = Arc::new(AtomicUsize::new(0));
    let observed_factories = factories.clone();
    let policy = WsRequestRetryPolicy::builder()
        .schedule(
            crate::api::retry::RetrySchedule::builder()
                .max_attempts(2)
                .build()?,
        )
        .safety(WsReplaySafety::IdempotentByContract)
        .classifier(|_| RetryDecision::Retry)
        .build()?;
    let receipt = v2_test_support::requests(&runtime)
        .request_with_factory("unselected-final-disconnect", move |_| {
            observed_factories.fetch_add(1, Ordering::SeqCst);
            Ok(Message::text("body"))
        })?
        .retry_setting(RetrySetting::Policy(policy))
        .enqueue()
        .await
        .map_err(EnqueueError::into_error)?;
    let handle = receipt.handle().clone();
    let queued = v2_test_support::bounded(async {
        loop {
            if let Some(queued) = runtime.queue.try_next() {
                break queued;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let registration = handle
        .current_registration()
        .ok_or("registration missing")?;
    check!(queued
        .dispatch_phase
        .mark_writing(ConnectionId::from_allocated(1)))?;
    check!(runtime
        .pending
        .bind_connection(&registration, ConnectionId::from_allocated(1))?)?;
    runtime.connection_ended(
        ConnectionId::from_allocated(1),
        NetError::from(ErrorKind::NotConnected),
    )?;
    runtime.end(
        NetError::from(ErrorKind::NotConnected),
        TaskEndCause::Disconnected,
    );
    drop(queued);
    let error = v2_test_support::bounded(receipt.response())
        .await?
        .err()
        .ok_or("unselected disconnected request became success")?;
    check!(matches!(
        error.kind(),
        ErrorKind::Closed | ErrorKind::NotConnected
    ))?;
    check_eq!(handle.retry_report().attempts, 1)?;
    check_eq!(factories.load(Ordering::SeqCst), 1)?;
    v2_test_support::bounded(runtime.wait_requests()).await?;
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(runtime.queue.try_next().is_none())?;
    drop(runtime.queue.try_reserve(64)?);
    Ok(())
}

async fn retryable_response_at_final_disconnect(after_scheduling: bool) -> TestResult {
    let runtime = v2_test_support::runtime(limited_config(), ResponseRouting::Manual, true).await?;
    let factories = Arc::new(AtomicUsize::new(0));
    let observed_factories = factories.clone();
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed_events = events.clone();
    let gate_error = Arc::new(Mutex::new(None));
    let classifier_error = gate_error.clone();
    let observer_error = gate_error.clone();
    let (block, mut gate) = callback_gate();
    let block = Arc::new(block);
    let classifier_block = block.clone();
    let policy = WsRequestRetryPolicy::builder()
        .schedule(
            crate::api::retry::RetrySchedule::builder()
                .max_attempts(2)
                .build()?,
        )
        .safety(WsReplaySafety::IdempotentByContract)
        .classifier(move |_| {
            if !after_scheduling {
                if let Err(error) = classifier_block() {
                    *lock_recover(&classifier_error) = Some(error);
                }
            }
            RetryDecision::Retry
        })
        .observer(move |event: &RetryEvent| {
            lock_recover(&observed_events).push(event.clone());
            if after_scheduling && matches!(event, RetryEvent::RetryScheduled { .. }) {
                if let Err(error) = block() {
                    *lock_recover(&observer_error) = Some(error);
                }
            }
        })
        .build()?;
    let receipt = v2_test_support::requests(&runtime)
        .request_with_factory("retryable-response-final-disconnect", move |_| {
            observed_factories.fetch_add(1, Ordering::SeqCst);
            Ok(Message::text("body"))
        })?
        .retry_setting(RetrySetting::Policy(policy))
        .enqueue()
        .await
        .map_err(EnqueueError::into_error)?;
    let handle = receipt.handle().clone();
    let queued = v2_test_support::bounded(async {
        loop {
            if let Some(queued) = runtime.queue.try_next() {
                break queued;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let registration = handle
        .current_registration()
        .ok_or("registration missing")?;
    check!(queued
        .dispatch_phase
        .mark_writing(ConnectionId::from_allocated(1)))?;
    check!(runtime
        .pending
        .bind_connection(&registration, ConnectionId::from_allocated(1))?)?;
    check!(queued.dispatch_phase.start_data_write(false)?)?;
    check!(runtime.pending.complete_response(
        registration.request_id(),
        &v2_test_support::incoming(1, "transient business failure"),
    )?)?;
    queued.complete(Ok(()));
    // Pause either the classifier or RetryScheduled callback after consuming the
    // same response, so only the point of natural retirement differs.
    gate.wait().await?;
    runtime.connection_ended(
        ConnectionId::from_allocated(1),
        NetError::from(ErrorKind::NotConnected),
    )?;
    runtime.end(
        NetError::from(ErrorKind::NotConnected),
        TaskEndCause::Disconnected,
    );
    gate.release()?;
    let response = v2_test_support::bounded(receipt.response()).await??;
    v2_test_support::bounded(runtime.wait_requests()).await?;
    if let Some(error) = lock_recover(&gate_error).take() {
        return Err(error.into());
    }
    let report = handle.retry_report();
    check_eq!(
        response.message().as_text(),
        Some("transient business failure")
    )?;
    check_eq!(response.connection_id(), ConnectionId::from_allocated(1))?;
    check_eq!(response.retry_report(), Some(&report))?;
    check_eq!(report.stop_reason, Some(RetryStopReason::NotRetryable))?;
    check_eq!(report.attempts, 1)?;
    check_eq!(report.retry_count, 0)?;
    check_eq!(report.transport_sends, 1)?;
    check_eq!(factories.load(Ordering::SeqCst), 1)?;
    let events = lock_recover(&events);
    check_eq!(
        events
            .iter()
            .filter(|event| matches!(event, RetryEvent::AttemptStarted { .. }))
            .count(),
        1
    )?;
    check_eq!(
        events
            .iter()
            .filter(|event| matches!(event, RetryEvent::RetryScheduled { .. }))
            .count(),
        usize::from(after_scheduling)
    )?;
    check_eq!(
        events
            .iter()
            .filter(|event| matches!(event, RetryEvent::Completed(_)))
            .count(),
        1
    )?;
    check_eq!(handle.cancel()?, TerminationOutcome::AlreadyFinished)?;
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(runtime.queue.try_next().is_none())?;
    drop(runtime.queue.try_reserve(64)?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_disconnect_during_classifier_retains_retryable_response_as_not_retryable(
) -> TestResult {
    retryable_response_at_final_disconnect(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_disconnect_after_retry_scheduled_retains_retryable_response_as_not_retryable(
) -> TestResult {
    retryable_response_at_final_disconnect(true).await
}

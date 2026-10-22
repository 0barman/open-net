use super::*;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::module::ws_client::v2_test_support;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn logical_options_and_factory_debug_hide_application_data() -> TestResult {
    let runtime = v2_test_support::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Disabled,
        true,
    )
    .await?;
    let options = WsRequestOperationOptions {
        metadata: Metadata::from([("private-field".into(), "private-value".into())]),
        ..WsRequestOperationOptions::default()
    };
    let builder = v2_test_support::requests(&runtime)
        .request_with_factory("private-key", |_| Ok(Message::text("private-body")))?
        .options(options.clone());
    let context = RequestAttemptContext {
        operation_id: runtime.allocate_operation()?,
        attempt: 1,
        request_id: RequestId::new("private-wire-id")?,
        idempotency_key: Arc::from("private-key"),
    };
    let source = TaskSource::RequestOperation(Arc::new(options.metadata.clone()));
    for rendered in [
        format!("{options:?}"),
        format!("{builder:?}"),
        format!("{context:?}"),
        format!("{source:?}"),
    ] {
        for secret in [
            "private-field",
            "private-value",
            "private-key",
            "private-body",
            "private-wire-id",
        ] {
            check!(
                !rendered.contains(secret),
                "private content in debug: {secret}"
            )?;
        }
    }
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    Ok(())
}

#[tokio::test]
async fn zero_total_timeout_never_calls_factory_and_reports_zero_attempts() -> TestResult {
    let runtime = v2_test_support::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Disabled,
        true,
    )
    .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded = calls.clone();
    let result = v2_test_support::requests(&runtime)
        .request_with_factory("key", move |_| {
            recorded.fetch_add(1, Ordering::Relaxed);
            Ok(Message::text("body"))
        })?
        .options(WsRequestOperationOptions {
            total_timeout: Some(Duration::ZERO),
            ..WsRequestOperationOptions::default()
        })
        .send()
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => return Err("zero total unexpectedly succeeded".into()),
    };
    check_eq!(error.kind(), ErrorKind::TimedOut)?;
    let report = error.retry_report().ok_or("missing admission report")?;
    check_eq!(report.attempts, 0)?;
    check_eq!(report.transport_sends, 0)?;
    check_eq!(report.stop_reason, Some(RetryStopReason::TimedOut))?;
    check_eq!(calls.load(Ordering::Relaxed), 0)?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    Ok(())
}

#[tokio::test]
async fn fixed_request_cannot_inherit_raw_default_or_enable_hidden_replay() -> TestResult {
    let policy = WsSendRetryPolicy::builder()
        .schedule(
            crate::api::retry::RetrySchedule::builder()
                .max_attempts(3)
                .build()?,
        )
        .safety(WsReplaySafety::IdempotentByContract)
        .build()?;
    let config = WebSocketClientConfig::default().with_default_send_retry_policy(policy.clone());
    let runtime = v2_test_support::runtime(config, ResponseRouting::Manual, true).await?;
    let builder = v2_test_support::requests(&runtime)
        .request(Request::new(RequestId::new("one")?, Message::text("one")));
    let prepared = builder.try_prepare().map_err(EnqueueError::into_error)?;
    check_eq!(prepared.handle().retry_report().attempts, 0)?;
    drop(prepared);
    let options = RequestOptions {
        send: SendOptions {
            retry_setting: RetrySetting::Policy(policy),
            ..SendOptions::default()
        },
        ..RequestOptions::default()
    };
    let result = v2_test_support::requests(&runtime)
        .request(Request::new(RequestId::new("two")?, Message::text("two")))
        .options(options)
        .try_enqueue();
    let error = match result {
        Err(error) => error.into_error(),
        Ok(_) => return Err("fixed request accepted multi-attempt replay".into()),
    };
    check_eq!(error.kind(), ErrorKind::InvalidConfig)?;
    check!(runtime.pending.snapshot()?.is_empty())?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    Ok(())
}

#[tokio::test]
async fn retiring_attempt_freezes_transport_count_before_releasing_registration() -> TestResult {
    // Exercise both deterministic orderings around the first data-write gate.
    // A retired current registration must never allow a later first write.
    for write_wins in [false, true] {
        let runtime = v2_test_support::runtime(
            WebSocketClientConfig::default(),
            ResponseRouting::Manual,
            true,
        )
        .await?;
        let core = v2_test_support::operation(&SendOptions::default(), true, 1)?;
        let lease = runtime.pending.try_reserve_slot()?;
        let (handle, _attempt_response) = runtime.pending.register(
            Arc::new(v2_test_support::request("race")?),
            &RequestOptions::default(),
            core.clone(),
            lease.clone(),
        )?;
        core.enqueue()?;
        check!(core.mark_writing(ConnectionId::from_allocated(1)))?;
        let mut retry = RetryState::new(crate::api::retry::RetrySchedule::no_retry());
        retry.begin_attempt()?;
        let (sender, _response) = oneshot::channel();
        let control = LogicalControl {
            id: runtime.allocate_operation()?,
            runtime: runtime.clone(),
            started: Instant::now(),
            deadline: None,
            cancellation: CancellationToken::new(),
            connection_lease: runtime.lease(DisconnectedPolicy::Reject)?,
            policy: WsRequestRetryPolicy::no_retry(),
            options: WsRequestOperationOptions::default(),
            source: TaskSource::RequestOperation(Arc::new(Metadata::new())),
            registration_gate: Mutex::new(()),
            state: Mutex::new(LogicalState {
                retry,
                current: Some(handle),
                terminal: false,
                elapsed: None,
                delivery: DeliveryEvidence::NotStarted,
                phase: OperationPhase::Queued,
                connection: None,
                response: Some(sender),
                reservation: Some(TaskEventReservation::unobserved(0, MessageLane::Normal)),
                lease: Some(lease),
                hook: None,
            }),
        };
        if write_wins {
            check!(core.start_data_write(false)?)?;
        }
        let retired = control.clear_current().ok_or("missing current attempt")?;
        check!(
            !core.start_data_write(false)?,
            "retired attempt started a late transport send"
        )?;
        check_eq!(control.report().transport_sends, u64::from(write_wins))?;
        check_eq!(
            retired.retry_report().transport_sends,
            u64::from(write_wins)
        )?;
        control.finish(
            Err(NetError::from(ErrorKind::Cancelled)),
            RetryStopReason::Cancelled,
        );
        runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    }
    Ok(())
}

#[tokio::test]
async fn reject_admission_stops_on_original_connection_loss_while_pending_is_full() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.requests.max_pending = 1;
    let runtime = v2_test_support::runtime(config, ResponseRouting::Manual, true).await?;
    let held_slot = runtime.pending.try_reserve_slot()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded = calls.clone();
    let builder =
        v2_test_support::requests(&runtime).request_with_factory("blocked", move |_| {
            recorded.fetch_add(1, Ordering::Relaxed);
            Ok(Message::text("never-sent"))
        })?;
    let mut pending = Box::pin(builder.enqueue());
    check!(futures_util::poll!(pending.as_mut()).is_pending())?;
    runtime.connection_ended(
        ConnectionId::from_allocated(1),
        NetError::from(ErrorKind::NotConnected),
    )?;
    let result = tokio::time::timeout(Duration::from_secs(1), pending).await?;
    let error = match result {
        Err(error) => error.into_error(),
        Ok(_) => return Err("Reject crossed its connection generation".into()),
    };
    check_eq!(error.kind(), ErrorKind::NotConnected)?;
    check_eq!(calls.load(Ordering::Relaxed), 0)?;
    check!(runtime.queue.try_next().is_none())?;
    drop(held_slot);
    check!(runtime.pending.try_reserve_slot().is_ok())?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    Ok(())
}

#[tokio::test]
async fn cancellation_releases_registered_pending_slot_before_completed_observer() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.requests.max_pending = 1;
    config.queues.normal.max_items = 1;
    let runtime = v2_test_support::runtime(config, ResponseRouting::Manual, true).await?;
    let occupied_queue = runtime.queue.try_reserve(1)?;
    let observed = Arc::new(Mutex::new(None));
    let recorded = observed.clone();
    let inspected_runtime = runtime.clone();
    let policy = WsRequestRetryPolicy::builder()
        .observer(move |event: &RetryEvent| {
            if matches!(event, RetryEvent::Completed(_)) {
                let empty = inspected_runtime
                    .pending
                    .snapshot()
                    .is_ok_and(|entries| entries.is_empty());
                let capacity_released = inspected_runtime.pending.try_reserve_slot().is_ok();
                *lock_recover(&recorded) = Some((empty, capacity_released));
            }
        })
        .build()?;
    let receipt = v2_test_support::requests(&runtime)
        .request_with_factory("queue-blocked", |_| Ok(Message::text("body")))?
        .retry_setting(RetrySetting::Policy(policy))
        .enqueue()
        .await
        .map_err(EnqueueError::into_error)?;
    let handle = receipt.handle().clone();
    tokio::time::timeout(Duration::from_secs(1), async {
        while handle.current_registration().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    handle.cancel()?;
    check_eq!(*lock_recover(&observed), Some((true, true)))?;
    let result = receipt.response().await;
    check!(result.is_err())?;
    drop(occupied_queue);
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    runtime.wait_requests().await;
    Ok(())
}

#[test]
fn write_timeout_reason_survives_unknown_delivery_wrapper() -> TestResult {
    let timeout = NetError::from(ErrorKind::TimedOut).with_stage(ErrorStage::Write);
    let uncertain =
        NetError::with_source(ErrorKind::DeliveryUnknown, timeout).with_stage(ErrorStage::Write);
    let policy = WsRequestRetryPolicy::builder()
        .retry_on(WsRequestRetryOn::none().with_write_timeout(true))
        .build()?;
    check!(policy.should_retry_error(&uncertain))?;
    check_eq!(uncertain.kind(), ErrorKind::DeliveryUnknown)?;
    let unrelated = NetError::with_source(
        ErrorKind::DeliveryUnknown,
        NetError::from(ErrorKind::TimedOut).with_stage(ErrorStage::Admission),
    )
    .with_stage(ErrorStage::Write);
    check!(!policy.should_retry_error(&unrelated))?;
    Ok(())
}

#[tokio::test]
async fn zero_attempt_budget_never_invokes_factory() -> TestResult {
    let runtime = v2_test_support::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded = calls.clone();
    let error = match v2_test_support::requests(&runtime)
        .request_with_factory("zero-attempt", move |_| {
            recorded.fetch_add(1, Ordering::Relaxed);
            Ok(Message::text("never-constructed"))
        })?
        .options(WsRequestOperationOptions {
            attempt_timeout: Some(Duration::ZERO),
            ..WsRequestOperationOptions::default()
        })
        .send()
        .await
    {
        Err(error) => error,
        Ok(_) => return Err("zero attempt unexpectedly succeeded".into()),
    };
    check_eq!(error.kind(), ErrorKind::TimedOut)?;
    check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Attempt))?;
    let report = error.retry_report().ok_or("missing attempt report")?;
    check_eq!(report.attempts, 1)?;
    check_eq!(report.transport_sends, 0)?;
    check_eq!(calls.load(Ordering::Relaxed), 0)?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    runtime.wait_requests().await;
    Ok(())
}

#[tokio::test]
async fn determined_timeouts_keep_their_source_after_attempt_deadline() -> TestResult {
    let runtime = v2_test_support::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let expired = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .ok_or("clock range")?;
    let cancel = CancellationToken::new();
    let options = WsRequestOperationOptions::default();
    let policy = WsRequestRetryPolicy::builder()
        .retry_on(
            WsRequestRetryOn::none()
                .with_write_timeout(true)
                .with_response_timeout(true),
        )
        .build()?;
    for (phase, stage) in [
        (WsTimeoutPhase::Write, ErrorStage::Write),
        (WsTimeoutPhase::Response, ErrorStage::Response),
    ] {
        let timeout = NetError::from(ErrorKind::TimedOut)
            .with_stage(stage)
            .with_ws_timeout_phase(phase);
        let determined = if phase == WsTimeoutPhase::Write {
            NetError::with_source(ErrorKind::DeliveryUnknown, timeout).with_stage(stage)
        } else {
            timeout
        };
        let (sender, receiver) = oneshot::channel::<Result<Response>>();
        sender
            .send(Err(determined))
            .map_err(|_| "ready result channel closed")?;
        let result = scope_attempt(
            &runtime,
            &options,
            &cancel,
            None,
            Some(TimeoutBudget::new(expired, WsTimeoutPhase::Attempt)),
            None,
            async {
                receiver
                    .await
                    .map_err(|_| NetError::from(ErrorKind::EngineDropped))?
            },
        )
        .await;
        let error = match result {
            Err(error) => preserve_timeout_phase(error),
            Ok(_) => return Err("timeout became success".into()),
        };
        check_eq!(error.ws_timeout_phase(), Some(phase))?;
        check!(policy.should_retry_error(&error))?;
        if phase == WsTimeoutPhase::Write {
            check_eq!(error.kind(), ErrorKind::DeliveryUnknown)?;
        }
    }
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    Ok(())
}

#[tokio::test]
async fn determined_success_survives_late_attempt_poll_but_total_and_cancel_override() -> TestResult
{
    let runtime = v2_test_support::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let expired = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .ok_or("clock range")?;
    let options = WsRequestOperationOptions::default();
    for mode in [0, 1, 2, 3] {
        let cancel = CancellationToken::new();
        if mode == 2 {
            cancel.cancel();
        }
        let response = Response::from_incoming(
            RequestId::new("confirmed")?,
            &v2_test_support::incoming(1, "ok"),
        )?;
        let (sender, receiver) = oneshot::channel();
        sender
            .send(Ok(response))
            .map_err(|_| "ready result channel closed")?;
        let result = scope_attempt(
            &runtime,
            &options,
            &cancel,
            if mode == 1 { Some(expired) } else { None },
            Some(TimeoutBudget::new(expired, WsTimeoutPhase::Attempt)),
            None,
            async {
                let result = receiver
                    .await
                    .map_err(|_| NetError::from(ErrorKind::EngineDropped))?;
                if mode == 3 {
                    cancel.cancel();
                }
                result
            },
        )
        .await;
        match mode {
            0 => {
                check!(
                    result.is_ok(),
                    "determined response was replaced by Attempt"
                )?;
            }
            1 => {
                let error = result.err().ok_or("total did not override ready success")?;
                check_eq!(error.kind(), ErrorKind::TimedOut)?;
                check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Total))?;
            }
            _ => {
                check_eq!(
                    result
                        .err()
                        .ok_or("cancel did not override ready success")?
                        .kind(),
                    ErrorKind::Cancelled
                )?;
            }
        }
    }
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    Ok(())
}

#[tokio::test]
async fn pending_outcome_uses_actual_attempt_timer() -> TestResult {
    let runtime = v2_test_support::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let expired = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .ok_or("clock range")?;
    let result: Result<Response> = scope_attempt(
        &runtime,
        &WsRequestOperationOptions::default(),
        &CancellationToken::new(),
        None,
        Some(TimeoutBudget::new(expired, WsTimeoutPhase::Attempt)),
        None,
        std::future::pending(),
    )
    .await;
    let error = result
        .err()
        .ok_or("pending outcome unexpectedly completed")?;
    check_eq!(error.kind(), ErrorKind::TimedOut)?;
    check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Attempt))?;
    let policy = WsRequestRetryPolicy::builder()
        .retry_on(
            WsRequestRetryOn::none()
                .with_write_timeout(true)
                .with_response_timeout(true),
        )
        .build()?;
    check!(!policy.should_retry_error(&error))?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    Ok(())
}

#[derive(Clone, Copy)]
enum PublicationBoundary {
    None,
    Total,
    Cancel,
    GroupCancel,
    Shutdown,
    ClientShutdown,
    SessionCancel,
}

async fn publication_window_result(
    selected_phase: Option<WsTimeoutPhase>,
    boundary: PublicationBoundary,
    natural_disconnect: bool,
) -> TestResult {
    let runtime = v2_test_support::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let receipt = v2_test_support::requests(&runtime)
        .request(v2_test_support::request("publication-window")?)
        .try_enqueue()
        .map_err(EnqueueError::into_error)?;
    let handle = receipt.handle().clone();
    let queued = runtime.queue.try_next().ok_or("missing queued request")?;
    check!(queued
        .dispatch_phase
        .mark_writing(ConnectionId::from_allocated(1)))?;
    check!(runtime
        .pending
        .bind_connection(handle.registration(), ConnectionId::from_allocated(1))?)?;
    if selected_phase == Some(WsTimeoutPhase::Write) {
        check!(queued.dispatch_phase.start_data_write(false)?)?;
    }
    let (selected_tx, selected_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    runtime.pending.set_before_completion_for_test(move || {
        let _ = selected_tx.send(());
        let _ = release_rx.recv_timeout(Duration::from_secs(3));
    })?;
    let table = runtime.pending.clone();
    let request_id = handle.request_id().clone();
    let producer = tokio::task::spawn_blocking(move || -> Result<()> {
        if let Some(phase) = selected_phase {
            let stage = if phase == WsTimeoutPhase::Write {
                ErrorStage::Write
            } else {
                ErrorStage::Response
            };
            queued.complete(Err(NetError::from(ErrorKind::TimedOut)
                .with_stage(stage)
                .with_ws_timeout_phase(phase)));
        } else {
            if !table.complete_response(&request_id, &v2_test_support::incoming(1, "selected"))? {
                return Err(NetError::from(ErrorKind::Internal));
            }
            queued.complete(Ok(()));
        }
        Ok(())
    });
    tokio::time::timeout(Duration::from_secs(2), selected_rx).await??;
    if natural_disconnect {
        runtime.connection_ended(
            ConnectionId::from_allocated(1),
            NetError::from(ErrorKind::NotConnected),
        )?;
        runtime.end(NetError::from(ErrorKind::NotConnected), TaskEndCause::Disconnected);
    }
    let (arbitrated_tx, arbitrated_rx) = oneshot::channel();
    runtime.pending.set_before_prepare_for_test(move || {
        let _ = arbitrated_tx.send(());
    })?;
    let cancel = CancellationToken::new();
    let group = CancellationGroup::new();
    let options = WsRequestOperationOptions {
        cancellation: Some(group.clone()),
        ..WsRequestOperationOptions::default()
    };
    let expired = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .ok_or("clock range")?;
    let total = if matches!(boundary, PublicationBoundary::Total) {
        Some(
            Instant::now()
                .checked_add(Duration::from_millis(50))
                .ok_or("clock range")?,
        )
    } else {
        None
    };
    let mut waiting = Box::pin(scope_attempt(
        &runtime,
        &options,
        &cancel,
        total,
        Some(TimeoutBudget::new(expired, WsTimeoutPhase::Attempt)),
        Some(&handle),
        receipt.response(),
    ));
    // The result is already selected, but its producer is stopped before any
    // publication. The expired Attempt must join that same terminal authority.
    let arbitration = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            biased;
            result = &mut waiting => Err(format!("result escaped publication gate: {result:?}")),
            result = arbitrated_rx => result.map_err(|error| error.to_string()),
        }
    })
    .await;
    if arbitration.as_ref().is_err() || arbitration.as_ref().is_ok_and(|result| result.is_err()) {
        let _ = release_tx.send(());
        producer.await??;
        arbitration??;
        return Err("missing timeout arbitration".into());
    }
    let override_kind = match boundary {
        PublicationBoundary::None => None,
        PublicationBoundary::Total => Some(ErrorKind::TimedOut),
        PublicationBoundary::Cancel | PublicationBoundary::GroupCancel => Some(ErrorKind::Cancelled),
        _ => Some(ErrorKind::Closed),
    };
    let result = if override_kind.is_some() {
        match boundary {
            PublicationBoundary::Cancel => cancel.cancel(),
            PublicationBoundary::GroupCancel => { group.cancel(); },
            PublicationBoundary::Shutdown => runtime.end(
                NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown,
            ),
            PublicationBoundary::ClientShutdown => runtime.shutdown.cancel(),
            PublicationBoundary::SessionCancel => { runtime.lifecycle.request_cancel(); },
            PublicationBoundary::None | PublicationBoundary::Total => {},
        }
        let result = tokio::time::timeout(Duration::from_secs(2), &mut waiting).await;
        let _ = release_tx.send(());
        producer.await??;
        result?
    } else {
        release_tx
            .send(())
            .map_err(|_| "completion gate was released early")?;
        producer.await??;
        tokio::time::timeout(Duration::from_secs(2), &mut waiting).await?
    };
    if let Some(kind) = override_kind {
        let error = result
            .err()
            .ok_or("outer limit did not override selected result")?;
        check_eq!(error.kind(), kind)?;
        if kind == ErrorKind::TimedOut {
            check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Total))?;
        }
    } else if let Some(phase) = selected_phase {
        let error = preserve_timeout_phase(result.err().ok_or("selected failure became success")?);
        check_eq!(error.ws_timeout_phase(), Some(phase))?;
        if phase == WsTimeoutPhase::Write {
            check_eq!(error.kind(), ErrorKind::DeliveryUnknown)?;
        }
    } else {
        check_eq!(result?.request_id(), handle.request_id())?;
    }
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    Ok(())
}

#[tokio::test]
async fn selected_success_and_failure_wait_for_publication_after_attempt_timeout() -> TestResult {
    for phase in [
        None,
        Some(WsTimeoutPhase::Write),
        Some(WsTimeoutPhase::Response),
    ] {
        publication_window_result(phase, PublicationBoundary::None, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn total_and_cancel_override_selected_result_while_publication_is_blocked() -> TestResult {
    for boundary in [PublicationBoundary::Total, PublicationBoundary::Cancel] {
        publication_window_result(None, boundary, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn natural_disconnect_preserves_selected_results_before_publication() -> TestResult {
    for phase in [None, Some(WsTimeoutPhase::Write), Some(WsTimeoutPhase::Response)] {
        publication_window_result(phase, PublicationBoundary::None, true).await?;
    }
    Ok(())
}

#[tokio::test]
async fn outer_boundaries_interrupt_publication_after_natural_disconnect() -> TestResult {
    for boundary in [
        PublicationBoundary::Total,
        PublicationBoundary::Cancel,
        PublicationBoundary::GroupCancel,
        PublicationBoundary::Shutdown,
        PublicationBoundary::ClientShutdown,
        PublicationBoundary::SessionCancel,
    ] {
        for phase in [None, Some(WsTimeoutPhase::Write)] {
            publication_window_result(phase, boundary, true).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn attempt_timer_selects_through_pending_and_keeps_earlier_response_budget() -> TestResult {
    use crate::module::ws_client::operation_control::OperationControl;
    let runtime = v2_test_support::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    for (response_timeout, expected) in [
        (Duration::from_millis(1), WsTimeoutPhase::Response),
        (Duration::from_secs(1), WsTimeoutPhase::Attempt),
    ] {
        let absolute = Instant::now()
            .checked_add(Duration::from_millis(10))
            .ok_or("clock range")?;
        let request = Arc::new(v2_test_support::request("timer-arbitration")?);
        let options = RequestOptions {
            send: SendOptions {
                deadline: Some(absolute),
                ..SendOptions::default()
            },
            response_timeout,
            response_timeout_origin: ResponseTimeoutOrigin::Registered,
            ..RequestOptions::default()
        };
        let core = OperationControl::new_with_deadline_phase(
            runtime.client_id,
            runtime.id,
            runtime.allocate_operation()?,
            &options.send,
            true,
            TaskSource::Request(request.clone()),
            TaskEventReservation::unobserved(0, MessageLane::Normal),
            WsTimeoutPhase::Attempt,
        );
        let (handle, receiver) = runtime.pending.register(
            request,
            &options,
            core,
            runtime.pending.try_reserve_slot()?,
        )?;
        runtime.wait_deadline(Some(absolute)).await?;
        let result = scope_attempt(
            &runtime,
            &WsRequestOperationOptions::default(),
            &CancellationToken::new(),
            None,
            Some(TimeoutBudget::new(absolute, WsTimeoutPhase::Attempt)),
            Some(&handle),
            async {
                receiver
                    .await
                    .map_err(|_| NetError::from(ErrorKind::EngineDropped))?
            },
        )
        .await;
        let error = result
            .err()
            .ok_or("timer arbitration unexpectedly succeeded")?;
        check_eq!(error.ws_timeout_phase(), Some(expected))?;
        check!(runtime.pending.snapshot()?.is_empty())?;
        check_eq!(
            handle
                .state()?
                .result
                .and_then(|result| result.err())
                .ok_or("no selected terminal")?
                .ws_timeout_phase(),
            Some(expected)
        )?;
    }
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    Ok(())
}

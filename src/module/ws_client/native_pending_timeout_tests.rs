use super::*;
use crate::module::ws_client::native_task_observer::TaskEventReservation;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::module::ws_client::v2_test_support;
use crate::ws::{MessageLane, OperationId, SendOptions, TaskSource, WsTimeoutPhase};

fn timed_core(options: &SendOptions, phase: WsTimeoutPhase) -> Result<Arc<OperationControl>> {
    let request = Arc::new(v2_test_support::request("timed")?);
    Ok(OperationControl::new_with_deadline_phase(
        ClientId::from_allocated(1),
        SessionId::from_allocated(1),
        OperationId::from_allocated(1),
        options,
        true,
        TaskSource::Request(request),
        TaskEventReservation::unobserved(0, MessageLane::Normal),
        phase,
    ))
}

#[tokio::test]
async fn delayed_pending_expiry_preserves_earliest_response_or_absolute_source() -> TestResult {
    for (response_ms, absolute_ms, absolute_phase, expected) in [
        (10, 20, WsTimeoutPhase::Attempt, WsTimeoutPhase::Response),
        (20, 10, WsTimeoutPhase::Attempt, WsTimeoutPhase::Attempt),
        (20, 10, WsTimeoutPhase::Total, WsTimeoutPhase::Total),
    ] {
        let pending = v2_test_support::pending(1)?;
        let start = now();
        let options = RequestOptions {
            send: SendOptions {
                deadline: Some(add(start, Duration::from_millis(absolute_ms))?),
                ..SendOptions::default()
            },
            response_timeout: Duration::from_millis(response_ms),
            response_timeout_origin: ResponseTimeoutOrigin::Registered,
            ..RequestOptions::default()
        };
        let core = timed_core(&options.send, absolute_phase)?;
        let (_, response) = pending.register(
            Arc::new(v2_test_support::request("timed")?),
            &options,
            core.clone(),
            pending.try_reserve_slot()?,
        )?;
        // Deliberately service the pending timer after BOTH cutoffs. The chosen
        // source must come from the original budgets, not the service time.
        pending.expire_due(add(start, Duration::from_secs(1))?)?;
        let error = response
            .await?
            .err()
            .ok_or("expired pending completed successfully")?;
        check_eq!(error.ws_timeout_phase(), Some(expected))?;
        check_eq!(core.deadline_error().ws_timeout_phase(), Some(expected))?;
        check!(pending.snapshot()?.is_empty())?;
    }
    Ok(())
}

#[tokio::test]
async fn absolute_attempt_budget_still_bounds_manual_response_grace() -> TestResult {
    let pending = v2_test_support::pending(1)?;
    let deadline = add(now(), Duration::from_secs(1))?;
    let options = RequestOptions {
        send: SendOptions {
            deadline: Some(deadline),
            ..SendOptions::default()
        },
        response_timeout: Duration::from_millis(10),
        response_timeout_origin: ResponseTimeoutOrigin::Registered,
        ..RequestOptions::default()
    };
    let core = timed_core(&options.send, WsTimeoutPhase::Attempt)?;
    let (handle, response) = pending.register(
        Arc::new(v2_test_support::request("timed")?),
        &options,
        core,
        pending.try_reserve_slot()?,
    )?;
    {
        let mut state = pending.lock()?;
        let entry = state
            .entries
            .get_mut(handle.registration().request_id())
            .ok_or("missing entry")?;
        // This is the same stored state as an eligible Manual dispatch whose
        // grace window reaches the absolute upper bound.
        entry.grace_used = true;
        entry.grace_deadline = Some(deadline);
        entry.deferred_error = Some(timeout());
        entry.deferred_cause = Some(TaskEndCause::Expired);
        entry.core.set_response_deadline(Some(deadline));
    }
    pending.expire_due(deadline)?;
    let error = response
        .await?
        .err()
        .ok_or("grace exceeded absolute budget")?;
    check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Attempt))?;
    check!(pending.snapshot()?.is_empty())?;
    Ok(())
}

#[tokio::test]
async fn expired_absolute_registration_keeps_attempt_provenance() -> TestResult {
    let pending = v2_test_support::pending(1)?;
    let past = now()
        .checked_sub(Duration::from_secs(1))
        .ok_or("clock range")?;
    let options = RequestOptions {
        send: SendOptions {
            deadline: Some(past),
            ..SendOptions::default()
        },
        ..RequestOptions::default()
    };
    let core = timed_core(&options.send, WsTimeoutPhase::Attempt)?;
    let result = pending.register(
        Arc::new(v2_test_support::request("timed")?),
        &options,
        core,
        pending.try_reserve_slot()?,
    );
    let error = result.err().ok_or("expired registration was accepted")?;
    check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Attempt))?;
    check!(pending.snapshot()?.is_empty())?;
    Ok(())
}

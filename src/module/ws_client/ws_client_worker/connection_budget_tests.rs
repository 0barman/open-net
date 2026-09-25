use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::sync::{Arc, Barrier};

#[tokio::test(start_paused = true)]
async fn budget_chooses_earliest_expiry_and_initial_wins_a_tie() -> TestResult {
    let now = Instant::now();
    for (initial, cycle, expected, kind) in [
        (Some(2), Some(5), 2, ErrorKind::TimedOut),
        (Some(5), Some(2), 2, ErrorKind::RetryExhausted),
        (Some(2), Some(2), 2, ErrorKind::TimedOut),
        (None, Some(2), 2, ErrorKind::RetryExhausted),
        (Some(2), None, 2, ErrorKind::TimedOut),
    ] {
        let budget = ConnectionBudget::new(
            now,
            initial.map(|seconds| now + Duration::from_secs(seconds)),
            cycle.map(Duration::from_secs),
        )?;
        let deadline = budget
            .deadline()
            .ok_or_else(|| test_error("missing budget"))?;
        check_eq!(deadline.at(), now + Duration::from_secs(expected))?;
        check_eq!(deadline.error().kind(), kind)?;
        check!(deadline.expired_error().is_none())?;
    }
    check!(ConnectionBudget::new(now, None, None)?.deadline().is_none())?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn attempt_budget_is_minimum_and_never_restarts_the_cycle_clock() -> TestResult {
    let started = Instant::now();
    let budget = ConnectionBudget::new(
        started,
        Some(started + Duration::from_secs(9)),
        Some(Duration::from_secs(6)),
    )?;
    let first = budget.attempt_deadline(Duration::from_secs(3))?;
    check_eq!(first.at(), started + Duration::from_secs(3))?;
    check_eq!(first.error().kind(), ErrorKind::TimedOut)?;
    tokio::time::advance(Duration::from_secs(5)).await;
    let next = budget.attempt_deadline(Duration::from_secs(3))?;
    check_eq!(next.at(), started + Duration::from_secs(6))?;
    check_eq!(next.error().kind(), ErrorKind::RetryExhausted)?;
    tokio::time::advance(Duration::from_secs(1)).await;
    check_eq!(
        next.expired_error().map(|e| e.kind()),
        Some(ErrorKind::RetryExhausted)
    )?;
    Ok(())
}

#[test]
fn unrepresentable_deadlines_return_configuration_errors() -> TestResult {
    let now = Instant::now();
    let cycle = ConnectionBudget::new(now, None, Some(Duration::MAX))
        .err()
        .ok_or_else(|| test_error("overflowed cycle deadline accepted"))?;
    check_eq!(cycle.kind(), ErrorKind::InvalidConfig)?;
    let handshake = ConnectionBudget::new(now, None, None)?
        .attempt_deadline(Duration::MAX)
        .err()
        .ok_or_else(|| test_error("overflowed handshake deadline accepted"))?;
    check_eq!(handshake.kind(), ErrorKind::InvalidConfig)?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn successful_admission_cannot_be_rewritten_by_later_timeout() -> TestResult {
    let deadline = ConnectionBudget::new(Instant::now(), None, None)?
        .attempt_deadline(Duration::from_secs(1))?;
    let admission = HandshakeAdmission::new(deadline);
    admission.try_accept()?;
    tokio::time::advance(Duration::from_secs(1)).await;
    check!(!admission.expire())?;
    let duplicate = admission
        .try_accept()
        .err()
        .ok_or_else(|| test_error("duplicate admission accepted"))?;
    check_eq!(duplicate.kind(), ErrorKind::Internal)?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn expired_admission_rejects_late_success_with_original_budget_kind() -> TestResult {
    for initial in [false, true] {
        let now = Instant::now();
        let budget = ConnectionBudget::new(
            now,
            initial.then_some(now + Duration::from_secs(1)),
            Some(Duration::from_secs(1)),
        )?;
        let deadline = budget
            .deadline()
            .ok_or_else(|| test_error("missing deadline"))?;
        let admission = HandshakeAdmission::new(deadline);
        tokio::time::advance(Duration::from_secs(1)).await;
        let failure = admission
            .try_accept()
            .err()
            .ok_or_else(|| test_error("expired admission accepted"))?;
        check_eq!(
            failure.kind(),
            if initial {
                ErrorKind::TimedOut
            } else {
                ErrorKind::RetryExhausted
            }
        )?;
        check!(admission.expire())?;
    }
    Ok(())
}

#[test]
fn timeout_and_worker_admission_have_exactly_one_winner() -> TestResult {
    for _ in 0..16 {
        let deadline = ConnectionBudget::new(Instant::now(), None, None)?
            .attempt_deadline(Duration::from_secs(60))?;
        let admission = Arc::new(HandshakeAdmission::new(deadline));
        let barrier = Arc::new(Barrier::new(2));
        let accepted = Arc::clone(&admission);
        let start = Arc::clone(&barrier);
        let worker = std::thread::Builder::new().spawn(move || {
            start.wait();
            accepted.try_accept().is_ok()
        })?;
        barrier.wait();
        let expired = admission.expire();
        let accepted = worker
            .join()
            .map_err(|_| test_error("admission probe failed"))?;
        check!(
            accepted != expired,
            "timeout and admission did not have exactly one winner"
        )?;
    }
    Ok(())
}

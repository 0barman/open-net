use super::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

fn rejected_field<T>(result: Result<T, RetryConfigError>, field: &str) -> TestResult {
    match result {
        Ok(_) => Err(std::io::Error::other("invalid retry configuration was accepted").into()),
        Err(error) => check(error.field() == field, "wrong configuration field reported"),
    }
}

#[test]
fn defaults_are_one_immediate_attempt_and_no_jitter() -> TestResult {
    let schedule = RetrySchedule::builder().build()?;
    check(
        schedule.max_attempts() == 1,
        "default attempted more than once",
    )?;
    check(
        schedule.backoff() == &Backoff::None,
        "default backoff changed",
    )?;
    check(schedule.jitter() == Jitter::None, "default added jitter")?;
    rejected_field(schedule.delay_for(1), "retry_ordinal")?;
    let mut state = RetryState::with_seed(schedule, 10);
    check(state.begin_attempt()? == 1, "initial attempt was not one")?;
    check(
        state.next_delay()?.is_none(),
        "default offered a second attempt",
    )?;
    check(
        matches!(
            state.begin_attempt(),
            Err(RetryStateError::AttemptsExhausted)
        ),
        "attempt limit ignored",
    )
}

#[test]
fn finite_attempt_count_has_no_legacy_cap_or_per_attempt_allocation() -> TestResult {
    rejected_field(
        RetrySchedule::builder().max_attempts(0).build(),
        "max_attempts",
    )?;
    let schedule = RetrySchedule::builder()
        .max_attempts(u32::MAX)
        .backoff(Backoff::constant(Duration::from_millis(1)))
        .build()?;
    check(
        schedule.max_attempts() == u32::MAX,
        "maximum finite count was clamped",
    )?;
    check(
        schedule.delay_for(u32::MAX - 1)? == Duration::from_millis(1),
        "large ordinal changed delay",
    )?;
    let mut state = RetryState::new(schedule);
    check(
        state.begin_attempt()? == 1,
        "large budget prevented the first attempt",
    )
}

#[test]
fn explicit_sequence_matches_exactly_and_never_repeats_last_delay() -> TestResult {
    let schedule = RetrySchedule::builder()
        .max_attempts(5)
        .backoff(Backoff::sequence([
            Duration::from_millis(500),
            Duration::from_millis(800),
            Duration::from_millis(1500),
            Duration::from_millis(2000),
        ]))
        .build()?;
    let mut state = RetryState::with_seed(schedule, 20);
    for expected in [500, 800, 1500, 2000] {
        state.begin_attempt()?;
        check(
            state.next_delay()? == Some(Duration::from_millis(expected)),
            "sequence delay changed",
        )?;
    }
    state.begin_attempt()?;
    check(
        state.next_delay()?.is_none(),
        "sequence repeated its last value",
    )?;
    for delays in [vec![], vec![Duration::ZERO], vec![Duration::ZERO; 3]] {
        rejected_field(
            RetrySchedule::builder()
                .max_attempts(3)
                .backoff(Backoff::sequence(delays))
                .build(),
            "backoff.sequence",
        )?;
    }
    RetrySchedule::builder()
        .max_attempts(1)
        .backoff(Backoff::sequence([]))
        .build()?;
    rejected_field(
        Backoff::sequence([Duration::ZERO]).delay_for(2),
        "retry_ordinal",
    )
}

#[test]
fn exponential_supports_custom_multiplier_and_large_ordinals() -> TestResult {
    let backoff = Backoff::exponential(Duration::from_millis(100), 3.0, Duration::from_secs(1));
    for (ordinal, expected) in [(1, 100), (2, 300), (3, 900), (4, 1000), (u32::MAX, 1000)] {
        check(
            backoff.delay_for(ordinal)? == Duration::from_millis(expected),
            "exponential delay changed",
        )?;
    }
    let unit_multiplier =
        Backoff::exponential(Duration::from_nanos(7), 1.0, Duration::from_secs(1));
    check(
        unit_multiplier.delay_for(u32::MAX)? == Duration::from_nanos(7),
        "unit multiplier grew",
    )?;
    let zero_initial = Backoff::exponential(Duration::ZERO, f64::MAX, Duration::from_secs(1));
    check(
        zero_initial.delay_for(u32::MAX)? == Duration::ZERO,
        "zero initial delay grew",
    )
}

#[test]
fn invalid_numeric_inputs_and_unrepresentable_durations_return_errors() -> TestResult {
    for multiplier in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, 0.9] {
        rejected_field(
            RetrySchedule::builder()
                .backoff(Backoff::exponential(
                    Duration::ZERO,
                    multiplier,
                    Duration::ZERO,
                ))
                .build(),
            "backoff.multiplier",
        )?;
    }
    rejected_field(
        RetrySchedule::builder()
            .backoff(Backoff::exponential(
                Duration::from_secs(2),
                2.0,
                Duration::from_secs(1),
            ))
            .build(),
        "backoff.max",
    )?;
    rejected_field(
        RetrySchedule::builder()
            .backoff(Backoff::constant(Duration::MAX))
            .build(),
        "backoff.constant",
    )?;
    for ratio in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
        rejected_field(
            RetrySchedule::builder()
                .jitter(Jitter::Proportional { ratio })
                .build(),
            "jitter.ratio",
        )?;
    }
    rejected_field(Backoff::None.delay_for(0), "retry_ordinal")
}

#[test]
fn jitter_stays_bounded_and_exponential_maximum_is_retained() -> TestResult {
    for jitter in [Jitter::Full, Jitter::Proportional { ratio: 0.2 }] {
        let schedule = RetrySchedule::builder()
            .max_attempts(129)
            .backoff(Backoff::constant(Duration::from_secs(1)))
            .jitter(jitter)
            .build()?;
        let mut state = RetryState::with_seed(schedule, 42);
        for _ in 0..128 {
            state.begin_attempt()?;
            let delay = state
                .next_delay()?
                .ok_or_else(|| std::io::Error::other("delay absent"))?;
            match jitter {
                Jitter::Full => check(
                    delay <= Duration::from_secs(1),
                    "full jitter exceeded delay",
                )?,
                Jitter::Proportional { .. } => check(
                    (Duration::from_millis(800)..=Duration::from_millis(1200)).contains(&delay),
                    "proportional jitter exceeded range",
                )?,
                Jitter::None => return Err(std::io::Error::other("unexpected jitter case").into()),
            }
        }
    }
    let schedule = RetrySchedule::builder()
        .max_attempts(10)
        .backoff(Backoff::exponential(
            Duration::from_secs(1),
            2.0,
            Duration::from_secs(1),
        ))
        .jitter(Jitter::Proportional { ratio: 1.0 })
        .build()?;
    for sample in [0.0, 0.5, 1.0] {
        check(
            schedule.sampled_delay(1, sample)? <= Duration::from_secs(1),
            "jitter bypassed exponential cap",
        )?;
    }
    Ok(())
}

#[test]
fn operation_state_is_independent_and_delay_reads_do_not_resample() -> TestResult {
    let schedule = RetrySchedule::builder()
        .max_attempts(4)
        .backoff(Backoff::constant(Duration::from_secs(1)))
        .jitter(Jitter::Full)
        .build()?;
    let mut first = RetryState::with_seed(schedule.clone(), 101);
    let mut second = RetryState::with_seed(schedule, 101);
    first.begin_attempt()?;
    let first_delay = first.next_delay()?;
    check(
        first.next_delay()? == first_delay,
        "reading delay consumed another random sample",
    )?;
    first.begin_attempt()?;
    first.next_delay()?;
    check(
        second.attempts() == 0,
        "cloned schedule shared attempt state",
    )?;
    second.begin_attempt()?;
    check(
        second.next_delay()? == first_delay,
        "cloned schedule shared random state",
    )
}

#[test]
fn reports_preserve_budget_counts_and_first_terminal_reason() -> TestResult {
    let schedule = RetrySchedule::builder().max_attempts(3).build()?;
    let mut state = RetryState::with_seed(schedule, 0);
    check(
        matches!(state.next_delay(), Err(RetryStateError::AttemptNotStarted)),
        "delay existed before an attempt",
    )?;
    check(
        matches!(
            state.record_transport_send(),
            Err(RetryStateError::AttemptNotStarted)
        ),
        "send counted before attempt",
    )?;
    state.begin_attempt()?;
    state.record_transport_send()?;
    state.record_transport_send()?;
    state.begin_attempt()?;
    state.record_transport_send()?;
    check(
        state.finish(RetryStopReason::Completed),
        "first completion failed",
    )?;
    check(
        !state.finish(RetryStopReason::Cancelled),
        "late cancellation changed completion",
    )?;
    check(!state.can_retry(), "completed operation could retry")?;
    check(
        matches!(state.begin_attempt(), Err(RetryStateError::Finished)),
        "completed operation started again",
    )?;
    let report = state.report(Duration::from_millis(50));
    check(
        report.attempts == 2 && report.retry_count == 1 && report.transport_sends == 3,
        "report lost counts",
    )?;
    check(
        report.stop_reason == Some(RetryStopReason::Completed),
        "report lost terminal reason",
    )?;
    let context = state.context(Duration::from_millis(50), Some(Duration::from_millis(100)));
    check(
        context.attempt == 2 && context.remaining == Some(Duration::from_millis(100)),
        "context lost budget",
    )
}

#[test]
fn policy_selection_keeps_inherit_disabled_and_override_distinct() -> TestResult {
    let default = String::from("client");
    check(
        RetrySetting::Inherit.resolve(&default, "disabled".into()) == "client",
        "inherit ignored default",
    )?;
    check(
        RetrySetting::Disabled.resolve(&default, "disabled".into()) == "disabled",
        "disabled inherited client",
    )?;
    check(
        RetrySetting::Policy(String::from("request")).resolve(&default, "disabled".into())
            == "request",
        "override ignored policy",
    )
}

#[test]
fn observer_delivers_bounded_report_without_protocol_data() -> TestResult {
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&events);
    let observer = RetryObserverHandle::new(move |event: &RetryEvent| match captured.lock() {
        Ok(mut events) => events.push(event.clone()),
        Err(_) => {
            crate::log_e!(crate::LogType::Common; "retry.observer_test", "error", "event_mutex_failed")
        }
    });
    observer.notify(RetryEvent::AttemptStarted { attempt: 1 });
    observer.notify(RetryEvent::Completed(RetryReport::new()));
    let events = events
        .lock()
        .map_err(|_| std::io::Error::other("event mutex failed"))?;
    check(events.len() == 2, "observer did not deliver both events")
}

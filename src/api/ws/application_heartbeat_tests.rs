use super::*;
use crate::error::ErrorKind;
use crate::ws::Message;
use std::time::Duration;

type TestResult = std::result::Result<(), crate::BoxError>;

#[test]
fn heartbeat_diagnostics_bound_alternating_failures_and_preserve_first_after_subscription(
) -> TestResult {
    let send = HeartbeatFailure {
        phase: HeartbeatPhase::Send,
        kind: ErrorKind::Io,
        stage: Some(ErrorStage::Write),
    };
    let matcher = HeartbeatFailure {
        phase: HeartbeatPhase::Matcher,
        kind: ErrorKind::CallbackPanicked,
        stage: Some(ErrorStage::Receive),
    };
    let mut first = HeartbeatDiagnostics::default();
    let mut second = HeartbeatDiagnostics::default();
    check(
        !first.should_report(send, false),
        "unsubscribed failure emitted",
    )?;
    check(
        first.should_report(send, true),
        "unsubscribed failure consumed first diagnostic",
    )?;
    check(
        first.should_report(matcher, true),
        "new reason was suppressed",
    )?;
    let queue_timeout = HeartbeatFailure {
        phase: HeartbeatPhase::Send,
        kind: ErrorKind::TimedOut,
        stage: Some(ErrorStage::Queue),
    };
    let write_timeout = HeartbeatFailure {
        stage: Some(ErrorStage::Write),
        ..queue_timeout
    };
    let unstaged_timeout = HeartbeatFailure {
        stage: None,
        ..queue_timeout
    };
    check(
        first.should_report(queue_timeout, true),
        "queue timeout was suppressed",
    )?;
    check(
        first.should_report(write_timeout, true),
        "same kind at a different stage was suppressed",
    )?;
    check(
        first.should_report(unstaged_timeout, true),
        "unstaged reason shared a stage slot",
    )?;
    for _ in 0..1_000 {
        check(
            !first.should_report(send, true),
            "alternating send failure emitted again",
        )?;
        check(
            !first.should_report(matcher, true),
            "alternating matcher failure emitted again",
        )?;
        check(
            !first.should_report(queue_timeout, true),
            "alternating queue timeout emitted again",
        )?;
        check(
            !first.should_report(write_timeout, true),
            "alternating write timeout emitted again",
        )?;
        check(
            !first.should_report(unstaged_timeout, true),
            "alternating unstaged timeout emitted again",
        )?;
    }
    check(
        second.should_report(send, true),
        "one heartbeat suppressed another",
    )?;
    first.recovered(98_101);
    check(
        first.should_report(send, true),
        "recovery did not reset fault cycle",
    )?;
    check(
        !first.should_report(send, false),
        "disabled interval emitted",
    )?;
    check(
        first.should_report(send, true),
        "re-enabled subscription missed failure",
    )?;
    first.failures = u64::MAX;
    check(
        !first.should_report(send, true) && first.failures == u64::MAX,
        "failure counter wrapped",
    )?;
    check(
        !first.should_report(
            HeartbeatFailure {
                kind: ErrorKind::Cancelled,
                ..send
            },
            true,
        ),
        "normal cancellation logged as error",
    )
}

#[test]
fn heartbeat_diagnostics_emit_safe_phases_and_one_recovery() -> TestResult {
    let (tx, rx) = std::sync::mpsc::channel();
    let _subscription = crate::OpenNetLogger::register_log_listener(
        Box::new(move |record| {
            if record.tag.starts_with("ws.heartbeat.") && record.content.contains("98102") {
                let _ = tx.send(record);
            }
        }),
        &[crate::LogType::WSC],
    )?;
    let mut diagnostics = HeartbeatDiagnostics {
        client_id: 98_102,
        ..Default::default()
    };
    let error = NetError::with_source(
        ErrorKind::Io,
        std::io::Error::other("SECRET_heartbeat_source"),
    )
    .with_stage(ErrorStage::Write);
    for _ in 0..100 {
        diagnostics.error(98_102, HeartbeatPhase::Send, &error);
    }
    diagnostics.recovered(98_102);
    diagnostics.recovered(98_102);
    diagnostics.failed(
        98_102,
        HeartbeatFailure {
            phase: HeartbeatPhase::Matcher,
            kind: ErrorKind::CallbackPanicked,
            stage: Some(ErrorStage::Receive),
        },
    );
    diagnostics.stopped(98_102);
    crate::log_s!(crate::LogType::WSC, crate::LogLevel::Key; "ws.heartbeat.test_barrier", "client_id", 98_102);
    let mut records = Vec::new();
    loop {
        let record = rx.recv_timeout(Duration::from_secs(3))?;
        if record.tag == "ws.heartbeat.test_barrier" {
            break;
        }
        check(
            !record.content.contains("SECRET_"),
            "heartbeat log exposed source",
        )?;
        records.push(record);
    }
    check(
        records.len() == 4,
        "heartbeat emitted duplicate or missing lifecycle logs",
    )?;
    check(
        records[0].level == crate::LogLevel::Error && records[0].content.contains("send"),
        "send phase absent",
    )?;
    check(
        records[1].tag == "ws.heartbeat.recovered" && records[1].content.contains("100"),
        "recovery did not summarize failures",
    )?;
    check(
        records[2].content.contains("matcher") && records[3].tag == "ws.heartbeat.stopped",
        "matcher or stop phase absent",
    )
}

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

#[test]
fn default_configuration_is_fixed_interval_and_write_only() -> TestResult {
    let config = ApplicationHeartbeatConfig::new(Duration::from_secs(20), Message::text("ping"));
    config.validate()?;
    check(
        config.interval == Duration::from_secs(20),
        "interval changed",
    )?;
    check(
        config.response_timeout.is_none(),
        "default unexpectedly waits for response",
    )?;
    check(config.matcher.is_none(), "default unexpectedly has matcher")?;
    check(
        config.receive_options.overflow == crate::ws::ReceiveOverflow::DropOldest,
        "matcher queue default must not disconnect the session",
    )?;
    check(
        config.failure_action == ApplicationHeartbeatFailureAction::Continue,
        "default failure action changed",
    )
}

#[test]
fn zero_interval_and_send_timeout_are_rejected() -> TestResult {
    let interval = ApplicationHeartbeatConfig::new(Duration::ZERO, "ping");
    let interval_error = interval
        .validate()
        .err()
        .ok_or("zero interval was accepted")?;
    check(
        interval_error.kind() == ErrorKind::InvalidConfig,
        "zero interval error kind changed",
    )?;

    let send = ApplicationHeartbeatConfig::new(Duration::from_secs(1), "ping")
        .with_send_timeout(Duration::ZERO);
    let send_error = send
        .validate()
        .err()
        .ok_or("zero send timeout was accepted")?;
    check(
        send_error.kind() == ErrorKind::InvalidConfig,
        "zero send timeout error kind changed",
    )
}

#[test]
fn unrepresentable_interval_is_rejected() -> TestResult {
    let config = ApplicationHeartbeatConfig::new(Duration::MAX, "ping");
    let error = config
        .validate()
        .err()
        .ok_or("unrepresentable interval was accepted")?;
    check(
        error.kind() == ErrorKind::InvalidConfig,
        "unrepresentable interval returned the wrong error kind",
    )
}

#[test]
fn write_only_heartbeat_does_not_validate_unused_receive_limits() -> TestResult {
    let config = ApplicationHeartbeatConfig {
        receive_options: crate::ws::ReceiveOptions {
            max_messages: 0,
            ..crate::ws::ReceiveOptions::default()
        },
        ..ApplicationHeartbeatConfig::new(Duration::from_secs(1), "ping")
    };
    check(
        config.validate().is_ok(),
        "write-only heartbeat validated an unused receive queue",
    )
}

#[test]
fn response_timeout_requires_matcher_and_positive_miss_limit() -> TestResult {
    let timeout_without_matcher = ApplicationHeartbeatConfig::new(Duration::from_secs(1), "ping")
        .with_send_timeout(Duration::from_secs(1));
    let timeout_without_matcher = ApplicationHeartbeatConfig {
        response_timeout: Some(Duration::from_secs(1)),
        ..timeout_without_matcher
    };
    let timeout_error = timeout_without_matcher
        .validate()
        .err()
        .ok_or("response timeout without matcher was accepted")?;
    check(
        timeout_error.kind() == ErrorKind::InvalidConfig,
        "response matcher validation error kind changed",
    )?;

    let zero_misses = ApplicationHeartbeatConfig::new(Duration::from_secs(1), "ping")
        .with_max_missed_responses(0)
        .with_response_matcher(Duration::from_secs(1), |message| {
            message.message().and_then(Message::as_text) == Some("pong")
        });
    check(
        zero_misses.validate().is_err(),
        "zero missed response limit was accepted",
    )
}

#[test]
fn response_matcher_accepts_only_expected_message() -> TestResult {
    let config = ApplicationHeartbeatConfig::new(Duration::from_secs(1), "ping")
        .with_response_matcher(Duration::from_secs(1), |message| {
            message.message().and_then(Message::as_text) == Some("pong")
        });
    check(
        config.matcher.is_some(),
        "response matcher was not installed",
    )?;
    check(
        config.validate().is_ok(),
        "valid response matcher configuration was rejected",
    )
}

#[test]
fn snapshot_starts_running_with_zero_counters() -> TestResult {
    let snapshot = ApplicationHeartbeatSnapshot::default();
    check(
        snapshot.state == ApplicationHeartbeatState::Running,
        "snapshot did not start running",
    )?;
    check(snapshot.sent == 0, "snapshot sent count was not zero")?;
    check(
        snapshot.matched_responses == 0,
        "snapshot response count was not zero",
    )?;
    check(
        snapshot.missed_responses == 0,
        "snapshot misses were not zero",
    )?;
    check(snapshot.failures == 0, "snapshot failures were not zero")?;
    check(snapshot.last_error.is_none(), "snapshot retained an error")
}

#[test]
fn send_options_keep_a_finite_absolute_deadline() -> TestResult {
    let options = super::send_options(Duration::from_secs(2))?;
    check(
        options.deadline.is_some(),
        "heartbeat send options dropped the absolute deadline",
    )
}

#[test]
fn unrepresentable_deadline_is_rejected() -> TestResult {
    let config = ApplicationHeartbeatConfig::new(Duration::from_secs(1), "ping")
        .with_send_timeout(Duration::MAX);
    check(
        config.validate().is_err(),
        "unrepresentable send deadline was accepted",
    )
}

#[test]
fn control_frames_are_rejected_for_application_matching() -> TestResult {
    let config = ApplicationHeartbeatConfig::new(Duration::from_secs(1), "ping")
        .with_response_matcher(Duration::from_secs(1), |_| true)
        .with_receive_options(crate::ws::ReceiveOptions {
            include_control_frames: true,
            ..crate::ws::ReceiveOptions::default()
        });
    let error = config
        .validate()
        .err()
        .ok_or("control frames were accepted by application heartbeat")?;
    check(
        error.kind() == ErrorKind::InvalidConfig,
        "control-frame validation returned the wrong error kind",
    )
}

#[test]
fn matcher_requires_response_timeout() -> TestResult {
    let config = ApplicationHeartbeatConfig {
        matcher: Some(std::sync::Arc::new(|message| {
            message.message().and_then(Message::as_text) == Some("pong")
        })),
        ..ApplicationHeartbeatConfig::new(Duration::from_secs(1), "ping")
    };
    check(
        config.validate().is_err(),
        "matcher without response timeout was accepted",
    )
}

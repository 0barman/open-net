use super::tests::journal;
use super::*;
use crate::module::transport::failure::ConnectStage;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use std::time::Duration;

#[tokio::test]
async fn lifecycle_logs_follow_commits_and_bound_retries_without_sensitive_fields() -> TestResult {
    let (tx, rx) = std::sync::mpsc::channel();
    let _subscription = crate::OpenNetLogger::register_log_listener(
        Box::new(move |record| {
            if record.tag.starts_with("ws.") && record.content.contains("97101") {
                let _ = tx.send(record);
            }
        }),
        &[crate::LogType::WSC],
    )?;
    let metadata = Arc::new(crate::Metadata::from([(
        "name".to_owned(),
        "SECRET_metadata".to_owned(),
    )]));
    let (session, _events) =
        ConnectionSession::journal_fixture_with_metadata(97_101, 97_102, journal(32), metadata)?;
    let failure = || {
        ConnectionFailure::new(
            NetError::with_source(ErrorKind::Io, std::io::Error::other("SECRET_source")),
            ConnectStage::Tcp,
            None,
            true,
        )
    };
    for attempt in 0..5 {
        session.begin_attempt(
            session.handshake_attempt(1, attempt),
            session.reserve_attempt().await?,
        )?;
        session.attempt_failed(
            1,
            attempt,
            failure(),
            RetryDecision::Scheduled {
                after: Duration::from_millis(1),
            },
        )?;
    }
    session.begin_attempt(
        session.handshake_attempt(1, 5),
        session.reserve_attempt().await?,
    )?;
    check!(session.commit_established(1, 5).is_err())?;
    session.set_attempt_credential_version(1, 5, Some("SECRET_credential".to_owned()))?;
    session.established(1, 5)?;
    check!(session.commit_established(1, 5).is_err())?;
    session.connection_terminated_with_details(
        TerminationReason::IoFailure,
        Some(failure()),
        ConnectionTerminationDetails {
            peer_close: Some(crate::ws::PeerClose {
                code: Some(1000),
                reason: "SECRET_close".to_owned(),
            }),
            ..Default::default()
        },
        Some(2),
    )?;
    session.begin_cycle(2, true)?;
    session.begin_attempt(
        session.handshake_attempt(2, 0),
        session.reserve_attempt().await?,
    )?;
    session.attempt_failed(2, 0, failure(), RetryDecision::Stop)?;
    session.terminate(TerminationReason::RetryExhausted, None)?;
    session.terminate(TerminationReason::RetryExhausted, None)?;
    crate::log_s!(crate::LogType::WSC, crate::LogLevel::Key; "ws.lifecycle.lifecycle_logs_follow_commits_and_bound_retries_without_sensitive_fields", "client_id", 97_101);
    let mut records = Vec::new();
    loop {
        let record = rx.recv_timeout(Duration::from_secs(3))?;
        if record.tag == "ws.lifecycle.lifecycle_logs_follow_commits_and_bound_retries_without_sensitive_fields" {
            break;
        }
        check_eq!(record.tag.as_str(), "ws.connection_session.emit")?;
        check_eq!(record.level, crate::LogLevel::Key)?;
        check!(!record.content.contains("SECRET_"))?;
        records.push(record);
    }
    let fields = records
        .iter()
        .map(|record| serde_json::from_str::<serde_json::Value>(&record.content))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    check_eq!(
        fields
            .iter()
            .map(|fields| fields["event"].as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec![
            "ws.reconnect.started",
            "ws.connection.established",
            "ws.connection.disconnected",
            "ws.reconnect.started",
            "ws.session.closed",
        ]
    )?;
    let established = &fields[1];
    // The initial failed connect starts the episode; only the five attempts
    // admitted after that transition count as reconnect attempts.
    check_eq!(established["reconnect_attempts"].as_u64(), Some(5))?;
    check_eq!(established["last_reconnect_stage"].as_str(), Some("Tcp"))?;
    let closed = &fields[4];
    check_eq!(closed["error_kind"].as_str(), Some("RetryExhausted"))?;
    check_eq!(closed["last_reconnect_kind"].as_str(), Some("Io"))?;
    check_eq!(closed["reconnect_attempts"].as_u64(), Some(1))?;
    check_eq!(
        session
            .terminal_result_checked()?
            .ok_or("terminal result")?
            .err()
            .ok_or("expected terminal failure")?
            .kind(),
        ErrorKind::RetryExhausted
    )?;
    Ok(())
}

#[tokio::test]
async fn cancelled_preparation_and_repeated_termination_do_not_log_established() -> TestResult {
    let (tx, rx) = std::sync::mpsc::channel();
    let _subscription = crate::OpenNetLogger::register_log_listener(
        Box::new(move |record| {
            if record.tag.starts_with("ws.") && record.content.contains("97103") {
                let _ = tx.send(record);
            }
        }),
        &[crate::LogType::WSC],
    )?;
    let (session, _events) = ConnectionSession::journal_fixture(97_103, 97_104, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(1, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(1, 0, None)?;
    check!(session.request_cancel())?;
    check!(session.prepare_established(1, 0).is_err())?;
    session.terminate(TerminationReason::Cancelled, None)?;
    session.terminate(TerminationReason::Cancelled, None)?;
    crate::log_s!(crate::LogType::WSC, crate::LogLevel::Key; "ws.lifecycle.cancelled_preparation_and_repeated_termination_do_not_log_established", "client_id", 97_103);
    let record = rx.recv_timeout(Duration::from_secs(3))?;
    check_eq!(record.tag.as_str(), "ws.connection_session.emit")?;
    let fields: serde_json::Value = serde_json::from_str(&record.content)?;
    check_eq!(fields["event"].as_str(), Some("ws.session.closed"))?;
    check_eq!(
        rx.recv_timeout(Duration::from_secs(3))?.tag.as_str(),
        "ws.lifecycle.cancelled_preparation_and_repeated_termination_do_not_log_established"
    )?;
    Ok(())
}

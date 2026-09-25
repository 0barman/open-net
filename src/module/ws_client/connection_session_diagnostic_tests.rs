use super::tests::{closed, journal, next, require_pending, started};
use super::*;
use crate::module::transport::failure::ConnectStage;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::ws::HandshakeDiagnosticKind as DiagnosticKind;
use std::time::Duration;

fn rejected() -> ConnectionFailure {
    ConnectionFailure::new(
        NetError::from(ErrorKind::HandshakeRejected),
        ConnectStage::WebSocketUpgrade,
        Some(503),
        true,
    )
}
async fn publish_failure(
    session: &ConnectionSession,
    cycle: u64,
    attempt: u64,
    version: u64,
    kind: DiagnosticKind,
) -> TestResult {
    session.begin_attempt(
        session.handshake_attempt(cycle, attempt),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(cycle, attempt, Some(version.to_string()))?;
    session.attempt_failed_with_diagnostic(
        cycle,
        attempt,
        rejected(),
        RetryDecision::Scheduled {
            after: Duration::from_millis(250),
        },
        Some(HandshakeDiagnostic::new(kind)),
    )?;
    Ok(())
}
fn failed(event: ConnectionEvent) -> TestResult<(HandshakeAttempt, Option<String>, NetError)> {
    match event.kind {
        ConnectionEventKind::AttemptFailed {
            attempt,
            credential_version,
            error,
            ..
        } => Ok((attempt, credential_version, error)),
        _ => Err(test_error("missing diagnostic failure")),
    }
}

#[tokio::test]
async fn diagnostics_are_atomic_with_original_attempt_identity() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(10, 20, journal(6))?;
    publish_failure(&session, 40, 0, 50, DiagnosticKind::HttpRejected).await?;
    publish_failure(&session, 40, 1, 51, DiagnosticKind::Protocol).await?;
    for (id, version, kind) in [
        (0, 50, DiagnosticKind::HttpRejected),
        (1, 51, DiagnosticKind::Protocol),
    ] {
        started(&mut events, 40, id).await?;
        let (attempt, credential, error) = failed(next(&mut events).await?)?;
        check_eq!(attempt.client_id.as_u64(), 10)?;
        check_eq!(attempt.session_id.as_u64(), 20)?;
        check_eq!(attempt.cycle_id.as_u64(), 40)?;
        check_eq!(attempt.attempt_id.as_u64(), id)?;
        check_eq!(credential, Some(version.to_string()))?;
        check_eq!(error.kind(), ErrorKind::HandshakeRejected)?;
        check_eq!(error.context().client_id, Some(attempt.client_id))?;
        check_eq!(error.context().session_id, Some(attempt.session_id))?;
        check_eq!(error.context().attempt_id, Some(attempt.attempt_id))?;
        check_eq!(
            error.context().http_status,
            Some(http::StatusCode::SERVICE_UNAVAILABLE)
        )?;
        check_eq!(
            error
                .context()
                .diagnostic
                .as_ref()
                .map(|value| value.kind()),
            Some(kind)
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn consuming_facts_releases_slots_without_duplicate_diagnostics() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    publish_failure(&session, 4, 0, 5, DiagnosticKind::HttpRejected).await?;
    let mut waiting = Box::pin(session.reserve_attempt());
    require_pending(waiting.as_mut()).await?;
    started(&mut events, 4, 0).await?;
    require_pending(waiting.as_mut()).await?;
    let first = next(&mut events).await?;
    check_eq!(first.sequence, 2)?;
    check_eq!(
        failed(first)?
            .2
            .context()
            .diagnostic
            .as_ref()
            .map(|value| value.kind()),
        Some(DiagnosticKind::HttpRejected)
    )?;
    session.begin_attempt(session.handshake_attempt(4, 1), waiting.await?)?;
    session.set_attempt_credential_version(4, 1, Some("6".to_owned()))?;
    session.attempt_failed_with_diagnostic(
        4,
        1,
        rejected(),
        RetryDecision::Stop,
        Some(HandshakeDiagnostic::new(DiagnosticKind::Protocol)),
    )?;
    session.terminate(TerminationReason::ConnectFailed, Some(rejected()))?;
    started(&mut events, 4, 1).await?;
    let second = next(&mut events).await?;
    check_eq!(second.sequence, 4)?;
    let (attempt, _, error) = failed(second)?;
    check_eq!(attempt.attempt_id.as_u64(), 1)?;
    check_eq!(
        error
            .context()
            .diagnostic
            .as_ref()
            .map(|value| value.kind()),
        Some(DiagnosticKind::Protocol)
    )?;
    check!(closed(&mut events).await?.is_err())?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn cancelling_receive_preserves_the_next_diagnostic() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    let mut receive = Box::pin(events.recv());
    require_pending(receive.as_mut()).await?;
    publish_failure(&session, 4, 0, 5, DiagnosticKind::HttpRejected).await?;
    drop(receive);
    started(&mut events, 4, 0).await?;
    let event = next(&mut events).await?;
    check_eq!(event.sequence, 2)?;
    check_eq!(
        failed(event)?
            .2
            .context()
            .diagnostic
            .as_ref()
            .map(|value| value.kind()),
        Some(DiagnosticKind::HttpRejected)
    )?;
    Ok(())
}

#[tokio::test]
async fn full_queue_cancel_keeps_diagnostic_and_one_terminal() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    publish_failure(&session, 4, 0, 5, DiagnosticKind::HttpRejected).await?;
    session.terminate(TerminationReason::Cancelled, None)?;
    check!(session.completion_token().is_cancelled())?;
    started(&mut events, 4, 0).await?;
    check!(failed(next(&mut events).await?)?
        .2
        .context()
        .diagnostic
        .is_some())?;
    let terminal = closed(&mut events).await?;
    check!(matches!(terminal, Err(error) if error.kind() == ErrorKind::Cancelled))?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn stale_attempt_diagnostic_cannot_replace_current_attempt() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(4, 1),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(4, 1, Some("6".to_owned()))?;
    check_eq!(
        session.attempt_failed_with_diagnostic(
            4,
            0,
            rejected(),
            RetryDecision::Stop,
            Some(HandshakeDiagnostic::new(DiagnosticKind::Protocol))
        ),
        Err(NetError::from(ErrorKind::Cancelled))
    )?;
    session.attempt_failed_with_diagnostic(
        4,
        1,
        rejected(),
        RetryDecision::Stop,
        Some(HandshakeDiagnostic::new(DiagnosticKind::HttpRejected)),
    )?;
    started(&mut events, 4, 1).await?;
    let (attempt, _, error) = failed(next(&mut events).await?)?;
    check_eq!(attempt.attempt_id.as_u64(), 1)?;
    check_eq!(
        error
            .context()
            .diagnostic
            .as_ref()
            .map(|value| value.kind()),
        Some(DiagnosticKind::HttpRejected)
    )?;
    Ok(())
}

#[tokio::test]
async fn events_survive_session_release_without_affecting_new_session() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    let weak = Arc::downgrade(&session);
    publish_failure(&session, 4, 0, 5, DiagnosticKind::HttpRejected).await?;
    started(&mut events, 4, 0).await?;
    let event = next(&mut events).await?;
    drop(events);
    drop(session);
    check!(weak.upgrade().is_none())?;
    let (new_session, _new_events) = ConnectionSession::journal_fixture(1, 20, journal(4))?;
    check!(!new_session.cancel_token().is_cancelled())?;
    check_eq!(event.session_id.as_u64(), 2)?;
    check!(failed(event)?.2.context().diagnostic.is_some())?;
    Ok(())
}

#[test]
fn public_events_and_failures_are_clone() -> TestResult {
    fn requires_clone<T: Clone>() {}
    requires_clone::<ConnectionFailure>();
    requires_clone::<ConnectionEvent>();
    Ok(())
}

use super::*;
use crate::module::transport::failure::ConnectStage;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::ws::ConnectOptions;
use crate::ws::HandshakeHeaders;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

pub(super) fn journal(events: usize) -> JournalOptions {
    JournalOptions {
        max_events: events,
        max_bytes: match events.checked_mul(MAX_CONNECTION_EVENT_BYTES) {
            Some(bytes) => bytes,
            None => usize::MAX,
        },
    }
}
fn failure(status: u16) -> ConnectionFailure {
    ConnectionFailure::new(
        NetError::from(ErrorKind::HandshakeRejected),
        ConnectStage::WebSocketUpgrade,
        Some(status),
        status >= 500,
    )
}
fn retry() -> RetryDecision {
    RetryDecision::Scheduled {
        after: Duration::from_millis(250),
    }
}
pub(super) async fn require_pending<F: Future>(mut future: Pin<&mut F>) -> TestResult {
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(Ok(())),
        Poll::Ready(_) => Poll::Ready(Err(test_error(
            "operation unexpectedly completed instead of applying backpressure",
        ))),
    })
    .await
}
pub(super) async fn next(
    events: &mut EventReceiver<ConnectionEvent>,
) -> TestResult<ConnectionEvent> {
    tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await??
        .ok_or_else(|| test_error("event stream ended before the expected fact"))
}
pub(super) async fn started(
    events: &mut EventReceiver<ConnectionEvent>,
    cycle: u64,
    attempt: u64,
) -> TestResult {
    let event = next(events).await?;
    let ConnectionEventKind::AttemptStarted { attempt: value } = event.kind else {
        return Err(test_error("missing AttemptStarted"));
    };
    check_eq!(value.client_id, event.client_id)?;
    check_eq!(value.session_id, event.session_id)?;
    check_eq!(value.cycle_id.as_u64(), cycle)?;
    check_eq!(value.attempt_id.as_u64(), attempt)?;
    Ok(())
}
async fn established(events: &mut EventReceiver<ConnectionEvent>) -> TestResult<ConnectionInfo> {
    match next(events).await?.kind {
        ConnectionEventKind::Established { connection } => Ok(connection),
        _ => Err(test_error("missing Established")),
    }
}
async fn disconnected(
    events: &mut EventReceiver<ConnectionEvent>,
) -> TestResult<(ConnectionInfo, ConnectionEnd)> {
    match next(events).await?.kind {
        ConnectionEventKind::Disconnected { connection, end } => Ok((connection, end)),
        _ => Err(test_error("missing Disconnected")),
    }
}
async fn failed(
    events: &mut EventReceiver<ConnectionEvent>,
) -> TestResult<(HandshakeAttempt, Option<String>, NetError, RetryDecision)> {
    match next(events).await?.kind {
        ConnectionEventKind::AttemptFailed {
            attempt,
            credential_version,
            error,
            retry,
        } => Ok((attempt, credential_version, error, retry)),
        _ => Err(test_error("missing AttemptFailed")),
    }
}
pub(super) async fn closed(
    events: &mut EventReceiver<ConnectionEvent>,
) -> TestResult<Result<SessionEnd, NetError>> {
    match next(events).await?.kind {
        ConnectionEventKind::Closed { result } => Ok(result),
        _ => Err(test_error("missing Closed")),
    }
}

#[tokio::test]
async fn unversioned_attempt_requires_one_preparation_and_its_original_metadata() -> TestResult {
    let metadata = Arc::new(crate::Metadata::from([(
        "scope".to_owned(),
        "tenant-a".to_owned(),
    )]));
    let (session, mut events) = ConnectionSession::journal_fixture_with_metadata(
        11,
        21,
        journal(4),
        Arc::clone(&metadata),
    )?;
    let attempt = session.handshake_attempt(41, 0);
    check!(Arc::ptr_eq(&attempt.metadata, &metadata))?;
    let mut foreign = attempt.clone();
    foreign.metadata = Arc::new(crate::Metadata::new());
    check_eq!(
        session.begin_attempt(foreign, session.reserve_attempt().await?),
        Err(NetError::from(ErrorKind::InvalidConfig))
    )?;
    session.begin_attempt(attempt, session.reserve_attempt().await?)?;
    check_eq!(
        session.prepare_established(41, 0),
        Err(NetError::from(ErrorKind::InvalidConfig))
    )?;
    session.set_attempt_credential_version(41, 0, None)?;
    check_eq!(
        session.set_attempt_credential_version(41, 0, Some("second".to_owned())),
        Err(NetError::from(ErrorKind::InvalidConfig))
    )?;
    session.established(41, 0)?;
    started(&mut events, 41, 0).await?;
    check_eq!(established(&mut events).await?.credential_version, None)?;
    Ok(())
}

#[test]
fn connection_session_is_created_without_runtime_and_rejects_invalid_capacity() -> TestResult {
    for capacity in [0, 1, 2, 3, usize::MAX] {
        check!(
            matches!(ConnectionSession::journal_fixture(11, 21, journal(capacity)), Err(error) if error.kind() == ErrorKind::InvalidConfig)
        )?;
    }
    let (session, events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    check!(!session.cancel_token().is_cancelled())?;
    drop(events);
    check!(!session.cancel_token().is_cancelled())?;
    Ok(())
}

#[test]
fn cancelling_one_lifecycle_keeps_other_sessions_and_journals_independent() -> TestResult {
    let (first, first_journal) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    let (second, second_journal) = ConnectionSession::journal_fixture(11, 22, journal(4))?;
    first.request_cancel();
    check!(first.cancel_token().is_cancelled())?;
    check!(!second.cancel_token().is_cancelled())?;
    drop((first_journal, second_journal));
    check!(!second.cancel_token().is_cancelled())?;
    Ok(())
}

#[tokio::test]
async fn session_cancel_rejects_prepared_handshake_and_capacity_waiter() -> TestResult {
    let (session, _journal) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(41, 0, None)?;
    let mut waiter = Box::pin(session.reserve_attempt());
    require_pending(waiter.as_mut()).await?;
    session.request_cancel();
    check_eq!(
        session.prepare_established(41, 0),
        Err(NetError::from(ErrorKind::Cancelled))
    )?;
    check!(
        matches!(tokio::time::timeout(Duration::from_secs(1), waiter).await?, Err(error) if error.kind() == ErrorKind::Cancelled)
    )?;
    Ok(())
}

#[tokio::test]
async fn connection_session_failed_attempt_preserves_context_and_actual_retry_decision(
) -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(41, 0, Some("51".to_owned()))?;
    session.attempt_failed(41, 0, failure(503), RetryDecision::Stop)?;
    session.terminate(TerminationReason::RetryExhausted, Some(failure(503)))?;
    started(&mut events, 41, 0).await?;
    let event = next(&mut events).await?;
    check_eq!(event.sequence, 2)?;
    check_eq!(event.client_id.as_u64(), 11)?;
    check_eq!(event.session_id.as_u64(), 21)?;
    let ConnectionEventKind::AttemptFailed {
        attempt,
        credential_version,
        error,
        retry,
    } = event.kind
    else {
        return Err(test_error("missing failure"));
    };
    check_eq!(attempt.cycle_id.as_u64(), 41)?;
    check_eq!(attempt.attempt_id.as_u64(), 0)?;
    check_eq!(credential_version.as_deref(), Some("51"))?;
    check_eq!(error.kind(), ErrorKind::HandshakeRejected)?;
    check_eq!(error.context().stage, Some(ErrorStage::Upgrade))?;
    check_eq!(
        error.context().http_status,
        Some(http::StatusCode::SERVICE_UNAVAILABLE)
    )?;
    check!(matches!(retry, RetryDecision::Stop))?;
    let terminal = next(&mut events).await?;
    check_eq!(terminal.sequence, 3)?;
    let ConnectionEventKind::Closed { result: Err(error) } = terminal.kind else {
        return Err(test_error(
            "retry exhaustion did not produce a terminal error",
        ));
    };
    check_eq!(error.kind(), ErrorKind::RetryExhausted)?;
    check_eq!(
        error.context().http_status,
        Some(http::StatusCode::SERVICE_UNAVAILABLE)
    )?;
    let original = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<NetError>())
        .ok_or_else(|| test_error("retry exhaustion lost its original handshake failure"))?;
    check_eq!(original.kind(), ErrorKind::HandshakeRejected)?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn connection_session_full_journal_pauses_only_the_next_attempt() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        session.reserve_attempt().await?,
    )?;
    session.attempt_failed(41, 0, failure(503), retry())?;
    let mut waiting = Box::pin(session.reserve_attempt());
    require_pending(waiting.as_mut()).await?;
    started(&mut events, 41, 0).await?;
    require_pending(waiting.as_mut()).await?;
    check!(
        matches!(failed(&mut events).await?.3, RetryDecision::Scheduled { after } if after == Duration::from_millis(250))
    )?;
    session.begin_attempt(session.handshake_attempt(41, 1), waiting.await?)?;
    session.set_attempt_credential_version(41, 1, Some("52".to_owned()))?;
    session.established(41, 1)?;
    session.terminate(TerminationReason::Cancelled, None)?;
    started(&mut events, 41, 1).await?;
    check_eq!(
        established(&mut events)
            .await?
            .credential_version
            .as_deref(),
        Some("52")
    )?;
    disconnected(&mut events).await?;
    check!(
        matches!(closed(&mut events).await?, Err(error) if error.kind() == ErrorKind::Cancelled)
    )?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn connection_session_cancel_finishes_attempt_before_final_event_and_rejects_late_success(
) -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(41, 0, Some("51".to_owned()))?;
    session.request_cancel();
    session.terminate(TerminationReason::Cancelled, None)?;
    session.terminate(TerminationReason::ClientShutdown, None)?;
    check!(session.established(41, 0).is_err())?;
    check!(session
        .attempt_failed(41, 0, failure(401), RetryDecision::Stop)
        .is_err())?;
    check!(session.completion_token().is_cancelled())?;
    started(&mut events, 41, 0).await?;
    let (_, version, error, _) = failed(&mut events).await?;
    check_eq!(error.kind(), ErrorKind::Cancelled)?;
    check_eq!(version.as_deref(), Some("51"))?;
    check!(
        matches!(closed(&mut events).await?, Err(error) if error.kind() == ErrorKind::Cancelled)
    )?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn connection_session_cancels_reservation_wait_without_consumer_progress() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        session.reserve_attempt().await?,
    )?;
    session.attempt_failed(41, 0, failure(503), retry())?;
    let mut waiting = Box::pin(session.reserve_attempt());
    require_pending(waiting.as_mut()).await?;
    session.request_cancel();
    check!(matches!(waiting.await, Err(error) if error.kind() == ErrorKind::Cancelled))?;
    session.terminate(TerminationReason::Cancelled, None)?;
    started(&mut events, 41, 0).await?;
    failed(&mut events).await?;
    check!(
        matches!(closed(&mut events).await?, Err(error) if error.kind() == ErrorKind::Cancelled)
    )?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn connection_session_cancellation_before_start_uses_only_session_slot() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    let reservation = session.reserve_attempt().await?;
    session.request_cancel();
    session.terminate(TerminationReason::Cancelled, None)?;
    check_eq!(
        session.begin_attempt(session.handshake_attempt(41, 0), reservation),
        Err(NetError::from(ErrorKind::Cancelled))
    )?;
    let terminal = next(&mut events).await?;
    check_eq!(terminal.sequence, 1)?;
    check!(
        matches!(terminal.kind, ConnectionEventKind::Closed { result: Err(error) } if error.context().attempt_id.is_none())
    )?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn connection_session_old_attempt_cannot_consume_current_reservation() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(6))?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        session.reserve_attempt().await?,
    )?;
    session.attempt_failed(41, 0, failure(503), retry())?;
    started(&mut events, 41, 0).await?;
    failed(&mut events).await?;
    session.begin_attempt(
        session.handshake_attempt(41, 1),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(41, 1, Some("52".to_owned()))?;
    check!(session
        .set_attempt_credential_version(41, 0, Some("999".to_owned()))
        .is_err())?;
    check!(session.established(41, 0).is_err())?;
    check!(session
        .attempt_failed(41, 0, failure(401), RetryDecision::Stop)
        .is_err())?;
    session.established(41, 1)?;
    session.terminate(TerminationReason::LocalClose, None)?;
    started(&mut events, 41, 1).await?;
    let connection = established(&mut events).await?;
    check_eq!(connection.attempt_id.as_u64(), 1)?;
    check_eq!(connection.credential_version.as_deref(), Some("52"))?;
    disconnected(&mut events).await?;
    check_eq!(
        closed(&mut events).await??.reason,
        TerminationReason::LocalClose
    )?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn connection_session_old_handle_drop_does_not_cancel_replacement_session() -> TestResult {
    let (old_session, old_events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    let (new_session, new_events) = ConnectionSession::journal_fixture(11, 22, journal(4))?;
    old_session.terminate(TerminationReason::Cancelled, None)?;
    drop(old_events);
    check!(!new_session.cancel_token().is_cancelled())?;
    drop(new_events);
    check!(!new_session.cancel_token().is_cancelled())?;
    Ok(())
}

#[test]
fn connection_options_accept_empty_headers_and_redact_secrets() -> TestResult {
    let mut options = ConnectOptions::new("ws://localhost/secret?token=secret");
    options.validate()?;
    options.headers.insert(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_static("Bearer secret"),
    );
    options.validate()?;
    check!(!format!("{options:?}").contains("secret"))?;
    check!(!format!("{:?}", HandshakeHeaders::new(options.headers)).contains("secret"))?;
    for capacity in [3, usize::MAX] {
        check_eq!(
            journal(capacity).validate(),
            Err(NetError::from(ErrorKind::InvalidConfig))
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn connection_session_receive_future_cancellation_preserves_next_event() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    let mut receiving = Box::pin(events.recv());
    require_pending(receiving.as_mut()).await?;
    session.terminate(TerminationReason::Cancelled, None)?;
    drop(receiving);
    let terminal = next(&mut events).await?;
    check_eq!(terminal.sequence, 1)?;
    check!(matches!(terminal.kind, ConnectionEventKind::Closed { .. }))?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn connection_session_metadata_is_immutable_and_foreign_reservations_are_rejected(
) -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    let (other_session, _other_events) = ConnectionSession::journal_fixture(11, 22, journal(4))?;
    check_eq!(
        session.begin_attempt(
            session.handshake_attempt(41, 0),
            other_session.reserve_attempt().await?
        ),
        Err(NetError::from(ErrorKind::InvalidConfig))
    )?;
    let mut wrong_identity = session.handshake_attempt(41, 0);
    wrong_identity.client_id = ClientId::from_allocated(12);
    check_eq!(
        session.begin_attempt(wrong_identity, session.reserve_attempt().await?),
        Err(NetError::from(ErrorKind::InvalidConfig))
    )?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(41, 0, Some("51".to_owned()))?;
    check_eq!(
        session.set_attempt_credential_version(41, 0, Some("52".to_owned())),
        Err(NetError::from(ErrorKind::InvalidConfig))
    )?;
    session.established(41, 0)?;
    check!(session.established(41, 0).is_err())?;
    started(&mut events, 41, 0).await?;
    check_eq!(
        established(&mut events)
            .await?
            .credential_version
            .as_deref(),
        Some("51")
    )?;
    session.terminate(TerminationReason::Cancelled, None)?;
    Ok(())
}

#[tokio::test]
async fn connection_session_auto_reconnect_releases_physical_reservation_and_keeps_session(
) -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(41, 0, Some("51".to_owned()))?;
    session.established(41, 0)?;
    started(&mut events, 41, 0).await?;
    let first = established(&mut events).await?;
    let io_failure = ConnectionFailure::new(
        NetError::from(ErrorKind::Io),
        ConnectStage::WebSocketIo,
        None,
        true,
    );
    session.connection_terminated_with_details(
        TerminationReason::IoFailure,
        Some(io_failure),
        ConnectionTerminationDetails::default(),
        Some(42),
    )?;
    check!(!session.cancel_token().is_cancelled())?;
    let mut waiting = Box::pin(session.reserve_attempt());
    require_pending(waiting.as_mut()).await?;
    let (ended, end) = disconnected(&mut events).await?;
    check_eq!(ended.connection_id, first.connection_id)?;
    check_eq!(ended.connected_at, first.connected_at)?;
    check_eq!(end.error.as_ref().map(NetError::kind), Some(ErrorKind::Io))?;
    session.begin_attempt(session.handshake_attempt(42, 0), waiting.await?)?;
    session.set_attempt_credential_version(42, 0, Some("52".to_owned()))?;
    session.established(42, 0)?;
    session.terminate(TerminationReason::LocalClose, None)?;
    started(&mut events, 42, 0).await?;
    let second = established(&mut events).await?;
    check!(first.connection_id != second.connection_id)?;
    check_eq!(second.session_id, first.session_id)?;
    check_eq!(second.credential_version.as_deref(), Some("52"))?;
    disconnected(&mut events).await?;
    check_eq!(
        closed(&mut events).await??.reason,
        TerminationReason::LocalClose
    )?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn connection_session_sequence_exhaustion_is_reported_without_wrapping_or_hanging_cancel(
) -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        session.reserve_attempt().await?,
    )?;
    session.lock_state()?.last_sequence = u64::MAX;
    check_eq!(
        session.attempt_failed(41, 0, failure(401), RetryDecision::Stop),
        Err(NetError::from(ErrorKind::ResourceExhausted))
    )?;
    check!(session.cancel_token().is_cancelled())?;
    check_eq!(
        session.terminate(TerminationReason::ConnectFailed, Some(failure(401))),
        Err(NetError::from(ErrorKind::ResourceExhausted))
    )?;
    started(&mut events, 41, 0).await?;
    check!(
        matches!(events.recv().await, Err(crate::error::ReceiveError::Failed(error)) if error.kind() == ErrorKind::ResourceExhausted)
    )?;
    check_eq!(
        session.closed().await.map(|_| ()),
        Err(NetError::from(ErrorKind::ResourceExhausted))
    )?;
    check!(
        matches!(session.terminal_result(), Some(Err(error)) if error.kind() == ErrorKind::ResourceExhausted)
    )?;
    Ok(())
}

#[tokio::test]
async fn connection_session_cancel_waits_for_worker_cleanup_even_with_an_empty_journal(
) -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    session.request_cancel();
    let mut cancelling = Box::pin(session.closed());
    require_pending(cancelling.as_mut()).await?;
    check!(session.cancel_token().is_cancelled())?;
    check!(!session.completion_token().is_cancelled())?;
    session.terminate(TerminationReason::Cancelled, None)?;
    check!(matches!(cancelling.await, Err(error) if error.kind() == ErrorKind::Cancelled))?;
    check!(session.completion_token().is_cancelled())?;
    check!(closed(&mut events).await?.is_err())?;
    Ok(())
}

#[tokio::test]
async fn connection_session_success_decision_before_cancel_commits_before_termination() -> TestResult
{
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(41, 0, Some("51".to_owned()))?;
    check_eq!(
        session.commit_established(41, 0),
        Err(NetError::from(ErrorKind::InvalidConfig))
    )?;
    session.prepare_established(41, 0)?;
    check!(session
        .attempt_failed(41, 0, failure(401), RetryDecision::Stop)
        .is_err())?;
    session.request_cancel();
    session.commit_established(41, 0)?;
    session.terminate(TerminationReason::Cancelled, None)?;
    started(&mut events, 41, 0).await?;
    established(&mut events).await?;
    disconnected(&mut events).await?;
    check!(closed(&mut events).await?.is_err())?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn connection_session_cancel_before_success_decision_prevents_established() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(41, 0, Some("51".to_owned()))?;
    session.request_cancel();
    check_eq!(
        session.prepare_established(41, 0),
        Err(NetError::from(ErrorKind::Cancelled))
    )?;
    check_eq!(
        session.commit_established(41, 0),
        Err(NetError::from(ErrorKind::InvalidConfig))
    )?;
    session.terminate(TerminationReason::Cancelled, None)?;
    started(&mut events, 41, 0).await?;
    failed(&mut events).await?;
    check!(closed(&mut events).await?.is_err())?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn error_source_drop_reenters_session_after_sequence_exhaustion() -> TestResult {
    for scenario in 0..3 {
        let (session, _events) = ConnectionSession::journal_fixture(11, 21, journal(4))?;
        session.begin_attempt(
            session.handshake_attempt(41, 0),
            session.reserve_attempt().await?,
        )?;
        if scenario == 1 {
            session.set_attempt_credential_version(41, 0, Some("51".to_owned()))?;
            session.established(41, 0)?;
        }
        session.lock_state()?.last_sequence = u64::MAX;
        let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::clone(&unlocked);
        let original = Arc::downgrade(&session);
        let source = crate::module::ws_client::test_support::error_with_drop_probe(move || {
            let can_reenter = original
                .upgrade()
                .is_some_and(|session| session.state.try_lock().is_ok());
            observed.store(can_reenter, std::sync::atomic::Ordering::SeqCst);
        });
        let failure = ConnectionFailure::new(source, ConnectStage::WebSocketIo, None, false);
        let result = match scenario {
            0 => session.attempt_failed(41, 0, failure, RetryDecision::Stop),
            1 => session.connection_terminated(TerminationReason::IoFailure, Some(failure)),
            _ => session.terminate(TerminationReason::ConnectFailed, Some(failure)),
        };
        check_eq!(result, Err(NetError::from(ErrorKind::ResourceExhausted)))?;
        check!(
            unlocked.load(std::sync::atomic::Ordering::SeqCst),
            "source dropped under session lock in scenario {scenario}"
        )?;
    }
    Ok(())
}

use super::*;
use crate::api::wsc::web_socket_connection_event::{
    WebSocketConnectStage, WebSocketConnectionEventKind,
};
use crate::api::wsc::web_socket_context_connect_options::WebSocketContextConnectOptions;
use crate::api::wsc::web_socket_handshake_context::WebSocketHandshakeSnapshot;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

fn attempt(cycle: u64, id: u64) -> WebSocketHandshakeAttempt {
    WebSocketHandshakeAttempt::new(11, 21, cycle, id, 31)
}

fn failure(status: u16) -> WebSocketConnectionFailure {
    WebSocketConnectionFailure::new(
        NetError::ConnectError,
        WebSocketConnectStage::WebSocketUpgrade,
        Some(status),
        status >= 500,
    )
}

async fn require_pending<F: Future>(mut future: Pin<&mut F>) -> TestResult {
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(Ok(())),
        Poll::Ready(_) => Poll::Ready(Err(test_error(
            "operation unexpectedly completed instead of applying backpressure",
        ))),
    })
    .await
}

async fn next(events: &mut WebSocketConnectionEvents) -> TestResult<WebSocketConnectionEvent> {
    tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await??
        .ok_or_else(|| test_error("event stream ended before the expected fact"))
}

#[test]
fn connection_session_is_created_without_runtime_and_rejects_invalid_capacity() -> TestResult {
    for capacity in [0, 1, 2, 4097, usize::MAX] {
        check!(matches!(
            ConnectionSession::new(11, 21, 31, capacity),
            Err(NetError::ConfigError)
        ))?;
    }
    let (session, events) = ConnectionSession::new(11, 21, 31, 3)?;
    check!(!session.cancel_token().is_cancelled())?;
    drop(events);
    check!(session.cancel_token().is_cancelled())?;
    Ok(())
}

#[test]
fn request_scope_revokes_child_sessions_but_session_cancel_keeps_siblings() -> TestResult {
    let scope = RequestScope::new();
    let (first, first_events) = ConnectionSession::new_with_scope(11, 21, 31, 3, Some(&scope))?;
    let (second, second_events) = ConnectionSession::new_with_scope(11, 22, 31, 3, Some(&scope))?;
    first.request_cancel();
    check!(first.cancel_token().is_cancelled())?;
    check!(!scope.is_cancelled())?;
    check!(!second.cancel_token().is_cancelled())?;
    scope.cancel();
    check!(second.cancel_token().is_cancelled())?;
    check!(matches!(
        ConnectionSession::new_with_scope(11, 23, 31, 3, Some(&scope)),
        Err(NetError::Cancelled)
    ))?;
    drop((first_events, second_events));
    Ok(())
}

#[tokio::test]
async fn request_scope_cancel_rejects_prepared_handshake_and_capacity_waiter() -> TestResult {
    let scope = RequestScope::new();
    let (session, events) = ConnectionSession::new_with_scope(11, 21, 31, 3, Some(&scope))?;
    session.begin_attempt(attempt(41, 0), session.reserve_attempt().await?)?;
    session.set_attempt_context(41, 0, 51)?;
    let mut waiter = Box::pin(session.reserve_attempt());
    require_pending(waiter.as_mut()).await?;
    scope.cancel();
    check_eq!(session.prepare_established(41, 0), Err(NetError::Cancelled))?;
    check!(matches!(
        tokio::time::timeout(Duration::from_secs(1), waiter).await?,
        Err(NetError::Cancelled)
    ))?;
    drop(events);
    Ok(())
}

#[tokio::test]
async fn connection_session_failed_attempt_preserves_context_and_actual_retry_decision(
) -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    let reservation = session.reserve_attempt().await?;
    session.begin_attempt(attempt(41, 0), reservation)?;
    session.set_attempt_context(41, 0, 51)?;
    session.attempt_failed(41, 0, failure(503), false)?;
    session.terminate(
        WebSocketTerminationReason::RetryExhausted,
        Some(failure(503)),
    )?;
    let event = next(&mut events).await?;
    check_eq!(event.kind(), WebSocketConnectionEventKind::AttemptFailed)?;
    check_eq!(event.sequence(), 1)?;
    check_eq!(event.client_instance_id(), 11)?;
    check_eq!(event.session_id(), 21)?;
    check_eq!(event.cycle_id(), Some(41))?;
    check_eq!(event.attempt_id(), Some(0))?;
    check_eq!(event.session_context_id(), 31)?;
    check_eq!(event.attempt_context_id(), Some(51))?;
    check_eq!(event.failure(), Some(failure(503)))?;
    check!(!event.will_retry())?;
    let terminal = next(&mut events).await?;
    check_eq!(
        terminal.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    check_eq!(terminal.sequence(), 2)?;
    check_eq!(terminal.attempt_context_id(), Some(51))?;
    check_eq!(
        terminal.termination_reason(),
        Some(WebSocketTerminationReason::RetryExhausted)
    )?;
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

#[tokio::test]
async fn connection_session_full_journal_pauses_only_the_next_attempt() -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    session.begin_attempt(attempt(41, 0), session.reserve_attempt().await?)?;
    session.attempt_failed(41, 0, failure(503), true)?;
    let mut waiting = Box::pin(session.reserve_attempt());
    require_pending(waiting.as_mut()).await?;
    let first = next(&mut events).await?;
    check!(first.will_retry())?;
    let reservation = waiting.await?;
    session.begin_attempt(attempt(41, 1), reservation)?;
    session.set_attempt_context(41, 1, 52)?;
    session.established(41, 1)?;
    session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    let established = next(&mut events).await?;
    let disconnected = next(&mut events).await?;
    let terminal = next(&mut events).await?;
    check_eq!(
        established.kind(),
        WebSocketConnectionEventKind::Established
    )?;
    check_eq!(established.attempt_context_id(), Some(52))?;
    check_eq!(
        disconnected.kind(),
        WebSocketConnectionEventKind::ConnectionTerminated
    )?;
    check_eq!(
        terminal.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    check_eq!(terminal.sequence(), 4)?;
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

#[tokio::test]
async fn connection_session_cancel_finishes_attempt_before_final_event_and_rejects_late_success(
) -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    session.begin_attempt(attempt(41, 0), session.reserve_attempt().await?)?;
    session.set_attempt_context(41, 0, 51)?;
    session.request_cancel();
    session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    session.terminate(WebSocketTerminationReason::Shutdown, None)?;
    check!(session.established(41, 0).is_err())?;
    check!(session.attempt_failed(41, 0, failure(401), false).is_err())?;
    events.cancel().await?;
    let cancelled = next(&mut events).await?;
    check_eq!(
        cancelled.kind(),
        WebSocketConnectionEventKind::AttemptFailed
    )?;
    check_eq!(
        cancelled.failure().map(|item| item.error()),
        Some(NetError::Cancelled)
    )?;
    check_eq!(cancelled.attempt_context_id(), Some(51))?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

#[tokio::test]
async fn connection_session_cancels_reservation_wait_without_consumer_progress() -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    session.begin_attempt(attempt(41, 0), session.reserve_attempt().await?)?;
    session.attempt_failed(41, 0, failure(503), true)?;
    let mut waiting = Box::pin(session.reserve_attempt());
    require_pending(waiting.as_mut()).await?;
    session.request_cancel();
    check!(matches!(waiting.await, Err(NetError::Cancelled)))?;
    session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::AttemptFailed
    )?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

#[tokio::test]
async fn connection_session_cancellation_before_start_uses_only_session_slot() -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    let reservation = session.reserve_attempt().await?;
    session.request_cancel();
    session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    check!(matches!(
        session.begin_attempt(attempt(41, 0), reservation),
        Err(NetError::Cancelled)
    ))?;
    let terminal = next(&mut events).await?;
    check_eq!(
        terminal.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    check_eq!(terminal.attempt_id(), None)?;
    check_eq!(terminal.attempt_context_id(), None)?;
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

#[tokio::test]
async fn connection_session_old_attempt_cannot_consume_current_reservation() -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 4)?;
    session.begin_attempt(attempt(41, 0), session.reserve_attempt().await?)?;
    session.attempt_failed(41, 0, failure(503), true)?;
    session.begin_attempt(attempt(41, 1), session.reserve_attempt().await?)?;
    session.set_attempt_context(41, 1, 52)?;
    check!(session.set_attempt_context(41, 0, 999).is_err())?;
    check!(session.established(41, 0).is_err())?;
    check!(session.attempt_failed(41, 0, failure(401), false).is_err())?;
    session.established(41, 1)?;
    session.terminate(WebSocketTerminationReason::Disconnected, None)?;
    let _ = next(&mut events).await?;
    let established = next(&mut events).await?;
    check_eq!(established.attempt_id(), Some(1))?;
    check_eq!(established.attempt_context_id(), Some(52))?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::ConnectionTerminated
    )?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

#[tokio::test]
async fn connection_session_old_handle_drop_does_not_cancel_replacement_session() -> TestResult {
    let (old_session, old_events) = ConnectionSession::new(11, 21, 31, 3)?;
    let (new_session, new_events) = ConnectionSession::new(11, 22, 32, 3)?;
    old_session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    drop(old_events);
    check!(!new_session.cancel_token().is_cancelled())?;
    drop(new_events);
    check!(new_session.cancel_token().is_cancelled())?;
    Ok(())
}

#[test]
fn connection_options_require_explicit_static_context_and_redact_secrets() -> TestResult {
    let missing = WebSocketContextConnectOptions::new("ws://localhost/secret?token=secret", 1);
    check_eq!(missing.validate(), Err(NetError::ConfigError))?;
    let options = missing.with_headers(vec![("authorization".into(), "Bearer secret".into())], 2);
    options.validate()?;
    check!(!format!("{options:?}").contains("secret"))?;
    let snapshot =
        WebSocketHandshakeSnapshot::new(vec![("authorization".into(), "Bearer secret".into())], 2);
    check!(!format!("{snapshot:?}").contains("secret"))?;
    for capacity in [2, 4097] {
        check_eq!(
            options.clone().with_event_capacity(capacity).validate(),
            Err(NetError::ConfigError)
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn connection_session_receive_future_cancellation_preserves_next_event() -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    let mut receiving = Box::pin(events.recv());
    require_pending(receiving.as_mut()).await?;
    drop(receiving);
    session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    let terminal = next(&mut events).await?;
    check_eq!(terminal.sequence(), 1)?;
    check_eq!(
        terminal.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

#[tokio::test]
async fn connection_session_metadata_is_immutable_and_foreign_reservations_are_rejected(
) -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    let (other_session, _other_events) = ConnectionSession::new(11, 22, 32, 3)?;
    let foreign = other_session.reserve_attempt().await?;
    check_eq!(
        session.begin_attempt(attempt(41, 0), foreign),
        Err(NetError::ConfigError)
    )?;
    let wrong_identity = WebSocketHandshakeAttempt::new(12, 21, 41, 0, 31);
    check_eq!(
        session.begin_attempt(wrong_identity, session.reserve_attempt().await?),
        Err(NetError::ConfigError)
    )?;
    session.begin_attempt(attempt(41, 0), session.reserve_attempt().await?)?;
    session.set_attempt_context(41, 0, 51)?;
    check_eq!(
        session.set_attempt_context(41, 0, 52),
        Err(NetError::ConfigError)
    )?;
    session.established(41, 0)?;
    check!(session.established(41, 0).is_err())?;
    check_eq!(next(&mut events).await?.attempt_context_id(), Some(51))?;
    session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    Ok(())
}

#[tokio::test]
async fn connection_session_auto_reconnect_releases_physical_reservation_and_keeps_session(
) -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    session.begin_attempt(attempt(41, 0), session.reserve_attempt().await?)?;
    session.set_attempt_context(41, 0, 51)?;
    session.established(41, 0)?;
    let established = next(&mut events).await?;
    check_eq!(established.cycle_id(), Some(41))?;
    let io_failure = WebSocketConnectionFailure::new(
        NetError::NetworkError,
        WebSocketConnectStage::WebSocketIo,
        None,
        true,
    );
    session.connection_terminated(WebSocketTerminationReason::IoFailure, Some(io_failure))?;
    check!(!session.cancel_token().is_cancelled())?;
    let mut waiting = Box::pin(session.reserve_attempt());
    require_pending(waiting.as_mut()).await?;
    let disconnected = next(&mut events).await?;
    check_eq!(
        disconnected.kind(),
        WebSocketConnectionEventKind::ConnectionTerminated
    )?;
    check_eq!(disconnected.failure(), Some(io_failure))?;
    session.begin_attempt(attempt(42, 0), waiting.await?)?;
    session.set_attempt_context(42, 0, 52)?;
    session.established(42, 0)?;
    session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    let reconnected = next(&mut events).await?;
    check_eq!(reconnected.cycle_id(), Some(42))?;
    check_eq!(reconnected.attempt_context_id(), Some(52))?;
    let _ = next(&mut events).await?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

#[tokio::test]
async fn connection_session_sequence_exhaustion_is_reported_without_wrapping_or_hanging_cancel(
) -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    session.begin_attempt(attempt(41, 0), session.reserve_attempt().await?)?;
    session.lock_state()?.last_sequence = u64::MAX;
    check_eq!(
        session.attempt_failed(41, 0, failure(401), false),
        Err(NetError::InternalError)
    )?;
    check!(session.cancel_token().is_cancelled())?;
    check_eq!(
        session.terminate(
            WebSocketTerminationReason::ConnectFailed,
            Some(failure(401))
        ),
        Err(NetError::InternalError)
    )?;
    check_eq!(events.recv().await, Err(NetError::InternalError))?;
    check_eq!(events.cancel().await, Err(NetError::InternalError))?;
    Ok(())
}

#[tokio::test]
async fn connection_session_cancel_waits_for_worker_cleanup_even_with_an_empty_journal(
) -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    let mut cancelling = Box::pin(events.cancel());
    require_pending(cancelling.as_mut()).await?;
    check!(session.cancel_token().is_cancelled())?;
    check!(!session.completion_token().is_cancelled())?;
    session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    cancelling.await?;
    check!(session.completion_token().is_cancelled())?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    Ok(())
}

#[tokio::test]
async fn connection_session_success_decision_before_cancel_commits_before_termination() -> TestResult
{
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    session.begin_attempt(attempt(41, 0), session.reserve_attempt().await?)?;
    session.set_attempt_context(41, 0, 51)?;
    check_eq!(
        session.commit_established(41, 0),
        Err(NetError::ConfigError)
    )?;
    session.prepare_established(41, 0)?;
    check!(session.attempt_failed(41, 0, failure(401), false).is_err())?;
    session.request_cancel();
    session.commit_established(41, 0)?;
    session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::Established
    )?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::ConnectionTerminated
    )?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

#[tokio::test]
async fn connection_session_cancel_before_success_decision_prevents_established() -> TestResult {
    let (session, mut events) = ConnectionSession::new(11, 21, 31, 3)?;
    session.begin_attempt(attempt(41, 0), session.reserve_attempt().await?)?;
    session.set_attempt_context(41, 0, 51)?;
    session.request_cancel();
    check_eq!(session.prepare_established(41, 0), Err(NetError::Cancelled))?;
    check_eq!(
        session.commit_established(41, 0),
        Err(NetError::ConfigError)
    )?;
    session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::AttemptFailed
    )?;
    check_eq!(
        next(&mut events).await?.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

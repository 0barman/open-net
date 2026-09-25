use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};

#[tokio::test]
async fn four_reserved_events_finish_without_consumer_progress() -> TestResult {
    let (session, mut events) = ConnectionSession::journal_fixture_with_metadata(
        1,
        2,
        crate::ws::JournalOptions {
            max_events: 4,
            max_bytes: 4 * crate::ws::MAX_CONNECTION_EVENT_BYTES,
        },
        Arc::new(crate::Metadata::new()),
    )?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(4, 0, None)?;
    session.prepare_established(4, 0)?;
    session.commit_established(4, 0)?;
    session.terminate(crate::ws::TerminationReason::LocalClose, None)?;
    let result = session
        .terminal_result()
        .ok_or_else(|| test_error("terminal result depends on consuming the journal"))?;
    check!(result.is_ok())?;
    let mut kinds = Vec::new();
    while let Some(event) = events.recv().await? {
        kinds.push(match event.kind {
            crate::ws::ConnectionEventKind::AttemptStarted { .. } => 0,
            crate::ws::ConnectionEventKind::Established { .. } => 1,
            crate::ws::ConnectionEventKind::Disconnected { .. } => 2,
            crate::ws::ConnectionEventKind::Closed { .. } => 3,
            _ => return Err(test_error("unexpected four-slot journal fact")),
        });
    }
    check_eq!(kinds, vec![0, 1, 2, 3])?;
    check!(session
        .terminal_result()
        .is_some_and(|result| result.is_ok()))?;
    Ok(())
}

#[tokio::test]
async fn actual_event_bytes_replace_reservation_and_release_on_receive() -> TestResult {
    use super::tests::{journal, next};
    use crate::module::transport::failure::ConnectStage;
    let (session, mut events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    let total = 4 * MAX_CONNECTION_EVENT_BYTES;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .bytes
            .available_permits(),
        total - MAX_CONNECTION_EVENT_BYTES
    )?;
    let reservation = session.reserve_attempt().await?;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .bytes
            .available_permits(),
        0
    )?;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .slots
            .available_permits(),
        0
    )?;
    session.begin_attempt(session.handshake_attempt(4, 0), reservation)?;
    let mut history = session.subscribe_events()?;
    let started_size = history.try_recv()?.measured_size()?;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .bytes
            .available_permits(),
        MAX_CONNECTION_EVENT_BYTES - started_size
    )?;
    session.attempt_failed(
        4,
        0,
        ConnectionFailure::new(
            NetError::from(ErrorKind::Io),
            ConnectStage::Tcp,
            None,
            false,
        ),
        RetryDecision::Stop,
    )?;
    let failed_size = history.try_recv()?.measured_size()?;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .bytes
            .available_permits(),
        3 * MAX_CONNECTION_EVENT_BYTES - started_size - failed_size
    )?;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .slots
            .available_permits(),
        1
    )?;
    next(&mut events).await?;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .bytes
            .available_permits(),
        3 * MAX_CONNECTION_EVENT_BYTES - failed_size
    )?;
    next(&mut events).await?;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .bytes
            .available_permits(),
        3 * MAX_CONNECTION_EVENT_BYTES
    )?;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .slots
            .available_permits(),
        3
    )?;
    session.terminate(TerminationReason::Cancelled, None)?;
    let terminal = next(&mut events).await?;
    check!(matches!(terminal.kind, ConnectionEventKind::Closed { .. }))?;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .bytes
            .available_permits(),
        total
    )?;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .slots
            .available_permits(),
        4
    )?;
    Ok(())
}

#[tokio::test]
async fn byte_budget_waits_even_when_event_slots_are_available_and_cancel_returns_partial_reservation(
) -> TestResult {
    use super::tests::{next, require_pending};
    use crate::module::transport::failure::ConnectStage;
    for cancel in [false, true] {
        let (session, mut events) = ConnectionSession::journal_fixture(
            1,
            2,
            JournalOptions {
                max_events: 16,
                max_bytes: 4 * MAX_CONNECTION_EVENT_BYTES,
            },
        )?;
        session.begin_attempt(
            session.handshake_attempt(4, 0),
            session.reserve_attempt().await?,
        )?;
        session.attempt_failed(
            4,
            0,
            ConnectionFailure::new(NetError::from(ErrorKind::Io), ConnectStage::Tcp, None, true),
            RetryDecision::Scheduled {
                after: std::time::Duration::ZERO,
            },
        )?;
        check_eq!(
            session
                .journal
                .as_ref()
                .ok_or_else(|| test_error("journal budget missing"))?
                .slots
                .available_permits(),
            13
        )?;
        let mut waiting = Box::pin(session.reserve_attempt());
        require_pending(waiting.as_mut()).await?;
        check_eq!(
            session
                .journal
                .as_ref()
                .ok_or_else(|| test_error("journal budget missing"))?
                .slots
                .available_permits(),
            10
        )?;
        if cancel {
            drop(waiting);
            check_eq!(
                session
                    .journal
                    .as_ref()
                    .ok_or_else(|| test_error("journal budget missing"))?
                    .slots
                    .available_permits(),
                13
            )?;
        } else {
            next(&mut events).await?;
            require_pending(waiting.as_mut()).await?;
            next(&mut events).await?;
            let reservation = waiting.await?;
            check_eq!(
                session
                    .journal
                    .as_ref()
                    .ok_or_else(|| test_error("journal budget missing"))?
                    .bytes
                    .available_permits(),
                0
            )?;
            drop(reservation);
            check_eq!(
                session
                    .journal
                    .as_ref()
                    .ok_or_else(|| test_error("journal budget missing"))?
                    .bytes
                    .available_permits(),
                3 * MAX_CONNECTION_EVENT_BYTES
            )?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn closed_failure_retains_the_same_physical_error_context_and_source() -> TestResult {
    use super::tests::{journal, next};
    use crate::module::transport::failure::ConnectStage;
    use std::error::Error;
    let (session, mut events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(4, 0, None)?;
    session.established(4, 0)?;
    let original = NetError::from(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "physical reset",
    ));
    let failure = ConnectionFailure::new(original.clone(), ConnectStage::WebSocketIo, None, false);
    let details = ConnectionTerminationDetails {
        peer_close: Some(crate::ws::PeerClose {
            code: Some(1002),
            reason: "bad frame".to_owned(),
        }),
        io_end_kind: Some(crate::ws::IoEndKind::ConnectionReset),
    };
    session.connection_terminated_with_details(
        TerminationReason::IoFailure,
        Some(failure.clone()),
        details,
        None,
    )?;
    session.terminate(TerminationReason::IoFailure, Some(failure))?;
    let terminal = session
        .terminal_result()
        .ok_or_else(|| test_error("missing independent terminal"))?;
    let Err(error) = terminal else {
        return Err(test_error("reset reported successful session"));
    };
    check_eq!(
        error.context().connection_id,
        Some(ConnectionId::from_allocated(4))
    )?;
    check_eq!(
        error
            .context()
            .peer_close
            .as_ref()
            .map(|peer| peer.reason.as_str()),
        Some("bad frame")
    )?;
    check_eq!(
        error.context().io_end,
        Some(crate::ws::IoEndKind::ConnectionReset)
    )?;
    check!(std::ptr::eq(
        error
            .source()
            .ok_or_else(|| test_error("terminal source missing"))?,
        original
            .source()
            .ok_or_else(|| test_error("original source missing"))?
    ))?;
    for _ in 0..3 {
        next(&mut events).await?;
    }
    let ConnectionEventKind::Closed {
        result: Err(journal_error),
    } = next(&mut events).await?.kind
    else {
        return Err(test_error("journal terminal missing failure"));
    };
    check_eq!(
        journal_error.context().connection_id,
        error.context().connection_id
    )?;
    check_eq!(journal_error.context().io_end, error.context().io_end)?;
    Ok(())
}

#[tokio::test]
async fn closed_failure_preserves_current_attempt_diagnostic_without_reusing_old_connection(
) -> TestResult {
    use super::tests::{journal, next};
    use crate::module::transport::failure::ConnectStage;
    let (session, mut events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(4, 0, None)?;
    session.established(4, 0)?;
    session.connection_terminated_with_details(
        TerminationReason::IoFailure,
        Some(ConnectionFailure::new(
            NetError::from(ErrorKind::Io),
            ConnectStage::WebSocketIo,
            None,
            true,
        )),
        ConnectionTerminationDetails::default(),
        Some(5),
    )?;
    for _ in 0..3 {
        next(&mut events).await?;
    }
    session.begin_attempt(
        session.handshake_attempt(5, 0),
        session.reserve_attempt().await?,
    )?;
    let failure = ConnectionFailure::new(
        NetError::from(ErrorKind::HandshakeRejected),
        ConnectStage::WebSocketUpgrade,
        Some(401),
        false,
    );
    session.attempt_failed_with_diagnostic(
        5,
        0,
        failure.clone(),
        RetryDecision::Stop,
        Some(HandshakeDiagnostic::new(
            crate::ws::HandshakeDiagnosticKind::HttpRejected,
        )),
    )?;
    session.terminate(TerminationReason::ConnectFailed, Some(failure))?;
    let Err(error) = session
        .terminal_result()
        .ok_or_else(|| test_error("terminal missing"))?
    else {
        return Err(test_error("rejected handshake was successful"));
    };
    check_eq!(error.kind(), ErrorKind::HandshakeRejected)?;
    check_eq!(error.context().connection_id, None)?;
    check_eq!(
        error
            .context()
            .diagnostic
            .as_ref()
            .map(|value| value.kind()),
        Some(crate::ws::HandshakeDiagnosticKind::HttpRejected)
    )?;
    Ok(())
}

#[tokio::test]
async fn next_attempt_retains_the_latest_failure_until_replacement_then_retires_it_outside_lock(
) -> TestResult {
    use super::tests::{journal, next};
    use crate::module::transport::failure::ConnectStage;
    use std::sync::atomic::{AtomicBool, Ordering};
    let listeners = crate::module::ws_client::listener_store::ListenerStore::new(
        &crate::ws::WebSocketClientConfig::default(),
    )?;
    let (session, events) = ConnectionSession::new_with_observers(
        1,
        2,
        Some(journal(4)),
        Arc::new(crate::Metadata::new()),
        EventOptions {
            max_events: 1,
            max_bytes: MAX_CONNECTION_EVENT_BYTES,
        },
        listeners.connection_observers(),
    )?;
    let mut events = events.ok_or_else(|| test_error("journal missing"))?;
    session.begin_cycle(4, false)?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    let dropped = Arc::new(AtomicBool::new(false));
    let unlocked = Arc::new(AtomicBool::new(false));
    let observed_drop = Arc::clone(&dropped);
    let observed_unlock = Arc::clone(&unlocked);
    let weak = Arc::downgrade(&session);
    let source = crate::module::ws_client::test_support::error_with_drop_probe(move || {
        observed_unlock.store(
            weak.upgrade()
                .is_some_and(|session| session.state.try_lock().is_ok()),
            Ordering::SeqCst,
        );
        observed_drop.store(true, Ordering::SeqCst);
    });
    session.attempt_failed(
        4,
        0,
        ConnectionFailure::new(source, ConnectStage::Provider, None, true),
        RetryDecision::Scheduled {
            after: std::time::Duration::ZERO,
        },
    )?;
    drop(next(&mut events).await?);
    drop(next(&mut events).await?);
    check!(!dropped.load(Ordering::SeqCst))?;
    session.begin_attempt(
        session.handshake_attempt(4, 1),
        session.reserve_attempt().await?,
    )?;
    check!(!dropped.load(Ordering::SeqCst))?;
    session.terminate(TerminationReason::Cancelled, None)?;
    check!(dropped.load(Ordering::SeqCst))?;
    check!(unlocked.load(Ordering::SeqCst))?;
    Ok(())
}

#[tokio::test]
async fn replacement_connection_keeps_recent_error_until_session_retirement_outside_lock(
) -> TestResult {
    use super::tests::{journal, next};
    use crate::module::transport::failure::ConnectStage;
    use std::sync::atomic::{AtomicBool, Ordering};
    let (session, mut events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(4, 0, None)?;
    session.established(4, 0)?;
    let dropped = Arc::new(AtomicBool::new(false));
    let unlocked = Arc::new(AtomicBool::new(false));
    let observed_drop = Arc::clone(&dropped);
    let observed_unlock = Arc::clone(&unlocked);
    let weak = Arc::downgrade(&session);
    let source = crate::module::ws_client::test_support::error_with_drop_probe(move || {
        observed_unlock.store(
            weak.upgrade()
                .is_none_or(|session| session.state.try_lock().is_ok()),
            Ordering::SeqCst,
        );
        observed_drop.store(true, Ordering::SeqCst);
    });
    session.connection_terminated_with_details(
        TerminationReason::IoFailure,
        Some(ConnectionFailure::new(
            source,
            ConnectStage::WebSocketIo,
            None,
            true,
        )),
        ConnectionTerminationDetails::default(),
        Some(5),
    )?;
    for _ in 0..3 {
        drop(next(&mut events).await?);
    }
    check!(!dropped.load(Ordering::SeqCst))?;
    session.begin_attempt(
        session.handshake_attempt(5, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(5, 0, None)?;
    session.established(5, 0)?;
    session.connection_terminated(TerminationReason::LocalClose, None)?;
    check!(!dropped.load(Ordering::SeqCst))?;
    check_eq!(
        session.snapshot()?.last_error.as_ref().map(NetError::kind),
        Some(ErrorKind::Io)
    )?;
    session.terminate(TerminationReason::LocalClose, None)?;
    check!(session.terminal_result().is_some_and(|result| matches!(result, Ok(end) if end.last_connection.as_ref().is_some_and(|last| last.error.is_none()))))?;
    drop(events);
    drop(session);
    check!(dropped.load(Ordering::SeqCst))?;
    check!(unlocked.load(Ordering::SeqCst))?;
    Ok(())
}

struct LockProbeWake {
    session: std::sync::Weak<ConnectionSession>,
    count: std::sync::atomic::AtomicUsize,
    locked: std::sync::atomic::AtomicBool,
}
impl std::task::Wake for LockProbeWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        use std::sync::atomic::Ordering;
        if self
            .session
            .upgrade()
            .is_some_and(|session| session.state.try_lock().is_err())
        {
            self.locked.store(true, Ordering::SeqCst);
        }
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn event_publication_and_lease_return_wake_only_outside_session_lock() -> TestResult {
    use super::tests::{journal, next};
    use crate::module::transport::failure::ConnectStage;
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};
    let (session, mut events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    let probe = Arc::new(LockProbeWake {
        session: Arc::downgrade(&session),
        count: AtomicUsize::new(0),
        locked: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut cx = Context::from_waker(&waker);
    let mut receive = Box::pin(events.recv());
    check!(matches!(receive.as_mut().poll(&mut cx), Poll::Pending))?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    check!(probe.count.load(Ordering::SeqCst) > 0)?;
    check!(!probe.locked.load(Ordering::SeqCst))?;
    drop(receive);
    session.attempt_failed(
        4,
        0,
        ConnectionFailure::new(NetError::from(ErrorKind::Io), ConnectStage::Tcp, None, true),
        RetryDecision::Scheduled {
            after: std::time::Duration::ZERO,
        },
    )?;
    let mut reserve = Box::pin(session.reserve_attempt());
    check!(matches!(reserve.as_mut().poll(&mut cx), Poll::Pending))?;
    let previous = probe.count.load(Ordering::SeqCst);
    next(&mut events).await?;
    next(&mut events).await?;
    check!(probe.count.load(Ordering::SeqCst) > previous)?;
    check!(!probe.locked.load(Ordering::SeqCst))?;
    drop(reserve.await?);
    Ok(())
}

#[tokio::test]
async fn oversized_fact_fails_explicitly_without_leaking_reserved_capacity() -> TestResult {
    use super::tests::{journal, next};
    use crate::module::transport::failure::ConnectStage;
    let (session, mut events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    let error = NetError::from(ErrorKind::Io).with_context(crate::error::ErrorContext {
        peer_close: Some(crate::ws::PeerClose {
            code: None,
            reason: "x".repeat(MAX_CONNECTION_EVENT_BYTES),
        }),
        ..Default::default()
    });
    check_eq!(
        session.attempt_failed(
            4,
            0,
            ConnectionFailure::new(error, ConnectStage::Tcp, None, false),
            RetryDecision::Stop
        ),
        Err(NetError::from(ErrorKind::ItemTooLarge))
    )?;
    check!(session.completion_token().is_cancelled())?;
    check!(
        matches!(session.terminal_result(), Some(Err(error)) if error.kind() == ErrorKind::ItemTooLarge)
    )?;
    drop(next(&mut events).await?);
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .slots
            .available_permits(),
        4
    )?;
    check_eq!(
        session
            .journal
            .as_ref()
            .ok_or_else(|| test_error("journal budget missing"))?
            .bytes
            .available_permits(),
        4 * MAX_CONNECTION_EVENT_BYTES
    )?;
    check!(
        matches!(events.recv().await, Err(crate::error::ReceiveError::Failed(error)) if error.kind() == ErrorKind::ItemTooLarge)
    )?;
    Ok(())
}

#[tokio::test]
async fn cancellation_and_engine_drop_cannot_reuse_a_historic_io_failure() -> TestResult {
    use super::tests::journal;
    use crate::module::transport::failure::ConnectStage;
    for (reason, kind) in [
        (TerminationReason::Cancelled, ErrorKind::Cancelled),
        (TerminationReason::EngineDropped, ErrorKind::EngineDropped),
    ] {
        let (session, _events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
        session.begin_attempt(
            session.handshake_attempt(4, 0),
            session.reserve_attempt().await?,
        )?;
        session.set_attempt_credential_version(4, 0, None)?;
        session.established(4, 0)?;
        session.connection_terminated(
            TerminationReason::IoFailure,
            Some(ConnectionFailure::new(
                NetError::from(ErrorKind::Io),
                ConnectStage::WebSocketIo,
                None,
                true,
            )),
        )?;
        session.terminate(reason, None)?;
        let Err(error) = session
            .terminal_result()
            .ok_or_else(|| test_error("terminal missing"))?
        else {
            return Err(test_error("abandoned session reported success"));
        };
        check_eq!(error.kind(), kind)?;
        session.terminate(TerminationReason::LocalClose, None)?;
        check!(matches!(session.terminal_result(), Some(Err(error)) if error.kind() == kind))?;
    }
    Ok(())
}

#[tokio::test]
async fn terminal_failure_keeps_its_explicit_source_when_prior_connection_error_has_same_kind(
) -> TestResult {
    use super::tests::journal;
    use crate::module::transport::failure::ConnectStage;
    use std::error::Error;
    let (session, _events) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(4, 0, None)?;
    session.established(4, 0)?;
    let first = NetError::from(std::io::Error::other("read error"));
    session.connection_terminated_with_details(
        TerminationReason::IoFailure,
        Some(ConnectionFailure::new(
            first,
            ConnectStage::WebSocketIo,
            None,
            false,
        )),
        ConnectionTerminationDetails {
            peer_close: None,
            io_end_kind: Some(crate::ws::IoEndKind::ConnectionReset),
        },
        None,
    )?;
    let terminal_source = NetError::from(std::io::Error::other("cleanup error"));
    session.terminate(
        TerminationReason::IoFailure,
        Some(ConnectionFailure::new(
            terminal_source.clone(),
            ConnectStage::WebSocketIo,
            None,
            false,
        )),
    )?;
    let Err(error) = session
        .terminal_result()
        .ok_or_else(|| test_error("terminal missing"))?
    else {
        return Err(test_error("cleanup failure became success"));
    };
    check!(std::ptr::eq(
        error
            .source()
            .ok_or_else(|| test_error("terminal source missing"))?,
        terminal_source
            .source()
            .ok_or_else(|| test_error("explicit source missing"))?
    ))?;
    check_eq!(
        error.context().connection_id,
        Some(ConnectionId::from_allocated(4))
    )?;
    check_eq!(
        error.context().io_end,
        Some(crate::ws::IoEndKind::ConnectionReset)
    )?;
    Ok(())
}

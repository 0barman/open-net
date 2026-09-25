use super::tests::{journal, next};
use super::*;
use crate::error::ReceiveError;
use crate::module::transport::failure::ConnectStage;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::ws::ConnectionState;

#[tokio::test]
async fn initial_snapshot_is_fixed_and_current_does_not_consume_it() -> TestResult {
    let (session, _owner) = ConnectionSession::journal_fixture(1, 2, journal(8))?;
    let mut states = session.watch_state()?;
    let initial = states.current();
    check_eq!(initial.revision, 0)?;
    check!(matches!(initial.state, ConnectionState::Connecting))?;
    session.begin_cycle(4, false)?;
    session.waiting_for_network(4)?;
    check!(matches!(
        states.current().state,
        ConnectionState::WaitingForNetwork
    ))?;
    let first = states
        .recv()
        .await?
        .ok_or_else(|| test_error("initial state missing"))?;
    check_eq!(first.revision, 0)?;
    check!(matches!(first.state, ConnectionState::Connecting))?;
    let latest = states
        .recv()
        .await?
        .ok_or_else(|| test_error("waiting state missing"))?;
    check!(matches!(latest.state, ConnectionState::WaitingForNetwork))?;
    check_eq!(latest.revision, session.snapshot()?.revision)?;
    Ok(())
}

#[tokio::test]
async fn journal_and_history_share_the_committed_fact_and_connection_snapshot() -> TestResult {
    let (session, mut journal) = ConnectionSession::journal_fixture(1, 2, journal(8))?;
    let mut events = session.subscribe_events()?;
    session.begin_cycle(4, false)?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(4, 0, Some("credential".to_owned()))?;
    session.prepare_established(4, 0)?;
    check!(matches!(
        session.snapshot()?.state,
        ConnectionState::Connecting
    ))?;
    session.commit_established(4, 0)?;
    for _ in 0..2 {
        let original = next(&mut journal).await?;
        let observed = events
            .recv()
            .await?
            .ok_or_else(|| test_error("history fact missing"))?;
        check_eq!(original.sequence, observed.sequence)?;
        check_eq!(original.occurred_at, observed.occurred_at)?;
        check_eq!(original.client_id, observed.client_id)?;
        check_eq!(original.session_id, observed.session_id)?;
        if let ConnectionEventKind::Established { connection } = original.kind {
            let ConnectionState::Connected(current) = session.snapshot()?.state else {
                return Err(test_error("Established exposed without Connected snapshot"));
            };
            check_eq!(connection.connection_id, current.connection_id)?;
            check_eq!(connection.connected_at, current.connected_at)?;
            check_eq!(connection.credential_version, current.credential_version)?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn progress_is_cycle_scoped_and_closing_prevents_late_updates() -> TestResult {
    let (session, _owner) = ConnectionSession::journal_fixture(1, 2, journal(8))?;
    session.begin_cycle(4, false)?;
    session.waiting_for_network(4)?;
    session.preparing_attempt(4, None)?;
    check!(matches!(
        session.snapshot()?.state,
        ConnectionState::Connecting
    ))?;
    session.begin_cycle(5, true)?;
    let until = std::time::Instant::now();
    session.preparing_attempt(5, Some(until))?;
    check!(
        matches!(session.snapshot()?.state, ConnectionState::Reconnecting { cycle_id, next_attempt_at: Some(value) } if cycle_id.as_u64() == 5 && value == until)
    )?;
    check_eq!(
        session
            .waiting_for_network(4)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Cancelled)
    )?;
    session.begin_closing()?;
    check_eq!(
        session
            .preparing_attempt(5, None)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Cancelled)
    )?;
    check_eq!(
        session.begin_cycle(6, true).err().map(|error| error.kind()),
        Some(ErrorKind::Cancelled)
    )?;
    check!(matches!(
        session.snapshot()?.state,
        ConnectionState::Closing
    ))?;
    Ok(())
}

#[tokio::test]
async fn latest_annotated_failure_survives_a_successful_retry() -> TestResult {
    let (session, _owner) = ConnectionSession::journal_fixture(1, 2, journal(12))?;
    session.begin_cycle(4, false)?;
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
    session.preparing_attempt(4, None)?;
    session.begin_attempt(
        session.handshake_attempt(4, 1),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(4, 1, None)?;
    session.established(4, 1)?;
    let snapshot = session.snapshot()?;
    check!(matches!(snapshot.state, ConnectionState::Connected(_)))?;
    let error = snapshot
        .last_error
        .ok_or_else(|| test_error("recovery erased the most recent failure"))?;
    check_eq!(error.kind(), ErrorKind::Io)?;
    check_eq!(error.context().attempt_id.map(|id| id.as_u64()), Some(0))?;
    Ok(())
}

#[tokio::test]
async fn business_failure_is_a_final_snapshot_value_then_fused_end() -> TestResult {
    let (session, _owner) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    let mut states = session.watch_state()?;
    states.recv().await?;
    session.terminate(TerminationReason::Cancelled, None)?;
    let final_value = states
        .recv()
        .await?
        .ok_or_else(|| test_error("final state missing"))?;
    check!(
        matches!(final_value.state, ConnectionState::Closed(Err(ref error)) if error.kind() == ErrorKind::Cancelled)
    )?;
    check!(states.recv().await?.is_none())?;
    check!(states.recv().await?.is_none())?;
    check_eq!(
        session.closed().await.err().map(|error| error.kind()),
        Some(ErrorKind::Cancelled)
    )?;
    Ok(())
}

#[tokio::test]
async fn history_failure_does_not_fail_the_journal_or_session_result() -> TestResult {
    let (session, mut journal) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    let mut events = session.subscribe_events()?;
    session.fail_history_allocation_for_test();
    session.begin_cycle(4, false)?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    check!(
        matches!(events.recv().await, Err(ReceiveError::Failed(ref error)) if error.kind() == ErrorKind::ResourceExhausted)
    )?;
    check!(events.recv().await?.is_none())?;
    check!(matches!(
        next(&mut journal).await?.kind,
        ConnectionEventKind::AttemptStarted { .. }
    ))?;
    session.terminate(TerminationReason::LocalClose, None)?;
    check_eq!(
        session.closed().await?.reason,
        TerminationReason::LocalClose
    )?;
    Ok(())
}

#[tokio::test]
async fn state_source_failure_does_not_fail_the_journal_or_session_result() -> TestResult {
    let (session, mut journal) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    let mut states = session.watch_state()?;
    states.recv().await?;
    session.fail_state_publication_for_test();
    session.begin_cycle(4, false)?;
    session.waiting_for_network(4)?;
    let last = states
        .recv()
        .await?
        .ok_or_else(|| test_error("source failure erased its last snapshot"))?;
    check_eq!(last.revision, 0)?;
    check_eq!(
        states.recv().await.err().map(|error| error.kind()),
        Some(ErrorKind::ResourceExhausted)
    )?;
    check!(states.recv().await?.is_none())?;
    check_eq!(
        session.snapshot().err().map(|error| error.kind()),
        Some(ErrorKind::ResourceExhausted)
    )?;
    session.terminate(TerminationReason::LocalClose, None)?;
    check_eq!(
        session.closed().await?.reason,
        TerminationReason::LocalClose
    )?;
    check!(matches!(
        next(&mut journal).await?.kind,
        ConnectionEventKind::Closed { result: Ok(_) }
    ))?;
    Ok(())
}

#[tokio::test]
async fn snapshot_and_state_seed_wait_for_the_history_transaction_to_finish() -> TestResult {
    use std::sync::mpsc;
    use std::time::Duration;
    let (session, _owner) = ConnectionSession::journal_fixture(1, 2, journal(8))?;
    session.begin_cycle(4, false)?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(4, 0, None)?;
    session.prepare_established(4, 0)?;
    let mut events = session.subscribe_events()?;
    events.recv().await?;
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    session.before_snapshot_for_test(move || {
        let _ = entered_tx.send(());
        let _ = release_rx.recv_timeout(Duration::from_secs(5));
    })?;
    let writer_session = session.clone();
    let writer =
        std::thread::Builder::new().spawn(move || writer_session.commit_established(4, 0))?;
    entered_rx.recv_timeout(Duration::from_secs(5))?;
    let locked = !session.can_lock_for_test();
    let committed = events.try_recv()?;
    let ConnectionEventKind::Established { connection } = committed.kind else {
        let _ = release_tx.send(());
        let _ = writer.join();
        return Err(test_error(
            "history was not committed at the controlled boundary",
        ));
    };
    let (reading_tx, reading_rx) = mpsc::channel();
    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let reader_session = session.clone();
    let reader = std::thread::Builder::new().spawn(move || {
        let _ = reading_tx.send(());
        let result = reader_session.snapshot();
        let _ = snapshot_tx.send(result);
    })?;
    reading_rx.recv_timeout(Duration::from_secs(5))?;
    let (seeding_tx, seeding_rx) = mpsc::channel();
    let (seed_tx, seed_rx) = mpsc::channel();
    let seed_session = session.clone();
    let seeder = std::thread::Builder::new().spawn(move || {
        let _ = seeding_tx.send(());
        let _ = seed_tx.send(seed_session.watch_state());
    })?;
    seeding_rx.recv_timeout(Duration::from_secs(5))?;
    let blocked = matches!(
        snapshot_rx.recv_timeout(Duration::from_millis(30)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    let seed_blocked = matches!(
        seed_rx.recv_timeout(Duration::from_millis(30)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    release_tx.send(())?;
    writer
        .join()
        .map_err(|_| test_error("transaction thread failed"))??;
    reader
        .join()
        .map_err(|_| test_error("snapshot thread failed"))?;
    seeder
        .join()
        .map_err(|_| test_error("seed thread failed"))?;
    check!(locked)?;
    check!(blocked)?;
    check!(seed_blocked)?;
    let snapshot = snapshot_rx.recv_timeout(Duration::from_secs(5))??;
    let ConnectionState::Connected(current) = snapshot.state else {
        return Err(test_error("snapshot returned an older connection state"));
    };
    check_eq!(current.connection_id, connection.connection_id)?;
    let mut late_state = seed_rx.recv_timeout(Duration::from_secs(5))??;
    let late = late_state
        .recv()
        .await?
        .ok_or_else(|| test_error("late state seed missing"))?;
    check_eq!(late.revision, snapshot.revision)?;
    check!(matches!(late.state, ConnectionState::Connected(_)))?;
    Ok(())
}

#[tokio::test]
async fn closing_rejects_late_preparation_and_success_but_still_finishes_the_attempt() -> TestResult
{
    let (session, mut journal) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    session.begin_cycle(4, false)?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(4, 0, None)?;
    session.prepare_established(4, 0)?;
    session.begin_closing()?;
    check_eq!(
        session
            .commit_established(4, 0)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Cancelled)
    )?;
    check_eq!(
        session
            .set_attempt_credential_version(4, 0, Some("late".to_owned()))
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Cancelled)
    )?;
    check_eq!(
        session
            .prepare_established(4, 0)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Cancelled)
    )?;
    check!(matches!(
        session.snapshot()?.state,
        ConnectionState::Closing
    ))?;
    session.terminate(TerminationReason::Cancelled, None)?;
    check!(matches!(
        next(&mut journal).await?.kind,
        ConnectionEventKind::AttemptStarted { .. }
    ))?;
    check!(matches!(
        next(&mut journal).await?.kind,
        ConnectionEventKind::AttemptFailed { .. }
    ))?;
    check!(matches!(
        next(&mut journal).await?.kind,
        ConnectionEventKind::Closed { result: Err(_) }
    ))?;
    Ok(())
}

struct HistoryFailureWake {
    session: std::sync::Weak<ConnectionSession>,
    locked: std::sync::atomic::AtomicBool,
}
impl std::task::Wake for HistoryFailureWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if self
            .session
            .upgrade()
            .is_some_and(|session| !session.can_lock_for_test())
        {
            self.locked.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

#[tokio::test]
async fn a_later_journal_fault_cannot_discard_a_prepared_history_failure() -> TestResult {
    use std::future::Future;
    let (session, _owner) = ConnectionSession::journal_fixture(1, 2, journal(4))?;
    session.begin_cycle(4, false)?;
    session.begin_attempt(
        session.handshake_attempt(4, 0),
        session.reserve_attempt().await?,
    )?;
    let mut events = session.subscribe_events()?;
    events.recv().await?;
    let probe = Arc::new(HistoryFailureWake {
        session: Arc::downgrade(&session),
        locked: std::sync::atomic::AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(probe.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    let mut waiting = Box::pin(events.recv());
    check!(waiting.as_mut().poll(&mut cx).is_pending())?;
    drop(waiting);
    // The first cleanup fact violates the observer cursor; the second then
    // exhausts the journal sequence. Both failures must survive the transaction.
    session.lock_state()?.last_sequence = u64::MAX - 1;
    let error = session
        .terminate(TerminationReason::Cancelled, None)
        .err()
        .ok_or_else(|| test_error("journal sequence fault was hidden"))?;
    check_eq!(error.kind(), ErrorKind::ResourceExhausted)?;
    check!(!probe.locked.load(std::sync::atomic::Ordering::SeqCst))?;
    check!(
        matches!(events.recv().await, Err(ReceiveError::Failed(ref error)) if error.kind() == ErrorKind::InvalidInput)
    )?;
    check!(events.recv().await?.is_none())?;
    check_eq!(
        session.closed().await.err().map(|error| error.kind()),
        Some(ErrorKind::ResourceExhausted)
    )?;
    Ok(())
}

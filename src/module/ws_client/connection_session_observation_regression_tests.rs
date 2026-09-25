use super::*;
use crate::error::{ErrorKind, ErrorStage, TryReceiveError};
use crate::module::transport::failure::ConnectStage;
use crate::module::ws_client::listener_store::ConnectionObservers;
use crate::module::ws_client::test_support::{check, check_eq, error_with_drop_probe, TestResult};
use crate::subscription::CallbackExecutor;
use crate::ws::{ConnectionSnapshot, ConnectionState, EventOptions, IoEndKind};
use futures::{task::noop_waker_ref, Stream};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Weak;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

async fn bounded<T>(future: impl Future<Output = T>) -> Result<T, crate::BoxError> {
    Ok(tokio::time::timeout(Duration::from_secs(2), future).await?)
}

#[derive(Default)]
struct InlineExecutor;
impl CallbackExecutor for InlineExecutor {
    fn ensure_ready(&self) -> crate::Result<()> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> crate::Result<()> {
        job();
        Ok(())
    }
}

fn fixture(
    history_items: usize,
) -> crate::Result<(Arc<ConnectionSession>, EventReceiver<ConnectionEvent>)> {
    let executor: Arc<dyn CallbackExecutor> = Arc::new(InlineExecutor);
    let (session, journal) = ConnectionSession::new_with_observers(
        11,
        21,
        Some(JournalOptions {
            max_events: 16,
            max_bytes: 16 * MAX_CONNECTION_EVENT_BYTES,
        }),
        Arc::new(crate::Metadata::new()),
        EventOptions {
            max_events: history_items,
            max_bytes: 128 * 1024,
        },
        ConnectionObservers {
            state_executor: executor.clone(),
            state_quota: Arc::new(Semaphore::new(8)),
            event_executor: executor,
            event_quota: Arc::new(Semaphore::new(8)),
        },
    )?;
    let owner = journal.ok_or_else(|| NetError::from(ErrorKind::Internal))?;
    Ok((session, owner))
}

async fn establish(session: &ConnectionSession, cycle: u64) -> TestResult {
    session.begin_cycle(cycle, cycle != 41)?;
    session.begin_attempt(
        session.handshake_attempt(cycle, 0),
        bounded(session.reserve_attempt()).await??,
    )?;
    session.set_attempt_credential_version(cycle, 0, Some(format!("version-{cycle}")))?;
    session.prepare_established(cycle, 0)?;
    session.commit_established(cycle, 0)?;
    Ok(())
}

fn closed_failure(snapshot: ConnectionSnapshot) -> Result<NetError, crate::BoxError> {
    match snapshot.state {
        ConnectionState::Closed(Err(error)) => Ok(error),
        other => {
            Err(format!("expected normal final snapshot containing failure, got {other:?}").into())
        }
    }
}

fn same_source(left: &NetError, right: &NetError) -> TestResult {
    check_eq!(left.kind(), right.kind())?;
    check_eq!(left.context().stage, right.context().stage)?;
    check_eq!(left.context().client_id, right.context().client_id)?;
    check_eq!(left.context().session_id, right.context().session_id)?;
    check_eq!(left.context().connection_id, right.context().connection_id)?;
    check_eq!(left.context().attempt_id, right.context().attempt_id)?;
    check_eq!(left.context().io_end, right.context().io_end)?;
    check!(std::ptr::eq(
        left.source().ok_or("left source missing")?,
        right.source().ok_or("right source missing")?
    ))?;
    Ok(())
}

#[tokio::test]
async fn dropping_observers_and_cancelled_waiters_does_not_cancel_the_session_owner() -> TestResult
{
    let (session, mut journal) = fixture(4)?;
    let state = session.watch_state()?;
    let events = session.subscribe_events()?;
    let mut waiting = Box::pin(session.wait_connected());
    let mut closed = Box::pin(session.closed());
    let mut cx = Context::from_waker(noop_waker_ref());
    check!(waiting.as_mut().poll(&mut cx).is_pending())?;
    check!(closed.as_mut().poll(&mut cx).is_pending())?;
    drop((waiting, closed, state, events));
    check!(!session.cancel_token().is_cancelled())?;
    check!(!session.completion_token().is_cancelled())?;
    establish(&session, 41).await?;
    let connected = bounded(session.wait_connected()).await??;
    check_eq!(connected.cycle_id.as_u64(), 41)?;
    session.terminate(TerminationReason::LocalClose, None)?;
    let result = bounded(session.closed()).await??;
    check_eq!(result.reason, TerminationReason::LocalClose)?;
    for sequence in 1..=4 {
        let event = bounded(journal.recv())
            .await??
            .ok_or("observer Drop lost journal fact")?;
        check_eq!(event.sequence, sequence)?;
    }
    check!(bounded(journal.recv()).await??.is_none())?;
    Ok(())
}

#[tokio::test]
async fn late_failed_state_is_a_final_value_and_history_keeps_the_same_owned_failure() -> TestResult
{
    let (session, mut journal) = fixture(2)?;
    establish(&session, 41).await?;
    let physical = NetError::from(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "reset source",
    ));
    session.terminate_with_details(
        TerminationReason::IoFailure,
        Some(ConnectionFailure::new(
            physical,
            ConnectStage::WebSocketIo,
            None,
            false,
        )),
        ConnectionTerminationDetails {
            peer_close: None,
            io_end_kind: Some(IoEndKind::ConnectionReset),
        },
    )?;
    let terminal = bounded(session.closed())
        .await?
        .err()
        .ok_or("failed session closed successfully")?;
    check_eq!(terminal.kind(), ErrorKind::Io)?;
    check_eq!(terminal.context().stage, Some(ErrorStage::Receive))?;
    let mut state = session.watch_state()?;
    let initial = state.current();
    let final_value = bounded(state.recv())
        .await??
        .ok_or("late state omitted final value")?;
    check_eq!(initial.revision, final_value.revision)?;
    same_source(&terminal, &closed_failure(initial)?)?;
    same_source(&terminal, &closed_failure(final_value)?)?;
    check!(bounded(state.recv()).await??.is_none())?;
    check!(bounded(state.recv()).await??.is_none())?;
    let mut history = session.subscribe_events()?;
    check!(matches!(
        history.try_recv(),
        Err(TryReceiveError::Lagged { skipped: 2 })
    ))?;
    let ended = history.try_recv()?;
    check_eq!(ended.sequence, 3)?;
    let ConnectionEventKind::Disconnected { connection, end } = ended.kind else {
        return Err("late history omitted physical end".into());
    };
    check_eq!(
        terminal.context().connection_id,
        Some(connection.connection_id)
    )?;
    same_source(&terminal, &end.error.ok_or("physical failure missing")?)?;
    let ended = history.try_recv()?;
    check_eq!(ended.sequence, 4)?;
    let ConnectionEventKind::Closed { result: Err(error) } = ended.kind else {
        return Err("late history changed terminal result".into());
    };
    same_source(&terminal, &error)?;
    check!(matches!(history.try_recv(), Err(TryReceiveError::Closed)))?;
    for sequence in 1..=4 {
        let event = bounded(journal.recv())
            .await??
            .ok_or("journal lost fact while observations replayed")?;
        check_eq!(event.sequence, sequence)?;
        if let ConnectionEventKind::Closed { result: Err(error) } = event.kind {
            same_source(&terminal, &error)?;
        }
    }
    check!(bounded(journal.recv()).await??.is_none())?;
    Ok(())
}

#[tokio::test]
async fn reconnect_waiter_cannot_return_the_previous_connected_snapshot() -> TestResult {
    let (session, _journal) = fixture(8)?;
    establish(&session, 41).await?;
    let original = bounded(session.wait_connected()).await??;
    session.connection_terminated_with_details(
        TerminationReason::IoFailure,
        Some(ConnectionFailure::new(
            ErrorKind::Io.into(),
            ConnectStage::WebSocketIo,
            None,
            true,
        )),
        ConnectionTerminationDetails::default(),
        Some(42),
    )?;
    let mut waiting = Box::pin(session.wait_connected());
    let mut cx = Context::from_waker(noop_waker_ref());
    check!(waiting.as_mut().poll(&mut cx).is_pending())?;
    session.waiting_for_network(42)?;
    check!(waiting.as_mut().poll(&mut cx).is_pending())?;
    session.preparing_attempt(42, None)?;
    check!(waiting.as_mut().poll(&mut cx).is_pending())?;
    session.begin_attempt(
        session.handshake_attempt(42, 0),
        bounded(session.reserve_attempt()).await??,
    )?;
    session.set_attempt_credential_version(42, 0, None)?;
    session.prepare_established(42, 0)?;
    check!(waiting.as_mut().poll(&mut cx).is_pending())?;
    session.commit_established(42, 0)?;
    let connected = bounded(waiting).await??;
    check_eq!(connected.session_id, original.session_id)?;
    check_eq!(connected.client_id, original.client_id)?;
    check_eq!(connected.cycle_id.as_u64(), 42)?;
    check!(connected.connection_id != original.connection_id)?;
    session.terminate(TerminationReason::LocalClose, None)?;
    check_eq!(
        bounded(session.closed()).await??.reason,
        TerminationReason::LocalClose
    )?;
    Ok(())
}

struct SessionWake {
    session: Weak<ConnectionSession>,
    calls: AtomicUsize,
    blocked: AtomicBool,
}
impl Wake for SessionWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self
            .session
            .upgrade()
            .is_some_and(|session| !session.can_lock_for_test())
        {
            self.blocked.store(true, Ordering::SeqCst);
        }
    }
}

#[tokio::test]
async fn state_and_history_wakers_can_reenter_the_actual_session_lock() -> TestResult {
    let (session, _journal) = fixture(4)?;
    let mut state = session.watch_state()?;
    bounded(state.recv())
        .await??
        .ok_or("initial snapshot missing")?;
    let mut events = session.subscribe_events()?;
    let state_probe = Arc::new(SessionWake {
        session: Arc::downgrade(&session),
        calls: AtomicUsize::new(0),
        blocked: AtomicBool::new(false),
    });
    let event_probe = Arc::new(SessionWake {
        session: Arc::downgrade(&session),
        calls: AtomicUsize::new(0),
        blocked: AtomicBool::new(false),
    });
    let state_waker = Waker::from(state_probe.clone());
    let event_waker = Waker::from(event_probe.clone());
    check!(matches!(
        Pin::new(&mut state).poll_next(&mut Context::from_waker(&state_waker)),
        Poll::Pending
    ))?;
    check!(matches!(
        Pin::new(&mut events).poll_next(&mut Context::from_waker(&event_waker)),
        Poll::Pending
    ))?;
    session.begin_cycle(41, false)?;
    session.waiting_for_network(41)?;
    session.preparing_attempt(41, None)?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        bounded(session.reserve_attempt()).await??,
    )?;
    check!(state_probe.calls.load(Ordering::SeqCst) > 0)?;
    check!(event_probe.calls.load(Ordering::SeqCst) > 0)?;
    check!(!state_probe.blocked.load(Ordering::SeqCst))?;
    check!(!event_probe.blocked.load(Ordering::SeqCst))?;
    session.terminate(TerminationReason::Cancelled, None)?;
    Ok(())
}

#[tokio::test]
async fn replacing_last_error_retires_the_original_source_after_unlocking_session() -> TestResult {
    let (session, mut journal) = fixture(1)?;
    let retired = Arc::new(AtomicUsize::new(0));
    let blocked = Arc::new(AtomicBool::new(false));
    let weak = Arc::downgrade(&session);
    let (on_drop, on_block) = (retired.clone(), blocked.clone());
    let failure = error_with_drop_probe(move || {
        on_drop.fetch_add(1, Ordering::SeqCst);
        if weak
            .upgrade()
            .is_some_and(|session| !session.can_lock_for_test())
        {
            on_block.store(true, Ordering::SeqCst);
        }
    });
    session.begin_cycle(41, false)?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        bounded(session.reserve_attempt()).await??,
    )?;
    session.attempt_failed(
        41,
        0,
        ConnectionFailure::new(failure, ConnectStage::Provider, None, true),
        RetryDecision::Scheduled {
            after: Duration::ZERO,
        },
    )?;
    for sequence in 1..=2 {
        check_eq!(
            bounded(journal.recv())
                .await??
                .ok_or("first attempt fact missing")?
                .sequence,
            sequence
        )?;
    }
    session.preparing_attempt(41, None)?;
    session.begin_attempt(
        session.handshake_attempt(41, 1),
        bounded(session.reserve_attempt()).await??,
    )?;
    check_eq!(retired.load(Ordering::SeqCst), 0)?;
    session.attempt_failed(
        41,
        1,
        ConnectionFailure::new(ErrorKind::Io.into(), ConnectStage::Tcp, None, false),
        RetryDecision::Stop,
    )?;
    check_eq!(retired.load(Ordering::SeqCst), 1)?;
    check!(!blocked.load(Ordering::SeqCst))?;
    session.terminate(TerminationReason::ConnectFailed, None)?;
    Ok(())
}

#[test]
fn retained_observers_do_not_keep_the_session_allocation_alive() -> TestResult {
    let (session, owner) = fixture(4)?;
    let state = session.watch_state()?;
    let events = session.subscribe_events()?;
    let weak = Arc::downgrade(&session);
    drop(owner);
    check!(!session.cancel_token().is_cancelled())?;
    drop(session);
    check!(weak.upgrade().is_none())?;
    drop((state, events));
    Ok(())
}

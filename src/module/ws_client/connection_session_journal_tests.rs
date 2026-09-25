use super::*;
use crate::error::TryReceiveError;
use crate::module::transport::failure::ConnectStage;
use crate::subscription::CallbackExecutor;
use futures::task::noop_waker_ref;
use std::error::Error;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

type TestResult<T = ()> = std::result::Result<T, crate::BoxError>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

fn ready<F: Future>(future: F) -> TestResult<F::Output> {
    let mut future = Box::pin(future);
    let mut cx = Context::from_waker(noop_waker_ref());
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(value) => Ok(value),
        Poll::Pending => Err("operation unexpectedly required consumer progress".into()),
    }
}

#[derive(Default)]
struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

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

fn minimum_journal() -> JournalOptions {
    JournalOptions {
        max_events: 4,
        max_bytes: 4 * MAX_CONNECTION_EVENT_BYTES,
    }
}

fn fixture(
    journal: Option<JournalOptions>,
    history_items: usize,
    event_slots: usize,
) -> crate::Result<(
    Arc<ConnectionSession>,
    Option<EventReceiver<ConnectionEvent>>,
)> {
    fixture_with_executor(
        journal,
        history_items,
        event_slots,
        Arc::new(InlineExecutor),
    )
}

fn fixture_with_executor(
    journal: Option<JournalOptions>,
    history_items: usize,
    event_slots: usize,
    executor: Arc<dyn CallbackExecutor>,
) -> crate::Result<(
    Arc<ConnectionSession>,
    Option<EventReceiver<ConnectionEvent>>,
)> {
    ConnectionSession::new_with_observers(
        11,
        21,
        journal,
        Arc::new(crate::Metadata::new()),
        EventOptions {
            max_events: history_items,
            max_bytes: 16 * MAX_CONNECTION_EVENT_BYTES,
        },
        ConnectionObservers {
            state_executor: executor.clone(),
            state_quota: Arc::new(Semaphore::new(8)),
            event_executor: executor,
            event_quota: Arc::new(Semaphore::new(event_slots)),
        },
    )
}

fn fail_attempt(session: &ConnectionSession, attempt: u64) -> TestResult {
    session.begin_attempt(
        session.handshake_attempt(41, attempt),
        ready(session.reserve_attempt())??,
    )?;
    session.attempt_failed(
        41,
        attempt,
        ConnectionFailure::new(NetError::from(ErrorKind::Io), ConnectStage::Tcp, None, true),
        RetryDecision::Scheduled {
            after: Duration::ZERO,
        },
    )?;
    Ok(())
}

fn establish_reserved(
    session: &ConnectionSession,
    attempt: u64,
    reservation: AttemptReservation,
) -> TestResult {
    session.begin_attempt(session.handshake_attempt(41, attempt), reservation)?;
    session.set_attempt_credential_version(41, attempt, None)?;
    session.prepare_established(41, attempt)?;
    session.commit_established(41, attempt)?;
    Ok(())
}

#[test]
fn absent_journal_never_applies_backpressure_and_history_stays_bounded() -> TestResult {
    let (session, journal) = fixture(None, 2, 8)?;
    check(journal.is_none(), "None created a journal subscription")?;
    session.begin_cycle(41, false)?;
    for attempt in 0..8 {
        fail_attempt(&session, attempt)?;
    }
    check(
        !session.cancel_token().is_cancelled(),
        "unobserved retries cancelled the session",
    )?;
    session.terminate(TerminationReason::LocalClose, None)?;
    check(
        ready(session.closed())??.reason == TerminationReason::LocalClose,
        "closed result required a journal receiver",
    )?;
    let mut history = session.subscribe_events()?;
    check(
        matches!(
            history.try_recv(),
            Err(TryReceiveError::Lagged { skipped: 15 })
        ),
        "history failed to bound the first fifteen facts",
    )?;
    check(
        history.try_recv()?.sequence == 16,
        "retained failure missing",
    )?;
    let closed = history.try_recv()?;
    check(
        closed.sequence == 17
            && matches!(closed.kind, ConnectionEventKind::Closed { result: Ok(_) }),
        "retained final fact missing",
    )?;
    check(
        matches!(history.try_recv(), Err(TryReceiveError::Closed)),
        "finished history was not fused",
    )
}

#[test]
fn full_minimum_journal_unsubscribe_releases_reservation_and_keeps_history_continuous() -> TestResult
{
    let (session, journal) = fixture(Some(minimum_journal()), 8, 8)?;
    let journal = journal.ok_or("journal missing")?;
    session.begin_cycle(41, false)?;
    fail_attempt(&session, 0)?;
    let mut waiting = Box::pin(session.reserve_attempt());
    let wakes = Arc::new(CountWake::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    check(
        waiting.as_mut().poll(&mut cx).is_pending(),
        "minimum journal did not retain reserved event capacity",
    )?;
    check(journal.unsubscribe(), "initial detach was rejected")?;
    check(!journal.unsubscribe(), "detach was not idempotent")?;
    check(
        wakes.0.load(Ordering::SeqCst) > 0,
        "detaching journal did not wake reservation",
    )?;
    let reservation = match waiting.as_mut().poll(&mut cx) {
        Poll::Ready(result) => result?,
        Poll::Pending => return Err("detached journal continued to block reservation".into()),
    };
    check(
        !session.cancel_token().is_cancelled(),
        "journal unsubscribe cancelled its session",
    )?;
    establish_reserved(&session, 1, reservation)?;
    check(
        ready(session.wait_connected())??.attempt_id.as_u64() == 1,
        "session could not connect after journal detach",
    )?;
    session.terminate(TerminationReason::LocalClose, None)?;
    check(
        ready(session.closed())?.is_ok(),
        "detached session lost closed result",
    )?;
    let mut history = session.subscribe_events()?;
    for sequence in 1..=6 {
        check(
            history.try_recv()?.sequence == sequence,
            "journal detach interrupted ordinary history sequence",
        )?;
    }
    check(
        matches!(history.try_recv(), Err(TryReceiveError::Closed)),
        "history did not end after the six lifecycle facts",
    )
}

#[test]
fn detaching_during_the_byte_wait_releases_the_partial_reservation() -> TestResult {
    let (session, journal) = fixture(
        Some(JournalOptions {
            max_events: 16,
            max_bytes: 4 * MAX_CONNECTION_EVENT_BYTES,
        }),
        8,
        8,
    )?;
    let journal = journal.ok_or("journal missing")?;
    session.begin_cycle(41, false)?;
    fail_attempt(&session, 0)?;
    // Two queued facts and the Closed reservation leave thirteen event slots.
    // Their nonzero bytes prevent reserving three full maximum-size events.
    let mut waiting = Box::pin(session.reserve_attempt());
    let wakes = Arc::new(CountWake::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    check(
        waiting.as_mut().poll(&mut cx).is_pending(),
        "byte limit did not block while event slots were available",
    )?;
    drop(journal);
    check(
        wakes.0.load(Ordering::SeqCst) > 0,
        "journal Drop did not wake the byte reservation",
    )?;
    let reservation = match waiting.as_mut().poll(&mut cx) {
        Poll::Ready(result) => result?,
        Poll::Pending => return Err("byte reservation remained blocked after detach".into()),
    };
    check(
        !session.cancel_token().is_cancelled(),
        "byte reservation detach became SessionCancelled",
    )?;
    drop(reservation);
    for _ in 0..8 {
        drop(ready(session.reserve_attempt())??);
    }
    fail_attempt(&session, 1)?;
    session.terminate(TerminationReason::LocalClose, None)?;
    check(
        ready(session.closed())?.is_ok(),
        "byte detach prevented terminal completion",
    )
}

#[test]
fn unobserved_reservations_still_reject_another_session_scope() -> TestResult {
    for detached in [false, true] {
        let options = detached.then(minimum_journal);
        let (first, first_journal) = fixture(options.clone(), 8, 8)?;
        let (second, second_journal) = fixture(options, 8, 8)?;
        // Equal numeric identities must not make separately owned reservations interchangeable.
        drop((first_journal, second_journal));
        first.begin_cycle(41, false)?;
        second.begin_cycle(41, false)?;
        let foreign = ready(first.reserve_attempt())??;
        let error = second
            .begin_attempt(second.handshake_attempt(41, 0), foreign)
            .err()
            .ok_or("foreign unobserved reservation was accepted")?;
        check(
            error.kind() == ErrorKind::InvalidConfig,
            "foreign reservation did not retain its ownership error",
        )?;
        fail_attempt(&first, 0)?;
        fail_attempt(&second, 0)?;
    }
    Ok(())
}

#[test]
fn unobserved_success_commit_cannot_be_replayed() -> TestResult {
    for detached in [false, true] {
        let (session, journal) = fixture(detached.then(minimum_journal), 8, 8)?;
        drop(journal);
        session.begin_cycle(41, false)?;
        establish_reserved(&session, 0, ready(session.reserve_attempt())??)?;
        let established = ready(session.wait_connected())??;
        check(
            session.commit_established(41, 0).is_err(),
            "duplicate success commit was accepted without a journal lease",
        )?;
        check(
            ready(session.wait_connected())??.connection_id == established.connection_id,
            "rejected duplicate changed the connected identity",
        )?;
        session.terminate(TerminationReason::LocalClose, None)?;
        let mut history = session.subscribe_events()?;
        for sequence in 1..=4 {
            check(
                history.try_recv()?.sequence == sequence,
                "duplicate commit altered the lifecycle sequence",
            )?;
        }
        check(
            matches!(history.try_recv(), Err(TryReceiveError::Closed)),
            "duplicate commit appended an extra fact",
        )?;
    }
    Ok(())
}

#[test]
fn detached_journal_preserves_terminal_error_source_and_last_error() -> TestResult {
    let (session, journal) = fixture(Some(minimum_journal()), 8, 8)?;
    drop(journal);
    session.begin_cycle(41, false)?;
    establish_reserved(&session, 0, ready(session.reserve_attempt())??)?;
    let physical = NetError::from(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "retained transport failure",
    ));
    session.terminate(
        TerminationReason::IoFailure,
        Some(ConnectionFailure::new(
            physical.clone(),
            ConnectStage::WebSocketIo,
            None,
            false,
        )),
    )?;
    let terminal = ready(session.closed())?
        .err()
        .ok_or("detached journal lost the business failure")?;
    let last = session.snapshot()?.last_error.ok_or("last_error missing")?;
    for error in [&terminal, &last] {
        check(
            error.kind() == ErrorKind::Io,
            "error kind changed after detach",
        )?;
        check(
            std::ptr::eq(
                error.source().ok_or("observed source missing")?,
                physical.source().ok_or("physical source missing")?,
            ),
            "journal detach changed the retained error source",
        )?;
        check(
            error.context().connection_id.is_some(),
            "terminal connection identity missing",
        )?;
    }
    let mut history = session.subscribe_events()?;
    for _ in 0..3 {
        history.try_recv()?;
    }
    check(
        matches!(history.try_recv()?.kind, ConnectionEventKind::Closed { result: Err(ref error) } if error.kind() == ErrorKind::Io),
        "ordinary history lost detached terminal error",
    )
}

#[test]
fn native_journal_uses_one_event_subscription_slot_and_detach_returns_it() -> TestResult {
    let (session, journal) = fixture(Some(minimum_journal()), 8, 1)?;
    let journal = journal.ok_or("journal missing")?;
    check(
        session.subscribe_events().err().map(|error| error.kind())
            == Some(ErrorKind::SubscriptionLimitReached),
        "journal did not occupy exactly one shared event subscription slot",
    )?;
    journal.unsubscribe();
    let ordinary = session.subscribe_events()?;
    check(
        !session.cancel_token().is_cancelled(),
        "releasing a subscription slot cancelled the session",
    )?;
    drop(ordinary);
    let (unobserved, absent) = fixture(None, 8, 1)?;
    check(absent.is_none(), "journal None created a receiver")?;
    let _ordinary = unobserved.subscribe_events()?;
    check(
        unobserved
            .subscribe_events()
            .err()
            .map(|error| error.kind())
            == Some(ErrorKind::SubscriptionLimitReached),
        "absent journal consumed an event subscription slot",
    )
}

#[test]
fn journal_callback_dispatch_failure_only_terminates_its_subscription() -> TestResult {
    struct RejectDispatch(NetError);
    impl CallbackExecutor for RejectDispatch {
        fn ensure_ready(&self) -> crate::Result<()> {
            Ok(())
        }
        fn submit(&self, _job: Box<dyn FnOnce() + Send>) -> crate::Result<()> {
            Err(self.0.clone())
        }
    }

    let dispatch_error = NetError::with_source(
        ErrorKind::RuntimeUnavailable,
        std::io::Error::other("journal executor rejected the accepted lifecycle fact"),
    )
    .with_stage(ErrorStage::Dispatch);
    let (session, journal) = fixture_with_executor(
        Some(minimum_journal()),
        8,
        8,
        Arc::new(RejectDispatch(dispatch_error.clone())),
    )?;
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = calls.clone();
    let subscription = journal
        .ok_or("journal missing")?
        .into_callback(move |_, _| {
            callback_calls.fetch_add(1, Ordering::SeqCst);
        })?;
    check(
        subscription.is_active(),
        "empty callback conversion was rejected",
    )?;
    session.begin_cycle(41, false)?;
    session.begin_attempt(
        session.handshake_attempt(41, 0),
        ready(session.reserve_attempt())??,
    )?;
    check(
        !subscription.is_active() && calls.load(Ordering::SeqCst) == 0,
        "rejected dispatch did not terminate only the journal callback",
    )?;
    let failure = ready(subscription.close())?
        .err()
        .ok_or("subscription close lost the dispatch failure")?;
    check(
        failure.kind() == ErrorKind::RuntimeUnavailable
            && failure.context().stage == Some(ErrorStage::Dispatch),
        "dispatch failure classification changed",
    )?;
    check(
        std::ptr::eq(
            failure
                .source()
                .ok_or("subscription failure source missing")?,
            dispatch_error
                .source()
                .ok_or("original failure source missing")?,
        ),
        "subscription did not retain the dispatch source",
    )?;
    check(
        !session.cancel_token().is_cancelled(),
        "callback executor failure cancelled the session",
    )?;
    drop(ready(session.reserve_attempt())??);
    session.attempt_failed(
        41,
        0,
        ConnectionFailure::new(NetError::from(ErrorKind::Io), ConnectStage::Tcp, None, true),
        RetryDecision::Scheduled {
            after: Duration::ZERO,
        },
    )?;
    fail_attempt(&session, 1)?;
    session.terminate(TerminationReason::LocalClose, None)?;
    check(
        ready(session.closed())??.reason == TerminationReason::LocalClose,
        "journal dispatch failure replaced the business result",
    )?;
    let mut history = session.subscribe_events()?;
    for sequence in 1..=5 {
        check(
            history.try_recv()?.sequence == sequence,
            "journal dispatch failure interrupted ordinary history",
        )?;
    }
    check(
        matches!(history.try_recv(), Err(TryReceiveError::Closed)),
        "history did not end after isolated dispatch failure",
    )
}

use super::connection_history::ConnectionHistory;
use super::test_support::{check, check_eq, error_with_drop_probe, TestResult};
use crate::error::{ErrorKind, NetError, TryReceiveError};
use crate::subscription::{CallbackExecutor, EventReceiver};
use crate::ws::{
    AttemptId, ClientId, ConnectionEvent, ConnectionEventKind, CycleId, EventOptions,
    HandshakeAttempt, RetryDecision, SessionEnd, SessionId, TerminationReason,
};
use futures::Stream;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, SystemTime};
use tokio::sync::Semaphore;

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
fn fact(sequence: u64, extra: usize) -> ConnectionEvent {
    ConnectionEvent {
        sequence,
        client_id: ClientId::from_allocated(1),
        session_id: SessionId::from_allocated(2),
        occurred_at: SystemTime::UNIX_EPOCH,
        kind: ConnectionEventKind::AttemptStarted {
            attempt: HandshakeAttempt {
                client_id: ClientId::from_allocated(1),
                session_id: SessionId::from_allocated(2),
                cycle_id: CycleId::from_allocated(3),
                attempt_id: AttemptId::from_allocated(sequence),
                metadata: Arc::new(crate::Metadata::from([(
                    "body".to_owned(),
                    "x".repeat(extra),
                )])),
            },
        },
    }
}
fn closed(sequence: u64) -> ConnectionEvent {
    ConnectionEvent {
        kind: ConnectionEventKind::Closed {
            result: Ok(SessionEnd {
                reason: TerminationReason::LocalClose,
                last_connection: None,
            }),
        },
        ..fact(sequence, 0)
    }
}
fn history(items: usize, bytes: usize, subscriptions: usize) -> crate::Result<ConnectionHistory> {
    ConnectionHistory::new(
        EventOptions {
            max_events: items,
            max_bytes: bytes,
        },
        Arc::new(InlineExecutor),
        Arc::new(Semaphore::new(subscriptions)),
    )
}
fn publish(history: &ConnectionHistory, event: ConnectionEvent) -> crate::Result<()> {
    let publication = history.prepare_record(event);
    let result = publication.result();
    publication.dispatch()?;
    result
}
fn lagged(receiver: &mut EventReceiver<ConnectionEvent>, expected: u64) -> TestResult {
    match receiver.try_recv() {
        Err(TryReceiveError::Lagged { skipped }) => check_eq!(skipped, expected),
        other => Err(format!("expected Lagged({expected}), got {other:?}").into()),
    }
}
fn next(receiver: &mut EventReceiver<ConnectionEvent>, sequence: u64) -> TestResult {
    let event = receiver.try_recv()?;
    check_eq!(event.sequence, sequence)?;
    check_eq!(event.client_id, ClientId::from_allocated(1))?;
    check_eq!(event.session_id, SessionId::from_allocated(2))?;
    Ok(())
}

#[test]
fn late_receiver_reports_exact_missing_prefix_then_history_and_live() -> TestResult {
    let history = history(3, 128 * 1024, 3)?;
    for sequence in 1..=5 {
        publish(&history, fact(sequence, 0))?;
    }
    let mut first = history.subscribe()?;
    lagged(&mut first, 2)?;
    for sequence in 3..=5 {
        next(&mut first, sequence)?;
    }
    check!(matches!(first.try_recv(), Err(TryReceiveError::Empty)))?;
    publish(&history, fact(6, 0))?;
    next(&mut first, 6)?;
    let mut late = history.subscribe()?;
    check!(first.id() != late.id())?;
    lagged(&mut late, 3)?;
    for sequence in 4..=6 {
        next(&mut late, sequence)?;
    }
    Ok(())
}

#[test]
fn byte_limit_controls_suffix_independently_of_item_limit() -> TestResult {
    let size = fact(1, 9).measured_size()?;
    let history = history(20, 2 * size - 1, 2)?;
    let mut live = history.subscribe()?;
    publish(&history, fact(1, 9))?;
    next(&mut live, 1)?;
    publish(&history, fact(2, 9))?;
    publish(&history, fact(3, 9))?;
    lagged(&mut live, 1)?;
    next(&mut live, 3)?;
    let mut late = history.subscribe()?;
    lagged(&mut late, 2)?;
    next(&mut late, 3)?;
    Ok(())
}

#[test]
fn oversized_history_fact_discards_whole_prefix_and_resumes_without_a_gap() -> TestResult {
    let size = fact(1, 0).measured_size()?;
    let history = history(8, 2 * size, 4)?;
    let mut slow = history.subscribe()?;
    let mut fast = history.subscribe()?;
    publish(&history, fact(1, 0))?;
    next(&mut fast, 1)?;
    publish(&history, fact(2, 0))?;
    publish(&history, fact(3, 2 * size))?;
    lagged(&mut fast, 2)?;
    let mut after_gap = history.subscribe()?;
    lagged(&mut after_gap, 3)?;
    check!(matches!(after_gap.try_recv(), Err(TryReceiveError::Empty)))?;
    publish(&history, fact(4, 0))?;
    lagged(&mut slow, 3)?;
    next(&mut slow, 4)?;
    next(&mut fast, 4)?;
    next(&mut after_gap, 4)?;
    let mut newest = history.subscribe()?;
    lagged(&mut newest, 3)?;
    next(&mut newest, 4)?;
    Ok(())
}

#[test]
fn closed_history_is_replayed_then_fused_for_existing_and_late_receivers() -> TestResult {
    let history = history(3, 128 * 1024, 2)?;
    let mut live = history.subscribe()?;
    publish(&history, fact(1, 0))?;
    publish(&history, closed(2))?;
    let mut late = history.subscribe()?;
    for receiver in [&mut live, &mut late] {
        next(receiver, 1)?;
        let last = receiver.try_recv()?;
        check_eq!(last.sequence, 2)?;
        check!(
            matches!(last.kind, ConnectionEventKind::Closed { result: Ok(end) } if end.reason == TerminationReason::LocalClose)
        )?;
        for _ in 0..2 {
            check!(matches!(receiver.try_recv(), Err(TryReceiveError::Closed)))?;
        }
    }
    let publication = history.prepare_record(fact(3, 0));
    let error = publication
        .result()
        .err()
        .ok_or("history accepted an event after Closed")?;
    check_eq!(error.kind(), ErrorKind::Closed)?;
    let _ = publication.dispatch();
    Ok(())
}

#[test]
fn dropped_observers_release_quota_without_ending_history() -> TestResult {
    let quota = Arc::new(Semaphore::new(1));
    let history = ConnectionHistory::new(
        EventOptions::default(),
        Arc::new(InlineExecutor),
        quota.clone(),
    )?;
    for sequence in 1..=100 {
        let receiver = history.subscribe()?;
        check_eq!(quota.available_permits(), 0)?;
        let error = history
            .subscribe()
            .err()
            .ok_or("quota admitted a second observer")?;
        check_eq!(error.kind(), ErrorKind::SubscriptionLimitReached)?;
        drop(receiver);
        check_eq!(quota.available_permits(), 1)?;
        publish(&history, fact(sequence, 0))?;
    }
    let mut final_receiver = history.subscribe()?;
    lagged(&mut final_receiver, 68)?;
    for sequence in 69..=100 {
        next(&mut final_receiver, sequence)?;
    }
    Ok(())
}

#[test]
fn publication_dispatch_order_does_not_reorder_callback_delivery() -> TestResult {
    let history = history(8, 128 * 1024, 1)?;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let target = observed.clone();
    let subscription = history
        .subscribe()?
        .into_callback(move |_, value| match target.lock() {
            Ok(mut values) => values.push(value.map(|event| event.sequence)),
            Err(error) => eprintln!("test callback observation lock failed: {error}"),
        })?;
    let first = history.prepare_record(fact(1, 0));
    first.result()?;
    let second = history.prepare_record(closed(2));
    second.result()?;
    check!(observed.lock().map_err(NetError::from_poison)?.is_empty())?;
    second.dispatch()?;
    first.dispatch()?;
    let values = observed.lock().map_err(NetError::from_poison)?;
    check_eq!(values.len(), 2)?;
    for (observed, expected) in values.iter().zip([1, 2]) {
        check_eq!(
            observed
                .as_ref()
                .map_err(|error| format!("callback failed: {error:?}"))?,
            &expected
        )?;
    }
    drop(values);
    drop(subscription);
    Ok(())
}

struct ProbeWake {
    history: std::sync::Weak<ConnectionHistory>,
    outer: Arc<Mutex<()>>,
    calls: AtomicUsize,
    blocked: AtomicBool,
}
impl Wake for ProbeWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.outer.try_lock().is_err()
            || self
                .history
                .upgrade()
                .is_some_and(|history| !history.can_lock_for_test())
        {
            self.blocked.store(true, Ordering::SeqCst);
        }
    }
}
#[test]
fn preparation_defers_receiver_wakes_until_outer_lock_is_released() -> TestResult {
    let history = Arc::new(history(1, 128 * 1024, 1)?);
    let outer = Arc::new(Mutex::new(()));
    let probe = Arc::new(ProbeWake {
        history: Arc::downgrade(&history),
        outer: outer.clone(),
        calls: AtomicUsize::new(0),
        blocked: AtomicBool::new(false),
    });
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    let mut receiver = history.subscribe()?;
    check!(matches!(
        Pin::new(&mut receiver).poll_next(&mut cx),
        Poll::Pending
    ))?;
    let guard = outer.lock().map_err(NetError::from_poison)?;
    let publication = history.prepare_record(fact(1, 0));
    publication.result()?;
    check_eq!(probe.calls.load(Ordering::SeqCst), 0)?;
    drop(guard);
    publication.dispatch()?;
    check_eq!(probe.calls.load(Ordering::SeqCst), 1)?;
    check!(!probe.blocked.load(Ordering::SeqCst))?;
    next(&mut receiver, 1)?;
    Ok(())
}

#[test]
fn preparation_retires_the_last_error_source_outside_the_outer_lock() -> TestResult {
    let history = Arc::new(history(1, 128 * 1024, 1)?);
    let outer = Arc::new(Mutex::new(()));
    let retired = Arc::new(AtomicUsize::new(0));
    let blocked = Arc::new(AtomicBool::new(false));
    let (probe_outer, probe_retired, probe_blocked) =
        (outer.clone(), retired.clone(), blocked.clone());
    let weak_history = Arc::downgrade(&history);
    let error = error_with_drop_probe(move || {
        probe_retired.fetch_add(1, Ordering::SeqCst);
        if probe_outer.try_lock().is_err()
            || weak_history
                .upgrade()
                .is_some_and(|history| !history.can_lock_for_test())
        {
            probe_blocked.store(true, Ordering::SeqCst);
        }
    });
    let mut observed = fact(1, 0);
    let ConnectionEventKind::AttemptStarted { attempt } = observed.kind else {
        return Err("fixture omitted attempt".into());
    };
    observed.kind = ConnectionEventKind::AttemptFailed {
        attempt,
        credential_version: None,
        error,
        retry: RetryDecision::Stop,
    };
    publish(&history, observed)?;
    let guard = outer.lock().map_err(NetError::from_poison)?;
    let publication = history.prepare_record(fact(2, 0));
    publication.result()?;
    check_eq!(retired.load(Ordering::SeqCst), 0)?;
    drop(guard);
    publication.dispatch()?;
    check_eq!(retired.load(Ordering::SeqCst), 1)?;
    check!(!blocked.load(Ordering::SeqCst))?;
    Ok(())
}

#[test]
fn invalid_sequence_and_foreign_scope_do_not_replace_valid_history() -> TestResult {
    let history = history(8, 128 * 1024, 1)?;
    let invalid = history.prepare_record(fact(2, 0));
    check_eq!(
        invalid
            .result()
            .err()
            .ok_or("accepted skipped initial sequence")?
            .kind(),
        ErrorKind::InvalidInput
    )?;
    let _ = invalid.dispatch();
    publish(&history, fact(1, 0))?;
    let mut foreign = fact(2, 0);
    foreign.session_id = SessionId::from_allocated(99);
    let invalid = history.prepare_record(foreign);
    check_eq!(
        invalid
            .result()
            .err()
            .ok_or("accepted foreign session")?
            .kind(),
        ErrorKind::InvalidInput
    )?;
    let _ = invalid.dispatch();
    publish(&history, fact(2, 0))?;
    let mut receiver = history.subscribe()?;
    next(&mut receiver, 1)?;
    next(&mut receiver, 2)?;
    Ok(())
}

#[test]
fn source_failure_is_owned_and_replayed_after_retained_prefix() -> TestResult {
    let history = history(8, 128 * 1024, 2)?;
    let mut live = history.subscribe()?;
    publish(&history, fact(1, 0))?;
    let error = NetError::with_source(
        ErrorKind::ResourceExhausted,
        std::io::Error::other("history allocation"),
    );
    history.prepare_fail(error.clone()).dispatch()?;
    let mut late = history.subscribe()?;
    for receiver in [&mut live, &mut late] {
        next(receiver, 1)?;
        let Err(TryReceiveError::Failed(failure)) = receiver.try_recv() else {
            return Err("history failure was not explicit".into());
        };
        check_eq!(failure.kind(), ErrorKind::ResourceExhausted)?;
        let original = std::error::Error::source(&error).ok_or("missing source")?;
        let received = std::error::Error::source(&failure).ok_or("received failure lost source")?;
        check!(std::ptr::eq(original, received))?;
        check!(matches!(receiver.try_recv(), Err(TryReceiveError::Closed)))?;
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_subscribers_receive_history_and_live_without_duplicates_or_gaps() -> TestResult
{
    let history = Arc::new(history(128, 128 * 1024, 64)?);
    let rendezvous = Arc::new(Barrier::new(2));
    let producer = history.clone();
    let producer_gate = rendezvous.clone();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let worker = std::thread::Builder::new()
        .name("history-test-producer".to_owned())
        .spawn(move || {
            let result = (|| -> TestResult {
                producer_gate.wait();
                for sequence in 1..=64 {
                    publish(&producer, fact(sequence, 0))?;
                    std::thread::yield_now();
                }
                publish(&producer, closed(65))?;
                Ok(())
            })();
            if done_tx.send(result).is_err() {
                eprintln!("history producer observer closed");
            }
        })?;
    rendezvous.wait();
    let mut receivers = Vec::new();
    for _ in 0..64 {
        receivers.push(history.subscribe()?);
        std::thread::yield_now();
    }
    tokio::time::timeout(Duration::from_secs(5), done_rx).await???;
    worker.join().map_err(|_| "history producer unwound")?;
    for receiver in &mut receivers {
        for sequence in 1..=65 {
            next(receiver, sequence)?;
        }
        check!(matches!(receiver.try_recv(), Err(TryReceiveError::Closed)))?;
    }
    Ok(())
}

#[test]
fn allocation_failure_explicitly_ends_history_after_its_retained_prefix() -> TestResult {
    let history = history(8, 128 * 1024, 2)?;
    let mut live = history.subscribe()?;
    publish(&history, fact(1, 0))?;
    history.fail_next_record_allocation_for_test();
    let failed = history.prepare_record(fact(2, 0));
    check_eq!(
        failed
            .result()
            .err()
            .ok_or("injected allocation succeeded")?
            .kind(),
        ErrorKind::ResourceExhausted
    )?;
    check_eq!(
        failed
            .dispatch()
            .err()
            .ok_or("dispatch hid history allocation error")?
            .kind(),
        ErrorKind::ResourceExhausted
    )?;
    let mut late = history.subscribe()?;
    for receiver in [&mut live, &mut late] {
        next(receiver, 1)?;
        let Err(TryReceiveError::Failed(error)) = receiver.try_recv() else {
            return Err("allocation failure silently lost a fact".into());
        };
        check_eq!(error.kind(), ErrorKind::ResourceExhausted)?;
        check!(matches!(receiver.try_recv(), Err(TryReceiveError::Closed)))?;
    }
    let rejected = history.prepare_record(fact(2, 0));
    check_eq!(
        rejected
            .result()
            .err()
            .ok_or("failed history resumed silently")?
            .kind(),
        ErrorKind::Closed
    )?;
    let _ = rejected.dispatch();
    Ok(())
}

struct RejectExecutor;
impl CallbackExecutor for RejectExecutor {
    fn ensure_ready(&self) -> crate::Result<()> {
        Ok(())
    }
    fn submit(&self, _job: Box<dyn FnOnce() + Send>) -> crate::Result<()> {
        Err(NetError::from(ErrorKind::RuntimeUnavailable))
    }
}
#[test]
fn callback_dispatch_failure_only_retires_its_own_observer() -> TestResult {
    let history = ConnectionHistory::new(
        EventOptions::default(),
        Arc::new(RejectExecutor),
        Arc::new(Semaphore::new(2)),
    )?;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let subscription = history.subscribe()?.into_callback(move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
    })?;
    let mut receiver = history.subscribe()?;
    publish(&history, fact(1, 0))?;
    check!(!subscription.is_active())?;
    let error = futures::executor::block_on(subscription.close())
        .err()
        .ok_or("callback scheduling failure was hidden")?;
    check_eq!(error.kind(), ErrorKind::RuntimeUnavailable)?;
    check_eq!(calls.load(Ordering::SeqCst), 0)?;
    next(&mut receiver, 1)?;
    publish(&history, fact(2, 0))?;
    next(&mut receiver, 2)?;
    let mut replacement = history.subscribe()?;
    next(&mut replacement, 1)?;
    next(&mut replacement, 2)?;
    Ok(())
}

#[test]
fn dropping_history_finishes_existing_receivers_without_erasing_their_prefix() -> TestResult {
    let history = history(8, 128 * 1024, 1)?;
    let mut receiver = history.subscribe()?;
    publish(&history, fact(1, 0))?;
    drop(history);
    next(&mut receiver, 1)?;
    check!(matches!(receiver.try_recv(), Err(TryReceiveError::Closed)))?;
    Ok(())
}

#[test]
fn a_closed_fact_that_cannot_fit_still_ends_with_exact_lag_for_late_receivers() -> TestResult {
    let history = history(8, 1, 2)?;
    let mut live = history.subscribe()?;
    publish(&history, fact(1, 0))?;
    publish(&history, closed(2))?;
    let mut late = history.subscribe()?;
    for receiver in [&mut live, &mut late] {
        lagged(receiver, 2)?;
        check!(matches!(receiver.try_recv(), Err(TryReceiveError::Closed)))?;
    }
    Ok(())
}

#[test]
fn rejected_input_defers_its_last_source_until_both_locks_are_released() -> TestResult {
    let history = Arc::new(history(8, 128 * 1024, 1)?);
    publish(&history, fact(1, 0))?;
    let outer = Arc::new(Mutex::new(()));
    let calls = Arc::new(AtomicUsize::new(0));
    let locked = Arc::new(AtomicBool::new(false));
    let (callback_outer, callback_calls, callback_locked) =
        (outer.clone(), calls.clone(), locked.clone());
    let weak = Arc::downgrade(&history);
    let error = error_with_drop_probe(move || {
        callback_calls.fetch_add(1, Ordering::SeqCst);
        if callback_outer.try_lock().is_err()
            || weak
                .upgrade()
                .is_some_and(|history| !history.can_lock_for_test())
        {
            callback_locked.store(true, Ordering::SeqCst);
        }
    });
    let mut invalid = fact(1, 0);
    let ConnectionEventKind::AttemptStarted { attempt } = invalid.kind else {
        return Err("fixture missing attempt".into());
    };
    invalid.kind = ConnectionEventKind::AttemptFailed {
        attempt,
        credential_version: None,
        error,
        retry: RetryDecision::Stop,
    };
    let guard = outer.lock().map_err(NetError::from_poison)?;
    let publication = history.prepare_record(invalid);
    check_eq!(
        publication
            .result()
            .err()
            .ok_or("duplicate sequence accepted")?
            .kind(),
        ErrorKind::InvalidInput
    )?;
    check_eq!(calls.load(Ordering::SeqCst), 0)?;
    drop(guard);
    check_eq!(
        publication
            .dispatch()
            .err()
            .ok_or("rejected input became successful")?
            .kind(),
        ErrorKind::InvalidInput
    )?;
    check_eq!(calls.load(Ordering::SeqCst), 1)?;
    check!(!locked.load(Ordering::SeqCst))?;
    publish(&history, fact(2, 0))?;
    Ok(())
}

#[test]
fn publication_cannot_cross_the_history_seed_and_live_registration_boundary() -> TestResult {
    let history = Arc::new(history(8, 128 * 1024, 1)?);
    publish(&history, fact(1, 0))?;
    let (seeded_tx, seeded_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (subscribed_tx, subscribed_rx) = std::sync::mpsc::channel();
    let subscribing = history.clone();
    let subscriber = std::thread::Builder::new()
        .name("history-seed-boundary".to_owned())
        .spawn(move || {
            let result = subscribing.subscribe_with_seed_hook_for_test(|| {
                seeded_tx
                    .send(subscribing.can_lock_for_test())
                    .map_err(|error| NetError::with_source(ErrorKind::Internal, error))?;
                release_rx
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|error| NetError::with_source(ErrorKind::TimedOut, error))?;
                Ok(())
            });
            if subscribed_tx.send(result).is_err() {
                eprintln!("history subscription test observer closed");
            }
        })?;
    check!(!seeded_rx.recv_timeout(Duration::from_secs(5))?)?;
    let (attempted_tx, attempted_rx) = std::sync::mpsc::channel();
    let (recorded_tx, recorded_rx) = std::sync::mpsc::channel();
    let recording = history.clone();
    let publisher = std::thread::Builder::new()
        .name("history-live-boundary".to_owned())
        .spawn(move || {
            if attempted_tx.send(()).is_err() {
                eprintln!("history publication test observer closed");
                return;
            }
            let result = publish(&recording, fact(2, 0));
            if recorded_tx.send(result).is_err() {
                eprintln!("history record test observer closed");
            }
        })?;
    attempted_rx.recv_timeout(Duration::from_secs(5))?;
    check!(matches!(
        recorded_rx.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ))?;
    release_tx.send(())?;
    let mut receiver = subscribed_rx.recv_timeout(Duration::from_secs(5))??;
    recorded_rx.recv_timeout(Duration::from_secs(5))??;
    subscriber
        .join()
        .map_err(|_| "history seed worker unwound")?;
    publisher
        .join()
        .map_err(|_| "history live worker unwound")?;
    next(&mut receiver, 1)?;
    next(&mut receiver, 2)?;
    check!(matches!(receiver.try_recv(), Err(TryReceiveError::Empty)))?;
    Ok(())
}

use super::*;
use crate::subscription::event::{event_channel, EventPublisher, EventReceiver, EventSeed};
use futures::{executor::block_on, task::noop_waker_ref};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;

type TestResult<T = ()> = std::result::Result<T, crate::BoxError>;

fn check(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[derive(Default)]
struct ManualExecutor {
    jobs: Mutex<VecDeque<Box<dyn FnOnce() + Send>>>,
}
impl CallbackExecutor for ManualExecutor {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
        self.jobs
            .lock()
            .map_err(NetError::from_poison)?
            .push_back(job);
        Ok(())
    }
}
impl ManualExecutor {
    fn drain(&self) -> TestResult {
        loop {
            let job = self.jobs.lock().map_err(|_| "executor lock")?.pop_front();
            match job {
                Some(job) => job(),
                None => return Ok(()),
            }
        }
    }
    fn len(&self) -> TestResult<usize> {
        Ok(self.jobs.lock().map_err(|_| "executor lock")?.len())
    }
}
fn channel<T>(
    items: usize,
    bytes: usize,
    policy: EventOverflow,
) -> TestResult<(EventPublisher<T>, EventReceiver<T>, Arc<ManualExecutor>)> {
    let executor = Arc::new(ManualExecutor::default());
    let (publisher, receiver) = event_channel(
        EventQueueLimit {
            max_items: items,
            max_bytes: bytes,
        },
        policy,
        executor.clone(),
        None,
    )?;
    Ok((publisher, receiver, executor))
}
fn seed<T>(
    entries: impl IntoIterator<Item = (T, usize)>,
    lagged: u64,
    closed: bool,
    failure: Option<NetError>,
) -> EventSeed<T> {
    EventSeed {
        entries: entries.into_iter().collect(),
        lagged,
        source_closed: closed,
        failure,
    }
}

#[test]
fn reversed_dispatch_preserves_committed_order_and_defers_callback_submission() -> TestResult {
    let (publisher, receiver, executor) = channel(3, 3, EventOverflow::DropOldest)?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let capture = seen.clone();
    let subscription = receiver.into_callback(move |_, value| {
        if let (Ok(value), Ok(mut seen)) = (value, capture.lock()) {
            seen.push(value);
        }
    })?;
    let outer = Mutex::new(());
    let guard = outer.lock().map_err(|_| "outer lock")?;
    let first = publisher.prepare_try_publish(1_u8, 1);
    let second = publisher.prepare_try_publish(2, 1);
    let closed = publisher.prepare_finish();
    first.result()?;
    second.result()?;
    closed.result()?;
    check(
        executor.len()? == 0,
        "prepare submitted a callback under the outer lock",
    )?;
    drop(guard);
    closed.dispatch()?;
    second.dispatch()?;
    first.dispatch()?;
    check(
        executor.len()? == 1,
        "reversed dispatch duplicated the runner",
    )?;
    executor.drain()?;
    check(
        *seen.lock().map_err(|_| "seen lock")? == vec![1, 2],
        "dispatch reordered facts",
    )?;
    block_on(subscription.close())?;
    check(
        !publisher.is_active(),
        "closed callback registration stayed active",
    )
}

struct DropProbe {
    outer: Arc<Mutex<()>>,
    registration: Weak<EventRegistration<DropProbe>>,
    drops: Arc<AtomicUsize>,
    blocked: Arc<AtomicUsize>,
}
impl Drop for DropProbe {
    fn drop(&mut self) {
        if self.outer.try_lock().is_err() {
            self.blocked.fetch_add(1, Ordering::SeqCst);
        }
        if self
            .registration
            .upgrade()
            .is_some_and(|registration| registration.state.try_lock().is_err())
        {
            self.blocked.fetch_add(1, Ordering::SeqCst);
        }
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
fn probe(
    receiver: &EventReceiver<DropProbe>,
    outer: &Arc<Mutex<()>>,
    drops: &Arc<AtomicUsize>,
    blocked: &Arc<AtomicUsize>,
) -> DropProbe {
    DropProbe {
        outer: outer.clone(),
        registration: Arc::downgrade(&receiver.registration),
        drops: drops.clone(),
        blocked: blocked.clone(),
    }
}

#[test]
fn prepare_retains_evicted_rejected_and_failed_reservation_values_until_dispatch() -> TestResult {
    let (publisher, receiver, _) = channel(1, 1, EventOverflow::DropOldest)?;
    let outer = Arc::new(Mutex::new(()));
    let drops = Arc::new(AtomicUsize::new(0));
    let blocked = Arc::new(AtomicUsize::new(0));
    publisher.try_publish(probe(&receiver, &outer, &drops, &blocked), 1)?;
    let guard = outer.lock().map_err(|_| "outer lock")?;
    let replacement = publisher.prepare_try_publish(probe(&receiver, &outer, &drops, &blocked), 1);
    replacement.result()?;
    let oversized = publisher.prepare_try_publish(probe(&receiver, &outer, &drops, &blocked), 2);
    check(
        matches!(oversized.result(), Err(error) if error.kind() == ErrorKind::ItemTooLarge),
        "oversized input accepted",
    )?;
    lock(&receiver.registration.state).fail_retirement_reserve = true;
    let failed = publisher.prepare_try_publish(probe(&receiver, &outer, &drops, &blocked), 1);
    check(
        matches!(failed.result(), Err(error) if error.kind() == ErrorKind::ResourceExhausted),
        "reservation injection was ignored",
    )?;
    check(
        drops.load(Ordering::SeqCst) == 0,
        "prepare retired user input",
    )?;
    drop(guard);
    replacement.dispatch()?;
    check(
        oversized.dispatch().is_err(),
        "dispatch lost admission error",
    )?;
    check(
        failed.dispatch().is_err(),
        "dispatch lost allocation failure",
    )?;
    check(
        drops.load(Ordering::SeqCst) == 3,
        "publication retained retired inputs",
    )?;
    check(receiver.unsubscribe(), "receiver did not unsubscribe")?;
    check(
        drops.load(Ordering::SeqCst) == 4,
        "failed prepare changed queued ownership",
    )?;
    check(
        blocked.load(Ordering::SeqCst) == 0,
        "user destructor ran under a lock",
    )
}

#[test]
fn atomic_seed_delivers_gap_then_entries_then_failure_then_eof() -> TestResult {
    let (publisher, mut receiver, _) = channel(3, 5, EventOverflow::DropOldest)?;
    let publication = publisher.prepare_seed(seed(
        [(7_u8, 2), (8, 3)],
        6,
        true,
        Some(NetError::from(ErrorKind::Io)),
    ));
    publication.result()?;
    publication.dispatch()?;
    check(
        matches!(
            receiver.try_recv(),
            Err(TryReceiveError::Lagged { skipped: 6 })
        ),
        "seed gap missing",
    )?;
    check(
        receiver.try_recv()? == 7 && receiver.try_recv()? == 8,
        "seed entries reordered",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Failed(error)) if error.kind() == ErrorKind::Io),
        "seed terminal failure missing",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Closed)),
        "seed terminal EOF missing",
    )?;
    check(
        !publisher.is_active(),
        "seed EOF did not retire registration",
    )
}

#[test]
fn seed_rejects_receivers_after_empty_poll_drain_or_prior_empty_seed() -> TestResult {
    let (publisher, mut receiver, _) = channel::<u8>(2, 2, EventOverflow::DropOldest)?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Empty)),
        "fresh queue was not empty",
    )?;
    check(
        matches!(publisher.prepare_seed(seed([(1, 1)], 0, false, None)).dispatch(), Err(error) if error.kind() == ErrorKind::InvalidInput),
        "empty receive allowed seed reset",
    )?;
    publisher.try_publish(2, 1)?;
    check(receiver.try_recv()? == 2, "rejected seed changed queue")?;
    check(
        publisher
            .prepare_seed(seed([(3, 1)], 0, false, None))
            .dispatch()
            .is_err(),
        "drained receiver allowed seed reset",
    )?;
    let (fresh, _receiver, _) = channel::<u8>(1, 1, EventOverflow::DropOldest)?;
    fresh.prepare_seed(seed([], 0, false, None)).dispatch()?;
    check(
        matches!(fresh.prepare_seed(seed([], 0, false, None)).dispatch(), Err(error) if error.kind() == ErrorKind::InvalidInput),
        "empty seed was reusable",
    )
}

#[test]
fn skip_clears_pending_prefix_counts_exact_loss_and_preserves_capacity() -> TestResult {
    let (publisher, mut receiver, _) = channel(2, 2, EventOverflow::DropOldest)?;
    publisher
        .prepare_seed(seed([(1_u8, 1), (2, 1)], 3, false, None))
        .dispatch()?;
    let capacity = lock(&receiver.registration.state).queue.capacity();
    publisher.prepare_skip(2).dispatch()?;
    check(
        lock(&receiver.registration.state).queue.capacity() == capacity,
        "skip discarded reserved queue capacity",
    )?;
    publisher.try_publish(5, 2)?;
    check(
        matches!(
            receiver.try_recv(),
            Err(TryReceiveError::Lagged { skipped: 7 })
        ),
        "skip did not count old gap, queue and explicit loss",
    )?;
    check(receiver.try_recv()? == 5, "post-gap event missing")?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Empty)),
        "skipped prefix survived",
    )
}

#[test]
fn skip_rejects_other_policies_and_count_or_allocation_failures_are_transactional() -> TestResult {
    for policy in [EventOverflow::Wait, EventOverflow::Disconnect] {
        let (publisher, mut receiver, _) = channel(1, 1, policy)?;
        publisher.try_publish(9_u8, 1)?;
        check(
            matches!(publisher.prepare_skip(1).dispatch(), Err(error) if error.kind() == ErrorKind::InvalidInput),
            "skip accepted a non-lossy policy",
        )?;
        check(
            receiver.try_recv()? == 9,
            "rejected skip changed reliable queue",
        )?;
    }
    let (publisher, mut receiver, _) = channel(1, 1, EventOverflow::DropOldest)?;
    publisher
        .prepare_seed(seed([(8_u8, 1)], u64::MAX, false, None))
        .dispatch()?;
    check(
        matches!(publisher.prepare_skip(1).dispatch(), Err(error) if error.kind() == ErrorKind::ResourceExhausted),
        "lag count wrapped",
    )?;
    check(
        matches!(
            receiver.try_recv(),
            Err(TryReceiveError::Lagged { skipped: u64::MAX })
        ),
        "count failure changed previous lag",
    )?;
    lock(&receiver.registration.state).fail_retirement_reserve = true;
    check(
        matches!(publisher.prepare_skip(1).dispatch(), Err(error) if error.kind() == ErrorKind::ResourceExhausted),
        "skip ignored allocation failure",
    )?;
    check(
        receiver.try_recv()? == 8,
        "failed skip evicted original queue",
    )
}

struct WakeProbe {
    outer: Arc<Mutex<()>>,
    calls: AtomicUsize,
    blocked: AtomicUsize,
}
impl Wake for WakeProbe {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if self.outer.try_lock().is_err() {
            self.blocked.fetch_add(1, Ordering::SeqCst);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn prepare_finish_defers_receive_wake_until_outer_lock_is_released() -> TestResult {
    let (publisher, mut receiver, _) = channel::<u8>(1, 1, EventOverflow::Wait)?;
    let outer = Arc::new(Mutex::new(()));
    let wake = Arc::new(WakeProbe {
        outer: outer.clone(),
        calls: AtomicUsize::new(0),
        blocked: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut receiving = Box::pin(receiver.recv());
    check(
        receiving.as_mut().poll(&mut cx).is_pending(),
        "empty receiver completed",
    )?;
    let guard = outer.lock().map_err(|_| "outer lock")?;
    let publication = publisher.prepare_finish();
    check(
        wake.calls.load(Ordering::SeqCst) == 0,
        "prepare woke receiver",
    )?;
    drop(guard);
    publication.dispatch()?;
    check(
        wake.calls.load(Ordering::SeqCst) == 1 && wake.blocked.load(Ordering::SeqCst) == 0,
        "receive wake held outer lock",
    )?;
    check(
        matches!(receiving.as_mut().poll(&mut cx), Poll::Ready(Ok(None))),
        "finish did not close pending receive",
    )
}

#[test]
fn prepared_wait_rejection_keeps_existing_async_capacity_and_cancel_safety() -> TestResult {
    let (publisher, mut receiver, _) = channel(1, 1, EventOverflow::Wait)?;
    publisher.prepare_try_publish(1_u8, 1).dispatch()?;
    check(
        matches!(publisher.prepare_try_publish(2, 1).dispatch(), Err(error) if error.kind() == ErrorKind::QueueFull),
        "prepared Wait rejection changed classification",
    )?;
    let mut cx = Context::from_waker(noop_waker_ref());
    let mut pending = Box::pin(publisher.publish(3, 1));
    check(
        pending.as_mut().poll(&mut cx).is_pending(),
        "full async publisher did not wait",
    )?;
    drop(pending);
    check(
        receiver.try_recv()? == 1,
        "cancelled publisher overwrote queued item",
    )?;
    publisher.prepare_try_publish(4, 1).dispatch()?;
    check(
        receiver.try_recv()? == 4,
        "cancelled publisher stranded capacity",
    )
}

#[test]
fn failed_seed_retains_every_input_until_dispatch_and_does_not_consume_freshness() -> TestResult {
    let (publisher, receiver, _) = channel(2, 1, EventOverflow::DropOldest)?;
    let outer = Arc::new(Mutex::new(()));
    let drops = Arc::new(AtomicUsize::new(0));
    let blocked = Arc::new(AtomicUsize::new(0));
    let guard = outer.lock().map_err(|_| "outer lock")?;
    let publication = publisher.prepare_seed(seed(
        [
            (probe(&receiver, &outer, &drops, &blocked), 1),
            (probe(&receiver, &outer, &drops, &blocked), 1),
        ],
        4,
        false,
        None,
    ));
    check(
        matches!(publication.result(), Err(error) if error.kind() == ErrorKind::QueueFull),
        "aggregate seed byte overflow accepted",
    )?;
    check(
        drops.load(Ordering::SeqCst) == 0,
        "rejected seed dropped inputs under outer lock",
    )?;
    drop(guard);
    check(publication.dispatch().is_err(), "seed admission error lost")?;
    check(
        drops.load(Ordering::SeqCst) == 2 && blocked.load(Ordering::SeqCst) == 0,
        "seed cleanup held a lock or retained input",
    )?;
    publisher
        .prepare_seed(seed(
            [(probe(&receiver, &outer, &drops, &blocked), 1)],
            0,
            false,
            None,
        ))
        .dispatch()?;
    check(
        receiver.unsubscribe(),
        "seeded receiver did not unsubscribe",
    )?;
    check(
        drops.load(Ordering::SeqCst) == 3,
        "valid seed did not own its event",
    )
}

#[derive(Debug)]
struct ErrorDropProbe {
    outer: Arc<Mutex<()>>,
    registration: Weak<EventRegistration<u8>>,
    drops: Arc<AtomicUsize>,
    blocked: Arc<AtomicUsize>,
}
impl fmt::Display for ErrorDropProbe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("error drop probe")
    }
}
impl std::error::Error for ErrorDropProbe {}
impl Drop for ErrorDropProbe {
    fn drop(&mut self) {
        if self.outer.try_lock().is_err() {
            self.blocked.fetch_add(1, Ordering::SeqCst);
        }
        if self
            .registration
            .upgrade()
            .is_some_and(|registration| registration.state.try_lock().is_err())
        {
            self.blocked.fetch_add(1, Ordering::SeqCst);
        }
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn redundant_failure_keeps_last_user_error_source_until_dispatch() -> TestResult {
    let (publisher, _receiver, _) = channel::<u8>(1, 1, EventOverflow::DropOldest)?;
    publisher.prepare_finish().dispatch()?;
    let outer = Arc::new(Mutex::new(()));
    let drops = Arc::new(AtomicUsize::new(0));
    let blocked = Arc::new(AtomicUsize::new(0));
    let source = ErrorDropProbe {
        outer: outer.clone(),
        registration: Arc::downgrade(&_receiver.registration),
        drops: drops.clone(),
        blocked: blocked.clone(),
    };
    let guard = outer.lock().map_err(|_| "outer lock")?;
    let publication = publisher.prepare_fail(NetError::with_source(ErrorKind::Io, source));
    publication.result()?;
    check(
        drops.load(Ordering::SeqCst) == 0,
        "redundant failure released source in prepare",
    )?;
    drop(guard);
    publication.dispatch()?;
    check(
        drops.load(Ordering::SeqCst) == 1 && blocked.load(Ordering::SeqCst) == 0,
        "failure source was retained or destroyed under a lock",
    )
}

#[test]
fn seed_validation_and_callback_conversion_preserve_the_initial_cursor() -> TestResult {
    for (entries, closed, failure, expected) in [
        (vec![(1_u8, 1), (2, 1)], false, None, ErrorKind::QueueFull),
        (vec![(1, 3)], false, None, ErrorKind::ItemTooLarge),
        (
            vec![],
            false,
            Some(NetError::from(ErrorKind::Io)),
            ErrorKind::InvalidInput,
        ),
    ] {
        let (publisher, _receiver, _) = channel(1, 2, EventOverflow::DropOldest)?;
        check(
            matches!(publisher.prepare_seed(seed(entries, 0, closed, failure)).dispatch(), Err(error) if error.kind() == expected),
            "seed validation classification changed",
        )?;
    }
    let (publisher, receiver, executor) = channel(2, 2, EventOverflow::DropOldest)?;
    let original_id = receiver.id();
    publisher
        .prepare_seed(seed(
            [(5_u8, 1), (6, 1)],
            4,
            true,
            Some(NetError::from(ErrorKind::Io)),
        ))
        .dispatch()?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let capture = seen.clone();
    let subscription = receiver.into_callback(move |context, item| {
        let value = match item {
            Ok(value) => (context.id(), u64::from(value)),
            Err(ReceiveError::Lagged { skipped }) => (context.id(), skipped),
            Err(ReceiveError::Failed(_)) => (context.id(), 0),
        };
        if let Ok(mut seen) = capture.lock() {
            seen.push(value);
        }
    })?;
    check(
        subscription.id() == original_id,
        "seed conversion changed subscription identity",
    )?;
    executor.drain()?;
    check(
        *seen.lock().map_err(|_| "seen lock")?
            == vec![
                (original_id, 4),
                (original_id, 5),
                (original_id, 6),
                (original_id, 0),
            ],
        "callback did not consume the seed cursor exactly once",
    )?;
    check(
        matches!(block_on(subscription.close()), Err(error) if error.kind() == ErrorKind::Io),
        "seed error was lost from close",
    )
}

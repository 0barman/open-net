use super::*;
use crate::error::{ReceiveError, TryReceiveError};
use futures::{executor::block_on, task::noop_waker_ref, Stream};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

type TestResult<T = ()> = std::result::Result<T, crate::BoxError>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
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
    fn next(&self) -> TestResult<Option<Box<dyn FnOnce() + Send>>> {
        Ok(self
            .jobs
            .lock()
            .map_err(|_| "executor queue poisoned")?
            .pop_front())
    }

    fn drain(&self) -> TestResult {
        while let Some(job) = self.next()? {
            job();
        }
        Ok(())
    }
}

fn channel<T>(
    max_items: usize,
    max_bytes: usize,
    overflow: EventOverflow,
) -> TestResult<(EventPublisher<T>, EventReceiver<T>, Arc<ManualExecutor>)> {
    let executor = Arc::new(ManualExecutor::default());
    let (publisher, receiver) = event_channel(
        EventQueueLimit {
            max_items,
            max_bytes,
        },
        overflow,
        executor.clone(),
        None,
    )?;
    Ok((publisher, receiver, executor))
}

struct OwnedEvent {
    id: u8,
    drops: Arc<AtomicUsize>,
}

impl OwnedEvent {
    fn new(id: u8, drops: &Arc<AtomicUsize>) -> Self {
        Self {
            id,
            drops: drops.clone(),
        }
    }
}

impl Drop for OwnedEvent {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn non_clone_events_move_out_and_release_byte_capacity_before_application_drop() -> TestResult {
    let (publisher, mut receiver, _) = channel(2, 2, EventOverflow::Wait)?;
    let drops = Arc::new(AtomicUsize::new(0));
    publisher.try_publish(OwnedEvent::new(1, &drops), 2)?;
    let held = receiver.try_recv()?;
    check(
        held.id == 1,
        "non-Clone event was not moved to the application",
    )?;
    publisher.try_publish(OwnedEvent::new(2, &drops), 2)?;
    check(
        drops.load(Ordering::SeqCst) == 0,
        "delivery dropped an application-owned event",
    )?;
    check(receiver.unsubscribe(), "unsubscribe did not close delivery")?;
    check(
        drops.load(Ordering::SeqCst) == 1,
        "unsubscribe retained queued payload",
    )?;
    drop(held);
    check(
        drops.load(Ordering::SeqCst) == 2,
        "application payload was retained by the queue",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Closed)),
        "unsubscribed queue stayed open",
    )
}

#[test]
fn cancelled_receive_keeps_an_arrived_event_and_last_publisher_drop_fuses_stream() -> TestResult {
    let (publisher, mut receiver, _) = channel(2, 2, EventOverflow::Wait)?;
    let last_publisher = publisher.clone();
    let mut cx = Context::from_waker(noop_waker_ref());
    let mut receiving = Box::pin(receiver.recv());
    check(
        matches!(receiving.as_mut().poll(&mut cx), Poll::Pending),
        "empty recv was ready",
    )?;
    publisher.try_publish(7_u8, 1)?;
    drop(receiving);
    drop(publisher);
    check(
        receiver.try_recv()? == 7,
        "cancelled future consumed the arrived event",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Empty)),
        "one publisher Drop ended its live clone",
    )?;
    last_publisher.try_publish(8, 1)?;
    drop(last_publisher);
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(Some(Ok(8)))
        ),
        "last publisher Drop discarded buffered event",
    )?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(None)
        ),
        "publisher ownership was retained by receiver",
    )?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(None)
        ),
        "event stream reopened after EOF",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Closed)),
        "try_recv conflated EOF and empty",
    )
}

#[derive(Debug, PartialEq)]
enum Seen {
    Event(u8),
    Lagged(u64),
    Failed(ErrorKind),
}

#[test]
fn successive_byte_evictions_report_each_gap_before_surviving_events_after_conversion() -> TestResult
{
    let (publisher, mut receiver, executor) = channel(3, 6, EventOverflow::DropOldest)?;
    publisher.try_publish(1_u8, 2)?;
    publisher.try_publish(2, 2)?;
    publisher.try_publish(3, 2)?;
    publisher.try_publish(4, 3)?;
    check(
        matches!(
            receiver.try_recv(),
            Err(TryReceiveError::Lagged { skipped: 2 })
        ),
        "byte eviction did not report exactly its two-item prefix",
    )?;
    publisher.try_publish(5, 2)?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let capture = seen.clone();
    let subscription = receiver.into_callback(move |_, event| {
        let item = match event {
            Ok(value) => Seen::Event(value),
            Err(ReceiveError::Lagged { skipped }) => Seen::Lagged(skipped),
            Err(ReceiveError::Failed(error)) => Seen::Failed(error.kind()),
        };
        match capture.lock() {
            Ok(mut items) => items.push(item),
            Err(error) => eprintln!("event observation capture failed: {error}"),
        }
    })?;
    publisher.finish()?;
    executor.drain()?;
    check(
        *seen.lock().map_err(|_| "observation lock poisoned")?
            == vec![Seen::Lagged(1), Seen::Event(4), Seen::Event(5)],
        "callback conversion reordered or repeated the pending gap",
    )?;
    block_on(subscription.close())?;
    Ok(())
}

#[test]
fn oversized_event_rejection_preserves_queue_and_does_not_report_false_loss() -> TestResult {
    let (publisher, mut receiver, _) = channel(1, 2, EventOverflow::DropOldest)?;
    publisher.try_publish(1_u8, 2)?;
    check(
        matches!(publisher.try_publish(2, 3), Err(error) if error.kind() == ErrorKind::ItemTooLarge),
        "oversized event was not rejected as ItemTooLarge",
    )?;
    check(
        receiver.try_recv()? == 1,
        "oversized rejection evicted the valid queued event",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Empty)),
        "rejected input manufactured a Lagged event",
    )?;
    publisher.try_publish(3, 2)?;
    check(
        receiver.try_recv()? == 3,
        "oversized rejection closed the receiver",
    )
}

#[test]
fn unsubscribe_releases_waiting_publisher_and_both_owned_payloads() -> TestResult {
    let (publisher, mut receiver, _) = channel(1, 1, EventOverflow::Wait)?;
    let drops = Arc::new(AtomicUsize::new(0));
    publisher.try_publish(OwnedEvent::new(1, &drops), 1)?;
    let mut pending = Box::pin(publisher.publish(OwnedEvent::new(2, &drops), 1));
    let mut cx = Context::from_waker(noop_waker_ref());
    check(
        matches!(pending.as_mut().poll(&mut cx), Poll::Pending),
        "full Wait queue did not wait",
    )?;
    check(
        receiver.unsubscribe(),
        "unsubscribe lost its active registration",
    )?;
    check(
        matches!(pending.as_mut().poll(&mut cx), Poll::Ready(Err(error)) if error.kind() == ErrorKind::Closed),
        "unsubscribe did not release blocked queue admission",
    )?;
    drop(pending);
    check(
        drops.load(Ordering::SeqCst) == 2,
        "unsubscribe retained queued or rejected waiting payload",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Closed)),
        "cancelled queue could still deliver",
    )
}

struct CountWake(AtomicUsize);

impl std::task::Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn cancelling_one_of_multiple_waiting_publishers_preserves_wakeups_and_capacity() -> TestResult {
    let (publisher, mut receiver, _) = channel(1, 1, EventOverflow::Wait)?;
    let drops = Arc::new(AtomicUsize::new(0));
    publisher.try_publish(OwnedEvent::new(1, &drops), 1)?;
    let mut first = Box::pin(publisher.publish(OwnedEvent::new(2, &drops), 1));
    let mut cancelled = Box::pin(publisher.publish(OwnedEvent::new(3, &drops), 1));
    let mut last = Box::pin(publisher.publish(OwnedEvent::new(4, &drops), 1));
    let first_wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let last_wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let first_waker = std::task::Waker::from(first_wake.clone());
    let last_waker = std::task::Waker::from(last_wake.clone());
    let mut first_cx = Context::from_waker(&first_waker);
    let mut last_cx = Context::from_waker(&last_waker);
    let mut cancelled_cx = Context::from_waker(noop_waker_ref());
    check(
        matches!(first.as_mut().poll(&mut first_cx), Poll::Pending)
            && matches!(cancelled.as_mut().poll(&mut cancelled_cx), Poll::Pending)
            && matches!(last.as_mut().poll(&mut last_cx), Poll::Pending),
        "all publishers must wait behind the same full queue",
    )?;
    drop(cancelled);
    check(
        drops.load(Ordering::SeqCst) == 1,
        "cancelled publisher retained its unadmitted payload",
    )?;
    let initial = receiver.try_recv()?;
    check(initial.id == 1, "waiting input replaced the queued event")?;
    drop(initial);
    check(
        first_wake.0.load(Ordering::SeqCst) > 0 && last_wake.0.load(Ordering::SeqCst) > 0,
        "cancelling one waiter consumed another publisher's capacity wakeup",
    )?;
    check(
        matches!(first.as_mut().poll(&mut first_cx), Poll::Ready(Ok(()))),
        "first surviving publisher did not acquire released capacity",
    )?;
    check(
        matches!(last.as_mut().poll(&mut last_cx), Poll::Pending),
        "concurrent publisher exceeded the single-event queue limit",
    )?;
    let last_wakes_before = last_wake.0.load(Ordering::SeqCst);
    let second = receiver.try_recv()?;
    check(second.id == 2, "first surviving publisher's event was lost")?;
    drop(second);
    check(
        last_wake.0.load(Ordering::SeqCst) > last_wakes_before,
        "remaining publisher was not woken after re-registering for capacity",
    )?;
    check(
        matches!(last.as_mut().poll(&mut last_cx), Poll::Ready(Ok(()))),
        "last surviving publisher stayed blocked despite available capacity",
    )?;
    let final_event = receiver.try_recv()?;
    check(
        final_event.id == 4,
        "cancelled input entered the event stream",
    )?;
    drop(final_event);
    drop((first, last));
    check(
        drops.load(Ordering::SeqCst) == 4,
        "a cancelled or delivered payload was retained by a completed publisher",
    )?;
    publisher.finish()?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Closed)),
        "completed publishers left an unexpected buffered event",
    )
}

#[test]
fn queued_callback_cancellation_keeps_pool_permit_until_the_cancelled_job_retires() -> TestResult {
    let executor = Arc::new(ManualExecutor::default());
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned()?;
    let (publisher, receiver) = event_channel(
        EventQueueLimit {
            max_items: 1,
            max_bytes: 1,
        },
        EventOverflow::Wait,
        executor.clone(),
        Some(permit),
    )?;
    let calls = Arc::new(AtomicUsize::new(0));
    let capture = calls.clone();
    let subscription =
        receiver.into_callback(move |_, _: std::result::Result<u8, ReceiveError>| {
            capture.fetch_add(1, Ordering::SeqCst);
        })?;
    publisher.try_publish(1, 1)?;
    check(subscription.unsubscribe(), "callback unsubscribe lost")?;
    check(
        slots.available_permits() == 0,
        "cancelled queued job released capacity before retirement",
    )?;
    executor.drain()?;
    check(
        calls.load(Ordering::SeqCst) == 0,
        "unsubscribed queued job acquired an event",
    )?;
    check(
        slots.available_permits() == 1,
        "retired queued job leaked its permit",
    )?;
    block_on(subscription.close())?;
    Ok(())
}

struct RejectReady;
impl CallbackExecutor for RejectReady {
    fn ensure_ready(&self) -> Result<()> {
        Err(NetError::from(ErrorKind::RuntimeUnavailable))
    }
    fn submit(&self, _job: Box<dyn FnOnce() + Send>) -> Result<()> {
        Err(NetError::from(ErrorKind::Internal))
    }
}

#[test]
fn failed_callback_conversion_closes_receiver_and_releases_queued_payload_and_quota() -> TestResult
{
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned()?;
    let drops = Arc::new(AtomicUsize::new(0));
    let (publisher, receiver) = event_channel(
        EventQueueLimit {
            max_items: 1,
            max_bytes: 1,
        },
        EventOverflow::Wait,
        Arc::new(RejectReady),
        Some(permit),
    )?;
    publisher.try_publish(OwnedEvent::new(1, &drops), 1)?;
    check(
        matches!(receiver.into_callback(|_, _| {}), Err(error) if error.kind() == ErrorKind::RuntimeUnavailable),
        "callback conversion replaced executor initialization failure",
    )?;
    check(
        drops.load(Ordering::SeqCst) == 1 && slots.available_permits() == 1,
        "failed conversion retained buffered event or subscription quota",
    )?;
    check(
        matches!(publisher.try_publish(OwnedEvent::new(2, &drops), 1), Err(error) if error.kind() == ErrorKind::Closed),
        "failed conversion left consumed receiver alive",
    )
}

struct RejectSubmit;
impl CallbackExecutor for RejectSubmit {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, _job: Box<dyn FnOnce() + Send>) -> Result<()> {
        Err(NetError::from(ErrorKind::QueueFull))
    }
}

#[test]
fn callback_scheduling_failure_is_returned_by_publish_and_close() -> TestResult {
    let (publisher, receiver) = event_channel(
        EventQueueLimit {
            max_items: 1,
            max_bytes: 1,
        },
        EventOverflow::Wait,
        Arc::new(RejectSubmit),
        None,
    )?;
    let calls = Arc::new(AtomicUsize::new(0));
    let capture = calls.clone();
    let subscription =
        receiver.into_callback(move |_, _: std::result::Result<u8, ReceiveError>| {
            capture.fetch_add(1, Ordering::SeqCst);
        })?;
    check(
        matches!(publisher.try_publish(1, 1), Err(error) if error.kind() == ErrorKind::QueueFull),
        "publish hid callback scheduling failure",
    )?;
    check(
        !subscription.is_active() && calls.load(Ordering::SeqCst) == 0,
        "failed callback submission still delivered",
    )?;
    check(
        matches!(block_on(subscription.close()), Err(error) if error.kind() == ErrorKind::QueueFull),
        "close hid callback scheduling failure",
    )
}

#[test]
fn close_waits_for_acquired_callback_and_preserves_source_failure_without_a_second_acquire(
) -> TestResult {
    let (publisher, receiver, executor) = channel(2, 2, EventOverflow::Wait)?;
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let calls = Arc::new(AtomicUsize::new(0));
    let capture = calls.clone();
    let subscription =
        receiver.into_callback(move |_, _: std::result::Result<u8, ReceiveError>| {
            if capture.fetch_add(1, Ordering::SeqCst) == 0 {
                if let Err(error) = started_tx.send(()) {
                    eprintln!("callback start delivery failed: {error}");
                }
                match release_rx.lock() {
                    Ok(release) => {
                        if let Err(error) = release.recv() {
                            eprintln!("callback release failed: {error}");
                        }
                    }
                    Err(error) => eprintln!("callback release gate failed: {error}"),
                }
            }
        })?;
    publisher.try_publish(1, 1)?;
    let job = executor.next()?.ok_or("callback job missing")?;
    let worker = std::thread::Builder::new()
        .name("event-acquired-close".into())
        .spawn(job)?;
    started_rx.recv_timeout(Duration::from_secs(5))?;
    publisher.try_publish(2, 1)?;
    publisher.fail(NetError::from(ErrorKind::Protocol))?;
    let mut closing = Box::pin(subscription.close());
    let mut cx = Context::from_waker(noop_waker_ref());
    let before = closing.as_mut().poll(&mut cx);
    release_tx.send(())?;
    worker.join().map_err(|_| "event callback worker unwound")?;
    let after = closing.as_mut().poll(&mut cx);
    check(
        matches!(before, Poll::Pending),
        "close returned before its acquired callback completed",
    )?;
    check(
        matches!(after, Poll::Ready(Err(error)) if error.kind() == ErrorKind::Protocol),
        "close lost source failure after callback retirement",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 1,
        "unsubscribe allowed a second callback acquire",
    )
}

struct LockProbeEvent {
    unlocked: Arc<dyn Fn() -> bool + Send + Sync>,
    observations: Arc<Mutex<Vec<bool>>>,
}

impl Drop for LockProbeEvent {
    fn drop(&mut self) {
        let unlocked = (self.unlocked)();
        match self.observations.lock() {
            Ok(mut observations) => observations.push(unlocked),
            Err(error) => eprintln!("event Drop probe capture failed: {error}"),
        }
    }
}

#[test]
fn prefix_eviction_and_unsubscribe_drop_payloads_after_unlocking_registration() -> TestResult {
    let (publisher, receiver, _) = channel::<LockProbeEvent>(1, 1, EventOverflow::DropOldest)?;
    let registration = Arc::downgrade(&receiver.registration);
    let unlocked: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
        registration
            .upgrade()
            .is_none_or(|registration| registration.state.try_lock().is_ok())
    });
    let observations = Arc::new(Mutex::new(Vec::new()));
    for _ in 0..2 {
        publisher.try_publish(
            LockProbeEvent {
                unlocked: unlocked.clone(),
                observations: observations.clone(),
            },
            1,
        )?;
    }
    check(
        *observations
            .lock()
            .map_err(|_| "Drop observation lock poisoned")?
            == vec![true],
        "DropOldest dropped its retired prefix under the registration lock",
    )?;
    check(receiver.unsubscribe(), "unsubscribe did not win")?;
    check(
        *observations
            .lock()
            .map_err(|_| "Drop observation lock poisoned")?
            == vec![true, true],
        "unsubscribe dropped buffered payload under the registration lock",
    )?;
    Ok(())
}

struct LockProbeWaker {
    unlocked: Arc<dyn Fn() -> bool + Send + Sync>,
    observations: Arc<Mutex<Vec<(&'static str, bool)>>>,
}

impl LockProbeWaker {
    fn record(&self, operation: &'static str) {
        let unlocked = (self.unlocked)();
        match self.observations.lock() {
            Ok(mut observations) => observations.push((operation, unlocked)),
            Err(error) => eprintln!("waker probe capture failed: {error}"),
        }
    }
}

impl std::task::Wake for LockProbeWaker {
    fn wake(self: Arc<Self>) {
        self.record("wake");
    }
}

impl Drop for LockProbeWaker {
    fn drop(&mut self) {
        self.record("drop");
    }
}

#[test]
fn replacing_and_waking_receive_wakers_never_runs_user_code_with_queue_locked() -> TestResult {
    let (publisher, mut receiver, _) = channel::<u8>(1, 1, EventOverflow::Wait)?;
    let registration = Arc::downgrade(&receiver.registration);
    let unlocked: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
        registration
            .upgrade()
            .is_none_or(|registration| registration.state.try_lock().is_ok())
    });
    let observations = Arc::new(Mutex::new(Vec::new()));
    let first = std::task::Waker::from(Arc::new(LockProbeWaker {
        unlocked: unlocked.clone(),
        observations: observations.clone(),
    }));
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut Context::from_waker(&first)),
            Poll::Pending
        ),
        "first waker did not wait",
    )?;
    drop(first);
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut Context::from_waker(noop_waker_ref())),
            Poll::Pending
        ),
        "waker replacement consumed an event",
    )?;
    check(
        *observations
            .lock()
            .map_err(|_| "waker observation lock poisoned")?
            == vec![("drop", true)],
        "replacing waker ran its destructor under queue lock",
    )?;
    let second = std::task::Waker::from(Arc::new(LockProbeWaker {
        unlocked,
        observations: observations.clone(),
    }));
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut Context::from_waker(&second)),
            Poll::Pending
        ),
        "second waker did not wait",
    )?;
    drop(second);
    publisher.try_publish(1, 1)?;
    let captured = observations
        .lock()
        .map_err(|_| "waker observation lock poisoned")?;
    check(
        captured.iter().all(|(_, unlocked)| *unlocked)
            && captured.iter().any(|(operation, _)| *operation == "wake"),
        "publisher woke user code with queue locked",
    )?;
    drop(captured);
    check(
        receiver.try_recv()? == 1,
        "waker notification consumed the event",
    )
}

#[test]
fn event_and_state_receivers_allocate_from_one_process_identity_domain() -> TestResult {
    let (_, event, _) = channel::<u8>(1, 1, EventOverflow::Wait)?;
    let (_, source) = StateSource::new(1_u8, Arc::new(ManualExecutor::default()), 1)?;
    let state = source.subscribe()?;
    check(
        event.id().as_u64() != 0 && state.id().as_u64() != 0 && event.id() != state.id(),
        "state and event receivers reused an opaque subscription identity",
    )
}

#[derive(Default)]
struct ReadyActionExecutor {
    action: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl CallbackExecutor for ReadyActionExecutor {
    fn ensure_ready(&self) -> Result<()> {
        let action = self.action.lock().map_err(NetError::from_poison)?.take();
        if let Some(action) = action {
            action();
        }
        Ok(())
    }

    fn submit(&self, _job: Box<dyn FnOnce() + Send>) -> Result<()> {
        Err(NetError::from(ErrorKind::Internal))
    }
}

#[test]
fn unsubscribe_during_executor_initialization_cannot_reactivate_callback_delivery() -> TestResult {
    let executor = Arc::new(ReadyActionExecutor::default());
    let (publisher, receiver) = event_channel(
        EventQueueLimit {
            max_items: 1,
            max_bytes: 1,
        },
        EventOverflow::Wait,
        executor.clone(),
        None,
    )?;
    publisher.try_publish(1_u8, 1)?;
    let registration = Arc::downgrade(&receiver.registration);
    let unsubscribed = Arc::new(AtomicUsize::new(0));
    let changed = unsubscribed.clone();
    *executor
        .action
        .lock()
        .map_err(|_| "ready action lock poisoned")? = Some(Box::new(move || {
        if let Some(registration) = registration.upgrade() {
            if SubscriptionControl::unsubscribe(registration.as_ref()) {
                changed.fetch_add(1, Ordering::SeqCst);
            }
        }
    }));
    let calls = Arc::new(AtomicUsize::new(0));
    let capture = calls.clone();
    let converted = receiver.into_callback(move |_, _| {
        capture.fetch_add(1, Ordering::SeqCst);
    });
    check(
        matches!(converted, Err(error) if error.kind() == ErrorKind::Closed),
        "conversion restored a concurrently unsubscribed registration",
    )?;
    check(
        unsubscribed.load(Ordering::SeqCst) == 1 && calls.load(Ordering::SeqCst) == 0,
        "executor initialization race admitted callback delivery",
    )?;
    check(
        matches!(publisher.try_publish(2, 1), Err(error) if error.kind() == ErrorKind::Closed),
        "unsubscribe lost its closed queue",
    )
}

#[test]
fn executor_abandoning_an_accepted_job_makes_callback_close_fail_explicitly() -> TestResult {
    let (publisher, receiver, executor) = channel(1, 1, EventOverflow::Wait)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let capture = calls.clone();
    let subscription =
        receiver.into_callback(move |_, _: std::result::Result<u8, ReceiveError>| {
            capture.fetch_add(1, Ordering::SeqCst);
        })?;
    publisher.try_publish(1, 1)?;
    let job = executor.next()?.ok_or("accepted callback job missing")?;
    drop(job);
    check(
        !subscription.is_active() && calls.load(Ordering::SeqCst) == 0,
        "abandoned job left callback active or invoked it",
    )?;
    check(
        matches!(block_on(subscription.close()), Err(error) if error.kind() == ErrorKind::RuntimeUnavailable && error.context().stage == Some(crate::error::ErrorStage::Dispatch)),
        "close hid an accepted job that never ran",
    )
}

struct DropSubmittedJob;
impl CallbackExecutor for DropSubmittedJob {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
        drop(job);
        Ok(())
    }
}

#[test]
fn executor_dropping_job_inside_submit_cannot_claim_callback_conversion_succeeded() -> TestResult {
    let (publisher, receiver) = event_channel(
        EventQueueLimit {
            max_items: 1,
            max_bytes: 1,
        },
        EventOverflow::Wait,
        Arc::new(DropSubmittedJob),
        None,
    )?;
    publisher.try_publish(1_u8, 1)?;
    let converted = receiver.into_callback(|_, _| {});
    check(
        matches!(converted, Err(error) if error.kind() == ErrorKind::RuntimeUnavailable && error.context().stage == Some(crate::error::ErrorStage::Dispatch)),
        "inline job abandonment returned a live subscription",
    )?;
    check(
        matches!(publisher.try_publish(2, 1), Err(error) if error.kind() == ErrorKind::Closed),
        "failed callback conversion retained delivery ownership",
    )
}

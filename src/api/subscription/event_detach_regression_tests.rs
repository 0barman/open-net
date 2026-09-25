use super::*;
use crate::error::ErrorStage;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

type TestResult<T = ()> = std::result::Result<T, crate::BoxError>;

fn check(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
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
impl CountWake {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct ManualExecutor(Mutex<VecDeque<Box<dyn FnOnce() + Send>>>);
impl CallbackExecutor for ManualExecutor {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
        let mut jobs = self.0.lock().map_err(NetError::from_poison)?;
        jobs.try_reserve(1)
            .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
        jobs.push_back(job);
        Ok(())
    }
}
impl ManualExecutor {
    fn run_one(&self) -> TestResult {
        let job = self
            .0
            .lock()
            .map_err(NetError::from_poison)?
            .pop_front()
            .ok_or("callback job missing")?;
        job();
        Ok(())
    }
}

fn channel<T>(
    executor: Arc<dyn CallbackExecutor>,
) -> TestResult<(EventPublisher<T>, EventReceiver<T>)> {
    Ok(event_channel(
        EventQueueLimit {
            max_items: 2,
            max_bytes: 64,
        },
        EventOverflow::Wait,
        executor,
        None,
    )?)
}

#[test]
fn receiver_exit_wakes_every_registered_waiter_without_cancelled_waiter_stealing_notification(
) -> TestResult {
    let (publisher, receiver) = channel::<u8>(Arc::new(ManualExecutor::default()))?;
    let first_count = Arc::new(CountWake::default());
    let cancelled_count = Arc::new(CountWake::default());
    let last_count = Arc::new(CountWake::default());
    let first_waker = Waker::from(first_count.clone());
    let cancelled_waker = Waker::from(cancelled_count.clone());
    let last_waker = Waker::from(last_count.clone());
    let mut first = Box::pin(publisher.receiver_closed());
    let mut cancelled = Box::pin(publisher.receiver_closed());
    let mut last = Box::pin(publisher.receiver_closed());
    check(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending(),
        "first waiter completed before detach",
    )?;
    check(
        cancelled
            .as_mut()
            .poll(&mut Context::from_waker(&cancelled_waker))
            .is_pending(),
        "middle waiter completed before detach",
    )?;
    check(
        last.as_mut()
            .poll(&mut Context::from_waker(&last_waker))
            .is_pending(),
        "last waiter completed before detach",
    )?;
    drop(cancelled);
    check(
        receiver.unsubscribe(),
        "first unsubscribe did not revoke the receiver",
    )?;
    check(
        first_count.count() == 1 && last_count.count() == 1,
        "detach failed to wake both live waiters exactly once",
    )?;
    check(
        cancelled_count.count() == 0,
        "cancelled waiter retained a detach notification",
    )?;
    check(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_ready(),
        "first wake did not make producer ready",
    )?;
    check(
        last.as_mut()
            .poll(&mut Context::from_waker(&last_waker))
            .is_ready(),
        "last wake did not make producer ready",
    )?;
    let mut late = Box::pin(publisher.receiver_closed());
    check(
        late.as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_ready(),
        "late producer missed permanent detach",
    )?;
    check(
        !receiver.unsubscribe(),
        "repeated unsubscribe changed the subscription again",
    )?;
    check(
        first_count.count() == 1 && last_count.count() == 1,
        "idempotent unsubscribe notified already completed waiters",
    )
}

#[test]
fn data_capacity_changes_and_source_finish_do_not_wake_receiver_exit_waiters() -> TestResult {
    let (publisher, mut receiver) = channel::<u8>(Arc::new(ManualExecutor::default()))?;
    let count = Arc::new(CountWake::default());
    let waker = Waker::from(count.clone());
    let mut cx = Context::from_waker(&waker);
    let mut detached = Box::pin(publisher.receiver_closed());
    check(
        detached.as_mut().poll(&mut cx).is_pending(),
        "empty live receiver was detached",
    )?;
    publisher.try_publish(1, 1)?;
    publisher.try_publish(2, 1)?;
    check(receiver.try_recv()? == 1, "first buffered item was changed")?;
    publisher.finish()?;
    check(
        count.count() == 0,
        "data or source-finish notification leaked into receiver_closed",
    )?;
    check(
        detached.as_mut().poll(&mut cx).is_pending(),
        "finish detached a receiver with a buffered prefix",
    )?;
    check(
        receiver.try_recv()? == 2,
        "source finish lost the buffered suffix",
    )?;
    check(
        count.count() == 0,
        "last item delivery detached before consumer EOF",
    )?;
    check(
        matches!(
            receiver.try_recv(),
            Err(crate::error::TryReceiveError::Closed)
        ),
        "source finish did not produce EOF",
    )?;
    check(
        count.count() == 1,
        "consumer EOF failed to wake its producer",
    )?;
    check(
        detached.as_mut().poll(&mut cx).is_ready(),
        "producer did not observe consumed EOF",
    )
}

struct UnavailableExecutor;
impl CallbackExecutor for UnavailableExecutor {
    fn ensure_ready(&self) -> Result<()> {
        Err(NetError::from(ErrorKind::RuntimeUnavailable).with_stage(ErrorStage::Dispatch))
    }
    fn submit(&self, _job: Box<dyn FnOnce() + Send>) -> Result<()> {
        Err(NetError::from(ErrorKind::RuntimeUnavailable).with_stage(ErrorStage::Dispatch))
    }
}

#[test]
fn failed_callback_conversion_detaches_and_releases_the_unconsumed_entry() -> TestResult {
    let (publisher, receiver) = channel::<u8>(Arc::new(UnavailableExecutor))?;
    let items = Arc::new(Semaphore::new(1));
    publisher
        .prepare_try_publish_with_resources(
            7,
            1,
            EventResources {
                items: Some(items.clone().try_acquire_owned()?),
                ..EventResources::default()
            },
        )
        .dispatch()?;
    let count = Arc::new(CountWake::default());
    let waker = Waker::from(count.clone());
    let mut cx = Context::from_waker(&waker);
    let mut detached = Box::pin(publisher.receiver_closed());
    check(
        detached.as_mut().poll(&mut cx).is_pending(),
        "consumer detached before conversion",
    )?;
    let result = receiver.into_callback(|_, _| {});
    check(
        matches!(result, Err(ref error) if error.kind() == ErrorKind::RuntimeUnavailable),
        "callback startup failure was hidden",
    )?;
    check(
        items.available_permits() == 1,
        "failed conversion retained queued resource",
    )?;
    check(
        count.count() == 1,
        "failed conversion did not wake the waiting producer",
    )?;
    check(
        detached.as_mut().poll(&mut cx).is_ready(),
        "producer remained attached after failed conversion",
    )
}

#[test]
fn running_callback_fault_detaches_before_close_and_retires_unread_resources() -> TestResult {
    let executor = Arc::new(ManualExecutor::default());
    let (publisher, receiver) = channel::<u8>(executor.clone())?;
    let registration = receiver.registration.clone();
    let items = Arc::new(Semaphore::new(2));
    for value in [1, 2] {
        publisher
            .prepare_try_publish_with_resources(
                value,
                1,
                EventResources {
                    items: Some(items.clone().try_acquire_owned()?),
                    ..EventResources::default()
                },
            )
            .dispatch()?;
    }
    let count = Arc::new(CountWake::default());
    let waker = Waker::from(count.clone());
    let mut cx = Context::from_waker(&waker);
    let mut detached = Box::pin(publisher.receiver_closed());
    check(
        detached.as_mut().poll(&mut cx).is_pending(),
        "producer detached before callback execution",
    )?;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let subscription = receiver.into_callback(move |_, _| {
        observed.fetch_add(1, Ordering::SeqCst);
        // Inject the caught-unwind outcome inside an acquired callback without
        // constructing a real panic path in the test process.
        registration.fail_callback_for_test();
    })?;
    executor.run_one()?;
    check(
        calls.load(Ordering::SeqCst) == 1,
        "faulted callback consumed a later queued event",
    )?;
    check(
        items.available_permits() == 2,
        "callback fault retained queued resource",
    )?;
    check(
        count.count() == 1 && detached.as_mut().poll(&mut cx).is_ready(),
        "callback fault did not release the producer",
    )?;
    let mut closing = Box::pin(subscription.close());
    check(
        matches!(closing.as_mut().poll(&mut cx), Poll::Ready(Err(ref error)) if error.kind() == ErrorKind::CallbackPanicked),
        "close lost the acquired callback failure",
    )
}

#[derive(Debug, Eq, PartialEq)]
enum ReservationOutcome {
    Detached,
    Reserved,
}

#[test]
fn detach_interrupts_a_partial_count_and_byte_reservation_without_cancelling_its_owner(
) -> TestResult {
    let (publisher, receiver) = channel::<u8>(Arc::new(ManualExecutor::default()))?;
    let items = Arc::new(Semaphore::new(3));
    let bytes = Arc::new(Semaphore::new(3));
    publisher
        .prepare_try_publish_with_resources(
            1,
            3,
            EventResources {
                bytes: Some(bytes.clone().try_acquire_many_owned(3)?),
                ..EventResources::default()
            },
        )
        .dispatch()?;
    let (owner_cancel, mut owner_for_reservation) = tokio::sync::watch::channel(false);
    let items_for_reservation = items.clone();
    let bytes_for_reservation = bytes.clone();
    let mut reservation = Box::pin(async {
        let partial = items_for_reservation
            .acquire_many_owned(3)
            .await
            .map_err(|_| NetError::from(ErrorKind::Closed))?;
        let outcome = tokio::select! {
            biased;
            _ = owner_for_reservation.changed() => Err(NetError::from(ErrorKind::Cancelled)),
            _ = publisher.receiver_closed() => Ok(ReservationOutcome::Detached),
            acquired = bytes_for_reservation.acquire_many_owned(3) => {
                let acquired = acquired.map_err(|_| NetError::from(ErrorKind::Closed))?;
                drop(acquired);
                Ok(ReservationOutcome::Reserved)
            }
        };
        drop(partial);
        outcome
    });
    let count = Arc::new(CountWake::default());
    let waker = Waker::from(count.clone());
    let mut cx = Context::from_waker(&waker);
    check(
        reservation.as_mut().poll(&mut cx).is_pending(),
        "reservation did not wait on exhausted bytes",
    )?;
    check(
        items.available_permits() == 0,
        "reservation did not own its partial count permits",
    )?;
    drop(receiver);
    check(
        count.count() > 0,
        "receiver Drop did not wake the partial reservation",
    )?;
    check(
        matches!(
            reservation.as_mut().poll(&mut cx),
            Poll::Ready(Ok(ReservationOutcome::Detached))
        ),
        "observation detach became owner cancellation or a fresh reservation",
    )?;
    check(
        items.available_permits() == 3 && bytes.available_permits() == 3,
        "detached reservation leaked count or bytes",
    )?;
    let owner_state = owner_cancel.subscribe();
    let owner_is_active = !*::tokio::sync::watch::Receiver::borrow(&owner_state);
    check(
        owner_is_active,
        "receiver Drop cancelled the independent owner",
    )
}

#[test]
fn receiver_drop_rejects_all_pending_resource_publications_and_returns_their_permits() -> TestResult
{
    let (publisher, receiver) = channel::<u8>(Arc::new(ManualExecutor::default()))?;
    publisher.try_publish(1, 1)?;
    publisher.try_publish(2, 1)?;
    let items = Arc::new(Semaphore::new(2));
    let mut first = Box::pin(publisher.publish_with_resources(
        3,
        1,
        EventResources {
            items: Some(items.clone().try_acquire_owned()?),
            ..EventResources::default()
        },
    ));
    let mut second = Box::pin(publisher.publish_with_resources(
        4,
        1,
        EventResources {
            items: Some(items.clone().try_acquire_owned()?),
            ..EventResources::default()
        },
    ));
    let first_count = Arc::new(CountWake::default());
    let second_count = Arc::new(CountWake::default());
    let first_waker = Waker::from(first_count.clone());
    let second_waker = Waker::from(second_count.clone());
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);
    check(
        first.as_mut().poll(&mut first_cx).is_pending(),
        "first publish bypassed queue capacity",
    )?;
    check(
        second.as_mut().poll(&mut second_cx).is_pending(),
        "second publish bypassed queue capacity",
    )?;
    drop(receiver);
    check(
        first_count.count() > 0 && second_count.count() > 0,
        "receiver Drop failed to wake every blocked publisher",
    )?;
    check(
        matches!(first.as_mut().poll(&mut first_cx), Poll::Ready(Err(ref error)) if error.kind() == ErrorKind::Closed),
        "first pending publish did not report detached receiver",
    )?;
    check(
        matches!(second.as_mut().poll(&mut second_cx), Poll::Ready(Err(ref error)) if error.kind() == ErrorKind::Closed),
        "second pending publish did not report detached receiver",
    )?;
    check(
        items.available_permits() == 2,
        "detached pending publications retained resources",
    )
}

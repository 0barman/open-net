use super::*;
use futures::{task::noop_waker_ref, StreamExt};
use std::future::Future;
use std::task::Poll;

type TestResult<T = ()> = std::result::Result<T, crate::BoxError>;
fn check(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

fn ready<F: Future>(future: F) -> TestResult<F::Output> {
    let mut future = Box::pin(future);
    let mut context = Context::from_waker(noop_waker_ref());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => Ok(value),
        Poll::Pending => {
            Err(std::io::Error::other("an already-ready operation remained pending").into())
        }
    }
}

#[derive(Default)]
struct HeldExecutor(Mutex<Vec<Box<dyn FnOnce() + Send>>>);
impl CallbackExecutor for HeldExecutor {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
        let mut jobs = self.0.lock().map_err(NetError::from_poison)?;
        jobs.try_reserve(1)
            .map_err(|error| NetError::with_source(ErrorKind::ResourceExhausted, error))?;
        jobs.push(job);
        Ok(())
    }
}
fn channel<T>(limit: usize) -> TestResult<(EventPublisher<T>, EventReceiver<T>)> {
    Ok(event_channel(
        EventQueueLimit {
            max_items: limit,
            max_bytes: 64,
        },
        EventOverflow::Wait,
        Arc::new(HeldExecutor::default()),
        None,
    )?)
}

#[test]
fn receive_paths_release_entry_resources_before_application_drops_the_value() -> TestResult {
    for mode in 0..3 {
        let (publisher, mut receiver) = channel(1)?;
        let items = Arc::new(Semaphore::new(1));
        let bytes = Arc::new(Semaphore::new(8));
        let resources = EventResources {
            items: Some(items.clone().try_acquire_owned()?),
            bytes: Some(bytes.clone().try_acquire_many_owned(8)?),
            shared_bytes: None,
        };
        let data = Arc::new(vec![7_u8; 8]);
        publisher
            .prepare_try_publish_with_resources(data.clone(), 8, resources)
            .dispatch()?;
        check(
            items.available_permits() == 0 && bytes.available_permits() == 0,
            "queued entry returned its permits early",
        )?;
        let weak = Arc::downgrade(&receiver.registration);
        let probe = Arc::new(WakeProbe {
            check_unlocked: Box::new(move || {
                weak.upgrade()
                    .is_none_or(|registration| registration.state.try_lock().is_ok())
            }),
            calls: std::sync::atomic::AtomicUsize::new(0),
            unlocked: std::sync::atomic::AtomicBool::new(true),
        });
        let waker = futures::task::waker(probe.clone());
        let mut context = Context::from_waker(&waker);
        let mut waiting = Box::pin(items.clone().acquire_owned());
        check(
            waiting.as_mut().poll(&mut context).is_pending(),
            "capacity did not wait for dequeue",
        )?;
        let value = match mode {
            0 => ready(receiver.recv())??
                .ok_or_else(|| std::io::Error::other("missing recv item"))?,
            1 => receiver.try_recv()?,
            _ => ready(receiver.next())?
                .transpose()?
                .ok_or_else(|| std::io::Error::other("missing stream item"))?,
        };
        check(
            Arc::ptr_eq(&value, &data),
            "delivery copied or replaced the owned value",
        )?;
        check(
            probe.calls.load(std::sync::atomic::Ordering::SeqCst) > 0
                && probe.unlocked.load(std::sync::atomic::Ordering::SeqCst),
            "dequeue woke capacity under the queue lock or lost its wake",
        )?;
        drop(ready(waiting)??);
        check(
            items.available_permits() == 1 && bytes.available_permits() == 8,
            "application ownership still consumed queue permits",
        )?;
    }
    Ok(())
}

#[test]
fn receiver_closed_waits_for_consumer_exit_not_source_finish_and_is_cancel_safe() -> TestResult {
    let (publisher, mut receiver) = channel(1)?;
    publisher.try_publish(7_u8, 1)?;
    publisher.finish()?;
    let mut first = Box::pin(publisher.receiver_closed());
    let mut second = Box::pin(publisher.receiver_closed());
    let mut context = Context::from_waker(noop_waker_ref());
    check(
        first.as_mut().poll(&mut context).is_pending(),
        "source finish detached a live consumer",
    )?;
    check(
        second.as_mut().poll(&mut context).is_pending(),
        "second waiter did not stay pending",
    )?;
    drop(first);
    check(
        receiver.try_recv()? == 7,
        "source finish discarded buffered data",
    )?;
    check(
        second.as_mut().poll(&mut context).is_pending(),
        "delivery alone detached the receiver",
    )?;
    check(
        matches!(
            receiver.try_recv(),
            Err(crate::error::TryReceiveError::Closed)
        ),
        "finished receiver did not reach EOF",
    )?;
    check(
        matches!(second.as_mut().poll(&mut context), Poll::Ready(())),
        "consumer exit did not complete the waiter",
    )?;
    check(
        matches!(
            Box::pin(publisher.receiver_closed())
                .as_mut()
                .poll(&mut context),
            Poll::Ready(())
        ),
        "late waiter missed consumer exit",
    )?;
    Ok(())
}

#[test]
fn cancelled_capacity_wait_releases_only_its_unqueued_entry_resources() -> TestResult {
    let (publisher, mut receiver) = channel(1)?;
    publisher.try_publish(1_u8, 1)?;
    let items = Arc::new(Semaphore::new(1));
    let resources = EventResources {
        items: Some(items.clone().try_acquire_owned()?),
        ..EventResources::default()
    };
    let mut publish = Box::pin(publisher.publish_with_resources(2_u8, 1, resources));
    let mut context = Context::from_waker(noop_waker_ref());
    check(
        publish.as_mut().poll(&mut context).is_pending(),
        "Wait overflow accepted a full queue",
    )?;
    check(
        items.available_permits() == 0,
        "pending publish lost owned resources",
    )?;
    drop(publish);
    check(
        items.available_permits() == 1,
        "cancelled publish leaked resources",
    )?;
    check(
        receiver.try_recv()? == 1,
        "cancelled publish consumed the prior item",
    )?;
    check(
        matches!(
            receiver.try_recv(),
            Err(crate::error::TryReceiveError::Empty)
        ),
        "cancelled publish left an extra item",
    )?;
    Ok(())
}

#[test]
fn successful_capacity_retry_keeps_the_original_resources_until_actual_delivery() -> TestResult {
    let (publisher, mut receiver) = channel(1)?;
    publisher.try_publish(1_u8, 1)?;
    let items = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(8));
    let resources = EventResources {
        items: Some(items.clone().try_acquire_owned()?),
        bytes: Some(bytes.clone().try_acquire_many_owned(8)?),
        shared_bytes: None,
    };
    let mut publish = Box::pin(publisher.publish_with_resources(2_u8, 8, resources));
    let mut context = Context::from_waker(noop_waker_ref());
    check(
        publish.as_mut().poll(&mut context).is_pending(),
        "full queue failed to wait",
    )?;
    check(receiver.try_recv()? == 1, "queue prefix changed")?;
    ready(publish)??;
    check(
        items.available_permits() == 0 && bytes.available_permits() == 0,
        "Wait retry released or replaced its original resources",
    )?;
    check(receiver.try_recv()? == 2, "retry lost the queued value")?;
    check(
        items.available_permits() == 1 && bytes.available_permits() == 8,
        "delivered retry leaked resources",
    )?;
    Ok(())
}

struct WakeProbe {
    check_unlocked: Box<dyn Fn() -> bool + Send + Sync>,
    calls: std::sync::atomic::AtomicUsize,
    unlocked: std::sync::atomic::AtomicBool,
}
impl futures::task::ArcWake for WakeProbe {
    fn wake_by_ref(probe: &Arc<Self>) {
        probe
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !(probe.check_unlocked)() {
            probe
                .unlocked
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

#[test]
fn eviction_and_rejected_inputs_keep_permits_until_lock_free_dispatch() -> TestResult {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    for mode in 0..4 {
        let policy = if mode == 0 {
            EventOverflow::DropOldest
        } else {
            EventOverflow::Wait
        };
        let (publisher, receiver) = event_channel(
            EventQueueLimit {
                max_items: 1,
                max_bytes: 1,
            },
            policy,
            Arc::new(HeldExecutor::default()),
            None,
        )?;
        let permits = Arc::new(Semaphore::new(1));
        let resources = EventResources {
            items: Some(permits.clone().try_acquire_owned()?),
            ..EventResources::default()
        };
        let outer = Arc::new(Mutex::new(()));
        let outer_probe = outer.clone();
        let weak = Arc::downgrade(&receiver.registration);
        let probe = Arc::new(WakeProbe {
            check_unlocked: Box::new(move || {
                outer_probe.try_lock().is_ok()
                    && weak
                        .upgrade()
                        .is_none_or(|registration| registration.state.try_lock().is_ok())
            }),
            calls: AtomicUsize::new(0),
            unlocked: AtomicBool::new(true),
        });
        let waker = futures::task::waker(probe.clone());
        let mut context = Context::from_waker(&waker);
        let mut capacity = Box::pin(permits.clone().acquire_owned());
        check(
            capacity.as_mut().poll(&mut context).is_pending(),
            "resource waiter was not pending",
        )?;
        let publication = match mode {
            0 => {
                publisher
                    .prepare_try_publish_with_resources(1_u8, 1, resources)
                    .dispatch()?;
                let guard = outer.lock().map_err(NetError::from_poison)?;
                let publication = publisher.prepare_try_publish(2_u8, 1);
                check(
                    probe.calls.load(Ordering::SeqCst) == 0,
                    "eviction woke capacity under outer lock",
                )?;
                drop(guard);
                publication
            }
            _ => {
                if mode == 1 {
                    publisher.try_publish(1_u8, 1)?;
                }
                if mode == 3 {
                    receiver.unsubscribe();
                }
                let guard = outer.lock().map_err(NetError::from_poison)?;
                let publication = publisher.prepare_try_publish_with_resources(
                    2_u8,
                    if mode == 2 { 2 } else { 1 },
                    resources,
                );
                let expected = match mode {
                    1 => ErrorKind::QueueFull,
                    2 => ErrorKind::ItemTooLarge,
                    _ => ErrorKind::Closed,
                };
                check(
                    publication
                        .result()
                        .err()
                        .is_some_and(|error| error.kind() == expected),
                    "rejection changed error kind",
                )?;
                check(
                    probe.calls.load(Ordering::SeqCst) == 0,
                    "rejected input released permits before dispatch",
                )?;
                drop(guard);
                publication
            }
        };
        let result = publication.dispatch();
        check(result.is_ok() == (mode == 0), "dispatch result changed")?;
        check(
            probe.calls.load(Ordering::SeqCst) > 0,
            "permit release lost the waiting producer wake",
        )?;
        check(
            probe.unlocked.load(Ordering::SeqCst),
            "permit release invoked a waker under a queue or caller lock",
        )?;
        let returned = match capacity.as_mut().poll(&mut context) {
            Poll::Ready(result) => result?,
            Poll::Pending => return Err(std::io::Error::other("permit was not returned").into()),
        };
        drop(returned);
        check(
            permits.available_permits() == 1,
            "resource count changed after one retirement",
        )?;
    }
    Ok(())
}

#[test]
fn shared_payload_budget_lasts_until_the_last_library_delivery_not_application_ownership(
) -> TestResult {
    let bytes = Arc::new(Semaphore::new(8));
    let shared = Arc::new(bytes.clone().try_acquire_many_owned(8)?);
    let payload = Arc::new(vec![9_u8; 8]);
    let (first, mut first_receiver) = channel(1)?;
    let (second, second_receiver) = channel(1)?;
    for publisher in [&first, &second] {
        publisher
            .prepare_try_publish_with_resources(
                payload.clone(),
                8,
                EventResources {
                    shared_bytes: Some(shared.clone()),
                    ..EventResources::default()
                },
            )
            .dispatch()?;
    }
    drop(shared);
    let retained = first_receiver.try_recv()?;
    check(
        Arc::ptr_eq(&retained, &payload),
        "shared delivery copied its payload",
    )?;
    check(
        bytes.available_permits() == 0,
        "first receiver prematurely released a shared budget",
    )?;
    drop(second_receiver);
    check(
        bytes.available_permits() == 8,
        "last library delivery retained the shared budget",
    )?;
    check(retained.len() == 8, "application value was not preserved")?;
    Ok(())
}

#[test]
fn callback_receives_owned_value_after_entry_permits_return_without_releasing_its_job_slot(
) -> TestResult {
    use std::sync::atomic::{AtomicBool, Ordering};
    let executor = Arc::new(HeldExecutor::default());
    let quota = Arc::new(Semaphore::new(1));
    let items = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(8));
    let (publisher, receiver) = event_channel(
        EventQueueLimit {
            max_items: 1,
            max_bytes: 8,
        },
        EventOverflow::Wait,
        executor.clone(),
        Some(quota.clone().try_acquire_owned()?),
    )?;
    let payload = Arc::new(vec![3_u8; 8]);
    publisher
        .prepare_try_publish_with_resources(
            payload.clone(),
            8,
            EventResources {
                items: Some(items.clone().try_acquire_owned()?),
                bytes: Some(bytes.clone().try_acquire_many_owned(8)?),
                shared_bytes: None,
            },
        )
        .dispatch()?;
    let correct = Arc::new(AtomicBool::new(false));
    let observed = correct.clone();
    let callback_quota = quota.clone();
    let callback_items = items.clone();
    let callback_bytes = bytes.clone();
    let subscription = receiver.into_callback(move |context, item| {
        observed.store(
            item.is_ok()
                && callback_items.available_permits() == 1
                && callback_bytes.available_permits() == 8
                && callback_quota.available_permits() == 0,
            Ordering::SeqCst,
        );
        context.unsubscribe();
    })?;
    let job = executor
        .0
        .lock()
        .map_err(NetError::from_poison)?
        .pop()
        .ok_or_else(|| std::io::Error::other("callback was not scheduled"))?;
    job();
    check(
        correct.load(Ordering::SeqCst),
        "callback resource or job-slot lifetime was incorrect",
    )?;
    ready(subscription.close())??;
    check(
        quota.available_permits() == 1,
        "retired callback job retained its own slot",
    )?;
    check(
        payload.len() == 8,
        "callback altered the application's value",
    )?;
    Ok(())
}

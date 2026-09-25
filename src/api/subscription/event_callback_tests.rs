use super::*;
use futures::{executor::block_on, task::noop_waker_ref};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

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
    fn next(&self) -> TestResult<Option<Box<dyn FnOnce() + Send>>> {
        Ok(self
            .jobs
            .lock()
            .map_err(|_| std::io::Error::other("jobs lock"))?
            .pop_front())
    }
}
fn channel<T>(
    executor: Arc<ManualExecutor>,
) -> TestResult<(
    super::super::EventPublisher<T>,
    super::super::EventReceiver<T>,
)> {
    Ok(super::super::event_channel(
        EventQueueLimit {
            max_items: 1,
            max_bytes: 8,
        },
        EventOverflow::DropOldest,
        executor,
        None,
    )?)
}

#[test]
fn event_runner_fault_retires_running_state_and_close_reports_it() -> TestResult {
    let executor = Arc::new(ManualExecutor::default());
    let (publisher, receiver) = channel(executor.clone())?;
    publisher.try_publish(7, 1)?;
    let registration = receiver.registration.clone();
    let subscription = receiver.into_callback(|_, _| {})?;
    lock(&registration.state).running = true;
    {
        let _completion = RunnerCompletion {
            registration: &registration,
            armed: true,
        };
        registration.complete_runner(Err(Box::new("caught runner fault")));
    }
    check(
        !subscription.is_active(),
        "runner fault kept callback active",
    )?;
    let error = block_on(subscription.close())
        .err()
        .ok_or("runner fault was not returned from close")?;
    check(
        error.kind() == ErrorKind::CallbackPanicked,
        "runner fault kind changed",
    )?;
    if let Some(job) = executor.next()? {
        job();
    }
    Ok(())
}

struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[test]
fn one_event_callback_runs_while_close_discards_the_queued_successor_and_waits() -> TestResult {
    let executor = Arc::new(ManualExecutor::default());
    let (publisher, receiver) = channel(executor.clone())?;
    publisher.try_publish(1, 1)?;
    let concurrent = Arc::new(AtomicUsize::new(0));
    let failure = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let running = concurrent.clone();
    let failed = failure.clone();
    let values = observed.clone();
    let (entered, received) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let release = Release(Some(release));
    let subscription = receiver.into_callback(move |_, value| {
        if running.fetch_add(1, Ordering::SeqCst) != 0 {
            failed.store(true, Ordering::SeqCst);
        }
        if let Ok(mut values) = values.lock() {
            values.push(value.ok());
        } else {
            failed.store(true, Ordering::SeqCst);
        }
        if entered.send(()).is_err() {
            failed.store(true, Ordering::SeqCst);
        }
        let released = released
            .lock()
            .ok()
            .and_then(|receiver| receiver.recv_timeout(Duration::from_secs(5)).ok());
        if released.is_none() {
            failed.store(true, Ordering::SeqCst);
        }
        running.fetch_sub(1, Ordering::SeqCst);
    })?;
    let job = executor
        .next()?
        .ok_or("initial callback was not scheduled")?;
    let worker = std::thread::Builder::new().spawn(job)?;
    received.recv_timeout(Duration::from_secs(5))?;
    publisher.try_publish(2, 1)?;
    check(
        executor.next()?.is_none(),
        "one subscription scheduled another concurrent job",
    )?;
    let mut close = Box::pin(subscription.close());
    let mut cx = Context::from_waker(noop_waker_ref());
    check(
        close.as_mut().poll(&mut cx).is_pending(),
        "close did not await acquired callback",
    )?;
    drop(release);
    worker
        .join()
        .map_err(|_| std::io::Error::other("callback job failed"))?;
    check(
        matches!(close.as_mut().poll(&mut cx), Poll::Ready(Ok(()))),
        "finished callback did not release close",
    )?;
    check(
        !failure.load(Ordering::SeqCst) && concurrent.load(Ordering::SeqCst) == 0,
        "callback overlapped or probe failed",
    )?;
    let only_first = observed
        .lock()
        .map_err(|_| std::io::Error::other("values lock"))?
        .as_slice()
        == [Some(1)];
    check(only_first, "unsubscribe delivered queued successor")
}

#[test]
fn lag_counter_exhaustion_rejects_publish_without_saturating_or_evicting() -> TestResult {
    let (publisher, mut receiver) = channel(Arc::new(ManualExecutor::default()))?;
    publisher.try_publish(1, 1)?;
    lock(&receiver.registration.state).lagged = u64::MAX;
    let error = publisher
        .try_publish(2, 1)
        .err()
        .ok_or("lag counter overflow was accepted")?;
    check(
        error.kind() == ErrorKind::ResourceExhausted,
        "wrong lag counter error",
    )?;
    check(
        matches!(
            receiver.try_recv(),
            Err(TryReceiveError::Lagged { skipped: u64::MAX })
        ),
        "lag count wrapped or saturated",
    )?;
    check(
        receiver.try_recv()? == 1,
        "rejected counter overflow evicted the existing item",
    )
}

#[test]
fn retirement_allocation_failure_does_not_change_queue_or_lag() -> TestResult {
    let (publisher, mut receiver) = channel(Arc::new(ManualExecutor::default()))?;
    publisher.try_publish(1, 1)?;
    lock(&receiver.registration.state).fail_retirement_reserve = true;
    let error = publisher
        .try_publish(2, 1)
        .err()
        .ok_or("retirement allocation failure was ignored")?;
    check(
        error.kind() == ErrorKind::ResourceExhausted,
        "wrong retirement error",
    )?;
    check(
        receiver.try_recv()? == 1,
        "failed retirement changed existing queue",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Empty)),
        "failed retirement created a gap",
    )?;
    publisher.try_publish(3, 1)?;
    check(
        receiver.try_recv()? == 3,
        "retirement failure poisoned later publication",
    )
}

#[test]
fn event_limit_validation_and_subscription_ids_are_shared_with_state() -> TestResult {
    let executor = Arc::new(ManualExecutor::default());
    for limit in [
        EventQueueLimit {
            max_items: 0,
            max_bytes: 1,
        },
        EventQueueLimit {
            max_items: 1,
            max_bytes: 0,
        },
        EventQueueLimit {
            max_items: usize::MAX,
            max_bytes: 1,
        },
        EventQueueLimit {
            max_items: 1,
            max_bytes: usize::MAX,
        },
    ] {
        let error =
            super::super::event_channel::<()>(limit, EventOverflow::Wait, executor.clone(), None)
                .err()
                .ok_or("invalid event capacity accepted")?;
        check(
            error.kind() == ErrorKind::InvalidConfig,
            "capacity error category changed",
        )?;
    }
    let (_publisher, source) = super::super::StateSource::new(1, executor.clone(), 1)?;
    let state = source.subscribe()?;
    let (_publisher, event) = channel::<()>(executor)?;
    check(
        event.id().as_u64() > state.id().as_u64(),
        "event IDs do not share state subscription allocator",
    )
}

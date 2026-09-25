use super::*;
use futures::{executor::block_on, task::noop_waker_ref, Stream};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
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
            .map_err(|_| std::io::Error::other("executor lock"))?
            .pop_front())
    }
    fn drain(&self) -> TestResult {
        while let Some(job) = self.next()? {
            job();
        }
        Ok(())
    }
}
fn source(value: u64) -> TestResult<(StatePublisher<u64>, StateSource<u64>, Arc<ManualExecutor>)> {
    let executor = Arc::new(ManualExecutor::default());
    let (publisher, source) = StateSource::new(value, executor.clone(), 4)?;
    Ok((publisher, source, executor))
}

#[test]
fn initial_snapshot_current_coalescing_and_fused_completion() -> TestResult {
    block_on(async {
        let (publisher, source, _) = source(1)?;
        let mut receiver = source.subscribe()?;
        publisher.publish(2)?;
        publisher.publish(3)?;
        check(receiver.current() == 3, "current snapshot did not advance")?;
        check(
            receiver.recv().await? == Some(1),
            "initial snapshot was overwritten",
        )?;
        check(
            receiver.recv().await? == Some(3),
            "intermediate states did not coalesce",
        )?;
        publisher.finish(4)?;
        check(receiver.recv().await? == Some(4), "final snapshot missing")?;
        check(
            receiver.recv().await?.is_none(),
            "receiver did not terminate",
        )?;
        check(
            receiver.recv().await?.is_none(),
            "receiver termination was not fused",
        )?;
        check(
            publisher.publish(5).is_err(),
            "publisher reopened after finish",
        )?;
        Ok(())
    })
}

#[test]
fn cancelling_recv_does_not_consume_and_publisher_drop_ends_observation() -> TestResult {
    block_on(async {
        let (publisher, source, _) = source(1)?;
        let mut receiver = source.subscribe()?;
        check(
            receiver.recv().await? == Some(1),
            "initial snapshot missing",
        )?;
        let mut waiting = Box::pin(receiver.recv());
        check(
            matches!(futures::poll!(waiting.as_mut()), Poll::Pending),
            "empty receive completed",
        )?;
        publisher.publish(2)?;
        drop(waiting);
        drop(publisher);
        check(
            source.current() == 2,
            "source snapshot lost publisher final value",
        )?;
        check(
            receiver.recv().await? == Some(2),
            "cancelled receive stole final value",
        )?;
        check(
            receiver.recv().await?.is_none(),
            "source handle kept publisher alive",
        )?;
        Ok(())
    })
}

#[test]
fn stream_initial_and_unsubscribe_release_quota() -> TestResult {
    let executor = Arc::new(ManualExecutor::default());
    let (_publisher, source) = StateSource::new(7, executor, 1)?;
    let mut receiver = source.subscribe()?;
    check(
        source
            .subscribe()
            .err()
            .is_some_and(|error| error.kind() == ErrorKind::SubscriptionLimitReached),
        "active subscription quota was not enforced",
    )?;
    let mut cx = Context::from_waker(noop_waker_ref());
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(Some(Ok(7)))
        ),
        "Stream initial snapshot missing",
    )?;
    check(receiver.unsubscribe(), "first unsubscribe lost")?;
    check(!receiver.unsubscribe(), "unsubscribe was not idempotent")?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(None)
        ),
        "unsubscribed stream remained open",
    )?;
    drop(source.subscribe()?);
    Ok(())
}

#[test]
fn callback_conversion_preserves_identity_cursor_and_serial_coalescing() -> TestResult {
    block_on(async {
        let (publisher, source, executor) = source(1)?;
        let mut receiver = source.subscribe()?;
        let id = receiver.id();
        check(
            receiver.recv().await? == Some(1),
            "initial snapshot missing",
        )?;
        publisher.publish(2)?;
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let captured = delivered.clone();
        let subscription = receiver.into_callback(move |context, value| {
            if let Ok(mut values) = captured.lock() {
                values.push((context.id(), value.map_err(|error| error.kind())));
            }
        })?;
        check(
            subscription.id() == id,
            "conversion changed subscription identity",
        )?;
        publisher.publish(3)?;
        publisher.publish(4)?;
        executor.drain()?;
        let values = delivered
            .lock()
            .map_err(|_| std::io::Error::other("values lock"))?;
        check(
            values.as_slice() == [(id, Ok(4))],
            "conversion replayed consumed cursor or failed coalescing",
        )?;
        drop(values);
        subscription.close().await?;
        Ok(())
    })
}

#[test]
fn cancelled_queued_callback_retains_quota_until_job_retires() -> TestResult {
    let executor = Arc::new(ManualExecutor::default());
    let (_publisher, source) = StateSource::new(1, executor.clone(), 1)?;
    let subscription = source.subscribe()?.into_callback(|_, _| {})?;
    subscription.unsubscribe();
    check(
        source
            .subscribe()
            .err()
            .is_some_and(|error| error.kind() == ErrorKind::SubscriptionLimitReached),
        "cancelled job released its queue bound before retirement",
    )?;
    drop(subscription);
    executor.drain()?;
    drop(source.subscribe()?);
    Ok(())
}

#[test]
fn final_publication_wins_over_reordered_old_updates() -> TestResult {
    block_on(async {
        let (publisher, source, _) = source(1)?;
        let mut receiver = source.subscribe()?;
        let registration = receiver.registration.clone();
        registration.update(Arc::new(3), 2, true, None);
        registration.update(Arc::new(2), 1, false, None);
        check(
            receiver.current() == 3,
            "late older publication replaced final state",
        )?;
        check(
            receiver.recv().await? == Some(1),
            "final publication overwrote initial state",
        )?;
        check(
            receiver.recv().await? == Some(3),
            "final publication missing",
        )?;
        check(
            receiver.recv().await?.is_none(),
            "final publication reopened",
        )?;
        drop(publisher);
        Ok(())
    })
}

#[test]
fn last_publisher_clone_closes_without_source_or_receiver_ownership() -> TestResult {
    block_on(async {
        let (publisher, source, _) = source(1)?;
        let other = publisher.clone();
        let mut receiver = source.subscribe()?;
        check(receiver.recv().await? == Some(1), "initial missing")?;
        drop(publisher);
        let mut waiting = Box::pin(receiver.recv());
        check(
            matches!(futures::poll!(waiting.as_mut()), Poll::Pending),
            "first publisher drop closed remaining owner",
        )?;
        drop(waiting);
        other.publish(2)?;
        drop(other);
        check(
            receiver.recv().await? == Some(2),
            "last publisher value missing",
        )?;
        check(
            receiver.recv().await?.is_none(),
            "observation kept publisher open",
        )?;
        let mut late = source.subscribe()?;
        check(
            late.recv().await? == Some(2),
            "late observer lost final snapshot",
        )?;
        check(
            late.recv().await?.is_none(),
            "late observer reopened source",
        )?;
        Ok(())
    })
}

#[test]
fn publisher_failure_delivers_pending_snapshot_error_then_fused_end() -> TestResult {
    block_on(async {
        let (publisher, source, _) = source(1)?;
        let mut receiver = source.subscribe()?;
        publisher.publish(2)?;
        publisher.fail(NetError::from(ErrorKind::Internal))?;
        check(
            receiver.recv().await? == Some(1),
            "failure replaced initial value",
        )?;
        check(
            receiver.recv().await? == Some(2),
            "failure replaced latest value",
        )?;
        check(
            receiver
                .recv()
                .await
                .err()
                .is_some_and(|error| error.kind() == ErrorKind::Internal),
            "source failure missing",
        )?;
        check(
            receiver.recv().await?.is_none(),
            "failure did not close stream",
        )?;
        check(
            receiver.recv().await?.is_none(),
            "failure terminal state was not fused",
        )?;
        Ok(())
    })
}

#[test]
fn subscription_ids_stop_at_exhaustion_without_wrap() -> TestResult {
    use std::sync::atomic::{AtomicU64, Ordering};
    let counter = AtomicU64::new(u64::MAX - 1);
    check(
        super::source::next_id(&counter)?.as_u64() == u64::MAX,
        "last identity not allocated",
    )?;
    check(
        super::source::next_id(&counter)
            .err()
            .is_some_and(|error| error.kind() == ErrorKind::ResourceExhausted),
        "identity exhaustion wrapped",
    )?;
    check(
        counter.load(Ordering::Relaxed) == u64::MAX,
        "exhaustion modified counter",
    )?;
    Ok(())
}

#[test]
fn callback_initial_snapshot_survives_conversion_and_queued_cancellation_revokes_delivery(
) -> TestResult {
    let (publisher, source, executor) = source(1)?;
    let receiver = source.subscribe()?;
    let (sent, received) = std::sync::mpsc::channel();
    publisher.publish(2)?;
    let subscription = receiver.into_callback(move |context, value| {
        let _ = sent.send((context.id(), value.map_err(|error| error.kind())));
    })?;
    executor.drain()?;
    check(
        received.try_recv()? == (subscription.id(), Ok(1)),
        "conversion replaced unconsumed initial snapshot",
    )?;
    check(
        received.try_recv()? == (subscription.id(), Ok(2)),
        "conversion lost latest publication",
    )?;
    publisher.publish(3)?;
    subscription.unsubscribe();
    executor.drain()?;
    check(
        received.try_recv().is_err(),
        "cancelled queued callback entered",
    )?;
    block_on(subscription.close())?;
    Ok(())
}

struct FailingExecutor {
    fail_ready: bool,
    fail_submit: std::sync::atomic::AtomicBool,
    jobs: ManualExecutor,
}
impl CallbackExecutor for FailingExecutor {
    fn ensure_ready(&self) -> Result<()> {
        if self.fail_ready {
            Err(NetError::from(ErrorKind::RuntimeUnavailable))
        } else {
            Ok(())
        }
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
        if self.fail_submit.load(std::sync::atomic::Ordering::SeqCst) {
            Err(NetError::from(ErrorKind::QueueClosed))
        } else {
            self.jobs.submit(job)
        }
    }
}
#[test]
fn callback_conversion_failure_consumes_receiver_and_releases_capacity() -> TestResult {
    for fail_ready in [true, false] {
        let executor = Arc::new(FailingExecutor {
            fail_ready,
            fail_submit: true.into(),
            jobs: ManualExecutor::default(),
        });
        let (_publisher, source) = StateSource::new(1, executor, 1)?;
        let expected = if fail_ready {
            ErrorKind::RuntimeUnavailable
        } else {
            ErrorKind::QueueClosed
        };
        check(
            source
                .subscribe()?
                .into_callback(|_, _| {})
                .err()
                .is_some_and(|error| error.kind() == expected),
            "conversion failure was not returned",
        )?;
        drop(source.subscribe()?);
    }
    Ok(())
}

#[test]
fn later_dispatch_failure_is_reported_by_close() -> TestResult {
    let executor = Arc::new(FailingExecutor {
        fail_ready: false,
        fail_submit: false.into(),
        jobs: ManualExecutor::default(),
    });
    let (publisher, source) = StateSource::new(1, executor.clone(), 1)?;
    let subscription = source.subscribe()?.into_callback(|_, _| {})?;
    executor.jobs.drain()?;
    executor
        .fail_submit
        .store(true, std::sync::atomic::Ordering::SeqCst);
    publisher.publish(2)?;
    check(
        !subscription.is_active(),
        "failed dispatch left subscription active",
    )?;
    check(
        block_on(subscription.close())
            .err()
            .is_some_and(|error| error.kind() == ErrorKind::QueueClosed),
        "close swallowed dispatch failure",
    )?;
    Ok(())
}

#[test]
fn caught_callback_panic_terminates_only_that_subscription_and_close_reports_fault() -> TestResult {
    let (publisher, source, executor) = source(1)?;
    let receiver = source.subscribe()?;
    let registration = receiver.registration.clone();
    let subscription = receiver.into_callback(|_, _| {})?;
    let other = source.subscribe()?.into_callback(|_, _| {})?;
    // Inject the result of catch_unwind without creating a panic in the test process.
    check(
        !registration.complete_callback(Err(Box::new("caught callback fault"))),
        "caught panic was accepted",
    )?;
    executor.drain()?;
    check(
        !subscription.is_active(),
        "caught panic left callback active",
    )?;
    check(
        other.is_active(),
        "caught panic terminated a different subscription",
    )?;
    publisher.publish(2)?;
    executor.drain()?;
    check(
        block_on(subscription.close()).err().is_some_and(|error| {
            error.kind() == ErrorKind::CallbackPanicked
                && error.context().stage == Some(crate::error::ErrorStage::Dispatch)
        }),
        "close lost callback panic classification",
    )?;
    block_on(other.close())?;
    Ok(())
}

#[test]
fn close_waits_for_its_running_callback_but_not_another_subscription() -> TestResult {
    use std::sync::mpsc;
    use std::time::Duration;
    let (_publisher, source, executor) = source(1)?;
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let first = source.subscribe()?.into_callback(move |_, _| {
        let _ = started_tx.send(());
        if let Ok(release) = release_rx.lock() {
            let _ = release.recv();
        }
    })?;
    let first_job = executor
        .next()?
        .ok_or_else(|| std::io::Error::other("missing first job"))?;
    let first_thread = std::thread::Builder::new()
        .name("subscription-close-first".into())
        .spawn(first_job)?;
    started_rx.recv_timeout(Duration::from_secs(5))?;
    let (other_started_tx, other_started_rx) = mpsc::channel();
    let (other_release_tx, other_release_rx) = mpsc::channel();
    let other_release_rx = Mutex::new(other_release_rx);
    let other = source.subscribe()?.into_callback(move |_, _| {
        let _ = other_started_tx.send(());
        if let Ok(release) = other_release_rx.lock() {
            let _ = release.recv();
        }
    })?;
    let other_job = executor
        .next()?
        .ok_or_else(|| std::io::Error::other("missing second job"))?;
    let other_thread = std::thread::Builder::new()
        .name("subscription-close-other".into())
        .spawn(other_job)?;
    other_started_rx.recv_timeout(Duration::from_secs(5))?;
    let mut close = Box::pin(first.close());
    let mut cx = Context::from_waker(noop_waker_ref());
    let before_release = std::future::Future::poll(close.as_mut(), &mut cx);
    release_tx.send(())?;
    first_thread
        .join()
        .map_err(|_| std::io::Error::other("first runner unwound"))?;
    let after_release = std::future::Future::poll(close.as_mut(), &mut cx);
    // Release both workers before any failing check so a failed assertion cannot strand a worker.
    other_release_tx.send(())?;
    other_thread
        .join()
        .map_err(|_| std::io::Error::other("other runner unwound"))?;
    block_on(other.close())?;
    check(
        matches!(before_release, Poll::Pending),
        "close returned while its callback ran",
    )?;
    check(
        matches!(after_release, Poll::Ready(Ok(()))),
        "close waited for another subscription",
    )?;
    Ok(())
}

#[test]
fn running_callback_is_serial_and_updates_coalesce_without_extra_jobs() -> TestResult {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use std::time::Duration;
    let (publisher, source, executor) = source(1)?;
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let (values_tx, values_rx) = mpsc::channel();
    let running = Arc::new(AtomicUsize::new(0));
    let captured_running = running.clone();
    let subscription = source.subscribe()?.into_callback(move |_, value| {
        let previous = captured_running.fetch_add(1, Ordering::SeqCst);
        let is_first = matches!(value, Ok(1));
        let _ = values_tx.send((previous, value.map_err(|error| error.kind())));
        if is_first {
            let _ = started_tx.send(());
            if let Ok(release) = release_rx.lock() {
                let _ = release.recv();
            }
        }
        captured_running.fetch_sub(1, Ordering::SeqCst);
    })?;
    let job = executor
        .next()?
        .ok_or_else(|| std::io::Error::other("missing callback job"))?;
    let thread = std::thread::Builder::new()
        .name("subscription-serial".into())
        .spawn(job)?;
    started_rx.recv_timeout(Duration::from_secs(5))?;
    publisher.publish(2)?;
    publisher.publish(3)?;
    let redundant_job = executor.next()?;
    release_tx.send(())?;
    thread
        .join()
        .map_err(|_| std::io::Error::other("callback runner unwound"))?;
    block_on(subscription.close())?;
    check(
        redundant_job.is_none(),
        "concurrent update scheduled a second runner",
    )?;
    check(
        values_rx.try_recv()? == (0, Ok(1)),
        "initial callback overlapped",
    )?;
    check(
        values_rx.try_recv()? == (0, Ok(3)),
        "next callback overlapped or did not coalesce",
    )?;
    check(
        values_rx.try_recv().is_err(),
        "intermediate state was delivered",
    )?;
    check(
        running.load(Ordering::SeqCst) == 0,
        "callback stayed running",
    )?;
    Ok(())
}

struct InlineExecutor;
impl CallbackExecutor for InlineExecutor {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
        job();
        Ok(())
    }
}
#[test]
fn inline_first_callback_can_unsubscribe_and_context_does_not_own_registration() -> TestResult {
    let (_publisher, source) = StateSource::new(1, Arc::new(InlineExecutor), 1)?;
    let (sent, received) = std::sync::mpsc::channel();
    let subscription = source.subscribe()?.into_callback(move |context, _| {
        let changed = context.unsubscribe();
        let _ = sent.send((changed, context));
    })?;
    let (changed, context) = received.try_recv()?;
    check(
        changed && !subscription.is_active(),
        "inline callback could not unsubscribe before conversion returned",
    )?;
    drop(subscription);
    check(
        context.control.upgrade().is_none(),
        "callback context retained registration",
    )?;
    check(
        !context.unsubscribe(),
        "retired callback context changed subscription",
    )?;
    drop(source.subscribe()?);
    Ok(())
}

struct ReentrantErrorDrop {
    registration: Arc<Mutex<Option<Weak<registration::Registration<NetError>>>>>,
    sent: std::sync::mpsc::Sender<ErrorKind>,
}
impl fmt::Debug for ReentrantErrorDrop {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReentrantErrorDrop")
    }
}
impl fmt::Display for ReentrantErrorDrop {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("reentrant error drop")
    }
}
impl std::error::Error for ReentrantErrorDrop {}
impl Drop for ReentrantErrorDrop {
    fn drop(&mut self) {
        let registration = self
            .registration
            .lock()
            .ok()
            .and_then(|holder| holder.as_ref().and_then(Weak::upgrade));
        if let Some(registration) = registration {
            let _ = self.sent.send(registration.current().kind());
        }
    }
}
#[test]
fn replacing_receiver_snapshot_drops_last_error_source_outside_registration_lock() -> TestResult {
    let holder = Arc::new(Mutex::new(None));
    let (sent, received) = std::sync::mpsc::channel();
    let initial = NetError::protocol(ReentrantErrorDrop {
        registration: holder.clone(),
        sent,
    });
    let (publisher, source) = StateSource::new(initial, Arc::new(ManualExecutor::default()), 1)?;
    let mut receiver = source.subscribe()?;
    *holder.lock().map_err(|_| "registration holder poisoned")? =
        Some(Arc::downgrade(&receiver.registration));
    drop(block_on(receiver.recv())?);
    let worker = std::thread::Builder::new()
        .spawn(move || publisher.publish(NetError::from(ErrorKind::Closed)))?;
    check(
        received.recv_timeout(std::time::Duration::from_secs(5))? == ErrorKind::Closed,
        "error source Drop did not reenter latest receiver state",
    )?;
    worker.join().map_err(|_| "publisher thread unwound")??;
    Ok(())
}

struct CaptureDrop {
    context: Arc<Mutex<Option<CallbackContext>>>,
    source: StateSource<u64>,
    sent: std::sync::mpsc::Sender<(bool, u64)>,
}
impl Drop for CaptureDrop {
    fn drop(&mut self) {
        let context = self.context.lock().ok().and_then(|holder| holder.clone());
        if let Some(context) = context {
            let _ = self
                .sent
                .send((context.unsubscribe(), self.source.current()));
        }
    }
}
#[test]
fn unsubscribe_releases_last_callback_capture_outside_registration_and_source_locks() -> TestResult
{
    let (_publisher, source, executor) = source(1)?;
    let holder = Arc::new(Mutex::new(None));
    let (sent, received) = std::sync::mpsc::channel();
    let capture = CaptureDrop {
        context: holder.clone(),
        source: source.clone(),
        sent,
    };
    let callback_holder = holder.clone();
    let subscription = source.subscribe()?.into_callback(move |context, _| {
        let _keep_capture = &capture;
        if let Ok(mut holder) = callback_holder.lock() {
            *holder = Some(context);
        }
    })?;
    executor.drain()?;
    let worker = std::thread::Builder::new().spawn(move || drop(subscription))?;
    check(
        received.recv_timeout(std::time::Duration::from_secs(5))? == (false, 1),
        "callback capture Drop did not safely reenter unsubscribed state",
    )?;
    worker.join().map_err(|_| "unsubscribe thread unwound")?;
    let context = holder
        .lock()
        .map_err(|_| "callback context holder poisoned")?
        .take()
        .ok_or("callback context missing")?;
    check(
        context.control.upgrade().is_none(),
        "retired context kept callback owner alive",
    )?;
    Ok(())
}

struct ReentrantWakerDrop {
    registration: Weak<registration::Registration<u64>>,
    sent: std::sync::mpsc::Sender<u64>,
}
impl std::task::Wake for ReentrantWakerDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for ReentrantWakerDrop {
    fn drop(&mut self) {
        if let Some(registration) = self.registration.upgrade() {
            let _ = self.sent.send(registration.current());
        }
    }
}
#[test]
fn replacing_receive_waker_drops_last_user_waker_outside_registration_lock() -> TestResult {
    let (_publisher, source, _) = source(1)?;
    let mut receiver = source.subscribe()?;
    check(
        block_on(receiver.recv())? == Some(1),
        "initial state missing",
    )?;
    let (sent, received) = std::sync::mpsc::channel();
    let waker = std::task::Waker::from(Arc::new(ReentrantWakerDrop {
        registration: Arc::downgrade(&receiver.registration),
        sent,
    }));
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut Context::from_waker(&waker)),
            Poll::Pending
        ),
        "receive did not install user waker",
    )?;
    drop(waker);
    let worker = std::thread::Builder::new().spawn(move || {
        let pending = matches!(
            Pin::new(&mut receiver).poll_next(&mut Context::from_waker(noop_waker_ref())),
            Poll::Pending
        );
        (pending, receiver)
    })?;
    check(
        received.recv_timeout(std::time::Duration::from_secs(5))? == 1,
        "old waker destructor did not reenter receiver",
    )?;
    let (pending, receiver) = worker
        .join()
        .map_err(|_| "waker replacement thread unwound")?;
    check(pending, "waker replacement consumed a state")?;
    drop(receiver);
    Ok(())
}

#[test]
fn unsubscribed_receiver_keeps_snapshot_without_retaining_callback_executor() -> TestResult {
    let executor = Arc::new(ManualExecutor::default());
    let weak_executor = Arc::downgrade(&executor);
    let (publisher, source) = StateSource::new(1, executor, 1)?;
    let receiver = source.subscribe()?;
    drop(publisher);
    drop(source);
    receiver.unsubscribe();
    check(
        weak_executor.upgrade().is_none(),
        "unsubscribed receiver retained callback executor",
    )?;
    check(
        receiver.current() == 1,
        "executor retirement lost final snapshot",
    )?;
    Ok(())
}

#[test]
fn preparing_publication_defers_user_drop_wake_and_callback_submission() -> TestResult {
    struct Probe {
        outer: Arc<Mutex<()>>,
        dropped: std::sync::mpsc::Sender<bool>,
    }
    impl fmt::Debug for Probe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Probe")
        }
    }
    impl fmt::Display for Probe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("probe")
        }
    }
    impl std::error::Error for Probe {}
    impl Drop for Probe {
        fn drop(&mut self) {
            let _ = self.dropped.send(self.outer.try_lock().is_ok());
        }
    }
    struct ProbeWake {
        outer: Arc<Mutex<()>>,
        woke: std::sync::mpsc::Sender<bool>,
    }
    impl std::task::Wake for ProbeWake {
        fn wake(self: Arc<Self>) {
            let _ = self.woke.send(self.outer.try_lock().is_ok());
        }
    }
    let outer = Arc::new(Mutex::new(()));
    let (drop_tx, drop_rx) = std::sync::mpsc::channel();
    let executor = Arc::new(ManualExecutor::default());
    let initial = NetError::protocol(Probe {
        outer: outer.clone(),
        dropped: drop_tx,
    });
    let (publisher, source) = StateSource::new(initial, executor.clone(), 4)?;
    let mut receiver = source.subscribe()?;
    drop(block_on(receiver.recv())?);
    let callback = source.subscribe()?.into_callback(|_, _| {})?;
    executor.drain()?;
    let (wake_tx, wake_rx) = std::sync::mpsc::channel();
    let waker = std::task::Waker::from(Arc::new(ProbeWake {
        outer: outer.clone(),
        woke: wake_tx,
    }));
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut Context::from_waker(&waker)),
            Poll::Pending
        ),
        "receiver not pending",
    )?;
    let held = outer.lock().map_err(|_| "outer lock poisoned")?;
    let publication = publisher.prepare_arc(Arc::new(NetError::from(ErrorKind::Closed)));
    check(
        source.current().kind() == ErrorKind::Closed,
        "prepare did not atomically commit source snapshot",
    )?;
    check(
        drop_rx.try_recv().is_err(),
        "prepare dropped user value under outer lock",
    )?;
    check(
        wake_rx.try_recv().is_err(),
        "prepare woke public receiver under outer lock",
    )?;
    check(
        executor.next()?.is_none(),
        "prepare submitted callback under outer lock",
    )?;
    drop(held);
    publication.dispatch()?;
    check(
        wake_rx.try_recv()?,
        "dispatch woke receiver while outer lock held",
    )?;
    check(
        drop_rx.try_recv()?,
        "dispatch dropped source while outer lock held",
    )?;
    check(
        executor.next()?.is_some(),
        "dispatch failed to submit callback",
    )?;
    drop(callback);
    Ok(())
}

#[test]
fn deferred_publications_cannot_restore_old_state_after_newer_or_final_dispatch() -> TestResult {
    block_on(async {
        let (publisher, source, _) = source(1)?;
        let mut receiver = source.subscribe()?;
        let old = publisher.prepare_arc(Arc::new(2));
        let final_value = publisher.prepare_finish_arc(Arc::new(3));
        final_value.dispatch()?;
        old.dispatch()?;
        check(
            source.current() == 3 && receiver.current() == 3,
            "old dispatch restored old snapshot",
        )?;
        check(
            receiver.recv().await? == Some(1),
            "deferred dispatch replaced initial state",
        )?;
        check(
            receiver.recv().await? == Some(3),
            "final deferred value missing",
        )?;
        check(
            receiver.recv().await?.is_none(),
            "old deferred dispatch reopened receiver",
        )?;
        Ok(())
    })
}

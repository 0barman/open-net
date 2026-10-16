// Explicit gates also identify test helpers when the production-source guard scans this file.
use super::*;
use crate::module::net_status::inner::network_status_snapshot::{
    NetworkStatusPublisher, NetworkStatusSource, NetworkViewHandle,
};
use crate::net_status::{MonitorState, NetworkEventOptions, NetworkStatus};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Default)]
struct ManualExecutor {
    jobs: Mutex<VecDeque<Job>>,
    available: Notify,
    reject: AtomicBool,
}
impl CallbackExecutor for ManualExecutor {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Job) -> Result<()> {
        if self.reject.load(Ordering::SeqCst) {
            return Err(dispatch_error(ErrorKind::QueueFull));
        }
        lock(&self.jobs).push_back(job);
        self.available.notify_waiters();
        Ok(())
    }
}
impl ManualExecutor {
    async fn next(&self) -> Result<Job> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.available.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(job) = lock(&self.jobs).pop_front() {
                    return job;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| NetError::from(ErrorKind::TimedOut))
    }
    async fn run_one(&self) -> Result<()> {
        self.next().await?();
        Ok(())
    }
}

struct Fixture {
    _source: NetworkStatusSource,
    publisher: NetworkStatusPublisher,
    view: NetworkViewHandle,
    executor: Arc<ManualExecutor>,
    replacements: Arc<AtomicUsize>,
}
impl Fixture {
    fn new() -> Result<Self> {
        let executor = Arc::new(ManualExecutor::default());
        let source = NetworkStatusSource::new(executor.clone(), 16)?;
        let (publisher, initial) = source.begin_generation()?;
        initial.dispatch()?;
        publisher
            .prepare_observation(NetworkStatus::Available, None, None)?
            .dispatch()?;
        let view = source.new_view(executor.clone(), None)?;
        view.activate()?.1.dispatch();
        Ok(Self {
            _source: source,
            publisher,
            view,
            executor,
            replacements: Arc::new(AtomicUsize::new(0)),
        })
    }
    fn options(capacity: usize) -> NetworkEventOptions {
        NetworkEventOptions {
            capacity,
            max_bytes: 64 * 1024,
        }
    }
    fn observer(
        &self,
        options: NetworkEventOptions,
        callback: impl Fn(CallbackContext, Result<NetworkObservation>) + Send + Sync + 'static,
    ) -> Result<Subscription> {
        let view = self.view.clone();
        let count = self.replacements.clone();
        subscribe(
            self.view.subscribe_events(options)?,
            Arc::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
                view.subscribe_events(options)
            }),
            self.executor.clone(),
            tokio::runtime::Handle::current(),
            None,
            callback,
        )
    }
    fn publish(&self, status: NetworkStatus) -> Result<()> {
        self.publisher
            .prepare_observation(status, None, None)?
            .dispatch()
    }
}

#[cfg(test)]
async fn bounded<T>(
    future: impl Future<Output = T>,
) -> std::result::Result<T, Box<dyn std::error::Error + Send + Sync>> {
    Ok(tokio::time::timeout(Duration::from_secs(5), future).await?)
}

#[tokio::test]
#[cfg(test)]
async fn actual_overflow_keeps_prefix_reconciles_missed_outage_and_preserves_identity() -> TestResult
{
    let fixture = Fixture::new()?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let captured = calls.clone();
    let subscription = fixture.observer(Fixture::options(2), move |context, value| {
        lock(&captured).push((context.id(), value));
    })?;
    let id = subscription.id();
    // Hold the Initial callback while the real bounded receiver overflows.
    let initial = fixture.executor.next().await?;
    fixture.publish(NetworkStatus::Unavailable)?;
    fixture.publish(NetworkStatus::Available)?;
    fixture.publish(NetworkStatus::Unavailable)?;
    fixture.publish(NetworkStatus::Available)?;
    initial();
    for _ in 0..3 {
        fixture.executor.run_one().await?;
    }
    let baseline = {
        let calls = lock(&calls);
        assert_eq!(calls.len(), 4);
        assert!(calls.iter().all(|(actual, _)| *actual == id));
        assert!(
            matches!(&calls[0].1, Ok(NetworkObservation::Event(event)) if event.kind == NetworkEventKind::Initial)
        );
        assert!(
            matches!(&calls[1].1, Ok(NetworkObservation::Event(event)) if event.snapshot.reachability == Some(NetworkStatus::Unavailable))
        );
        assert!(
            matches!(&calls[2].1, Ok(NetworkObservation::Event(event)) if event.snapshot.reachability == Some(NetworkStatus::Available))
        );
        match &calls[3].1 {
            Ok(NetworkObservation::Reconciled { previous, current }) => {
                assert_eq!(previous.generation, current.generation);
                assert_eq!(
                    previous.snapshot.reachability,
                    Some(NetworkStatus::Available)
                );
                assert_eq!(
                    current.snapshot.reachability,
                    Some(NetworkStatus::Available)
                );
                assert!(current.snapshot.loss_epoch > previous.snapshot.loss_epoch);
                assert_eq!(current.kind, NetworkEventKind::Initial);
                current.sequence
            }
            _ => return Err("overflow did not yield reconciliation".into()),
        }
    };
    fixture.publish(NetworkStatus::Unavailable)?;
    fixture.executor.run_one().await?;
    assert!(
        matches!(&lock(&calls)[4].1, Ok(NetworkObservation::Event(event)) if event.sequence == baseline + 1)
    );
    bounded(subscription.close()).await??;
    assert_eq!(fixture.replacements.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
#[cfg(test)]
async fn recovery_while_stopped_stays_subscribed_and_restart_changes_generation() -> TestResult {
    let fixture = Fixture::new()?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let captured = calls.clone();
    let subscription = fixture.observer(Fixture::options(1), move |_, value| {
        lock(&captured).push(value)
    })?;
    let initial = fixture.executor.next().await?;
    fixture.publish(NetworkStatus::Unavailable)?;
    fixture.publish(NetworkStatus::Available)?;
    fixture.view.retire(false, None)?.dispatch();
    initial();
    fixture.executor.run_one().await?; // retained Unavailable
    fixture.executor.run_one().await?; // Reconciled Stopped
    let old_generation = match &lock(&calls)[2] {
        Ok(NetworkObservation::Reconciled { current, .. }) => {
            assert!(matches!(current.snapshot.state, MonitorState::Stopped));
            assert!(current.snapshot.reachability.is_none());
            current.generation
        }
        _ => return Err("stopped recovery lost its baseline".into()),
    };
    fixture.view.activate()?.1.dispatch();
    fixture.executor.run_one().await?;
    assert!(
        matches!(&lock(&calls)[3], Ok(NetworkObservation::Event(event)) if event.generation > old_generation)
    );
    fixture.view.retire(true, None)?.dispatch();
    fixture.executor.run_one().await?;
    assert!(
        matches!(&lock(&calls)[4], Ok(NetworkObservation::Event(event)) if matches!(event.snapshot.state, MonitorState::Closed))
    );
    bounded(subscription.close()).await??;
    Ok(())
}

#[tokio::test]
#[cfg(test)]
async fn overflow_that_loses_closed_event_recovers_a_final_closed_baseline() -> TestResult {
    let fixture = Fixture::new()?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let captured = calls.clone();
    let subscription = fixture.observer(Fixture::options(1), move |_, value| {
        lock(&captured).push(value)
    })?;
    let initial = fixture.executor.next().await?;
    fixture.publish(NetworkStatus::Unavailable)?;
    fixture.view.retire(true, None)?.dispatch();
    initial();
    fixture.executor.run_one().await?;
    fixture.executor.run_one().await?;
    assert!(
        matches!(&lock(&calls)[2], Ok(NetworkObservation::Reconciled { current, .. }) if matches!(current.snapshot.state, MonitorState::Closed))
    );
    bounded(subscription.close()).await??;
    assert_eq!(fixture.replacements.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(test)]
async fn close_waits_for_running_callback_and_revokes_overflow_recovery() -> TestResult {
    let fixture = Fixture::new()?;
    let (entered, started) = oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = calls.clone();
    let subscription = fixture.observer(Fixture::options(1), move |_, _| {
        captured.fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = lock(&entered).take() {
            let _ = entered.send(());
        }
        lock(&released)
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
    })?;
    let job = fixture.executor.next().await?;
    let worker = std::thread::spawn(job);
    bounded(started).await??;
    fixture.publish(NetworkStatus::Unavailable)?;
    fixture.publish(NetworkStatus::Available)?;
    let mut close = Box::pin(subscription.close());
    assert!(futures::poll!(close.as_mut()).is_pending());
    release.send(())?;
    worker.join().map_err(|_| "callback worker panicked")?;
    bounded(close).await??;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.replacements.load(Ordering::SeqCst), 0);
    assert!(lock(&fixture.executor.jobs).is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(test)]
async fn dropping_pending_close_retires_callback_and_driver_without_recovery() -> TestResult {
    let fixture = Fixture::new()?;
    let (entered, started) = oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let (callback_capture, mut callback_retired) = oneshot::channel::<()>();
    let (driver_capture, driver_retired) = oneshot::channel::<()>();
    let calls = Arc::new(AtomicUsize::new(0));
    let captured_calls = calls.clone();
    let replacements = fixture.replacements.clone();
    let view = fixture.view.clone();
    let options = Fixture::options(1);
    let subscription = subscribe(
        fixture.view.subscribe_events(options)?,
        Arc::new(move || {
            // Closing this channel proves that the driver released its recovery closure.
            let _capture = &driver_capture;
            replacements.fetch_add(1, Ordering::SeqCst);
            view.subscribe_events(options)
        }),
        fixture.executor.clone(),
        tokio::runtime::Handle::current(),
        None,
        move |context, _| {
            let _capture = &callback_capture;
            captured_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(entered) = lock(&entered).take() {
                let _ = entered.send(context);
            }
            lock(&released)
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        },
    )?;
    let worker = std::thread::spawn(fixture.executor.next().await?);
    let context = bounded(started).await??;
    fixture.publish(NetworkStatus::Unavailable)?;
    fixture.publish(NetworkStatus::Available)?;
    let mut close = Box::pin(subscription.close());
    let pending = futures::poll!(close.as_mut()).is_pending();
    drop(close);
    let already_unsubscribed = !context.unsubscribe();
    let callback_retained = matches!(
        callback_retired.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
    release.send(())?;
    worker.join().map_err(|_| "callback worker panicked")?;
    assert!(pending, "close returned before the callback retired");
    assert!(
        already_unsubscribed,
        "cancelling close reactivated delivery"
    );
    assert!(
        callback_retained,
        "cancelling close interrupted the callback"
    );
    assert!(bounded(callback_retired).await?.is_err());
    assert!(bounded(driver_retired).await?.is_err());
    fixture.publish(NetworkStatus::Unavailable)?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.replacements.load(Ordering::SeqCst), 0);
    assert!(lock(&fixture.executor.jobs).is_empty());
    Ok(())
}

#[tokio::test]
#[cfg(test)]
async fn close_from_callback_cancels_and_returns_explicit_context_error() -> TestResult {
    let fixture = Fixture::new()?;
    let owner = Arc::new(Mutex::new(None::<Subscription>));
    let captured = owner.clone();
    let error = Arc::new(Mutex::new(None));
    let observed = error.clone();
    let subscription = fixture.observer(Fixture::options(1), move |context, _| {
        let subscription = lock(&captured).take().unwrap();
        assert_eq!(context.id(), subscription.id());
        *lock(&observed) = futures::executor::block_on(subscription.close()).err();
        assert!(!context.unsubscribe());
    })?;
    *lock(&owner) = Some(subscription);
    fixture.executor.run_one().await?;
    let error = lock(&error)
        .clone()
        .ok_or("callback close incorrectly succeeded")?;
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.context().stage, Some(ErrorStage::Dispatch));
    assert_eq!(
        error.config_error().map(|error| error.field()),
        Some("subscription.close")
    );
    assert_eq!(fixture.replacements.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
#[cfg(test)]
async fn callback_panic_stops_and_is_preserved_by_close() -> TestResult {
    let fixture = Fixture::new()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = calls.clone();
    let subscription = fixture.observer(Fixture::options(1), move |_, _| {
        captured.fetch_add(1, Ordering::SeqCst);
        panic!("expected observer panic");
    })?;
    fixture.executor.run_one().await?;
    let error = bounded(subscription.close())
        .await?
        .expect_err("panic was lost");
    assert_eq!(error.kind(), ErrorKind::CallbackPanicked);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.replacements.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
#[cfg(test)]
async fn rejected_and_abandoned_callback_jobs_end_without_attempting_error_callback() -> TestResult
{
    for abandoned in [false, true] {
        let fixture = Fixture::new()?;
        fixture.executor.reject.store(!abandoned, Ordering::SeqCst);
        let calls = Arc::new(AtomicUsize::new(0));
        let captured = calls.clone();
        let subscription = fixture.observer(Fixture::options(1), move |_, _| {
            captured.fetch_add(1, Ordering::SeqCst);
        })?;
        if abandoned {
            drop(fixture.executor.next().await?);
        } else {
            bounded(async {
                while subscription.is_active() {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
        }
        let error = bounded(subscription.close())
            .await?
            .expect_err("dispatch failure was lost");
        assert_eq!(
            error.kind(),
            if abandoned {
                ErrorKind::RuntimeUnavailable
            } else {
                ErrorKind::QueueFull
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.replacements.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
#[cfg(test)]
async fn oversized_later_event_is_terminal_and_does_not_resubscribe() -> TestResult {
    let fixture = Fixture::new()?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let captured = calls.clone();
    let subscription = fixture.observer(
        NetworkEventOptions {
            capacity: 2,
            max_bytes: std::mem::size_of::<NetworkEvent>(),
        },
        move |_, value| lock(&captured).push(value),
    )?;
    fixture.executor.run_one().await?;
    fixture
        .publisher
        .prepare_observation(NetworkStatus::Unavailable, None, Some("x".repeat(256)))?
        .dispatch()?;
    fixture.executor.run_one().await?;
    assert!(matches!(&lock(&calls)[1], Err(error) if error.kind() == ErrorKind::ItemTooLarge));
    let error = bounded(subscription.close())
        .await?
        .expect_err("oversized event fault was lost");
    assert_eq!(error.kind(), ErrorKind::ItemTooLarge);
    assert_eq!(fixture.replacements.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(test)]
async fn cancellation_during_replacement_discards_it_and_close_waits_for_setup() -> TestResult {
    let fixture = Fixture::new()?;
    let (entered, started) = oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let view = fixture.view.clone();
    let options = Fixture::options(1);
    let subscription = subscribe(
        fixture.view.subscribe_events(options)?,
        Arc::new(move || {
            if let Some(entered) = lock(&entered).take() {
                let _ = entered.send(());
            }
            lock(&released)
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            view.subscribe_events(options)
        }),
        fixture.executor.clone(),
        tokio::runtime::Handle::current(),
        None,
        |_, _| {},
    )?;
    let initial = fixture.executor.next().await?;
    fixture.publish(NetworkStatus::Unavailable)?;
    fixture.publish(NetworkStatus::Available)?;
    initial();
    fixture.executor.run_one().await?;
    bounded(started).await??;
    let mut close = Box::pin(subscription.close());
    assert!(futures::poll!(close.as_mut()).is_pending());
    release.send(())?;
    bounded(close).await??;
    assert!(lock(&fixture.executor.jobs).is_empty());
    // Recovering/cancelled registrations must return every original quota slot.
    let receivers: Result<Vec<_>> = (0..1024)
        .map(|_| fixture.view.subscribe_events(options))
        .collect();
    assert_eq!(receivers?.len(), 1024);
    Ok(())
}

#[tokio::test]
#[cfg(test)]
async fn replacement_failure_is_reported_once_and_retained_for_close() -> TestResult {
    let fixture = Fixture::new()?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let captured = calls.clone();
    let subscription = subscribe(
        fixture.view.subscribe_events(Fixture::options(1))?,
        Arc::new(|| Err(NetError::from(ErrorKind::SubscriptionLimitReached))),
        fixture.executor.clone(),
        tokio::runtime::Handle::current(),
        None,
        move |_, value| lock(&captured).push(value),
    )?;
    let initial = fixture.executor.next().await?;
    fixture.publish(NetworkStatus::Unavailable)?;
    fixture.publish(NetworkStatus::Available)?;
    initial();
    fixture.executor.run_one().await?;
    fixture.executor.run_one().await?;
    assert!(
        matches!(&lock(&calls)[2], Err(error) if error.kind() == ErrorKind::SubscriptionLimitReached)
    );
    assert_eq!(
        bounded(subscription.close())
            .await?
            .expect_err("replacement failure was lost")
            .kind(),
        ErrorKind::SubscriptionLimitReached
    );
    Ok(())
}

#[test]
#[cfg(test)]
fn stopped_runtime_retires_an_unpolled_driver_and_its_receiver() -> TestResult {
    let fixture = Fixture::new()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let options = Fixture::options(1);
    let view = fixture.view.clone();
    let subscription = subscribe(
        fixture.view.subscribe_events(options)?,
        Arc::new(move || view.subscribe_events(options)),
        fixture.executor.clone(),
        runtime.handle().clone(),
        None,
        |_, _| panic!("driver never ran"),
    )?;
    drop(runtime);
    assert!(!subscription.is_active());
    assert_eq!(
        futures::executor::block_on(subscription.close())
            .expect_err("abandoned driver appeared successful")
            .kind(),
        ErrorKind::RuntimeUnavailable
    );
    assert!(lock(&fixture.executor.jobs).is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(test)]
async fn root_drop_during_overflow_delivers_closed_and_releases_engine_with_handle_retained(
) -> TestResult {
    use crate::common::CommonEngine;
    use crate::module::net_status::inner::{
        facade::NetworkObservationOwner, shared::SharedNetworkService,
    };
    let engine = Arc::new(CommonEngine::new_with_runtime_worker_threads(
        16,
        16,
        Some(1),
    )?);
    let weak_engine = Arc::downgrade(&engine);
    let service = SharedNetworkService::new(engine.clone())?;
    let owner = NetworkObservationOwner::new(service.context())?;
    let source = service.context().observation_source()?;
    let (publisher, initial) = source.begin_generation()?;
    initial.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Available, None, None)?
        .dispatch()?;
    owner.activate_view_for_test()?;
    let (entered, started) = oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let subscription = owner.on_recoverable_event(Fixture::options(1), move |_, value| {
        if let Some(entered) = lock(&entered).take() {
            let _ = entered.send(());
            lock(&released)
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        let _ = sent.send(value);
    })?;
    bounded(started).await??;
    publisher
        .prepare_observation(NetworkStatus::Unavailable, None, None)?
        .dispatch()?;
    // Root-service Drop closes the source while its final event cannot fit.
    // Drop the view owner too: recovery must not depend on upgrading that owner.
    drop(service);
    drop(owner);
    drop(publisher);
    drop(source);
    drop(engine);
    release.send(())?;
    let closed = bounded(async {
        while let Some(observation) = received.recv().await {
            match observation? {
                NetworkObservation::Reconciled { current, .. }
                    if matches!(current.snapshot.state, MonitorState::Closed) =>
                {
                    return Ok::<_, NetError>(true)
                }
                _ => {}
            }
        }
        Ok(false)
    })
    .await??;
    assert!(closed, "root drop lost the final reconciliation");
    bounded(async {
        while subscription.is_active() || weak_engine.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    // A retained, inactive logical handle must not retain its source/runtime.
    assert!(weak_engine.upgrade().is_none());
    bounded(subscription.close()).await??;
    Ok(())
}

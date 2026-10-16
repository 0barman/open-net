//! Worker tests use explicit gates and never invoke an operating-system monitor.
use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;
use std::task::{Context, Poll};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn observation() -> SampleObservation {
    SampleObservation {
        reachability: NetworkStatus::Available,
        network_name: Some("worker-test".into()),
        name_source: NetworkNameSource::NetworkListManager,
        name_diagnostics: Vec::new(),
    }
}

fn request_deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
}

async fn bounded<F: Future>(future: F) -> Result<F::Output, tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(5), future).await
}

/// The release sender's Drop also unblocks the provider after a failed assertion.
struct WorkerFixture {
    sampler: Arc<WindowsSampler>,
    entered: tokio::sync::oneshot::Receiver<()>,
    release: mpsc::Sender<()>,
    calls: Arc<AtomicUsize>,
    changed_thread: Arc<AtomicBool>,
}

fn gated_worker() -> WorkerFixture {
    let (entered, received) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let thread = Mutex::new(None);
    let changed_thread = Arc::new(AtomicBool::new(false));
    let changed = changed_thread.clone();
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(move |_, _| {
        let current = std::thread::current().id();
        let mut previous = thread.lock().map_err(NetError::from_poison)?;
        if previous.is_some_and(|id| id != current) {
            changed.store(true, Ordering::Release);
        }
        *previous = Some(current);
        drop(previous);
        if count.fetch_add(1, Ordering::AcqRel) == 0 {
            if let Some(entered) = entered.lock().map_err(NetError::from_poison)?.take() {
                let _ = entered.send(());
            }
            released
                .lock()
                .map_err(NetError::from_poison)?
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| error(ErrorKind::Internal))?;
        }
        Ok(observation())
    })));
    WorkerFixture {
        sampler,
        entered: received,
        release,
        calls,
        changed_thread,
    }
}

fn launch(
    sampler: Arc<WindowsSampler>,
    cancelled: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<Result<CompletedSample, NetError>> {
    tokio::spawn(async move { sampler.sample(request_deadline(), cancelled).await })
}

#[cfg(test)]
#[tokio::test]
async fn dropped_queued_future_never_invokes_the_provider() -> TestResult {
    let fixture = gated_worker();
    let first = launch(fixture.sampler.clone(), Arc::new(AtomicBool::new(false)));
    bounded(fixture.entered).await??;
    let mut abandoned = Box::pin(
        fixture
            .sampler
            .sample(request_deadline(), Arc::new(AtomicBool::new(false))),
    );
    assert!(poll_once(abandoned.as_mut()).is_pending());
    drop(abandoned);
    fixture.release.send(())?;
    bounded(first).await???;
    // Completion of a later request establishes that the abandoned queue item
    // has been inspected, without relying on a sleep or a scheduling guess.
    bounded(
        fixture
            .sampler
            .sample(request_deadline(), Arc::new(AtomicBool::new(false))),
    )
    .await??;
    assert_eq!(fixture.calls.load(Ordering::Acquire), 2);
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn sixteen_accepted_requests_share_one_blocked_worker() -> TestResult {
    let fixture = gated_worker();
    let first = launch(fixture.sampler.clone(), Arc::new(AtomicBool::new(false)));
    bounded(fixture.entered).await??;
    let mut queued = Vec::new();
    for _ in 0..15 {
        let mut request = Box::pin(
            fixture
                .sampler
                .sample(request_deadline(), Arc::new(AtomicBool::new(false))),
        );
        assert!(poll_once(request.as_mut()).is_pending());
        queued.push(request);
    }
    let rejected = fixture
        .sampler
        .sample(request_deadline(), Arc::new(AtomicBool::new(false)))
        .await;
    assert_eq!(
        rejected.err().ok_or("accepted beyond capacity")?.kind(),
        ErrorKind::QueueFull
    );
    assert_eq!(fixture.calls.load(Ordering::Acquire), 1);
    fixture.release.send(())?;
    let mut previous_id = bounded(first).await???.id;
    for request in queued {
        let completed = bounded(request).await??;
        assert!(completed.id > previous_id);
        previous_id = completed.id;
    }
    assert_eq!(fixture.calls.load(Ordering::Acquire), 16);
    assert!(!fixture.changed_thread.load(Ordering::Acquire));
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn cancelling_one_running_query_does_not_cancel_the_next() -> TestResult {
    let fixture = gated_worker();
    let cancel = Cancellation::new();
    let first = launch(fixture.sampler.clone(), cancel.flag.clone());
    bounded(fixture.entered).await??;
    let mut next = Box::pin(
        fixture
            .sampler
            .sample(request_deadline(), Arc::new(AtomicBool::new(false))),
    );
    assert!(poll_once(next.as_mut()).is_pending());
    drop(cancel);
    fixture.release.send(())?;
    let cancelled = bounded(first).await??;
    assert_eq!(
        cancelled.err().ok_or("cancelled sample succeeded")?.kind(),
        ErrorKind::Cancelled
    );
    let completed = bounded(next).await??;
    assert_eq!(
        completed.observation.network_name.as_deref(),
        Some("worker-test")
    );
    assert_eq!(fixture.calls.load(Ordering::Acquire), 2);
    assert!(!fixture.changed_thread.load(Ordering::Acquire));
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn provider_panic_releases_its_resources_and_worker_continues() -> TestResult {
    struct Resource(Arc<AtomicUsize>);
    impl Drop for Resource {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let released = Arc::new(AtomicUsize::new(0));
    let releases = released.clone();
    let sampler = WindowsSampler::with_provider(Arc::new(move |_, _| {
        let _resource = Resource(releases.clone());
        if count.fetch_add(1, Ordering::AcqRel) == 0 {
            panic!("injected provider panic");
        }
        Ok(observation())
    }));
    let first =
        bounded(sampler.sample(request_deadline(), Arc::new(AtomicBool::new(false)))).await?;
    assert_eq!(
        first.err().ok_or("panic was hidden")?.kind(),
        ErrorKind::Internal
    );
    assert_eq!(released.load(Ordering::Acquire), 1);
    let next =
        bounded(sampler.sample(request_deadline(), Arc::new(AtomicBool::new(false)))).await??;
    assert_eq!(next.id, 2);
    assert_eq!(calls.load(Ordering::Acquire), 2);
    assert_eq!(released.load(Ordering::Acquire), 2);
    Ok(())
}

#[cfg(test)]
#[test]
fn worker_can_be_polled_without_a_tokio_runtime() -> TestResult {
    let sampler = WindowsSampler::with_provider(Arc::new(|_, _| Ok(observation())));
    let completed = futures::executor::block_on(
        sampler.sample(request_deadline(), Arc::new(AtomicBool::new(false))),
    )?;
    assert_eq!(completed.id, 1);
    assert_eq!(completed.observation.reachability, NetworkStatus::Available);
    Ok(())
}

#[tokio::test]
async fn close_cancels_running_work_rejects_queue_and_joins_with_retained_sampler() -> TestResult {
    let fixture = gated_worker();
    let cancelled = Arc::new(AtomicBool::new(false));
    let first = launch(fixture.sampler.clone(), cancelled.clone());
    bounded(fixture.entered).await??;
    let mut queued = Box::pin(
        fixture
            .sampler
            .sample(request_deadline(), Arc::new(AtomicBool::new(false))),
    );
    assert!(poll_once(queued.as_mut()).is_pending());
    fixture.sampler.close();
    fixture.sampler.close();
    assert!(cancelled.load(Ordering::Acquire));
    let rejected = fixture
        .sampler
        .sample(request_deadline(), Arc::new(AtomicBool::new(false)))
        .await;
    assert_eq!(
        rejected
            .err()
            .ok_or("closed sampler accepted a request")?
            .kind(),
        ErrorKind::Closed
    );
    let background = fixture
        .sampler
        .sample_background(request_deadline(), Arc::new(AtomicBool::new(false)))
        .await;
    assert_eq!(
        background
            .err()
            .ok_or("closed sampler accepted background work")?
            .kind(),
        ErrorKind::Closed
    );
    let threads = fixture.sampler.threads.clone();
    let joining = tokio::task::spawn_blocking(move || threads.seal_and_join());
    fixture.release.send(())?;
    let running_result = bounded(first).await??;
    assert_eq!(
        running_result
            .err()
            .ok_or("running work ignored close")?
            .kind(),
        ErrorKind::Cancelled
    );
    let queued_result = bounded(queued).await?;
    assert_eq!(
        queued_result
            .err()
            .ok_or("queued work ran after close")?
            .kind(),
        ErrorKind::Closed
    );
    bounded(joining).await???;
    assert_eq!(fixture.calls.load(Ordering::Acquire), 1);
    // The sampler remains alive here: joining cannot depend on its last Arc drop.
    assert!(
        fixture
            .sampler
            .state
            .lock()
            .map_err(NetError::from_poison)?
            .closed
    );
    Ok(())
}

#[test]
fn close_before_first_query_does_not_start_a_worker() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let sampler = WindowsSampler::with_provider(Arc::new(move |_, _| {
        called.fetch_add(1, Ordering::AcqRel);
        Ok(observation())
    }));
    sampler.close();
    let result = futures::executor::block_on(
        sampler.sample_background(request_deadline(), Arc::new(AtomicBool::new(false))),
    );
    assert_eq!(
        result.err().ok_or("closed sampler started")?.kind(),
        ErrorKind::Closed
    );
    sampler.threads.seal_and_join()?;
    assert_eq!(calls.load(Ordering::Acquire), 0);
    Ok(())
}

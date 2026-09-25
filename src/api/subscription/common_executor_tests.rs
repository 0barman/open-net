use super::CommonCallbackExecutor;
use crate::common::CommonEngine;
use crate::error::ErrorKind;
use crate::subscription::{CallbackExecutor, StateSource};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

type TestResult = std::result::Result<(), crate::BoxError>;

#[test]
fn constructing_subscription_executor_keeps_the_existing_pool_lazy() -> TestResult {
    let engine = CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1))?;
    let executor = CommonCallbackExecutor::new(&engine);
    if engine.cb_pool.max_count() != 0 {
        return Err("subscription executor eagerly started callback threads".into());
    }
    executor.ensure_ready()?;
    let workers = engine.cb_pool.max_count();
    if workers == 0 {
        return Err("subscription did not start the existing callback pool".into());
    }
    let second = CommonCallbackExecutor::new(&engine);
    second.ensure_ready()?;
    if engine.cb_pool.max_count() != workers {
        return Err("a second subscriber changed the callback worker set".into());
    }
    Ok(())
}

#[test]
fn callbacks_use_the_existing_pool_and_network_runtime_context() -> TestResult {
    let engine = CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1))?;
    let executor = CommonCallbackExecutor::new(&engine);
    executor.ensure_ready()?;
    let caller = std::thread::current().id();
    let (sent, received) = mpsc::channel();
    executor.submit(Box::new(move || {
        let thread = std::thread::current();
        let _ = sent.send((
            thread.id(),
            thread.name().map(str::to_owned),
            tokio::runtime::Handle::try_current().is_ok(),
        ));
    }))?;
    let (thread, name, has_runtime) = received.recv_timeout(Duration::from_secs(5))?;
    if thread == caller
        || !name.is_some_and(|name| name.starts_with("open-net-callback-"))
        || !has_runtime
    {
        return Err("network subscription changed callback thread or runtime context".into());
    }
    Ok(())
}

#[test]
fn callback_pool_startup_failure_is_returned_and_allows_retry() -> TestResult {
    let engine = CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1))?;
    let executor = CommonCallbackExecutor::new(&engine);
    engine.cb_pool.fail_next_start_after(1);
    let error = executor
        .ensure_ready()
        .err()
        .ok_or("injected pool startup failure was not returned")?;
    if error.kind() != ErrorKind::RuntimeUnavailable || engine.cb_pool.max_count() != 0 {
        return Err("failed callback pool startup changed category or published workers".into());
    }
    executor.ensure_ready()?;
    if engine.cb_pool.max_count() == 0 {
        return Err("callback pool could not retry after startup failure".into());
    }
    Ok(())
}

#[test]
fn observing_executor_does_not_keep_the_engine_owner_alive() -> TestResult {
    let engine = Arc::new(CommonEngine::new_with_runtime_worker_threads(
        4,
        4,
        Some(1),
    )?);
    let owner = Arc::downgrade(&engine);
    let executor = CommonCallbackExecutor::new(&engine);
    executor.ensure_ready()?;
    drop(engine);
    if owner.upgrade().is_some() {
        return Err("callback executor retained the business engine owner".into());
    }
    let (sent, received) = mpsc::channel();
    executor.submit(Box::new(move || {
        let _ = sent.send(());
    }))?;
    received.recv_timeout(Duration::from_secs(5))?;
    Ok(())
}

#[tokio::test]
async fn receiving_state_alone_never_starts_callback_workers() -> TestResult {
    let engine = CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1))?;
    let executor = Arc::new(CommonCallbackExecutor::new(&engine));
    let (publisher, source) = StateSource::new(10, executor, 4)?;
    let mut receiver = source.subscribe()?;
    publisher.publish(11)?;
    if receiver.current() != 11 || receiver.recv().await? != Some(10) {
        return Err("current consumed or replaced the subscription's initial state".into());
    }
    if receiver.recv().await? != Some(11) || engine.cb_pool.max_count() != 0 {
        return Err("state receiving lost an update or started callback workers".into());
    }
    publisher.finish(12)?;
    if receiver.recv().await? != Some(12) || receiver.recv().await?.is_some() {
        return Err("final state was not followed by stream termination".into());
    }
    Ok(())
}

#[tokio::test]
async fn first_callback_can_unsubscribe_before_guard_is_observed() -> TestResult {
    let engine = CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1))?;
    let executor = Arc::new(CommonCallbackExecutor::new(&engine));
    let (publisher, source) = StateSource::new(20, executor, 4)?;
    let receiver = source.subscribe()?;
    let id = receiver.id();
    publisher.publish(21)?;
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);
    let subscription = receiver.into_callback(move |context, value| {
        callback_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let cancelled = context.unsubscribe();
        let _ = sent.send((context.id(), value, cancelled));
    })?;
    let (callback_id, value, cancelled) =
        tokio::time::timeout(Duration::from_secs(5), received.recv())
            .await?
            .ok_or("missing first state callback")?;
    if callback_id != id || subscription.id() != id || value? != 20 || !cancelled {
        return Err("callback conversion lost initial state, identity or self-unsubscribe".into());
    }
    publisher.publish(22)?;
    subscription.close().await?;
    if calls.load(std::sync::atomic::Ordering::SeqCst) != 1 {
        return Err("queued or future state callbacks survived self-unsubscribe".into());
    }
    Ok(())
}

struct ReenterOnDrop {
    source: Arc<Mutex<Option<StateSource<crate::NetError>>>>,
    drops: Arc<std::sync::atomic::AtomicUsize>,
}

impl std::fmt::Debug for ReenterOnDrop {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReenterOnDrop")
    }
}

impl std::fmt::Display for ReenterOnDrop {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("state replacement probe")
    }
}

impl std::error::Error for ReenterOnDrop {}

impl Drop for ReenterOnDrop {
    fn drop(&mut self) {
        let source = match self.source.lock() {
            Ok(source) => source.clone(),
            Err(_) => return,
        };
        if let Some(source) = source {
            let _current = source.current();
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

#[test]
fn replacing_the_last_error_source_allows_reentrant_snapshot_reads() -> TestResult {
    let engine = CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1))?;
    let holder = Arc::new(Mutex::new(None));
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let initial = crate::NetError::protocol(ReenterOnDrop {
        source: Arc::clone(&holder),
        drops: Arc::clone(&drops),
    });
    let (publisher, source) =
        StateSource::new(initial, Arc::new(CommonCallbackExecutor::new(&engine)), 4)?;
    *holder.lock().map_err(|_| "state holder lock poisoned")? = Some(source);
    let (sent, received) = mpsc::channel();
    let worker = std::thread::Builder::new().spawn(move || {
        let result = publisher.publish(crate::NetError::from(ErrorKind::Closed));
        let _ = sent.send(result);
    })?;
    received.recv_timeout(Duration::from_secs(5))??;
    worker
        .join()
        .map_err(|_| "state replacement thread failed")?;
    if drops.load(std::sync::atomic::Ordering::SeqCst) != 1 {
        return Err(
            "the last error source was retained or did not reenter the state source".into(),
        );
    }
    holder
        .lock()
        .map_err(|_| "state holder lock poisoned")?
        .take();
    Ok(())
}

struct ReenterOnWake {
    source: StateSource<u64>,
    sent: mpsc::Sender<u64>,
}

impl std::task::Wake for ReenterOnWake {
    fn wake(self: Arc<Self>) {
        let _ = self.sent.send(self.source.current());
    }
}

#[test]
fn notifying_a_receiver_allows_its_waker_to_read_the_source() -> TestResult {
    let engine = CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1))?;
    let (publisher, source) =
        StateSource::new(30, Arc::new(CommonCallbackExecutor::new(&engine)), 4)?;
    let mut receiver = source.subscribe()?;
    let (sent, received) = mpsc::channel();
    let waker = std::task::Waker::from(Arc::new(ReenterOnWake { source, sent }));
    let mut context = std::task::Context::from_waker(&waker);
    if !matches!(
        futures::Stream::poll_next(std::pin::Pin::new(&mut receiver), &mut context),
        std::task::Poll::Ready(Some(Ok(30)))
    ) || !matches!(
        futures::Stream::poll_next(std::pin::Pin::new(&mut receiver), &mut context),
        std::task::Poll::Pending
    ) {
        return Err("receiver did not install a waker after its initial state".into());
    }
    let (done_sent, done_received) = mpsc::channel();
    let worker = std::thread::Builder::new().spawn(move || {
        let result = publisher.publish(31);
        let _ = done_sent.send(result);
    })?;
    if received.recv_timeout(Duration::from_secs(5))? != 31 {
        return Err("reentrant waker observed an obsolete state".into());
    }
    done_received.recv_timeout(Duration::from_secs(5))??;
    worker.join().map_err(|_| "state wake thread failed")?;
    Ok(())
}

use super::CommonEngine;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::oneshot;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(5);

#[track_caller]
fn test_error(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    let location = std::panic::Location::caller();
    let message = message.into();
    std::io::Error::other(format!("{location}: {message}")).into()
}

async fn wait_for_last_owner<T: Send + Sync + 'static>(owner: Arc<T>) -> TestResult<Arc<T>> {
    let stopped = tokio::time::timeout(WAIT, async {
        while Arc::strong_count(&owner) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    if stopped.is_err() {
        // A failed precondition must not itself drop the old raw Runtime inside
        // this async test. Move cleanup to a thread that permits blocking.
        tokio::task::spawn_blocking(move || drop(owner)).await?;
        return Err(test_error("common engine runtime owner did not exit"));
    }
    Ok(owner)
}

#[tokio::test]
async fn last_common_runtime_owner_can_drop_in_async_context() -> TestResult {
    let engine = CommonEngine::new(4, 4)
        .map_err(|error| test_error(format!("create common engine: {error:?}")))?;
    // Match CommonEngine's field drop order, then force the background owner to
    // finish first. This makes the real teardown race deterministic.
    let CommonEngine {
        cb_pool,
        async_tx,
        sync_tx,
        rt,
        ..
    } = engine;
    drop(cb_pool);
    drop(async_tx);
    drop(sync_tx);
    let rt = wait_for_last_owner(rt).await?;
    tokio::spawn(async move {
        drop(rt);
    })
    .await
    .map_err(|error| test_error(format!("async runtime destruction failed: {error}")))?;
    Ok(())
}

struct BlockingGate {
    release: Option<std::sync::mpsc::Sender<()>>,
    task: Option<tokio::task::JoinHandle<TestResult>>,
}

impl BlockingGate {
    fn start(handle: &Handle) -> (Self, oneshot::Receiver<()>) {
        let (release, wait_for_release) = std::sync::mpsc::channel();
        let (started, start) = oneshot::channel();
        let task = handle.spawn_blocking(move || {
            if started.send(()).is_ok() {
                wait_for_release.recv_timeout(WAIT)?;
            }
            Ok(())
        });
        (
            Self {
                release: Some(release),
                task: Some(task),
            },
            start,
        )
    }

    fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
    }

    async fn finish(&mut self) -> TestResult {
        let released = self
            .release
            .take()
            .ok_or_else(|| test_error("blocking gate was already released"))?
            .send(());
        let task = self
            .task
            .take()
            .ok_or_else(|| test_error("blocking task was already joined"))?;
        let completed = tokio::time::timeout(WAIT, task).await;
        released?;
        completed???;
        Ok(())
    }
}

impl Drop for BlockingGate {
    fn drop(&mut self) {
        // Closing the sender releases the blocking receiver on every error path.
        self.release.take();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_runtime_drop_returns_before_running_blocking_task_finishes() -> TestResult {
    let engine = CommonEngine::new(4, 4)
        .map_err(|error| test_error(format!("create common engine: {error:?}")))?;
    let CommonEngine {
        cb_pool,
        async_tx,
        sync_tx,
        rt,
        ..
    } = engine;
    drop(cb_pool);
    drop(async_tx);
    drop(sync_tx);
    let rt = wait_for_last_owner(rt).await?;
    let (mut gate, started) = BlockingGate::start(rt.handle());
    tokio::time::timeout(WAIT, started).await??;

    let mut drop_task = tokio::spawn(async move {
        drop(rt);
    });
    let early_drop = tokio::time::timeout(WAIT, &mut drop_task).await;
    let blocking_finished_before_release = gate.is_finished();
    let cleanup = gate.finish().await;
    let dropped = match early_drop {
        Ok(result) => result.map_err(|error| test_error(format!("async drop: {error}"))),
        Err(error) => {
            // Releasing the gate also permits cleanup of an incorrectly blocking
            // implementation before reporting the bounded-drop failure.
            tokio::time::timeout(WAIT, drop_task).await??;
            Err(test_error(format!(
                "async drop waited for blocking code: {error}"
            )))
        }
    };
    cleanup?;
    dropped?;
    if blocking_finished_before_release {
        return Err(test_error(
            "blocking task must stay gated until after async drop",
        ));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_thread_runtime_drop_waits_for_running_blocking_task() -> TestResult {
    let engine = CommonEngine::new(4, 4)
        .map_err(|error| test_error(format!("create common engine: {error:?}")))?;
    let CommonEngine {
        cb_pool,
        async_tx,
        sync_tx,
        rt,
        ..
    } = engine;
    drop(cb_pool);
    drop(async_tx);
    drop(sync_tx);
    let rt = wait_for_last_owner(rt).await?;
    let (mut gate, started) = BlockingGate::start(rt.handle());
    tokio::time::timeout(WAIT, started).await??;

    let (dropping, drop_started) = oneshot::channel();
    let (finished, mut drop_finished) = oneshot::channel();
    let drop_thread = std::thread::Builder::new()
        .name("common-runtime-drop-test".to_string())
        .spawn(move || {
            let _ = dropping.send(());
            drop(rt);
            let _ = finished.send(());
        })?;
    let entered = tokio::time::timeout(WAIT, drop_started).await;
    // The running blocking task is already at a receive gate. A synchronous
    // runtime destructor must not finish before the test releases that gate.
    let early_drop = tokio::time::timeout(Duration::from_millis(100), &mut drop_finished).await;
    let returned_before_release = early_drop.is_ok();
    let cleanup = gate.finish().await;
    let completed = match early_drop {
        Ok(result) => Ok(result),
        Err(_) => tokio::time::timeout(WAIT, &mut drop_finished).await,
    };
    let joined = tokio::task::spawn_blocking(move || drop_thread.join()).await?;
    cleanup?;
    entered??;
    completed??;
    joined.map_err(|_| test_error("ordinary runtime drop thread failed"))?;
    if returned_before_release {
        return Err(test_error(
            "ordinary thread drop stopped waiting for blocking work",
        ));
    }
    Ok(())
}

#[test]
fn explicit_shutdown_joins_callbacks_and_blocking_tasks_with_live_handles() -> TestResult {
    let engine = Arc::new(
        CommonEngine::new(4, 4).map_err(|error| test_error(format!("create engine: {error:?}")))?,
    );
    let retained_pool = engine.cb_pool.clone();
    let (callback_started, callback_entry) = std::sync::mpsc::channel();
    let (callback_release, callback_gate) = std::sync::mpsc::channel();
    retained_pool
        .execute(move || {
            let _ = callback_started.send(());
            let _ = callback_gate.recv_timeout(WAIT);
        })
        .map_err(|error| test_error(format!("submit callback: {error:?}")))?;
    callback_entry.recv_timeout(WAIT)?;
    let (blocking_started, blocking_entry) = std::sync::mpsc::channel();
    let (blocking_release, blocking_gate) = std::sync::mpsc::channel();
    engine.runtime_handle().spawn_blocking(move || {
        let _ = blocking_started.send(());
        let _ = blocking_gate.recv_timeout(WAIT);
    });
    blocking_entry.recv_timeout(WAIT)?;
    let (finished, completion) = std::sync::mpsc::channel();
    let closing = Arc::clone(&engine);
    let coordinator = std::thread::spawn(move || {
        let _ = finished.send(closing.shutdown_blocking());
    });
    assert!(completion.recv_timeout(Duration::from_millis(50)).is_err());
    callback_release.send(())?;
    assert!(completion.recv_timeout(Duration::from_millis(50)).is_err());
    blocking_release.send(())?;
    assert_eq!(completion.recv_timeout(WAIT)?, Ok(()));
    coordinator
        .join()
        .map_err(|_| test_error("shutdown coordinator panicked"))?;
    assert_eq!(
        retained_pool.ensure_ready(),
        Err(super::CommonError::PostError)
    );
    assert_eq!(engine.shutdown_blocking(), Ok(()));
    Ok(())
}

#[test]
fn management_drain_finishes_accepted_work_and_keeps_runtime_available() -> TestResult {
    let engine =
        CommonEngine::new(4, 4).map_err(|error| test_error(format!("create engine: {error:?}")))?;
    let (sent, received) = std::sync::mpsc::channel();
    for index in 0..4 {
        let sent = sent.clone();
        engine.post(async move {
            let _ = sent.send(index);
        });
    }
    assert_eq!(engine.drain_management(), Ok(()));
    for index in 0..4 {
        assert_eq!(received.recv_timeout(WAIT)?, index);
    }
    let (sent, received) = std::sync::mpsc::channel();
    engine.runtime_handle().spawn(async move {
        let _ = sent.send(());
    });
    received.recv_timeout(WAIT)?;
    assert_eq!(engine.invoke(async {}), Err(super::CommonError::PostError));
    assert_eq!(engine.shutdown_blocking(), Ok(()));
    Ok(())
}

#[test]
fn shutdown_keeps_callback_pool_open_until_deferred_retirement_dispatch_finishes() -> TestResult {
    let engine = Arc::new(
        CommonEngine::new(4, 4).map_err(|error| test_error(format!("create engine: {error:?}")))?,
    );
    let lease = engine.retirement_work().register();
    let pool = engine.cb_pool.clone();
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let (callback, callback_received) = std::sync::mpsc::channel();
    let (submitted, submission) = std::sync::mpsc::channel();
    engine.runtime_handle().spawn_blocking(move || {
        let _lease = lease;
        let _ = entered.send(());
        let _ = gate.recv_timeout(WAIT);
        let result = pool.execute(move || {
            let _ = callback.send(());
        });
        let _ = submitted.send(result);
    });
    entry.recv_timeout(WAIT)?;
    let (finished, completion) = std::sync::mpsc::channel();
    let retiring = Arc::clone(&engine);
    let coordinator = std::thread::spawn(move || {
        let _ = finished.send(retiring.shutdown_blocking());
    });
    assert!(completion.recv_timeout(Duration::from_millis(50)).is_err());
    release.send(())?;
    assert_eq!(submission.recv_timeout(WAIT)?, Ok(()));
    assert_eq!(completion.recv_timeout(WAIT)?, Ok(()));
    callback_received.recv_timeout(WAIT)?;
    coordinator
        .join()
        .map_err(|_| test_error("retirement dispatch coordinator panicked"))?;
    Ok(())
}

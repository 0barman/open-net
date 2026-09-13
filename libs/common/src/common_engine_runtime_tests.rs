use super::CommonEngine;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::oneshot;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(5);

fn test_error(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::other(message.into()).into()
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

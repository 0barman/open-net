use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::module::ws_client::ws_client_inner::WSClientInner;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct ReleaseOnDrop(Option<std::sync::mpsc::Sender<()>>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        // Disconnecting releases the callback even when the test returns early.
        self.0.take();
    }
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn start_lane(worker: &mut WSClientWorker) -> TestResult<AbortOnDrop> {
    let receiver = worker
        .status_callback_rx
        .take()
        .ok_or_else(|| test_error("missing status callback receiver"))?;
    Ok(AbortOnDrop(tokio::spawn(status_callback_loop(
        Arc::clone(&worker.listeners),
        Arc::clone(&worker.state),
        receiver,
    ))))
}

async fn wait_until_dequeued(worker: &WSClientWorker) -> TestResult {
    // These tests use a current-thread runtime: after dequeuing, the lane keeps
    // polling until its initialization/completion wait yields to this test.
    tokio::time::timeout(TEST_TIMEOUT, async {
        while worker.status_callback_tx.capacity() != worker.status_callback_tx.max_capacity() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

type BlockedRegistration = (
    ReleaseOnDrop,
    std::thread::JoinHandle<()>,
    mpsc::UnboundedReceiver<ConnectionStatus>,
);

async fn register_blocked_initial(inner: Arc<WSClientInner>) -> TestResult<BlockedRegistration> {
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release = ReleaseOnDrop(Some(release_tx));
    let release_rx = Mutex::new(release_rx);
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let (seen_tx, seen_rx) = mpsc::unbounded_channel();
    let calls = AtomicUsize::new(0);
    let register = std::thread::Builder::new().spawn(move || {
        inner.register_status_listener(Box::new(move |status| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let _ = entered_tx.send(());
                match release_rx.lock() {
                    Ok(receiver) => {
                        // Only the RAII sender can release this call. An automatic
                        // timeout could hide a broken registration-retirement signal.
                        let _ = receiver.recv();
                    }
                    Err(error) => {
                        crate::log_e!(LogType::WSC; "blocked_initial_test", "error", error.to_string());
                    }
                }
            }
            let _ = seen_tx.send(status);
        }));
    })?;
    tokio::time::timeout(TEST_TIMEOUT, entered_rx.recv())
        .await?
        .ok_or_else(|| test_error("initial callback did not start"))?;
    Ok((release, register, seen_rx))
}

#[tokio::test]
async fn initial_status_finishes_before_later_status_and_lane_reads_latest_state() -> TestResult {
    let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())
        .map_err(|error| test_error(format!("create client: {error:?}")))?;
    let (release, register, mut seen) = register_blocked_initial(inner).await?;
    let _lane = start_lane(&mut worker)?;
    worker.set_status(ConnectionStatus::Connecting).await;
    // Give the queued event a chance to reach the initialization barrier.
    let early = tokio::time::timeout(Duration::from_millis(100), seen.recv()).await;
    worker.set_status(ConnectionStatus::Closing).await;
    worker.set_status(ConnectionStatus::Closed).await;
    drop(release);
    register
        .join()
        .map_err(|_| test_error("registration thread did not finish normally"))?;
    check!(
        early.is_err(),
        "later status overtook the blocked initial callback"
    )?;
    check_eq!(
        tokio::time::timeout(TEST_TIMEOUT, seen.recv()).await?,
        Some(ConnectionStatus::Idle)
    )?;
    check_eq!(
        tokio::time::timeout(TEST_TIMEOUT, seen.recv()).await?,
        Some(ConnectionStatus::Closed)
    )?;
    Ok(())
}

#[tokio::test]
async fn replacing_blocked_initial_listener_does_not_strand_status_lane() -> TestResult {
    let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())
        .map_err(|error| test_error(format!("create client: {error:?}")))?;
    let (release, register, _old_seen) = register_blocked_initial(Arc::clone(&inner)).await?;
    let _lane = start_lane(&mut worker)?;
    worker.set_status(ConnectionStatus::Connecting).await;
    wait_until_dequeued(&worker).await?;
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    inner.register_status_listener(Box::new(move |status| {
        let _ = seen_tx.send(status);
    }));
    tokio::time::timeout(TEST_TIMEOUT, seen_rx.recv())
        .await?
        .ok_or_else(|| test_error("replacement initial callback missing"))?;
    worker.set_status(ConnectionStatus::Closing).await;
    worker.set_status(ConnectionStatus::Closed).await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        while let Some(status) = seen_rx.recv().await {
            if status == ConnectionStatus::Closed {
                return Ok(());
            }
        }
        Err(test_error("replacement never received Closed"))
    })
    .await??;
    drop(release);
    register
        .join()
        .map_err(|_| test_error("registration thread did not finish normally"))?;
    Ok(())
}

#[tokio::test]
async fn unregistering_blocked_initial_listener_releases_delivery_waiter() -> TestResult {
    let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())
        .map_err(|error| test_error(format!("create client: {error:?}")))?;
    let (release, register, _seen) = register_blocked_initial(Arc::clone(&inner)).await?;
    let _lane = start_lane(&mut worker)?;
    let (delivered_tx, delivered_rx) = oneshot::channel();
    worker
        .status_callback_tx
        .send(CallbackEvent::Status {
            status: ConnectionStatus::Idle,
            delivered: Some(delivered_tx),
        })
        .await?;
    wait_until_dequeued(&worker).await?;
    inner.unregister_status_listener();
    tokio::time::timeout(TEST_TIMEOUT, delivered_rx).await??;
    drop(release);
    register
        .join()
        .map_err(|_| test_error("registration thread did not finish normally"))?;
    Ok(())
}

#[tokio::test]
async fn blocked_initial_status_does_not_prevent_bounded_worker_shutdown() -> TestResult {
    let (inner, worker) = WSClientInner::new(WebSocketClientConfig {
        close_timeout: Duration::from_millis(50),
        response_dispatch_grace: Duration::ZERO,
        ..WebSocketClientConfig::default()
    })
    .map_err(|error| test_error(format!("create client: {error:?}")))?;
    let (release, register, _seen) = register_blocked_initial(Arc::clone(&inner)).await?;
    let worker = AbortOnDrop(tokio::spawn(worker.run_async()));
    tokio::time::timeout(Duration::from_secs(2), inner.shutdown())
        .await?
        .map_err(|error| test_error(format!("shutdown failed: {error:?}")))?;
    check_eq!(inner.connection_status(), ConnectionStatus::Closed)?;
    drop(release);
    register
        .join()
        .map_err(|_| test_error("registration thread did not finish normally"))?;
    drop(worker);
    Ok(())
}

#[tokio::test]
async fn initial_status_callback_can_wait_for_shutdown_within_the_close_budget() -> TestResult {
    let (inner, worker) = WSClientInner::new(WebSocketClientConfig {
        close_timeout: Duration::from_millis(50),
        response_dispatch_grace: Duration::ZERO,
        ..WebSocketClientConfig::default()
    })
    .map_err(|error| test_error(format!("create client: {error:?}")))?;
    let worker = AbortOnDrop(tokio::spawn(worker.run_async()));
    let weak_inner = Arc::downgrade(&inner);
    let register_inner = Arc::clone(&inner);
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();
    let register = std::thread::Builder::new().spawn(move || {
        register_inner.register_status_listener(Box::new(move |_| {
            let result = (|| {
                let inner = weak_inner
                    .upgrade()
                    .ok_or_else(|| test_error("client dropped before callback shutdown"))?;
                futures::executor::block_on(inner.shutdown())
                    .map_err(|error| test_error(format!("reentrant shutdown: {error:?}")))?;
                check_eq!(inner.connection_status(), ConnectionStatus::Closed)
            })();
            let _ = done_tx.send(result);
        }));
    })?;
    tokio::time::timeout(Duration::from_secs(2), done_rx.recv())
        .await?
        .ok_or_else(|| test_error("reentrant shutdown callback did not complete"))??;
    register
        .join()
        .map_err(|_| test_error("registration thread did not finish normally"))?;
    drop(worker);
    Ok(())
}

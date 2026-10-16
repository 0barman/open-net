use crate::error::ErrorKind;
use crate::{NetError, OpenNet, OpenNetConfig};
use std::future::Future;
use std::pin::Pin;
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll};
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(5);

fn engine() -> Result<Arc<OpenNet>, NetError> {
    OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(1)).map(Arc::new)
}

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = futures::task::noop_waker();
    future.poll(&mut Context::from_waker(&waker))
}

/// Dropping the sender also releases the callback on every test failure path.
fn hold_callback(net: &OpenNet) -> TestResult<mpsc::Sender<()>> {
    let (entered, entry) = mpsc::channel();
    let (release, held) = mpsc::channel();
    net.inner
        .common_engine
        .cb_pool
        .execute(move || {
            let _ = entered.send(());
            let _ = held.recv_timeout(WAIT);
        })
        .map_err(NetError::from)?;
    entry.recv_timeout(WAIT)?;
    Ok(release)
}

#[test]
fn concurrent_shutdown_waiters_survive_an_abandoned_waiter() -> TestResult {
    let net = engine()?;
    let release = hold_callback(&net)?;
    let mut abandoned = Box::pin(net.shutdown());
    assert!(poll_once(abandoned.as_mut()).is_pending());
    drop(abandoned);

    let (sent, received) = mpsc::channel();
    let mut waiters = Vec::new();
    for _ in 0..4 {
        let net = Arc::clone(&net);
        let sent = sent.clone();
        waiters.push(std::thread::spawn(move || {
            let _ = sent.send(futures::executor::block_on(net.shutdown()));
        }));
    }
    assert!(received.recv_timeout(Duration::from_millis(50)).is_err());
    release.send(())?;
    for _ in 0..4 {
        received.recv_timeout(WAIT)??;
    }
    for waiter in waiters {
        waiter.join().map_err(|_| "shutdown waiter panicked")?;
    }
    futures::executor::block_on(net.shutdown())?;
    Ok(())
}

#[test]
fn cleanup_survives_the_callers_current_thread_runtime_drop() -> TestResult {
    let net = engine()?;
    let release = hold_callback(&net)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut abandoned = Box::pin(net.shutdown());
    runtime.block_on(std::future::poll_fn(|cx| {
        assert!(abandoned.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    }));
    drop(abandoned);
    drop(runtime);
    release.send(())?;
    let (sent, received) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let _ = sent.send(futures::executor::block_on(net.shutdown()));
    });
    received.recv_timeout(WAIT)??;
    waiter
        .join()
        .map_err(|_| "runtime-independent waiter panicked")?;
    Ok(())
}

#[test]
fn pending_shutdown_rechecks_context_when_polled_inside_common_callback() -> TestResult {
    type ShutdownFuture = Pin<Box<dyn Future<Output = Result<(), NetError>> + Send>>;
    let net = engine()?;
    let (submitted, callback_future) = mpsc::channel::<ShutdownFuture>();
    let (entered, entry) = mpsc::channel();
    let (sent, received) = mpsc::channel();
    net.inner
        .common_engine
        .cb_pool
        .execute(move || {
            let _ = entered.send(());
            if let Ok(future) = callback_future.recv_timeout(WAIT) {
                let _ = sent.send(futures::executor::block_on(future));
            }
        })
        .map_err(NetError::from)?;
    entry.recv_timeout(WAIT)?;
    let waiting_net = Arc::clone(&net);
    let mut future: ShutdownFuture = Box::pin(async move { waiting_net.shutdown().await });
    assert!(poll_once(future.as_mut()).is_pending());
    submitted
        .send(future)
        .map_err(|_| "callback stopped before polling shutdown")?;
    let result = received.recv_timeout(WAIT)?;
    assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    futures::executor::block_on(net.shutdown())?;
    Ok(())
}

#[test]
fn one_blocked_engine_does_not_hold_up_another_engines_receipt() -> TestResult {
    let blocked = engine()?;
    let release = hold_callback(&blocked)?;
    blocked.begin_shutdown();
    let independent = engine()?;
    let (sent, received) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let _ = sent.send(futures::executor::block_on(independent.shutdown()));
    });
    received.recv_timeout(WAIT)??;
    waiter
        .join()
        .map_err(|_| "independent engine waiter panicked")?;
    let mut waiting = Box::pin(blocked.shutdown());
    assert!(poll_once(waiting.as_mut()).is_pending());
    release.send(())?;
    futures::executor::block_on(waiting)?;
    Ok(())
}

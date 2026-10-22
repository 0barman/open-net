#![cfg(test)]

//! The callback pool must remain open until deferred terminal delivery finishes.
use super::super::network_status_snapshot::NetworkStatusSource;
use super::*;
use crate::subscription::common_executor::CommonCallbackExecutor;
use std::future::Future;
use std::sync::mpsc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(5);

struct GateWake {
    entered: mpsc::Sender<()>,
    released: Mutex<mpsc::Receiver<()>>,
}
impl Wake for GateWake {
    fn wake(self: Arc<Self>) {
        let _ = self.entered.send(());
        if let Ok(released) = self.released.lock() {
            // Sender Drop is the failure-path release, so an assertion cannot
            // strand the SDK runtime's blocking worker during test teardown.
            let _ = released.recv_timeout(WAIT);
        }
    }
}

#[test]
fn terminal_dispatch_finishes_before_callback_pool_closes() -> TestResult {
    let engine = Arc::new(CommonEngine::new_with_runtime_worker_threads(
        8,
        8,
        Some(1),
    )?);
    let initial = NetworkView::new().snapshot;
    let (publisher, source) = StateSource::new(
        initial.clone(),
        Arc::new(CommonCallbackExecutor::new(&engine)),
        4,
    )?;
    let (delivered, received) = mpsc::channel();
    let subscription = source.subscribe()?.into_callback(move |_, value| {
        let _ = delivered.send(value.map(|snapshot| snapshot.state));
    })?;
    if !matches!(received.recv_timeout(WAIT)??, MonitorState::Stopped) {
        return Err("initial callback did not complete".into());
    }

    let (invalidation, mut invalidated) = watch::channel(0_u64);
    let (entered, entered_receiver) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let wake = Waker::from(Arc::new(GateWake {
        entered,
        released: Mutex::new(released),
    }));
    let mut changed = Box::pin(invalidated.changed());
    if !matches!(
        changed.as_mut().poll(&mut Context::from_waker(&wake)),
        Poll::Pending
    ) {
        return Err("invalidation gate was not installed".into());
    }
    let mut terminal = initial;
    terminal.state = MonitorState::Closed;
    let mut publication = ViewPublication::empty();
    publication.state = Some(publisher.prepare_finish_arc(Arc::new(terminal)));
    publication.invalidation = Some((invalidation, 1));
    publication.track(&Some(engine.clone()));
    publication.dispatch_from_monitor();
    entered_receiver.recv_timeout(WAIT)?;

    let retiring = engine.clone();
    let shutdown = std::thread::spawn(move || retiring.shutdown_blocking());
    if !engine.retirement_work().wait_started_for_test(WAIT) {
        drop(release);
        let _ = shutdown.join();
        return Err("engine did not reach its deferred-work barrier".into());
    }
    // This assertion fails if callback shutdown is moved before the work
    // barrier, even though runtime shutdown would eventually await the gate.
    let pool_open = engine.cb_pool.ensure_ready();
    drop(release);
    shutdown
        .join()
        .map_err(|_| "engine retirement panicked")??;
    pool_open?;
    if !matches!(received.recv_timeout(WAIT)??, MonitorState::Closed) {
        return Err("normal shutdown lost its Closed callback".into());
    }
    futures::executor::block_on(subscription.close())?;
    Ok(())
}

fn prepared_publication_holds_shutdown(
    engine: Arc<CommonEngine>,
    dispatch: impl FnOnce() -> Result<(), NetError>,
) -> TestResult {
    let (finished, completion) = mpsc::channel();
    let retiring = engine.clone();
    let shutdown = std::thread::spawn(move || {
        let result = retiring.shutdown_blocking();
        let _ = finished.send(());
        result
    });
    if !engine.retirement_work().wait_started_for_test(WAIT) {
        dispatch()?;
        let _ = shutdown.join();
        return Err("engine did not reach its deferred-work barrier".into());
    }
    // The preparing caller is deliberately paused before spawn/dispatch. The
    // coordinator must stay pending even though there are no live providers,
    // bridges, callbacks, or SDK tasks to hold it on this caller's behalf.
    let still_pending = matches!(
        completion.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    let pool_open = engine.cb_pool.ensure_ready();
    let dispatched = dispatch();
    shutdown
        .join()
        .map_err(|_| "engine retirement panicked")??;
    if !still_pending {
        return Err("shutdown overtook a prepared publication before its dispatch".into());
    }
    pool_open?;
    dispatched?;
    Ok(())
}

#[test]
fn prepared_source_publication_holds_shutdown_before_dispatch() -> TestResult {
    let engine = Arc::new(CommonEngine::new_with_runtime_worker_threads(
        8,
        8,
        Some(1),
    )?);
    let source = NetworkStatusSource::new(Arc::new(CommonCallbackExecutor::new(&engine)), 4)?
        .with_retirement(engine.retirement_work());
    let publication = source.prepare_closed()?;
    prepared_publication_holds_shutdown(engine, move || publication.dispatch())
}

#[test]
fn prepared_view_publication_holds_shutdown_before_dispatch() -> TestResult {
    let engine = Arc::new(CommonEngine::new_with_runtime_worker_threads(
        8,
        8,
        Some(1),
    )?);
    let executor = Arc::new(CommonCallbackExecutor::new(&engine));
    let source = NetworkStatusSource::new(executor.clone(), 4)?;
    let view = source.new_view(executor, Some(engine.clone()))?;
    let publication = view.retire(true, None)?;
    prepared_publication_holds_shutdown(engine, move || {
        publication.dispatch();
        Ok(())
    })
}

#[test]
fn recoverable_driver_delivers_terminal_after_inflight_callback_before_pool_close() -> TestResult {
    let engine = Arc::new(CommonEngine::new_with_runtime_worker_threads(
        8,
        8,
        Some(1),
    )?);
    let executor = Arc::new(CommonCallbackExecutor::new(&engine));
    let source = NetworkStatusSource::new(executor.clone(), 4)?;
    // No source/view leases: only the recoverable driver can protect its
    // successor callback while the first callback holds this gate.
    let view = source.new_view(executor.clone(), None)?;
    let options = crate::net_status::NetworkEventOptions::default();
    let receiver = view.subscribe_events(options)?;
    let replacement = view.clone();
    let (entered, initial) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(Some(released));
    let (delivered, terminal) = mpsc::channel();
    let subscription = super::super::recoverable::subscribe(
        receiver,
        Arc::new(move || replacement.subscribe_events(options)),
        executor,
        engine.runtime_handle(),
        Some(engine.retirement_work().register()),
        move |_, observation| {
            if let Some(released) = released.lock().unwrap().take() {
                let _ = entered.send(());
                let _ = released.recv_timeout(WAIT);
            }
            let state = observation.map(|observation| match observation {
                crate::net_status::NetworkObservation::Event(event) => event.snapshot.state,
                crate::net_status::NetworkObservation::Reconciled { current, .. } => {
                    current.snapshot.state
                }
            });
            let _ = delivered.send(state);
        },
    )?;
    initial.recv_timeout(WAIT)?;
    source.prepare_closed()?.dispatch()?;
    let result = prepared_publication_holds_shutdown(engine, move || {
        drop(release);
        Ok(())
    });
    result?;
    if !matches!(terminal.recv_timeout(WAIT)??, MonitorState::Stopped)
        || !matches!(terminal.recv_timeout(WAIT)??, MonitorState::Closed)
    {
        return Err("recoverable driver lost the terminal lifecycle callback".into());
    }
    futures::executor::block_on(subscription.close())?;
    Ok(())
}

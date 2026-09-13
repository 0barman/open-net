use super::{data_callback_loop, data_callback_loop_with_dispatch};
use crate::api::wsc::wsc_response::{PendingRequestView, WSCResponse};
use crate::common::log::log_def::LogType;
use crate::module::ws_client::callback_event::CallbackEvent;
use crate::module::ws_client::callback_executor::try_start_user_callback_with;
use crate::module::ws_client::io_event::IoEvent;
use crate::module::ws_client::listener_store::DataListener;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
type CallbackDone = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Default)]
struct CallbackGate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl CallbackGate {
    fn wait(&self) -> TestResult {
        let mut released = self
            .released
            .lock()
            .map_err(|error| test_error(format!("callback gate lock: {error}")))?;
        while !*released {
            released = self
                .changed
                .wait(released)
                .map_err(|error| test_error(format!("callback gate wait: {error}")))?;
        }
        Ok(())
    }

    fn release(&self) {
        let mut released = match self.released.lock() {
            Ok(released) => released,
            Err(error) => {
                crate::log_e!(LogType::WSC; "callback_budget_test_cleanup", "error", error.to_string());
                error.into_inner()
            }
        };
        *released = true;
        self.changed.notify_all();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Observation {
    Started(u64, u8),
    Finished(u64, u8),
}

#[derive(Clone, Default)]
struct CallbackState {
    active: usize,
    peak: usize,
    observations: Vec<Observation>,
}

/// Own every real callback thread and release its gate before joining on any exit path.
struct CallbackThreads {
    gate: Arc<CallbackGate>,
    handles: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
    dispatched: Arc<AtomicUsize>,
    state: Arc<Mutex<CallbackState>>,
}

impl CallbackThreads {
    fn new() -> Self {
        Self {
            gate: Arc::new(CallbackGate::default()),
            handles: Arc::new(Mutex::new(Vec::new())),
            dispatched: Arc::new(AtomicUsize::new(0)),
            state: Arc::new(Mutex::new(CallbackState::default())),
        }
    }

    fn dispatch(&self) -> impl FnMut(Box<dyn FnOnce() + Send>) -> std::io::Result<CallbackDone> {
        let handles = Arc::clone(&self.handles);
        let dispatched = Arc::clone(&self.dispatched);
        move |callback| {
            let handles = Arc::clone(&handles);
            let completed =
                try_start_user_callback_with("callback-budget-test", callback, move |task| {
                    let mut handles = handles.lock().map_err(|error| {
                        std::io::Error::other(format!("callback thread list lock: {error}"))
                    })?;
                    let handle = std::thread::Builder::new()
                        .name("callback-budget-test".into())
                        .spawn(task)?;
                    handles.push(handle);
                    Ok(())
                })?;
            dispatched.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(completed) as CallbackDone)
        }
    }

    fn listener(
        &self,
        entered: mpsc::Sender<(u64, u8)>,
        results: std::sync::mpsc::SyncSender<TestResult>,
    ) -> DataListener {
        let gate = Arc::clone(&self.gate);
        let state = Arc::clone(&self.state);
        Arc::new(move |response| {
            let result = (|| -> TestResult {
                let generation = response.connection_generation();
                let Message::Binary(payload) = response.message() else {
                    return Err(test_error("expected binary callback payload"));
                };
                let id = payload
                    .first()
                    .copied()
                    .ok_or_else(|| test_error("callback payload is empty"))?;
                {
                    let mut state = state
                        .lock()
                        .map_err(|error| test_error(format!("callback state lock: {error}")))?;
                    state.active = state
                        .active
                        .checked_add(1)
                        .ok_or_else(|| test_error("callback active count overflow"))?;
                    state.peak = state.peak.max(state.active);
                    state
                        .observations
                        .push(Observation::Started(generation, id));
                }
                entered.try_send((generation, id)).map_err(|error| {
                    test_error(format!("callback entered observation: {error}"))
                })?;
                gate.wait()?;
                {
                    let mut state = state
                        .lock()
                        .map_err(|error| test_error(format!("callback state lock: {error}")))?;
                    state.active = state
                        .active
                        .checked_sub(1)
                        .ok_or_else(|| test_error("callback active count underflow"))?;
                    state
                        .observations
                        .push(Observation::Finished(generation, id));
                }
                Ok(())
            })();
            if let Err(error) = results.try_send(result) {
                crate::log_e!(LogType::WSC; "callback_budget_test", "error", error.to_string());
            }
        })
    }

    fn snapshot(&self) -> TestResult<CallbackState> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|error| test_error(format!("callback state snapshot: {error}")))
    }
}

impl Drop for CallbackThreads {
    fn drop(&mut self) {
        self.gate.release();
        let handles = match self.handles.lock() {
            Ok(mut handles) => std::mem::take(&mut *handles),
            Err(error) => {
                crate::log_e!(LogType::WSC; "callback_budget_test_cleanup", "error", error.to_string());
                std::mem::take(&mut *error.into_inner())
            }
        };
        for handle in handles {
            if handle.join().is_err() {
                crate::log_e!(LogType::WSC; "callback_budget_test_cleanup", "error", "callback_thread_join_failed");
            }
        }
    }
}

fn data_event(
    bytes: &Arc<Semaphore>,
    listener: Option<DataListener>,
    generation: u64,
    payload: Vec<u8>,
) -> TestResult<CallbackEvent> {
    let byte_count = u32::try_from(payload.len().max(1))?;
    Ok(CallbackEvent::Data {
        listener,
        response: WSCResponse::new(
            Message::Binary(payload.into()),
            PendingRequestView::default(),
            generation,
        ),
        byte_permit: Arc::clone(bytes).try_acquire_many_owned(byte_count)?,
    })
}

#[tokio::test]
async fn running_callback_holds_payload_budget_until_it_returns() -> TestResult {
    let callbacks = CallbackThreads::new();
    let bytes = Arc::new(Semaphore::new(4));
    let (entered_tx, mut entered_rx) = mpsc::channel(1);
    let (results_tx, results_rx) = std::sync::mpsc::sync_channel(1);
    let listener = callbacks.listener(entered_tx, results_tx);
    let (callback_tx, callback_rx) = mpsc::channel(1);
    callback_tx.try_send(data_event(&bytes, Some(listener), 7, vec![1; 4])?)?;
    drop(callback_tx);
    let (io_tx, _io_rx) = mpsc::channel(1);
    let lane = data_callback_loop_with_dispatch(
        callback_rx,
        1,
        io_tx,
        CancellationToken::new(),
        callbacks.dispatch(),
    );
    tokio::pin!(lane);
    check!(futures::poll!(&mut lane).is_pending())?;
    check_eq!(
        timeout(TEST_TIMEOUT, entered_rx.recv()).await?,
        Some((7, 1))
    )?;
    check_eq!(bytes.available_permits(), 0)?;
    check!(Arc::clone(&bytes).try_acquire_owned().is_err())?;
    check_eq!(callbacks.snapshot()?.active, 1)?;

    callbacks.gate.release();
    timeout(TEST_TIMEOUT, &mut lane).await?;
    results_rx.recv_timeout(TEST_TIMEOUT)??;
    check_eq!(callbacks.snapshot()?.active, 0)?;
    check_eq!(bytes.available_permits(), 4)?;
    Ok(())
}

#[tokio::test]
async fn callback_dispatch_never_exceeds_configured_concurrency() -> TestResult {
    let callbacks = CallbackThreads::new();
    let bytes = Arc::new(Semaphore::new(5));
    let (entered_tx, mut entered_rx) = mpsc::channel(5);
    let (results_tx, results_rx) = std::sync::mpsc::sync_channel(5);
    let listener = callbacks.listener(entered_tx, results_tx);
    let (callback_tx, callback_rx) = mpsc::channel(5);
    for id in 1..=5 {
        callback_tx.try_send(data_event(
            &bytes,
            Some(Arc::clone(&listener)),
            7,
            vec![id],
        )?)?;
    }
    let (io_tx, _io_rx) = mpsc::channel(1);
    let lane = data_callback_loop_with_dispatch(
        callback_rx,
        2,
        io_tx,
        CancellationToken::new(),
        callbacks.dispatch(),
    );
    tokio::pin!(lane);
    check!(futures::poll!(&mut lane).is_pending())?;
    check_eq!(callbacks.dispatched.load(Ordering::SeqCst), 2)?;
    check_eq!(callback_tx.capacity(), 2)?;
    for _ in 0..2 {
        timeout(TEST_TIMEOUT, entered_rx.recv())
            .await?
            .ok_or_else(|| test_error("missing callback start"))?;
    }
    check_eq!(callbacks.snapshot()?.active, 2)?;
    check_eq!(bytes.available_permits(), 0)?;

    callbacks.gate.release();
    drop(callback_tx);
    timeout(TEST_TIMEOUT, &mut lane).await?;
    for _ in 0..5 {
        results_rx.recv_timeout(TEST_TIMEOUT)??;
    }
    let state = callbacks.snapshot()?;
    check_eq!(state.active, 0)?;
    check_eq!(state.peak, 2)?;
    check_eq!(callbacks.dispatched.load(Ordering::SeqCst), 5)?;
    let mut finished: Vec<_> = state
        .observations
        .iter()
        .filter_map(|observation| match observation {
            Observation::Finished(_, id) => Some(*id),
            Observation::Started(_, _) => None,
        })
        .collect();
    finished.sort_unstable();
    check_eq!(finished, vec![1, 2, 3, 4, 5])?;
    check_eq!(bytes.available_permits(), 5)?;
    Ok(())
}

#[tokio::test]
async fn single_callback_lane_preserves_start_and_completion_order() -> TestResult {
    let callbacks = CallbackThreads::new();
    let bytes = Arc::new(Semaphore::new(3));
    let (entered_tx, mut entered_rx) = mpsc::channel(3);
    let (results_tx, results_rx) = std::sync::mpsc::sync_channel(3);
    let listener = callbacks.listener(entered_tx, results_tx);
    let (callback_tx, callback_rx) = mpsc::channel(3);
    for id in 1..=3 {
        callback_tx.try_send(data_event(
            &bytes,
            Some(Arc::clone(&listener)),
            7,
            vec![id],
        )?)?;
    }
    let (io_tx, _io_rx) = mpsc::channel(1);
    let lane = data_callback_loop_with_dispatch(
        callback_rx,
        1,
        io_tx,
        CancellationToken::new(),
        callbacks.dispatch(),
    );
    tokio::pin!(lane);
    check!(futures::poll!(&mut lane).is_pending())?;
    check_eq!(callbacks.dispatched.load(Ordering::SeqCst), 1)?;
    check_eq!(callback_tx.capacity(), 1)?;
    check_eq!(
        timeout(TEST_TIMEOUT, entered_rx.recv()).await?,
        Some((7, 1))
    )?;
    check_eq!(
        callbacks.snapshot()?.observations,
        vec![Observation::Started(7, 1)]
    )?;

    callbacks.gate.release();
    drop(callback_tx);
    timeout(TEST_TIMEOUT, &mut lane).await?;
    for _ in 0..3 {
        results_rx.recv_timeout(TEST_TIMEOUT)??;
    }
    check_eq!(
        callbacks.snapshot()?.observations,
        vec![
            Observation::Started(7, 1),
            Observation::Finished(7, 1),
            Observation::Started(7, 2),
            Observation::Finished(7, 2),
            Observation::Started(7, 3),
            Observation::Finished(7, 3),
        ]
    )?;
    check_eq!(bytes.available_permits(), 3)?;
    Ok(())
}

#[tokio::test]
async fn callback_without_a_listener_releases_payload_budget() -> TestResult {
    let bytes = Arc::new(Semaphore::new(3));
    let (callback_tx, callback_rx) = mpsc::channel(1);
    callback_tx.try_send(data_event(&bytes, None, 7, vec![1, 2, 3])?)?;
    check_eq!(bytes.available_permits(), 0)?;
    drop(callback_tx);
    let (io_tx, _io_rx) = mpsc::channel(1);

    timeout(
        TEST_TIMEOUT,
        data_callback_loop(callback_rx, 1, io_tx, CancellationToken::new()),
    )
    .await?;
    check_eq!(bytes.available_permits(), 3)?;
    Ok(())
}

#[tokio::test]
async fn one_callback_lane_keeps_old_and_new_generations_in_the_same_byte_budget() -> TestResult {
    let callbacks = CallbackThreads::new();
    let bytes = Arc::new(Semaphore::new(4));
    let (entered_tx, mut entered_rx) = mpsc::channel(2);
    let (results_tx, results_rx) = std::sync::mpsc::sync_channel(2);
    let listener = callbacks.listener(entered_tx, results_tx);
    let (callback_tx, callback_rx) = mpsc::channel(2);
    callback_tx.try_send(data_event(
        &bytes,
        Some(Arc::clone(&listener)),
        7,
        vec![1; 2],
    )?)?;
    callback_tx.try_send(data_event(&bytes, Some(listener), 8, vec![2; 2])?)?;
    drop(callback_tx);
    let (io_tx, _io_rx) = mpsc::channel(1);
    let lane = data_callback_loop_with_dispatch(
        callback_rx,
        2,
        io_tx,
        CancellationToken::new(),
        callbacks.dispatch(),
    );
    tokio::pin!(lane);
    check!(futures::poll!(&mut lane).is_pending())?;
    let mut started = Vec::new();
    for _ in 0..2 {
        started.push(
            timeout(TEST_TIMEOUT, entered_rx.recv())
                .await?
                .ok_or_else(|| test_error("missing generation callback start"))?,
        );
    }
    started.sort_unstable();
    check_eq!(started, vec![(7, 1), (8, 2)])?;
    check_eq!(bytes.available_permits(), 0)?;
    check!(Arc::clone(&bytes).try_acquire_owned().is_err())?;

    callbacks.gate.release();
    timeout(TEST_TIMEOUT, &mut lane).await?;
    for _ in 0..2 {
        results_rx.recv_timeout(TEST_TIMEOUT)??;
    }
    check_eq!(callbacks.snapshot()?.active, 0)?;
    check_eq!(bytes.available_permits(), 4)?;
    Ok(())
}

#[tokio::test]
async fn rejected_dispatch_preserves_another_running_callbacks_byte_permit() -> TestResult {
    let callbacks = CallbackThreads::new();
    let bytes = Arc::new(Semaphore::new(5));
    let (entered_tx, mut entered_rx) = mpsc::channel(2);
    let (results_tx, results_rx) = std::sync::mpsc::sync_channel(2);
    let listener = callbacks.listener(entered_tx, results_tx);
    let (callback_tx, callback_rx) = mpsc::channel(2);
    callback_tx.try_send(data_event(
        &bytes,
        Some(Arc::clone(&listener)),
        7,
        vec![1; 3],
    )?)?;
    callback_tx.try_send(data_event(&bytes, Some(listener), 7, vec![2; 2])?)?;
    drop(callback_tx);
    let (io_tx, mut io_rx) = mpsc::channel(1);
    let mut dispatch = callbacks.dispatch();
    let mut first = true;
    let lane = data_callback_loop_with_dispatch(
        callback_rx,
        2,
        io_tx,
        CancellationToken::new(),
        move |callback| {
            if first {
                first = false;
                dispatch(callback)
            } else {
                drop(callback);
                Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
            }
        },
    );
    tokio::pin!(lane);
    check!(futures::poll!(&mut lane).is_pending())?;
    check_eq!(
        timeout(TEST_TIMEOUT, entered_rx.recv()).await?,
        Some((7, 1))
    )?;
    check!(matches!(
        io_rx.try_recv()?,
        IoEvent::CallbackDispatchFailed { generation: 7 }
    ))?;
    check_eq!(callbacks.dispatched.load(Ordering::SeqCst), 1)?;
    check_eq!(callbacks.snapshot()?.active, 1)?;
    check_eq!(bytes.available_permits(), 2)?;
    check!(Arc::clone(&bytes).try_acquire_many_owned(3).is_err())?;

    callbacks.gate.release();
    timeout(TEST_TIMEOUT, &mut lane).await?;
    results_rx.recv_timeout(TEST_TIMEOUT)??;
    check_eq!(
        callbacks.snapshot()?.observations,
        vec![Observation::Started(7, 1), Observation::Finished(7, 1)]
    )?;
    check_eq!(callbacks.snapshot()?.active, 0)?;
    check_eq!(bytes.available_permits(), 5)?;
    Ok(())
}

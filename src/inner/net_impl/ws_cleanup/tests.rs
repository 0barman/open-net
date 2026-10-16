use super::*;
use std::sync::mpsc;
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(5);

#[test]
fn engine_receipt_waits_for_ws_fallback_completion_waker_to_return() -> TestResult {
    use std::future::Future;
    use std::task::{Context, Wake, Waker};
    struct HeldWake {
        entered: mpsc::Sender<bool>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl Wake for HeldWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self
                .entered
                .send(crate::common::retirement::ExecutionScope::is_current());
            let _ = self
                .release
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .recv_timeout(WAIT);
        }
    }
    let net = Arc::new(crate::OpenNet::new()?);
    let clients = Arc::new(Mutex::new(HashMap::from([(
        "tail".to_owned(),
        ClientSlot::Closing,
    )])));
    let (task, mut completed) = CleanupTask::new(
        net.inner.common_engine.clone(),
        clients.clone(),
        Arc::new(Notify::new()),
        "tail".to_owned(),
        None,
    );
    let (entered, observed) = mpsc::channel();
    let (release_wake, wake_gate) = mpsc::channel();
    let waker = Waker::from(Arc::new(HeldWake {
        entered,
        release: Mutex::new(wake_gate),
    }));
    assert!(std::pin::Pin::new(&mut completed)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    // An unsubmitted destroy task follows exactly the cancelled-waiter fallback.
    drop(task);
    assert!(observed.recv_timeout(WAIT)?);
    assert!(clients.lock().unwrap_or_else(|e| e.into_inner()).is_empty());
    let closing_net = net.clone();
    let (closed, completion) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let _ = closed.send(futures::executor::block_on(closing_net.shutdown()));
    });
    assert!(matches!(
        completion.recv_timeout(Duration::from_millis(30)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release_wake.send(())?;
    completion.recv_timeout(WAIT)??;
    waiter
        .join()
        .map_err(|_| "engine shutdown waiter panicked")?;
    futures::executor::block_on(completed)??;
    Ok(())
}

struct Fixture {
    task: CleanupTask,
    completed: oneshot::Receiver<Outcome>,
    clients: Arc<Mutex<HashMap<String, ClientSlot>>>,
    engine: Arc<CommonEngine>,
}

fn fixture(worker: impl FnOnce() + Send + 'static) -> TestResult<Fixture> {
    let engine = Arc::new(CommonEngine::new_with_runtime_worker_threads(
        4,
        4,
        Some(1),
    )?);
    let clients = Arc::new(Mutex::new(HashMap::from([(
        "cleanup".to_owned(),
        ClientSlot::Closing,
    )])));
    let (task, completed) = CleanupTask::new(
        engine.clone(),
        clients.clone(),
        Arc::new(Notify::new()),
        "cleanup".to_owned(),
        Some(
            engine
                .resource_threads()
                .spawn_tracked("cleanup-test", worker)?,
        ),
    );
    Ok(Fixture {
        task,
        completed,
        clients,
        engine,
    })
}

fn completion(receiver: oneshot::Receiver<Outcome>) -> TestResult<Outcome> {
    // Keep a lost completion from hanging the entire test binary.
    let (tx, rx) = mpsc::channel();
    let waiter = std::thread::Builder::new().spawn(move || {
        let _ = tx.send(futures::executor::block_on(receiver));
    })?;
    let result = rx.recv_timeout(WAIT)??;
    waiter.join().map_err(|_| "completion waiter panicked")?;
    Ok(result)
}

#[test]
fn join_uses_owned_runtime_from_plain_thread() -> TestResult {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let fixture = fixture(|| {})?;
    fixture.task.submit(&fixture.engine.runtime_handle())?;
    completion(fixture.completed)??;
    assert!(fixture.clients.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn rejection_before_queue_recovers_the_join_and_releases_name() -> TestResult {
    let fixture = fixture(|| {})?;
    let error = fixture
        .task
        .submit_with(|_task| panic!("injected rejection before queue"))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::RuntimeUnavailable);
    completion(fixture.completed)??;
    assert!(fixture.clients.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn rejection_after_queue_cannot_join_twice_or_remove_replacement() -> TestResult {
    let fixture = fixture(|| {})?;
    let mut queued = None;
    let error = fixture
        .task
        .submit_with(|task| {
            queued = Some(task);
            panic!("injected rejection after queue");
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::RuntimeUnavailable);
    completion(fixture.completed)??;
    assert!(fixture.clients.lock().unwrap().is_empty());
    // Simulate another generation acquiring the name before a stale queued
    // task finally executes. Its already-claimed payload must do nothing.
    fixture
        .clients
        .lock()
        .unwrap()
        .insert("cleanup".to_owned(), ClientSlot::Closing);
    queued.take().ok_or("job was not queued")?.run();
    assert!(fixture.clients.lock().unwrap().contains_key("cleanup"));
    Ok(())
}

#[test]
fn discarded_queued_task_keeps_name_until_worker_exit() -> TestResult {
    let (release, gate) = mpsc::channel();
    let fixture = fixture(move || {
        let _ = gate.recv_timeout(WAIT);
    })?;
    let mut queued = None;
    fixture.task.submit_with(|task| queued = Some(task))?;
    drop(queued);
    assert!(fixture.clients.lock().unwrap().contains_key("cleanup"));
    release.send(())?;
    completion(fixture.completed)??;
    assert!(fixture.clients.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn cancelled_waiter_does_not_own_the_join_or_runtime() -> TestResult {
    let (release, gate) = mpsc::channel();
    let fixture = fixture(move || {
        let _ = gate.recv_timeout(WAIT);
    })?;
    let runtime_owner = Arc::downgrade(&fixture.engine);
    let mut queued = None;
    fixture.task.submit_with(|task| queued = Some(task))?;
    drop(fixture.completed);
    drop(fixture.engine);
    assert!(runtime_owner.upgrade().is_some());
    assert!(fixture.clients.lock().unwrap().contains_key("cleanup"));
    release.send(())?;
    queued.take().ok_or("job was not queued")?.run();
    assert!(fixture.clients.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn panicking_worker_and_payload_still_publish_join_failure() -> TestResult {
    struct PanickingPayload;
    impl Drop for PanickingPayload {
        fn drop(&mut self) {
            panic!("injected panic payload destructor");
        }
    }
    let fixture = fixture(|| std::panic::panic_any(PanickingPayload))?;
    fixture.task.submit(&fixture.engine.runtime_handle())?;
    let error = completion(fixture.completed)?.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Internal);
    assert!(fixture.clients.lock().unwrap().is_empty());
    Ok(())
}

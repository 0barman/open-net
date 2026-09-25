//! Bounded OS-thread executor shared by a client's listener registrations.

use crate::common::log::log_def::LogType;
use crate::error::NetError;
use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

pub(crate) type CallbackJob = Box<dyn FnOnce() + Send + 'static>;

/// Bounds the eventual fixed worker set; excessive settings are rejected during
/// client construction, before any registration can start OS threads.
pub(crate) const MAX_LISTENER_WORKERS: usize = 256;

/// The queue counts waiting jobs; at most `workers` more jobs can be running.
/// The first registration starts the fixed worker set. Submission never creates
/// workers, and a blocked callback holds its worker without a replacement.
pub(crate) struct ListenerExecutor {
    shared: Arc<ExecutorShared>,
    startup: Mutex<bool>,
    name: String,
    workers: usize,
    #[cfg(test)]
    started_workers: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    fail_start_after: std::sync::atomic::AtomicUsize,
}

struct ExecutorShared {
    state: Mutex<ExecutorState>,
    changed: Condvar,
    queue_capacity: usize,
}

struct ExecutorState {
    accepting: bool,
    ready: bool,
    pending: VecDeque<CallbackJob>,
}

impl ListenerExecutor {
    pub(crate) fn new(
        name: &str,
        workers: usize,
        queue_capacity: usize,
    ) -> Result<Arc<Self>, NetError> {
        if workers == 0 || workers > MAX_LISTENER_WORKERS || queue_capacity == 0 {
            return Err(NetError::from(crate::error::ErrorKind::InvalidConfig));
        }
        let mut pending = VecDeque::new();
        pending.try_reserve(queue_capacity).map_err(|error| {
            crate::log_e!(LogType::WSC; "listener_executor", "queue_allocation_error", error.to_string());
            NetError::with_source(crate::error::ErrorKind::ResourceExhausted, error)
        })?;
        let shared = Arc::new(ExecutorShared {
            state: Mutex::new(ExecutorState {
                accepting: true,
                ready: false,
                pending,
            }),
            changed: Condvar::new(),
            queue_capacity,
        });
        let executor = Arc::new(Self {
            shared,
            startup: Mutex::new(false),
            name: name.to_owned(),
            workers,
            #[cfg(test)]
            started_workers: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            fail_start_after: std::sync::atomic::AtomicUsize::new(usize::MAX),
        });
        Ok(executor)
    }

    /// Starts the pool before publishing the first subscription. Failed startup
    /// is retryable only after its workers have exited; they never run user jobs.
    pub(crate) fn ensure_started(&self) -> Result<(), NetError> {
        #[cfg(test)]
        let mut attempted = 0;
        self.ensure_started_with_spawner(|builder, task| {
            #[cfg(test)]
            {
                if self
                    .fail_start_after
                    .compare_exchange(attempted, usize::MAX, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return Err(std::io::Error::other("injected listener startup failure"));
                }
                attempted += 1;
            }
            builder.spawn(task)
        })
    }

    fn ensure_started_with_spawner<F>(&self, mut spawn: F) -> Result<(), NetError>
    where
        F: FnMut(std::thread::Builder, CallbackJob) -> std::io::Result<std::thread::JoinHandle<()>>,
    {
        let mut started = self.startup.lock().map_err(NetError::from_poison)?;
        {
            let state = self.shared.state.lock().map_err(NetError::from_poison)?;
            if !state.accepting {
                return Err(NetError::from(crate::error::ErrorKind::QueueClosed));
            }
            if *started {
                return Ok(());
            }
        }
        let mut handles = Vec::new();
        handles.try_reserve(self.workers).map_err(|error| {
            NetError::with_source(crate::error::ErrorKind::ResourceExhausted, error)
        })?;
        let cancelled = Arc::new(AtomicBool::new(false));
        for index in 0..self.workers {
            let shared = Arc::clone(&self.shared);
            let worker_cancelled = Arc::clone(&cancelled);
            let job: CallbackJob = Box::new(move || shared.run_worker(&worker_cancelled));
            let builder = std::thread::Builder::new().name(format!("{}-{index}", self.name));
            match spawn(builder, job) {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    self.rollback_startup(&cancelled, handles);
                    crate::log_e!(LogType::WSC; "listener_executor", "thread_start_error", error.to_string());
                    return Err(NetError::with_source(
                        crate::error::ErrorKind::RuntimeUnavailable,
                        error,
                    )
                    .with_stage(crate::error::ErrorStage::Runtime));
                }
            }
            #[cfg(test)]
            self.started_workers
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        let mut state = match self.shared.state.lock() {
            Ok(state) => state,
            Err(error) => {
                drop(error.into_inner());
                self.rollback_startup(&cancelled, handles);
                return Err(NetError::from(crate::error::ErrorKind::Internal));
            }
        };
        if !state.accepting {
            drop(state);
            self.rollback_startup(&cancelled, handles);
            return Err(NetError::from(crate::error::ErrorKind::QueueClosed));
        }
        state.ready = true;
        *started = true;
        drop(state);
        self.shared.changed.notify_all();
        // Running callbacks are never joined, including on ordinary close/drop.
        drop(handles);
        Ok(())
    }

    fn rollback_startup(&self, cancelled: &AtomicBool, handles: Vec<std::thread::JoinHandle<()>>) {
        {
            // Coordinate with the wait predicate so cancellation cannot miss a
            // worker about to sleep. No user job is admitted before ready=true.
            let state = match self.shared.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            cancelled.store(true, Ordering::Release);
            drop(state);
        }
        self.shared.changed.notify_all();
        for handle in handles {
            if handle.join().is_err() {
                crate::log_e!(LogType::WSC; "listener_executor", "startup_rollback_error", "worker_unwound");
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn started_worker_count(&self) -> usize {
        self.started_workers.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_start_after(&self, started_workers: usize) {
        self.fail_start_after
            .store(started_workers, Ordering::SeqCst);
    }

    /// Accepts work without blocking. Rejected job captures are released after
    /// the executor lock is released, so their destructors may reenter the API.
    pub(crate) fn try_submit(&self, job: CallbackJob) -> Result<(), NetError> {
        let mut state = self.shared.state.lock().map_err(NetError::from_poison)?;
        if !state.accepting {
            return Err(NetError::from(crate::error::ErrorKind::QueueClosed));
        }
        if !state.ready {
            return Err(NetError::from(crate::error::ErrorKind::RuntimeUnavailable));
        }
        if state.pending.len() >= self.shared.queue_capacity {
            return Err(NetError::from(crate::error::ErrorKind::QueueFull));
        }
        state.pending.push_back(job);
        drop(state);
        self.shared.changed.notify_one();
        Ok(())
    }

    /// Stops accepting work and lets workers drain already accepted jobs.
    /// It never waits for arbitrary application callbacks to return.
    pub(crate) fn close(&self) -> Result<(), NetError> {
        let mut state = self.shared.state.lock().map_err(NetError::from_poison)?;
        state.accepting = false;
        drop(state);
        self.shared.changed.notify_all();
        Ok(())
    }
}

impl Drop for ListenerExecutor {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            crate::log_e!(LogType::WSC; "listener_executor", "close_error", error.to_string());
        }
    }
}

impl crate::subscription::CallbackExecutor for ListenerExecutor {
    fn ensure_ready(&self) -> crate::Result<()> {
        self.ensure_started()
    }

    fn submit(&self, job: CallbackJob) -> crate::Result<()> {
        self.try_submit(job)
    }
}

impl ExecutorShared {
    fn next_job(&self, cancelled: &AtomicBool) -> Result<Option<CallbackJob>, NetError> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        loop {
            if cancelled.load(Ordering::Acquire) {
                return Ok(None);
            }
            if state.ready {
                if let Some(job) = state.pending.pop_front() {
                    return Ok(Some(job));
                }
            }
            if !state.accepting {
                return Ok(None);
            }
            state = self.changed.wait(state).map_err(NetError::from_poison)?;
        }
    }

    fn run_worker(&self, cancelled: &AtomicBool) {
        loop {
            match self.next_job(cancelled) {
                Ok(Some(job)) => {
                    if catch_unwind(AssertUnwindSafe(job)).is_err() {
                        crate::log_e!(LogType::WSC; "listener_executor", "error", "user_callback_unwound");
                    }
                }
                Ok(None) => return,
                Err(error) => {
                    crate::log_e!(LogType::WSC; "listener_executor", "worker_error", error.to_string());
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "listener_executor_lazy_tests.rs"]
mod lazy_tests;

#[cfg(test)]
#[path = "listener_subscription_executor_tests.rs"]
mod subscription_executor_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check, check_eq, TestResult};
    use std::collections::HashSet;
    use std::sync::mpsc;
    use std::time::Duration;

    const DEADLINE: Duration = Duration::from_secs(5);

    #[test]
    fn construction_without_subscribers_starts_no_workers() -> TestResult {
        let executor = ListenerExecutor::new("listener-executor-lazy", 2, 8)?;
        check_eq!(
            executor
                .started_workers
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        )?;
        Ok(())
    }

    #[test]
    fn callbacks_reuse_a_fixed_runtime_free_worker_set() -> TestResult {
        let executor = ListenerExecutor::new("listener-executor-reuse", 2, 32)?;
        executor.ensure_started()?;
        let (observed_tx, observed_rx) = mpsc::channel();
        for _ in 0..32 {
            let observed_tx = observed_tx.clone();
            executor.try_submit(Box::new(move || {
                let _ = observed_tx.send((
                    std::thread::current().id(),
                    tokio::runtime::Handle::try_current().is_ok(),
                ));
            }))?;
        }
        let mut workers = HashSet::new();
        for _ in 0..32 {
            let (worker, has_runtime) = observed_rx.recv_timeout(DEADLINE)?;
            check!(!has_runtime)?;
            check!(worker != std::thread::current().id())?;
            workers.insert(worker);
        }
        check!(!workers.is_empty() && workers.len() <= 2)?;
        executor.close()?;
        Ok(())
    }

    #[test]
    fn queue_capacity_and_close_are_enforced_while_started_work_can_finish() -> TestResult {
        let executor = ListenerExecutor::new("listener-executor-capacity", 1, 1)?;
        executor.ensure_started()?;
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        executor.try_submit(Box::new(move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
        }))?;
        started_rx.recv_timeout(DEADLINE)?;
        let (finished_tx, finished_rx) = mpsc::channel();
        executor.try_submit(Box::new(move || {
            let _ = finished_tx.send(());
        }))?;
        check_eq!(
            executor.try_submit(Box::new(|| {})),
            Err(NetError::from(crate::error::ErrorKind::QueueFull))
        )?;
        executor.close()?;
        check_eq!(
            executor.try_submit(Box::new(|| {})),
            Err(NetError::from(crate::error::ErrorKind::QueueClosed))
        )?;
        drop(release_tx);
        finished_rx.recv_timeout(DEADLINE)?;
        Ok(())
    }

    #[test]
    fn invalid_configuration_starts_no_worker() -> TestResult {
        check!(matches!(
            ListenerExecutor::new("invalid-executor", 0, 1),
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::InvalidConfig)))?;
        check!(matches!(
            ListenerExecutor::new("invalid-executor", 1, 0),
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::InvalidConfig)))?;
        Ok(())
    }

    #[test]
    fn excessive_worker_count_is_rejected_before_any_spawn_attempt() -> TestResult {
        let mut attempts = 0;
        let result = ListenerExecutor::new("excessive-listener-workers", usize::MAX, 1).and_then(
            |executor| {
                executor.ensure_started_with_spawner(|_, _| {
                    attempts += 1;
                    Err(std::io::Error::other("worker count guard was bypassed"))
                })
            },
        );
        check!(
            matches!(result, Err(error) if matches!(error.kind(), crate::error::ErrorKind::InvalidConfig))
        )?;
        check_eq!(attempts, 0)?;
        Ok(())
    }

    #[test]
    fn partial_thread_start_failure_releases_already_started_workers() -> TestResult {
        let (finished_tx, finished_rx) = mpsc::channel();
        let mut attempts = 0;
        let executor = ListenerExecutor::new("listener-executor-spawn-failure", 2, 1)?;
        let result = executor.ensure_started_with_spawner(|builder, task| {
            attempts += 1;
            if attempts == 2 {
                return Err(std::io::Error::other("injected thread resource exhaustion"));
            }
            let finished_tx = finished_tx.clone();
            builder.spawn(move || {
                task();
                let _ = finished_tx.send(());
            })
        });
        check!(
            matches!(result, Err(error) if matches!(error.kind(), crate::error::ErrorKind::RuntimeUnavailable))
        )?;
        check_eq!(attempts, 2)?;
        finished_rx.recv_timeout(DEADLINE)?;
        // Retry only after the failed attempt's worker has completely exited.
        executor.ensure_started()?;
        let (delivered_tx, delivered_rx) = mpsc::channel();
        executor.try_submit(Box::new(move || {
            let _ = delivered_tx.send(());
        }))?;
        delivered_rx.recv_timeout(DEADLINE)?;
        Ok(())
    }

    #[test]
    fn dropping_last_executor_owner_drains_accepted_work() -> TestResult {
        let executor = ListenerExecutor::new("listener-executor-drop", 1, 1)?;
        executor.ensure_started()?;
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        executor.try_submit(Box::new(move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
        }))?;
        started_rx.recv_timeout(DEADLINE)?;
        let (finished_tx, finished_rx) = mpsc::channel();
        executor.try_submit(Box::new(move || {
            let _ = finished_tx.send(());
        }))?;
        drop(executor);
        drop(release_tx);
        finished_rx.recv_timeout(DEADLINE)?;
        Ok(())
    }
}

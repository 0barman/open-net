use crate::common::common_error::CommonError;
use crate::common::log::log_def::LogType;
use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};

#[cfg(test)]
#[path = "lazy_callback_pool_tests.rs"]
mod tests;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Clones own the executor, while workers own only its queue. Dropping the last
/// clone closes the queue without joining a callback that may be that caller.
#[derive(Clone)]
pub(crate) struct LazyCallbackPool {
    owner: Arc<PoolOwner>,
}

struct PoolOwner {
    thread_count: usize,
    ready: Mutex<Option<Arc<Workers>>>,
    #[cfg(test)]
    fail_after: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    reserve_next: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    counts: Arc<TestWorkerCounts>,
}

#[derive(Default)]
struct Queue {
    jobs: VecDeque<Job>,
    closed: bool,
}

struct Workers {
    queue: Mutex<Queue>,
    available: Condvar,
}

impl LazyCallbackPool {
    pub(crate) fn new(thread_count: usize) -> Self {
        Self {
            owner: Arc::new(PoolOwner {
                thread_count,
                ready: Mutex::new(None),
                #[cfg(test)]
                fail_after: std::sync::atomic::AtomicUsize::new(usize::MAX),
                #[cfg(test)]
                reserve_next: std::sync::atomic::AtomicUsize::new(1),
                #[cfg(test)]
                counts: Arc::new(TestWorkerCounts::default()),
            }),
        }
    }

    /// Complete fallible startup before publishing a public listener. A failed
    /// attempt joins every worker it already started, then permits a retry.
    pub(crate) fn ensure_ready(&self) -> Result<(), CommonError> {
        self.workers().map(|_| ())
    }

    fn workers(&self) -> Result<Arc<Workers>, CommonError> {
        let mut ready = self
            .owner
            .ready
            .lock()
            .map_err(|_| CommonError::RuntimeError)?;
        if let Some(workers) = ready.as_ref() {
            return Ok(Arc::clone(workers));
        }
        if self.owner.thread_count == 0 {
            return Err(CommonError::RuntimeError);
        }
        let workers = Arc::new(Workers {
            queue: Mutex::new(Queue::default()),
            available: Condvar::new(),
        });
        let mut handles = Vec::new();
        handles
            .try_reserve(self.owner.thread_count)
            .map_err(|_| CommonError::RuntimeError)?;
        #[cfg(test)]
        let fail_after = self
            .owner
            .fail_after
            .swap(usize::MAX, std::sync::atomic::Ordering::AcqRel);
        for index in 0..self.owner.thread_count {
            let shared = Arc::clone(&workers);
            #[cfg(test)]
            let counts = Arc::clone(&self.owner.counts);
            let spawn = || {
                #[cfg(test)]
                if index == fail_after {
                    return Err(std::io::Error::other(
                        "injected callback worker spawn failure",
                    ));
                }
                std::thread::Builder::new()
                    .name(format!("open-net-callback-{index}"))
                    .spawn(move || {
                        #[cfg(test)]
                        let _count = WorkerCountGuard::new(counts);
                        shared.run();
                    })
            };
            match spawn() {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    workers.close();
                    let mut join_failed = false;
                    for handle in handles {
                        if handle.join().is_err() {
                            join_failed = true;
                        }
                    }
                    drop(ready);
                    crate::log_e!(LogType::Common; "callback_pool_start", "error", crate::common::log::summary::error(&error));
                    if join_failed {
                        crate::log_e!(LogType::Common; "callback_pool_start", "error", "rollback_worker_join_failed");
                    }
                    return Err(CommonError::RuntimeError);
                }
            }
        }
        *ready = Some(Arc::clone(&workers));
        // Workers drain accepted jobs after the final owner closes the queue.
        // Their handles are detached to make callback-initiated teardown safe.
        drop(handles);
        Ok(workers)
    }

    pub(crate) fn execute<F>(&self, job: F) -> Result<(), CommonError>
    where
        F: FnOnce() + Send + 'static,
    {
        let workers = self.workers()?;
        // Keep captures outside the queue guard so failure cannot drop user
        // values under a lock that their destructors may reenter.
        let job: Job = Box::new(job);
        let mut queue = workers.queue.lock().map_err(|_| CommonError::PostError)?;
        if queue.closed {
            return Err(CommonError::PostError);
        }
        #[cfg(test)]
        let reserve = self
            .owner
            .reserve_next
            .swap(1, std::sync::atomic::Ordering::AcqRel);
        #[cfg(not(test))]
        let reserve = 1;
        queue
            .jobs
            .try_reserve(reserve)
            .map_err(|_| CommonError::PostError)?;
        queue.jobs.push_back(job);
        drop(queue);
        workers.available.notify_one();
        Ok(())
    }

    /// Existing callback wrappers return (), so resource failures are observable
    /// through logging instead of invoking a user callback on the caller thread.
    pub(crate) fn execute_or_log<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Err(error) = self.execute(job) {
            crate::log_e!(LogType::Common; "callback_dispatch", "error", format!("{error:?}"));
        }
    }

    #[cfg(test)]
    pub(crate) fn max_count(&self) -> usize {
        match self.owner.ready.lock() {
            Ok(ready) if ready.is_some() => self.owner.thread_count,
            _ => 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_next_start_after(&self, count: usize) {
        self.owner
            .fail_after
            .store(count, std::sync::atomic::Ordering::Release);
    }
}

impl Workers {
    fn close(&self) {
        let mut queue = match self.queue.lock() {
            Ok(queue) => queue,
            Err(poisoned) => {
                crate::log_e!(LogType::Common; "callback_pool_close", "error", "queue_lock_poisoned_recovered");
                poisoned.into_inner()
            }
        };
        queue.closed = true;
        drop(queue);
        self.available.notify_all();
    }

    fn run(&self) {
        loop {
            let job = {
                let mut queue = match self.queue.lock() {
                    Ok(queue) => queue,
                    Err(poisoned) => {
                        crate::log_e!(LogType::Common; "callback_pool_worker", "error", "queue_lock_poisoned_recovered");
                        poisoned.into_inner()
                    }
                };
                loop {
                    if let Some(job) = queue.jobs.pop_front() {
                        break job;
                    }
                    if queue.closed {
                        return;
                    }
                    queue = match self.available.wait(queue) {
                        Ok(queue) => queue,
                        Err(poisoned) => {
                            crate::log_e!(LogType::Common; "callback_pool_worker", "error", "queue_wait_poisoned_recovered");
                            poisoned.into_inner()
                        }
                    };
                }
            };
            if catch_unwind(AssertUnwindSafe(job)).is_err() {
                crate::log_e!(LogType::Common; "callback_dispatch", "error", "user_callback_panicked");
            }
        }
    }
}

impl Drop for PoolOwner {
    fn drop(&mut self) {
        let ready = match self.ready.get_mut() {
            Ok(ready) => ready,
            Err(poisoned) => {
                crate::log_e!(LogType::Common; "callback_pool_drop", "error", "initialization_lock_poisoned_recovered");
                poisoned.into_inner()
            }
        };
        if let Some(workers) = ready.take() {
            workers.close();
        }
    }
}

#[cfg(test)]
#[derive(Default)]
struct TestWorkerCounts {
    started: std::sync::atomic::AtomicUsize,
    live: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
struct WorkerCountGuard(Arc<TestWorkerCounts>);

#[cfg(test)]
impl WorkerCountGuard {
    fn new(counts: Arc<TestWorkerCounts>) -> Self {
        counts
            .started
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        counts
            .live
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Self(counts)
    }
}

#[cfg(test)]
impl Drop for WorkerCountGuard {
    fn drop(&mut self) {
        self.0
            .live
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

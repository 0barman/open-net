//! Explicit retirement of SDK-owned threads, independent of public handle clones.

use super::CommonError;
use std::cell::Cell;
use std::io;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;

thread_local! {
    static EXECUTION_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Synchronous SDK execution cannot wait for the SDK resources containing it.
pub(crate) struct ExecutionScope;

impl ExecutionScope {
    pub(crate) fn enter() -> Self {
        EXECUTION_DEPTH.with(|depth| depth.set(depth.get() + 1));
        Self
    }

    pub(crate) fn is_current() -> bool {
        EXECUTION_DEPTH.with(|depth| depth.get() != 0)
    }

    /// Runtime worker threads retain this role for their complete lifetime.
    pub(crate) fn mark_current_thread() {
        EXECUTION_DEPTH.with(|depth| depth.set(depth.get() + 1));
    }
}

impl Drop for ExecutionScope {
    fn drop(&mut self) {
        EXECUTION_DEPTH.with(|depth| depth.set(depth.get() - 1));
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone)]
pub(crate) struct ThreadHandle(Arc<ThreadState>);

struct ThreadState {
    thread_id: std::thread::ThreadId,
    state: Mutex<JoinState>,
    changed: Condvar,
}

struct JoinState {
    handle: Option<JoinHandle<()>>,
    result: Option<Result<(), CommonError>>,
}

impl ThreadHandle {
    pub(crate) fn is_finished(&self) -> bool {
        let state = lock(&self.0.state);
        state.result.is_some() || state.handle.as_ref().is_some_and(JoinHandle::is_finished)
    }

    pub(crate) fn join(&self) -> Result<(), CommonError> {
        if self.0.thread_id == std::thread::current().id() {
            return Err(CommonError::RuntimeError);
        }
        let mut state = lock(&self.0.state);
        loop {
            if let Some(result) = state.result {
                return result;
            }
            if let Some(handle) = state.handle.take() {
                drop(state);
                let result = match handle.join() {
                    Ok(()) => Ok(()),
                    Err(payload) => {
                        discard_panic_payload(payload);
                        Err(CommonError::RuntimeError)
                    }
                };
                state = lock(&self.0.state);
                state.result = Some(result);
                self.0.changed.notify_all();
                return result;
            }
            state = self
                .0
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

#[derive(Default)]
struct RegistryState {
    threads: Vec<ThreadHandle>,
    sealed: bool,
    joining: bool,
    reaping: usize,
    starting: usize,
    failed: bool,
    result: Option<Result<(), CommonError>>,
}

#[derive(Default)]
pub(crate) struct ThreadRegistry {
    state: Mutex<RegistryState>,
    changed: Condvar,
}

struct StartingThread(Arc<ThreadRegistry>);

impl Drop for StartingThread {
    fn drop(&mut self) {
        let mut state = lock(&self.0.state);
        state.starting -= 1;
        drop(state);
        self.0.changed.notify_all();
    }
}

/// Counts accepted asynchronous retirement work while producers are shutting
/// down. A producer registers derived work before releasing its own lease.
/// There is deliberately no seal: accepted bridges may still dispatch terminal
/// observations while the independent coordinator is waiting for them.
#[derive(Default)]
pub(crate) struct WorkTracker {
    active: Mutex<usize>,
    changed: Condvar,
    #[cfg(test)]
    wait_started: (Mutex<bool>, Condvar),
}

pub(crate) struct WorkLease {
    tracker: Arc<WorkTracker>,
}

impl WorkTracker {
    pub(crate) fn register(self: &Arc<Self>) -> WorkLease {
        let mut active = lock(&self.active);
        *active = active
            .checked_add(1)
            .expect("retirement work count overflow");
        WorkLease {
            tracker: Arc::clone(self),
        }
    }

    /// The caller first closes all external producers. Only already registered
    /// work may register descendants while this waits on an ordinary OS thread.
    pub(crate) fn wait(&self) {
        #[cfg(test)]
        {
            *lock(&self.wait_started.0) = true;
            self.wait_started.1.notify_all();
        }
        let mut active = lock(&self.active);
        while *active != 0 {
            active = self
                .changed
                .wait(active)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    #[cfg(test)]
    pub(crate) fn wait_started_for_test(&self, timeout: std::time::Duration) -> bool {
        let started = lock(&self.wait_started.0);
        let (started, _) = self
            .wait_started
            .1
            .wait_timeout_while(started, timeout, |started| !*started)
            .unwrap_or_else(|error| error.into_inner());
        *started
    }
}

impl Drop for WorkLease {
    fn drop(&mut self) {
        let mut active = lock(&self.tracker.active);
        *active -= 1;
        let finished = *active == 0;
        drop(active);
        if finished {
            self.tracker.changed.notify_all();
        }
    }
}

impl ThreadRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn spawn(
        self: &Arc<Self>,
        name: impl Into<String>,
        run: impl FnOnce() + Send + 'static,
    ) -> io::Result<()> {
        self.spawn_tracked(name, run).map(|_| ())
    }

    pub(crate) fn spawn_tracked(
        self: &Arc<Self>,
        name: impl Into<String>,
        run: impl FnOnce() + Send + 'static,
    ) -> io::Result<ThreadHandle> {
        self.spawn_inner(name, run, true)
    }

    /// For startup callers holding another lifecycle lock: reclaiming an old
    /// panic payload may run a user destructor, so leave that work to a later
    /// unlocked spawn or the explicit retirement coordinator.
    pub(crate) fn spawn_tracked_without_reaping(
        self: &Arc<Self>,
        name: impl Into<String>,
        run: impl FnOnce() + Send + 'static,
    ) -> io::Result<ThreadHandle> {
        self.spawn_inner(name, run, false)
    }

    fn spawn_inner(
        self: &Arc<Self>,
        name: impl Into<String>,
        run: impl FnOnce() + Send + 'static,
        reap: bool,
    ) -> io::Result<ThreadHandle> {
        let name = name.into();
        if name.contains('\0') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SDK thread name contains NUL",
            ));
        }
        let mut state = lock(&self.state);
        if state.sealed {
            drop(state);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "SDK thread registry is closed",
            ));
        }
        // Completed threads are joined before replacing their slots, so repeated
        // connection/provider creation does not accumulate detached handles.
        if reap {
            let mut retired = Vec::new();
            retired
                .try_reserve(state.threads.len())
                .map_err(io::Error::other)?;
            let mut index = 0;
            while index < state.threads.len() {
                if state.threads[index].is_finished() {
                    retired.push(state.threads.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            if !retired.is_empty() {
                state.reaping += 1;
                drop(state);
                let mut failed = false;
                for handle in retired {
                    failed |= handle.join().is_err();
                }
                state = lock(&self.state);
                state.failed |= failed;
                state.reaping -= 1;
                self.changed.notify_all();
                if state.sealed {
                    drop(state);
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "SDK thread registry is closed",
                    ));
                }
            }
        }
        let reserved = state
            .starting
            .checked_add(1)
            .ok_or_else(|| io::Error::other("SDK starting thread count overflow"))?;
        state
            .threads
            .try_reserve(reserved)
            .map_err(io::Error::other)?;
        state.starting += 1;
        drop(state);
        let starting = StartingThread(Arc::clone(self));
        // Failed spawning destroys closure captures. Keep user destructors out
        // of the registry lock; sealing waits for this accepted start to finish.
        let handle = {
            // A rejected closure's destructor is still SDK-owned cleanup and
            // cannot wait for this registry's own pending-start reservation.
            let _scope = ExecutionScope::enter();
            std::thread::Builder::new().name(name).spawn(move || {
                let _scope = ExecutionScope::enter();
                run();
            })?
        };
        let handle = ThreadHandle(Arc::new(ThreadState {
            thread_id: handle.thread().id(),
            state: Mutex::new(JoinState {
                handle: Some(handle),
                result: None,
            }),
            changed: Condvar::new(),
        }));
        let mut state = lock(&self.state);
        state.threads.push(handle.clone());
        drop(state);
        drop(starting);
        Ok(handle)
    }

    /// No new threads can be registered after sealing. All callers see the same
    /// result, and success is published only after every OS thread was joined.
    pub(crate) fn seal_and_join(&self) -> Result<(), CommonError> {
        let mut state = lock(&self.state);
        state.sealed = true;
        loop {
            if let Some(result) = state.result {
                return result;
            }
            if !state.joining && state.reaping == 0 && state.starting == 0 {
                state.joining = true;
                let threads = std::mem::take(&mut state.threads);
                let mut failed = state.failed;
                drop(state);
                for thread in threads {
                    failed |= thread.join().is_err();
                }
                let result = if failed {
                    Err(CommonError::RuntimeError)
                } else {
                    Ok(())
                };
                state = lock(&self.state);
                state.result = Some(result);
                self.changed.notify_all();
                return result;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

pub(crate) fn discard_panic_payload(payload: Box<dyn std::any::Any + Send>) {
    // A user panic payload may itself panic in Drop. Preserve the retirement
    // result rather than unwinding and stranding concurrent joiners.
    let _scope = ExecutionScope::enter();
    if let Err(second) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(second);
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;

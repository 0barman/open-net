use crate::common::CommonError;
use std::future::Future;
use std::sync::{Condvar, Mutex};
use tokio::runtime::{Handle, Runtime};

/// Owns the common engine's multithreaded runtime independently of the thread
/// that releases its final Arc. Explicit retirement closes the runtime even if
/// external owners still retain this object or a cloned handle.
pub(crate) struct OwnedRuntime {
    handle: Handle,
    state: Mutex<RuntimeState>,
    changed: Condvar,
}

struct RuntimeState {
    runtime: Option<Runtime>,
    retiring: bool,
    result: Option<Result<(), CommonError>>,
}

impl OwnedRuntime {
    pub(super) fn new(runtime: Runtime) -> Self {
        Self {
            handle: runtime.handle().clone(),
            state: Mutex::new(RuntimeState {
                runtime: Some(runtime),
                retiring: false,
                result: None,
            }),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn handle(&self) -> &Handle {
        &self.handle
    }

    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        // CommonEngine always builds a multithreaded runtime; its workers drive
        // timers and I/O while this handle waits for the submitted future.
        self.handle.block_on(future)
    }

    /// Called by the independent shutdown coordinator, never by runtime work.
    pub(crate) fn shutdown_blocking(&self) -> Result<(), CommonError> {
        if Handle::try_current().is_ok() {
            return Err(CommonError::RuntimeError);
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        loop {
            if let Some(result) = state.result {
                return result;
            }
            if !state.retiring {
                state.retiring = true;
                let runtime = state.runtime.take();
                drop(state);
                let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    drop(runtime)
                })) {
                    Ok(()) => Ok(()),
                    Err(payload) => {
                        crate::common::retirement::discard_panic_payload(payload);
                        Err(CommonError::RuntimeError)
                    }
                };
                state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                state.result = Some(result);
                self.changed.notify_all();
                return result;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        let Some(runtime) = state.runtime.take() else {
            return;
        };
        if Handle::try_current().is_ok() {
            // An entered Tokio context may forbid blocking teardown. This stops
            // async tasks without waiting here; already running blocking user
            // code cannot be forcibly stopped and may finish in the background.
            runtime.shutdown_background();
        } else {
            // Preserve normal synchronous teardown on an ordinary OS thread,
            // including waiting for already running blocking tasks to return.
            drop(runtime);
        }
    }
}

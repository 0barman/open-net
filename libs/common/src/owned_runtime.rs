use std::future::Future;
use tokio::runtime::{Handle, Runtime};

/// Owns the common engine's multithreaded runtime independently of the thread
/// that releases its final Arc. The handle remains usable while this owner lives.
pub(crate) struct OwnedRuntime {
    handle: Handle,
    runtime: Option<Runtime>,
}

impl OwnedRuntime {
    pub(super) fn new(runtime: Runtime) -> Self {
        Self {
            handle: runtime.handle().clone(),
            runtime: Some(runtime),
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
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
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

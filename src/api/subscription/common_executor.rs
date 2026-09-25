//! Adapter that runs common-engine subscription callbacks on the shared pool.

use super::CallbackExecutor;
use crate::common::common_engine::LazyCallbackPool;
use crate::common::CommonEngine;
use crate::{NetError, Result};

/// Dispatches monitor observers through the engine's existing callback pool.
/// Keeping a pool and a runtime handle does not retain the engine owner.
pub(crate) struct CommonCallbackExecutor {
    pool: LazyCallbackPool,
    runtime: tokio::runtime::Handle,
}

impl CommonCallbackExecutor {
    /// Create an executor borrowing the engine's callback pool and runtime handle.
    ///
    /// The adapter keeps only those handles; it does not retain the engine owner
    /// or extend the engine lifetime by itself.
    pub(crate) fn new(engine: &CommonEngine) -> Self {
        Self {
            pool: engine.cb_pool.clone(),
            runtime: engine.runtime_handle(),
        }
    }
}

impl CallbackExecutor for CommonCallbackExecutor {
    fn ensure_ready(&self) -> Result<()> {
        self.pool.ensure_ready().map_err(NetError::from)
    }

    fn submit(&self, job: Box<dyn FnOnce() + Send + 'static>) -> Result<()> {
        let runtime = self.runtime.clone();
        self.pool
            .execute(move || {
                // Existing network monitor callbacks enter the engine runtime.
                // This does not create a runtime or move work onto its workers.
                let _runtime_guard = runtime.enter();
                job();
            })
            .map_err(NetError::from)
    }
}

#[cfg(test)]
#[path = "common_executor_tests.rs"]
mod tests;

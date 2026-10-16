//! Worker retirement is owned by the SDK, independently of shutdown waiters.
use crate::error::{ErrorKind, ErrorStage, NetError};
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use tokio::sync::Notify;

type Cleanup = Box<dyn FnOnce() + Send + 'static>;

pub(super) enum WorkerJoin {
    Owned(JoinHandle<()>),
    Tracked(crate::common::retirement::ThreadHandle),
}
impl WorkerJoin {
    pub(super) fn join(self) -> std::thread::Result<()> {
        match self {
            Self::Owned(handle) => handle.join(),
            Self::Tracked(handle) => handle.join().map_err(|_| {
                Box::new("tracked HTTP worker failed") as Box<dyn std::any::Any + Send>
            }),
        }
    }
}

struct State {
    joins: Vec<WorkerJoin>,
    started: bool,
    retired: bool,
    result: Option<Result<(), NetError>>,
}

pub(super) struct WorkerShutdown {
    state: Mutex<State>,
    notify: Notify,
    threads: Option<Arc<crate::common::retirement::ThreadRegistry>>,
}

impl WorkerShutdown {
    #[cfg(test)]
    pub(super) fn new(joins: Vec<JoinHandle<()>>) -> Self {
        Self::from_joins(joins.into_iter().map(WorkerJoin::Owned).collect(), None)
    }

    pub(super) fn from_joins(
        joins: Vec<WorkerJoin>,
        threads: Option<Arc<crate::common::retirement::ThreadRegistry>>,
    ) -> Self {
        let retired = joins.is_empty();
        Self {
            state: Mutex::new(State {
                joins,
                started: false,
                retired,
                result: retired.then_some(Ok(())),
            }),
            notify: Notify::new(),
            threads,
        }
    }

    fn locked(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(super) fn workers_joined(&self) -> bool {
        self.locked().retired
    }

    pub(super) fn start(self: &Arc<Self>) -> Result<(), NetError> {
        self.start_with(|cleanup| match &self.threads {
            Some(threads) => threads
                .spawn_tracked_without_reaping("open-net-http-destroy-cleanup", cleanup)
                .map(drop),
            None => thread::Builder::new()
                .name("open-net-http-destroy-cleanup".to_owned())
                .spawn(cleanup)
                .map(drop),
        })
    }

    // The launcher must transfer the work to another thread before returning Ok.
    // Holding this gate until launch succeeds prevents duplicate launches and
    // keeps the join handles here if the OS refuses to create a cleanup thread.
    fn start_with<F>(self: &Arc<Self>, launch: F) -> Result<(), NetError>
    where
        F: FnOnce(Cleanup) -> io::Result<()>,
    {
        let mut state = self.locked();
        if state.started || state.result.is_some() {
            return Ok(());
        }
        let owner = Arc::clone(self);
        launch(Box::new(move || owner.join_workers())).map_err(|error| {
            NetError::with_source(ErrorKind::RuntimeUnavailable, error)
                .with_stage(ErrorStage::Runtime)
        })?;
        state.started = true;
        Ok(())
    }

    pub(super) async fn wait(&self) -> Result<(), NetError> {
        loop {
            let notified = self.notify.notified();
            if let Some(result) = self.locked().result.clone() {
                return result;
            }
            notified.await;
        }
    }

    // Also preserves the existing final-owner Drop fallback when thread launch
    // fails. Explicit async shutdown never calls this on the caller's thread.
    pub(super) fn join_workers(&self) {
        let _scope = crate::common::retirement::ExecutionScope::enter();
        let joins = {
            let mut state = self.locked();
            state.started = true;
            std::mem::take(&mut state.joins)
        };
        let mut failed = false;
        let mut retired = true;
        for join in joins {
            match catch_unwind(AssertUnwindSafe(|| join.join())) {
                Ok(Ok(())) => {}
                Ok(Err(payload)) => {
                    // A panicking worker has still exited. Its payload is user
                    // owned and may itself panic on drop; finish every join.
                    failed = true;
                    drop_panic_payload(payload);
                }
                Err(payload) => {
                    // join itself failed (for example self-join in the final
                    // Drop fallback), so retirement is not confirmed.
                    failed = true;
                    retired = false;
                    drop_panic_payload(payload);
                }
            }
        }
        let result = if failed {
            Err(NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Close))
        } else {
            Ok(())
        };
        {
            let mut state = self.locked();
            state.retired = retired;
            state.result = Some(result);
        }
        self.notify.notify_waiters();
    }
}

fn drop_panic_payload(payload: Box<dyn std::any::Any + Send>) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        // Repeatedly dropping hostile panic payloads can abort the process.
        std::mem::forget(secondary);
    }
}

#[cfg(test)]
#[path = "worker_shutdown_tests.rs"]
mod tests;

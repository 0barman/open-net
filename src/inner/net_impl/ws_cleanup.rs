//! A join job retains its runtime and its name reservation until the worker exits.

use super::{client_slot::ClientSlot, release_closing_slot};
use crate::common::retirement::ThreadHandle;
use crate::common::CommonEngine;
use crate::error::{ErrorKind, ErrorStage, NetError};
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, Notify};

type Outcome = Result<(), NetError>;

struct CleanupWork {
    // A Handle alone does not keep the runtime alive. Retain only the internal
    // engine, not OpenNet, so a root drop can still shut down network services.
    _engine: Arc<CommonEngine>,
    clients: Arc<Mutex<HashMap<String, ClientSlot>>>,
    changed: Arc<Notify>,
    name: String,
    worker: Option<ThreadHandle>,
    completed: oneshot::Sender<Outcome>,
}

impl CleanupWork {
    fn run(self) {
        let _scope = crate::common::retirement::ExecutionScope::enter();
        let result = match self.worker {
            Some(worker) => match worker.join() {
                Ok(()) => Ok(()),
                Err(_) => Err(NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Close)),
            },
            None => Ok(()),
        };
        release_closing_slot(&self.clients, &self.changed, &self.name);
        let _ = self.completed.send(result);
    }
}

/// The executor and its rejection/cancellation fallback compete for one payload.
/// In particular, Tokio can enqueue a blocking job *before* thread creation
/// panics. Retrying with an independently owned join would lose that guarantee.
pub(super) struct CleanupTask {
    pending: Arc<Mutex<Option<CleanupWork>>>,
}

impl CleanupTask {
    pub(super) fn new(
        engine: Arc<CommonEngine>,
        clients: Arc<Mutex<HashMap<String, ClientSlot>>>,
        changed: Arc<Notify>,
        name: String,
        worker: Option<ThreadHandle>,
    ) -> (Self, oneshot::Receiver<Outcome>) {
        let (completed, receiver) = oneshot::channel();
        let work = CleanupWork {
            _engine: engine,
            clients,
            changed,
            name,
            worker,
            completed,
        };
        (
            Self {
                pending: Arc::new(Mutex::new(Some(work))),
            },
            receiver,
        )
    }

    pub(super) fn submit(self, runtime: &tokio::runtime::Handle) -> Outcome {
        self.submit_with(|task| {
            // Completion belongs to CleanupWork, not this Tokio JoinHandle or
            // the public destroy future. Dropping either waiter cannot cancel it.
            drop(runtime.spawn_blocking(move || task.run()));
        })
    }

    fn submit_with(self, submit: impl FnOnce(Self)) -> Outcome {
        let pending = Arc::clone(&self.pending);
        match catch_unwind(AssertUnwindSafe(|| submit(self))) {
            Ok(()) => Ok(()),
            Err(payload) => {
                // The job may still be queued, so its Drop is not sufficient.
                // The fallback atomically claims exactly the same payload.
                Self::fallback(&pending);
                discard_panic(payload);
                Err(NetError::from(ErrorKind::RuntimeUnavailable).with_stage(ErrorStage::Runtime))
            }
        }
    }

    fn run(self) {
        Self::run_pending(&self.pending);
    }

    fn run_pending(pending: &Mutex<Option<CleanupWork>>) {
        let work = pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(work) = work {
            work.run();
        }
    }

    fn fallback(pending: &Arc<Mutex<Option<CleanupWork>>>) {
        let threads = {
            let pending = pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(work) = pending.as_ref() else {
                return;
            };
            work._engine.resource_threads()
        };
        let transferred = Arc::clone(pending);
        let spawned = threads.spawn("open-net-ws-destroy-cleanup", move || {
            Self::run_pending(&transferred)
        });
        if spawned.is_err() {
            // Preserve the existing last-owner Drop fallback: if the OS cannot
            // start cleanup, finish synchronously rather than detach the worker.
            Self::run_pending(pending);
        }
    }
}

impl Drop for CleanupTask {
    fn drop(&mut self) {
        Self::fallback(&self.pending);
    }
}

fn discard_panic(payload: Box<dyn std::any::Any + Send>) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(secondary);
    }
}

#[cfg(test)]
mod tests;

//! Tracked callback dispatch and abandonment cleanup.
//!
//! Admission into the engine pool is deliberately separate from worker start:
//! a queued job may be retired before it runs. The ticket state below ensures
//! that such retirement reports a dispatch failure exactly once and releases
//! captured application values outside subscription locks.

use super::{lock, CallbackExecutor};
use crate::error::{ErrorKind, ErrorStage, NetError};
use crate::Result;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

type Abandoned = Box<dyn FnOnce(NetError) + Send + 'static>;

#[derive(Clone, Copy)]
enum Phase {
    Submitting,
    Accepted,
    Started,
    Dropped,
    Rejected,
}

struct State {
    phase: Phase,
    abandoned: Option<Abandoned>,
}

struct Ticket {
    state: Mutex<State>,
}

impl Ticket {
    fn start(&self) {
        let retired = {
            let mut state = lock(&self.state);
            state.phase = Phase::Started;
            state.abandoned.take()
        };
        drop(retired);
    }

    fn accept(&self) -> Result<()> {
        let abandoned = {
            let mut state = lock(&self.state);
            match state.phase {
                Phase::Submitting => {
                    state.phase = Phase::Accepted;
                    None
                }
                Phase::Dropped => state.abandoned.take(),
                Phase::Accepted | Phase::Started | Phase::Rejected => None,
            }
        };
        match abandoned {
            Some(abandoned) => {
                report_abandoned(abandoned);
                Err(dispatch_failure())
            }
            None => Ok(()),
        }
    }

    fn reject(&self) {
        let retired = {
            let mut state = lock(&self.state);
            state.phase = Phase::Rejected;
            state.abandoned.take()
        };
        drop(retired);
    }

    fn retire(&self) {
        let abandoned = {
            let mut state = lock(&self.state);
            match state.phase {
                Phase::Submitting => {
                    state.phase = Phase::Dropped;
                    None
                }
                Phase::Accepted => {
                    state.phase = Phase::Dropped;
                    state.abandoned.take()
                }
                Phase::Started | Phase::Dropped | Phase::Rejected => None,
            }
        };
        if let Some(abandoned) = abandoned {
            report_abandoned(abandoned);
        }
    }
}

struct JobGuard(Arc<Ticket>);
impl Drop for JobGuard {
    fn drop(&mut self) {
        self.0.retire();
    }
}

fn dispatch_failure() -> NetError {
    NetError::from(ErrorKind::RuntimeUnavailable).with_stage(ErrorStage::Dispatch)
}

fn report_abandoned(abandoned: Abandoned) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| abandoned(dispatch_failure()))) {
        // Failure cleanup can retire application captures. Its unwind must not
        // escape this job's Drop and kill the engine's dispatcher.
        crate::log_e!(crate::common::log::log_def::LogType::Common;
            "subscription_dispatch_cleanup", "kind", format!("{:?}", ErrorKind::CallbackPanicked));
        if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            std::mem::forget(secondary);
        }
    }
}

/// A successful queue admission does not guarantee that a worker starts the job.
/// Synchronous rejection retains its original error; later abandonment closes
/// the registration even if a pool cannot create a worker or shuts its queue.
pub(super) fn submit_tracked(
    executor: &dyn CallbackExecutor,
    job: Box<dyn FnOnce() + Send + 'static>,
    abandoned: impl FnOnce(NetError) + Send + 'static,
) -> Result<()> {
    let ticket = Arc::new(Ticket {
        state: Mutex::new(State {
            phase: Phase::Submitting,
            abandoned: Some(Box::new(abandoned)),
        }),
    });
    let guard = JobGuard(ticket.clone());
    let submitted = executor.submit(Box::new(move || {
        let guard = guard;
        guard.0.start();
        job();
    }));
    match submitted {
        Ok(()) => ticket.accept(),
        Err(error) => {
            ticket.reject();
            Err(error)
        }
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;

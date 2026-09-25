use crate::error::{ErrorKind, ErrorStage, NetError};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug)]
pub(crate) struct BudgetDeadline {
    at: Instant,
    kind: ErrorKind,
}

impl BudgetDeadline {
    pub(crate) fn for_handshake(at: Instant) -> Self {
        Self {
            at,
            kind: ErrorKind::TimedOut,
        }
    }
    pub(crate) fn at(self) -> Instant {
        self.at
    }
    pub(crate) fn error(self) -> NetError {
        NetError::from(self.kind).with_stage(ErrorStage::Admission)
    }
    pub(crate) fn expired_error(self) -> Option<NetError> {
        (Instant::now() >= self.at).then(|| self.error())
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnectionBudget {
    deadline: Option<BudgetDeadline>,
}

impl ConnectionBudget {
    pub(crate) fn new(
        started_at: Instant,
        initial_connect_deadline: Option<Instant>,
        max_elapsed: Option<Duration>,
    ) -> crate::Result<Self> {
        let cycle = max_elapsed
            .map(|duration| {
                started_at
                    .checked_add(duration)
                    .map(|at| BudgetDeadline {
                        at,
                        kind: ErrorKind::RetryExhausted,
                    })
                    .ok_or_else(|| {
                        NetError::config(
                            "reconnect.max_elapsed",
                            "must fit a monotonic Instant deadline",
                        )
                    })
            })
            .transpose()?;
        let initial = initial_connect_deadline.map(|at| BudgetDeadline {
            at,
            kind: ErrorKind::TimedOut,
        });
        let deadline = match (initial, cycle) {
            (Some(initial), Some(cycle)) if cycle.at < initial.at => Some(cycle),
            (Some(initial), _) => Some(initial),
            (None, cycle) => cycle,
        };
        Ok(Self { deadline })
    }

    pub(crate) fn deadline(self) -> Option<BudgetDeadline> {
        self.deadline
    }

    pub(crate) fn attempt_deadline(
        self,
        handshake_timeout: Duration,
    ) -> crate::Result<BudgetDeadline> {
        let at = Instant::now()
            .checked_add(handshake_timeout)
            .ok_or_else(|| {
                NetError::config("handshake_timeout", "must fit a monotonic Instant deadline")
            })?;
        match self.deadline {
            Some(deadline) if deadline.at <= at => Ok(deadline),
            _ => Ok(BudgetDeadline::for_handshake(at)),
        }
    }
}

const PENDING: u8 = 0;
const ACCEPTED: u8 = 1;
const EXPIRED: u8 = 2;

/// A deadline races the worker's success admission, before the journal's
/// existing prepare/commit sequence. Once accepted that sequence must finish.
pub(crate) struct HandshakeAdmission {
    deadline: BudgetDeadline,
    decision: AtomicU8,
}

impl HandshakeAdmission {
    pub(crate) fn new(deadline: BudgetDeadline) -> Self {
        Self {
            deadline,
            decision: AtomicU8::new(PENDING),
        }
    }

    pub(crate) fn try_accept(&self) -> crate::Result<()> {
        loop {
            match self.decision.load(Ordering::Acquire) {
                ACCEPTED => {
                    return Err(
                        NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Admission)
                    )
                }
                EXPIRED => return Err(self.deadline.error()),
                _ => {}
            }
            if self.deadline.expired_error().is_some() {
                if self.expire() {
                    return Err(self.deadline.error());
                }
                continue;
            }
            match self.decision.compare_exchange(
                PENDING,
                ACCEPTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(_) => continue,
            }
        }
    }

    /// Returns false only when success already won admission. Callers then
    /// await its acknowledgement instead of rewriting it as a timeout.
    pub(crate) fn expire(&self) -> bool {
        match self
            .decision
            .compare_exchange(PENDING, EXPIRED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(EXPIRED) => true,
            Err(_) => false,
        }
    }
}

#[cfg(test)]
#[path = "connection_budget_tests.rs"]
mod tests;

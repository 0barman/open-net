//! Absolute HTTP budget arbitration shared by admission, attempts, and bodies.

use crate::api::error::{ErrorKind, ErrorStage, NetError};
use crate::api::http::TimeoutPhase;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::{Instant, Sleep};

#[derive(Debug)]
struct BudgetExpired {
    deadline: Instant,
    phase: TimeoutPhase,
}
impl std::fmt::Display for BudgetExpired {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("HTTP time budget expired")
    }
}
impl std::error::Error for BudgetExpired {}

/// The typed source survives cloning and nested SDK boundary checks.
pub(crate) fn timeout_deadline(error: &NetError) -> Option<(Instant, TimeoutPhase)> {
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        if let Some(expired) = cause.downcast_ref::<BudgetExpired>() {
            return Some((expired.deadline, expired.phase));
        }
        source = cause.source();
    }
    None
}

/// Only the earliest deadline matters; adding a budget can never extend a lease.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TimeoutBudget {
    earliest: Option<(Instant, TimeoutPhase)>,
}

impl TimeoutBudget {
    pub(crate) fn total(start: Instant, duration: Option<Duration>) -> Result<Self, NetError> {
        Self::default().with_phase(start, duration, TimeoutPhase::Total)
    }

    pub(crate) fn with_phase(
        self,
        start: Instant,
        duration: Option<Duration>,
        phase: TimeoutPhase,
    ) -> Result<Self, NetError> {
        let Some(duration) = duration else {
            return Ok(self);
        };
        if duration.is_zero() {
            return Err(NetError::config(field(phase), "timeout must be nonzero"));
        }
        let deadline = start.checked_add(duration).ok_or_else(|| {
            NetError::config(field(phase), "timeout exceeds the monotonic clock range")
        })?;
        match self.earliest {
            Some((current, current_phase))
                if (current, priority(current_phase)) <= (deadline, priority(phase)) =>
            {
                Ok(self)
            }
            _ => Ok(Self {
                earliest: Some((deadline, phase)),
            }),
        }
    }

    pub(crate) fn deadline(self) -> Option<Instant> {
        self.earliest.map(|(deadline, _)| deadline)
    }

    pub(crate) fn candidate_wins(self, deadline: Instant, phase: TimeoutPhase) -> bool {
        self.earliest.is_none_or(|(current, current_phase)| {
            (deadline, priority(phase)) <= (current, priority(current_phase))
        })
    }

    pub(crate) fn expired(self, now: Instant, stage: ErrorStage) -> Option<NetError> {
        let (deadline, phase) = self.earliest?;
        if now < deadline {
            return None;
        }
        let kind = if phase == TimeoutPhase::Total {
            ErrorKind::DeadlineExceeded
        } else {
            ErrorKind::TimedOut
        };
        let stage = match phase {
            TimeoutPhase::Admission => ErrorStage::Admission,
            TimeoutPhase::Headers => ErrorStage::Response,
            TimeoutPhase::IdleRead => ErrorStage::Receive,
            TimeoutPhase::Connect | TimeoutPhase::Attempt | TimeoutPhase::Total => stage,
        };
        Some(
            NetError::with_source(kind, BudgetExpired { deadline, phase })
                .with_stage(stage)
                .with_timeout_phase(phase),
        )
    }

    pub(crate) fn timer(self) -> Option<Pin<Box<Sleep>>> {
        self.deadline()
            .map(|deadline| Box::pin(tokio::time::sleep_until(deadline)))
    }
}

fn priority(phase: TimeoutPhase) -> u8 {
    match phase {
        TimeoutPhase::Total => 0,
        TimeoutPhase::Admission => 1,
        TimeoutPhase::Attempt => 2,
        TimeoutPhase::Connect => 3,
        TimeoutPhase::Headers => 4,
        TimeoutPhase::IdleRead => 5,
    }
}

fn field(phase: TimeoutPhase) -> &'static str {
    match phase {
        TimeoutPhase::Total => "http.timeouts.total",
        TimeoutPhase::Admission => "http.timeouts.admission",
        TimeoutPhase::Attempt => "http.timeouts.attempt",
        TimeoutPhase::Connect => "http.connect_timeout",
        TimeoutPhase::Headers => "http.timeouts.headers",
        TimeoutPhase::IdleRead => "http.timeouts.idle_read",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn one_absolute_budget_spans_stages_and_uses_tokio_time() -> TestResult {
        let start = Instant::now();
        let budget = TimeoutBudget::total(start, Some(Duration::from_secs(20)))?.with_phase(
            start,
            Some(Duration::from_secs(10)),
            TimeoutPhase::Attempt,
        )?;
        tokio::time::advance(Duration::from_secs(6)).await;
        let headers = budget.with_phase(
            Instant::now(),
            Some(Duration::from_secs(8)),
            TimeoutPhase::Headers,
        )?;
        let timer = headers
            .timer()
            .ok_or_else(|| std::io::Error::other("timer missing"))?;
        timer.await;
        let error = headers
            .expired(Instant::now(), ErrorStage::Write)
            .ok_or_else(|| std::io::Error::other("absolute attempt did not expire"))?;
        check(
            Instant::now().duration_since(start) == Duration::from_secs(10),
            "attempt was extended",
        )?;
        check(
            error.timeout_phase() == Some(TimeoutPhase::Attempt),
            "wrong earliest budget",
        )?;
        check(
            error.kind() == ErrorKind::TimedOut,
            "attempt must be TimedOut",
        )?;
        check(
            error.context().stage == Some(ErrorStage::Write),
            "attempt must retain observable stage",
        )
    }

    #[tokio::test(start_paused = true)]
    async fn exact_deadline_ties_use_fixed_priority_in_any_insertion_order() -> TestResult {
        let start = Instant::now();
        let phases = [
            TimeoutPhase::Total,
            TimeoutPhase::Admission,
            TimeoutPhase::Attempt,
            TimeoutPhase::Connect,
            TimeoutPhase::Headers,
            TimeoutPhase::IdleRead,
        ];
        tokio::time::advance(Duration::from_secs(1)).await;
        for (position, phase) in phases.iter().copied().enumerate() {
            for later in phases.iter().copied().skip(position) {
                for (first, second) in [(phase, later), (later, phase)] {
                    let budget = TimeoutBudget::default()
                        .with_phase(start, Some(Duration::from_secs(1)), first)?
                        .with_phase(start, Some(Duration::from_secs(1)), second)?;
                    let error = budget
                        .expired(Instant::now(), ErrorStage::Provider)
                        .ok_or_else(|| std::io::Error::other("deadline not expired"))?;
                    check(
                        error.timeout_phase() == Some(phase),
                        "tie priority changed with insertion order",
                    )?;
                    check(
                        error.kind()
                            == if phase == TimeoutPhase::Total {
                                ErrorKind::DeadlineExceeded
                            } else {
                                ErrorKind::TimedOut
                            },
                        "total must use DeadlineExceeded",
                    )?;
                }
            }
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn actual_deadline_precedes_priority_and_disabled_phase_keeps_parent() -> TestResult {
        let start = Instant::now();
        let budget = TimeoutBudget::total(start, Some(Duration::from_secs(20)))?
            .with_phase(start, Some(Duration::from_secs(3)), TimeoutPhase::IdleRead)?
            .with_phase(start, None, TimeoutPhase::Attempt)?;
        check(
            budget
                .expired(Instant::now(), ErrorStage::Provider)
                .is_none(),
            "budget expired early",
        )?;
        tokio::time::advance(Duration::from_secs(30)).await;
        let error = budget
            .expired(Instant::now(), ErrorStage::Provider)
            .ok_or_else(|| std::io::Error::other("deadline not expired"))?;
        check(
            error.timeout_phase() == Some(TimeoutPhase::IdleRead),
            "phase priority overrode earlier deadline",
        )?;
        check(
            error.context().stage == Some(ErrorStage::Receive),
            "idle stage must be Receive",
        )?;
        let unbounded = TimeoutBudget::total(start, None)?;
        check(
            unbounded.deadline().is_none() && unbounded.timer().is_none(),
            "disabled budget allocated timer",
        )?;
        check(
            unbounded
                .expired(Instant::now(), ErrorStage::Receive)
                .is_none(),
            "disabled budget expired",
        )
    }

    #[test]
    fn invalid_budget_is_rejected_without_panicking() -> TestResult {
        for duration in [Duration::ZERO, Duration::MAX] {
            check(
                TimeoutBudget::total(Instant::now(), Some(duration)).is_err(),
                "invalid budget accepted",
            )?;
        }
        Ok(())
    }
}

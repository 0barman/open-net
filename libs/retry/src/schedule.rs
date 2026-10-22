use super::{Backoff, Jitter, RetryConfigError};
use std::time::Duration;

/// Whether an operation inherits, disables, or replaces its client's retry policy.
#[derive(Clone, Debug, PartialEq)]
pub enum RetrySetting<P> {
    /// Resolve to the configured client default.
    Inherit,
    /// Execute at most once, independently of the client default.
    Disabled,
    /// Replace the entire client policy for this operation.
    Policy(P),
}

impl<P> Default for RetrySetting<P> {
    fn default() -> Self {
        Self::Inherit
    }
}

impl<P: Clone> RetrySetting<P> {
    /// Resolve the selection with protocol-specific default and disabled values.
    pub fn resolve(&self, default: &P, disabled: P) -> P {
        match self {
            Self::Inherit => default.clone(),
            Self::Disabled => disabled,
            Self::Policy(policy) => policy.clone(),
        }
    }
}

/// The result of a synchronous, read-only protocol or business classifier.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RetryDecision {
    /// Apply the protocol's configured built-in classification.
    #[default]
    Default,
    /// Request another attempt, subject to safety, cancellation and all budgets.
    Retry,
    /// Stop even if a built-in rule would otherwise retry.
    Stop,
}

/// Immutable, validated attempt and delay configuration shared by all transports.
///
/// Sharing or cloning a schedule never shares counters or a random stream.
/// Each executing operation creates its own private runtime state.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrySchedule {
    max_attempts: u32,
    backoff: Backoff,
    jitter: Jitter,
}

impl RetrySchedule {
    /// Start a builder with one immediate attempt and no jitter.
    pub fn builder() -> RetryScheduleBuilder {
        RetryScheduleBuilder::default()
    }

    /// Configure exactly one attempt, with no retry delay.
    pub fn no_retry() -> Self {
        Self {
            max_attempts: 1,
            backoff: Backoff::None,
            jitter: Jitter::None,
        }
    }

    /// Return the total attempt limit, including the initial attempt.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Return the built-in backoff rule.
    pub fn backoff(&self) -> &Backoff {
        &self.backoff
    }

    /// Return the jitter configuration.
    pub fn jitter(&self) -> Jitter {
        self.jitter
    }

    /// Return a deterministic delay before a one-based retry, without jitter.
    ///
    /// For a schedule of five attempts the valid ordinals are one through four.
    pub fn delay_for(&self, retry_ordinal: u32) -> Result<Duration, RetryConfigError> {
        if retry_ordinal == 0 || retry_ordinal >= self.max_attempts {
            return Err(RetryConfigError::new(
                "retry_ordinal",
                "must be between one and max_attempts - 1",
            ));
        }
        self.backoff.delay_for_validated(retry_ordinal)
    }

    pub(crate) fn sampled_delay(
        &self,
        retry_ordinal: u32,
        unit: f64,
    ) -> Result<Duration, RetryConfigError> {
        self.jitter
            .apply(self.delay_for(retry_ordinal)?, unit, &self.backoff)
    }
}

impl Default for RetrySchedule {
    fn default() -> Self {
        Self::no_retry()
    }
}

/// Builder for one validated retry schedule.
#[derive(Clone, Debug)]
pub struct RetryScheduleBuilder {
    max_attempts: u32,
    backoff: Backoff,
    jitter: Jitter,
}

impl Default for RetryScheduleBuilder {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff: Backoff::None,
            jitter: Jitter::None,
        }
    }
}

impl RetryScheduleBuilder {
    /// Set the total number of attempts, including the first. Zero is invalid.
    pub fn max_attempts(mut self, count: u32) -> Self {
        self.max_attempts = count;
        self
    }

    /// Replace the delay rule. A sequence requires exactly `max_attempts - 1` entries.
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Replace the optional delay randomization.
    pub fn jitter(mut self, jitter: Jitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// Validate all settings without allocating per-attempt runtime state.
    pub fn build(self) -> Result<RetrySchedule, RetryConfigError> {
        if self.max_attempts == 0 {
            return Err(RetryConfigError::new(
                "max_attempts",
                "must be at least one",
            ));
        }
        self.backoff.validate(self.max_attempts)?;
        self.jitter.validate(&self.backoff)?;
        Ok(RetrySchedule {
            max_attempts: self.max_attempts,
            backoff: self.backoff,
            jitter: self.jitter,
        })
    }
}

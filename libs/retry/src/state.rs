use super::{RetryConfigError, RetryContext, RetryReport, RetrySchedule, RetryStopReason};
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static NEXT_RANDOM_STREAM: AtomicU64 = AtomicU64::new(1);

/// One operation's mutable state; schedules never own or share this state.
#[derive(Debug)]
pub(crate) struct RetryState {
    schedule: RetrySchedule,
    attempts: u32,
    transport_sends: u64,
    random: u64,
    pending_delay: Option<Duration>,
    stop_reason: Option<RetryStopReason>,
}

#[derive(Debug)]
pub(crate) enum RetryStateError {
    Finished,
    AttemptsExhausted,
    AttemptNotStarted,
    TransportCountOverflow,
    Schedule(RetryConfigError),
}

impl fmt::Display for RetryStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Finished => formatter.write_str("retry operation has already finished"),
            Self::AttemptsExhausted => formatter.write_str("retry attempt budget is exhausted"),
            Self::AttemptNotStarted => formatter.write_str("no retry attempt has started"),
            Self::TransportCountOverflow => {
                formatter.write_str("transport send counter overflowed")
            }
            Self::Schedule(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for RetryStateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Schedule(error) => Some(error),
            _ => None,
        }
    }
}

impl From<RetryConfigError> for RetryStateError {
    fn from(error: RetryConfigError) -> Self {
        Self::Schedule(error)
    }
}

impl RetryState {
    pub(crate) fn new(schedule: RetrySchedule) -> Self {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(NEXT_RANDOM_STREAM.fetch_add(1, Ordering::Relaxed));
        Self::with_seed(schedule, hasher.finish())
    }

    pub(crate) fn with_seed(schedule: RetrySchedule, seed: u64) -> Self {
        Self {
            schedule,
            attempts: 0,
            transport_sends: 0,
            random: seed,
            pending_delay: None,
            stop_reason: None,
        }
    }

    pub(crate) fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Reserve exactly one attempt, including the initial attempt.
    pub(crate) fn begin_attempt(&mut self) -> Result<u32, RetryStateError> {
        if self.stop_reason.is_some() {
            return Err(RetryStateError::Finished);
        }
        if self.attempts >= self.schedule.max_attempts() {
            return Err(RetryStateError::AttemptsExhausted);
        }
        self.attempts = self
            .attempts
            .checked_add(1)
            .ok_or(RetryStateError::AttemptsExhausted)?;
        self.pending_delay = None;
        Ok(self.attempts)
    }

    pub(crate) fn can_retry(&self) -> bool {
        self.stop_reason.is_none()
            && self.attempts > 0
            && self.attempts < self.schedule.max_attempts()
    }

    /// Sample the next delay once per attempt. Repeated reads do not resample jitter.
    pub(crate) fn next_delay(&mut self) -> Result<Option<Duration>, RetryStateError> {
        if self.attempts == 0 {
            return Err(RetryStateError::AttemptNotStarted);
        }
        if !self.can_retry() {
            return Ok(None);
        }
        if let Some(delay) = self.pending_delay {
            return Ok(Some(delay));
        }
        let sample = self.next_unit();
        let delay = self.schedule.sampled_delay(self.attempts, sample)?;
        self.pending_delay = Some(delay);
        Ok(Some(delay))
    }

    pub(crate) fn record_transport_send(&mut self) -> Result<(), RetryStateError> {
        if self.stop_reason.is_some() {
            return Err(RetryStateError::Finished);
        }
        if self.attempts == 0 {
            return Err(RetryStateError::AttemptNotStarted);
        }
        self.transport_sends = self
            .transport_sends
            .checked_add(1)
            .ok_or(RetryStateError::TransportCountOverflow)?;
        Ok(())
    }

    /// Select a terminal reason once. Late cancellation cannot replace an existing result.
    pub(crate) fn finish(&mut self, reason: RetryStopReason) -> bool {
        if self.stop_reason.is_some() {
            return false;
        }
        self.stop_reason = Some(reason);
        self.pending_delay = None;
        true
    }

    pub(crate) fn report(&self, elapsed: Duration) -> RetryReport {
        RetryReport {
            attempts: self.attempts,
            retry_count: self.attempts.saturating_sub(1),
            transport_sends: self.transport_sends,
            elapsed,
            stop_reason: self.stop_reason,
        }
    }

    pub(crate) fn context(&self, elapsed: Duration, remaining: Option<Duration>) -> RetryContext {
        RetryContext {
            attempt: self.attempts,
            elapsed,
            remaining,
        }
    }

    fn next_unit(&mut self) -> f64 {
        // SplitMix64 is a delay randomizer, never a credential or request-ID source.
        self.random = self.random.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.random;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^= value >> 31;
        (value >> 11) as f64 / 9_007_199_254_740_992.0
    }
}

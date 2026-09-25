use crate::{NetError, Result};
use std::time::{Duration, Instant};

/// Automatic retry policy for one connection cycle; defaults to jittered exponential backoff.
#[derive(Clone, Debug)]
pub enum ReconnectPolicy {
    /// Do not retry failed connections or reconnect after termination.
    Disabled,
    /// Retry with jittered exponential backoff and count/elapsed-time limits.
    Backoff(
        /// Retry count, delay, and elapsed-time limits for the cycle.
        BackoffConfig,
    ),
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self::Backoff(BackoffConfig::default())
    }
}

impl ReconnectPolicy {
    /// Validates the selected retry policy.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Disabled => Ok(()),
            Self::Backoff(config) => config.validate(),
        }
    }
}

/// Full-jitter exponential backoff parameters; each wait is random from zero to its cap.
#[derive(Clone, Debug)]
pub struct BackoffConfig {
    /// Additional retries after the initial attempt (6 by default; zero disables retries).
    pub max_retries: usize,
    /// First retry's random delay cap (250 ms by default); later caps grow by powers of two.
    pub initial_delay: Duration,
    /// Maximum random delay cap (8 s by default), which must not be below the initial delay.
    pub max_delay: Duration,
    /// Maximum cycle elapsed time including attempts and waits (30 s default); `None` is unlimited.
    pub max_elapsed: Option<Duration>,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            max_retries: 6,
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(8),
            max_elapsed: Some(Duration::from_secs(30)),
        }
    }
}

impl BackoffConfig {
    /// Validates delay ordering and monotonic deadline representability.
    pub fn validate(&self) -> Result<()> {
        validate_duration(self.initial_delay, "reconnect.initial_delay")?;
        validate_duration(self.max_delay, "reconnect.max_delay")?;
        if let Some(max_elapsed) = self.max_elapsed {
            validate_duration(max_elapsed, "reconnect.max_elapsed")?;
        }
        if self.max_delay < self.initial_delay {
            return Err(NetError::config(
                "reconnect.max_delay",
                "must be greater than or equal to reconnect.initial_delay",
            ));
        }
        Ok(())
    }
}

fn validate_duration(value: Duration, field: &str) -> Result<()> {
    if value.is_zero() {
        return Err(NetError::config(field, "must be greater than zero"));
    }
    if Instant::now().checked_add(value).is_none() {
        return Err(NetError::config(
            field,
            "must fit a monotonic Instant deadline",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "reconnect_tests.rs"]
mod tests;

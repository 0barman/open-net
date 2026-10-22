use super::RetryConfigError;
use std::time::{Duration, Instant};

/// Built-in delay calculation between attempts. The initial attempt has no delay.
#[derive(Clone, Debug, PartialEq)]
pub enum Backoff {
    /// Retry immediately, while still respecting cancellation and deadlines.
    None,
    /// Use one fixed delay for every retry.
    Constant(Duration),
    /// Exactly one delay for each retry; the schedule validates its length.
    Sequence(Vec<Duration>),
    /// Multiply `initial` by `multiplier` for each retry, bounded by `max`.
    Exponential {
        initial: Duration,
        multiplier: f64,
        max: Duration,
    },
}

impl Backoff {
    /// Construct a fixed delay.
    pub fn constant(delay: Duration) -> Self {
        Self::Constant(delay)
    }

    /// Construct an explicit delay sequence. Validation occurs when building a schedule.
    pub fn sequence(delays: impl IntoIterator<Item = Duration>) -> Self {
        Self::Sequence(delays.into_iter().collect())
    }

    /// Construct an exponential delay. The multiplier must be finite and at least one.
    pub fn exponential(initial: Duration, multiplier: f64, max: Duration) -> Self {
        Self::Exponential {
            initial,
            multiplier,
            max,
        }
    }

    /// Calculate a one-based retry delay without jitter.
    ///
    /// Ordinal one is the delay before attempt two. Zero and sequence indices
    /// outside the configured sequence are errors, rather than implicit repeats.
    pub fn delay_for(&self, retry_ordinal: u32) -> Result<Duration, RetryConfigError> {
        if retry_ordinal == 0 {
            return Err(RetryConfigError::new(
                "retry_ordinal",
                "must be at least one",
            ));
        }
        self.validate_values()?;
        self.delay_for_validated(retry_ordinal)
    }

    pub(crate) fn delay_for_validated(
        &self,
        retry_ordinal: u32,
    ) -> Result<Duration, RetryConfigError> {
        if retry_ordinal == 0 {
            return Err(RetryConfigError::new(
                "retry_ordinal",
                "must be at least one",
            ));
        }
        let delay = match self {
            Self::None => Duration::ZERO,
            Self::Constant(delay) => *delay,
            Self::Sequence(delays) => {
                let index = usize::try_from(retry_ordinal - 1).map_err(|_| {
                    RetryConfigError::new("retry_ordinal", "cannot be represented on this target")
                })?;
                delays.get(index).copied().ok_or_else(|| {
                    RetryConfigError::new("retry_ordinal", "exceeds the explicit delay sequence")
                })?
            }
            Self::Exponential {
                initial,
                multiplier,
                max,
            } => exponential_delay(*initial, *multiplier, *max, retry_ordinal)?,
        };
        validate_duration(delay, "backoff.delay")?;
        Ok(delay)
    }

    pub(crate) fn validate(&self, max_attempts: u32) -> Result<(), RetryConfigError> {
        self.validate_values()?;
        if let Self::Sequence(delays) = self {
            let expected = max_attempts.saturating_sub(1);
            if u64::try_from(delays.len()).ok() != Some(u64::from(expected)) {
                return Err(RetryConfigError::new(
                    "backoff.sequence",
                    format!("requires {expected} delays, received {}", delays.len()),
                ));
            }
        }
        Ok(())
    }

    fn validate_values(&self) -> Result<(), RetryConfigError> {
        match self {
            Self::None => Ok(()),
            Self::Constant(delay) => validate_duration(*delay, "backoff.constant"),
            Self::Sequence(delays) => {
                for delay in delays {
                    validate_duration(*delay, "backoff.sequence")?;
                }
                Ok(())
            }
            Self::Exponential {
                initial,
                multiplier,
                max,
            } => {
                if !multiplier.is_finite() || *multiplier < 1.0 {
                    return Err(RetryConfigError::new(
                        "backoff.multiplier",
                        "must be finite and at least one",
                    ));
                }
                if max < initial {
                    return Err(RetryConfigError::new(
                        "backoff.max",
                        "must not be below the initial delay",
                    ));
                }
                validate_duration(*initial, "backoff.initial")?;
                validate_duration(*max, "backoff.max")
            }
        }
    }

    pub(crate) fn maximum_delay(&self) -> Duration {
        match self {
            Self::None => Duration::ZERO,
            Self::Constant(delay) => *delay,
            Self::Sequence(delays) => delays.iter().copied().max().map_or(Duration::ZERO, |v| v),
            Self::Exponential { max, .. } => *max,
        }
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::None
    }
}

/// Optional randomization of each operation's retry delays.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum Jitter {
    /// Use the exact backoff delay.
    #[default]
    None,
    /// Select a delay uniformly between zero and the backoff delay.
    Full,
    /// Scale the backoff by a uniform factor between `1-ratio` and `1+ratio`.
    /// Exponential schedules still apply their configured maximum.
    Proportional { ratio: f64 },
}

impl Jitter {
    pub(crate) fn validate(self, backoff: &Backoff) -> Result<(), RetryConfigError> {
        if let Self::Proportional { ratio } = self {
            if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
                return Err(RetryConfigError::new(
                    "jitter.ratio",
                    "must be finite and between zero and one",
                ));
            }
            if !matches!(backoff, Backoff::Exponential { .. }) {
                scale_duration(backoff.maximum_delay(), 1.0 + ratio, "jitter.delay")?;
            }
        }
        Ok(())
    }

    pub(crate) fn apply(
        self,
        delay: Duration,
        unit: f64,
        backoff: &Backoff,
    ) -> Result<Duration, RetryConfigError> {
        if !unit.is_finite() || !(0.0..=1.0).contains(&unit) {
            return Err(RetryConfigError::new(
                "jitter.sample",
                "must be between zero and one",
            ));
        }
        let factor = match self {
            Self::None => return Ok(delay),
            Self::Full => unit,
            Self::Proportional { ratio } => (1.0 - ratio) + 2.0 * ratio * unit,
        };
        let seconds = delay.as_secs_f64() * factor;
        if let Backoff::Exponential { max, .. } = backoff {
            if seconds >= max.as_secs_f64() {
                return Ok(*max);
            }
        }
        scale_duration(delay, factor, "jitter.delay")
    }
}

fn exponential_delay(
    initial: Duration,
    multiplier: f64,
    max: Duration,
    ordinal: u32,
) -> Result<Duration, RetryConfigError> {
    if ordinal == 1 || initial.is_zero() || multiplier == 1.0 || initial == max {
        return Ok(initial);
    }
    let exponent = f64::from(ordinal - 1);
    let initial_seconds = initial.as_secs_f64();
    let maximum_seconds = max.as_secs_f64();
    // Compare logarithms before exponentiation, so a huge valid ordinal never
    // overflows merely to discover that the configured cap has been reached.
    let capped_exponent = (maximum_seconds / initial_seconds).ln() / multiplier.ln();
    if exponent >= capped_exponent {
        return Ok(max);
    }
    let seconds = initial_seconds * multiplier.powf(exponent);
    if seconds >= maximum_seconds {
        return Ok(max);
    }
    duration_from_seconds(seconds, "backoff.exponential")
}

fn scale_duration(delay: Duration, factor: f64, field: &str) -> Result<Duration, RetryConfigError> {
    if factor == 1.0 {
        validate_duration(delay, field)?;
        return Ok(delay);
    }
    duration_from_seconds(delay.as_secs_f64() * factor, field)
}

fn duration_from_seconds(seconds: f64, field: &str) -> Result<Duration, RetryConfigError> {
    let delay = Duration::try_from_secs_f64(seconds)
        .map_err(|_| RetryConfigError::new(field, "duration arithmetic overflowed"))?;
    validate_duration(delay, field)?;
    Ok(delay)
}

fn validate_duration(delay: Duration, field: &str) -> Result<(), RetryConfigError> {
    if Instant::now().checked_add(delay).is_none() {
        return Err(RetryConfigError::new(
            field,
            "duration exceeds the monotonic clock range",
        ));
    }
    Ok(())
}

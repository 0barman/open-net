//! Protocol-independent retry configuration and operation state.

mod backoff;
mod error;
mod observation;
mod schedule;
mod state;

pub use backoff::{Backoff, Jitter};
pub use error::RetryConfigError;
pub use observation::{
    RetryContext, RetryEvent, RetryObserver, RetryObserverHandle, RetryReport, RetryStopReason,
};
pub use schedule::{RetryDecision, RetrySchedule, RetryScheduleBuilder, RetrySetting};
pub(crate) use state::{RetryState, RetryStateError};

#[cfg(test)]
mod tests;

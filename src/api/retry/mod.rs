//! Shared retry schedules, decisions and observations.
//!
//! This module is available without transport features. HTTP and WebSocket
//! policies compose a [`RetrySchedule`] with their own error classification and
//! replay-safety rules. A schedule does not execute or own an asynchronous task.

pub use crate::retry_core::{
    Backoff, Jitter, RetryConfigError, RetryContext, RetryDecision, RetryEvent, RetryObserver,
    RetryObserverHandle, RetryReport, RetrySchedule, RetryScheduleBuilder, RetrySetting,
    RetryStopReason,
};

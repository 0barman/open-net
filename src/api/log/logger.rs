use super::{LogListener, LogType};
use crate::common::log::logger::{self, SubscriptionState};
use std::io;
use std::sync::Arc;

const DEFAULT_QUEUE_CAPACITY: usize = 256;

/// Owns a single filtered log subscription. Dropping it stops accepting records,
/// discards queued records and wakes an idle callback thread without waiting for
/// an already-running callback. This handle may be shared or moved across threads.
///
/// Callbacks run sequentially on a dedicated thread. They may query the SDK,
/// replace/unsubscribe listeners or panic without blocking network workers. Logs
/// synchronously produced on the callback thread are suppressed to prevent loops.
/// Work explicitly spawned by a callback must avoid creating its own log loop.
#[must_use = "keep the subscription alive to continue receiving logs"]
pub struct LogSubscription {
    pub(crate) id: u64,
    pub(crate) state: Arc<SubscriptionState>,
}

impl LogSubscription {
    /// Total records discarded because this subscription's queue was full.
    pub fn dropped_count(&self) -> u64 {
        self.state.dropped_count()
    }
}

impl Drop for LogSubscription {
    fn drop(&mut self) {
        logger::unregister(self.id, &self.state);
    }
}

/// Callback-only logging. The library does not print or persist log records.
pub struct Logger;

impl Logger {
    /// Registers an independent subscription with a queue of 256 records.
    ///
    /// Only explicitly listed origins are accepted, including for overflow reports.
    /// An empty type list is invalid. The returned handle must remain alive.
    /// Registration can fail if the callback thread cannot be created.
    pub fn register_log_listener(
        listener: LogListener,
        log_types: &[LogType],
    ) -> io::Result<LogSubscription> {
        Self::register_log_listener_with_capacity(listener, log_types, DEFAULT_QUEUE_CAPACITY)
    }

    /// Registers a subscription with a bounded number of queued records.
    ///
    /// Producers never wait for queue capacity. Overflow increments
    /// [`LogSubscription::dropped_count`] and is reported after the consumer resumes
    /// as a synthetic `log_subscription_overflow-S` record using the first listed
    /// origin. Zero capacity and an empty type list return `InvalidInput`.
    pub fn register_log_listener_with_capacity(
        listener: LogListener,
        log_types: &[LogType],
        capacity: usize,
    ) -> io::Result<LogSubscription> {
        logger::register_log_listener_with_capacity(listener, log_types, capacity)
    }

    /// Clears all global subscriptions without waiting for callbacks in progress.
    pub fn clear_global_log_listener() {
        logger::clear_global_log_listener();
    }

    /// Used by macros to avoid evaluating arguments for disabled origins.
    #[doc(hidden)]
    pub fn is_enabled(log_type: LogType) -> bool {
        logger::is_enabled(log_type)
    }
}

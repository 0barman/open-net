use super::{LogLevel, LogListener, LogSubscriptionOptions, LogType, SharedLogListener};
use crate::common::log::logger::{self, SubscriptionState};
use std::io;
use std::sync::Arc;

const DEFAULT_QUEUE_CAPACITY: usize = 256;

/// Cumulative capacity-related drops for one subscription.
///
/// Checks are ordered: single-record size, queued-byte budget, then queue slots.
/// Each rejected record contributes to exactly one counter. Filtering, closure,
/// and suppressed overflow reports are not counted. Concurrent updates mean the
/// fields need not represent one atomic instant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LogDropStats {
    /// Records discarded because no queue slot was available.
    pub queue_full: u64,
    /// Records discarded because the queued/reserved byte budget was exhausted.
    pub byte_budget: u64,
    /// Records exceeding the configured single-record byte limit.
    pub oversized: u64,
}

/// Owns a single filtered log subscription. Dropping it stops accepting records,
/// wakes an idle callback thread without waiting for an already-running callback.
/// Pending records are discarded when the worker exits; a blocked callback can
/// retain its queue until it returns. This handle may be shared or moved across threads.
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

    /// Reads cumulative drops. `dropped_count()` remains equal to `queue_full`;
    /// it does not include byte-budget or single-record-size rejections.
    pub fn drop_stats(&self) -> LogDropStats {
        self.state.drop_stats()
    }
}

impl Drop for LogSubscription {
    fn drop(&mut self) {
        logger::unregister(self.id, &self.state);
    }
}

/// Callback-only logging. The library does not print or persist log records.
pub struct OpenNetLogger;

impl OpenNetLogger {
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
    /// as a synthetic `log_subscription_overflow` record using the first listed
    /// origin.
    ///
    /// Capacity must be in `1..=16_384`. Zero, larger capacities and an empty
    /// type list return [`io::ErrorKind::InvalidInput`] before allocating the
    /// queue or starting the callback thread. The limit counts queued records,
    /// not their payload bytes or the total memory used by all subscriptions.
    /// Registration can also fail if the callback thread cannot be created.
    pub fn register_log_listener_with_capacity(
        listener: LogListener,
        log_types: &[LogType],
        capacity: usize,
    ) -> io::Result<LogSubscription> {
        logger::register_log_listener_with_capacity(listener, log_types, capacity)
    }

    /// Registers a subscription sharing immutable records with other listeners.
    ///
    /// Only configured origins and levels are delivered. Exported log macros skip
    /// content parameter evaluation when no subscription needs the origin/level;
    /// another listener (including a legacy listener accepting every level) may
    /// still require construction. Concurrent subscription changes can occur
    /// after this check. Legacy listeners receive owned records at delivery time
    /// and can still require a deep copy.
    ///
    /// Producers never wait for queue space. Records exceeding a configured limit
    /// are dropped whole and counted by [`LogSubscription::drop_stats`]. A Debug
    /// `log_subscription_overflow` report can be delivered after a callback
    /// returns, using the first configured origin. It bypasses the queue but
    /// obeys the subscription's level and single-record limit. A suppressed report
    /// does not create another drop; counters remain available independently.
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] before allocating a queue or starting
    /// a worker for empty origins, a capacity outside `1..=16_384`, zero byte
    /// limits, or a single-record limit greater than the queued-byte limit.
    /// Registration can also fail if the callback thread cannot be created.
    ///
    /// ```
    /// use open_net::{LogLevel, LogSubscriptionOptions, LogType, OpenNetLogger};
    /// let subscription = OpenNetLogger::register_shared_log_listener(
    ///     Box::new(|record| eprintln!("{}: {}", record.tag, record.content)),
    ///     LogSubscriptionOptions::new(&[LogType::Engine, LogType::HTTP])
    ///         .with_max_level(LogLevel::Warn),
    /// )?;
    /// // Keep the handle alive while logging is needed.
    /// let oversized = subscription.drop_stats().oversized;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn register_shared_log_listener(
        listener: SharedLogListener,
        options: LogSubscriptionOptions,
    ) -> io::Result<LogSubscription> {
        logger::register_shared_log_listener(listener, options)
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

    /// Used by exported macros to check joint origin and severity interest.
    #[doc(hidden)]
    pub fn is_enabled_for(log_type: LogType, level: LogLevel) -> bool {
        logger::is_enabled_for(log_type, level)
    }
}

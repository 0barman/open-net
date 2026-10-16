use super::{LogLevel, LogType};

/// Filtering and queue budgets for a shared log subscription.
///
/// Defaults: all real levels (`Debug`), 256 queued records, 16 KiB per record,
/// and 1 MiB of queued/reserved payload bytes. These are policy defaults, not
/// measurements of an application's logging needs.
///
/// Payload bytes are the sum of `location.len()`, `tag.len()` and `content.len()`
/// in UTF-8. Each subscription accounts for its own logical bytes even when
/// records share storage. The budget includes brief reservations before enqueue,
/// but excludes dequeued records, user-retained records, allocation overhead and
/// formatting temporaries. It is not a process memory or allocation limit.
#[derive(Clone, Debug)]
pub struct LogSubscriptionOptions {
    pub(crate) log_types: Vec<LogType>,
    pub(crate) max_level: LogLevel,
    pub(crate) queue_capacity: usize,
    pub(crate) max_record_bytes: usize,
    pub(crate) max_queued_bytes: usize,
}

impl LogSubscriptionOptions {
    /// Selects explicit origins with the documented default levels and budgets.
    /// An empty origin list is rejected when registering the subscription.
    pub fn new(log_types: &[LogType]) -> Self {
        Self {
            log_types: log_types.to_vec(),
            max_level: LogLevel::Debug,
            queue_capacity: 256,
            max_record_bytes: 16 * 1024,
            max_queued_bytes: 1024 * 1024,
        }
    }

    /// Accepts real levels up to this value: `Warn` accepts Error and Warn,
    /// `Debug` accepts all real levels, and `None` disables this subscription.
    /// A record whose level is `None` is never accepted by a shared subscription.
    pub fn with_max_level(mut self, level: LogLevel) -> Self {
        self.max_level = level;
        self
    }

    /// Sets the number of waiting records, excluding the running callback.
    /// Registration rejects values outside `1..=16_384`.
    pub fn with_queue_capacity(mut self, records: usize) -> Self {
        self.queue_capacity = records;
        self
    }

    /// Sets the maximum logical payload bytes of one record, including synthetic
    /// overflow reports. Registration rejects zero or a value greater than the
    /// queued-byte budget. Oversized records are dropped whole, never truncated.
    /// The check happens after formatting and cannot bound parameter allocations.
    pub fn with_max_record_bytes(mut self, bytes: usize) -> Self {
        self.max_record_bytes = bytes;
        self
    }

    /// Sets the logical payload budget for queued records and pending enqueue
    /// reservations. Registration rejects zero or a value below the single-record
    /// limit. Bytes are released before invoking the callback, or when an enqueue
    /// fails or a queued record is destroyed.
    pub fn with_max_queued_bytes(mut self, bytes: usize) -> Self {
        self.max_queued_bytes = bytes;
        self
    }
}

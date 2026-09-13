use crate::common::log::listener::LogListener;
use crate::common::log::log_def::{format_tag, timestamp_millis, LogType};
use crate::common::log::log_info::LogInfo;
use crate::common::log::log_level::LogLevel;
use std::cell::Cell;
use std::collections::HashMap;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

const DEFAULT_QUEUE_CAPACITY: usize = 256;
static NEXT_SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);
static SUBSCRIPTIONS: OnceLock<RwLock<HashMap<u64, Weak<SubscriptionState>>>> = OnceLock::new();

thread_local! {
    // Covers the entire dedicated callback thread, including callback destruction.
    // Calling an instrumented SDK method from a callback must not feed logs back.
    static IN_LOG_CALLBACK: Cell<bool> = const { Cell::new(false) };
}

fn subscriptions() -> &'static RwLock<HashMap<u64, Weak<SubscriptionState>>> {
    SUBSCRIPTIONS.get_or_init(RwLock::default)
}

struct SubscriptionState {
    sender: Mutex<Option<SyncSender<LogInfo>>>,
    log_types: Vec<LogType>,
    active: AtomicBool,
    dropped: AtomicU64,
    unreported_dropped: AtomicU64,
}

impl SubscriptionState {
    fn close(&self) {
        self.active.store(false, Ordering::Release);
        // Disconnecting the sender wakes an idle worker. Never join user code.
        self.sender
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }

    fn accepts(&self, log_type: LogType) -> bool {
        self.active.load(Ordering::Acquire) && self.log_types.contains(&log_type)
    }

    fn enqueue(&self, record: LogInfo) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        let sender = self
            .sender
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if let Some(sender) = sender {
            if let Err(TrySendError::Full(_)) = sender.try_send(record) {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                self.unreported_dropped.fetch_add(1, Ordering::Release);
            }
        }
    }
}

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
    id: u64,
    state: Arc<SubscriptionState>,
}

impl LogSubscription {
    /// Total records discarded because this subscription's queue was full.
    pub fn dropped_count(&self) -> u64 {
        self.state.dropped.load(Ordering::Relaxed)
    }
}

impl Drop for LogSubscription {
    fn drop(&mut self) {
        subscriptions()
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.id);
        self.state.close();
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
        if capacity == 0 || log_types.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "log subscription needs nonzero capacity and at least one log type",
            ));
        }
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let state = Arc::new(SubscriptionState {
            sender: Mutex::new(Some(sender)),
            log_types: log_types.to_vec(),
            active: AtomicBool::new(true),
            dropped: AtomicU64::new(0),
            unreported_dropped: AtomicU64::new(0),
        });
        let worker_state = Arc::clone(&state);
        std::thread::Builder::new()
            .name("open-net-log".to_string())
            .spawn(move || {
                IN_LOG_CALLBACK.with(|active| active.set(true));
                while let Ok(record) = receiver.recv() {
                    if !worker_state.active.load(Ordering::Acquire) {
                        break;
                    }
                    let _ = catch_unwind(AssertUnwindSafe(|| listener(record)));
                    if !worker_state.active.load(Ordering::Acquire) {
                        break;
                    }
                    let count = worker_state.unreported_dropped.swap(0, Ordering::AcqRel);
                    if count != 0 {
                        let log_type = worker_state.log_types[0];
                        let record = LogInfo {
                            log_type,
                            location: String::new(),
                            level: LogLevel::Debug,
                            tag: format_tag(log_type, "log_subscription_overflow", "S"),
                            content: serde_json::json!({
                                "dropped": count,
                                "dropped_total": worker_state.dropped.load(Ordering::Relaxed),
                            })
                            .to_string(),
                            create_time: timestamp_millis(),
                        };
                        let _ = catch_unwind(AssertUnwindSafe(|| listener(record)));
                    }
                }
            })?;
        let id = NEXT_SUBSCRIPTION_ID.fetch_add(1, Ordering::Relaxed);
        subscriptions()
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(id, Arc::downgrade(&state));
        Ok(LogSubscription { id, state })
    }

    /// Clears all global subscriptions without waiting for callbacks in progress.
    pub fn clear_global_log_listener() {
        let old = {
            let mut subscriptions = subscriptions()
                .write()
                .unwrap_or_else(|error| error.into_inner());
            std::mem::take(&mut *subscriptions)
        };
        for state in old.into_values().filter_map(|state| state.upgrade()) {
            state.close();
        }
    }

    /// Used by macros to avoid evaluating arguments for disabled origins.
    #[doc(hidden)]
    pub fn is_enabled(log_type: LogType) -> bool {
        if IN_LOG_CALLBACK.with(Cell::get) {
            return false;
        }
        subscriptions()
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter_map(Weak::upgrade)
            .any(|state| state.accepts(log_type))
    }

    pub(crate) fn dispatch(record: LogInfo) {
        if IN_LOG_CALLBACK.with(Cell::get) {
            return;
        }
        let targets: Vec<_> = subscriptions()
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter_map(Weak::upgrade)
            .filter(|state| state.accepts(record.log_type))
            .collect();
        for target in targets {
            target.enqueue(record.clone());
        }
    }
}

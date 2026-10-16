use crate::api::log::{LogDropStats, LogSubscriptionOptions, SharedLogListener};
pub(crate) use crate::api::log::{LogSubscription, OpenNetLogger};
use crate::common::log::listener::LogListener;
use crate::common::log::log_def::{timestamp_millis, LogType};
use crate::common::log::log_info::LogInfo;
use crate::common::log::log_level::LogLevel;
use std::cell::Cell;
use std::collections::HashMap;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard, Weak};

// Bound the channel's eager allocation per subscription. Record payload bytes
// and the number of subscriptions require separate budgets.
const MAX_LOG_QUEUE_CAPACITY: usize = 16_384;

static NEXT_SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);
static SUBSCRIPTIONS: OnceLock<RwLock<HashMap<u64, Weak<SubscriptionState>>>> = OnceLock::new();
// A negative answer avoids the registry lock. A positive answer still requires
// exact origin filtering and checking the subscription's active state.
// Every publication happens while holding the registry write lock so a stale
// handle dropped after clear cannot overwrite a newer registration's state.
static MAY_HAVE_SUBSCRIPTIONS: AtomicBool = AtomicBool::new(false);

thread_local! {
    // Covers the entire dedicated callback thread, including callback destruction.
    // Calling an instrumented SDK method from a callback must not feed logs back.
    static IN_LOG_CALLBACK: Cell<bool> = const { Cell::new(false) };
    #[cfg(test)]
    static SUBSCRIPTION_READS: Cell<usize> = const { Cell::new(0) };
    #[cfg(test)]
    static FAIL_NEXT_WORKER_SPAWN: Cell<bool> = const { Cell::new(false) };
}

fn subscriptions() -> &'static RwLock<HashMap<u64, Weak<SubscriptionState>>> {
    SUBSCRIPTIONS.get_or_init(RwLock::default)
}

fn read_subscriptions() -> RwLockReadGuard<'static, HashMap<u64, Weak<SubscriptionState>>> {
    #[cfg(test)]
    SUBSCRIPTION_READS.with(|reads| reads.set(reads.get().saturating_add(1)));
    match subscriptions().read() {
        Ok(subscriptions) => subscriptions,
        Err(error) => error.into_inner(),
    }
}

fn spawn_log_worker(worker: impl FnOnce() + Send + 'static) -> io::Result<()> {
    #[cfg(test)]
    if FAIL_NEXT_WORKER_SPAWN.with(|fail| fail.replace(false)) {
        return Err(io::Error::other("injected log worker creation failure"));
    }
    std::thread::Builder::new()
        .name("open-net-log".to_string())
        .spawn(worker)
        .map(drop)
}

// Queue entries hold this separate allocation, never SubscriptionState. Holding
// the state here would create a state -> sender -> queued entry -> state cycle.
struct QueueBudget {
    used: AtomicUsize,
    limit: usize,
}

impl QueueBudget {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<QueuePermit> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .ok()?;
        Some(QueuePermit {
            budget: Arc::clone(self),
            bytes,
        })
    }
}

struct QueuePermit {
    budget: Arc<QueueBudget>,
    bytes: usize,
}

impl Drop for QueuePermit {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct QueuedRecord {
    record: Arc<LogInfo>,
    permit: Option<QueuePermit>,
}

impl QueuedRecord {
    fn into_record(self) -> Arc<LogInfo> {
        let Self { record, permit } = self;
        // The budget covers the queue, not the callback or application retention.
        drop(permit);
        record
    }
}

#[derive(Default)]
struct DropCounters {
    queue_full: AtomicU64,
    byte_budget: AtomicU64,
    oversized: AtomicU64,
}

impl DropCounters {
    fn snapshot(&self) -> LogDropStats {
        LogDropStats {
            queue_full: self.queue_full.load(Ordering::Relaxed),
            byte_budget: self.byte_budget.load(Ordering::Relaxed),
            oversized: self.oversized.load(Ordering::Relaxed),
        }
    }

    fn take(&self) -> LogDropStats {
        LogDropStats {
            queue_full: self.queue_full.swap(0, Ordering::AcqRel),
            byte_budget: self.byte_budget.swap(0, Ordering::AcqRel),
            oversized: self.oversized.swap(0, Ordering::AcqRel),
        }
    }

    fn increment(&self, reason: DropReason, ordering: Ordering) {
        let counter = match reason {
            DropReason::QueueFull => &self.queue_full,
            DropReason::ByteBudget => &self.byte_budget,
            DropReason::Oversized => &self.oversized,
        };
        counter.fetch_add(1, ordering);
    }
}

#[derive(Clone, Copy)]
enum DropReason {
    QueueFull,
    ByteBudget,
    Oversized,
}

enum Listener {
    Owned(LogListener),
    Shared(SharedLogListener),
}

impl Listener {
    fn invoke(&self, record: Arc<LogInfo>) {
        match self {
            Self::Owned(listener) => {
                listener(Arc::try_unwrap(record).unwrap_or_else(|record| (*record).clone()));
            }
            Self::Shared(listener) => listener(record),
        }
    }
}

fn record_bytes(record: &LogInfo) -> Option<usize> {
    record
        .location
        .len()
        .checked_add(record.tag.len())?
        .checked_add(record.content.len())
}

pub(crate) struct SubscriptionState {
    sender: Mutex<Option<SyncSender<QueuedRecord>>>,
    log_types: Vec<LogType>,
    // None preserves the owned API's historical acceptance of every severity,
    // including records with LogLevel::None sent through the hidden helper.
    max_level: Option<LogLevel>,
    max_record_bytes: Option<usize>,
    budget: Option<Arc<QueueBudget>>,
    active: AtomicBool,
    dropped: DropCounters,
    unreported_dropped: DropCounters,
}

impl SubscriptionState {
    pub(crate) fn dropped_count(&self) -> u64 {
        self.dropped.queue_full.load(Ordering::Relaxed)
    }

    pub(crate) fn drop_stats(&self) -> LogDropStats {
        self.dropped.snapshot()
    }

    fn close(&self) {
        // Disconnecting the sender wakes an idle worker. Never join user code.
        let mut sender = self
            .sender
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.active.store(false, Ordering::Release);
        sender.take();
    }

    fn accepts(&self, log_type: LogType) -> bool {
        self.active.load(Ordering::Acquire) && self.log_types.contains(&log_type)
    }

    fn accepts_level(&self, log_type: LogType, level: LogLevel) -> bool {
        self.accepts(log_type)
            && self
                .max_level
                .is_none_or(|max| max != LogLevel::None && level != LogLevel::None && level <= max)
    }

    fn record_drop(&self, reason: DropReason) {
        self.dropped.increment(reason, Ordering::Relaxed);
        self.unreported_dropped.increment(reason, Ordering::Release);
    }

    fn fits_record(&self, bytes: Option<usize>) -> bool {
        match self.max_record_bytes {
            Some(limit) => bytes.is_some_and(|bytes| bytes <= limit),
            None => true,
        }
    }

    fn enqueue(&self, record: &Arc<LogInfo>, bytes: Option<usize>) {
        let sender = self
            .sender
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(sender) = sender.as_ref() else {
            return;
        };
        if !self.accepts_level(record.log_type, record.level) {
            return;
        }
        // Admission and close share the sender lock. Only capacity failures of
        // an active subscription count, with one reason in this fixed order.
        if !self.fits_record(bytes) {
            self.record_drop(DropReason::Oversized);
            return;
        }
        let permit = match &self.budget {
            Some(budget) => {
                let Some(permit) = bytes.and_then(|bytes| budget.reserve(bytes)) else {
                    self.record_drop(DropReason::ByteBudget);
                    return;
                };
                Some(permit)
            }
            None => None,
        };
        let entry = QueuedRecord {
            record: Arc::clone(record),
            permit,
        };
        if let Err(TrySendError::Full(_)) = sender.try_send(entry) {
            self.record_drop(DropReason::QueueFull);
        }
        // Full and disconnected errors both drop the entry and its permit.
    }

    fn overflow_record(&self, log_type: LogType) -> Option<Arc<LogInfo>> {
        let recent = self.unreported_dropped.take();
        if recent == LogDropStats::default() || !self.accepts_level(log_type, LogLevel::Debug) {
            return None;
        }
        let total = self.drop_stats();
        let mut content = serde_json::json!({
            "dropped": recent.queue_full,
            "dropped_total": total.queue_full,
        });
        if self.max_level.is_some() {
            content["drop_stats"] = serde_json::json!({
                "queue_full": recent.queue_full,
                "byte_budget": recent.byte_budget,
                "oversized": recent.oversized,
            });
            content["drop_stats_total"] = serde_json::json!({
                "queue_full": total.queue_full,
                "byte_budget": total.byte_budget,
                "oversized": total.oversized,
            });
        }
        let record = LogInfo {
            log_type,
            location: String::new(),
            level: LogLevel::Debug,
            tag: "log_subscription_overflow".to_owned(),
            content: content.to_string(),
            create_time: timestamp_millis(),
        };
        // Reports are delivered directly after a callback, not queued. Suppress
        // oversized reports without recursively incrementing any drop counter.
        (self.fits_record(record_bytes(&record)) && self.accepts_level(log_type, LogLevel::Debug))
            .then(|| Arc::new(record))
    }
}

pub(crate) fn unregister(id: u64, state: &SubscriptionState) {
    let mut subscriptions = subscriptions()
        .write()
        .unwrap_or_else(|error| error.into_inner());
    subscriptions.remove(&id);
    MAY_HAVE_SUBSCRIPTIONS.store(!subscriptions.is_empty(), Ordering::Release);
    drop(subscriptions);
    state.close();
}

pub(crate) fn register_log_listener_with_capacity(
    listener: LogListener,
    log_types: &[LogType],
    capacity: usize,
) -> io::Result<LogSubscription> {
    register(
        Listener::Owned(listener),
        log_types.to_vec(),
        None,
        capacity,
        None,
        None,
    )
}

pub(crate) fn register_shared_log_listener(
    listener: SharedLogListener,
    options: LogSubscriptionOptions,
) -> io::Result<LogSubscription> {
    if options.max_record_bytes == 0
        || options.max_queued_bytes == 0
        || options.max_record_bytes > options.max_queued_bytes
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "log byte limits must be nonzero, with max_record_bytes <= max_queued_bytes",
        ));
    }
    register(
        Listener::Shared(listener),
        options.log_types,
        Some(options.max_level),
        options.queue_capacity,
        Some(options.max_record_bytes),
        Some(options.max_queued_bytes),
    )
}

fn register(
    listener: Listener,
    log_types: Vec<LogType>,
    max_level: Option<LogLevel>,
    capacity: usize,
    max_record_bytes: Option<usize>,
    max_queued_bytes: Option<usize>,
) -> io::Result<LogSubscription> {
    let overflow_log_type = log_types.first().copied().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "log subscription needs nonzero capacity and at least one log type",
        )
    })?;
    if !(1..=MAX_LOG_QUEUE_CAPACITY).contains(&capacity) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("log subscription capacity {capacity} is outside 1..={MAX_LOG_QUEUE_CAPACITY}"),
        ));
    }
    let (sender, receiver) = mpsc::sync_channel(capacity);
    let state = Arc::new(SubscriptionState {
        sender: Mutex::new(Some(sender)),
        log_types,
        max_level,
        max_record_bytes,
        budget: max_queued_bytes.map(|limit| {
            Arc::new(QueueBudget {
                used: AtomicUsize::new(0),
                limit,
            })
        }),
        active: AtomicBool::new(true),
        dropped: DropCounters::default(),
        unreported_dropped: DropCounters::default(),
    });
    let worker_state = Arc::clone(&state);
    spawn_log_worker(move || {
        IN_LOG_CALLBACK.with(|active| active.set(true));
        while let Ok(entry) = receiver.recv() {
            let record = entry.into_record();
            if !worker_state.active.load(Ordering::Acquire) {
                break;
            }
            let _ = catch_unwind(AssertUnwindSafe(|| listener.invoke(record)));
            if !worker_state.active.load(Ordering::Acquire) {
                break;
            }
            if let Some(record) = worker_state.overflow_record(overflow_log_type) {
                let _ = catch_unwind(AssertUnwindSafe(|| listener.invoke(record)));
            }
        }
    })?;
    let id = NEXT_SUBSCRIPTION_ID.fetch_add(1, Ordering::Relaxed);
    let mut subscriptions = subscriptions()
        .write()
        .unwrap_or_else(|error| error.into_inner());
    subscriptions.insert(id, Arc::downgrade(&state));
    MAY_HAVE_SUBSCRIPTIONS.store(!subscriptions.is_empty(), Ordering::Release);
    Ok(LogSubscription { id, state })
}

pub(crate) fn clear_global_log_listener() {
    let old = {
        let mut subscriptions = subscriptions()
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let old = std::mem::take(&mut *subscriptions);
        MAY_HAVE_SUBSCRIPTIONS.store(!subscriptions.is_empty(), Ordering::Release);
        old
    };
    for state in old.into_values().filter_map(|state| state.upgrade()) {
        state.close();
    }
}

pub(crate) fn is_enabled(log_type: LogType) -> bool {
    if IN_LOG_CALLBACK.with(Cell::get) || !MAY_HAVE_SUBSCRIPTIONS.load(Ordering::Acquire) {
        return false;
    }
    read_subscriptions()
        .values()
        .filter_map(Weak::upgrade)
        .any(|state| state.accepts(log_type))
}

pub(crate) fn is_enabled_for(log_type: LogType, level: LogLevel) -> bool {
    if IN_LOG_CALLBACK.with(Cell::get) || !MAY_HAVE_SUBSCRIPTIONS.load(Ordering::Acquire) {
        return false;
    }
    read_subscriptions()
        .values()
        .filter_map(Weak::upgrade)
        .any(|state| state.accepts_level(log_type, level))
}

pub(crate) fn dispatch(record: LogInfo) {
    if IN_LOG_CALLBACK.with(Cell::get) || !MAY_HAVE_SUBSCRIPTIONS.load(Ordering::Acquire) {
        return;
    }
    let targets: Vec<_> = read_subscriptions()
        .values()
        .filter_map(Weak::upgrade)
        .filter(|state| state.accepts_level(record.log_type, record.level))
        .collect();
    if targets.is_empty() {
        return;
    }
    let bytes = record_bytes(&record);
    let record = Arc::new(record);
    for target in targets {
        target.enqueue(&record, bytes);
    }
}

#[cfg(test)]
#[path = "fast_path_tests.rs"]
mod fast_path_tests;

#[cfg(test)]
#[path = "budget_tests.rs"]
mod budget_tests;

pub(crate) use crate::api::log::{LogSubscription, Logger};
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
use std::sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard, Weak};

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

pub(crate) struct SubscriptionState {
    sender: Mutex<Option<SyncSender<LogInfo>>>,
    log_types: Vec<LogType>,
    active: AtomicBool,
    dropped: AtomicU64,
    unreported_dropped: AtomicU64,
}

impl SubscriptionState {
    pub(crate) fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

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
    let overflow_log_type = log_types.first().copied().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "log subscription needs nonzero capacity and at least one log type",
        )
    })?;
    if capacity == 0 {
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
    spawn_log_worker(move || {
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
                let log_type = overflow_log_type;
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

pub(crate) fn dispatch(record: LogInfo) {
    if IN_LOG_CALLBACK.with(Cell::get) || !MAY_HAVE_SUBSCRIPTIONS.load(Ordering::Acquire) {
        return;
    }
    let targets: Vec<_> = read_subscriptions()
        .values()
        .filter_map(Weak::upgrade)
        .filter(|state| state.accepts(record.log_type))
        .collect();
    for target in targets {
        target.enqueue(record.clone());
    }
}

#[cfg(test)]
#[path = "fast_path_tests.rs"]
mod fast_path_tests;

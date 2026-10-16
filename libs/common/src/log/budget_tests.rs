use super::*;
use std::sync::mpsc::Receiver;
use std::sync::Barrier;

fn record(content: &str, level: LogLevel) -> Arc<LogInfo> {
    Arc::new(LogInfo {
        log_type: LogType::Engine,
        location: String::new(),
        level,
        tag: String::new(),
        content: content.to_owned(),
        create_time: 0,
    })
}

fn subscription(
    capacity: usize,
    max_record_bytes: usize,
    max_queued_bytes: usize,
    max_level: LogLevel,
) -> (SubscriptionState, Receiver<QueuedRecord>) {
    let (sender, receiver) = mpsc::sync_channel(capacity);
    (
        SubscriptionState {
            sender: Mutex::new(Some(sender)),
            log_types: vec![LogType::Engine],
            max_level: Some(max_level),
            max_record_bytes: Some(max_record_bytes),
            budget: Some(Arc::new(QueueBudget {
                used: AtomicUsize::new(0),
                limit: max_queued_bytes,
            })),
            active: AtomicBool::new(true),
            dropped: DropCounters::default(),
            unreported_dropped: DropCounters::default(),
        },
        receiver,
    )
}

fn queued_bytes(state: &SubscriptionState) -> usize {
    state.budget.as_ref().unwrap().used.load(Ordering::Acquire)
}

#[test]
fn concurrent_reservations_stay_bounded_and_release_exactly_once() {
    let budget = Arc::new(QueueBudget {
        used: AtomicUsize::new(0),
        limit: 21,
    });
    let barrier = Arc::new(Barrier::new(8));
    let violated = Arc::new(AtomicBool::new(false));
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let budget = Arc::clone(&budget);
            let barrier = Arc::clone(&barrier);
            let violated = Arc::clone(&violated);
            scope.spawn(move || {
                for _ in 0..64 {
                    let permit = budget.reserve(7);
                    barrier.wait();
                    if budget.used.load(Ordering::Acquire) != 21 {
                        violated.store(true, Ordering::Relaxed);
                    }
                    barrier.wait();
                    drop(permit);
                    barrier.wait();
                    if budget.used.load(Ordering::Acquire) != 0 {
                        violated.store(true, Ordering::Relaxed);
                    }
                    barrier.wait();
                }
            });
        }
    });
    assert!(!violated.load(Ordering::Relaxed));
    assert_eq!(budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn byte_reservation_uses_checked_arithmetic_and_utf8_lengths() {
    let budget = Arc::new(QueueBudget {
        used: AtomicUsize::new(0),
        limit: usize::MAX,
    });
    let permit = budget.reserve(usize::MAX).unwrap();
    assert!(budget.reserve(1).is_none());
    assert_eq!(budget.used.load(Ordering::Acquire), usize::MAX);
    drop(permit);
    assert_eq!(budget.used.load(Ordering::Acquire), 0);

    let record = LogInfo {
        log_type: LogType::Engine,
        location: "位置".to_owned(),
        level: LogLevel::Info,
        tag: "tag".to_owned(),
        content: "日志".to_owned(),
        create_time: 0,
    };
    assert_eq!(record_bytes(&record), Some(15));
}

#[test]
fn oversized_precedes_byte_budget_and_byte_budget_precedes_queue_full() {
    let (state, receiver) = subscription(1, 8, 8, LogLevel::Debug);
    let full = record("12345678", LogLevel::Info);
    state.enqueue(&full, record_bytes(&full));
    assert_eq!(queued_bytes(&state), 8);
    let oversized = record("123456789", LogLevel::Info);
    state.enqueue(&oversized, record_bytes(&oversized));
    let small = record("1", LogLevel::Info);
    state.enqueue(&small, record_bytes(&small));
    assert_eq!(
        state.drop_stats(),
        LogDropStats {
            queue_full: 0,
            byte_budget: 1,
            oversized: 1,
        }
    );
    assert_eq!(state.dropped_count(), 0);
    assert_eq!(queued_bytes(&state), 8);
    let retained = receiver.try_recv().unwrap().into_record();
    assert_eq!(queued_bytes(&state), 0);
    assert!(Arc::ptr_eq(&retained, &full));
    // Application retention no longer occupies the subscription's queue budget.
    state.enqueue(&full, record_bytes(&full));
    assert_eq!(queued_bytes(&state), 8);
}

#[test]
fn full_queue_and_disconnection_return_reserved_bytes() {
    let (state, receiver) = subscription(1, 8, 16, LogLevel::Debug);
    let record = record("12345678", LogLevel::Info);
    state.enqueue(&record, record_bytes(&record));
    state.enqueue(&record, record_bytes(&record));
    assert_eq!(state.dropped_count(), 1);
    assert_eq!(queued_bytes(&state), 8);
    drop(receiver);
    assert_eq!(queued_bytes(&state), 0);
    state.enqueue(&record, record_bytes(&record));
    assert_eq!(queued_bytes(&state), 0);
    assert_eq!(state.dropped_count(), 1);
}

#[test]
fn close_stops_admission_and_receiver_destruction_releases_backlog() {
    let (state, receiver) = subscription(2, 8, 16, LogLevel::Debug);
    let record = record("12345678", LogLevel::Info);
    state.enqueue(&record, record_bytes(&record));
    state.enqueue(&record, record_bytes(&record));
    state.close();
    let huge = record_bytes(&record).unwrap() + 1;
    state.enqueue(&record, Some(huge));
    assert_eq!(state.drop_stats(), LogDropStats::default());
    // An already-running callback can delay receiver destruction. The queued
    // entries continue to own their permits until that receiver is released.
    assert_eq!(queued_bytes(&state), 16);
    drop(receiver);
    assert_eq!(queued_bytes(&state), 0);
}

#[test]
fn permits_do_not_retain_subscription_state_or_records_after_receiver_drop() {
    let (state, receiver) = subscription(2, 8, 16, LogLevel::Debug);
    let state = Arc::new(state);
    let weak_state = Arc::downgrade(&state);
    let weak_budget = Arc::downgrade(state.budget.as_ref().unwrap());
    let record = record("payload", LogLevel::Info);
    let weak_record = Arc::downgrade(&record);
    state.enqueue(&record, record_bytes(&record));
    drop(record);
    drop(state);
    assert!(weak_state.upgrade().is_none());
    assert!(weak_budget.upgrade().is_some());
    assert!(weak_record.upgrade().is_some());
    drop(receiver);
    assert!(weak_budget.upgrade().is_none());
    assert!(weak_record.upgrade().is_none());
}

#[test]
fn subscriptions_share_payload_but_charge_independent_queue_budgets() {
    let (first, first_receiver) = subscription(1, 8, 8, LogLevel::Debug);
    let (second, second_receiver) = subscription(1, 8, 8, LogLevel::Debug);
    let record = record("12345678", LogLevel::Info);
    first.enqueue(&record, record_bytes(&record));
    second.enqueue(&record, record_bytes(&record));
    assert_eq!(queued_bytes(&first), 8);
    assert_eq!(queued_bytes(&second), 8);
    let first_record = first_receiver.try_recv().unwrap().into_record();
    assert_eq!(queued_bytes(&first), 0);
    assert_eq!(queued_bytes(&second), 8);
    let second_record = second_receiver.try_recv().unwrap().into_record();
    assert!(Arc::ptr_eq(&first_record, &second_record));
    assert!(Arc::ptr_eq(&first_record, &record));
    assert_eq!(queued_bytes(&second), 0);
}

#[test]
fn shared_level_filter_disables_none_and_legacy_preserves_all_levels() {
    let (mut state, receiver) = subscription(4, 16, 64, LogLevel::Warn);
    assert!(state.accepts_level(LogType::Engine, LogLevel::Error));
    assert!(state.accepts_level(LogType::Engine, LogLevel::Warn));
    assert!(!state.accepts_level(LogType::Engine, LogLevel::None));
    assert!(!state.accepts_level(LogType::Engine, LogLevel::Key));
    assert!(!state.accepts_level(LogType::HTTP, LogLevel::Error));
    let record = record("debug", LogLevel::Debug);
    state.enqueue(&record, record_bytes(&record));
    assert!(receiver.try_recv().is_err());
    assert_eq!(state.drop_stats(), LogDropStats::default());
    state.max_level = Some(LogLevel::None);
    assert!(state.accepts(LogType::Engine));
    assert!(!state.accepts_level(LogType::Engine, LogLevel::Error));
    state.max_level = None;
    for level in [LogLevel::None, LogLevel::Error, LogLevel::Debug] {
        assert!(state.accepts_level(LogType::Engine, level));
    }
}

#[test]
fn owned_delivery_reuses_unique_strings_and_clones_shared_strings() {
    let (sender, receiver) = mpsc::channel();
    let listener = Listener::Owned(Box::new(move |record| sender.send(record).unwrap()));
    let unique = record("owned payload", LogLevel::Info);
    let original_ptr = unique.content.as_ptr();
    listener.invoke(unique);
    let owned = receiver.try_recv().unwrap();
    assert_eq!(owned.content.as_ptr(), original_ptr);

    let shared = record("shared payload", LogLevel::Info);
    listener.invoke(Arc::clone(&shared));
    let owned = receiver.try_recv().unwrap();
    assert_eq!(owned.content, shared.content);
    assert_ne!(owned.content.as_ptr(), shared.content.as_ptr());
}

#[test]
fn overflow_reports_preserve_legacy_counts_and_include_shared_reasons() {
    let (mut state, _receiver) = subscription(1, 1024, 1024, LogLevel::Debug);
    state.record_drop(DropReason::QueueFull);
    state.record_drop(DropReason::ByteBudget);
    state.record_drop(DropReason::Oversized);
    let report = state.overflow_record(LogType::Engine).unwrap();
    let content: serde_json::Value = serde_json::from_str(&report.content).unwrap();
    assert_eq!(content["dropped"], 1);
    assert_eq!(content["dropped_total"], 1);
    assert_eq!(content["drop_stats"]["byte_budget"], 1);
    assert_eq!(content["drop_stats_total"]["oversized"], 1);
    assert!(state.overflow_record(LogType::Engine).is_none());
    assert_eq!(state.dropped_count(), 1);

    state.max_level = None;
    state.max_record_bytes = None;
    state.record_drop(DropReason::QueueFull);
    let report = state.overflow_record(LogType::Engine).unwrap();
    let content: serde_json::Value = serde_json::from_str(&report.content).unwrap();
    assert_eq!(content.as_object().unwrap().len(), 2);
    assert_eq!(content["dropped"], 1);
    assert_eq!(content["dropped_total"], 2);
}

#[test]
fn filtered_or_oversized_overflow_reports_do_not_add_drops() {
    for (max_level, max_bytes, origin, close) in [
        (LogLevel::Warn, 1024, LogType::Engine, false),
        (LogLevel::Debug, 1, LogType::Engine, false),
        (LogLevel::Debug, 1024, LogType::HTTP, false),
        (LogLevel::Debug, 1024, LogType::Engine, true),
    ] {
        let (state, _receiver) = subscription(1, max_bytes, max_bytes, max_level);
        state.record_drop(DropReason::ByteBudget);
        if close {
            state.close();
        }
        assert!(state.overflow_record(origin).is_none());
        assert_eq!(
            state.drop_stats(),
            LogDropStats {
                byte_budget: 1,
                ..LogDropStats::default()
            }
        );
        assert!(state.overflow_record(origin).is_none());
    }
}

#[test]
fn invalid_options_fail_before_worker_creation() {
    for (types, capacity, max_record, max_queued) in [
        (vec![], 1, 1, 1),
        (vec![LogType::Engine], 0, 1, 1),
        (vec![LogType::Engine], 16_385, 1, 1),
        (vec![LogType::Engine], 1, 0, 1),
        (vec![LogType::Engine], 1, 1, 0),
        (vec![LogType::Engine], 1, 2, 1),
    ] {
        FAIL_NEXT_WORKER_SPAWN.with(|fail| fail.set(true));
        let result = register_shared_log_listener(
            Box::new(|_| {}),
            LogSubscriptionOptions {
                log_types: types,
                max_level: LogLevel::Debug,
                queue_capacity: capacity,
                max_record_bytes: max_record,
                max_queued_bytes: max_queued,
            },
        );
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
        assert!(FAIL_NEXT_WORKER_SPAWN.with(|fail| fail.replace(false)));
    }
}

#[test]
fn failed_worker_creation_releases_shared_callback() {
    struct DropMarker(Arc<AtomicBool>);
    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let marker = DropMarker(Arc::clone(&dropped));
    FAIL_NEXT_WORKER_SPAWN.with(|fail| fail.set(true));
    let result = register_shared_log_listener(
        Box::new(move |_| {
            let _keep_alive = &marker;
        }),
        LogSubscriptionOptions {
            log_types: vec![LogType::Engine],
            max_level: LogLevel::Debug,
            queue_capacity: 1,
            max_record_bytes: 8,
            max_queued_bytes: 8,
        },
    );
    assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::Other));
    assert!(dropped.load(Ordering::Acquire));
}

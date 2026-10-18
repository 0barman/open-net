use open_net::{
    log_e, log_r, log_s, log_t, LogInfo, LogLevel, LogSubscription, LogType, OpenNetLogger,
};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

static TEST_LOCK: Mutex<()> = Mutex::new(());
const WAIT: Duration = Duration::from_secs(3);

fn receive(receiver: &Receiver<LogInfo>) -> LogInfo {
    receiver
        .recv_timeout(WAIT)
        .expect("log callback made progress")
}
fn subscribe(types: &[LogType]) -> (LogSubscription, Receiver<LogInfo>) {
    let (sender, receiver) = mpsc::channel();
    let subscription = OpenNetLogger::register_log_listener(
        Box::new(move |record| {
            let _ = sender.send(record);
        }),
        types,
    )
    .unwrap();
    (subscription, receiver)
}
struct NotifyDrop(Sender<()>);
impl Drop for NotifyDrop {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[test]
fn typed_filter_ignores_spoofed_tags_and_preserves_record_fields() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (_subscription, records) = subscribe(&[LogType::WSC, LogType::Common]);
    log_t!(LogType::HTTP; "O_WSC-spoof");
    log_t!(LogType::WSS; "O_WSC-spoof");
    log_t!(LogType::Database; "O_WSC-spoof");
    log_t!(LogType::None; "O_WSC-spoof");
    log_t!("O_WSC-spoof");
    log_t!(LogType::WSC; "connect", "a|b", 1, "hello");
    log_e!(LogType::Common; "invoke", "error", "failed");
    log_s!(LogType::WSC; "ready");
    let entry = receive(&records);
    assert_eq!(entry.log_type, LogType::WSC);
    assert_eq!(entry.level, LogLevel::Info);
    assert_eq!(entry.tag, "connect");
    assert_eq!(entry.content, r#"{"a":1,"b":"hello"}"#);
    assert!(entry.location.contains("tests.rs:"));
    assert!(entry.create_time > 0);
    let error = receive(&records);
    assert_eq!(error.log_type, LogType::Common);
    assert_eq!(error.level, LogLevel::Error);
    assert_eq!(error.tag, "invoke");
    let state = receive(&records);
    assert_eq!(state.tag, "ready");
    assert_eq!(state.level, LogLevel::Debug);
    assert_eq!(state.content, "{}");
    assert!(records.try_recv().is_err());
}

#[test]
fn macros_skip_disabled_arguments_and_evaluate_enabled_arguments_once() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let calls = AtomicUsize::new(0);
    log_t!(LogType::WSC; { calls.fetch_add(1, AtomicOrdering::SeqCst); "disabled" }, "value", calls.fetch_add(1, AtomicOrdering::SeqCst));
    assert_eq!(calls.load(AtomicOrdering::SeqCst), 0);
    let (_common, _common_records) = subscribe(&[LogType::Common]);
    log_e!(LogType::HTTP; "disabled", "value", calls.fetch_add(1, AtomicOrdering::SeqCst));
    assert_eq!(calls.load(AtomicOrdering::SeqCst), 0);
    let (_enabled, records) = subscribe(&[LogType::WSC, LogType::Engine]);
    log_t!(LogType::WSC; { calls.fetch_add(1, AtomicOrdering::SeqCst); "enabled" }, { calls.fetch_add(1, AtomicOrdering::SeqCst); "value" }, calls.fetch_add(1, AtomicOrdering::SeqCst));
    assert_eq!(calls.load(AtomicOrdering::SeqCst), 3);
    assert_eq!(receive(&records).tag, "enabled");
    log_r!("legacy", "value", 7,);
    let legacy = receive(&records);
    assert_eq!(legacy.log_type, LogType::Engine);
    assert_eq!(legacy.tag, "legacy");
    assert_eq!(legacy.level, LogLevel::Debug);
}

#[test]
fn explicit_state_levels_preserve_legacy_syntax_and_lazy_evaluation() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let types = AtomicUsize::new(0);
    let levels = AtomicUsize::new(0);
    let payloads = AtomicUsize::new(0);
    let emit = |origin, level| {
        log_s!(
            { types.fetch_add(1, AtomicOrdering::SeqCst); origin },
            { levels.fetch_add(1, AtomicOrdering::SeqCst); level };
            { payloads.fetch_add(1, AtomicOrdering::SeqCst); "explicit_state" },
            { payloads.fetch_add(1, AtomicOrdering::SeqCst); "value" },
            { payloads.fetch_add(1, AtomicOrdering::SeqCst); 42 },
        );
    };
    emit(LogType::WSC, LogLevel::Key);
    assert_eq!(types.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(levels.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(payloads.load(AtomicOrdering::SeqCst), 0);

    let (sender, records) = mpsc::channel();
    let subscription = OpenNetLogger::register_shared_log_listener(
        Box::new(move |record| {
            let _ = sender.send(record);
        }),
        open_net::LogSubscriptionOptions::new(&[LogType::WSC]).with_max_level(LogLevel::Key),
    )
    .unwrap();
    emit(LogType::HTTP, LogLevel::Key);
    assert_eq!(levels.load(AtomicOrdering::SeqCst), 0);
    emit(LogType::WSC, LogLevel::Debug);
    assert_eq!(levels.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(payloads.load(AtomicOrdering::SeqCst), 0);
    emit(LogType::WSC, LogLevel::Key);
    assert_eq!(types.load(AtomicOrdering::SeqCst), 4);
    assert_eq!(levels.load(AtomicOrdering::SeqCst), 2);
    assert_eq!(payloads.load(AtomicOrdering::SeqCst), 3);
    let record = records.recv_timeout(WAIT).unwrap();
    assert_eq!(record.level, LogLevel::Key);
    assert_eq!(record.content, r#"{"value":42}"#);
    log_s!(LogType::WSC, LogLevel::Key; "key_without_fields",);
    assert_eq!(records.recv_timeout(WAIT).unwrap().content, "{}");
    log_e!(LogType::WSC; "error_is_included",);
    assert_eq!(records.recv_timeout(WAIT).unwrap().level, LogLevel::Error);
    log_s!(LogType::WSC; "debug_is_filtered",);
    assert!(records.try_recv().is_err());
    drop(subscription);

    let (_legacy, records) = subscribe(&[LogType::WSC, LogType::Engine]);
    log_s!("legacy_without_fields",);
    log_s!("legacy_with_fields", "value", 7,);
    log_s!(LogType::WSC; "typed_without_fields",);
    log_s!(LogType::WSC; "typed_with_fields", "value", 8,);
    for (tag, origin, content) in [
        ("legacy_without_fields", LogType::Engine, "{}"),
        ("legacy_with_fields", LogType::Engine, r#"{"value":7}"#),
        ("typed_without_fields", LogType::WSC, "{}"),
        ("typed_with_fields", LogType::WSC, r#"{"value":8}"#),
    ] {
        let record = receive(&records);
        assert_eq!(
            (
                record.tag.as_str(),
                record.log_type,
                record.level,
                record.content.as_str()
            ),
            (tag, origin, LogLevel::Debug, content)
        );
    }
}

#[test]
fn subscriptions_are_independent_and_dropping_releases_idle_callback() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (first, first_records) = subscribe(&[LogType::WSC]);
    let (_second, second_records) = subscribe(&[LogType::WSC]);
    log_t!(LogType::WSC; "both");
    assert_eq!(receive(&first_records).tag, "both");
    assert_eq!(receive(&second_records).tag, "both");
    drop(first);
    let (_replacement, replacement_records) = subscribe(&[LogType::WSC]);
    log_s!(LogType::WSC; "after_replacement");
    assert_eq!(receive(&second_records).tag, "after_replacement");
    assert_eq!(receive(&replacement_records).tag, "after_replacement");
    assert!(first_records.recv_timeout(WAIT).is_err());
    let (dropped_sender, dropped_receiver) = mpsc::channel();
    let marker = NotifyDrop(dropped_sender);
    let idle = OpenNetLogger::register_log_listener(
        Box::new(move |_| {
            let _keep_alive = &marker;
        }),
        &[LogType::HTTP],
    )
    .unwrap();
    drop(idle);
    dropped_receiver
        .recv_timeout(WAIT)
        .expect("idle worker released its callback");
}

#[test]
fn callback_can_unsubscribe_itself_and_recursive_logs_are_suppressed() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let holder = Arc::new(Mutex::new(None::<LogSubscription>));
    let callback_holder = Arc::clone(&holder);
    let evaluated = Arc::new(AtomicUsize::new(0));
    let callback_evaluated = Arc::clone(&evaluated);
    let (done_sender, done_receiver) = mpsc::channel();
    let (_other, other_records) = subscribe(&[LogType::WSC]);
    let subscription = OpenNetLogger::register_log_listener(Box::new(move |_| {
        log_t!(LogType::WSC; "recursive", "value", callback_evaluated.fetch_add(1, AtomicOrdering::SeqCst));
        let old = callback_holder.lock().unwrap().take();
        drop(old);
        done_sender.send(()).unwrap();
    }), &[LogType::WSC]).unwrap();
    *holder.lock().unwrap() = Some(subscription);
    log_t!(LogType::WSC; "first");
    done_receiver.recv_timeout(WAIT).unwrap();
    assert_eq!(evaluated.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(receive(&other_records).tag, "first");
    log_t!(LogType::WSC; "second");
    assert_eq!(receive(&other_records).tag, "second");
    assert!(holder.lock().unwrap().is_none());
}

#[test]
fn callback_panics_do_not_stop_later_delivery() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (sender, receiver) = mpsc::channel();
    let _subscription = OpenNetLogger::register_log_listener(
        Box::new(move |record| {
            let should_panic = record.tag == "panic";
            sender.send(record).unwrap();
            if should_panic {
                panic!("test listener panic");
            }
        }),
        &[LogType::WSC],
    )
    .unwrap();
    log_t!(LogType::WSC; "panic");
    assert_eq!(receive(&receiver).tag, "panic");
    log_t!(LogType::WSC; "after_panic");
    assert_eq!(receive(&receiver).tag, "after_panic");
}

#[test]
fn full_queue_is_observable_and_slow_listener_does_not_block_other_listeners() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let release_receiver = Mutex::new(release_receiver);
    let (sender, receiver) = mpsc::channel();
    let slow = OpenNetLogger::register_log_listener_with_capacity(
        Box::new(move |record| {
            if record.tag == "block" {
                entered_sender.send(()).unwrap();
                release_receiver.lock().unwrap().recv_timeout(WAIT).unwrap();
            }
            sender.send(record).unwrap();
        }),
        &[LogType::WSC],
        1,
    )
    .unwrap();
    let (_fast, fast_records) = subscribe(&[LogType::WSC]);
    log_t!(LogType::WSC; "block");
    entered_receiver.recv_timeout(WAIT).unwrap();
    log_t!(LogType::WSC; "queued");
    log_t!(LogType::WSC; "overflow_one");
    log_t!(LogType::WSC; "overflow_two");
    assert_eq!(slow.dropped_count(), 2);
    for name in ["block", "queued", "overflow_one", "overflow_two"] {
        assert_eq!(receive(&fast_records).tag, name);
    }
    release_sender.send(()).unwrap();
    assert_eq!(receive(&receiver).tag, "block");
    let report = receive(&receiver);
    assert_eq!(report.log_type, LogType::WSC);
    assert_eq!(report.tag, "log_subscription_overflow");
    let report: serde_json::Value = serde_json::from_str(&report.content).unwrap();
    assert_eq!(report["dropped"], 2);
    assert_eq!(report["dropped_total"], 2);
    assert_eq!(receive(&receiver).tag, "queued");
}

#[test]
fn dropping_a_blocked_callback_does_not_wait_and_worker_exits_after_return() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let release_receiver = Mutex::new(release_receiver);
    let (dropped_sender, dropped_receiver) = mpsc::channel();
    let marker = NotifyDrop(dropped_sender);
    let subscription = OpenNetLogger::register_log_listener(
        Box::new(move |_| {
            let _keep_alive = &marker;
            entered_sender.send(()).unwrap();
            release_receiver.lock().unwrap().recv_timeout(WAIT).unwrap();
        }),
        &[LogType::WSC],
    )
    .unwrap();
    log_t!(LogType::WSC; "blocked");
    entered_receiver.recv_timeout(WAIT).unwrap();
    let (done_sender, done_receiver) = mpsc::channel();
    std::thread::spawn(move || {
        drop(subscription);
        done_sender.send(()).unwrap();
    });
    done_receiver
        .recv_timeout(WAIT)
        .expect("drop did not join the blocked callback");
    release_sender.send(()).unwrap();
    dropped_receiver
        .recv_timeout(WAIT)
        .expect("callback thread exited");
}

#[test]
fn invalid_registration_and_clear_are_explicit() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    assert!(
        matches!(OpenNetLogger::register_log_listener(Box::new(|_| {}), &[]), Err(error) if error.kind() == io::ErrorKind::InvalidInput)
    );
    for capacity in [0, 16_385, usize::MAX] {
        assert!(
            matches!(OpenNetLogger::register_log_listener_with_capacity(Box::new(|_| {}), &[LogType::WSC], capacity), Err(error) if error.kind() == io::ErrorKind::InvalidInput),
            "capacity {capacity} must return InvalidInput",
        );
    }
    let (_one, _one_records) = subscribe(&[LogType::WSC]);
    let (_two, _two_records) = subscribe(&[LogType::Common]);
    OpenNetLogger::clear_global_log_listener();
    assert!(!OpenNetLogger::is_enabled(LogType::WSC));
    assert!(!OpenNetLogger::is_enabled(LogType::Common));
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<LogSubscription>();
}

#[test]
fn capacity_boundaries_deliver_records_and_release_callbacks() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    for capacity in [1, 256, 2048, 16_384] {
        let (sender, records) = mpsc::channel();
        let (dropped_sender, dropped_receiver) = mpsc::channel();
        let marker = NotifyDrop(dropped_sender);
        let subscription = OpenNetLogger::register_log_listener_with_capacity(
            Box::new(move |record| {
                let _keep_alive = &marker;
                let _ = sender.send(record);
            }),
            &[LogType::WSC],
            capacity,
        )
        .unwrap();
        log_t!(LogType::WSC; "capacity_boundary", "capacity", capacity);
        let record = receive(&records);
        assert_eq!(record.log_type, LogType::WSC);
        assert_eq!(record.tag, "capacity_boundary");
        assert_eq!(record.content, format!("{{\"capacity\":{capacity}}}"),);
        assert_eq!(subscription.dropped_count(), 0);
        drop(subscription);
        dropped_receiver
            .recv_timeout(WAIT)
            .expect("callback released after boundary subscription dropped");
        assert!(!OpenNetLogger::is_enabled(LogType::WSC));
    }
}

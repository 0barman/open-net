use super::*;
use crate::{log_e, log_r, log_s, log_t};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::mpsc::{Receiver, Sender};
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
    let subscription = Logger::register_log_listener(
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
    log_t!(LogType::HTTP; "ON_WSC-spoof");
    log_t!(LogType::WSS; "ON_WSC-spoof");
    log_t!(LogType::Database; "ON_WSC-spoof");
    log_t!(LogType::None; "ON_WSC-spoof");
    log_t!("ON_WSC-spoof");
    log_t!(LogType::WSC; "connect", "a|b", 1, "hello");
    log_e!(LogType::Common; "invoke", "error", "failed");
    log_s!(LogType::WSC; "ready");
    let entry = receive(&records);
    assert_eq!(entry.log_type, LogType::WSC);
    assert_eq!(entry.level, LogLevel::Info);
    assert_eq!(entry.tag, "ON_WSC-connect-T");
    assert_eq!(entry.content, r#"{"a":1,"b":"hello"}"#);
    assert!(entry.location.contains("tests.rs:"));
    assert!(entry.create_time > 0);
    let error = receive(&records);
    assert_eq!(error.log_type, LogType::Common);
    assert_eq!(error.level, LogLevel::Error);
    let state = receive(&records);
    assert_eq!(state.tag, "ON_WSC-ready-S");
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
    assert_eq!(receive(&records).tag, "ON_WSC-enabled-T");
    log_r!("legacy", "value", 7,);
    let legacy = receive(&records);
    assert_eq!(legacy.log_type, LogType::Engine);
    assert_eq!(legacy.tag, "ON-legacy-R");
    assert_eq!(legacy.level, LogLevel::Debug);
}

#[test]
fn subscriptions_are_independent_and_dropping_releases_idle_callback() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (first, first_records) = subscribe(&[LogType::WSC]);
    let (_second, second_records) = subscribe(&[LogType::WSC]);
    log_t!(LogType::WSC; "both");
    assert_eq!(receive(&first_records).tag, "ON_WSC-both-T");
    assert_eq!(receive(&second_records).tag, "ON_WSC-both-T");
    drop(first);
    let (_replacement, replacement_records) = subscribe(&[LogType::WSC]);
    log_s!(LogType::WSC; "after_replacement");
    assert_eq!(receive(&second_records).tag, "ON_WSC-after_replacement-S");
    assert_eq!(
        receive(&replacement_records).tag,
        "ON_WSC-after_replacement-S"
    );
    assert!(first_records.recv_timeout(WAIT).is_err());
    let (dropped_sender, dropped_receiver) = mpsc::channel();
    let marker = NotifyDrop(dropped_sender);
    let idle = Logger::register_log_listener(
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
    let subscription = Logger::register_log_listener(Box::new(move |_| {
        log_t!(LogType::WSC; "recursive", "value", callback_evaluated.fetch_add(1, AtomicOrdering::SeqCst));
        let old = callback_holder.lock().unwrap().take();
        drop(old);
        done_sender.send(()).unwrap();
    }), &[LogType::WSC]).unwrap();
    *holder.lock().unwrap() = Some(subscription);
    log_t!(LogType::WSC; "first");
    done_receiver.recv_timeout(WAIT).unwrap();
    assert_eq!(evaluated.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(receive(&other_records).tag, "ON_WSC-first-T");
    log_t!(LogType::WSC; "second");
    assert_eq!(receive(&other_records).tag, "ON_WSC-second-T");
    assert!(holder.lock().unwrap().is_none());
}

#[test]
fn callback_panics_do_not_stop_later_delivery() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (sender, receiver) = mpsc::channel();
    let _subscription = Logger::register_log_listener(
        Box::new(move |record| {
            let should_panic = record.tag == "ON_WSC-panic-T";
            sender.send(record).unwrap();
            if should_panic {
                panic!("test listener panic");
            }
        }),
        &[LogType::WSC],
    )
    .unwrap();
    log_t!(LogType::WSC; "panic");
    assert_eq!(receive(&receiver).tag, "ON_WSC-panic-T");
    log_t!(LogType::WSC; "after_panic");
    assert_eq!(receive(&receiver).tag, "ON_WSC-after_panic-T");
}

#[test]
fn full_queue_is_observable_and_slow_listener_does_not_block_other_listeners() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let release_receiver = Mutex::new(release_receiver);
    let (sender, receiver) = mpsc::channel();
    let slow = Logger::register_log_listener_with_capacity(
        Box::new(move |record| {
            if record.tag == "ON_WSC-block-T" {
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
        assert_eq!(receive(&fast_records).tag, format!("ON_WSC-{name}-T"));
    }
    release_sender.send(()).unwrap();
    assert_eq!(receive(&receiver).tag, "ON_WSC-block-T");
    let report = receive(&receiver);
    assert_eq!(report.log_type, LogType::WSC);
    assert_eq!(report.tag, "ON_WSC-log_subscription_overflow-S");
    let report: serde_json::Value = serde_json::from_str(&report.content).unwrap();
    assert_eq!(report["dropped"], 2);
    assert_eq!(report["dropped_total"], 2);
    assert_eq!(receive(&receiver).tag, "ON_WSC-queued-T");
}

#[test]
fn dropping_a_blocked_callback_does_not_wait_and_worker_exits_after_return() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let release_receiver = Mutex::new(release_receiver);
    let (dropped_sender, dropped_receiver) = mpsc::channel();
    let marker = NotifyDrop(dropped_sender);
    let subscription = Logger::register_log_listener(
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
        matches!(Logger::register_log_listener(Box::new(|_| {}), &[]), Err(error) if error.kind() == io::ErrorKind::InvalidInput)
    );
    assert!(
        matches!(Logger::register_log_listener_with_capacity(Box::new(|_| {}), &[LogType::WSC], 0), Err(error) if error.kind() == io::ErrorKind::InvalidInput)
    );
    let (_one, _one_records) = subscribe(&[LogType::WSC]);
    let (_two, _two_records) = subscribe(&[LogType::Common]);
    Logger::clear_global_log_listener();
    assert!(!Logger::is_enabled(LogType::WSC));
    assert!(!Logger::is_enabled(LogType::Common));
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<LogSubscription>();
}

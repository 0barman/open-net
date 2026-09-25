//! These tests run in isolated child processes because the registry is global.

use super::{LogSubscription, LogType, Logger, FAIL_NEXT_WORKER_SPAWN, SUBSCRIPTION_READS};
use crate::{log_s, log_t, LogInfo};
use std::error::Error;
use std::io;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(3);
const CHILD: &str = "OPEN_NET_LOG_FAST_PATH_TEST_CHILD";

#[track_caller]
fn check(condition: bool, context: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        let location = std::panic::Location::caller();
        Err(io::Error::other(format!("{location}: {}", context.into())).into())
    }
}

fn isolated(name: &str) -> TestResult<bool> {
    if std::env::var(CHILD).ok().as_deref() == Some(name) {
        return Ok(true);
    }
    let exact = format!("common::log::logger::fast_path_tests::{name}");
    let mut child = Command::new(std::env::current_exe()?)
        .args(["--exact", &exact, "--include-ignored", "--nocapture"])
        .env(CHILD, name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(20);
    while child.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(io::ErrorKind::TimedOut, exact).into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("{stdout}{stderr}");
    check(
        output.status.success(),
        format!("isolated {name} failed: {stderr}"),
    )?;
    check(
        stdout.contains("1 passed"),
        format!("isolated {name} did not run exactly one test"),
    )?;
    Ok(false)
}

fn subscribe(types: &[LogType]) -> TestResult<(LogSubscription, Receiver<LogInfo>)> {
    let (sender, receiver) = mpsc::channel();
    let subscription = Logger::register_log_listener(
        Box::new(move |record| {
            let _ = sender.send(record);
        }),
        types,
    )?;
    Ok((subscription, receiver))
}

fn read_count() -> usize {
    SUBSCRIPTION_READS.with(|reads| reads.replace(0))
}

fn verify_empty_without_reads(label: &str) -> TestResult {
    read_count();
    let evaluated = AtomicUsize::new(0);
    for _ in 0..1024 {
        check(
            !Logger::is_enabled(LogType::WSC),
            format!("{label}: WSC remained enabled"),
        )?;
        log_t!(LogType::WSC; "disabled", "value", evaluated.fetch_add(1, Ordering::SeqCst));
    }
    let reads = read_count();
    eprintln!(
        "{label}: registry_read_lock_accesses={reads}, parameter_evaluations={}",
        evaluated.load(Ordering::SeqCst)
    );
    check(
        evaluated.load(Ordering::SeqCst) == 0,
        format!("{label}: disabled parameters evaluated"),
    )?;
    check(
        reads == 0,
        format!("{label}: expected 0 subscription read-lock accesses; observed {reads}"),
    )
}

#[test]
fn t12_empty_registry_avoids_read_lock_and_preserves_filtering() -> TestResult {
    if !isolated("t12_empty_registry_avoids_read_lock_and_preserves_filtering")? {
        return Ok(());
    }
    Logger::clear_global_log_listener();
    verify_empty_without_reads("never registered")?;
    let (other, records) = subscribe(&[LogType::Common])?;
    read_count();
    let evaluated = AtomicUsize::new(0);
    log_t!(LogType::WSC; "filtered", "value", evaluated.fetch_add(1, Ordering::SeqCst));
    check(
        !Logger::is_enabled(LogType::WSC),
        "other origin enabled WSC",
    )?;
    check(
        read_count() > 0,
        "nonempty registry skipped exact filtering",
    )?;
    check(
        evaluated.load(Ordering::SeqCst) == 0,
        "filtered argument evaluated",
    )?;
    check(records.try_recv().is_err(), "filtered record delivered")?;
    log_t!(LogType::Common; "enabled", "value", evaluated.fetch_add(1, Ordering::SeqCst));
    let record = records.recv_timeout(WAIT)?;
    check(
        record.tag == "ON_COMMON-enabled-T",
        "enabled record was changed",
    )?;
    check(
        evaluated.load(Ordering::SeqCst) == 1,
        "enabled argument not evaluated exactly once",
    )?;
    drop(other);
    verify_empty_without_reads("last handle dropped")
}

#[test]
fn t13_registration_drop_clear_and_old_handles_preserve_new_subscriptions() -> TestResult {
    if !isolated("t13_registration_drop_clear_and_old_handles_preserve_new_subscriptions")? {
        return Ok(());
    }
    for old_before_new in [false, true] {
        FAIL_NEXT_WORKER_SPAWN.with(|fail| fail.set(true));
        check(
            Logger::register_log_listener(Box::new(|_| {}), &[LogType::WSC]).is_err(),
            "injected worker failure succeeded",
        )?;
        verify_empty_without_reads("worker creation failure with empty registry")?;
        let (old, old_records) = subscribe(&[LogType::WSC])?;
        let (second, second_records) = subscribe(&[LogType::WSC])?;
        log_s!(LogType::WSC; "two");
        check(
            old_records.recv_timeout(WAIT)?.tag == "ON_WSC-two-S",
            "first subscription missing",
        )?;
        check(
            second_records.recv_timeout(WAIT)?.tag == "ON_WSC-two-S",
            "second subscription missing",
        )?;
        drop(second);
        check(
            Logger::is_enabled(LogType::WSC),
            "dropping one subscription disabled survivor",
        )?;
        Logger::clear_global_log_listener();
        verify_empty_without_reads("clear with old handle alive")?;
        let mut old = Some(old);
        if old_before_new {
            drop(old.take());
        }
        let (new, new_records) = subscribe(&[LogType::WSC])?;
        drop(old.take());
        check(
            Logger::is_enabled(LogType::WSC),
            "old handle disabled new subscription",
        )?;
        log_s!(LogType::WSC; "new");
        check(
            new_records.recv_timeout(WAIT)?.tag == "ON_WSC-new-S",
            "new subscription did not receive",
        )?;
        check(
            old_records.recv_timeout(WAIT).is_err(),
            "old subscription revived after clear",
        )?;
        check(
            matches!(Logger::register_log_listener(Box::new(|_| {}), &[]), Err(error) if error.kind() == io::ErrorKind::InvalidInput),
            "empty origin registration accepted",
        )?;
        check(
            matches!(Logger::register_log_listener_with_capacity(Box::new(|_| {}), &[LogType::WSC], 0), Err(error) if error.kind() == io::ErrorKind::InvalidInput),
            "zero capacity registration accepted",
        )?;
        check(
            Logger::is_enabled(LogType::WSC),
            "failed registration disabled existing subscription",
        )?;
        FAIL_NEXT_WORKER_SPAWN.with(|fail| fail.set(true));
        check(
            Logger::register_log_listener(Box::new(|_| {}), &[LogType::WSC]).is_err(),
            "worker creation failure succeeded with active registry",
        )?;
        check(
            Logger::is_enabled(LogType::WSC),
            "worker creation failure disabled survivor",
        )?;
        drop(new);
        verify_empty_without_reads("new last handle dropped")?;
    }
    Ok(())
}

enum Step {
    Observe(bool),
    Emit,
    Drop(LogSubscription),
    Clear,
    DropAt(LogSubscription, Arc<Barrier>),
    ClearAt(Arc<Barrier>),
}

struct Worker {
    commands: Option<Sender<Step>>,
    replies: Receiver<TestResult>,
    task: Option<std::thread::JoinHandle<()>>,
}

impl Worker {
    fn start() -> TestResult<Self> {
        let (commands, incoming) = mpsc::channel();
        let (replies, outgoing) = mpsc::channel();
        let task = std::thread::Builder::new()
            .name("log-publication-check".into())
            .spawn(move || {
                while let Ok(step) = incoming.recv_timeout(WAIT) {
                    let result = match step {
                        Step::Observe(expected) => check(
                            Logger::is_enabled(LogType::WSC) == expected,
                            "cross-thread visibility mismatch",
                        ),
                        Step::Emit => {
                            log_t!(LogType::WSC; "worker");
                            Ok(())
                        }
                        Step::Drop(subscription) => {
                            drop(subscription);
                            Ok(())
                        }
                        Step::Clear => {
                            Logger::clear_global_log_listener();
                            Ok(())
                        }
                        Step::DropAt(subscription, barrier) => {
                            barrier.wait();
                            drop(subscription);
                            Ok(())
                        }
                        Step::ClearAt(barrier) => {
                            barrier.wait();
                            Logger::clear_global_log_listener();
                            Ok(())
                        }
                    };
                    if replies.send(result).is_err() {
                        break;
                    }
                }
            })?;
        Ok(Self {
            commands: Some(commands),
            replies: outgoing,
            task: Some(task),
        })
    }

    fn step(&self, step: Step) -> TestResult {
        self.begin(step)?;
        self.finish()
    }

    fn begin(&self, step: Step) -> TestResult {
        self.commands
            .as_ref()
            .ok_or_else(|| io::Error::other("worker closed"))?
            .send(step)?;
        Ok(())
    }

    fn finish(&self) -> TestResult {
        self.replies.recv_timeout(WAIT)?
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        drop(self.commands.take());
        if let Some(task) = self.task.take() {
            if task.join().is_err() {
                eprintln!("log publication worker terminated unexpectedly");
            }
        }
    }
}

#[test]
fn t14_cross_thread_publication_clear_and_drop_are_ordered() -> TestResult {
    if !isolated("t14_cross_thread_publication_clear_and_drop_are_ordered")? {
        return Ok(());
    }
    let reader = Worker::start()?;
    let cleaner = Worker::start()?;
    for _ in 0..16 {
        reader.step(Step::Observe(false))?;
        let (old, _) = subscribe(&[LogType::WSC])?;
        reader.step(Step::Observe(true))?;
        cleaner.step(Step::Clear)?;
        reader.step(Step::Observe(false))?;
        let (new, records) = subscribe(&[LogType::WSC])?;
        cleaner.step(Step::Drop(old))?;
        reader.step(Step::Observe(true))?;
        reader.step(Step::Emit)?;
        check(
            records.recv_timeout(WAIT)?.tag == "ON_WSC-worker-T",
            "published subscription missed worker record",
        )?;
        cleaner.step(Step::Drop(new))?;
        reader.step(Step::Observe(false))?;
        verify_empty_without_reads("cross-thread final drop")?;

        let (old, _) = subscribe(&[LogType::WSC])?;
        Logger::clear_global_log_listener();
        let gate = Arc::new(Barrier::new(2));
        cleaner.begin(Step::DropAt(old, Arc::clone(&gate)))?;
        gate.wait();
        let (new, records) = subscribe(&[LogType::WSC])?;
        cleaner.finish()?;
        reader.step(Step::Observe(true))?;
        reader.step(Step::Emit)?;
        check(
            records.recv_timeout(WAIT)?.tag == "ON_WSC-worker-T",
            "racing old drop hid new subscription",
        )?;
        drop(new);

        let gate = Arc::new(Barrier::new(2));
        cleaner.begin(Step::ClearAt(Arc::clone(&gate)))?;
        gate.wait();
        let (racing, records) = subscribe(&[LogType::WSC])?;
        cleaner.finish()?;
        // Clear can win before or after insertion. Once both operations have
        // completed, the handle's active state determines the valid outcome.
        let active = racing.state.active.load(Ordering::Acquire);
        reader.step(Step::Observe(active))?;
        reader.step(Step::Emit)?;
        if active {
            check(
                records.recv_timeout(WAIT)?.tag == "ON_WSC-worker-T",
                "clear-before-registration lost new listener",
            )?;
        } else {
            check(
                records.try_recv().is_err(),
                "registration-before-clear revived retired listener",
            )?;
        }
        drop(racing);
        verify_empty_without_reads("racing clear and registration settled")?;
    }
    Ok(())
}

#[test]
#[ignore = "explicit isolated release diagnostic; report timings without throughput assertions"]
fn p1_registry_check_release_diagnostic() -> TestResult {
    if !isolated("p1_registry_check_release_diagnostic")? {
        return Ok(());
    }
    check(!cfg!(debug_assertions), "diagnostic requires --release")?;
    const CHECKS: usize = 5_000_000;
    for repeat in 1..=5 {
        for mode in ["empty", "unmatched", "matched"] {
            let subscription = if mode == "empty" {
                None
            } else {
                Some(subscribe(&[LogType::WSC])?)
            };
            let origin = if mode == "unmatched" {
                LogType::HTTP
            } else {
                LogType::WSC
            };
            for _ in 0..100_000 {
                std::hint::black_box(Logger::is_enabled(std::hint::black_box(origin)));
            }
            read_count();
            let start = Instant::now();
            let mut enabled = 0usize;
            for _ in 0..CHECKS {
                enabled += usize::from(std::hint::black_box(Logger::is_enabled(
                    std::hint::black_box(origin),
                )));
            }
            let elapsed = start.elapsed();
            let reads = read_count();
            check(
                enabled == if mode == "matched" { CHECKS } else { 0 },
                "diagnostic filter mismatch",
            )?;
            eprintln!(
                "{}",
                serde_json::json!({"p1_diagnostic":true,"repeat":repeat,"mode":mode,"checks":CHECKS,"enabled":enabled,"registry_read_lock_accesses":reads,"elapsed_ns":elapsed.as_nanos(),"profile":"release","counter_scope":"thread-local test instrumentation at production read-lock entry"})
            );
            drop(subscription);
        }
    }
    Ok(())
}

#[test]
fn t14_callback_unsubscribe_preserves_recursion_suppression() -> TestResult {
    if !isolated("t14_callback_unsubscribe_preserves_recursion_suppression")? {
        return Ok(());
    }
    let holder = Arc::new(Mutex::new(None::<LogSubscription>));
    let callback_holder = Arc::clone(&holder);
    let evaluated = Arc::new(AtomicUsize::new(0));
    let callback_evaluated = Arc::clone(&evaluated);
    let (done, completed) = mpsc::channel();
    let (other, records) = subscribe(&[LogType::WSC])?;
    let own = Logger::register_log_listener(
        Box::new(move |_| {
            let result = (|| -> TestResult {
                log_t!(LogType::WSC; "recursive", "value", callback_evaluated.fetch_add(1, Ordering::SeqCst));
                let old = callback_holder
                    .lock()
                    .map_err(|_| io::Error::other("callback holder poisoned"))?
                    .take();
                drop(old);
                check(
                    !Logger::is_enabled(LogType::WSC),
                    "callback recursion guard became enabled",
                )
            })();
            let _ = done.send(result);
        }),
        &[LogType::WSC],
    )?;
    *holder
        .lock()
        .map_err(|_| io::Error::other("holder poisoned"))? = Some(own);
    log_t!(LogType::WSC; "outer");
    completed.recv_timeout(WAIT)??;
    check(
        evaluated.load(Ordering::SeqCst) == 0,
        "recursive arguments evaluated",
    )?;
    check(
        records.recv_timeout(WAIT)?.tag == "ON_WSC-outer-T",
        "other listener lost outer record",
    )?;
    check(records.try_recv().is_err(), "recursive record leaked")?;
    check(
        Logger::is_enabled(LogType::WSC),
        "callback drop disabled other listener",
    )?;
    drop(other);
    verify_empty_without_reads("callback and final listener dropped")
}

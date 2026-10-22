#![cfg(test)]

use super::{ExecutionScope, ThreadRegistry};
use crate::common::CommonError;
use std::sync::{mpsc, Arc};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(5);
type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn retirement_work_waits_for_derived_work_after_parent_retires() -> TestResult {
    let tracker = Arc::new(super::WorkTracker::default());
    let parent = tracker.register();
    let (started, entry) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let producer = Arc::clone(&tracker);
    let work = std::thread::spawn(move || {
        let derived = producer.register();
        drop(parent);
        let _ = started.send(());
        let _ = gate.recv_timeout(WAIT);
        drop(derived);
    });
    entry.recv_timeout(WAIT)?;
    let (finished, completion) = mpsc::channel();
    let waiting = Arc::clone(&tracker);
    let coordinator = std::thread::spawn(move || {
        waiting.wait();
        let _ = finished.send(());
    });
    assert!(completion.recv_timeout(Duration::from_millis(50)).is_err());
    release.send(())?;
    completion.recv_timeout(WAIT)?;
    work.join().map_err(|_| "tracked work panicked")?;
    coordinator
        .join()
        .map_err(|_| "work coordinator panicked")?;
    tracker.wait();
    Ok(())
}

#[test]
fn retirement_work_lease_is_released_during_unwinding() {
    let tracker = Arc::new(super::WorkTracker::default());
    let lease = tracker.register();
    let result = std::panic::catch_unwind(|| {
        let _lease = lease;
        panic!("retirement work panic");
    });
    assert!(result.is_err());
    assert_eq!(super::lock(&tracker.active).strong_count(), 0);
    tracker.wait();
}

#[test]
fn retirement_work_shares_a_generation_until_every_lease_retires() -> TestResult {
    let tracker = Arc::new(super::WorkTracker::default());
    let first = tracker.register();
    let second = tracker.register();
    assert!(Arc::ptr_eq(&first._generation, &second._generation));
    let generation = Arc::downgrade(&first._generation);
    let (finished, completion) = mpsc::channel();
    let waiting = Arc::clone(&tracker);
    let coordinator = std::thread::spawn(move || {
        waiting.wait();
        let _ = finished.send(());
    });
    assert!(tracker.wait_started_for_test(WAIT));
    drop(first);
    assert_eq!(generation.strong_count(), 1);
    assert!(completion.recv_timeout(Duration::from_millis(50)).is_err());
    drop(second);
    completion.recv_timeout(WAIT)?;
    coordinator
        .join()
        .map_err(|_| "work coordinator panicked")?;
    assert_eq!(generation.strong_count(), 0);
    Ok(())
}

#[test]
fn retirement_work_reuses_tracker_after_a_completed_generation() {
    let tracker = Arc::new(super::WorkTracker::default());
    let first = tracker.register();
    let completed = Arc::downgrade(&first._generation);
    drop(first);
    tracker.wait();
    let next = tracker.register();
    assert!(!completed.ptr_eq(&Arc::downgrade(&next._generation)));
    assert_eq!(super::lock(&tracker.active).strong_count(), 1);
    drop(next);
    tracker.wait();
}

#[test]
fn retired_generation_cannot_complete_a_successor_generation() -> TestResult {
    let tracker = Arc::new(super::WorkTracker::default());
    let first = tracker.register();
    let retiring = Arc::downgrade(&first._generation);
    let (retired, retirement) = mpsc::channel();
    // Force the last strong reference to disappear before its destructor can
    // take the tracker lock. Registration uses the same locked helper below,
    // making the old-drop/new-registration interleaving deterministic.
    let mut active = super::lock(&tracker.active);
    let retiring_thread = std::thread::spawn(move || {
        drop(first);
        let _ = retired.send(());
    });
    let deadline = std::time::Instant::now() + WAIT;
    while retiring.strong_count() != 0 {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    let successor = tracker.register_locked(&mut active);
    assert!(!retiring.ptr_eq(&Arc::downgrade(&successor._generation)));
    let (finished, completion) = mpsc::channel();
    let waiting = Arc::clone(&tracker);
    let coordinator = std::thread::spawn(move || {
        waiting.wait();
        let _ = finished.send(());
    });
    drop(active);
    retirement.recv_timeout(WAIT)?;
    assert!(tracker.wait_started_for_test(WAIT));
    assert!(completion.recv_timeout(Duration::from_millis(50)).is_err());
    drop(successor);
    completion.recv_timeout(WAIT)?;
    retiring_thread
        .join()
        .map_err(|_| "lease retirement panicked")?;
    coordinator
        .join()
        .map_err(|_| "work coordinator panicked")?;
    Ok(())
}

#[test]
fn panicking_panic_payload_still_publishes_join_result() -> TestResult {
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("payload destructor panic");
        }
    }
    let registry = Arc::new(ThreadRegistry::new());
    let worker = registry.spawn_tracked("payload-panic", || std::panic::panic_any(PanicOnDrop))?;
    assert_eq!(worker.join(), Err(CommonError::RuntimeError));
    assert_eq!(worker.join(), Err(CommonError::RuntimeError));
    assert_eq!(registry.seal_and_join(), Err(CommonError::RuntimeError));
    Ok(())
}

#[test]
fn reaped_panic_payload_can_reenter_registry_without_holding_its_lock() -> TestResult {
    struct Reenter(Arc<ThreadRegistry>, mpsc::Sender<bool>);
    impl Drop for Reenter {
        fn drop(&mut self) {
            let result = self.0.spawn("payload-reentered", || {}).is_ok();
            let _ = self.1.send(result);
        }
    }
    let registry = Arc::new(ThreadRegistry::new());
    let (sent, received) = mpsc::channel();
    let payload = Reenter(Arc::clone(&registry), sent);
    let worker =
        registry.spawn_tracked("reentrant-payload", move || std::panic::panic_any(payload))?;
    let deadline = std::time::Instant::now() + WAIT;
    while !worker.is_finished() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    registry.spawn("trigger-reaping", || {})?;
    assert!(received.recv_timeout(WAIT)?);
    assert_eq!(registry.seal_and_join(), Err(CommonError::RuntimeError));
    Ok(())
}

#[test]
fn registry_waits_for_actual_exit_with_concurrent_local_join() -> TestResult {
    let registry = Arc::new(ThreadRegistry::default());
    let (started, entry) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let worker = registry.spawn_tracked("retirement-test", move || {
        let _ = started.send(ExecutionScope::is_current());
        let _ = gate.recv_timeout(WAIT);
    })?;
    assert!(entry.recv_timeout(WAIT)?);
    let local = worker.clone();
    let local_join = std::thread::spawn(move || local.join());
    let (finished, completion) = mpsc::channel();
    let owner = Arc::clone(&registry);
    let retirement = std::thread::spawn(move || {
        let _ = finished.send(owner.seal_and_join());
    });
    assert!(completion.recv_timeout(Duration::from_millis(50)).is_err());
    release.send(())?;
    assert_eq!(completion.recv_timeout(WAIT)?, Ok(()));
    assert_eq!(local_join.join().map_err(|_| "local join failed")?, Ok(()));
    retirement.join().map_err(|_| "retirement failed")?;
    assert!(worker.is_finished());
    assert_eq!(worker.join(), Ok(()));
    assert_eq!(registry.seal_and_join(), Ok(()));
    assert!(registry.spawn("late-worker", || {}).is_err());
    assert!(!ExecutionScope::is_current());
    Ok(())
}

#[test]
fn invalid_thread_name_does_not_strand_registry_or_drop_captures_under_lock() -> TestResult {
    struct Reenter(Arc<ThreadRegistry>, mpsc::Sender<bool>);
    impl Drop for Reenter {
        fn drop(&mut self) {
            let _ = self.1.send(self.0.spawn("valid-reentry", || {}).is_ok());
        }
    }
    let registry = Arc::new(ThreadRegistry::new());
    let (sent, received) = mpsc::channel();
    let capture = Reenter(Arc::clone(&registry), sent);
    let result = registry.spawn("invalid\0thread", move || drop(capture));
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    assert!(received.recv_timeout(WAIT)?);
    assert_eq!(registry.seal_and_join(), Ok(()));
    Ok(())
}

#[test]
fn startup_without_reaping_defers_old_panic_payload_until_retirement() -> TestResult {
    struct CountDrop(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
    }
    let registry = Arc::new(ThreadRegistry::new());
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let payload = CountDrop(Arc::clone(&drops));
    let old = registry.spawn_tracked("old-worker-panic", move || std::panic::panic_any(payload))?;
    let deadline = std::time::Instant::now() + WAIT;
    while !old.is_finished() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    let next = registry.spawn_tracked_without_reaping("locked-startup", || {})?;
    assert_eq!(next.join(), Ok(()));
    assert_eq!(drops.load(std::sync::atomic::Ordering::Acquire), 0);
    assert_eq!(registry.seal_and_join(), Err(CommonError::RuntimeError));
    assert_eq!(drops.load(std::sync::atomic::Ordering::Acquire), 1);
    Ok(())
}

#[test]
fn registry_reaps_finished_handles_and_retains_panic_failure() -> TestResult {
    let registry = Arc::new(ThreadRegistry::new());
    let first = registry.spawn_tracked("panicking-worker", || panic!("test worker panic"))?;
    assert_eq!(first.join(), Err(CommonError::RuntimeError));
    for _ in 0..12 {
        let worker = registry.spawn_tracked("replacement-worker", || {})?;
        assert_eq!(worker.join(), Ok(()));
    }
    assert!(super::lock(&registry.state).threads.len() <= 1);
    assert_eq!(registry.seal_and_join(), Err(CommonError::RuntimeError));
    assert_eq!(registry.seal_and_join(), Err(CommonError::RuntimeError));
    Ok(())
}

use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Barrier};
use std::time::Duration;

#[test]
fn child_cancellation_is_local_and_root_cancellation_reaches_descendants() -> TestResult {
    let root = CancellationGroup::new();
    let first = root.child();
    let second = root.child();
    let grandchild = first.child();
    first.cancel();
    check!(first.is_cancelled())?;
    check!(grandchild.is_cancelled())?;
    check!(!root.is_cancelled())?;
    check!(!second.is_cancelled())?;
    root.cancel();
    check!(second.is_cancelled())?;
    let late_child = root.child();
    check!(late_child.is_cancelled())?;
    check!(late_child.inner.notification.clone().is_cancelled())?;
    Ok(())
}

#[test]
fn clone_shares_identity_and_drop_does_not_cancel() -> TestResult {
    let root = CancellationGroup::new();
    let clone = root.clone();
    let child = root.child();
    drop(root);
    check!(!clone.is_cancelled())?;
    check!(!child.is_cancelled())?;
    clone.cancel();
    check!(child.is_cancelled())?;
    Ok(())
}

#[test]
fn dropped_intermediate_domain_does_not_hide_its_descendants() -> TestResult {
    let root = CancellationGroup::new();
    let parent = root.child();
    let child = parent.child();
    drop(parent);
    root.cancel();
    check!(child.is_cancelled())?;
    Ok(())
}

#[test]
fn active_action_reads_state_without_reentry_and_cancelled_action_is_not_called() -> TestResult {
    let domain = CancellationGroup::new();
    check_eq!(
        domain
            .inner
            .lock_if_active()
            .map(|_gate| domain.is_cancelled())?,
        false
    )?;
    domain.cancel();
    let called = AtomicUsize::new(0);
    check_eq!(
        (domain
            .inner
            .lock_if_active()
            .map(|_gate| called.fetch_add(1, Ordering::SeqCst)))
        .map_err(|error| error.kind()),
        Err(crate::error::ErrorKind::Cancelled)
    )?;
    check_eq!(called.load(Ordering::SeqCst), 0)?;
    Ok(())
}

#[test]
fn hooks_run_once_for_live_guards_and_cancelled_domains_reject_binding() -> TestResult {
    let domain = CancellationGroup::new();
    let child = domain.child();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let _guard = child.inner.bind_cancel_hook(Arc::new(move || {
        counted.fetch_add(1, Ordering::SeqCst);
    }))?;
    child.cancel();
    domain.cancel();
    domain.cancel();
    check_eq!(calls.load(Ordering::SeqCst), 1)?;
    check!(matches!(
        domain.inner.bind_cancel_hook(Arc::new(|| {})),
        Err(error) if error.kind() == crate::error::ErrorKind::Cancelled
    ))?;
    Ok(())
}

#[test]
fn dropping_hook_guard_unregisters_without_cancelling_domain() -> TestResult {
    let domain = CancellationGroup::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let guard = domain.inner.bind_cancel_hook(Arc::new(move || {
        counted.fetch_add(1, Ordering::SeqCst);
    }))?;
    drop(guard);
    check!(!domain.is_cancelled())?;
    domain.cancel();
    check_eq!(calls.load(Ordering::SeqCst), 0)?;
    Ok(())
}

#[test]
fn cancellation_waits_for_admitted_action_and_then_closes_the_gate() -> TestResult {
    let domain = CancellationGroup::new();
    let active = domain.child();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (action_tx, action_rx) = mpsc::channel();
    let action = std::thread::spawn(move || {
        let result = active.inner.lock_if_active().map(|_gate| {
            entered_tx
                .send(())
                .map_err(|error| test_error(error.to_string()))?;
            release_rx
                .recv_timeout(Duration::from_secs(2))
                .map_err(|error| test_error(error.to_string()))?;
            Ok::<(), crate::module::ws_client::test_support::TestError>(())
        });
        let _ = action_tx.send(result);
    });
    entered_rx.recv_timeout(Duration::from_secs(2))?;
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let (cancelled_tx, cancelled_rx) = mpsc::channel();
    let cancelled = domain.clone();
    let cancellation = std::thread::spawn(move || {
        let _ = attempt_tx.send(());
        cancelled.cancel();
        let _ = cancelled_tx.send(());
    });
    attempt_rx.recv_timeout(Duration::from_secs(2))?;
    let before_release = cancelled_rx.recv_timeout(Duration::from_millis(30));
    release_tx.send(())?;
    action_rx.recv_timeout(Duration::from_secs(2))???;
    if before_release.is_err() {
        cancelled_rx.recv_timeout(Duration::from_secs(2))?;
    }
    action
        .join()
        .map_err(|_| test_error("action thread failed"))?;
    cancellation
        .join()
        .map_err(|_| test_error("cancellation thread failed"))?;
    check!(matches!(
        before_release,
        Err(mpsc::RecvTimeoutError::Timeout)
    ))?;
    check_eq!(
        (domain.inner.lock_if_active().map(|_gate| 7)).map_err(|error| error.kind()),
        Err(crate::error::ErrorKind::Cancelled)
    )?;
    Ok(())
}

#[test]
fn cancellation_hooks_can_reenter_the_closed_gate() -> TestResult {
    let domain = CancellationGroup::new();
    let observed = domain.clone();
    let (hook_tx, hook_rx) = mpsc::channel();
    let _guard = domain.inner.bind_cancel_hook(Arc::new(move || {
        let _ = hook_tx.send(observed.inner.lock_if_active().map(|_gate| ()));
    }))?;
    let worker = std::thread::spawn(move || domain.cancel());
    check_eq!(
        (hook_rx.recv_timeout(Duration::from_secs(2))?).map_err(|error| error.kind()),
        Err(crate::error::ErrorKind::Cancelled)
    )?;
    worker
        .join()
        .map_err(|_| test_error("hook cancellation thread failed"))?;
    Ok(())
}

#[test]
fn live_hook_keeps_its_domain_reachable_and_releases_it_without_a_cycle() -> TestResult {
    let root = CancellationGroup::new();
    let child = root.child();
    let weak = Arc::downgrade(&child.inner);
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let guard = child.inner.bind_cancel_hook(Arc::new(move || {
        counted.fetch_add(1, Ordering::SeqCst);
    }))?;
    drop(child);
    check!(weak.upgrade().is_some())?;
    root.cancel();
    check_eq!(calls.load(Ordering::SeqCst), 1)?;
    drop(guard);
    check!(weak.upgrade().is_none())?;
    check!(lock_recover(&root.inner.children).is_empty())?;
    Ok(())
}

#[test]
fn completion_can_drop_its_hook_inside_the_admission_gate() -> TestResult {
    let domain = CancellationGroup::new();
    let calls = Arc::new(AtomicUsize::new(0));
    for _ in 0..64 {
        let counted = calls.clone();
        let guard = domain.inner.bind_cancel_hook(Arc::new(move || {
            counted.fetch_add(1, Ordering::SeqCst);
        }))?;
        domain.inner.lock_if_active().map(|_gate| drop(guard))?;
    }
    check!(lock_recover(&domain.inner.hooks).is_empty())?;
    domain.cancel();
    check_eq!(calls.load(Ordering::SeqCst), 0)?;
    Ok(())
}

#[test]
fn binding_racing_parent_cancellation_cannot_miss_a_hook() -> TestResult {
    for _ in 0..32 {
        let root = CancellationGroup::new();
        let child = root.child();
        let barrier = Arc::new(Barrier::new(2));
        let other_barrier = barrier.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let binder = std::thread::spawn(move || {
            other_barrier.wait();
            child.inner.bind_cancel_hook(Arc::new(move || {
                counted.fetch_add(1, Ordering::SeqCst);
            }))
        });
        barrier.wait();
        root.cancel();
        let guard = binder
            .join()
            .map_err(|_| test_error("binding thread failed"))?;
        match guard {
            Ok(_guard) => check_eq!(calls.load(Ordering::SeqCst), 1)?,
            Err(error) => {
                check_eq!((error).kind(), crate::error::ErrorKind::Cancelled)?;
                check_eq!(calls.load(Ordering::SeqCst), 0)?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn parent_cancellation_wakes_child_waiters() -> TestResult {
    let root = CancellationGroup::new();
    let child = root.child();
    let token = child.inner.notification.clone();
    root.cancel();
    tokio::time::timeout(Duration::from_secs(1), token.cancelled()).await?;
    check!(child.is_cancelled())?;
    Ok(())
}

#[test]
fn deep_domain_drop_uses_bounded_stack() -> TestResult {
    const CHILD_ENV: &str = "OPEN_NET_TEST_DEEP_DOMAIN_DROP";
    if std::env::var(CHILD_ENV).as_deref() == Ok("1") {
        let worker =
            std::thread::Builder::new()
                .stack_size(256 * 1024)
                .spawn(|| -> TestResult {
                    for retain_root in [false, true] {
                        let root = CancellationGroup::new();
                        let weak_root = Arc::downgrade(&root.inner);
                        let retained = retain_root.then(|| root.clone());
                        let mut leaf = root;
                        for _ in 0..4000 {
                            leaf = leaf.child();
                        }
                        drop(leaf);
                        match retained {
                            Some(root) => {
                                check!(lock_recover(&root.inner.children).is_empty())?;
                                check!(!root.is_cancelled())?;
                                let next = root.child();
                                root.cancel();
                                check!(next.is_cancelled())?;
                            }
                            None => check!(weak_root.upgrade().is_none())?,
                        }
                    }
                    Ok(())
                })?;
        worker
            .join()
            .map_err(|_| test_error("deep domain cleanup worker failed"))??;
        return Ok(());
    }

    // A regression aborts only the child process, so the outer test can report
    // the failure without terminating the whole unit-test executable.
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("api::ws::cancellation::tests::deep_domain_drop_uses_bounded_stack")
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(10))
        .ok_or_else(|| test_error("deep-domain watchdog deadline overflow"))?;
    let mut timed_out = false;
    while child.try_wait()?.is_none() {
        if std::time::Instant::now() >= deadline {
            timed_out = true;
            child.kill()?;
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let output = child.wait_with_output()?;
    check!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "isolated child did not execute the deep cancellation test"
    )?;
    check!(
        !timed_out && output.status.success(),
        "deep-domain child failed: {}; timed out: {}; stderr: {}",
        output.status,
        timed_out,
        String::from_utf8_lossy(&output.stderr)
    )?;
    Ok(())
}

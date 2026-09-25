use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc;
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(5);

#[test]
fn concurrent_first_start_creates_one_fixed_worker_set() -> TestResult {
    let executor = ListenerExecutor::new("concurrent-lazy-listeners", 2, 16)?;
    let mut gates = Vec::new();
    let mut calls = Vec::new();
    for _ in 0..8 {
        let executor = Arc::clone(&executor);
        let (release, released) = mpsc::sync_channel::<()>(1);
        gates.push(release);
        calls.push(std::thread::Builder::new().spawn(move || -> TestResult {
            released.recv_timeout(DEADLINE)?;
            Ok(executor.ensure_started()?)
        })?);
    }
    for gate in gates {
        gate.send(())?;
    }
    for call in calls {
        call.join()
            .map_err(|_| test_error("first-start caller did not return"))??;
    }
    check_eq!(executor.started_worker_count(), 2)?;
    executor.close()?;
    Ok(())
}

#[test]
fn close_without_registration_starts_nothing_and_cannot_be_reopened() -> TestResult {
    let executor = ListenerExecutor::new("closed-lazy-listeners", 2, 8)?;
    executor.close()?;
    let mut attempts = 0;
    check_eq!(
        executor.ensure_started_with_spawner(|_, _| {
            attempts += 1;
            Err(std::io::Error::other("closed executor attempted startup"))
        }),
        Err(NetError::from(crate::error::ErrorKind::QueueClosed))
    )?;
    check_eq!(attempts, 0)?;
    check_eq!(executor.started_worker_count(), 0)?;
    check_eq!(
        executor.try_submit(Box::new(|| {})),
        Err(NetError::from(crate::error::ErrorKind::QueueClosed))
    )?;
    Ok(())
}

#[test]
fn close_during_startup_prevents_publication_and_retires_started_workers() -> TestResult {
    let executor = ListenerExecutor::new("closing-starting-listeners", 2, 8)?;
    let finished = Arc::new(AtomicUsize::new(0));
    let mut attempts = 0;
    let result = executor.ensure_started_with_spawner(|builder, task| {
        attempts += 1;
        if attempts == 1 {
            executor.close().map_err(std::io::Error::other)?;
        }
        let finished = Arc::clone(&finished);
        builder.spawn(move || {
            task();
            finished.fetch_add(1, Ordering::SeqCst);
        })
    });
    check_eq!(
        result,
        Err(NetError::from(crate::error::ErrorKind::QueueClosed))
    )?;
    check_eq!(finished.load(Ordering::SeqCst), attempts)?;
    check_eq!(
        executor.ensure_started(),
        Err(NetError::from(crate::error::ErrorKind::QueueClosed))
    )?;
    check_eq!(executor.started_worker_count(), attempts)?;
    Ok(())
}

#[test]
fn failed_startup_has_no_user_work_and_repeated_attempts_leave_no_workers() -> TestResult {
    let executor = ListenerExecutor::new("retry-lazy-listeners", 2, 8)?;
    let active = Arc::new(AtomicUsize::new(0));
    for _ in 0..3 {
        let mut attempts = 0;
        let mut submission = None;
        let result = executor.ensure_started_with_spawner(|builder, task| {
            attempts += 1;
            if attempts == 2 {
                submission = Some(executor.try_submit(Box::new(|| {})));
                return Err(std::io::Error::other("injected startup failure"));
            }
            let active = Arc::clone(&active);
            builder.spawn(move || {
                active.fetch_add(1, Ordering::SeqCst);
                task();
                active.fetch_sub(1, Ordering::SeqCst);
            })
        });
        check_eq!(
            result,
            Err(NetError::from(crate::error::ErrorKind::RuntimeUnavailable))
        )?;
        check_eq!(
            submission,
            Some(Err(NetError::from(
                crate::error::ErrorKind::RuntimeUnavailable
            )))
        )?;
        check_eq!(attempts, 2)?;
        check_eq!(active.load(Ordering::SeqCst), 0)?;
    }
    executor.ensure_started()?;
    let (delivered, received) = mpsc::channel();
    executor.try_submit(Box::new(move || {
        let _ = delivered.send(());
    }))?;
    received.recv_timeout(DEADLINE)?;
    check_eq!(executor.started_worker_count(), 5)?;
    Ok(())
}

#[test]
fn retry_waits_until_failed_workers_have_completely_exited() -> TestResult {
    let executor = ListenerExecutor::new("retiring-lazy-listeners", 2, 8)?;
    let (retiring, observed_retiring) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Arc::new(Mutex::new(released));
    let (retry_spawned, observed_retry) = mpsc::channel();
    let starter = Arc::clone(&executor);
    let failing = std::thread::Builder::new().spawn(move || {
        let mut attempts = 0;
        starter.ensure_started_with_spawner(|builder, task| {
            attempts += 1;
            if attempts == 2 {
                return Err(std::io::Error::other("injected startup failure"));
            }
            let retiring = retiring.clone();
            let released = Arc::clone(&released);
            builder.spawn(move || {
                task();
                let _ = retiring.send(());
                if let Ok(released) = released.lock() {
                    let _ = released.recv_timeout(DEADLINE);
                }
            })
        })
    })?;
    observed_retiring.recv_timeout(DEADLINE)?;
    let retry_executor = Arc::clone(&executor);
    let retry = std::thread::Builder::new().spawn(move || {
        retry_executor.ensure_started_with_spawner(|builder, task| {
            let _ = retry_spawned.send(());
            builder.spawn(task)
        })
    })?;
    check!(observed_retry
        .recv_timeout(Duration::from_millis(50))
        .is_err())?;
    drop(release);
    check_eq!(
        failing
            .join()
            .map_err(|_| test_error("failed startup caller did not return"))?,
        Err(NetError::from(crate::error::ErrorKind::RuntimeUnavailable))
    )?;
    retry
        .join()
        .map_err(|_| test_error("retry caller did not return"))??;
    observed_retry.recv_timeout(DEADLINE)?;
    check_eq!(executor.started_worker_count(), 3)?;
    Ok(())
}

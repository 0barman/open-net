//! User callbacks run outside the network runtime, including initial notifications.

use on_common::log::log_def::LogType;
use std::future::Future;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use tokio_util::sync::CancellationToken;

/// Starts immediately; dropping the returned waiter does not cancel user code.
/// Detached OS threads let network shutdown finish even when a callback is blocked.
pub(super) fn start_user_callback<F>(
    thread_name: &'static str,
    callback: F,
) -> impl Future<Output = ()> + Send
where
    F: FnOnce() + Send + 'static,
{
    start_user_callback_with(thread_name, callback, move |task| {
        std::thread::Builder::new()
            .name(thread_name.to_string())
            .spawn(task)
            .map(drop)
    })
}

pub(super) async fn run_user_callback<F>(thread_name: &'static str, callback: F)
where
    F: FnOnce() + Send + 'static,
{
    on_common::log_t!(LogType::WSC; "run_user_callback", "thread_name|callback", thread_name, std::any::type_name::<F>());
    start_user_callback(thread_name, callback).await;
}

/// Reports failure to start user code, while preserving immediate, detached execution.
pub(super) fn try_start_user_callback<F>(
    thread_name: &'static str,
    callback: F,
) -> io::Result<impl Future<Output = ()> + Send>
where
    F: FnOnce() + Send + 'static,
{
    try_start_user_callback_with(thread_name, callback, move |task| {
        std::thread::Builder::new()
            .name(thread_name.to_string())
            .spawn(task)
            .map(drop)
    })
}

/// The injectable spawn boundary lets tests exercise resource exhaustion without
/// exhausting OS threads or relying on process-wide mutable test hooks.
fn start_user_callback_with<F, S>(
    thread_name: &'static str,
    callback: F,
    spawn: S,
) -> impl Future<Output = ()> + Send
where
    F: FnOnce() + Send + 'static,
    S: FnOnce(Box<dyn FnOnce() + Send>) -> io::Result<()>,
{
    let started = try_start_user_callback_with(thread_name, callback, spawn);
    async move {
        if let Ok(completed) = started {
            completed.await;
        }
    }
}

pub(super) fn try_start_user_callback_with<F, S>(
    thread_name: &'static str,
    callback: F,
    spawn: S,
) -> io::Result<impl Future<Output = ()> + Send>
where
    F: FnOnce() + Send + 'static,
    S: FnOnce(Box<dyn FnOnce() + Send>) -> io::Result<()>,
{
    on_common::log_t!(LogType::WSC; "start_user_callback", "thread_name|callback", thread_name, std::any::type_name::<F>());
    let completed = CancellationToken::new();
    // Create before spawn so rejection and callback unwinding also release waiters.
    let completion_guard = completed.clone().drop_guard();
    let task = Box::new(move || {
        let _completion_guard = completion_guard;
        if catch_unwind(AssertUnwindSafe(callback)).is_err() {
            on_common::log_e!(LogType::WSC; "start_user_callback", "thread_name|error", thread_name, "user_callback_panicked");
        }
        on_common::log_s!(LogType::WSC; "start_user_callback", "thread_name|state", thread_name, "callback_finished");
    });
    if let Err(error) = spawn(task) {
        on_common::log_e!(LogType::WSC; "start_user_callback", "thread_name|error", thread_name, on_common::log::summary::error(&error));
        return Err(error);
    }
    Ok(async move { completed.cancelled().await })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check, check_eq, TestResult};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn rejected_data_spawn_returns_the_original_error_and_releases_captures() -> TestResult {
        let called = Arc::new(AtomicBool::new(false));
        let callback_called = Arc::clone(&called);
        let released = CancellationToken::new();
        let guard = released.clone().drop_guard();
        let started = try_start_user_callback_with(
            "rejected-data-callback-test",
            move || {
                let _guard = guard;
                callback_called.store(true, Ordering::SeqCst);
            },
            |task| {
                drop(task);
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            },
        );
        check!(!called.load(Ordering::SeqCst))?;
        check!(released.is_cancelled())?;
        match started {
            Err(error) => check_eq!(error.kind(), io::ErrorKind::WouldBlock)?,
            Ok(_) => {
                return Err(
                    io::Error::other("rejected data callback was reported as started").into(),
                )
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejected_spawn_releases_initial_barrier_without_executing_user_code() -> TestResult {
        let called = Arc::new(AtomicBool::new(false));
        let callback_called = Arc::clone(&called);
        let initial_done = CancellationToken::new();
        let initial_guard = initial_done.clone().drop_guard();
        let completed = start_user_callback_with(
            "rejected-callback-test",
            move || {
                let _initial_guard = initial_guard;
                callback_called.store(true, Ordering::SeqCst);
            },
            |task| {
                drop(task);
                Err(io::Error::other("injected thread resource exhaustion"))
            },
        );
        tokio::time::timeout(Duration::from_secs(2), completed).await?;
        check!(!called.load(Ordering::SeqCst))?;
        check!(initial_done.is_cancelled())?;
        Ok(())
    }

    #[test]
    fn dropping_waiter_still_runs_callback_on_a_runtime_free_thread() -> TestResult {
        let caller = std::thread::current().id();
        let (observed_tx, observed_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        drop(start_user_callback("detached-callback-test", move || {
            // The sender is dropped only after the waiter, so completion proves the
            // submitted callback survives even when nobody polls its future.
            let _ = release_rx.recv();
            let _ = observed_tx.send((
                std::thread::current().id(),
                tokio::runtime::Handle::try_current().is_ok(),
            ));
        }));
        drop(release_tx);
        let (callback_thread, has_runtime) = observed_rx.recv_timeout(Duration::from_secs(5))?;
        check!(callback_thread != caller)?;
        check!(!has_runtime)?;
        Ok(())
    }

    #[tokio::test]
    async fn successful_callback_releases_completion_after_return() -> TestResult {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let called = Arc::new(AtomicBool::new(false));
        let callback_called = Arc::clone(&called);
        let completed = start_user_callback("completion-callback-test", move || {
            let _ = entered_tx.send(());
            let _ = release_rx.recv();
            callback_called.store(true, Ordering::SeqCst);
        });
        tokio::pin!(completed);
        tokio::time::timeout(Duration::from_secs(2), entered_rx).await??;
        check!(futures::poll!(&mut completed).is_pending())?;
        check_eq!(called.load(Ordering::SeqCst), false)?;
        drop(release_tx);
        tokio::time::timeout(Duration::from_secs(2), completed).await?;
        check!(called.load(Ordering::SeqCst))?;
        Ok(())
    }
}

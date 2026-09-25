use crate::error::ErrorKind;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::ws::*;
use std::sync::Arc;
use std::time::Duration;

use crate::module::ws_client::{
    callback_event::CallbackEvent, callback_executor::DataCallbackPool,
    data_subscription_executor::DataSubscriptionExecutor,
};
use crate::subscription::CallbackExecutor;
#[test]
fn initial_callback_reservation_is_atomic_and_fork_has_independent_slot() -> TestResult {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let pool = Arc::new(DataCallbackPool::new(1));
    let executor = DataSubscriptionExecutor::for_session(
        tx.clone(),
        SessionId::from_allocated(7),
        pool.clone(),
    );
    executor.ensure_ready()?;
    check_eq!(
        executor.fork().ensure_ready().err().map(|e| e.kind()),
        Some(ErrorKind::QueueFull)
    )?;
    let (called, seen) = std::sync::mpsc::channel();
    executor.submit(Box::new(move || {
        let _ = called.send(42);
    }))?;
    let CallbackEvent::SessionJob { session_id, job } = rx.try_recv()? else {
        return Err(test_error("expected current session job"));
    };
    check_eq!(session_id.as_u64(), 7)?;
    job();
    check_eq!(seen.recv_timeout(Duration::from_secs(1))?, 42)?;
    executor.fork().ensure_ready()?;
    pool.close();
    Ok(())
}
#[test]
fn closed_callback_queue_rejects_start_and_releases_captured_job() -> TestResult {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(rx);
    let pool = Arc::new(DataCallbackPool::new(1));
    let executor =
        DataSubscriptionExecutor::for_session(tx, SessionId::from_allocated(1), pool.clone());
    check_eq!(
        executor.ensure_ready().err().map(|e| e.kind()),
        Some(ErrorKind::QueueClosed)
    )?;
    let retired = tokio_util::sync::CancellationToken::new();
    let guard = retired.clone().drop_guard();
    check_eq!(
        executor
            .submit(Box::new(move || drop(guard)))
            .err()
            .map(|e| e.kind()),
        Some(ErrorKind::QueueClosed)
    )?;
    check!(retired.is_cancelled())?;
    pool.close();
    Ok(())
}

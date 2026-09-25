use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::ws::*;

use super::*;
#[test]
fn unobserved_client_starts_no_state_or_event_workers() -> TestResult {
    let store = ListenerStore::new(&WebSocketClientConfig::default())?;
    check_eq!(store.state_executor.started_worker_count(), 0)?;
    check_eq!(store.event_executor.started_worker_count(), 0)?;
    store.clear()?;
    check_eq!(store.state_executor.started_worker_count(), 0)?;
    check_eq!(store.event_executor.started_worker_count(), 0)?;
    Ok(())
}
#[test]
fn state_and_event_workers_start_independently_and_close_is_idempotent() -> TestResult {
    let config = WebSocketClientConfig::default();
    let store = ListenerStore::new(&config)?;
    store.state_executor.ensure_started()?;
    check_eq!(
        store.state_executor.started_worker_count(),
        config.dispatch.state_callback_workers
    )?;
    check_eq!(store.event_executor.started_worker_count(), 0)?;
    store.event_executor.ensure_started()?;
    check_eq!(
        store.event_executor.started_worker_count(),
        config.dispatch.event_callback_workers
    )?;
    store.clear()?;
    store.clear()?;
    check!(store.state_executor.ensure_started().is_err())?;
    Ok(())
}

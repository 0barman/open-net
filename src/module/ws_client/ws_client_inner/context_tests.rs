//! V2 contract regression tests; fixtures construct native session authorities.
use crate::error::ErrorKind;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::ws::*;

#[test]
fn clients_allocate_distinct_identity_and_preserve_owned_configuration() -> TestResult {
    let (first, _) = super::super::test_support::new_inner(WebSocketClientConfig::default())?;
    let mut config = WebSocketClientConfig::default();
    config.queues.normal.max_items = 7;
    let (second, _) = super::super::test_support::new_inner(config)?;
    check!(first.id() != second.id())?;
    check_eq!(second.config().queues.normal.max_items, 7)?;
    Ok(())
}
#[test]
fn identity_exhaustion_never_wraps_into_an_existing_operation() -> TestResult {
    let next = std::sync::atomic::AtomicU64::new(u64::MAX);
    check_eq!(
        OperationId::allocate(&next).err().map(|e| e.kind()),
        Some(ErrorKind::ResourceExhausted)
    )?;
    check_eq!(next.load(std::sync::atomic::Ordering::Relaxed), u64::MAX)?;
    Ok(())
}

use super::*;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;
use crate::{error::ErrorKind, NetError};

#[test]
fn cancelled_groups_never_enter_the_indexed_queue() -> TestResult {
    let queue = PriorityWriteQueue::new(2, 32)?;
    let group = CancellationGroup::new();
    let item = fixture::queued(
        1,
        SendOptions {
            cancellation: Some(group.clone()),
            ..Default::default()
        },
    )?;
    group.cancel();
    let (_, error) = queue
        .push_existing(item)
        .err()
        .ok_or_else(|| test_error("cancelled group entered queue"))?;
    check_eq!(error.kind(), ErrorKind::Cancelled)?;
    check!(queue.try_next().is_none())?;
    Ok(())
}
#[test]
fn cancellation_removes_only_its_own_queue_node() -> TestResult {
    let queue = PriorityWriteQueue::new(3, 32)?;
    let first = fixture::queued(1, SendOptions::default())?;
    let token = first.dispatch_cancel.clone();
    queue.push_existing(first).map_err(|(_, e)| e)?;
    queue
        .push_existing(fixture::queued(2, SendOptions::default())?)
        .map_err(|(_, e)| e)?;
    check!(queue.cancel_queued_with_error(&token, NetError::from(ErrorKind::Cancelled)))?;
    check!(!queue.cancel_queued_with_error(&token, NetError::from(ErrorKind::Cancelled)))?;
    check_eq!(queue.try_next().map(|item| item.sequence), Some(2))?;
    check!(queue.try_next().is_none())?;
    Ok(())
}

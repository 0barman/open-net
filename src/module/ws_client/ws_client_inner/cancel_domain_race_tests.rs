//! V2 contract regression tests; fixtures construct native session authorities.
use crate::error::ErrorKind;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;

#[tokio::test]
async fn group_cancels_prepared_queued_and_writing_work_with_delivery_evidence() -> TestResult {
    for writing in [false, true] {
        let runtime = fixture::runtime(
            WebSocketClientConfig::default(),
            ResponseRouting::Disabled,
            true,
        )
        .await?;
        let group = CancellationGroup::new();
        let prepared = fixture::sender(&runtime)
            .message("payload")
            .options(SendOptions {
                cancellation: Some(group.clone()),
                ..Default::default()
            })
            .try_prepare()
            .map_err(|e| e.into_error())?;
        let receipt = prepared.commit()?;
        let mut queued = None;
        if writing {
            let item = runtime
                .queue
                .try_next()
                .ok_or_else(|| test_error("missing operation"))?;
            check!(item
                .dispatch_phase
                .mark_writing(ConnectionId::from_allocated(1)))?;
            check!(item.dispatch_phase.start_data_write(false)?)?;
            queued = Some(item);
        }
        group.cancel();
        check_eq!(
            receipt.written().await.err().map(|e| e.kind()),
            Some(if writing {
                ErrorKind::DeliveryUnknown
            } else {
                ErrorKind::Cancelled
            })
        )?;
        check_eq!(
            receipt.state()?.delivery,
            if writing {
                DeliveryEvidence::Unknown
            } else {
                DeliveryEvidence::NotStarted
            }
        )?;
        drop(queued);
        check!(runtime.queue.try_next().is_none())?;
        check_eq!(receipt.cancel()?, TerminationOutcome::AlreadyFinished)?;
    }
    Ok(())
}
#[test]
fn cancellation_hierarchy_and_drop_guards_preserve_sibling_ownership() -> TestResult {
    let root = CancellationGroup::new();
    let first = root.child();
    let second = root.child();
    drop(first.cancel_on_drop());
    check!(first.is_cancelled())?;
    check!(!root.is_cancelled())?;
    check!(!second.is_cancelled())?;
    let retained = second.cancel_on_drop().disarm();
    check!(!retained.is_cancelled())?;
    root.cancel();
    check!(second.is_cancelled())?;
    check!(root.child().is_cancelled())?;
    Ok(())
}
#[tokio::test]
async fn cancellation_racing_commit_leaves_no_queued_or_pending_work() -> TestResult {
    for _ in 0..32 {
        let runtime = fixture::runtime(
            WebSocketClientConfig::default(),
            ResponseRouting::Manual,
            true,
        )
        .await?;
        let group = CancellationGroup::new();
        let prepared = fixture::requests(&runtime)
            .request(fixture::request("race")?)
            .options(RequestOptions {
                send: SendOptions {
                    cancellation: Some(group.clone()),
                    ..Default::default()
                },
                ..Default::default()
            })
            .try_prepare()
            .map_err(|e| e.into_error())?;
        let handle = prepared.handle().clone();
        let cancel = std::thread::spawn(move || group.cancel());
        let result = prepared.commit();
        cancel
            .join()
            .map_err(|_| test_error("cancel thread failed"))?;
        if let Ok(receipt) = result {
            check!(receipt.response().await.is_err())?;
        }
        check!(handle.state()?.result.is_some())?;
        check!(runtime.pending.snapshot()?.is_empty())?;
        check!(runtime.queue.try_next().is_none())?;
    }
    Ok(())
}

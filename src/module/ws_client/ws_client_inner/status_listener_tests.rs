//! V2 contract regression tests; fixtures construct native session authorities.
use crate::module::ws_client::{
    test_support::{check, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;

#[tokio::test]
async fn state_receiver_drop_does_not_cancel_lifecycle_and_terminal_snapshot_is_retained(
) -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Disabled,
        true,
    )
    .await?;
    let mut states = runtime.lifecycle.watch_state()?;
    check!(matches!(
        states.current().state,
        ConnectionState::Connected(_)
    ))?;
    let events = runtime.lifecycle.subscribe_events()?;
    drop(events);
    check!(!runtime.lifecycle.cancel_token().is_cancelled())?;
    runtime
        .lifecycle
        .terminate(TerminationReason::LocalClose, None)?;
    check!(matches!(states.current().state, ConnectionState::Closed(_)))?;
    while states.recv().await?.is_some() {}
    check!(matches!(
        runtime.lifecycle.snapshot()?.state,
        ConnectionState::Closed(_)
    ))?;
    Ok(())
}

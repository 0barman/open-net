use super::*;
use crate::module::ws_client::{
    test_support::{check, TestResult},
    v2_test_support as fixture,
};

#[tokio::test]
async fn incoming_receiver_detach_returns_quota_and_does_not_end_the_session() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.dispatch.message_subscriptions = 1;
    let runtime = fixture::runtime(config, crate::ws::ResponseRouting::Disabled, true).await?;
    let receiver = runtime
        .messages
        .subscribe(crate::ws::ReceiveOptions::default())?;
    check!(runtime
        .messages
        .subscribe(crate::ws::ReceiveOptions::default())
        .is_err())?;
    drop(receiver);
    check!(runtime
        .messages
        .subscribe(crate::ws::ReceiveOptions::default())
        .is_ok())?;
    check!(!runtime.is_closed())?;
    runtime.messages.finish();
    check!(runtime
        .messages
        .subscribe(crate::ws::ReceiveOptions::default())
        .is_err())?;
    Ok(())
}

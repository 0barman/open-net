use super::*;
use crate::module::ws_client::{
    test_support::{check, test_error, TestResult},
    v2_test_support as fixture,
};

#[tokio::test]
async fn state_callbacks_receive_initial_before_latest_and_close_waits_for_retirement() -> TestResult
{
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        crate::ws::ResponseRouting::Disabled,
        false,
    )
    .await?;
    let (seen, mut received) = mpsc::unbounded_channel();
    let states = runtime.lifecycle.watch_state()?;
    let subscription = states.into_callback(move |_, result| {
        let _ = seen.send(result);
    })?;
    let first = fixture::bounded(received.recv())
        .await?
        .ok_or_else(|| test_error("initial state missing"))??;
    check!(matches!(
        first.state,
        crate::ws::ConnectionState::Connecting
    ))?;
    runtime.lifecycle.begin_cycle(1, false)?;
    runtime.lifecycle.waiting_for_network(1)?;
    fixture::bounded(async {
        loop {
            let state = received
                .recv()
                .await
                .ok_or_else(|| test_error("callback closed early"))??;
            if matches!(state.state, crate::ws::ConnectionState::WaitingForNetwork) {
                break Ok::<(), crate::module::ws_client::test_support::TestError>(());
            }
        }
    })
    .await??;
    subscription.close().await?;
    check!(!runtime.lifecycle.cancel_token().is_cancelled())?;
    Ok(())
}

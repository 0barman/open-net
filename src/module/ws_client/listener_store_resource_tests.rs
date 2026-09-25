use crate::error::ErrorKind;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::ws::*;
use std::sync::Arc;

use super::*;
#[test]
fn observer_quotas_are_shared_across_lifecycles_and_release_on_receiver_drop() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.dispatch.state_subscriptions = 1;
    config.dispatch.event_subscriptions = 1;
    let store = ListenerStore::new(&config)?;
    let make = |id| {
        crate::module::ws_client::connection_session::ConnectionSession::new_with_observers(
            1,
            id,
            None,
            Arc::new(crate::Metadata::new()),
            EventOptions::default(),
            store.connection_observers(),
        )
    };
    let (first, _) = make(1)?;
    let (second, _) = make(2)?;
    let state = first.watch_state()?;
    check_eq!(
        second.watch_state().err().map(|e| e.kind()),
        Some(ErrorKind::SubscriptionLimitReached)
    )?;
    drop(state);
    check!(second.watch_state().is_ok())?;
    let event = first.subscribe_events()?;
    check_eq!(
        second.subscribe_events().err().map(|e| e.kind()),
        Some(ErrorKind::SubscriptionLimitReached)
    )?;
    drop(event);
    check!(second.subscribe_events().is_ok())?;
    Ok(())
}

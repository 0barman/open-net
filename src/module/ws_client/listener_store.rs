use crate::error::NetError;
use crate::module::ws_client::listener_executor::ListenerExecutor;
use crate::ws::WebSocketClientConfig;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Client-wide quotas and lazy executors shared by each native session.
pub(crate) struct ListenerStore {
    state_quota: Arc<Semaphore>,
    state_executor: Arc<ListenerExecutor>,
    event_quota: Arc<Semaphore>,
    event_executor: Arc<ListenerExecutor>,
}

#[derive(Clone)]
pub(crate) struct ConnectionObservers {
    pub(crate) state_executor: Arc<dyn crate::subscription::CallbackExecutor>,
    pub(crate) state_quota: Arc<Semaphore>,
    pub(crate) event_executor: Arc<dyn crate::subscription::CallbackExecutor>,
    pub(crate) event_quota: Arc<Semaphore>,
}

impl ListenerStore {
    pub(crate) fn new(config: &WebSocketClientConfig) -> Result<Self, NetError> {
        config.dispatch.validate()?;
        Ok(Self {
            state_quota: Arc::new(Semaphore::new(config.dispatch.state_subscriptions)),
            state_executor: ListenerExecutor::new(
                "open-net-ws-status",
                config.dispatch.state_callback_workers,
                config.dispatch.state_subscriptions,
            )?,
            event_quota: Arc::new(Semaphore::new(config.dispatch.event_subscriptions)),
            event_executor: ListenerExecutor::new(
                "open-net-ws-events",
                config.dispatch.event_callback_workers,
                config.dispatch.event_subscriptions,
            )?,
        })
    }

    pub(crate) fn connection_observers(&self) -> ConnectionObservers {
        ConnectionObservers {
            state_executor: self.state_executor.clone(),
            state_quota: self.state_quota.clone(),
            event_executor: self.event_executor.clone(),
            event_quota: self.event_quota.clone(),
        }
    }

    pub(crate) fn clear(&self) -> Result<(), NetError> {
        let mut result = Ok(());
        for (lane, closing) in [
            ("state", self.state_executor.close()),
            ("event", self.event_executor.close()),
        ] {
            if let Err(error) = closing {
                crate::log_e!(crate::LogType::WSC; "listener_store_close", "lane|kind", lane, format!("{:?}", error.kind()));
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        result
    }
}

#[cfg(test)]
#[path = "listener_store_lazy_tests.rs"]
mod lazy_tests;

#[cfg(test)]
#[path = "listener_store_resource_tests.rs"]
mod resource_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check_eq, TestResult};
    #[test]
    fn connection_observers_reuse_client_quotas() -> TestResult {
        let store = ListenerStore::new(&WebSocketClientConfig::default())?;
        let first = store.connection_observers();
        let second = store.connection_observers();
        check_eq!(Arc::ptr_eq(&first.state_quota, &second.state_quota), true)?;
        check_eq!(Arc::ptr_eq(&first.event_quota, &second.event_quota), true)?;
        Ok(())
    }
}

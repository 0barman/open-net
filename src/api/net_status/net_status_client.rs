use std::fmt;
use std::sync::Arc;

use crate::common::CommonEngine;
use crate::net_status::NetworkSnapshot;
use crate::subscription::{CallbackContext, StateReceiver, Subscription};
use crate::Result;

use crate::module::net_status::inner::inner_net_status_client::InnerNetStatusClient;

/// A network monitor created by `OpenNet`; clones share one lifecycle.
#[derive(Clone)]
pub struct NetStatusClient {
    inner: Arc<InnerNetStatusClient>,
}

impl NetStatusClient {
    pub(crate) fn new(engine: Arc<CommonEngine>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(InnerNetStatusClient::new(engine)?),
        })
    }

    /// Start monitoring and await the first coherent observation.
    /// Repeated calls share the active monitor and its initialization result.
    pub async fn start(&self) -> Result<NetworkSnapshot> {
        self.inner.start().await
    }

    /// Stop monitoring while keeping subscriptions alive for a later restart.
    pub async fn stop(&self) -> Result<()> {
        self.inner.stop().await
    }

    /// Permanently close monitoring and publish its final Closed snapshot.
    pub async fn shutdown(&self) -> Result<()> {
        self.inner.shutdown().await
    }

    /// Read the latest coherent snapshot, including an inactive or final state.
    pub fn snapshot(&self) -> Result<NetworkSnapshot> {
        self.inner.snapshot()
    }

    /// Subscribe to the current snapshot and later changes across stop/restart.
    pub fn subscribe(&self) -> Result<StateReceiver<NetworkSnapshot>> {
        self.inner.subscribe_state()
    }

    /// Observe the same snapshots as an asynchronous subscription on the shared
    /// callback pool. Keep the returned owner alive while observing changes.
    pub fn on_change<F>(&self, callback: F) -> Result<Subscription>
    where
        F: Fn(CallbackContext, Result<NetworkSnapshot>) + Send + Sync + 'static,
    {
        self.subscribe()?.into_callback(callback)
    }

    pub(crate) fn request_destroy(&self) {
        self.inner.request_destroy();
    }

    pub(crate) async fn destroy(&self) -> Result<()> {
        self.inner.destroy().await
    }
}

impl fmt::Debug for NetStatusClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NetStatusClient")
            .finish_non_exhaustive()
    }
}

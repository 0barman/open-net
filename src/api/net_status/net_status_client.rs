use std::fmt;
use std::sync::Arc;

use crate::net_status::NetworkSnapshot;
use crate::net_status::NetworkStatusContext;
use crate::subscription::{CallbackContext, StateReceiver, Subscription};
use crate::{LogType, Result};

use crate::module::net_status::inner::facade::NetworkObservationOwner;

/// An independent view of the engine's shared network monitor.
/// Clones share this view's lifecycle; other named views remain independent.
#[derive(Clone)]
pub struct NetStatusClient {
    inner: Arc<NetworkObservationOwner>,
}

impl NetStatusClient {
    pub(crate) fn new(context: NetworkStatusContext) -> Result<Self> {
        crate::log_s!(LogType::Engine; "NetStatusClient-new");
        Ok(Self {
            inner: NetworkObservationOwner::new(context)?,
        })
    }

    /// Start monitoring and await the first coherent observation.
    /// Repeated calls share the active monitor and its initialization result.
    pub async fn start(&self) -> Result<NetworkSnapshot> {
        crate::log_s!(LogType::Engine; "NetStatusClient-start");
        self.inner.start().await
    }

    /// Stop monitoring while keeping subscriptions alive for a later restart.
    pub async fn stop(&self) -> Result<()> {
        crate::log_s!(LogType::Engine; "NetStatusClient-stop");
        self.inner.request_stop()?;
        self.inner.wait_cleanup().await
    }

    /// Permanently close monitoring and publish its final Closed snapshot.
    pub async fn shutdown(&self) -> Result<()> {
        crate::log_s!(LogType::Engine; "NetStatusClient-shutdown");
        self.inner.request_close();
        self.inner.wait_cleanup().await
    }

    /// Read the latest coherent snapshot, including an inactive or final state.
    pub fn snapshot(&self) -> Result<NetworkSnapshot> {
        self.inner.snapshot()
    }

    /// Subscribe to the current snapshot and later changes across stop/restart.
    pub fn subscribe(&self) -> Result<StateReceiver<NetworkSnapshot>> {
        self.inner.subscribe_terminal()
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
        self.inner.request_close();
    }

    pub(crate) async fn destroy(&self) -> Result<()> {
        crate::log_s!(LogType::Engine; "NetStatusClient-destroy");
        self.inner.request_close();
        self.inner.wait_cleanup().await
    }
}

impl fmt::Debug for NetStatusClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NetStatusClient")
            .finish_non_exhaustive()
    }
}

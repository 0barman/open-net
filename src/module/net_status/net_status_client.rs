use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::common::log::listener::LogListener;
use crate::common::CommonEngine;

use super::inner::inner_net_status_client::InnerNetStatusClient;
use super::{IpStack, NetworkStatus};
use crate::api::net_error::NetError;

/// Receives reachability changes on the shared engine callback pool.
/// Callbacks may use Tokio APIs and may run concurrently. Panics are contained.
pub type NetworkStatusListener = Box<dyn Fn(NetworkStatus) + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NetworkStatusListenerHandle(u64);

impl NetworkStatusListenerHandle {
    pub(crate) fn from_raw(id: u64) -> Self {
        Self(id)
    }
}

/// Network status client created by `OpenNet::create_net_status_client`.
/// Clones share monitoring state and listeners. Creation does not start monitoring.
#[derive(Clone)]
pub struct NetStatusClient {
    inner: Arc<InnerNetStatusClient>,
}

impl NetStatusClient {
    pub(crate) fn new(engine: Arc<CommonEngine>) -> Self {
        Self {
            inner: Arc::new(InnerNetStatusClient::new(engine)),
        }
    }

    /// Start monitoring on OpenNet's shared runtime and wait for the initial
    /// network snapshot. Repeated calls reuse the active monitor. Monitoring
    /// may restart after `shutdown`, but not after the owning OpenNet is destroyed.
    pub async fn start(&self) -> Result<(), NetError> {
        self.inner.start().await
    }

    /// Stop monitoring, clear status listeners and wait for native monitor
    /// resources to be released. Does not stop OpenNet or its other clients.
    /// Queued callbacks are skipped; callbacks already running may finish.
    pub async fn shutdown(&self) -> Result<(), NetError> {
        self.inner.shutdown().await
    }

    pub(crate) fn request_destroy(&self) {
        self.inner.request_destroy();
    }

    pub(crate) async fn destroy(&self) -> Result<(), NetError> {
        self.inner.destroy().await
    }

    /// Current local reachability; `Unavailable` before start and after shutdown.
    pub fn local_network_reachability(&self) -> Result<NetworkStatus, NetError> {
        self.inner.local_network_reachability()
    }

    /// Available local IP versions, not per-protocol Internet reachability.
    /// Returns `None` before start and after shutdown.
    pub fn ip_stack(&self) -> Result<IpStack, NetError> {
        self.inner.ip_stack()
    }

    pub fn has_ipv4(&self) -> Result<bool, NetError> {
        Ok(self.ip_stack()?.has_ipv4())
    }
    pub fn has_ipv6(&self) -> Result<bool, NetError> {
        Ok(self.ip_stack()?.has_ipv6())
    }

    /// Register a listener for subsequent reachability changes. Returns `None`
    /// while stopped. Registration does not synthesize an initial notification.
    pub fn register(
        &self,
        listener: NetworkStatusListener,
    ) -> Result<Option<NetworkStatusListenerHandle>, NetError> {
        self.inner.register(listener)
    }

    /// Remove a listener. A callback already submitted may still run.
    pub fn unregister(&self, handle: NetworkStatusListenerHandle) -> Result<bool, NetError> {
        self.inner.unregister(handle)
    }

    /// Clear status listeners. Returns `NotStarted` while stopped.
    pub fn clear_all_listener(&self) -> Result<(), NetError> {
        self.inner.clear_all_listener()
    }

    pub fn is_started(&self) -> bool {
        self.inner.is_started()
    }
    pub fn is_shutdown(&self) -> bool {
        !self.is_started()
    }

    /// Windows: prefer the current Wi-Fi SSID, then the connected network name.
    /// Returns `None` while stopped or when the platform cannot resolve a name.
    pub fn get_current_network_name(&self) -> Result<Option<String>, NetError> {
        self.inner.get_current_network_name()
    }

    /// Query usable physical LAN IPv4 candidates directly, even while stopped.
    /// VPN/TUN adapters and known proxy address ranges are excluded.
    pub fn preferred_lan_ipv4() -> Option<Ipv4Addr> {
        super::lan_addr::preferred_lan_ipv4()
    }

    /// Replace or remove this client's diagnostic log subscription. The
    /// subscription observes Engine and Common logs across instances, survives
    /// ordinary shutdown, and runs on a dedicated callback thread.
    /// Thread creation failure preserves the previous listener.
    pub fn set_log_listener(&self, listener: Option<LogListener>) {
        self.inner.set_log_listener(listener);
    }

    /// Like `set_log_listener`, returning any callback-thread creation error.
    pub fn try_set_log_listener(&self, listener: Option<LogListener>) -> std::io::Result<()> {
        self.inner.try_set_log_listener(listener)
    }
}

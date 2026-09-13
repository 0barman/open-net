use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use super::network_status_snapshot::NetworkStatusPublisher;
use crate::module::net_status::{IpStack, NetworkStatus, NetworkStatusListenerHandle};

// IDs span instances and restarts, so stale handles cannot remove another listener.
static NEXT_LISTENER_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) type SharedListener = Arc<dyn Fn(NetworkStatus) + Send + Sync + 'static>;
pub(crate) type Dispatcher = Arc<dyn Fn(SharedListener, NetworkStatus) + Send + Sync + 'static>;

/// Each start owns a fresh state. Retiring monitors cannot write a later
/// generation's values or listeners.
pub(crate) struct MonitorState {
    pub(crate) reachability: NetworkStatus,
    pub(crate) ip_stack: IpStack,
    pub(crate) listeners: HashMap<NetworkStatusListenerHandle, SharedListener>,
    pub(crate) active: Arc<AtomicBool>,
    pub(super) observation: Option<NetworkStatusPublisher>,
}

impl Default for MonitorState {
    fn default() -> Self {
        Self {
            reachability: NetworkStatus::Unavailable,
            ip_stack: IpStack::None,
            listeners: HashMap::new(),
            active: Arc::new(AtomicBool::new(true)),
            observation: None,
        }
    }
}

impl MonitorState {
    pub(crate) fn next_listener_handle(&mut self) -> NetworkStatusListenerHandle {
        loop {
            let id = NEXT_LISTENER_ID
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1);
            if id == 0 {
                continue;
            }
            let handle = NetworkStatusListenerHandle::from_raw(id);
            if !self.listeners.contains_key(&handle) {
                return handle;
            }
        }
    }
}

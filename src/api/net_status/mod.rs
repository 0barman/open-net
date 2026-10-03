//! Shared network monitoring with versioned snapshots and owned subscriptions.

mod ip_stack;
mod net_status_client;
mod network_status;

pub use crate::module::net_status::inner::shared::NetworkStatusContext;
use crate::NetError;
pub use ip_stack::IpStack;
pub use net_status_client::NetStatusClient;
pub use network_status::NetworkStatus;
use std::time::SystemTime;

/// One coherent observation, or an explicit state with no current observation.
#[derive(Clone, Debug)]
pub struct NetworkSnapshot {
    /// Monotonically increasing snapshot revision.
    pub revision: u64,
    /// Counter identifying the current loss/recovery epoch.
    pub loss_epoch: u64,
    /// Lifecycle state at the time of observation.
    pub state: MonitorState,
    /// Reachability observation, when one has been obtained.
    pub reachability: Option<NetworkStatus>,
    /// Locally available IP protocol stack, when known.
    pub ip_stack: Option<IpStack>,
    /// Timestamp at which the observation was collected.
    pub observed_at: Option<SystemTime>,
    /// Human-readable network identifier supplied by the platform, when available.
    pub network_name: Option<String>,
}

/// The lifecycle shared by every clone of a network status client.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum MonitorState {
    /// Monitoring has not been started.
    Stopped,
    /// Monitoring startup is in progress.
    Starting,
    /// Monitoring is active and publishing observations.
    Running,
    /// Startup or monitoring failed; contains the classified cause.
    Failed(NetError),
    /// Monitoring was permanently shut down.
    Closed,
}

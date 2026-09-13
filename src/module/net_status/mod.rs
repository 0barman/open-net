//! Network reachability, IP-stack capability and LAN address selection.
//!
//! Available in every open-net build; create a client through `OpenNet`.
pub(crate) mod inner;
mod ip_stack;
mod lan_addr;
mod net_status_client;
mod network_status;

pub use ip_stack::IpStack;
pub use net_status_client::{NetStatusClient, NetworkStatusListener, NetworkStatusListenerHandle};
pub use network_status::NetworkStatus;

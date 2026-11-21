//! Internal network monitoring and LAN address selection.
pub(crate) mod inner;
#[cfg(windows)]
pub(crate) mod interface_events;
pub(crate) mod lan_addr;
mod lan_platform;
pub(crate) mod lan_snapshot;
pub(crate) mod logging;

pub(crate) use crate::api::net_status::{IpStack, NetworkStatus};

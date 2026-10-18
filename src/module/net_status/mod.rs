//! Internal network monitoring and LAN address selection.
pub(crate) mod inner;
pub(crate) mod lan_addr;
mod lan_platform;
pub(crate) mod lan_snapshot;
mod logging;

pub(crate) use crate::api::net_status::{IpStack, NetworkStatus};

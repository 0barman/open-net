//! Internal network monitoring and LAN address selection.
pub(crate) mod inner;
pub(crate) mod lan_addr;

pub(crate) use crate::api::net_status::{IpStack, NetworkStatus};

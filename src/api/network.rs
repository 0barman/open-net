//! Network configuration and local-address discovery helpers.

use crate::api::NetError;
use std::net::Ipv4Addr;

#[cfg(feature = "ws-client")]
pub use crate::api::network_config::{
    ClientIdentity, NetworkConfig, NetworkStatusPolicy, ProxyBasicAuth, ProxyConfig,
    RootCertificateMode, TlsConfig,
};

/// Enumerate the local interfaces and pick the LAN IPv4 address most suitable
/// to advertise to LAN peers.
///
/// Replaces the old `connect(8.8.8.8)` route probe: under a VPN / Clash
/// global TUN setup that probe resolves to a virtual adapter address (e.g.
/// `198.18.x.x`), which LAN peers cannot reach. Enumerating interfaces plus
/// the curated subnet/name filters avoids virtual adapters by construction.
///
/// Returns `None` when no usable LAN address exists (the caller should then
/// refrain from advertising an address at all, rather than pretending with a
/// fallback like `127.0.0.1`).
pub fn preferred_lan_ipv4() -> Result<Option<Ipv4Addr>, NetError> {
    crate::module::net_status::lan_addr::preferred_lan_ipv4()
}

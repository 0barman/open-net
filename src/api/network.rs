//! Network configuration and local-address discovery helpers.

#[cfg(feature = "net-status")]
use crate::api::NetError;
#[cfg(feature = "net-status")]
use std::net::Ipv4Addr;

#[cfg(feature = "net-status")]
mod lan_ipv4;
#[cfg(feature = "net-status")]
pub use lan_ipv4::{
    LanInterfaceKind, LanIpv4Candidate, LanIpv4Completeness, LanIpv4Exclusion,
    LanIpv4RecommendationReason, LanIpv4Snapshot,
};

#[cfg(feature = "ws-client")]
pub use crate::api::network_config::{
    ClientIdentity, NetworkConfig, NetworkStatusPolicy, ProxyAuthTransportPolicy, ProxyBasicAuth,
    ProxyConfig, RootCertificateMode, TlsConfig,
};

/// Select a LAN IPv4 using the original compatibility heuristic.
///
/// Preserves its subnet/name filters and 192.168 > 10 > 172.16/12 ordering;
/// ties keep OS enumeration order. It does not inspect native link/type/route
/// evidence and can prefer an inactive bridge reported as operational up.
/// Use [`lan_ipv4_snapshot`] for evidence-based selection and full diagnostics.
/// None means no candidate passed this policy, not proof of no usable network.
#[cfg(feature = "net-status")]
pub fn preferred_lan_ipv4() -> Result<Option<Ipv4Addr>, NetError> {
    crate::module::net_status::lan_addr::preferred_lan_ipv4()
}

/// Synchronously collect local IPv4 candidates and an explainable recommendation.
///
/// Does not require an engine, runtime, monitor, or Internet access. Blocks the
/// caller while querying the OS; no remote probe is sent. Prefer a blocking
/// worker when integrating into an async or UI application.
///
/// Enumeration failure returns Err with the original I/O error. A successful
/// empty inventory is Ok; auxiliary metadata failures produce a Partial result
/// with candidates and diagnostics. Such a result must not be used as proof that
/// a previously healthy application's endpoint disappeared.
///
/// Native interface/link/default-route evidence precedes address preference;
/// the three private IPv4 ranges are equally ranked. Excluded VPN/CGNAT and
/// other addresses remain visible for application-specific selection. The
/// recommendation is not a guarantee of reachability, successful binding, or
/// an atomic view of the OS. No monitoring event or cached snapshot is changed.
///
/// ```no_run
/// # fn main() -> open_net::Result<()> {
/// let lan = open_net::network::lan_ipv4_snapshot()?;
/// for candidate in lan.candidates() {
///     println!("{} {}: {:?}", candidate.interface_name(), candidate.address(),
///         candidate.default_exclusions());
/// }
/// if let Some(selected) = lan.recommended() {
///     println!("recommended {}: {:?}", selected.address(), selected.recommendation_reason());
/// }
/// # Ok(()) }
/// ```
#[cfg(feature = "net-status")]
pub fn lan_ipv4_snapshot() -> Result<LanIpv4Snapshot, NetError> {
    crate::module::net_status::lan_snapshot::query()
}

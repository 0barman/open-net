//! Owned LAN inventory and the evidence behind its default recommendation.

use std::net::Ipv4Addr;
use std::time::SystemTime;

/// Interface role reported by platform metadata. This is not proof of reachability.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LanInterfaceKind {
    Wifi,
    Ethernet,
    Bridge,
    Tunnel,
    Virtual,
    Loopback,
    #[default]
    Unknown,
}

/// Whether the auxiliary platform collectors reported a failure.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LanIpv4Completeness {
    /// Collection completed without a reported failure. Individual optional
    /// facts can still be unknown; this is not an atomic OS-wide inventory.
    Complete,
    /// Interface addresses were obtained, but some platform evidence is missing.
    /// Inspect diagnostics and optional facts; absence is not proof of removal.
    Partial,
}

/// Why the default LAN advertisement policy does not recommend a candidate.
/// Policy exclusions such as VPN/CGNAT do not mean the address is unusable by
/// an application that understands its own topology.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LanIpv4Exclusion {
    InterfaceDown,
    InterfaceStateUnknown,
    PointToPoint,
    PointToPointStateUnknown,
    VirtualInterfaceNameHint,
    PlatformVirtualOrTunnel,
    LinkInactive,
    Loopback,
    LinkLocal,
    Unspecified,
    Broadcast,
    Multicast,
    ThisNetwork,
    Reserved,
    SharedAddressSpace,
    BenchmarkRange,
}

/// The strongest interface evidence supporting the selected candidate.
/// These variants describe a heuristic, not a successful peer connection.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LanIpv4RecommendationReason {
    ActiveLanDefaultRoute,
    ActiveWifiOrEthernet,
    ActiveBridge,
    LanLinkUnknown,
    MetadataUnknown,
    BridgeLinkUnknown,
}

/// One assigned IPv4 address, including candidates excluded by the default policy.
/// Multiple entries may belong to the same interface or share an IP address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LanIpv4Candidate {
    pub(crate) interface_id: String,
    pub(crate) interface_name: String,
    pub(crate) interface_index: Option<u32>,
    pub(crate) address: Ipv4Addr,
    pub(crate) netmask: Ipv4Addr,
    pub(crate) prefix_len: u8,
    pub(crate) interface_kind: LanInterfaceKind,
    pub(crate) oper_up: Option<bool>,
    pub(crate) point_to_point: Option<bool>,
    pub(crate) link_active: Option<bool>,
    pub(crate) default_route: Option<bool>,
    pub(crate) exclusions: Vec<LanIpv4Exclusion>,
    pub(crate) recommendation: Option<LanIpv4RecommendationReason>,
}

impl LanIpv4Candidate {
    /// Platform interface identity (adapter name on Windows, interface name on
    /// Unix). Stable for sorting this inventory, not a permanent hardware ID.
    pub fn interface_id(&self) -> &str {
        &self.interface_id
    }

    pub fn interface_name(&self) -> &str {
        &self.interface_name
    }

    /// Native interface index, when supplied. It can be reused after removal.
    pub fn interface_index(&self) -> Option<u32> {
        self.interface_index
    }

    pub fn address(&self) -> Ipv4Addr {
        self.address
    }

    pub fn netmask(&self) -> Ipv4Addr {
        self.netmask
    }

    pub fn prefix_len(&self) -> u8 {
        self.prefix_len
    }

    pub fn interface_kind(&self) -> LanInterfaceKind {
        self.interface_kind
    }

    /// Operational status from address enumeration. On Unix, Up is derived
    /// from IFF_RUNNING and does not establish physical carrier or peer access.
    pub fn is_oper_up(&self) -> Option<bool> {
        self.oper_up
    }

    pub fn is_point_to_point(&self) -> Option<bool> {
        self.point_to_point
    }

    /// Additional native link/media evidence, independently of operational flags.
    pub fn link_active(&self) -> Option<bool> {
        self.link_active
    }

    /// Preferred IPv4 default-interface evidence from the platform collector.
    /// None means unknown. This is not a destination-specific route lookup:
    /// VPN split routes and policy routing can select a different actual path.
    pub fn is_default_route(&self) -> Option<bool> {
        self.default_route
    }

    /// All applicable default-policy exclusions, in a deterministic order.
    /// An empty slice does not guarantee reachability from any particular peer.
    pub fn default_exclusions(&self) -> &[LanIpv4Exclusion] {
        &self.exclusions
    }

    /// Present only for the one recommended entry in this snapshot.
    pub fn recommendation_reason(&self) -> Option<LanIpv4RecommendationReason> {
        self.recommendation
    }

    pub fn is_recommended(&self) -> bool {
        self.recommendation.is_some()
    }
}

/// A fresh, self-contained IPv4 inventory and its default LAN recommendation.
/// It owns ordinary data, not an engine, subscription, or native handle.
#[derive(Clone, Debug)]
pub struct LanIpv4Snapshot {
    pub(crate) candidates: Vec<LanIpv4Candidate>,
    pub(crate) completeness: LanIpv4Completeness,
    pub(crate) diagnostics: Vec<String>,
    pub(crate) query_started_at: SystemTime,
    pub(crate) observed_at: SystemTime,
}

impl LanIpv4Snapshot {
    /// All IPv4 entries returned by the address enumerator, including excluded
    /// entries, sorted by interface identity/address. No IP-only deduplication.
    /// This does not include every OS address state (for example Windows DAD
    /// tentative addresses), IPv6, or interfaces without an assigned IPv4.
    pub fn candidates(&self) -> &[LanIpv4Candidate] {
        &self.candidates
    }

    /// At most one entry selected using interface/link/route evidence, with all
    /// three RFC1918 ranges equally ranked. None is an honest lack of a default
    /// recommendation, not permission to advertise loopback or an arbitrary IP.
    /// A Partial snapshot can have a recommendation based on remaining facts.
    pub fn recommended(&self) -> Option<&LanIpv4Candidate> {
        self.candidates
            .iter()
            .find(|candidate| candidate.is_recommended())
    }

    pub fn completeness(&self) -> LanIpv4Completeness {
        self.completeness
    }

    /// Human-readable collection failures. Do not parse these strings as an API.
    pub fn diagnostics(&self) -> &[String] {
        &self.diagnostics
    }

    /// Wall-clock time before enumeration began. Collection spans a window;
    /// addresses and metadata are not captured in an OS-wide transaction.
    pub fn query_started_at(&self) -> SystemTime {
        self.query_started_at
    }

    /// Wall-clock time when collection finished. Clock adjustments can make
    /// this earlier than query_started_at; neither value is a validity lease.
    pub fn observed_at(&self) -> SystemTime {
        self.observed_at
    }
}

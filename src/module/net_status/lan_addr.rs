//! Curated heuristics that pick the LAN IPv4 address a host should advertise
//! to its LAN peers.
//!
//! The naive approach — inferring the outbound address from a UDP `connect` to
//! a public IP, or trusting whatever `local-ip-address` / `netdev` return —
//! breaks under global-proxy / TUN setups (Clash fake-ip, WireGuard,
//! Tailscale, ...): the picked address then belongs to a virtual adapter and is
//! unreachable from other LAN hosts.
//!
//! This module instead enumerates the local interfaces and applies a curated
//! filter:
//!
//! 1. Hard exclusions: loopback, link-local, unspecified, broadcast, the Clash
//!    fake-ip / benchmark range `198.18.0.0/15`, and the CGNAT range
//!    `100.64.0.0/10` (Tailscale and some VPNs).
//! 2. Interfaces that are down, point-to-point (TUN/VPN adapters on Windows),
//!    or whose name matches a virtual-adapter blacklist (wintun, zerotier,
//!    hyper-v, wsl, docker, ...) are dropped.
//! 3. Survivors are ranked by private-segment priority: `192.168.0.0/16`
//!    first, then `10.0.0.0/8`, then `172.16.0.0/12`, with any other routable
//!    address as the last-resort fallback.
//!
//! When no candidate survives, the function honestly returns `None` instead of
//! falling back to a useless address (e.g. `127.0.0.1`).

use std::net::Ipv4Addr;

/// Priority score for a LAN interface candidate (a larger rank wins).
///
/// Used to pick the "most like a real physical LAN interface" address in
/// multi-homed environments; a `None` classification means the address is
/// eliminated outright (see [`classify_lan_ipv4`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LanScore {
    /// 192.168.0.0/16 — the most common home/office LAN segment.
    PrivateClassC,
    /// 10.0.0.0/8.
    PrivateClassA,
    /// 172.16.0.0/12.
    PrivateClassB,
    /// Any other routable IPv4 (last-resort fallback, only when no private
    /// segment exists at all).
    OtherRoutable,
}

impl LanScore {
    fn rank(self) -> u8 {
        match self {
            LanScore::PrivateClassC => 4,
            LanScore::PrivateClassA => 3,
            LanScore::PrivateClassB => 2,
            LanScore::OtherRoutable => 1,
        }
    }
}

/// Interface names containing any of these substrings (case-insensitive) are
/// treated as virtual/proxy adapters and excluded. The subnet filter (see
/// [`classify_lan_ipv4`]) is the primary defense; the name is only a
/// secondary hint, so it matches well-known English feature words only and
/// never risks killing a localized (e.g. CJK) adapter name.
const VIRTUAL_IFACE_NAME_HINTS: &[&str] = &[
    "clash",
    "wintun",
    "tun",
    "tap",
    "utun",
    "vpn",
    "wireguard",
    "wg",
    "tailscale",
    "zerotier",
    "hyper-v",
    "vethernet",
    "vmware",
    "virtualbox",
    "vbox",
    "loopback",
    "wsl",
    "docker",
];

/// Whether `ip` falls inside `base/prefix` (pure octet comparison; no
/// third-party CIDR library).
fn in_cidr(ip: Ipv4Addr, base: [u8; 4], prefix: u8) -> bool {
    let ip_bits = u32::from_be_bytes(ip.octets());
    let base_bits = u32::from_be_bytes(base);
    if prefix == 0 {
        return true;
    }
    let mask: u32 = u32::MAX << (32 - prefix);
    (ip_bits & mask) == (base_bits & mask)
}

/// Classify a single IPv4 address: `Some(score)` for a usable LAN candidate,
/// `None` for an address that must be excluded (loopback / link-local /
/// VPN-TUN characteristic ranges).
fn classify_lan_ipv4(ip: Ipv4Addr) -> Option<LanScore> {
    // Hard exclusions: loopback (127/8), link-local (169.254/16), unspecified
    // (0.0.0.0), broadcast.
    if ip.is_loopback() || ip.is_link_local() || ip.is_unspecified() || ip.is_broadcast() {
        return None;
    }
    // Virtual/proxy characteristic ranges:
    //  198.18.0.0/15 — Clash TUN benchmark / fake-ip default range.
    //  100.64.0.0/10 — CGNAT, used by Tailscale and some VPNs.
    if in_cidr(ip, [198, 18, 0, 0], 15) || in_cidr(ip, [100, 64, 0, 0], 10) {
        return None;
    }
    // Private LAN segments, ranked by how common they are.
    if in_cidr(ip, [192, 168, 0, 0], 16) {
        return Some(LanScore::PrivateClassC);
    }
    if in_cidr(ip, [10, 0, 0, 0], 8) {
        return Some(LanScore::PrivateClassA);
    }
    if in_cidr(ip, [172, 16, 0, 0], 12) {
        return Some(LanScore::PrivateClassB);
    }
    // Any other routable address as a fallback (the rare bridged LAN that
    // legitimately uses a public segment).
    Some(LanScore::OtherRoutable)
}

/// Whether an interface name hits a virtual/proxy feature word.
fn looks_like_virtual_iface(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    VIRTUAL_IFACE_NAME_HINTS
        .iter()
        .any(|hint| lower.contains(hint))
}

/// An interface candidate (the testable subset extracted from
/// `if_addrs::Interface`).
struct IfaceCandidate {
    name: String,
    ip: Ipv4Addr,
    /// Whether the interface is operational up.
    oper_up: bool,
    /// Whether the interface is point-to-point (TUN/VPN adapters usually are
    /// on Windows).
    is_p2p: bool,
}

/// Pure function: pick the best LAN IPv4 from a candidate list. Kept free of
/// real-interface access so it can be unit-tested.
///
/// Rules:
///  1. Drop interfaces that are down, point-to-point, or whose name hits a
///     virtual-adapter hint;
///  2. Classify the remaining addresses with [`classify_lan_ipv4`], dropping
///     the excluded ones;
///  3. Take the highest rank; ties keep enumeration order (first hit wins).
fn pick_lan_ipv4_from(candidates: &[IfaceCandidate]) -> Option<Ipv4Addr> {
    let mut best: Option<(u8, Ipv4Addr)> = None;
    for c in candidates {
        if !c.oper_up {
            continue;
        }
        if c.is_p2p {
            continue;
        }
        if looks_like_virtual_iface(&c.name) {
            continue;
        }
        let Some(score) = classify_lan_ipv4(c.ip) else {
            continue;
        };
        let rank = score.rank();
        match best {
            Some((best_rank, _)) if best_rank >= rank => {}
            _ => best = Some((rank, c.ip)),
        }
    }
    best.map(|(_, ip)| ip)
}

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
pub(crate) fn preferred_lan_ipv4() -> Option<Ipv4Addr> {
    let ifaces = if_addrs::get_if_addrs().ok()?;

    let mut candidates: Vec<IfaceCandidate> = Vec::new();
    for iface in &ifaces {
        // IPv4 only.
        let if_addrs::IfAddr::V4(v4) = &iface.addr else {
            continue;
        };
        candidates.push(IfaceCandidate {
            name: iface.name.clone(),
            ip: v4.ip,
            oper_up: iface.is_oper_up(),
            is_p2p: iface.is_p2p(),
        });
    }

    pick_lan_ipv4_from(&candidates)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(name: &str, ip: [u8; 4]) -> IfaceCandidate {
        IfaceCandidate {
            name: name.to_string(),
            ip: Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]),
            oper_up: true,
            is_p2p: false,
        }
    }

    #[test]
    fn prefers_private_192_over_clash_tun() {
        // 198.18.x.x is the Clash TUN default range and must be eliminated.
        let ifaces = vec![
            cand("Clash", [198, 18, 0, 1]),
            cand("WLAN", [192, 168, 1, 5]),
        ];
        assert_eq!(
            pick_lan_ipv4_from(&ifaces),
            Some(Ipv4Addr::new(192, 168, 1, 5))
        );
    }

    #[test]
    fn returns_none_when_only_virtual_or_invalid() {
        let ifaces = vec![
            cand("Loopback", [127, 0, 0, 1]),
            cand("wintun-clash", [198, 18, 0, 1]),
            cand("eth0", [169, 254, 3, 4]), // link-local
        ];
        assert_eq!(pick_lan_ipv4_from(&ifaces), None);
    }

    #[test]
    fn prefers_192_over_10() {
        let ifaces = vec![cand("eth1", [10, 0, 0, 7]), cand("WLAN", [192, 168, 0, 9])];
        assert_eq!(
            pick_lan_ipv4_from(&ifaces),
            Some(Ipv4Addr::new(192, 168, 0, 9))
        );
    }

    #[test]
    fn excludes_by_iface_name_even_if_subnet_looks_private() {
        // Some VPNs park a virtual adapter inside 192.168.x.x; the name hint
        // must still knock it out.
        let ifaces = vec![
            cand("VMware Network Adapter", [192, 168, 56, 1]),
            cand("Ethernet", [192, 168, 1, 20]),
        ];
        assert_eq!(
            pick_lan_ipv4_from(&ifaces),
            Some(Ipv4Addr::new(192, 168, 1, 20))
        );
    }

    #[test]
    fn excludes_point_to_point_and_down() {
        let mut p2p = cand("utun5", [192, 168, 9, 9]);
        p2p.is_p2p = true;
        let mut down = cand("Ethernet", [192, 168, 9, 10]);
        down.oper_up = false;
        let good = cand("WLAN", [192, 168, 9, 11]);
        let ifaces = vec![p2p, down, good];
        assert_eq!(
            pick_lan_ipv4_from(&ifaces),
            Some(Ipv4Addr::new(192, 168, 9, 11))
        );
    }

    #[test]
    fn excludes_cgnat_100_64() {
        let ifaces = vec![
            cand("tailscale0", [100, 100, 1, 1]),
            cand("Ethernet", [10, 1, 2, 3]),
        ];
        assert_eq!(
            pick_lan_ipv4_from(&ifaces),
            Some(Ipv4Addr::new(10, 1, 2, 3))
        );
    }

    #[test]
    fn cidr_boundaries() {
        // 198.18.0.0/15 covers 198.18.x and 198.19.x.
        assert!(classify_lan_ipv4(Ipv4Addr::new(198, 19, 255, 1)).is_none());
        // 198.20.x is outside that range and falls to the routable fallback.
        assert_eq!(
            classify_lan_ipv4(Ipv4Addr::new(198, 20, 0, 1)),
            Some(LanScore::OtherRoutable)
        );
        // 172.16/12 boundary: 172.16..=172.31 is private, 172.32 is not.
        assert_eq!(
            classify_lan_ipv4(Ipv4Addr::new(172, 31, 0, 1)),
            Some(LanScore::PrivateClassB)
        );
        assert_eq!(
            classify_lan_ipv4(Ipv4Addr::new(172, 32, 0, 1)),
            Some(LanScore::OtherRoutable)
        );
    }
}

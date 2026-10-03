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

use crate::NetError;

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
    // Linux container/VM networks commonly use these interface-name prefixes.
    // Keep this platform-specific and prefix-only: a host LAN may legitimately
    // live on br0 or bond0, and eth0 may be the usable LAN inside a container.
    #[cfg(target_os = "linux")]
    if ["veth", "virbr", "br-", "cni", "flannel"]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
    {
        return true;
    }
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

pub(crate) fn preferred_lan_ipv4() -> Result<Option<Ipv4Addr>, NetError> {
    select_interface_address(if_addrs::get_if_addrs())
}

fn select_interface_address(
    interfaces: std::io::Result<Vec<if_addrs::Interface>>,
) -> Result<Option<Ipv4Addr>, NetError> {
    let ifaces = interfaces.map_err(NetError::from)?;

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

    Ok(pick_lan_ipv4_from(&candidates))
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

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_container_and_vm_interfaces_do_not_outrank_the_physical_lan() {
        let physical_ip = Ipv4Addr::new(10, 1, 2, 3);
        for name in [
            "veth123abc",
            "virbr0",
            "br-a1b2c3d4e5f6",
            "docker0",
            "cni0",
            "flannel.1",
            "VETH123ABC",
        ] {
            let ifaces = vec![
                cand(name, [192, 168, 122, 1]),
                cand("enp3s0", physical_ip.octets()),
            ];
            assert_eq!(pick_lan_ipv4_from(&ifaces), Some(physical_ip), "{name}");
            assert_eq!(pick_lan_ipv4_from(&ifaces[..1]), None, "{name}");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_keeps_host_bridges_bonds_and_container_lan_interfaces() {
        let lan_ip = Ipv4Addr::new(192, 168, 1, 20);
        for name in ["br0", "bond0", "eth0", "enp3s0", "wlp2s0", "lan-veth"] {
            let ifaces = vec![cand(name, lan_ip.octets())];
            assert_eq!(pick_lan_ipv4_from(&ifaces), Some(lan_ip), "{name}");
        }
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn linux_interface_prefixes_do_not_change_other_platforms() {
        for name in [
            "veth123abc",
            "virbr0",
            "br-a1b2c3d4e5f6",
            "cni0",
            "flannel.1",
        ] {
            assert!(!looks_like_virtual_iface(name), "{name}");
        }
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

#[cfg(test)]
mod fallible_query_tests {
    use super::*;

    #[test]
    fn enumeration_failure_is_not_an_empty_address() -> Result<(), Box<dyn std::error::Error>> {
        let failure = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "interface lookup");
        if !matches!(
            select_interface_address(Err(failure)),
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), crate::error::ErrorKind::Io))
        {
            return Err("interface enumeration failure was hidden".into());
        }
        Ok(())
    }

    #[test]
    fn successful_empty_enumeration_has_no_address() -> Result<(), Box<dyn std::error::Error>> {
        if select_interface_address(Ok(Vec::new()))?.is_some() {
            return Err("empty interface enumeration invented an address".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod source_retention_tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn enumeration_error_retains_the_original_io_source() -> Result<(), Box<dyn Error>> {
        let failure = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "interface lookup");
        let error = select_interface_address(Err(failure))
            .err()
            .ok_or("enumeration unexpectedly succeeded")?;
        if error.io_kind() != Some(std::io::ErrorKind::PermissionDenied)
            || error
                .source()
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .is_none()
        {
            return Err("interface enumeration lost its original I/O source".into());
        }
        Ok(())
    }
}

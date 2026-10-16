//! Fresh inventory and a pure, evidence-based recommendation policy.

use std::io;
use std::time::SystemTime;

use crate::network::{
    LanInterfaceKind, LanIpv4Candidate, LanIpv4Completeness, LanIpv4Exclusion,
    LanIpv4RecommendationReason, LanIpv4Snapshot,
};
use crate::NetError;

use super::lan_addr::{enumerate_interfaces, in_cidr, looks_like_virtual_iface};
use super::lan_platform::{self, PlatformSnapshot};

pub(crate) fn query() -> Result<LanIpv4Snapshot, NetError> {
    let started = SystemTime::now();
    capture(enumerate_interfaces(), lan_platform::collect, started)
}

fn capture(
    interfaces: io::Result<Vec<if_addrs::Interface>>,
    collect: impl FnOnce(&[if_addrs::Interface]) -> PlatformSnapshot,
    started: SystemTime,
) -> Result<LanIpv4Snapshot, NetError> {
    let mut interfaces = interfaces.map_err(NetError::from)?;
    interfaces.retain(|iface| iface.ip().is_ipv4());
    // An empty successful IPv4 inventory needs no auxiliary metadata lookup.
    let platform = if interfaces.is_empty() {
        PlatformSnapshot::default()
    } else {
        collect(&interfaces)
    };
    let mut candidates = Vec::with_capacity(interfaces.len());
    for iface in interfaces {
        let if_addrs::IfAddr::V4(address) = &iface.addr else {
            continue;
        };
        let id = lan_platform::interface_key(&iface).to_owned();
        let metadata = platform.interfaces.get(&id).cloned().unwrap_or_default();
        let oper_up = match iface.oper_status {
            if_addrs::IfOperStatus::Up => Some(true),
            if_addrs::IfOperStatus::Unknown => None,
            _ => Some(false),
        };
        candidates.push(LanIpv4Candidate {
            interface_id: id,
            interface_name: iface.name.clone(),
            interface_index: iface.index,
            address: address.ip,
            netmask: address_netmask(address)?,
            prefix_len: address.prefixlen,
            interface_kind: metadata.kind,
            oper_up,
            point_to_point: Some(iface.is_p2p()),
            link_active: metadata.link_active,
            default_route: metadata.is_default_route,
            exclusions: Vec::new(),
            recommendation: None,
        });
    }
    Ok(select(candidates, platform.diagnostics, started))
}

fn address_netmask(address: &if_addrs::Ifv4Addr) -> Result<std::net::Ipv4Addr, NetError> {
    #[cfg(windows)]
    {
        // if-addrs obtains the Windows mask from the first matching adapter
        // prefix (which may be a host /32), independently of OnLinkPrefixLength.
        // The latter belongs to this unicast address and is the authoritative
        // prefix for this snapshot. Do not publish contradictory mask/prefix data.
        on_link_netmask(address.prefixlen)
    }
    #[cfg(not(windows))]
    {
        Ok(address.netmask)
    }
}

#[cfg(any(windows, test))]
fn on_link_netmask(prefix: u8) -> Result<std::net::Ipv4Addr, NetError> {
    let shift = 32u32.checked_sub(u32::from(prefix)).ok_or_else(|| {
        NetError::from(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid IPv4 on-link prefix length",
        ))
    })?;
    Ok(std::net::Ipv4Addr::from(
        u32::MAX.checked_shl(shift).unwrap_or(0),
    ))
}

fn exclusions(candidate: &LanIpv4Candidate) -> Vec<LanIpv4Exclusion> {
    use LanIpv4Exclusion::*;
    let mut reasons = Vec::new();
    match candidate.oper_up {
        Some(false) => reasons.push(InterfaceDown),
        None => reasons.push(InterfaceStateUnknown),
        Some(true) => {}
    }
    match candidate.point_to_point {
        Some(true) => reasons.push(PointToPoint),
        None => reasons.push(PointToPointStateUnknown),
        Some(false) => {}
    }
    if looks_like_virtual_iface(&candidate.interface_name) {
        reasons.push(VirtualInterfaceNameHint);
    }
    if matches!(
        candidate.interface_kind,
        LanInterfaceKind::Tunnel | LanInterfaceKind::Virtual
    ) {
        reasons.push(PlatformVirtualOrTunnel);
    }
    if candidate.link_active == Some(false) {
        reasons.push(LinkInactive);
    }
    let ip = candidate.address;
    if ip.is_loopback() || candidate.interface_kind == LanInterfaceKind::Loopback {
        reasons.push(Loopback);
    }
    if ip.is_link_local() {
        reasons.push(LinkLocal);
    }
    if ip.is_unspecified() {
        reasons.push(Unspecified);
    } else if ip.octets()[0] == 0 {
        reasons.push(ThisNetwork);
    }
    if ip.is_broadcast() {
        reasons.push(Broadcast);
    } else if ip.octets()[0] >= 240 {
        reasons.push(Reserved);
    }
    if ip.is_multicast() {
        reasons.push(Multicast);
    }
    if in_cidr(ip, [100, 64, 0, 0], 10) {
        reasons.push(SharedAddressSpace);
    }
    if in_cidr(ip, [198, 18, 0, 0], 15) {
        reasons.push(BenchmarkRange);
    }
    reasons
}

fn evidence(candidate: &LanIpv4Candidate) -> (u8, LanIpv4RecommendationReason) {
    use LanInterfaceKind::{Bridge, Ethernet, Wifi};
    use LanIpv4RecommendationReason::*;
    let kind = candidate.interface_kind;
    let active = candidate.link_active == Some(true);
    if matches!(kind, Wifi | Ethernet | Bridge) && active && candidate.default_route == Some(true) {
        (5, ActiveLanDefaultRoute)
    } else if matches!(kind, Wifi | Ethernet) && active {
        (4, ActiveWifiOrEthernet)
    } else if kind == Bridge && active {
        (3, ActiveBridge)
    } else if matches!(kind, Wifi | Ethernet) {
        (2, LanLinkUnknown)
    } else if kind == Bridge {
        (0, BridgeLinkUnknown)
    } else {
        (1, MetadataUnknown)
    }
}

fn select(
    mut candidates: Vec<LanIpv4Candidate>,
    mut diagnostics: Vec<String>,
    started: SystemTime,
) -> LanIpv4Snapshot {
    // Sorting is independent of recommendation. In particular, no private
    // address prefix gets promoted over another private prefix.
    candidates.sort_by(|a, b| {
        a.interface_id
            .cmp(&b.interface_id)
            .then_with(|| a.address.octets().cmp(&b.address.octets()))
            .then_with(|| a.interface_index.cmp(&b.interface_index))
            .then_with(|| a.interface_name.cmp(&b.interface_name))
            .then_with(|| a.netmask.octets().cmp(&b.netmask.octets()))
    });
    let mut best: Option<(usize, (u8, bool))> = None;
    for (index, candidate) in candidates.iter_mut().enumerate() {
        candidate.recommendation = None;
        candidate.exclusions = exclusions(candidate);
        if !candidate.exclusions.is_empty() {
            continue;
        }
        let rank = (evidence(candidate).0, candidate.address.is_private());
        if best.as_ref().is_none_or(|(_, best_rank)| rank > *best_rank) {
            best = Some((index, rank));
        }
    }
    if let Some((index, _)) = best {
        if let Some(candidate) = candidates.get_mut(index) {
            candidate.recommendation = Some(evidence(candidate).1);
        }
    }
    diagnostics.sort();
    diagnostics.dedup();
    LanIpv4Snapshot {
        candidates,
        completeness: if diagnostics.is_empty() {
            LanIpv4Completeness::Complete
        } else {
            LanIpv4Completeness::Partial
        },
        diagnostics,
        query_started_at: started,
        observed_at: SystemTime::now(),
    }
}

#[cfg(test)]
#[path = "lan_snapshot_tests.rs"]
mod tests;

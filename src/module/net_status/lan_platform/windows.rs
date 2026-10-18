use super::{interface_key, InterfaceMetadata, LanInterfaceKind, PlatformSnapshot};
use std::collections::HashMap;
use windows::Win32::{
    Foundation::NO_ERROR,
    NetworkManagement::{
        IpHelper::{
            FreeMibTable, GetIfEntry2, GetIpForwardTable2, GetIpInterfaceEntry,
            IF_TYPE_ETHERNET_CSMACD, IF_TYPE_IEEE80211, IF_TYPE_PPP, IF_TYPE_SOFTWARE_LOOPBACK,
            IF_TYPE_TUNNEL, MIB_IF_ROW2, MIB_IPFORWARD_ROW2, MIB_IPFORWARD_TABLE2,
            MIB_IPINTERFACE_ROW,
        },
        Ndis::{
            IfOperStatusDormant, IfOperStatusDown, IfOperStatusLowerLayerDown,
            IfOperStatusNotPresent, IfOperStatusTesting, IfOperStatusUp,
            MediaConnectStateConnected, MediaConnectStateDisconnected,
        },
    },
    Networking::WinSock::AF_INET,
};

fn default_routes() -> Result<HashMap<u32, u64>, String> {
    let mut table = std::ptr::null_mut();
    // SAFETY: a valid out pointer receives the native API's own aligned allocation.
    let result = unsafe { GetIpForwardTable2(AF_INET, &mut table) };
    if result != NO_ERROR {
        return Err(format!("GetIpForwardTable2 failed: {}", result.0));
    }
    if table.is_null() {
        return Err("GetIpForwardTable2 returned a null table".to_owned());
    }
    struct TableOwner(*mut MIB_IPFORWARD_TABLE2);
    impl Drop for TableOwner {
        fn drop(&mut self) {
            // SAFETY: this non-null pointer was allocated by GetIpForwardTable2 and
            // this owner frees it exactly once, including early-return paths.
            unsafe { FreeMibTable(self.0.cast()) };
        }
    }
    let owner = TableOwner(table);
    // SAFETY: a successful call returned a live native table with this header.
    let count = unsafe { (*owner.0).NumEntries } as usize;
    if count > 65_536 {
        return Err("IPv4 route table exceeded collection budget".to_owned());
    }
    // SAFETY: NumEntries describes the native allocation's trailing array. The
    // Rust [row; 1] member is only a placeholder; the table owner remains alive.
    let rows: &[MIB_IPFORWARD_ROW2] =
        unsafe { std::slice::from_raw_parts(std::ptr::addr_of!((*owner.0).Table).cast(), count) };
    let mut routes = HashMap::new();
    let mut interface_metrics = HashMap::new();
    for row in rows {
        if row.DestinationPrefix.PrefixLength != 0 || row.Loopback || row.ValidLifetime == 0 {
            continue;
        }
        // Repeated routes on one interface need only one native metric lookup.
        let interface_metric = match interface_metrics.get(&row.InterfaceIndex) {
            Some(metric) => *metric,
            None => {
                let mut interface = MIB_IPINTERFACE_ROW {
                    Family: AF_INET,
                    InterfaceIndex: row.InterfaceIndex,
                    ..MIB_IPINTERFACE_ROW::default()
                };
                // SAFETY: fully initialized writable structure with the required
                // family and native interface index inputs.
                let result = unsafe { GetIpInterfaceEntry(&mut interface) };
                if result != NO_ERROR {
                    // Incomplete metrics cannot establish which route is preferred.
                    return Err(format!(
                        "GetIpInterfaceEntry({}) failed: {}",
                        row.InterfaceIndex, result.0
                    ));
                }
                let metric = interface.Connected.then_some(interface.Metric);
                interface_metrics.insert(row.InterfaceIndex, metric);
                metric
            }
        };
        let Some(interface_metric) = interface_metric else {
            continue;
        };
        let metric = u64::from(row.Metric) + u64::from(interface_metric);
        routes
            .entry(row.InterfaceIndex)
            .and_modify(|current: &mut u64| *current = (*current).min(metric))
            .or_insert(metric);
    }
    Ok(routes)
}

fn row_metadata(row: &MIB_IF_ROW2) -> InterfaceMetadata {
    let flags = row.InterfaceAndOperStatusFlags._bitfield;
    let hardware = flags & 0x01 != 0;
    let filter = flags & 0x02 != 0;
    let kind = if row.Type == IF_TYPE_SOFTWARE_LOOPBACK {
        LanInterfaceKind::Loopback
    } else if matches!(row.Type, IF_TYPE_PPP | IF_TYPE_TUNNEL) || row.TunnelType.0 != 0 {
        LanInterfaceKind::Tunnel
    } else if filter {
        LanInterfaceKind::Virtual
    } else if row.Type == 209 || (row.Type == IF_TYPE_IEEE80211 && !hardware) {
        // IANA IF_TYPE_BRIDGE, or a hosted Wi-Fi interface. An active bridge can
        // still be a usable LAN, but it is not evidence of physical Wi-Fi hardware.
        LanInterfaceKind::Bridge
    } else if row.Type == IF_TYPE_IEEE80211 {
        LanInterfaceKind::Wifi
    } else if row.Type == IF_TYPE_ETHERNET_CSMACD && hardware {
        LanInterfaceKind::Ethernet
    } else if row.Type == IF_TYPE_ETHERNET_CSMACD {
        LanInterfaceKind::Virtual
    } else {
        LanInterfaceKind::Unknown
    };
    let link_active = if matches!(
        row.OperStatus,
        IfOperStatusDown
            | IfOperStatusDormant
            | IfOperStatusLowerLayerDown
            | IfOperStatusNotPresent
            | IfOperStatusTesting
    ) || row.MediaConnectState == MediaConnectStateDisconnected
        || flags & 0x18 != 0
    {
        Some(false)
    } else if row.OperStatus == IfOperStatusUp
        && row.MediaConnectState == MediaConnectStateConnected
    {
        Some(true)
    } else {
        None
    };
    InterfaceMetadata {
        kind,
        link_active,
        is_default_route: None,
    }
}

fn default_role(routes: Option<&HashMap<u32, u64>>, index: u32) -> Option<bool> {
    let routes = routes?;
    let Some(metric) = routes.get(&index) else {
        return Some(false);
    };
    Some(routes.values().min() == Some(metric))
}

pub(crate) fn collect(ifaces: &[if_addrs::Interface]) -> PlatformSnapshot {
    let mut snapshot = PlatformSnapshot::default();
    let routes = match default_routes() {
        Ok(routes) => Some(routes),
        Err(error) => {
            snapshot.failure_classes |= 4;
            snapshot.diagnostics.push(error);
            None
        }
    };
    for iface in ifaces {
        let key = interface_key(iface).to_owned();
        if snapshot.interfaces.contains_key(&key) {
            continue;
        }
        let mut metadata = InterfaceMetadata::default();
        if let Some(index) = iface.index {
            let mut row = MIB_IF_ROW2 {
                InterfaceIndex: index,
                ..MIB_IF_ROW2::default()
            };
            // SAFETY: fully initialized writable native structure with the required index.
            let result = unsafe { GetIfEntry2(&mut row) };
            if result == NO_ERROR {
                metadata = row_metadata(&row);
            } else {
                snapshot.failure_classes |= 8;
                snapshot
                    .diagnostics
                    .push(format!("GetIfEntry2({index}) failed: {}", result.0));
            }
            // Route evidence is independent from a failed interface metadata query.
            // A VPN can be the actual default; selection policy must handle its kind.
            metadata.is_default_route = default_role(routes.as_ref(), index);
        } else {
            snapshot
                .diagnostics
                .push(format!("interface {} has no native index", iface.name));
        }
        snapshot.interfaces.insert(key, metadata);
    }
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;
    use windows::Win32::NetworkManagement::Ndis::{IfOperStatusUnknown, MediaConnectStateUnknown};

    #[test]
    fn physical_wifi_and_hosted_wifi_have_distinct_kinds() -> Result<(), String> {
        let mut row = MIB_IF_ROW2 {
            Type: IF_TYPE_IEEE80211,
            OperStatus: IfOperStatusUp,
            MediaConnectState: MediaConnectStateConnected,
            ..MIB_IF_ROW2::default()
        };
        row.InterfaceAndOperStatusFlags._bitfield = 0x01;
        let physical = row_metadata(&row);
        row.InterfaceAndOperStatusFlags._bitfield = 0;
        let hosted = row_metadata(&row);
        if physical.kind != LanInterfaceKind::Wifi
            || hosted.kind != LanInterfaceKind::Bridge
            || physical.link_active != Some(true)
            || hosted.link_active != Some(true)
        {
            return Err("Wi-Fi hardware evidence was ignored".to_owned());
        }
        Ok(())
    }

    #[test]
    fn link_unknown_is_not_disconnected() -> Result<(), String> {
        let mut row = MIB_IF_ROW2 {
            OperStatus: IfOperStatusUnknown,
            MediaConnectState: MediaConnectStateUnknown,
            ..MIB_IF_ROW2::default()
        };
        if row_metadata(&row).link_active.is_some() {
            return Err("unknown link became a known state".to_owned());
        }
        row.MediaConnectState = MediaConnectStateDisconnected;
        if row_metadata(&row).link_active != Some(false) {
            return Err("explicit media disconnect was lost".to_owned());
        }
        row.MediaConnectState = MediaConnectStateConnected;
        row.OperStatus = IfOperStatusUp;
        row.InterfaceAndOperStatusFlags._bitfield = 0x08;
        if row_metadata(&row).link_active != Some(false) {
            return Err("not-authenticated interface was marked active".to_owned());
        }
        Ok(())
    }

    #[test]
    fn missing_empty_and_ranked_routes_have_distinct_roles() -> Result<(), String> {
        let empty = HashMap::new();
        let ranked = HashMap::from([(1, 5), (2, 10), (3, 5)]);
        if default_role(None, 1).is_some()
            || default_role(Some(&empty), 1) != Some(false)
            || default_role(Some(&ranked), 1) != Some(true)
            || default_role(Some(&ranked), 2) != Some(false)
            || default_role(Some(&ranked), 3) != Some(true)
            || default_role(Some(&ranked), 4) != Some(false)
        {
            return Err("unknown, absent, or preferred routes were conflated".to_owned());
        }
        Ok(())
    }
}

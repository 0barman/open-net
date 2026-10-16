use super::super::lan_platform::InterfaceMetadata;
use super::*;
use std::net::Ipv4Addr;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn candidate(name: &str, address: [u8; 4], kind: LanInterfaceKind) -> LanIpv4Candidate {
    LanIpv4Candidate {
        interface_id: name.into(),
        interface_name: name.into(),
        interface_index: Some(1),
        address: Ipv4Addr::from(address),
        netmask: Ipv4Addr::new(255, 255, 255, 0),
        prefix_len: 24,
        interface_kind: kind,
        oper_up: Some(true),
        point_to_point: Some(false),
        link_active: Some(true),
        default_route: Some(false),
        exclusions: Vec::new(),
        recommendation: None,
    }
}

fn snapshot(candidates: Vec<LanIpv4Candidate>) -> LanIpv4Snapshot {
    select(candidates, Vec::new(), SystemTime::now())
}

#[test]
fn active_wifi_beats_inactive_bridge_despite_iff_running_and_192_prefix() -> TestResult {
    let mut wifi = candidate("en0", [172, 16, 1, 46], LanInterfaceKind::Wifi);
    wifi.default_route = Some(true);
    let mut bridge = candidate("bridge100", [192, 168, 2, 1], LanInterfaceKind::Bridge);
    bridge.link_active = Some(false);
    for entries in [
        vec![wifi.clone(), bridge.clone()],
        vec![bridge.clone(), wifi.clone()],
    ] {
        let result = snapshot(entries);
        check(
            result.recommended().map(|c| c.address()) == Some(wifi.address),
            "inactive bridge displaced Wi-Fi",
        )?;
        let kept = result
            .candidates()
            .iter()
            .find(|c| c.interface_name() == "bridge100")
            .ok_or("excluded bridge vanished")?;
        check(
            kept.is_oper_up() == Some(true)
                && kept
                    .default_exclusions()
                    .contains(&LanIpv4Exclusion::LinkInactive),
            "link evidence was confused with operational flags",
        )?;
    }
    Ok(())
}

#[test]
fn private_prefixes_do_not_overrule_interface_evidence() -> TestResult {
    for address in [[172, 16, 1, 46], [10, 20, 1, 46], [192, 168, 3, 46]] {
        let wifi = candidate("wifi-lan", address, LanInterfaceKind::Wifi);
        let bridge = candidate("bridge100", [192, 168, 2, 1], LanInterfaceKind::Bridge);
        let result = snapshot(vec![bridge, wifi]);
        check(
            result.recommended().map(|c| c.address()) == Some(Ipv4Addr::from(address)),
            "private prefix changed the interface evidence winner",
        )?;
    }
    Ok(())
}

#[test]
fn equal_evidence_uses_identity_instead_of_private_prefix_or_enumeration_order() -> TestResult {
    let a = candidate("a-lan", [172, 16, 1, 4], LanInterfaceKind::Ethernet);
    let b = candidate("b-lan", [192, 168, 1, 4], LanInterfaceKind::Ethernet);
    let first = snapshot(vec![a.clone(), b.clone()]);
    let reversed = snapshot(vec![b, a]);
    check(
        first.candidates() == reversed.candidates(),
        "enumeration order changed the snapshot",
    )?;
    check(
        first.recommended().map(|c| c.interface_id()) == Some("a-lan"),
        "192 prefix still won an equal-evidence tie",
    )
}

#[test]
fn vpn_default_route_does_not_hide_physical_lan_without_default_route() -> TestResult {
    let mut tunnel = candidate("utun7", [198, 18, 0, 1], LanInterfaceKind::Tunnel);
    tunnel.default_route = Some(true);
    tunnel.point_to_point = Some(true);
    let lan = candidate("en0", [172, 16, 1, 46], LanInterfaceKind::Wifi);
    let result = snapshot(vec![tunnel, lan]);
    check(
        result.recommended().map(|c| c.interface_id()) == Some("en0"),
        "VPN default displaced physical LAN",
    )?;
    let tunnel = result
        .candidates()
        .iter()
        .find(|c| c.interface_id() == "utun7")
        .ok_or("VPN candidate disappeared")?;
    for reason in [
        LanIpv4Exclusion::PointToPoint,
        LanIpv4Exclusion::VirtualInterfaceNameHint,
        LanIpv4Exclusion::PlatformVirtualOrTunnel,
        LanIpv4Exclusion::BenchmarkRange,
    ] {
        check(
            tunnel.default_exclusions().contains(&reason),
            "VPN exclusion reason missing",
        )?;
    }
    Ok(())
}

#[test]
fn active_bridge_can_be_the_real_default_lan() -> TestResult {
    let mut bridge = candidate("br0", [172, 20, 1, 1], LanInterfaceKind::Bridge);
    bridge.default_route = Some(true);
    let lan = candidate("en0", [192, 168, 1, 1], LanInterfaceKind::Ethernet);
    let result = snapshot(vec![lan, bridge]);
    check(
        result
            .recommended()
            .map(|c| (c.interface_id(), c.recommendation_reason()))
            == Some((
                "br0",
                Some(LanIpv4RecommendationReason::ActiveLanDefaultRoute),
            )),
        "legitimate default bridge was rejected",
    )
}

#[test]
fn same_ip_on_different_interfaces_and_multiple_addresses_are_preserved() -> TestResult {
    let a = candidate("lan-a", [10, 1, 2, 3], LanInterfaceKind::Ethernet);
    let b = candidate("lan-b", [10, 1, 2, 3], LanInterfaceKind::Ethernet);
    let c = candidate("lan-a", [10, 1, 2, 4], LanInterfaceKind::Ethernet);
    let result = snapshot(vec![c, b, a]);
    check(
        result.candidates().len() == 3,
        "IP-only or interface-only deduplication lost an address",
    )?;
    check(
        result
            .candidates()
            .iter()
            .filter(|c| c.is_recommended())
            .count()
            == 1,
        "same IP marked multiple recommendations",
    )?;
    check(
        result.recommended().map(|c| c.interface_id()) == Some("lan-a"),
        "interface identity was lost",
    )
}

#[test]
fn unknown_operational_state_is_not_misreported_as_down_or_recommended() -> TestResult {
    let mut lan = candidate("en0", [172, 16, 1, 46], LanInterfaceKind::Wifi);
    lan.oper_up = None;
    let result = snapshot(vec![lan]);
    let lan = result
        .candidates()
        .first()
        .ok_or("unknown candidate disappeared")?;
    check(
        lan.is_oper_up().is_none() && result.recommended().is_none(),
        "unknown operational state was invented",
    )?;
    check(
        lan.default_exclusions() == [LanIpv4Exclusion::InterfaceStateUnknown],
        "unknown state was labelled down",
    )
}

#[test]
fn auxiliary_failure_retains_candidates_with_explicit_degraded_recommendation() -> TestResult {
    let mut lan = candidate("en0", [172, 16, 1, 46], LanInterfaceKind::Unknown);
    lan.link_active = None;
    lan.default_route = None;
    let result = select(
        vec![lan],
        vec!["native metadata query failed".into()],
        SystemTime::now(),
    );
    check(
        result.completeness() == LanIpv4Completeness::Partial && !result.diagnostics().is_empty(),
        "auxiliary failure looked complete",
    )?;
    check(
        result.recommended().and_then(|c| c.recommendation_reason())
            == Some(LanIpv4RecommendationReason::MetadataUnknown),
        "remaining facts were discarded or overstated",
    )?;
    check(
        result
            .recommended()
            .is_some_and(|c| c.is_default_route().is_none() && c.link_active().is_none()),
        "unknown metadata became false",
    )
}

#[test]
fn every_default_exclusion_remains_visible_including_invalid_address_ranges() -> TestResult {
    for (address, expected) in [
        ([127, 0, 0, 1], LanIpv4Exclusion::Loopback),
        ([169, 254, 1, 2], LanIpv4Exclusion::LinkLocal),
        ([0, 0, 0, 0], LanIpv4Exclusion::Unspecified),
        ([0, 1, 2, 3], LanIpv4Exclusion::ThisNetwork),
        ([255, 255, 255, 255], LanIpv4Exclusion::Broadcast),
        ([224, 0, 0, 1], LanIpv4Exclusion::Multicast),
        ([240, 1, 2, 3], LanIpv4Exclusion::Reserved),
        ([100, 64, 0, 1], LanIpv4Exclusion::SharedAddressSpace),
        ([100, 127, 255, 254], LanIpv4Exclusion::SharedAddressSpace),
        ([198, 18, 0, 1], LanIpv4Exclusion::BenchmarkRange),
        ([198, 19, 255, 254], LanIpv4Exclusion::BenchmarkRange),
    ] {
        let result = snapshot(vec![candidate("lan", address, LanInterfaceKind::Ethernet)]);
        let entry = result
            .candidates()
            .first()
            .ok_or("excluded address disappeared")?;
        check(
            result.recommended().is_none() && entry.default_exclusions().contains(&expected),
            "invalid/default-excluded address was recommended",
        )?;
    }
    Ok(())
}

#[test]
fn subnet_boundaries_and_public_lan_fallback_are_not_overfiltered() -> TestResult {
    for address in [
        [100, 63, 255, 254],
        [100, 128, 0, 1],
        [198, 20, 0, 1],
        [172, 32, 0, 1],
        [196, 1, 2, 3],
    ] {
        let result = snapshot(vec![candidate("lan", address, LanInterfaceKind::Ethernet)]);
        check(
            result.recommended().map(|c| c.address()) == Some(Ipv4Addr::from(address)),
            "CIDR boundary or public fallback was overfiltered",
        )?;
    }
    Ok(())
}

#[test]
fn name_hint_and_native_virtual_evidence_are_independently_explained() -> TestResult {
    let a = candidate("VPN-lan", [192, 168, 1, 5], LanInterfaceKind::Ethernet);
    let b = candidate("本地连接", [192, 168, 1, 6], LanInterfaceKind::Virtual);
    let result = snapshot(vec![a, b]);
    check(
        result.recommended().is_none() && result.candidates().len() == 2,
        "virtual candidates disappeared or were recommended",
    )?;
    check(
        result.candidates().iter().any(|c| {
            c.default_exclusions()
                .contains(&LanIpv4Exclusion::VirtualInterfaceNameHint)
        }),
        "name heuristic missing",
    )?;
    check(
        result.candidates().iter().any(|c| {
            c.default_exclusions()
                .contains(&LanIpv4Exclusion::PlatformVirtualOrTunnel)
        }),
        "native virtual evidence missing",
    )
}

fn raw_interface(oper_status: if_addrs::IfOperStatus) -> if_addrs::Interface {
    if_addrs::Interface {
        name: "lan".into(),
        addr: if_addrs::IfAddr::V4(if_addrs::Ifv4Addr {
            ip: Ipv4Addr::new(172, 16, 1, 46),
            netmask: Ipv4Addr::new(255, 255, 254, 0),
            prefixlen: 23,
            broadcast: Some(Ipv4Addr::new(172, 16, 1, 255)),
        }),
        index: Some(8),
        oper_status,
        is_p2p: false,
        #[cfg(windows)]
        adapter_name: "adapter-lan".into(),
    }
}

#[test]
fn collection_keeps_raw_unknown_state_mask_and_identity() -> TestResult {
    let iface = raw_interface(if_addrs::IfOperStatus::Unknown);
    let id = lan_platform::interface_key(&iface).to_owned();
    let result = capture(
        Ok(vec![iface]),
        |_| {
            let mut platform = PlatformSnapshot::default();
            platform.interfaces.insert(
                id.clone(),
                InterfaceMetadata {
                    kind: LanInterfaceKind::Wifi,
                    link_active: Some(true),
                    is_default_route: None,
                },
            );
            platform
        },
        SystemTime::now(),
    )?;
    let candidate = result
        .candidates()
        .first()
        .ok_or("raw address disappeared")?;
    check(
        candidate.interface_id() == id && candidate.interface_index() == Some(8),
        "raw interface identity changed",
    )?;
    check(
        candidate.prefix_len() == 23 && candidate.netmask() == Ipv4Addr::new(255, 255, 254, 0),
        "raw mask was lost",
    )?;
    check(
        candidate.is_oper_up().is_none() && candidate.is_default_route().is_none(),
        "unknown field was invented",
    )
}

#[test]
fn enumeration_failure_keeps_original_io_error_and_does_not_collect_metadata() -> TestResult {
    use std::error::Error;
    let called = std::cell::Cell::new(false);
    let result = capture(
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "fixture enumeration denied",
        )),
        |_| {
            called.set(true);
            PlatformSnapshot::default()
        },
        SystemTime::now(),
    );
    let error = result.err().ok_or("enumeration failure became success")?;
    check(!called.get(), "metadata queried after enumeration failure")?;
    check(
        error.io_kind() == Some(io::ErrorKind::PermissionDenied)
            && error
                .source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .is_some(),
        "original enumeration error was lost",
    )
}

#[test]
fn successful_empty_enumeration_has_no_recommendation_or_metadata_failure() -> TestResult {
    let called = std::cell::Cell::new(false);
    let result = capture(
        Ok(Vec::new()),
        |_| {
            called.set(true);
            PlatformSnapshot {
                diagnostics: vec!["should not run".into()],
                ..Default::default()
            }
        },
        SystemTime::now(),
    )?;
    check(
        !called.get()
            && result.candidates().is_empty()
            && result.recommended().is_none()
            && result.completeness() == LanIpv4Completeness::Complete,
        "empty inventory was confused with query failure",
    )
}

#[test]
fn windows_on_link_prefix_avoids_host_prefix_masks_and_rejects_invalid_lengths() -> TestResult {
    let mut iface = raw_interface(if_addrs::IfOperStatus::Up);
    let if_addrs::IfAddr::V4(address) = &mut iface.addr else {
        return Err("fixture is not IPv4".into());
    };
    // Windows adapter prefix inventory can list this host's /32 before its subnet.
    address.netmask = Ipv4Addr::BROADCAST;
    address.prefixlen = 24;
    check(
        on_link_netmask(address.prefixlen)? == Ipv4Addr::new(255, 255, 255, 0),
        "host prefix replaced on-link subnet",
    )?;
    #[cfg(windows)]
    check(
        address_netmask(address)? == Ipv4Addr::new(255, 255, 255, 0),
        "Windows capture used the adapter prefix mask",
    )?;
    check(
        on_link_netmask(0)? == Ipv4Addr::UNSPECIFIED && on_link_netmask(32)? == Ipv4Addr::BROADCAST,
        "on-link prefix boundary is incorrect",
    )?;
    for prefix in [33, 255] {
        let error = on_link_netmask(prefix)
            .err()
            .ok_or("invalid native prefix was fabricated into a mask")?;
        check(
            error.io_kind() == Some(io::ErrorKind::InvalidData),
            "invalid native prefix lost error kind",
        )?;
    }
    Ok(())
}

//! Linux reachability corrections around netwatch's interface snapshot.
//!
//! netwatch checks administrative `IFF_UP`, retains only one default interface,
//! and requires a gateway for IPv6 defaults. Check operational state and all
//! default routes without changing the shared IP-stack capability flags.

use std::io;
use std::net::{IpAddr, Ipv6Addr};

use crate::error::{ErrorKind, ErrorStage, NetError};
use crate::module::net_status::NetworkStatus;

const IPV4_ROUTE_PATH: &str = "/proc/net/route";
const IPV6_ROUTE_PATH: &str = "/proc/net/ipv6_route";
const RTF_UP: u32 = 0x0001;
const RTF_REJECT: u32 = 0x0200;

#[derive(Clone, Copy)]
enum RouteFamily {
    V4,
    V6,
}

impl RouteFamily {
    fn matches(self, ip: IpAddr) -> bool {
        matches!(
            (self, ip),
            (Self::V4, IpAddr::V4(_)) | (Self::V6, IpAddr::V6(_))
        )
    }
}

pub(crate) async fn current_reachability(
    state: &netwatch::netmon::State,
) -> Result<NetworkStatus, NetError> {
    // Only the procfs-missing fallback needs the cached default name. Cached
    // admin-up and capability flags may predate a newly working interface, so
    // they must not suppress this fresh reachability query. The caller still
    // derives the public IpStack from netwatch's original capability flags.
    let default_interface = state.default_route_interface.clone();
    // getifaddrs and procfs reads must not block the async monitor worker.
    tokio::task::spawn_blocking(move || query_reachability(default_interface.as_deref()))
        .await
        .map_err(|error| {
            let kind = if error.is_cancelled() {
                ErrorKind::RuntimeUnavailable
            } else {
                ErrorKind::Internal
            };
            NetError::with_source(kind, error).with_stage(ErrorStage::NetworkMonitor)
        })?
}

fn query_reachability(default_interface: Option<&str>) -> Result<NetworkStatus, NetError> {
    let interfaces = if_addrs::get_if_addrs().map_err(monitor_io_error)?;
    reachability_from(&interfaces, default_interface, |family| {
        std::fs::read_to_string(match family {
            RouteFamily::V4 => IPV4_ROUTE_PATH,
            RouteFamily::V6 => IPV6_ROUTE_PATH,
        })
    })
}

fn reachability_from(
    interfaces: &[if_addrs::Interface],
    default_interface: Option<&str>,
    read_routes: impl Fn(RouteFamily) -> io::Result<String>,
) -> Result<NetworkStatus, NetError> {
    for family in [RouteFamily::V4, RouteFamily::V6] {
        // Derive both families and operational state from one getifaddrs
        // result, after its fallible enumeration has succeeded.
        if !interfaces
            .iter()
            .any(|interface| family.matches(interface.ip()) && usable_interface(interface))
        {
            continue;
        }
        let available = match optional_routes(read_routes(family))? {
            Some(routes) => match family {
                RouteFamily::V4 => has_usable_ipv4_default(interfaces, &routes),
                RouteFamily::V6 => has_usable_ipv6_default(interfaces, &routes),
            },
            None => has_usable_default_interface(default_interface, interfaces, family),
        };
        if available {
            return Ok(NetworkStatus::Available);
        }
    }
    Ok(NetworkStatus::Unavailable)
}

fn monitor_io_error(error: io::Error) -> NetError {
    NetError::from(error).with_stage(ErrorStage::NetworkMonitor)
}

fn optional_routes(result: io::Result<String>) -> Result<Option<String>, NetError> {
    match result {
        Ok(routes) => Ok(Some(routes)),
        // IPv6 may be disabled or procfs absent. Only absent files permit the
        // less detailed netwatch default-interface fallback; all other errors
        // retain their original source for the monitor's Failed state.
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        // A denied or failed query is not evidence that the network is down.
        Err(error) => Err(monitor_io_error(error)),
    }
}

fn base_interface_name(name: &str) -> &str {
    // Linux IPv4 address labels can appear as eth0:1 in getifaddrs, while the
    // route and link still belong to eth0. Keep every address and its flags;
    // strip only the label suffix when joining these OS records.
    name.split_once(':').map_or(name, |(base, _)| base)
}

fn usable_interface(interface: &if_addrs::Interface) -> bool {
    // if-addrs 0.15 maps IFF_RUNNING to is_oper_up on Linux. The kernel defines
    // this as UP *or UNKNOWN*, so devices without operstate support remain
    // eligible, unlike a strict /sys/class/net/<name>/operstate == "up" check.
    // https://www.kernel.org/doc/html/v5.14/networking/operstates.html
    // Linux dev_get_flags() reports IFF_RUNNING only when netif_running() and
    // netif_oper_up() are both true, so no cached IFF_UP check is required.
    // https://github.com/torvalds/linux/blob/v5.0/net/core/dev.c
    let name = base_interface_name(&interface.name);
    name != "lo"
        && interface.is_oper_up()
        && match interface.ip() {
            IpAddr::V4(ip) => {
                // Keep netwatch's IPv4 link-local NAT support. This checks
                // local network capability, not end-to-end Internet access.
                !ip.is_loopback()
                    && !ip.is_unspecified()
                    && !ip.is_broadcast()
                    && !ip.is_multicast()
            }
            IpAddr::V6(ip) => usable_ipv6(ip),
        }
}

fn usable_ipv6(ip: Ipv6Addr) -> bool {
    // Match netwatch's usable IPv6 ranges: global unicast or unique-local.
    let first = ip.segments()[0];
    first & 0xe000 == 0x2000 || first & 0xfe00 == 0xfc00
}

fn has_usable_default_interface(
    default_interface: Option<&str>,
    interfaces: &[if_addrs::Interface],
    family: RouteFamily,
) -> bool {
    let Some(default) = default_interface else {
        return false;
    };
    interfaces.iter().any(|interface| {
        base_interface_name(&interface.name) == base_interface_name(default)
            && family.matches(interface.ip())
            && usable_interface(interface)
    })
}

fn parse_ipv4_default_route(line: &str) -> Option<&str> {
    // /proc/net/route uses hex for addresses and flags, decimal for counters.
    // Match the all-zero destination/mask without an endian-dependent decode.
    // Headers and malformed lines cannot establish reachability and are skipped.
    let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() != 11
        || base_interface_name(fields[0]) == "lo"
        || fields[1] != "00000000"
        || fields[7] != "00000000"
        || fields[2].len() != 8
        || !fields[2].bytes().all(|byte| byte.is_ascii_hexdigit())
        || ![4, 5, 6, 8, 9, 10].into_iter().all(|index| {
            !fields[index].is_empty() && fields[index].bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return None;
    }
    let flags = u32::from_str_radix(fields[3], 16).ok()?;
    (flags & RTF_UP != 0 && flags & RTF_REJECT == 0).then_some(base_interface_name(fields[0]))
}

fn has_usable_ipv4_default(interfaces: &[if_addrs::Interface], routes: &str) -> bool {
    routes
        .lines()
        .filter_map(parse_ipv4_default_route)
        .any(|name| {
            interfaces.iter().any(|interface| {
                base_interface_name(&interface.name) == name
                    && interface.ip().is_ipv4()
                    && usable_interface(interface)
            })
        })
}

struct Ipv6DefaultRoute<'a> {
    interface: &'a str,
    source: u128,
    source_prefix: u32,
}

impl Ipv6DefaultRoute<'_> {
    fn permits_source(&self, ip: Ipv6Addr) -> bool {
        if self.source_prefix == 0 {
            return true;
        }
        let mask = u128::MAX << (128 - self.source_prefix);
        u128::from(ip) & mask == self.source & mask
    }
}

fn parse_ipv6_default_route(line: &str) -> Option<Ipv6DefaultRoute<'_>> {
    // Linux proc IPv6 route ABI: destination/prefix, source/prefix, next hop,
    // metric, reference count, use count, flags, interface. All numbers are hex.
    let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() != 10
        || !fields[..9]
            .iter()
            .zip([32, 2, 32, 2, 32, 8, 8, 8, 8])
            .all(|(field, width)| {
                field.len() == width && field.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
    {
        return None;
    }
    let destination = u128::from_str_radix(fields[0], 16).ok()?;
    let prefix = u32::from_str_radix(fields[1], 16).ok()?;
    let source = u128::from_str_radix(fields[2], 16).ok()?;
    let source_prefix = u32::from_str_radix(fields[3], 16).ok()?;
    let flags = u32::from_str_radix(fields[8], 16).ok()?;
    if destination != 0
        || prefix != 0
        || source_prefix > 128
        || (source_prefix == 0 && source != 0)
        || flags & RTF_UP == 0
        || flags & RTF_REJECT != 0
        || base_interface_name(fields[9]) == "lo"
    {
        return None;
    }
    // A zero next hop is valid: `default dev eth0` is an on-link default.
    Some(Ipv6DefaultRoute {
        interface: base_interface_name(fields[9]),
        source,
        source_prefix,
    })
}

fn has_usable_ipv6_default(interfaces: &[if_addrs::Interface], routes: &str) -> bool {
    routes
        .lines()
        .filter_map(parse_ipv6_default_route)
        .any(|route| {
            interfaces.iter().any(|interface| {
                base_interface_name(&interface.name) == route.interface
                    && usable_interface(interface)
                    && matches!(interface.ip(), IpAddr::V6(ip) if route.permits_source(ip))
            })
        })
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::net::Ipv4Addr;

    use super::*;

    fn interface(name: &str, ip: IpAddr, oper_up: bool) -> if_addrs::Interface {
        let addr = match ip {
            IpAddr::V4(ip) => if_addrs::IfAddr::V4(if_addrs::Ifv4Addr {
                ip,
                netmask: Ipv4Addr::new(255, 255, 255, 0),
                prefixlen: 24,
                broadcast: None,
            }),
            IpAddr::V6(ip) => if_addrs::IfAddr::V6(if_addrs::Ifv6Addr {
                ip,
                netmask: Ipv6Addr::from(u128::MAX << 64),
                prefixlen: 64,
                broadcast: None,
            }),
        };
        if_addrs::Interface {
            name: name.to_owned(),
            addr,
            index: None,
            oper_status: if oper_up {
                if_addrs::IfOperStatus::Up
            } else {
                if_addrs::IfOperStatus::Down
            },
            is_p2p: false,
        }
    }

    fn v4(name: &str, oper_up: bool) -> if_addrs::Interface {
        interface(name, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2)), oper_up)
    }

    fn v6(name: &str, oper_up: bool) -> if_addrs::Interface {
        interface(
            name,
            IpAddr::V6(Ipv6Addr::new(0xfd00, 1, 0, 0, 0, 0, 0, 2)),
            oper_up,
        )
    }

    fn route(name: &str, source: Ipv6Addr, prefix: u8, gateway: Ipv6Addr, flags: u32) -> String {
        format!(
            "{:032x} 00 {:032x} {prefix:02x} {:032x} 00000400 00000000 00000000 {flags:08x} {name}",
            0_u128,
            u128::from(source),
            u128::from(gateway),
        )
    }

    fn on_link(name: &str) -> String {
        route(
            name,
            Ipv6Addr::UNSPECIFIED,
            0,
            Ipv6Addr::UNSPECIFIED,
            RTF_UP,
        )
    }

    fn ipv4_route(name: &str, flags: u32) -> String {
        format!("{name} 00000000 0101A8C0 {flags:04X} 0 0 100 00000000 0 0 0")
    }

    #[test]
    fn a_new_working_interface_is_available_before_netwatch_refreshes() {
        let old_snapshot = netwatch::netmon::State {
            interfaces: Default::default(),
            local_addresses: Default::default(),
            have_v4: false,
            have_v6: false,
            is_expensive: false,
            default_route_interface: Some("eth0".to_owned()),
            last_unsuspend: None,
        };
        // The old default lost carrier while a previously unknown interface
        // gained an address/default. Neither old capability flags nor its
        // missing interface entry may manufacture an outage/loss_epoch bump.
        for new_interface in [v4("eth1", true), v6("eth1", true)] {
            let observed = reachability_from(
                &[v4("eth0", false), new_interface],
                old_snapshot.default_route_interface.as_deref(),
                |family| {
                    Ok(match family {
                        RouteFamily::V4 => ipv4_route("eth1", RTF_UP),
                        RouteFamily::V6 => on_link("eth1"),
                    })
                },
            )
            .unwrap();
            assert_eq!(observed, NetworkStatus::Available);
        }
    }

    #[test]
    fn ipv4_address_label_matches_its_underlying_default_interface() {
        let interfaces = [v4("eth0:1", true)];
        assert!(has_usable_default_interface(
            Some("eth0"),
            &interfaces,
            RouteFamily::V4
        ));
        assert!(has_usable_ipv4_default(
            &interfaces,
            &ipv4_route("eth0", RTF_UP)
        ));
    }

    #[test]
    fn ipv4_address_label_does_not_match_another_base_interface() {
        let interfaces = [v4("eth1:1", true)];
        assert!(!has_usable_default_interface(
            Some("eth0"),
            &interfaces,
            RouteFamily::V4
        ));
        assert!(!has_usable_ipv4_default(
            &interfaces,
            &ipv4_route("eth0", RTF_UP)
        ));
    }

    #[test]
    fn another_operational_ipv4_default_survives_the_first_links_outage() {
        let interfaces = [v4("eth0", false), v4("eth1", true)];
        let routes = format!(
            "{}\n{}",
            ipv4_route("eth0", RTF_UP),
            ipv4_route("eth1", RTF_UP)
        );
        assert!(has_usable_ipv4_default(&interfaces, &routes));
        assert!(!has_usable_ipv4_default(&[v4("eth0", false)], &routes));
    }

    #[test]
    fn ipv4_default_cannot_borrow_an_ipv6_or_other_interfaces_address() {
        let routes = ipv4_route("eth0", RTF_UP);
        let interfaces = [v6("eth0", true), v4("eth1", true)];
        assert!(!has_usable_ipv4_default(&interfaces, &routes));
        assert!(!has_usable_default_interface(
            Some("eth0"),
            &interfaces,
            RouteFamily::V4
        ));
    }

    #[test]
    fn ipv4_parser_rejects_nondefault_down_reject_and_malformed_routes() {
        let valid = ipv4_route("eth0", RTF_UP);
        assert_eq!(parse_ipv4_default_route(&valid), Some("eth0"));
        assert_eq!(
            parse_ipv4_default_route(&valid.replace("0101A8C0", "00000000")),
            Some("eth0")
        );
        for line in [
            "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT".to_owned(),
            format!("{valid} extra"),
            valid.replacen("00000000", "0001A8C0", 1),
            valid.replace("100 00000000", "100 00FFFFFF"),
            valid.replace("0101A8C0", "invalid!"),
            valid.replace("100 00000000", "bad 00000000"),
            ipv4_route("eth0", RTF_UP | RTF_REJECT),
            ipv4_route("eth0", 0),
            ipv4_route("lo", RTF_UP),
        ] {
            assert!(
                parse_ipv4_default_route(&line).is_none(),
                "accepted invalid route: {line}"
            );
        }
    }

    #[test]
    fn default_interface_requires_current_operational_state() {
        assert!(has_usable_default_interface(
            Some("eth0"),
            &[v4("eth0", true)],
            RouteFamily::V4
        ));
        assert!(!has_usable_default_interface(
            Some("eth0"),
            &[v4("eth0", false)],
            RouteFamily::V4
        ));
    }

    #[test]
    fn another_interface_or_protocol_cannot_supply_the_default_address() {
        assert!(!has_usable_default_interface(
            Some("eth0"),
            &[v4("eth1", true)],
            RouteFamily::V4
        ));
        assert!(!has_usable_default_interface(
            Some("eth0"),
            &[v4("eth0", true)],
            RouteFamily::V6
        ));
        assert!(has_usable_default_interface(
            Some("eth0"),
            &[v6("eth0", true)],
            RouteFamily::V6
        ));
    }

    #[test]
    fn ipv6_defaults_work_with_and_without_a_gateway() {
        let interfaces = [v6("eth0", true)];
        assert!(has_usable_ipv6_default(&interfaces, &on_link("eth0")));
        let via = route(
            "eth0",
            Ipv6Addr::UNSPECIFIED,
            0,
            Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
            RTF_UP | 2,
        );
        assert!(has_usable_ipv6_default(&interfaces, &via));
    }

    #[test]
    fn ipv6_fallback_uses_its_own_working_interface() {
        let interfaces = [v4("eth0", false), v6("eth1", true)];
        assert!(!has_usable_default_interface(
            Some("eth0"),
            &interfaces,
            RouteFamily::V4
        ));
        assert!(has_usable_ipv6_default(&interfaces, &on_link("eth1")));
        assert!(!has_usable_ipv6_default(&interfaces, &on_link("eth0")));
        assert!(!has_usable_ipv6_default(
            &[v6("eth1", false)],
            &on_link("eth1")
        ));
        assert!(!has_usable_ipv6_default(
            &[v6("missing", true)],
            &on_link("eth0")
        ));
    }

    #[test]
    fn source_specific_default_requires_a_matching_local_address() {
        let interfaces = [v6("eth0", true)];
        let source = Ipv6Addr::new(0xfd00, 1, 0, 0, 0, 0, 0, 0);
        let matches = route("eth0", source, 64, Ipv6Addr::UNSPECIFIED, RTF_UP);
        let mismatch = route(
            "eth0",
            Ipv6Addr::new(0xfd00, 2, 0, 0, 0, 0, 0, 0),
            64,
            Ipv6Addr::UNSPECIFIED,
            RTF_UP,
        );
        assert!(has_usable_ipv6_default(&interfaces, &matches));
        assert!(!has_usable_ipv6_default(&interfaces, &mismatch));
        let exact = route(
            "eth0",
            Ipv6Addr::new(0xfd00, 1, 0, 0, 0, 0, 0, 2),
            128,
            Ipv6Addr::UNSPECIFIED,
            RTF_UP,
        );
        assert!(has_usable_ipv6_default(&interfaces, &exact));
    }

    #[test]
    fn rejects_nondefault_down_reject_loopback_and_malformed_routes() {
        let valid = on_link("eth0");
        let rejected = route(
            "eth0",
            Ipv6Addr::UNSPECIFIED,
            0,
            Ipv6Addr::UNSPECIFIED,
            RTF_UP | RTF_REJECT,
        );
        let down = route("eth0", Ipv6Addr::UNSPECIFIED, 0, Ipv6Addr::UNSPECIFIED, 0);
        for line in [
            String::new(),
            "not a route".to_owned(),
            format!("{valid} extra"),
            valid.replacen(" 00 ", " 80 ", 1),
            valid.replacen(
                "00000000000000000000000000000000",
                "20010000000000000000000000000000",
                1,
            ),
            valid.replacen("00000400", "zzzzzzzz", 1),
            route(
                "eth0",
                Ipv6Addr::UNSPECIFIED,
                129,
                Ipv6Addr::UNSPECIFIED,
                RTF_UP,
            ),
            rejected,
            down,
            on_link("lo"),
        ] {
            assert!(
                parse_ipv6_default_route(&line).is_none(),
                "accepted invalid route: {line}"
            );
        }
    }

    #[test]
    fn unusable_addresses_never_make_an_ipv6_default_available() {
        for ip in [
            Ipv6Addr::UNSPECIFIED,
            Ipv6Addr::LOCALHOST,
            Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 2),
            Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1),
        ] {
            assert!(!has_usable_ipv6_default(
                &[interface("eth0", IpAddr::V6(ip), true)],
                &on_link("eth0")
            ));
        }
        assert!(!has_usable_ipv6_default(
            &[v4("eth0", true)],
            &on_link("eth0")
        ));
    }

    #[test]
    fn missing_optional_route_file_is_not_a_monitor_failure() {
        assert!(
            optional_routes(Err(io::Error::from(io::ErrorKind::NotFound)))
                .unwrap()
                .is_none()
        );
        let error = optional_routes(Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "route access denied",
        )))
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Io);
        assert_eq!(error.io_kind(), Some(io::ErrorKind::PermissionDenied));
        assert_eq!(error.context().stage, Some(ErrorStage::NetworkMonitor));
        assert!(error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some());
        let interfaces = [v4("eth0", true)];
        assert_eq!(
            reachability_from(&interfaces, Some("eth0"), |_| Ok(String::new())).unwrap(),
            NetworkStatus::Unavailable,
        );
        assert_eq!(
            reachability_from(&interfaces, Some("eth0"), |_| Err(
                io::ErrorKind::NotFound.into()
            ))
            .unwrap(),
            NetworkStatus::Available,
        );
    }
}

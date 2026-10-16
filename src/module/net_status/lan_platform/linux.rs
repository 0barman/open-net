//! Linux metadata from the current network namespace, without subprocesses.
//!
//! Sysfs supplies carrier/operstate rather than relying on IFF_RUNNING (which
//! also includes UNKNOWN on Linux). `/proc/net/route` supplies the main IPv4
//! table; policy routing and a peer's actual reachability are not inferred.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::io;

use crate::network::LanInterfaceKind;

use super::{InterfaceMetadata, PlatformSnapshot};

trait NetworkFiles {
    fn read(&self, path: &str) -> io::Result<String>;
    fn exists(&self, path: &str) -> io::Result<bool>;
}

#[cfg(target_os = "linux")]
struct SystemFiles;

#[cfg(target_os = "linux")]
impl NetworkFiles for SystemFiles {
    fn read(&self, path: &str) -> io::Result<String> {
        std::fs::read_to_string(path)
    }

    fn exists(&self, path: &str) -> io::Result<bool> {
        match std::fs::metadata(path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn collect(ifaces: &[if_addrs::Interface]) -> PlatformSnapshot {
    collect_names(ifaces.iter().map(|iface| iface.name.as_str()), &SystemFiles)
}

fn collect_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
    files: &impl NetworkFiles,
) -> PlatformSnapshot {
    let mut diagnostics = Vec::new();
    let routes = match files.read("/proc/net/route") {
        Ok(contents) => match default_routes(&contents) {
            Ok(routes) => Some(routes),
            Err(error) => {
                diagnostics.push(format!("Linux IPv4 default route metadata: {error}"));
                None
            }
        },
        Err(error) => {
            diagnostics.push(format!("read /proc/net/route: {error}"));
            None
        }
    };

    // Multiple addresses on one interface must share one metadata sample.
    // Sorting also keeps diagnostics independent of address enumeration order.
    let names: BTreeSet<_> = names.into_iter().collect();
    let mut interfaces = HashMap::new();
    for name in names {
        let base = format!("/sys/class/net/{name}");
        interfaces.insert(
            name.to_owned(),
            InterfaceMetadata {
                kind: interface_kind(name, &base, files, &mut diagnostics),
                link_active: link_active(&base, files, &mut diagnostics),
                is_default_route: routes.as_ref().map(|routes| routes.contains(name)),
            },
        );
    }
    PlatformSnapshot {
        interfaces,
        diagnostics,
    }
}

fn optional_exists(
    path: &str,
    files: &impl NetworkFiles,
    diagnostics: &mut Vec<String>,
) -> Option<bool> {
    match files.exists(path) {
        Ok(exists) => Some(exists),
        Err(error) => {
            diagnostics.push(format!("inspect {path}: {error}"));
            None
        }
    }
}

fn read_attribute(
    path: &str,
    files: &impl NetworkFiles,
    diagnostics: &mut Vec<String>,
) -> Option<String> {
    match files.read(path) {
        Ok(value) => Some(value),
        Err(error) => {
            diagnostics.push(format!("read {path}: {error}"));
            None
        }
    }
}

fn interface_kind(
    name: &str,
    base: &str,
    files: &impl NetworkFiles,
    diagnostics: &mut Vec<String>,
) -> LanInterfaceKind {
    let wireless = optional_exists(&format!("{base}/wireless"), files, diagnostics);
    if wireless == Some(true) {
        return LanInterfaceKind::Wifi;
    }
    let bridge = optional_exists(&format!("{base}/bridge"), files, diagnostics);
    if bridge == Some(true) {
        return LanInterfaceKind::Bridge;
    }
    // TAP presents ARPHRD_ETHER, so inspect this kernel attribute before type.
    let tunnel = optional_exists(&format!("{base}/tun_flags"), files, diagnostics);
    if tunnel == Some(true) {
        return LanInterfaceKind::Tunnel;
    }
    let Some(kind) = read_attribute(&format!("{base}/type"), files, diagnostics) else {
        return LanInterfaceKind::Unknown;
    };
    // Values from Linux UAPI linux/if_arp.h. Do not equate an absent `device`
    // symlink with an unusable virtual device: bond0 and container eth0 may be
    // the only usable LAN interface in their network namespace.
    match kind.trim().parse::<u32>() {
        Ok(772) => LanInterfaceKind::Loopback,
        Ok(768 | 769 | 776 | 777 | 778 | 823) => LanInterfaceKind::Tunnel,
        Ok(801..=803) => LanInterfaceKind::Wifi,
        Ok(1) => {
            // A failed subtype probe must not silently turn a bridge/TAP/Wi-Fi
            // interface into a trusted Ethernet interface.
            if wireless.is_none() || bridge.is_none() || tunnel.is_none() {
                return LanInterfaceKind::Unknown;
            }
            let lower = name.to_ascii_lowercase();
            if [
                "veth", "virbr", "br-", "cni", "flannel", "docker", "vboxnet", "vmnet",
            ]
            .iter()
            .any(|prefix| lower.starts_with(prefix))
            {
                LanInterfaceKind::Virtual
            } else {
                LanInterfaceKind::Ethernet
            }
        }
        _ => {
            diagnostics.push(format!(
                "unknown Linux interface type for {name}: {}",
                kind.trim()
            ));
            LanInterfaceKind::Unknown
        }
    }
}

fn link_active(
    base: &str,
    files: &impl NetworkFiles,
    diagnostics: &mut Vec<String>,
) -> Option<bool> {
    let operstate =
        read_attribute(&format!("{base}/operstate"), files, diagnostics).and_then(|value| {
            match value.trim() {
                "up" => Some(true),
                "notpresent" | "down" | "lowerlayerdown" | "testing" | "dormant" => Some(false),
                "unknown" => None,
                _ => {
                    diagnostics.push(format!("invalid operstate for {base}: {}", value.trim()));
                    None
                }
            }
        });
    let carrier = read_attribute(&format!("{base}/carrier"), files, diagnostics).and_then(
        |value| match value.trim() {
            "1" => Some(true),
            "0" => Some(false),
            _ => {
                diagnostics.push(format!("invalid carrier for {base}: {}", value.trim()));
                None
            }
        },
    );

    // A dormant interface can have carrier while awaiting authentication.
    // Negative evidence also wins if the link changes during the two reads.
    match (operstate, carrier) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), _) | (_, Some(true)) => Some(true),
        (None, None) => {
            diagnostics.push(format!("Linux link state is unknown for {base}"));
            None
        }
    }
}

/// Interfaces with the lowest-metric usable default in the main IPv4 table.
/// A valid header with no such rows is a known absence, unlike read/parse errors.
fn default_routes(contents: &str) -> Result<HashSet<String>, String> {
    let mut lines = contents.lines().filter(|line| !line.trim().is_empty());
    let header = lines.next().ok_or("missing route table header")?;
    if header.split_whitespace().take(8).collect::<Vec<_>>()
        != [
            "Iface",
            "Destination",
            "Gateway",
            "Flags",
            "RefCnt",
            "Use",
            "Metric",
            "Mask",
        ]
    {
        return Err("unexpected route table header".to_owned());
    }

    let mut routes = HashSet::new();
    let mut best_metric = None;
    for (index, line) in lines.enumerate() {
        let fields: Vec<_> = line.split_whitespace().collect();
        let [iface, destination, _gateway, flags, _refcnt, _use, metric, mask, ..] =
            fields.as_slice()
        else {
            return Err(format!("malformed route row {}", index + 2));
        };
        let destination = u32::from_str_radix(destination, 16)
            .map_err(|_| format!("invalid destination in route row {}", index + 2))?;
        let mask = u32::from_str_radix(mask, 16)
            .map_err(|_| format!("invalid mask in route row {}", index + 2))?;
        let flags = u32::from_str_radix(flags, 16)
            .map_err(|_| format!("invalid flags in route row {}", index + 2))?;
        let metric = metric
            .parse::<u32>()
            .map_err(|_| format!("invalid metric in route row {}", index + 2))?;
        // RTF_UP=0x1; RTF_REJECT=0x200. A default need not have a gateway.
        if destination != 0 || mask != 0 || flags & 0x1 == 0 || flags & 0x200 != 0 {
            continue;
        }
        match best_metric {
            Some(best) if metric > best => continue,
            Some(best) if metric == best => {}
            _ => {
                best_metric = Some(metric);
                routes.clear();
            }
        }
        routes.insert((*iface).to_owned());
    }
    Ok(routes)
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;
    const ROUTE_HEADER: &str =
        "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\n";

    #[derive(Default)]
    struct FixtureFiles {
        contents: HashMap<String, String>,
        entries: HashSet<String>,
        denied: HashSet<String>,
    }

    impl FixtureFiles {
        fn routes(&mut self, rows: &str) {
            self.contents.insert(
                "/proc/net/route".to_owned(),
                format!("{ROUTE_HEADER}{rows}"),
            );
        }

        fn interface(&mut self, name: &str, kind: &str, operstate: &str, carrier: &str) {
            for (attribute, value) in [
                ("type", kind),
                ("operstate", operstate),
                ("carrier", carrier),
            ] {
                self.contents.insert(
                    format!("/sys/class/net/{name}/{attribute}"),
                    value.to_owned(),
                );
            }
        }

        fn entry(&mut self, name: &str, entry: &str) {
            self.entries
                .insert(format!("/sys/class/net/{name}/{entry}"));
        }
    }

    impl NetworkFiles for FixtureFiles {
        fn read(&self, path: &str) -> io::Result<String> {
            self.contents
                .get(path)
                .cloned()
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
        }

        fn exists(&self, path: &str) -> io::Result<bool> {
            if self.denied.contains(path) {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            } else {
                Ok(self.entries.contains(path))
            }
        }
    }

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(message.into())
        }
    }

    fn metadata<'a>(
        snapshot: &'a PlatformSnapshot,
        name: &str,
    ) -> Result<&'a InterfaceMetadata, Box<dyn std::error::Error>> {
        snapshot
            .interfaces
            .get(name)
            .ok_or_else(|| format!("missing metadata for {name}").into())
    }

    #[test]
    fn inactive_bridge_is_distinguished_from_active_wifi() -> TestResult {
        let mut files = FixtureFiles::default();
        files.routes("wlan0 00000000 010110AC 0003 0 0 100 00000000 0 0 0\n");
        files.interface("wlan0", "1", "up", "1");
        files.entry("wlan0", "wireless");
        files.interface("br0", "1", "down", "0");
        files.entry("br0", "bridge");
        let snapshot = collect_names(["br0", "wlan0", "wlan0"], &files);
        let wifi = metadata(&snapshot, "wlan0")?;
        let bridge = metadata(&snapshot, "br0")?;
        check(
            wifi.kind == LanInterfaceKind::Wifi && wifi.link_active == Some(true),
            "Wi-Fi link evidence was lost",
        )?;
        check(
            wifi.is_default_route == Some(true),
            "Wi-Fi default route was lost",
        )?;
        check(
            bridge.kind == LanInterfaceKind::Bridge && bridge.link_active == Some(false),
            "inactive bridge was not identified",
        )?;
        check(
            bridge.is_default_route == Some(false),
            "bridge acquired a fabricated default route",
        )?;
        check(
            snapshot.interfaces.len() == 2 && snapshot.diagnostics.is_empty(),
            "complete samples must deduplicate without diagnostics",
        )
    }

    #[test]
    fn virtual_interfaces_do_not_make_container_or_bond_ethernet_virtual() -> TestResult {
        let mut files = FixtureFiles::default();
        files.routes("");
        for name in ["veth123", "eth0", "bond0", "br0", "tap0"] {
            files.interface(name, "1", "up", "1");
        }
        files.entry("br0", "bridge");
        files.entry("tap0", "tun_flags");
        let snapshot = collect_names(["veth123", "eth0", "bond0", "br0", "tap0"], &files);
        check(
            metadata(&snapshot, "veth123")?.kind == LanInterfaceKind::Virtual,
            "veth must retain its virtual hint",
        )?;
        for name in ["eth0", "bond0"] {
            check(
                metadata(&snapshot, name)?.kind == LanInterfaceKind::Ethernet,
                "absence of a device symlink must not exclude usable Ethernet",
            )?;
        }
        check(
            metadata(&snapshot, "br0")?.kind == LanInterfaceKind::Bridge,
            "legal bridge must remain a bridge",
        )?;
        check(
            metadata(&snapshot, "tap0")?.kind == LanInterfaceKind::Tunnel,
            "TAP kernel evidence must override its Ethernet type",
        )?;
        check(
            snapshot
                .interfaces
                .values()
                .all(|entry| entry.is_default_route == Some(false)),
            "valid route table without defaults must report false",
        )
    }

    #[test]
    fn failed_or_malformed_routes_are_unknown_not_absent() -> TestResult {
        let mut files = FixtureFiles::default();
        files.interface("eth0", "1", "up", "1");
        for contents in [
            None,
            Some("broken header"),
            Some("Iface Destination Gateway Flags RefCnt Use Metric Mask\neth0 broken row"),
        ] {
            if let Some(contents) = contents {
                files
                    .contents
                    .insert("/proc/net/route".to_owned(), contents.to_owned());
            }
            let snapshot = collect_names(["eth0"], &files);
            check(
                metadata(&snapshot, "eth0")?.is_default_route.is_none(),
                "route failure became a known absence",
            )?;
            check(
                !snapshot.diagnostics.is_empty(),
                "route failure lost its diagnostic",
            )?;
            check(
                metadata(&snapshot, "eth0")?.link_active == Some(true),
                "route failure discarded independent link evidence",
            )?;
        }
        Ok(())
    }

    #[test]
    fn dormant_links_and_missing_metadata_are_not_assumed_active() -> TestResult {
        let mut files = FixtureFiles::default();
        files.routes("");
        files.interface("eth0", "1", "dormant", "1");
        files.interface("eth1", "1", "unknown", "1");
        files.interface("eth2", "1", "unknown", "invalid");
        let snapshot = collect_names(["eth0", "eth1", "eth2", "missing"], &files);
        check(
            metadata(&snapshot, "eth0")?.link_active == Some(false),
            "carrier alone must not mark dormant links active",
        )?;
        check(
            metadata(&snapshot, "eth1")?.link_active == Some(true),
            "known carrier must survive an unknown operstate",
        )?;
        check(
            metadata(&snapshot, "eth2")?.link_active.is_none(),
            "invalid carrier fabricated active state",
        )?;
        let missing = metadata(&snapshot, "missing")?;
        check(
            missing.link_active.is_none() && missing.kind == LanInterfaceKind::Unknown,
            "missing metadata must remain unknown",
        )?;
        check(
            !snapshot.diagnostics.is_empty(),
            "missing metadata must retain diagnostics",
        )
    }

    #[test]
    fn failed_subtype_probe_does_not_fabricate_ethernet() -> TestResult {
        let mut files = FixtureFiles::default();
        files.routes("");
        files.interface("eth0", "1", "up", "1");
        files
            .denied
            .insert("/sys/class/net/eth0/tun_flags".to_owned());
        let snapshot = collect_names(["eth0"], &files);
        check(
            metadata(&snapshot, "eth0")?.kind == LanInterfaceKind::Unknown,
            "failed TAP inspection must not fabricate Ethernet type",
        )?;
        check(
            !snapshot.diagnostics.is_empty(),
            "failed subtype inspection lost its diagnostic",
        )
    }

    #[test]
    fn lowest_metric_up_defaults_win_without_requiring_a_gateway() -> TestResult {
        let routes = default_routes(&format!(
            "{ROUTE_HEADER}\
            eth0 00000000 0100000A 0003 0 0 200 00000000 0 0 0\n\
            wlan0 00000000 00000000 0001 0 0 100 00000000 0 0 0\n\
            eth1 00000000 0100000A 0003 0 0 100 00000000 0 0 0\n\
            reject 00000000 00000000 0201 0 0 0 00000000 0 0 0\n\
            down 00000000 00000000 0002 0 0 0 00000000 0 0 0\n\
            subnet 00000000 00000000 0001 0 0 0 000000FF 0 0 0\n"
        ))?;
        check(
            routes == HashSet::from(["wlan0".to_owned(), "eth1".to_owned()]),
            "default route parsing did not respect flags, mask, and metric",
        )
    }
}

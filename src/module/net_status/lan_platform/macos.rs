use super::{LanInterfaceKind, PlatformSnapshot};
use core_foundation::{
    array::{CFArray, CFArrayRef},
    base::{CFAllocatorRef, CFType, CFTypeID, CFTypeRef, TCFType},
    boolean::CFBoolean,
    dictionary::{CFDictionary, CFDictionaryRef},
    string::{CFString, CFStringRef},
};
use std::ffi::c_void;

// SystemConfiguration's Create/Copy results are checked before transferring ownership
// to Core Foundation RAII values. No callbacks or run-loop sources are installed.
#[link(name = "SystemConfiguration", kind = "framework")]
extern "C" {
    fn SCDynamicStoreCreate(
        allocator: CFAllocatorRef,
        name: CFStringRef,
        callback: Option<unsafe extern "C" fn(*const c_void, CFArrayRef, *mut c_void)>,
        context: *mut c_void,
    ) -> CFTypeRef;
    fn SCDynamicStoreCopyMultiple(
        store: CFTypeRef,
        keys: CFArrayRef,
        patterns: CFArrayRef,
    ) -> CFDictionaryRef;
    fn SCNetworkInterfaceCopyAll() -> CFArrayRef;
    fn SCNetworkInterfaceGetTypeID() -> CFTypeID;
    fn SCNetworkInterfaceGetBSDName(interface: CFTypeRef) -> CFStringRef;
    fn SCNetworkInterfaceGetInterfaceType(interface: CFTypeRef) -> CFStringRef;
    fn SCError() -> i32;
}

fn dictionary_value(dictionary: &CFDictionary, key: &str) -> Option<CFType> {
    let key = CFString::new(key);
    let value = *dictionary.find(key.as_CFTypeRef())?;
    if value.is_null() {
        return None;
    }
    // SAFETY: SystemConfiguration dictionaries contain CF objects, retained by this
    // live dictionary. The get-rule owner retains the non-null value for its own lifetime.
    Some(unsafe { CFType::wrap_under_get_rule(value) })
}

fn string_value(dictionary: &CFDictionary, key: &str) -> Option<String> {
    dictionary_value(dictionary, key)?
        .downcast::<CFString>()
        .map(|value| value.to_string())
}

fn snapshot_store() -> Result<CFDictionary, String> {
    let name = CFString::new("open-net LAN interface snapshot");
    // SAFETY: the name lives through this call; the null allocator is the default
    // allocator, and null callback/context create a read-only dynamic store session.
    let store = unsafe {
        SCDynamicStoreCreate(
            std::ptr::null(),
            name.as_concrete_TypeRef(),
            None,
            std::ptr::null_mut(),
        )
    };
    if store.is_null() {
        // SAFETY: SCError has no pointer inputs.
        return Err(format!("SCDynamicStoreCreate failed: {}", unsafe {
            SCError()
        }));
    }
    // SAFETY: a non-null Create result is an owned CF object released once by RAII.
    let store = unsafe { CFType::wrap_under_create_rule(store) };
    let keys = CFArray::from_CFTypes(&[CFString::new("State:/Network/Global/IPv4")]);
    let patterns = CFArray::from_CFTypes(&[CFString::new("State:/Network/Interface/.*/Link")]);
    // SAFETY: every input owner remains alive and CopyMultiple returns a retained
    // dictionary or null; ownership is transferred only after checking the result.
    let values = unsafe {
        SCDynamicStoreCopyMultiple(
            store.as_CFTypeRef(),
            keys.as_concrete_TypeRef(),
            patterns.as_concrete_TypeRef(),
        )
    };
    if values.is_null() {
        // SAFETY: SCError has no pointer inputs.
        return Err(format!("SCDynamicStoreCopyMultiple failed: {}", unsafe {
            SCError()
        }));
    }
    // SAFETY: this is a non-null Copy result documented as a CFDictionary.
    Ok(unsafe { CFDictionary::wrap_under_create_rule(values) })
}

fn interface_kind(kind: &str) -> LanInterfaceKind {
    match kind {
        "IEEE80211" => LanInterfaceKind::Wifi,
        "Ethernet" => LanInterfaceKind::Ethernet,
        "Bridge" | "Bond" => LanInterfaceKind::Bridge,
        "PPP" | "IPSec" | "L2TP" | "PPTP" | "6to4" => LanInterfaceKind::Tunnel,
        "Loopback" => LanInterfaceKind::Loopback,
        // VLAN can be the host's legitimate LAN. Its native type alone does not
        // justify the default policy's exclusion of proxy/VM virtual adapters.
        "VLAN" => LanInterfaceKind::Unknown,
        _ => LanInterfaceKind::Unknown,
    }
}

fn interface_types(snapshot: &mut PlatformSnapshot) {
    // SAFETY: no input; the native API returns a retained array or null.
    let interfaces = unsafe { SCNetworkInterfaceCopyAll() };
    if interfaces.is_null() {
        // SAFETY: SCError has no pointer inputs.
        snapshot
            .diagnostics
            .push(format!("SCNetworkInterfaceCopyAll failed: {}", unsafe {
                SCError()
            }));
        return;
    }
    // SAFETY: the non-null Copy result contains CF interface objects owned by the array.
    let interfaces: CFArray<CFType> = unsafe { CFArray::wrap_under_create_rule(interfaces) };
    if interfaces.len() > 65_536 {
        snapshot
            .diagnostics
            .push("native interface inventory exceeded collection budget".to_owned());
        return;
    }
    for interface in interfaces.iter() {
        // SAFETY: GetTypeID has no inputs; interface-specific getters below are used
        // only after checking the object type while its owning array remains alive.
        if interface.type_of() != unsafe { SCNetworkInterfaceGetTypeID() } {
            snapshot
                .diagnostics
                .push("SCNetworkInterfaceCopyAll returned an unexpected CF object".to_owned());
            continue;
        }
        // SAFETY: the interface has the checked native type and is owned by the array.
        let (name, kind) = unsafe {
            (
                SCNetworkInterfaceGetBSDName(interface.as_CFTypeRef()),
                SCNetworkInterfaceGetInterfaceType(interface.as_CFTypeRef()),
            )
        };
        if name.is_null() || kind.is_null() {
            continue;
        }
        // SAFETY: both native getters return CFString or null; the checked non-null
        // strings are retained under the get rule before the parent can be released.
        let (name, kind) = unsafe {
            (
                CFString::wrap_under_get_rule(name).to_string(),
                CFString::wrap_under_get_rule(kind).to_string(),
            )
        };
        snapshot.interfaces.entry(name).or_default().kind = interface_kind(&kind);
    }
}

fn apply_store(values: &CFDictionary, names: &[&str], snapshot: &mut PlatformSnapshot) {
    let primary = dictionary_value(values, "State:/Network/Global/IPv4")
        .and_then(|value| value.downcast::<CFDictionary>())
        .and_then(|value| string_value(&value, "PrimaryInterface"))
        .filter(|name| !name.is_empty());
    if primary.is_none() && !names.is_empty() {
        snapshot
            .diagnostics
            .push("IPv4 primary interface is unavailable in SystemConfiguration".to_owned());
    }
    for name in names {
        let metadata = snapshot.interfaces.entry((*name).to_owned()).or_default();
        // A missing primary interface is unknown, not proof that every interface is non-default.
        metadata.is_default_route = primary.as_ref().map(|primary| primary == name);
        let link_key = format!("State:/Network/Interface/{name}/Link");
        let Some(value) = dictionary_value(values, &link_key) else {
            continue;
        };
        let active = value
            .downcast::<CFDictionary>()
            .and_then(|dictionary| dictionary_value(&dictionary, "Active"))
            .and_then(|value| value.downcast::<CFBoolean>())
            .map(bool::from);
        metadata.link_active = active;
        if active.is_none() {
            snapshot
                .diagnostics
                .push(format!("interface {name} has no valid Link/Active value"));
        }
    }
}

pub(crate) fn collect(ifaces: &[if_addrs::Interface]) -> PlatformSnapshot {
    let mut snapshot = PlatformSnapshot::default();
    interface_types(&mut snapshot);
    match snapshot_store() {
        Ok(values) => {
            let mut names: Vec<_> = ifaces.iter().map(|iface| iface.name.as_str()).collect();
            names.sort_unstable();
            names.dedup();
            apply_store(&values, &names, &mut snapshot);
        }
        Err(error) => snapshot.diagnostics.push(error),
    }
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dictionary(entries: &[(&str, CFType)]) -> CFDictionary {
        let entries: Vec<_> = entries
            .iter()
            .map(|(key, value)| (CFString::new(key), value.clone()))
            .collect();
        CFDictionary::from_CFType_pairs(&entries).into_untyped()
    }

    #[test]
    fn active_wifi_and_inactive_bridge_preserve_independent_evidence() -> Result<(), String> {
        let global = dictionary(&[("PrimaryInterface", CFString::new("en0").as_CFType())]);
        let wifi = dictionary(&[("Active", CFBoolean::true_value().as_CFType())]);
        let bridge = dictionary(&[("Active", CFBoolean::false_value().as_CFType())]);
        let values = dictionary(&[
            ("State:/Network/Global/IPv4", global.as_CFType()),
            ("State:/Network/Interface/en0/Link", wifi.as_CFType()),
            (
                "State:/Network/Interface/bridge100/Link",
                bridge.as_CFType(),
            ),
        ]);
        let mut snapshot = PlatformSnapshot::default();
        apply_store(&values, &["en0", "bridge100"], &mut snapshot);
        let wifi = snapshot
            .interfaces
            .get("en0")
            .ok_or("missing Wi-Fi evidence")?;
        let bridge = snapshot
            .interfaces
            .get("bridge100")
            .ok_or("missing bridge evidence")?;
        if wifi.link_active != Some(true)
            || wifi.is_default_route != Some(true)
            || bridge.link_active != Some(false)
            || bridge.is_default_route != Some(false)
            || !snapshot.diagnostics.is_empty()
        {
            return Err("native link and default route evidence were conflated".to_owned());
        }
        Ok(())
    }

    #[test]
    fn absent_or_malformed_route_and_link_values_remain_unknown() -> Result<(), String> {
        for values in [
            dictionary(&[]),
            dictionary(&[
                (
                    "State:/Network/Global/IPv4",
                    CFBoolean::false_value().as_CFType(),
                ),
                (
                    "State:/Network/Interface/en0/Link",
                    CFString::new("invalid").as_CFType(),
                ),
            ]),
        ] {
            let mut snapshot = PlatformSnapshot::default();
            apply_store(&values, &["en0"], &mut snapshot);
            let metadata = snapshot
                .interfaces
                .get("en0")
                .ok_or("missing interface evidence")?;
            if metadata.is_default_route.is_some()
                || metadata.link_active.is_some()
                || snapshot.diagnostics.is_empty()
            {
                return Err("unavailable native evidence was turned into a known fact".to_owned());
            }
        }
        Ok(())
    }

    #[test]
    fn native_interface_types_do_not_depend_on_bsd_names() -> Result<(), String> {
        for (native, expected) in [
            ("IEEE80211", LanInterfaceKind::Wifi),
            ("Ethernet", LanInterfaceKind::Ethernet),
            ("Bridge", LanInterfaceKind::Bridge),
            ("Bond", LanInterfaceKind::Bridge),
            ("IPSec", LanInterfaceKind::Tunnel),
            ("VLAN", LanInterfaceKind::Unknown),
            ("unrecognized", LanInterfaceKind::Unknown),
        ] {
            if interface_kind(native) != expected {
                return Err(format!("incorrect classification for {native}"));
            }
        }
        Ok(())
    }
}

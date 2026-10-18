//! Native interface facts; unavailable evidence is never converted into a negative fact.

use std::collections::HashMap;

use crate::network::LanInterfaceKind;

#[derive(Clone, Debug, Default)]
pub(crate) struct InterfaceMetadata {
    pub(crate) kind: LanInterfaceKind,
    pub(crate) link_active: Option<bool>,
    pub(crate) is_default_route: Option<bool>,
}

#[derive(Default)]
pub(crate) struct PlatformSnapshot {
    pub(crate) interfaces: HashMap<String, InterfaceMetadata>,
    pub(crate) diagnostics: Vec<String>,
    // Fixed logging classification, independent of potentially sensitive diagnostics.
    // Bits 2/3: route/interface queries actually failed (missing metadata is not failure).
    pub(crate) failure_classes: u8,
}

pub(crate) fn interface_key(iface: &if_addrs::Interface) -> &str {
    #[cfg(windows)]
    {
        &iface.adapter_name
    }
    #[cfg(not(windows))]
    {
        &iface.name
    }
}

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub(crate) use macos::collect;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(crate) use windows::collect;

// Pure Linux parser tests run on development hosts as well as Linux.
#[cfg(any(target_os = "linux", test))]
mod linux;
#[cfg(target_os = "linux")]
pub(crate) use linux::collect;

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
pub(crate) fn collect(_ifaces: &[if_addrs::Interface]) -> PlatformSnapshot {
    PlatformSnapshot {
        diagnostics: vec!["native LAN metadata unavailable on this platform".to_owned()],
        ..PlatformSnapshot::default()
    }
}

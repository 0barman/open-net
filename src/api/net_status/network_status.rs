use std::fmt;

/// Current host-level network reachability reported by the monitor.
///
/// Evidence differs by platform: Windows uses Network List Manager's Internet
/// connectivity flags; macOS uses netwatch's default-route and IP-capability
/// facts; Linux checks operational interfaces and default routes. These are
/// host observations, not probes of a particular server, DNS service, or URL.
/// Always handle errors from the actual network operation.
///
/// An absent `NetworkSnapshot::reachability` means no current observation, not
/// `Unavailable`. `Stopped`, `Starting`, `Failed`, and `Closed` describe the
/// monitor's lifecycle and must not be converted into a network disconnection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkStatus {
    /// The platform observation does not establish host network availability.
    /// This does not imply that every local or remote endpoint is unreachable.
    Unavailable,
    /// The platform observation indicates host network availability.
    /// A request can still fail because of DNS, TLS, a firewall, or the server.
    Available,
}

impl NetworkStatus {
    /// The variant name as a static string (e.g. `"Available"`).
    ///
    /// Used both by the [`fmt::Display`] implementation and as a stable,
    /// human-readable label when forwarding the status to logging.
    pub const fn name(&self) -> &'static str {
        match self {
            NetworkStatus::Unavailable => "Unavailable",
            NetworkStatus::Available => "Available",
        }
    }
}

impl fmt::Display for NetworkStatus {
    /// Renders the variant name, so `to_string()` yields `"Available"` /
    /// `"Unavailable"` rather than a structural form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

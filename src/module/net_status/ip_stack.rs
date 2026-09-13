use std::fmt;

/// The IP-stack capability currently available to the host.
///
/// This reflects which IP protocol versions the host has usable addresses /
/// routes for, as reported by the underlying [`netwatch`] interface state
/// (`have_v4` / `have_v6`). The semantics are identical across platforms: it
/// describes local stack availability, not per-protocol Internet reachability.
///
/// [`netwatch`]: https://crates.io/crates/netwatch
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IpStack {
    /// Neither IPv4 nor IPv6 is available.
    #[default]
    None,
    /// Only IPv4 is available.
    V4Only,
    /// Only IPv6 is available.
    V6Only,
    /// Both IPv4 and IPv6 are available.
    DualStack,
}

impl IpStack {
    /// Fold the two protocol-availability flags into a single [`IpStack`].
    pub(crate) const fn from_flags(have_v4: bool, have_v6: bool) -> Self {
        match (have_v4, have_v6) {
            (true, true) => IpStack::DualStack,
            (true, false) => IpStack::V4Only,
            (false, true) => IpStack::V6Only,
            (false, false) => IpStack::None,
        }
    }

    /// The variant name as a static string (e.g. `"DualStack"`).
    ///
    /// Used both by the [`fmt::Display`] implementation and as a stable,
    /// human-readable label when forwarding the value to logging.
    pub const fn name(&self) -> &'static str {
        match self {
            IpStack::None => "None",
            IpStack::V4Only => "V4Only",
            IpStack::V6Only => "V6Only",
            IpStack::DualStack => "DualStack",
        }
    }

    /// Whether IPv4 is available (true for [`IpStack::V4Only`] and
    /// [`IpStack::DualStack`]).
    pub const fn has_ipv4(&self) -> bool {
        matches!(self, IpStack::V4Only | IpStack::DualStack)
    }

    /// Whether IPv6 is available (true for [`IpStack::V6Only`] and
    /// [`IpStack::DualStack`]).
    pub const fn has_ipv6(&self) -> bool {
        matches!(self, IpStack::V6Only | IpStack::DualStack)
    }

    /// Whether both IPv4 and IPv6 are available.
    pub const fn is_dual_stack(&self) -> bool {
        matches!(self, IpStack::DualStack)
    }
}

impl fmt::Display for IpStack {
    /// Renders the variant name, so `to_string()` yields e.g. `"DualStack"`
    /// rather than a structural form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::IpStack;

    #[test]
    fn from_flags_covers_all_combinations() {
        assert_eq!(IpStack::from_flags(false, false), IpStack::None);
        assert_eq!(IpStack::from_flags(true, false), IpStack::V4Only);
        assert_eq!(IpStack::from_flags(false, true), IpStack::V6Only);
        assert_eq!(IpStack::from_flags(true, true), IpStack::DualStack);
    }

    #[test]
    fn default_is_none() {
        assert_eq!(IpStack::default(), IpStack::None);
    }

    #[test]
    fn has_ipv4_is_true_only_for_v4_and_dual() {
        assert!(!IpStack::None.has_ipv4());
        assert!(IpStack::V4Only.has_ipv4());
        assert!(!IpStack::V6Only.has_ipv4());
        assert!(IpStack::DualStack.has_ipv4());
    }

    #[test]
    fn has_ipv6_is_true_only_for_v6_and_dual() {
        assert!(!IpStack::None.has_ipv6());
        assert!(!IpStack::V4Only.has_ipv6());
        assert!(IpStack::V6Only.has_ipv6());
        assert!(IpStack::DualStack.has_ipv6());
    }

    #[test]
    fn is_dual_stack_is_true_only_for_dual() {
        assert!(!IpStack::None.is_dual_stack());
        assert!(!IpStack::V4Only.is_dual_stack());
        assert!(!IpStack::V6Only.is_dual_stack());
        assert!(IpStack::DualStack.is_dual_stack());
    }

    #[test]
    fn name_and_display_agree() {
        for variant in [
            IpStack::None,
            IpStack::V4Only,
            IpStack::V6Only,
            IpStack::DualStack,
        ] {
            assert_eq!(variant.name(), variant.to_string());
        }
        assert_eq!(IpStack::DualStack.name(), "DualStack");
    }

    #[test]
    fn names_are_stable_for_every_variant() {
        for (variant, expected) in [
            (IpStack::None, "None"),
            (IpStack::V4Only, "V4Only"),
            (IpStack::V6Only, "V6Only"),
            (IpStack::DualStack, "DualStack"),
        ] {
            assert_eq!(variant.name(), expected);
        }
    }
}

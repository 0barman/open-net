use std::fmt;

/// The IP-stack capability currently available to the host.
///
/// This reflects which IP protocol versions the host has usable addresses /
/// routes for, as reported by the underlying [`netwatch`] interface state
/// (`have_v4` / `have_v6`). The semantics are identical across platforms: it
/// describes local stack availability, not per-protocol Internet reachability.
///
/// [`netwatch`]: https://crates.io/crates/netwatch
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IpStack {
    /// Neither IPv4 nor IPv6 is available.
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

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    #[test]
    fn from_flags_covers_all_combinations() -> TestResult {
        for (v4, v6, expected) in [
            (false, false, IpStack::None),
            (true, false, IpStack::V4Only),
            (false, true, IpStack::V6Only),
            (true, true, IpStack::DualStack),
        ] {
            check(
                IpStack::from_flags(v4, v6) == expected,
                "incorrect IP flags mapping",
            )?;
        }
        Ok(())
    }

    #[test]
    fn has_ipv4_is_true_only_for_v4_and_dual() -> TestResult {
        check(
            !IpStack::None.has_ipv4()
                && IpStack::V4Only.has_ipv4()
                && !IpStack::V6Only.has_ipv4()
                && IpStack::DualStack.has_ipv4(),
            "IPv4 availability differs from the stack variant",
        )
    }

    #[test]
    fn has_ipv6_is_true_only_for_v6_and_dual() -> TestResult {
        check(
            !IpStack::None.has_ipv6()
                && !IpStack::V4Only.has_ipv6()
                && IpStack::V6Only.has_ipv6()
                && IpStack::DualStack.has_ipv6(),
            "IPv6 availability differs from the stack variant",
        )
    }

    #[test]
    fn is_dual_stack_is_true_only_for_dual() -> TestResult {
        check(
            !IpStack::None.is_dual_stack()
                && !IpStack::V4Only.is_dual_stack()
                && !IpStack::V6Only.is_dual_stack()
                && IpStack::DualStack.is_dual_stack(),
            "dual-stack query differs from the stack variant",
        )
    }

    #[test]
    fn name_and_display_agree() -> TestResult {
        for variant in [
            IpStack::None,
            IpStack::V4Only,
            IpStack::V6Only,
            IpStack::DualStack,
        ] {
            check(
                variant.name() == variant.to_string(),
                "IP stack name and Display disagree",
            )?;
        }
        check(
            IpStack::DualStack.name() == "DualStack",
            "dual-stack label changed",
        )
    }

    #[test]
    fn names_are_stable_for_every_variant() -> TestResult {
        for (variant, expected) in [
            (IpStack::None, "None"),
            (IpStack::V4Only, "V4Only"),
            (IpStack::V6Only, "V6Only"),
            (IpStack::DualStack, "DualStack"),
        ] {
            check(variant.name() == expected, "IP stack label changed")?;
        }
        Ok(())
    }
}

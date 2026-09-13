use std::fmt;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NetworkStatus {
    /// Network is unavailable.
    #[default]
    Unavailable,
    /// Network is available.
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

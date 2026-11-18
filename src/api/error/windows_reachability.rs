//! Portable values for diagnosing native Windows reachability failures.

use std::fmt;

/// The native call that failed while sampling Windows Internet reachability.
///
/// Available on all platforms without a Windows SDK dependency. Use
/// [`Self::as_str`] for stable identifiers in structured application events.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowsReachabilityOperation {
    /// `CoInitializeEx` failed to initialize the sampling thread's COM apartment.
    ComInit,
    /// `CoCreateInstance` failed to create the Network List Manager interface.
    NlmCreate,
    /// `INetworkListManager::GetConnectivity` failed to return a connectivity value.
    NlmGetConnectivity,
}

impl WindowsReachabilityOperation {
    /// Returns the stable operation identifier for structured diagnostics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ComInit => "com_init",
            Self::NlmCreate => "nlm_create",
            Self::NlmGetConnectivity => "nlm_get_connectivity",
        }
    }
}

/// The original native failure from Windows Internet-reachability sampling.
///
/// These details supplement [`super::ErrorKind::Io`] and
/// [`super::ErrorStage::NetworkMonitor`]. The original Windows error remains
/// available through [`std::error::Error::source`] on [`super::NetError`].
/// No system error text, machine name or network name is retained here.
#[non_exhaustive]
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct WindowsReachabilityDiagnostic {
    /// Native operation that returned this failure.
    pub operation: WindowsReachabilityOperation,
    /// Original HRESULT's 32-bit bit pattern, without Win32 error conversion.
    ///
    /// Format with `format!("0x{:08X}", detail.hresult)` for fixed-width hex.
    pub hresult: u32,
}

impl fmt::Debug for WindowsReachabilityDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WindowsReachabilityDiagnostic")
            .field("operation", &self.operation.as_str())
            .field("hresult", &format_args!("0x{:08X}", self.hresult))
            .finish()
    }
}

#[cfg(test)]
#[path = "windows_reachability_tests.rs"]
mod tests;

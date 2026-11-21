//! On-demand evidence from the operating system, independent of reachability.
use super::NetworkEventOptions;
use crate::NetError;
use std::time::SystemTime;
use tokio::sync::Semaphore;

/// Sources and per-subscriber queue limits for interface diagnostics.
/// Construct with `Default::default()` and set the fields your application needs.
#[non_exhaustive]
#[derive(Clone, Copy, Debug)]
pub struct InterfaceEventOptions {
    /// Collect Windows WLAN connection and disconnection records.
    pub wlan: bool,
    /// Observe preferred IPv4 and IPv6 default-route interfaces independently.
    pub default_route: bool,
    /// Maximum queued entries, including source status and route baselines.
    pub capacity: usize,
    /// Maximum accounted bytes in the queue, including interface identifiers.
    pub max_bytes: usize,
}

impl Default for InterfaceEventOptions {
    fn default() -> Self {
        let queue = NetworkEventOptions::default();
        Self {
            wlan: true,
            default_route: true,
            capacity: queue.capacity,
            max_bytes: queue.max_bytes,
        }
    }
}

impl InterfaceEventOptions {
    pub(crate) fn validate(&self) -> Result<(), NetError> {
        if !self.wlan && !self.default_route {
            return Err(NetError::config(
                "interface_events.sources",
                "select at least one source",
            ));
        }
        if self.capacity == 0 || self.capacity > Semaphore::MAX_PERMITS {
            return Err(NetError::config(
                "interface_events.capacity",
                "must be a positive supported capacity",
            ));
        }
        if self.max_bytes == 0
            || self.max_bytes > Semaphore::MAX_PERMITS
            || self.max_bytes > u32::MAX as usize
        {
            return Err(NetError::config(
                "interface_events.max_bytes",
                "must be a positive supported byte allowance",
            ));
        }
        Ok(())
    }
}

/// Independently selectable Windows diagnostic source.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum InterfaceEventSource {
    /// Connection records from Windows WLAN-AutoConfig/Operational.
    Wlan,
    /// Preferred default-route observations and per-family collection status.
    DefaultRoute,
}

/// IP family of an observed default route.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum InterfaceAddressFamily {
    Ipv4,
    Ipv6,
}

/// Result of a successful default-route observation for one IP family.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DefaultRouteState {
    /// No eligible default route.
    NoRoute,
    /// Several different interfaces share the lowest effective metric.
    Ambiguous,
    /// The preferred route(s) resolve to one interface.
    Unique { interface_id: String },
}

/// A change, baseline, or source diagnostic in the opt-in interface event stream.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum InterfaceEventKind {
    /// This source started observing.
    SourceReady,
    /// This source could not collect evidence. Other selected sources can continue.
    SourceUnavailable {
        error: NetError,
        native_code: Option<i64>,
    },
    /// The source detected a gap in its coverage. Historical entries are not replayed.
    SourceGap {
        error: NetError,
        native_code: Option<i64>,
    },
    /// A successful WLAN connection. The native code is the provider's event ID.
    /// The interface GUID is in lower-case hyphenated form, without braces.
    WlanConnected {
        interface_id: String,
        native_code: u32,
        reason_code: Option<u32>,
    },
    /// A WLAN disconnect record, with the native event ID and optional numeric reason.
    WlanDisconnected {
        interface_id: String,
        native_code: u32,
        reason_code: Option<u32>,
    },
    /// Initial baseline, recovery, or a change involving a non-unique or absent route.
    DefaultRouteObserved {
        family: InterfaceAddressFamily,
        state: DefaultRouteState,
    },
    /// Two consecutive successful observations uniquely identified different interfaces.
    DefaultRouteChanged {
        family: InterfaceAddressFamily,
        previous: String,
        current: String,
    },
    /// A read failed and invalidated the comparison baseline for this family.
    DefaultRouteQueryFailed {
        family: InterfaceAddressFamily,
        error: NetError,
        native_code: Option<i64>,
    },
}

/// One record from an explicit subscription. Sequence orders SDK delivery within
/// this subscription. Independent Windows sources can deliver records with delay.
/// WLAN records omit SSID, BSSID, and profile names.
#[derive(Clone, Debug)]
pub struct InterfaceEvent {
    /// Monotonically increasing sequence for this subscription.
    pub sequence: u64,
    /// SDK wall-clock time when this evidence was observed.
    pub observed_at: SystemTime,
    /// Windows record time when supplied by the source.
    pub native_time: Option<SystemTime>,
    /// Source to which this record or diagnostic belongs.
    pub source: InterfaceEventSource,
    /// Change, baseline, or status of this source.
    pub kind: InterfaceEventKind,
}

#[cfg(windows)]
impl InterfaceEvent {
    pub(crate) fn accounted_bytes(&self) -> usize {
        use InterfaceEventKind::*;
        let extra = match &self.kind {
            WlanConnected { interface_id, .. }
            | WlanDisconnected { interface_id, .. }
            | DefaultRouteObserved {
                state: DefaultRouteState::Unique { interface_id },
                ..
            } => interface_id.capacity(),
            DefaultRouteChanged {
                previous, current, ..
            } => previous.capacity().saturating_add(current.capacity()),
            // Source diagnostics carry an error kind, stage and optional numeric OS code.
            #[cfg(feature = "ws-client")]
            SourceUnavailable { .. } | SourceGap { .. } | DefaultRouteQueryFailed { .. } => {
                std::mem::size_of::<crate::error::ErrorContext>()
            }
            _ => 0,
        };
        std::mem::size_of::<Self>().saturating_add(extra)
    }
}

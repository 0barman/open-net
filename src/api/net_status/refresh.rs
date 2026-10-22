use std::time::SystemTime;

use super::NetworkSnapshot;
use crate::NetError;

/// Provenance of an explicitly requested network name.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkNameSource {
    /// Legacy provenance retained for source compatibility. Connectivity
    /// refreshes no longer preserve or return cached names.
    CachedObservation,
    /// An active Wi-Fi SSID.
    WifiSsid,
    /// A connected network reported by Windows Network List Manager.
    NetworkListManager,
    /// The system queries completed without a connected network name.
    None,
    /// No name query was requested. All connectivity refreshes return this value.
    NotCollected,
}

/// Provenance of the IP-stack field included in a refresh result.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkIpStackSource {
    /// The latest background interface observation, not this Windows query.
    BackgroundObservation,
}

/// A completed Windows sampling window and the associated client snapshot.
///
/// All refresh methods query only reachability. Network names require a
/// separate explicit name observer and never enter this result. The IP-stack
/// field retains the latest background interface observation. These system calls
/// form a sampling window, not an atomic Windows snapshot. A successful query
/// with unchanged values preserves the snapshot's revision and observed_at.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct NetworkRefreshResult {
    /// This client's latest snapshot after committing the sample. Compare
    /// `sampled_reachability` for this sampling window's own system result.
    pub snapshot: NetworkSnapshot,
    /// Reachability returned by this sampling window's own Windows query.
    /// `snapshot.reachability` may already contain a newer observation.
    pub sampled_reachability: super::NetworkStatus,
    /// Increasing sampling-start identifier within this OpenNet engine. Results
    /// may complete out of order; failed/cancelled samples may leave gaps. This
    /// is not a snapshot or event revision.
    pub sample_id: u64,
    /// Wall-clock time immediately before this round's system queries.
    pub sample_started_at: SystemTime,
    /// Wall-clock time immediately after the queries. Clock corrections can
    /// move wall time backwards; use sample_id for ordering.
    pub sampled_at: SystemTime,
    /// Compatibility field; always `NetworkNameSource::NotCollected`.
    pub network_name_source: NetworkNameSource,
    /// Compatibility field; always empty. Name failures belong to the explicit
    /// name observation and cannot fail a connectivity refresh.
    pub network_name_diagnostics: Vec<NetError>,
    /// Explicitly identifies the IP-stack field as a background observation.
    pub ip_stack_source: NetworkIpStackSource,
}

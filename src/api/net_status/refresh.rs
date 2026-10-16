use std::time::SystemTime;

use super::NetworkSnapshot;
use crate::NetError;

/// Provenance of the network name included in a refresh result.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkNameSource {
    /// The latest cached name, preserved without querying Windows for a name.
    /// Used by `NetStatusClient::refresh_reachability`.
    CachedObservation,
    /// An active Wi-Fi SSID.
    WifiSsid,
    /// A connected network reported by Windows Network List Manager.
    NetworkListManager,
    /// The system queries completed without a connected network name.
    None,
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
/// `refresh` freshly queries reachability and network name;
/// `refresh_reachability` queries only reachability and preserves the cached
/// name, identified by `NetworkNameSource::CachedObservation`. The IP-stack
/// field retains the latest background interface observation. These system calls
/// form a sampling window, not an atomic Windows snapshot. A successful query
/// with unchanged values preserves the snapshot's revision and observed_at.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct NetworkRefreshResult {
    /// This client's latest snapshot after committing the sample. Each field
    /// retains its newest observation: a full query whose name was slow may
    /// preserve reachability from a later reachability-only query. Compare
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
    /// Source of the network name, including a successful absence or a cached
    /// value which was not queried by this sample.
    pub network_name_source: NetworkNameSource,
    /// Query faults bypassed by a successful fresh NLM name fallback.
    pub network_name_diagnostics: Vec<NetError>,
    /// Explicitly identifies the IP-stack field as a background observation.
    pub ip_stack_source: NetworkIpStackSource,
}

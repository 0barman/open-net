//! Explicit, isolated observation of potentially identifying network names.
use std::fmt;
use std::time::{Duration, SystemTime};

use crate::error::{ErrorKind, ErrorStage, NetError};
use crate::module::net_status::inner::names::NameConsumer;
use crate::net_status::NetworkNameSource;
use std::sync::Arc;

/// Sampling options for an explicitly requested network-name observer.
///
/// This policy is independent of connection availability policies. Compiling
/// network monitoring support does not enable name collection.
#[derive(Clone, Copy, Debug)]
pub struct NetworkNameOptions {
    interval: Duration,
    timeout: Duration,
}

impl Default for NetworkNameOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(10),
        }
    }
}

impl NetworkNameOptions {
    /// Set the delay after one sample completes before this observer samples again.
    /// Valid values range from one second through one hour, inclusive.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Set the validity budget for a native sampling operation.
    /// Valid values range from 100 milliseconds through 30 seconds, inclusive.
    /// The budget begins when the serial worker starts this observer's query;
    /// it does not impose a deadline on waiting behind other observers.
    /// Native calls may finish after this budget, but expired values are discarded.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Delay between completed sampling operations for this observer.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Validity budget for each native query.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn validate(&self) -> Result<(), NetError> {
        if !(Duration::from_secs(1)..=Duration::from_secs(3600)).contains(&self.interval)
            || !(Duration::from_millis(100)..=Duration::from_secs(30)).contains(&self.timeout)
        {
            return Err(
                NetError::from(ErrorKind::InvalidInput).with_stage(ErrorStage::NetworkMonitor)
            );
        }
        Ok(())
    }
}

/// Lifecycle of a dedicated name observation.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkNameState {
    /// The observer has not received a completed sample.
    Pending,
    /// A query completed, possibly without an available name.
    Ready,
    /// The latest query failed. No previous name is retained.
    Failed,
    /// Collection stopped and the SDK's retained values were cleared.
    Closed,
}

/// An owned network-name observation, separate from ordinary connectivity state.
///
/// Names may identify a person or location. Only `name()` exposes the raw value;
/// `Debug` excludes it. Values already returned to the application are owned by
/// that application and cannot be revoked by closing the observer.
#[derive(Clone)]
pub struct NetworkNameSnapshot {
    state: NetworkNameState,
    name: Option<String>,
    source: NetworkNameSource,
    sampled_at: Option<SystemTime>,
    error: Option<NetError>,
    diagnostics: Vec<NetError>,
    #[cfg(test)]
    retention_probe: Option<Arc<()>>,
}

impl fmt::Debug for NetworkNameSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NetworkNameSnapshot")
            .field("state", &self.state)
            .field("name_present", &self.name.is_some())
            .field("source", &self.source)
            .field("sampled_at", &self.sampled_at)
            .field("error_kind", &self.error.as_ref().map(NetError::kind))
            .field("diagnostic_count", &self.diagnostics.len())
            .finish()
    }
}

impl NetworkNameSnapshot {
    /// Current collection state.
    pub fn state(&self) -> NetworkNameState {
        self.state
    }

    /// The raw network name, available only after a successful query.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Native source of the name, or NotCollected for an empty lifecycle state.
    pub fn source(&self) -> NetworkNameSource {
        self.source
    }

    /// Wall-clock completion time of a successful query.
    pub fn sampled_at(&self) -> Option<SystemTime> {
        self.sampled_at
    }

    /// A classified failure without raw native source text.
    pub fn error(&self) -> Option<&NetError> {
        self.error.as_ref()
    }

    /// Classified faults bypassed by a successful name fallback.
    pub fn diagnostics(&self) -> &[NetError] {
        &self.diagnostics
    }

    pub(crate) fn empty(state: NetworkNameState) -> Self {
        Self {
            state,
            name: None,
            source: NetworkNameSource::NotCollected,
            sampled_at: None,
            error: None,
            diagnostics: Vec::new(),
            #[cfg(test)]
            retention_probe: None,
        }
    }

    #[cfg(any(windows, test))]
    pub(crate) fn ready(
        name: Option<String>,
        source: NetworkNameSource,
        sampled_at: SystemTime,
        diagnostics: Vec<NetError>,
    ) -> Self {
        Self {
            state: NetworkNameState::Ready,
            name,
            source,
            sampled_at: Some(sampled_at),
            error: None,
            diagnostics: diagnostics.iter().map(classified).collect(),
            #[cfg(test)]
            retention_probe: None,
        }
    }

    pub(crate) fn failed(error: &NetError) -> Self {
        Self {
            error: Some(classified(error)),
            ..Self::empty(NetworkNameState::Failed)
        }
    }

    #[cfg(test)]
    pub(crate) fn retain_probe_for_test(&mut self, probe: Arc<()>) {
        self.retention_probe = Some(probe);
    }
}

fn classified(error: &NetError) -> NetError {
    // Neither arbitrary source objects nor potentially identifying context are
    // retained by the name observer, including successful fallback diagnostics.
    NetError::from(error.kind()).with_stage(ErrorStage::NetworkMonitor)
}

/// An explicitly enabled, independently cancellable network-name observer.
///
/// At most 32 observers may be active in one engine. Sampling uses a bounded,
/// serial native worker and does not update ordinary network snapshots/events.
/// This handle is intentionally not clonable. `recv` coalesces intermediate
/// observations: after its initial Pending value it returns the latest version.
///
/// Dropping the handle stops admission and clears retained names immediately;
/// use `close().await` to also wait for this observer's in-flight native query to
/// retire. The SDK cannot revoke values already returned to the application.
pub struct NetworkNameObserver {
    pub(crate) inner: Arc<NameConsumer>,
    pub(crate) changes: tokio::sync::watch::Receiver<()>,
    pub(crate) initial: bool,
    pub(crate) seen: u64,
    pub(crate) terminal: bool,
}

impl fmt::Debug for NetworkNameObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NetworkNameObserver")
            .finish_non_exhaustive()
    }
}

impl NetworkNameObserver {
    /// Read this observer's latest value without querying the operating system.
    pub fn snapshot(&self) -> Result<NetworkNameSnapshot, NetError> {
        self.inner.snapshot().map(|(_, snapshot)| snapshot)
    }

    /// Receive the latest observation. Closed is delivered once, followed by None.
    /// The initial Pending value is discarded if the observer closes first.
    pub async fn recv(&mut self) -> Result<Option<NetworkNameSnapshot>, NetError> {
        if self.terminal {
            return Ok(None);
        }
        loop {
            self.changes.borrow_and_update();
            if let Some((revision, snapshot)) = self.inner.next_snapshot(self.seen, self.initial) {
                self.initial = false;
                self.terminal = snapshot.state() == NetworkNameState::Closed;
                self.seen = revision;
                return Ok(Some(snapshot));
            }
            self.changes
                .changed()
                .await
                .map_err(|_| NetError::from(ErrorKind::Closed))?;
        }
    }

    /// Stop collection, clear retained values, and wait for native work to retire.
    /// Idempotent. Cancellation of this wait does not undo the requested close.
    pub async fn close(&mut self) -> Result<(), NetError> {
        self.inner.request_close();
        self.inner.wait_cleanup().await
    }
}

impl Drop for NetworkNameObserver {
    fn drop(&mut self) {
        self.inner.request_close();
    }
}

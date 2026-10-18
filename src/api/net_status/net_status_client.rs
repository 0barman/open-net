use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::net_status::NetworkSnapshot;
use crate::net_status::NetworkStatusContext;
use crate::net_status::{
    NetworkEvent, NetworkEventOptions, NetworkObservation, NetworkRefreshResult,
};
use crate::subscription::{CallbackContext, EventReceiver, StateReceiver, Subscription};
use crate::{LogType, Result};

use crate::module::net_status::inner::facade::NetworkObservationOwner;

/// An independent view of the engine's shared network monitor.
/// Clones share this view's lifecycle; other named views remain independent.
#[derive(Clone)]
pub struct NetStatusClient {
    inner: Arc<NetworkObservationOwner>,
}

impl NetStatusClient {
    pub(crate) fn new(context: NetworkStatusContext) -> Result<Self> {
        Ok(Self {
            inner: NetworkObservationOwner::new(context)?,
        })
    }

    /// Start monitoring and await the first coherent observation.
    /// Repeated calls share the active monitor and its initialization result.
    pub async fn start(&self) -> Result<NetworkSnapshot> {
        crate::log_t!(LogType::Engine; "net_status.start");
        self.inner.start().await
    }

    /// Stop monitoring while keeping subscriptions alive for a later restart.
    pub async fn stop(&self) -> Result<()> {
        crate::log_t!(LogType::Engine; "net_status.stop");
        self.inner.request_stop()?;
        self.inner.wait_cleanup().await
    }

    /// Permanently close monitoring and publish its final Closed snapshot.
    pub async fn shutdown(&self) -> Result<()> {
        crate::log_t!(LogType::Engine; "net_status.shutdown");
        self.inner.request_close();
        self.inner.wait_cleanup().await
    }

    /// Read the latest coherent snapshot, including an inactive or final state.
    pub fn snapshot(&self) -> Result<NetworkSnapshot> {
        self.inner.snapshot()
    }

    /// Collect a fresh LAN IPv4 inventory using [`crate::network::lan_ipv4_snapshot`].
    ///
    /// Synchronous OS query: it blocks this caller and needs no caller runtime.
    /// Stopped, Starting, Running and ordinary monitoring Failed states permit
    /// the query; it neither starts monitoring nor requires Internet access.
    /// It never changes the cached network snapshot, revisions or event stream.
    /// Existing network events do not guarantee notification of LAN-only changes.
    ///
    /// A closed client (including its clones), destroyed named client, or closing
    /// engine rejects new queries with Closed. A query admitted before closing
    /// may finish normally; shutdown does not wait for this caller's read-only
    /// query, which owns no monitored resources. Prefer a blocking worker when
    /// calling from async/UI code.
    pub fn query_lan_ipv4(&self) -> Result<crate::network::LanIpv4Snapshot> {
        self.inner.admit_lan_query()?;
        crate::network::lan_ipv4_snapshot()
    }

    /// Subscribe to the current snapshot and later changes across stop/restart.
    pub fn subscribe(&self) -> Result<StateReceiver<NetworkSnapshot>> {
        crate::log_t!(LogType::Engine; "net_status.subscribe");
        self.inner.subscribe_terminal()
    }

    /// Observe the same snapshots as an asynchronous subscription on the shared
    /// callback pool. Keep the returned owner alive while observing changes.
    pub fn on_change<F>(&self, callback: F) -> Result<Subscription>
    where
        F: Fn(CallbackContext, Result<NetworkSnapshot>) + Send + Sync + 'static,
    {
        self.subscribe()?.into_callback(callback)
    }

    /// Subscribe atomically to an initial baseline and ordered committed events.
    /// Each subscription has its own bounded queue. Overflow drains the queued
    /// prefix, reports a failure, and ends that subscription; subscribe again to
    /// establish a fresh baseline. Initial and lifecycle entries use capacity.
    pub fn subscribe_events(
        &self,
        options: NetworkEventOptions,
    ) -> Result<EventReceiver<NetworkEvent>> {
        crate::log_t!(LogType::Engine; "net_status.subscribe_events",
            "capacity|max_bytes", options.capacity, options.max_bytes);
        self.inner.subscribe_events(options)
    }

    /// Observe the same ordered events with a serial callback on the shared
    /// callback pool. Retain the returned Subscription while observing events.
    pub fn on_event<F>(&self, options: NetworkEventOptions, callback: F) -> Result<Subscription>
    where
        F: Fn(CallbackContext, std::result::Result<NetworkEvent, crate::error::ReceiveError>)
            + Send
            + Sync
            + 'static,
    {
        self.subscribe_events(options)?.into_callback(callback)
    }

    /// Observe ordered events and automatically reestablish an atomic baseline
    /// after a bounded queue overflows. The same logical subscription and
    /// callback context ID survive every recovery and monitor stop/restart.
    ///
    /// The retained prefix is delivered first, then `Reconciled`, then events
    /// after that baseline. Callbacks are serial across recovery. Only queue
    /// overflow is recovered; other errors terminate the observer, and callback
    /// panic stops delivery. Dispatch failures cannot always invoke the callback
    /// again, but remain observable through `Subscription::close` and logs.
    ///
    /// Unsubscribe cancels future callback admission and recovery. On success,
    /// [`Subscription::close`] additionally confirms that the driver and admitted
    /// callbacks have retired. Polling close inside a subscription callback
    /// follows its shared context rule: it unsubscribes and returns `InvalidInput`
    /// without confirming retirement; use [`CallbackContext::unsubscribe`] there.
    pub fn on_recoverable_event<F>(
        &self,
        options: NetworkEventOptions,
        callback: F,
    ) -> Result<Subscription>
    where
        F: Fn(CallbackContext, Result<NetworkObservation>) + Send + Sync + 'static,
    {
        crate::log_t!(LogType::Engine; "net_status.on_recoverable_event",
            "capacity|max_bytes|callback", options.capacity, options.max_bytes, "provided");
        self.inner.on_recoverable_event(options, callback)
    }

    /// Query fresh Windows reachability and network name and commit the result.
    /// Requires this view to be Running. The positive timeout covers admission,
    /// sampling and commit. Dropping the future invalidates uncommitted results.
    /// A slow name query preserves any later reachability observation while
    /// committing its fresh name; a newer name sample supersedes it with Cancelled.
    /// After input and lifecycle validation, other platforms return Unsupported;
    /// snapshot() remains a cheap cache read. Expired results cannot commit, but
    /// scheduling and lock contention can delay return beyond the timeout.
    /// Success does not wait for subscription callbacks to finish.
    pub async fn refresh(&self, timeout: Duration) -> Result<NetworkRefreshResult> {
        crate::log_t!(LogType::Engine; "net_status.refresh", "timeout", timeout);
        crate::module::net_status::inner::refresh::refresh(self.inner.clone(), timeout).await
    }

    /// Query and commit fresh Windows reachability without querying a name.
    /// Uses a separate bounded worker so a slow or failed network-name query
    /// cannot block this query or background reachability monitoring. The name
    /// and IP stack retain their latest cached values with explicit provenance.
    /// Requires Running and a positive timeout covering admission, sampling and
    /// commit. Dropping this future invalidates uncommitted results. A newer
    /// committed reachability sample supersedes an older query with Cancelled.
    /// After input and lifecycle validation, other platforms return Unsupported.
    /// As with [`Self::refresh`], the timeout is a result-validity budget, not a
    /// hard real-time return deadline; success does not await observer callbacks.
    pub async fn refresh_reachability(&self, timeout: Duration) -> Result<NetworkRefreshResult> {
        crate::log_t!(LogType::Engine; "net_status.refresh_reachability", "timeout", timeout);
        crate::module::net_status::inner::refresh::refresh_reachability(self.inner.clone(), timeout)
            .await
    }

    /// Synchronously query and commit fresh Windows reachability and network name.
    ///
    /// This is the blocking counterpart of [`Self::refresh`], with the same
    /// sampling, field provenance and lifecycle checks. A positive timeout is
    /// required; after input and lifecycle validation, other platforms return
    /// `Unsupported`. Errors never silently fall back to [`Self::snapshot`].
    /// Expired samples cannot commit, but scheduling and lock contention can
    /// delay the return beyond the budget; it is not a hard real-time bound.
    /// Success means the sample was committed, not that observers have finished.
    ///
    /// Blocks the calling thread. Ordinary threads, external Tokio runtimes,
    /// nested futures executors and the SDK's separate subscription callback
    /// pool are supported; the SDK drives its own timers and sampling workers.
    /// Prefer [`Self::refresh`] in async code and avoid blocking a UI thread.
    /// SDK management-runtime threads (including its blocking pool) and sampling
    /// threads are rejected before any query with `InvalidInput`, stage
    /// `NetworkMonitor`, and field `network_status.refresh_blocking.context`.
    /// Entering an SDK runtime context alone does not trigger this rejection.
    pub fn refresh_blocking(&self, timeout: Duration) -> Result<NetworkRefreshResult> {
        crate::log_t!(LogType::Engine; "net_status.refresh_blocking", "timeout", timeout);
        crate::module::net_status::inner::refresh::refresh_blocking(self.inner.clone(), timeout)
    }

    /// Synchronously query and commit fresh Windows reachability only.
    ///
    /// The blocking counterpart of [`Self::refresh_reachability`], using its
    /// independent sampling lane. Network name and IP-stack values remain
    /// cached, with their provenance preserved in the result. Requires Running
    /// and a positive timeout; other platforms return `Unsupported` after these
    /// checks. Never substitutes a cached snapshot for a failed fresh query.
    ///
    /// The blocking, timeout and SDK-thread restrictions of
    /// [`Self::refresh_blocking`] apply. Async callers should normally use
    /// [`Self::refresh_reachability`] instead.
    ///
    /// ```no_run
    /// use open_net::{NetStatusClient, NetworkRefreshResult, Result};
    /// use std::time::Duration;
    ///
    /// // Call from a synchronous integration after start().await has succeeded.
    /// fn query_now(client: &NetStatusClient) -> Result<NetworkRefreshResult> {
    ///     client.refresh_reachability_blocking(Duration::from_secs(3))
    /// }
    /// ```
    pub fn refresh_reachability_blocking(&self, timeout: Duration) -> Result<NetworkRefreshResult> {
        crate::log_t!(LogType::Engine; "net_status.refresh_reachability_blocking", "timeout", timeout);
        crate::module::net_status::inner::refresh::refresh_reachability_blocking(
            self.inner.clone(),
            timeout,
        )
    }

    pub(crate) fn request_destroy(&self) {
        self.inner.request_close();
    }

    pub(crate) async fn destroy(&self) -> Result<()> {
        self.inner.request_close();
        self.inner.wait_cleanup().await
    }
}

impl fmt::Debug for NetStatusClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NetStatusClient")
            .finish_non_exhaustive()
    }
}

//! Passive, opt-in diagnostics for HTTP and WebSocket SDK operations.
//!
//! Enable the `quality-doctor` Cargo feature, configure an owner with
//! [`QualityDoctorConfig::basic`] or [`QualityDoctorConfig::detailed`], and call that
//! owner's `quality_doctor()` accessor **before** the work you want to observe.
//! Configuration alone does not start collection. The first accessor call creates
//! one bounded collector shared by that owner and its clients; earlier work is not
//! reconstructed. [`QualityMode::Off`] is the default and provides no doctor.
//!
//! # Reading observations
//!
//! [`QualityDoctor::snapshot`] and [`QualityDoctor::lookup`], together with
//! subscription reads, drain pending facts and maintain retention on the reader's
//! thread. There is no collector task. Network adapters submit bounded facts without
//! waiting for diagnostic queue space or calling user code. Diagnostic overload
//! loses observations rather than delaying a network operation. Collection still
//! has measurement and synchronization costs.
//!
//! Reports distinguish the selected SDK result from the end of observation:
//! [`QualityReport::result_duration`] ends at result selection, whereas
//! [`QualityReport::duration`] can include callback delivery, streaming-body
//! consumption, or cleanup. For HTTP streams, result selection itself occurs at body
//! termination, so result duration also includes caller-driven consumption.
//! None of these intervals is a packet capture or pure wire-latency measurement. See [`QualityCapabilities`] before interpreting a
//! missing phase, and [`QualitySummary`] before treating statistics as traffic-wide.
//!
//! # Time, identity, and missing data
//!
//! Every offset in this module uses the same collector-local monotonic clock:
//! `tokio::time::Instant` elapsed since collector activation. A phase's offsets are
//! **not** relative to its report's start. Offsets are comparable only within the
//! same [`QualityReportRef::scope`]; they are not Unix timestamps and cannot identify
//! a wall-clock date. Durations use Rust's [`Duration`] with seconds and nanoseconds.
//!
//! Identifiers are process-local correlation values, not URLs, user request IDs,
//! credentials, or remote identifiers. A report reference does not pin its report
//! in memory. Capacity limits, expiration, and diagnostic loss can remove evidence.
//! `None`, an empty vector, or a missing lookup must not be interpreted as zero
//! latency or successful completion. Inspect observation/detail state, completeness,
//! loss counters, and capabilities together.
//!
//! # JSON export
//!
//! Reports and snapshots implement [`Serialize`] and provide `to_json()` helpers.
//! Serialization uses stored field names, which can differ from getters: for example
//! `started`, `result_at`, `finished`, `bytes`, and `phase_overflow` in reports;
//! `start` and `end` in phases; and `captured_at` in snapshots. `Duration` serializes
//! as an object with `secs` and `nanos`, optional values as `null`, and annotated
//! enums as snake-case strings. The schema discriminator describes the exported
//! shape, not the crate release. Error messages, request/response headers, URLs,
//! and payload contents are not retained by these diagnostic types.

use std::{fmt, sync::Arc, time::Duration};

use serde::Serialize;

pub(crate) use crate::module::quality::{Collector, SubscriptionInner};
/// How much diagnostic detail an activated owner collects.
///
/// The mode is fixed by the owner's configuration. Basic and detailed modes both attempt
/// bounded report summaries and coarse phases; detailed mode additionally samples interval
/// lifecycle and detail-only phases. Neither mode starts collection until the owner's
/// `quality_doctor()` accessor is called.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityMode {
    /// Disable diagnostics; the default owner does not allocate diagnostic collector state and
    /// its accessor returns `None`.
    #[default]
    Off,
    /// Retain bounded summaries, coarse completed intervals, and milestones after activation.
    /// Detailed interval lifecycle and detail-only phases are omitted.
    Basic,
    /// Attempt Basic observations plus sampled interval starts/lifecycle and detail-only
    /// phases. Sampling is controlled separately for connections and other operations by
    /// [`QualitySampling`].
    Detailed,
}

/// Transport family attached by the HTTP or WebSocket adapter when it begins an observation.
/// Selecting a protocol does not enable its Cargo feature.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityProtocol {
    /// HTTP request, stream, or connector observations emitted by the `http-client` adapter.
    Http,
    /// WebSocket message, request, receive, heartbeat, or connection observations emitted by
    /// the `ws-client` adapter.
    WebSocket,
}

/// Population of SDK work represented by one report.
///
/// Kinds identify observation boundaries rather than business request categories. Filter by
/// both protocol and kind before comparing durations; connection attempts, messages, requests,
/// received payloads, and heartbeats measure different work. Some kinds are reserved and have
/// no current producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityOperationKind {
    /// A buffered HTTP SDK request, including observed admission, retries/hops, body
    /// collection, and delivery/cleanup. Its selected result and final observation boundary can
    /// differ.
    HttpRequest,
    /// An HTTP streaming request. The API can return response headers before body consumption,
    /// but the diagnostic result is recorded when the body completes, fails, or is dropped.
    /// Both result and observation duration can therefore include caller-driven body time.
    HttpStream,
    /// A WebSocket send operation without response correlation. Success reflects the SDK
    /// write/result boundary, not peer acknowledgement or application processing.
    WsMessage,
    /// A WebSocket send operation with correlated response handling. Its observation can
    /// include write, response matching, resource release, and result delivery.
    WsRequest,
    /// A higher-level WebSocket request operation spanning its retry/build/resolve lifecycle.
    /// It may relate to individual child request reports through parent references.
    WsRequestOperation,
    /// A WebSocket heartbeat probe, from probe observation through matching Pong, timeout,
    /// abandonment, or unknown termination. This is an SDK heartbeat lifecycle, not pure
    /// network RTT.
    WsHeartbeat,
    /// A complete received WebSocket text/binary business message as it enters SDK routing.
    /// Measures receive-side dispatch/routing work, not the preceding socket-read wait. Ping,
    /// Pong, and Close control frames do not create this report population.
    WsReceive,
    /// An HTTP connector service invocation or one WebSocket connection-attempt observation.
    /// HTTP observations cannot identify the pool-selected connection; WebSocket observations
    /// may include backoff/network/admission waits before handshake.
    Connection,
    /// Reserved generic heartbeat population; current WebSocket heartbeat instrumentation uses
    /// [`Self::WsHeartbeat`].
    Heartbeat,
}
/// An SDK interval or milestone recorded by transport instrumentation.
///
/// A name identifies the instrumented boundary, not packet delivery or a guarantee that the
/// underlying work succeeded. [`QualityPhase::end_offset`] distinguishes completed intervals
/// from milestones; detailed [`QualityPhaseActivity`] entries expose interval lifecycle. Phases
/// may overlap, repeat across retries or redirect hops, or be absent due to configuration,
/// sampling, data loss, or unsupported instrumentation. Their durations must not be added to
/// infer a total.
///
/// [`QualityDoctor::capabilities`] describes which protocol/configuration combinations can
/// produce each phase. `TcpCandidate`, `RequestGetters`, `ResponseMatched`, `ManualDispatch`,
/// and `ManualResolve` require detail selection; other instrumented phases can be retained in
/// Basic mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum PhaseKind {
    /// Interval covering SDK submission/admission work. May include nested permit or queue
    /// waits depending on the operation path.
    Admission,
    /// HTTP interval waiting for the SDK in-flight admission permit. It does not measure
    /// connection-pool acquisition.
    PermitWait,
    /// HTTP interval waiting for capacity in the request-submission or streaming-supervision
    /// lane. It is not the full time between publication and worker dequeue.
    QueueWait,
    /// Milestone at the HTTP/WS queue publication boundary. HTTP records it immediately before
    /// sending into a reserved slot; WS records it after dispatch publication commits. It does
    /// not prove a worker has dequeued or executed the work.
    Published,
    /// Milestone when the HTTP worker takes a queued job or a WebSocket operation attaches to
    /// its dispatch connection.
    Dequeued,
    /// HTTP milestone at entry to request processing by the worker.
    WorkerStarted,
    /// HTTP interval covering per-attempt request preparation before execution. Nested request
    /// getter/build phases may overlap it.
    Preparation,
    /// WebSocket interval awaiting handshake/provider output. It includes the provider future's
    /// own scheduling or application work.
    Provider,
    /// Detailed HTTP interval reading application request metadata/getters. The request path
    /// can record more than one such interval.
    RequestGetters,
    /// Interval building an HTTP request, WebSocket handshake request, or retry request
    /// payload, according to the emitting adapter.
    RequestBuild,
    /// WebSocket milestone after the outgoing payload has been prepared and its application
    /// byte length counted.
    Prepared,
    /// HTTP interval around the dependency's execute future until response headers or an
    /// execution error. Includes opaque pool/connection/server work; it is not a separated
    /// DNS/TCP/TLS measurement.
    ExecuteToHeaders,
    /// HTTP milestone after a successful response head, including intermediate redirects. A
    /// separate HTTP-version fact records that response's version.
    ResponseHeaders,
    /// HTTP milestone on the first nonempty response-body chunk observed by the adapter. Empty
    /// bodies produce no such milestone; stream polling is caller-driven.
    FirstBodyChunk,
    /// HTTP body-reading interval. Buffered requests measure SDK collection; streams can
    /// include caller pauses until exhaustion or drop.
    ResponseBody,
    /// HTTP milestone when the operation's existing execution/middleware report is frozen. It
    /// does not mean the QualityDoctor report has finished.
    ReportFrozen,
    /// HTTP milestone at handoff toward response delivery. It is a queue boundary marker, not
    /// by itself a measured queue-wait interval.
    ResponseQueue,
    /// HTTP milestone immediately before awaiting enqueue of a completion job to the callback
    /// lane. The enqueue can still wait or fail; the marker is not evidence that a callback has
    /// run.
    CallbackQueue,
    /// HTTP interval surrounding callback delivery, including application callback execution.
    /// It is after result selection and cannot change the retained first result.
    Callback,
    /// Milestone when the instrumented SDK operation releases its tracked resources. The exact
    /// resource boundary depends on the adapter/path.
    ResourcesReleased,
    /// HTTP milestone after the completion value is cached and before waiters are notified. It
    /// does not prove that a consumer has resumed or processed the result.
    CompletionPublished,
    /// HTTP/WS milestone after the instrumented result/callback delivery boundary. Outstanding
    /// observed scopes can postpone report finish.
    DeliveryFinished,
    /// WebSocket milestone immediately after the SDK selects its operation result. The report's
    /// `result_at` comes from the separate result event.
    ResultSelected,
    /// HTTP interval around one connector service invocation until its future finishes or is
    /// dropped. Competing candidates and pooling prevent attribution to the connection
    /// ultimately used.
    Connector,
    /// HTTP milestone when the wrapper first polls a connector future. A connector invoked but
    /// dropped without polling has no such milestone.
    ConnectorPolled,
    /// WebSocket connection interval waiting for the SDK's network-availability gate. It is not
    /// socket read/write latency.
    NetworkWait,
    /// HTTP retry or WebSocket reconnect/request-retry delay interval. May include scheduler
    /// delay and can end early on cancellation or failure.
    Backoff,
    /// WebSocket connection interval reserving/admitting an attempt under the session budget,
    /// including surrounding gate/acknowledgement work.
    ConnectionAdmission,
    /// WebSocket aggregate system-resolution API interval. Resolver cache/search/retry behavior
    /// is opaque; an IP-literal target can also produce this interval without a DNS packet.
    /// HTTP dependency DNS timing is unsupported.
    Dns,
    /// WebSocket aggregate TCP-connect interval, which can contain several individual candidate
    /// attempts. System-resolution API time is separately observed by Dns; neither interval
    /// measures packets directly.
    TcpConnect,
    /// Detailed WebSocket interval for one individual TCP address candidate. An ended candidate
    /// interval alone does not imply that candidate won or connected successfully.
    TcpCandidate,
    /// WebSocket TLS handshake interval to a configured TLS proxy, distinct from target-server
    /// TLS.
    ProxyTls,
    /// WebSocket proxy CONNECT tunnel negotiation interval when the configured proxy path uses
    /// one.
    ProxyConnect,
    /// WebSocket TLS handshake interval to the target server. Plaintext paths omit it; HTTP TLS
    /// is opaque to this observer.
    Tls,
    /// WebSocket HTTP-upgrade handshake interval after the transport is prepared.
    WebSocketUpgrade,
    /// WebSocket interval spanning sink readiness, cancellation/budget checks, and the
    /// synchronous start_send attempt. Includes local backpressure and scheduling; guard drop
    /// can end it on failure. StartSend is separately marked after successful acceptance.
    SinkReadiness,
    /// WebSocket milestone after the sink accepts `start_send`. It is not proof that bytes
    /// reached the peer.
    StartSend,
    /// WebSocket interval awaiting sink flush. Successful flush is an SDK write boundary, not
    /// peer acknowledgement.
    Flush,
    /// WebSocket milestone when the operation or heartbeat records its write boundary.
    Written,
    /// Reserved response-wait category; current adapters do not emit a dedicated interval for
    /// this phase. Consult result/response milestones without inferring an absent interval.
    ResponseWait,
    /// Detailed WebSocket milestone when a pending correlated response is matched to its
    /// request.
    ResponseMatched,
    /// Detailed WebSocket receive milestone after the manual routing/publication path returns
    /// successfully. It does not prove any subscriber accepted or processed the message; the
    /// path can succeed with no registered receiver.
    ManualDispatch,
    /// Detailed WebSocket milestone when the manual-resolution path retires a pending entry. It
    /// can be recorded even if another result already won and resolution returns
    /// StaleOrFinished; it does not guarantee a successful response result.
    ManualResolve,
    /// WebSocket milestone when operation state switches back to queued before a re-enqueue
    /// attempt. The subsequent queue insertion can still fail; the marker does not prove a
    /// second dispatch occurred.
    Requeue,
    /// WebSocket milestone when a matching heartbeat Pong is accepted. The enclosing heartbeat
    /// report supplies probe lifecycle timestamps.
    Heartbeat,
    /// WebSocket milestone after a complete text/binary business message enters SDK receive
    /// handling and its application byte length is recorded. It is not a timestamp for packet
    /// arrival or control frames.
    PayloadReceived,
    /// WebSocket receive interval around synchronous response classification/routing. It does
    /// not encompass later asynchronous application consumption.
    ResponseRouting,
}

/// First retained SDK result classification for an observation.
///
/// This is independent of [`QualityObservationState`]. A successful result may be selected
/// before cleanup finishes; an expired observation may have no known result. The collector
/// keeps the first retained result and does not replace it with later callback or cleanup
/// outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityOutcome {
    /// The SDK observed successful completion at its selected result boundary. HTTP status can
    /// still be non-2xx; a successful WS write does not establish remote business success.
    Success,
    /// A non-cancellation, non-timeout error was selected, or a connector future returned an
    /// error without a sanitized NetError classification.
    Failed,
    /// A retained NetError was classified as `ErrorKind::Cancelled`; this is explicit SDK
    /// evidence, not an inference from future drop.
    Cancelled,
    /// A retained NetError was classified as `TimedOut` or `DeadlineExceeded`, or a heartbeat
    /// explicitly timed out.
    Timeout,
    /// An observation was explicitly abandoned, for example an HTTP connector future dropped
    /// before returning. It does not establish cancellation or failure on the peer.
    Abandoned,
    /// The adapter could not establish a terminal business/transport result, for example a
    /// dropped observation scope. Completed reports with this outcome are marked possibly
    /// incomplete.
    Unknown,
}
impl QualityOutcome {
    /// Classifies an SDK error using only [`crate::error::NetError::kind`].
    ///
    /// `Cancelled` maps to [`Self::Cancelled`]; `TimedOut` and `DeadlineExceeded` map to
    /// [`Self::Timeout`]; all other error kinds map to [`Self::Failed`]. This helper does not
    /// inspect source text, retain the error, or infer transport delivery.
    pub fn from_error(error: &crate::api::error::NetError) -> Self {
        use crate::api::error::ErrorKind;
        match error.kind() {
            ErrorKind::Cancelled => Self::Cancelled,
            ErrorKind::TimedOut | ErrorKind::DeadlineExceeded => Self::Timeout,
            _ => Self::Failed,
        }
    }
}

/// Lifecycle of diagnostic collection, independent of transport connectivity.
///
/// A live doctor reports Running, Closed, or Failed. Dormant names the pre-activation state but
/// is not returned by an already-created collector. Closed/failed collectors may still expose
/// retained data through queries.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityDoctorState {
    /// Reserved representation of configured but not activated collection. No public doctor
    /// exists in this state.
    Dormant,
    /// The collector accepts diagnostic facts subject to configured bounds. This says nothing
    /// about network connectivity.
    Running,
    /// Collection became unavailable, for example because an internal mutex was poisoned or
    /// identity allocation failed. Failure takes precedence over Closed in state reporting.
    Failed,
    /// The owning SDK lifetime closed collection. Already-retained data may remain readable
    /// until lazy eviction; no new observations are accepted.
    Closed,
}
/// Protocol switches stored in a doctor's configuration.
///
/// `Default` includes HTTP if `http-client` was compiled and WebSocket if `ws-client` was
/// compiled. The switches do not enable Cargo features: selecting an unavailable protocol fails
/// [`QualityDoctorConfig::validate`]. Selecting neither protocol is valid. JSON uses the
/// boolean fields `http` and `ws`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct QualityProtocols {
    pub(crate) http: bool,
    pub(crate) ws: bool,
}
impl QualityProtocols {
    /// Creates explicit protocol switches. `http` selects HTTP observations and `websocket`
    /// selects WebSocket observations. The values are stored unchanged; configuration
    /// validation rejects any selected protocol whose Cargo feature is absent.
    pub const fn new(http: bool, websocket: bool) -> Self {
        Self {
            http,
            ws: websocket,
        }
    }
    /// Selects HTTP only. Requires `http-client` to pass owner configuration validation; does
    /// not enable that feature.
    pub const fn http() -> Self {
        Self::new(true, false)
    }
    /// Selects WebSocket only. Requires `ws-client` to pass owner configuration validation;
    /// does not enable that feature.
    pub const fn websocket() -> Self {
        Self::new(false, true)
    }
    /// Selects neither protocol. This is valid and produces no network observations even if a
    /// collector is activated.
    pub const fn none() -> Self {
        Self::new(false, false)
    }
    /// Returns the stored switch for `protocol`. This reads configuration only and does not
    /// test whether any report exists or whether the Cargo feature was compiled.
    pub const fn includes(self, protocol: QualityProtocol) -> bool {
        match protocol {
            QualityProtocol::Http => self.http,
            QualityProtocol::WebSocket => self.ws,
        }
    }
}
impl Default for QualityProtocols {
    fn default() -> Self {
        Self::new(cfg!(feature = "http-client"), cfg!(feature = "ws-client"))
    }
}
/// Finite memory and query budgets, plus lazy retention durations.
///
/// Use builders to change defaults, then [`QualityDoctorConfig::validate`] to check them;
/// individual setters do not validate or clamp values. Capacities count diagnostic events,
/// records, phases, subscribers, or results as documented, not bytes. Network work continues if
/// diagnostic capacity is exhausted.
///
/// Defaults are 1,024 ingress events, 256 active records/probes, 256 historical records, 64
/// logical phases per report, 4 subscriptions, 128 slots per subscription, 128 query results,
/// 300 seconds retention, and a 60 second query window. In addition to each field's range,
/// combined report and subscription phase budgets are validated; see
/// [`QualityDoctorConfig::validate`].
#[derive(Clone, Debug)]
pub struct QualityLimits {
    pub(crate) ingress: usize,
    pub(crate) active: usize,
    pub(crate) history: usize,
    pub(crate) phases: usize,
    pub(crate) subscriptions: usize,
    pub(crate) subscription_capacity: usize,
    pub(crate) query_results: usize,
    pub(crate) ttl: Duration,
    pub(crate) window: Duration,
}
impl Default for QualityLimits {
    fn default() -> Self {
        Self {
            ingress: 1024,
            active: 256,
            history: 256,
            phases: 64,
            subscriptions: 4,
            subscription_capacity: 128,
            query_results: 128,
            ttl: Duration::from_secs(300),
            window: Duration::from_secs(60),
        }
    }
}
impl QualityLimits {
    /// Sets this bound, replacing the default of 1,024. Valid range: `1..=65_536`; validation
    /// is deferred to [`QualityDoctorConfig::validate`].
    ///
    /// Number of pending producer facts in the shared ingress queue. Full or contended ingress
    /// drops diagnostic facts without making network producers wait. The combined phase budgets
    /// documented on configuration validation also apply.
    pub fn with_ingress_capacity(mut self, value: usize) -> Self {
        self.ingress = value;
        self
    }
    /// Sets this bound, replacing the default of 256. Valid range: `1..=4096`; validation is
    /// deferred to [`QualityDoctorConfig::validate`].
    ///
    /// Maximum simultaneous diagnostic probe reservations and retained InProgress records. New
    /// observations can be skipped when reservations are exhausted; stale retained active
    /// records can be evicted when storage needs space. The combined phase budgets documented
    /// on configuration validation also apply.
    pub fn with_active_capacity(mut self, value: usize) -> Self {
        self.active = value;
        self
    }
    /// Sets this bound, replacing the default of 256. Valid range: `1..=4096`; validation is
    /// deferred to [`QualityDoctorConfig::validate`].
    ///
    /// Maximum retained terminal reports (Completed, Expired, or OwnerClosed). Oldest terminal
    /// records by insertion order are evicted when the bound is exceeded. The same capacity
    /// bounds eviction tombstones. The combined phase budgets documented on configuration
    /// validation also apply.
    pub fn with_history_capacity(mut self, value: usize) -> Self {
        self.history = value;
        self
    }
    /// Sets this bound, replacing the default of 64. Valid range: `1..=512`; validation is
    /// deferred to [`QualityDoctorConfig::validate`].
    ///
    /// Maximum logical phase/milestone reservations per report. An interval start/end share one
    /// reservation; repeated phases and milestones consume separate reservations. Detailed
    /// reports can retain both a completed entry and a lifecycle entry per reservation.
    /// Exhaustion records phase overflow when later facts are retained. The combined phase
    /// budgets documented on configuration validation also apply.
    pub fn with_max_phases(mut self, value: usize) -> Self {
        self.phases = value;
        self
    }
    /// Sets this bound, replacing the default of 4. Valid range: `1..=64`; validation is
    /// deferred to [`QualityDoctorConfig::validate`].
    ///
    /// Maximum concurrent registered receivers per collector. Further registrations fail with
    /// QualityError::Capacity until a receiver is dropped or unsubscribed. The combined phase
    /// budgets documented on configuration validation also apply.
    pub fn with_max_subscriptions(mut self, value: usize) -> Self {
        self.subscriptions = value;
        self
    }
    /// Sets this bound, replacing the default of 128. Valid range: `1..=4096`; validation is
    /// deferred to [`QualityDoctorConfig::validate`].
    ///
    /// Upper bound on each receiver's requested update slots, not its default allocation. The
    /// subscription option defaults to 64 slots; if this upper bound is smaller, explicitly
    /// lower the option too. Full receivers lose updates independently. The combined phase
    /// budgets documented on configuration validation also apply.
    pub fn with_subscription_capacity(mut self, value: usize) -> Self {
        self.subscription_capacity = value;
        self
    }
    /// Sets this bound, replacing the default of 128. Valid range: `1..=8192`; validation is
    /// deferred to [`QualityDoctorConfig::validate`].
    ///
    /// Default and maximum result limit for queries. Report and finding lists are bounded
    /// independently; summary statistics include all matching retained records even when output
    /// lists are truncated. The combined phase budgets documented on configuration validation
    /// also apply.
    pub fn with_max_query_results(mut self, value: usize) -> Self {
        self.query_results = value;
        self
    }
    /// Sets the lazy observation-retention duration (default 300 seconds). Must be nonzero and
    /// at most 24 hours, and no shorter than the configured window. An active record expires
    /// when this age from started is reached; terminal records are evicted when this age from
    /// finished is reached. Maintenance runs on reads, so this is not a background expiration
    /// timer and does not cancel network work.
    pub fn with_retention(mut self, value: Duration) -> Self {
        self.ttl = value;
        self
    }
    /// Sets the default snapshot age window (default 60 seconds). Must be nonzero and no
    /// greater than retention. Query-specific windows may override it up to retention.
    /// Membership uses terminal finished time or active started time, not the age of each
    /// individual phase.
    pub fn with_window(mut self, value: Duration) -> Self {
        self.window = value;
        self
    }
    /// Returns the configured bound. Default: 1,024; valid range: `1..=65_536`.
    ///
    /// Number of pending producer facts in the shared ingress queue. Full or contended ingress
    /// drops diagnostic facts without making network producers wait. Values returned from an
    /// unvalidated builder can lie outside that range.
    pub fn ingress_capacity(&self) -> usize {
        self.ingress
    }
    /// Returns the configured bound. Default: 256; valid range: `1..=4096`.
    ///
    /// Maximum simultaneous diagnostic probe reservations and retained InProgress records. New
    /// observations can be skipped when reservations are exhausted; stale retained active
    /// records can be evicted when storage needs space. Values returned from an unvalidated
    /// builder can lie outside that range.
    pub fn active_capacity(&self) -> usize {
        self.active
    }
    /// Returns the configured bound. Default: 256; valid range: `1..=4096`.
    ///
    /// Maximum retained terminal reports (Completed, Expired, or OwnerClosed). Oldest terminal
    /// records by insertion order are evicted when the bound is exceeded. The same capacity
    /// bounds eviction tombstones. Values returned from an unvalidated builder can lie outside
    /// that range.
    pub fn history_capacity(&self) -> usize {
        self.history
    }
    /// Returns the configured bound. Default: 64; valid range: `1..=512`.
    ///
    /// Maximum logical phase/milestone reservations per report. An interval start/end share one
    /// reservation; repeated phases and milestones consume separate reservations. Detailed
    /// reports can retain both a completed entry and a lifecycle entry per reservation.
    /// Exhaustion records phase overflow when later facts are retained. Values returned from an
    /// unvalidated builder can lie outside that range.
    pub fn max_phases(&self) -> usize {
        self.phases
    }
    /// Returns the configured bound. Default: 4; valid range: `1..=64`.
    ///
    /// Maximum concurrent registered receivers per collector. Further registrations fail with
    /// QualityError::Capacity until a receiver is dropped or unsubscribed. Values returned from
    /// an unvalidated builder can lie outside that range.
    pub fn max_subscriptions(&self) -> usize {
        self.subscriptions
    }
    /// Returns the configured bound. Default: 128; valid range: `1..=4096`.
    ///
    /// Upper bound on each receiver's requested update slots, not its default allocation. The
    /// subscription option defaults to 64 slots; if this upper bound is smaller, explicitly
    /// lower the option too. Full receivers lose updates independently. Values returned from an
    /// unvalidated builder can lie outside that range.
    pub fn subscription_capacity(&self) -> usize {
        self.subscription_capacity
    }
    /// Returns the configured bound. Default: 128; valid range: `1..=8192`.
    ///
    /// Default and maximum result limit for queries. Report and finding lists are bounded
    /// independently; summary statistics include all matching retained records even when output
    /// lists are truncated. Values returned from an unvalidated builder can lie outside that
    /// range.
    pub fn max_query_results(&self) -> usize {
        self.query_results
    }
    /// Returns the configured lazy retention duration (default 300 seconds). Active age is
    /// measured from started; terminal retention is measured from finished. Expired active
    /// observations receive a maintenance timestamp and can remain as terminal history for
    /// another retention period, subject to capacity eviction.
    pub fn retention(&self) -> Duration {
        self.ttl
    }
    /// Returns the default snapshot lookback duration (default 60 seconds), used when a
    /// [`QualityQuery`] has no override. It does not reset lifetime counters, constrain
    /// individual phase offsets, or filter report subscription notifications.
    pub fn window(&self) -> Duration {
        self.window
    }
}
/// Stable per-report selection rates for extra diagnostic detail.
///
/// `Default` selects all eligible operations and connections (`1.0` for both rates). Selection
/// uses an internal deterministic hash of the collector scope and report ID; it is stable for
/// the report's entire lifetime and is not independently redrawn for each phase. Rates control
/// detailed lifecycle/detail-only phases in Detailed mode, not whether coarse reports are
/// attempted. A rate is a fraction, not a percentage or guarantee of an exact selected count.
#[derive(Clone, Copy, Debug)]
pub struct QualitySampling {
    pub(crate) operations: f64,
    pub(crate) connections: f64,
}
impl Default for QualitySampling {
    fn default() -> Self {
        Self {
            operations: 1.0,
            connections: 1.0,
        }
    }
}
impl QualitySampling {
    /// Sets operation and connection detail-selection fractions. Each value must be finite and
    /// in `0.0..=1.0`; validation is deferred to [`QualityDoctorConfig::validate`]. Zero
    /// selects none and one selects all eligible reports. Both defaults are one. Only reports
    /// with kind Connection use `connection_detail_rate`; every other kind uses
    /// `operation_detail_rate`.
    pub fn new(operation_detail_rate: f64, connection_detail_rate: f64) -> Self {
        Self {
            operations: operation_detail_rate,
            connections: connection_detail_rate,
        }
    }
    /// Returns the configured fraction used for all non-Connection report kinds in Detailed
    /// mode. Default: `1.0`; valid range: finite `0.0..=1.0`. This is not the fraction of
    /// reports retained: Basic summaries are still attempted independently of detail selection.
    pub fn operation_detail_rate(&self) -> f64 {
        self.operations
    }
    /// Returns the configured detail-selection fraction for Connection reports, including both
    /// HTTP connector and WebSocket connection observations. Default: `1.0`; valid range:
    /// finite `0.0..=1.0`. Basic mode does not enable detailed lifecycle even when this rate is
    /// one.
    pub fn connection_detail_rate(&self) -> f64 {
        self.connections
    }
}
/// Read-only classification thresholds used when snapshots are built.
///
/// Thresholds produce findings and control percentile eligibility; they never change retry,
/// timeout, admission, or connection behavior. Defaults are a 500 ms slow-phase threshold, a
/// 0.2 failure-ratio threshold, and 20 minimum samples. Setters defer validation to
/// [`QualityDoctorConfig::validate`].
#[derive(Clone, Debug)]
pub struct QualityPolicy {
    pub(crate) slow_phase: Duration,
    pub(crate) failure_ratio: f64,
    pub(crate) min_samples: usize,
}
impl Default for QualityPolicy {
    fn default() -> Self {
        Self {
            slow_phase: Duration::from_millis(500),
            failure_ratio: 0.2,
            min_samples: 20,
        }
    }
}
impl QualityPolicy {
    /// Sets the duration at or above which a retained completed interval qualifies as slow.
    /// Default: 500 ms. Must be nonzero; there is no additional upper bound in configuration
    /// validation. Milestones and unfinished intervals have no duration and cannot qualify.
    /// Does not change a transport timeout.
    pub fn with_slow_phase_threshold(mut self, value: Duration) -> Self {
        self.slow_phase = value;
        self
    }
    /// Sets the minimum observed failure fraction, inclusive. Default: `0.2`; valid values are
    /// finite `0.0..=1.0`. The numerator is Failed + Timeout and the denominator is Success +
    /// Failed + Timeout; cancellations, Unknown, Abandoned, and absent outcomes are excluded.
    /// Findings also require enough samples and a single protocol/kind population.
    pub fn with_failure_ratio_threshold(mut self, value: f64) -> Self {
        self.failure_ratio = value;
        self
    }
    /// Sets the minimum sample count for percentile publication and failure-ratio findings.
    /// Default: 20; valid range: `1..=8192`. Percentiles count complete successful latency
    /// samples; failure-ratio findings count classified Success/Failed/Timeout outcomes, so the
    /// two sample sets differ.
    pub fn with_min_samples(mut self, value: usize) -> Self {
        self.min_samples = value;
        self
    }
    /// Returns the configured inclusive slow-interval threshold (default 500 ms). It is
    /// compared with retained [`QualityPhase::duration`] values when building snapshots, not
    /// with wall-clock timestamps or active interval age.
    pub fn slow_phase_threshold(&self) -> Duration {
        self.slow_phase
    }
    /// Returns the configured inclusive failure fraction (default `0.2`). See
    /// [`Self::with_failure_ratio_threshold`] for the exact numerator/denominator and
    /// population restrictions.
    pub fn failure_ratio_threshold(&self) -> f64 {
        self.failure_ratio
    }
    /// Returns the configured minimum number of eligible samples (default 20). See
    /// [`Self::with_min_samples`] for the different eligibility rules used by percentiles and
    /// failure-ratio findings.
    pub fn min_samples(&self) -> usize {
        self.min_samples
    }
}

/// Owner configuration for passive quality diagnostics.
///
/// `Default` uses [`QualityMode::Off`], default limits/sampling/policy, and the protocols
/// compiled into the crate. Use [`Self::basic`] or [`Self::detailed`] to opt in, install the
/// configuration on the HTTP/WS owner, and call its `quality_doctor()` accessor to activate
/// collection. Builders only store values; [`Self::validate`] checks them without activating a
/// collector.
///
/// ```
/// use open_net::quality::{QualityDoctorConfig, QualityLimits, QualitySampling};
/// use std::time::Duration;
///
/// let config = QualityDoctorConfig::detailed()
///     .with_limits(QualityLimits::default().with_window(Duration::from_secs(30)))
///     .with_sampling(QualitySampling::new(0.25, 1.0));
/// config.validate()?;
/// # Ok::<(), open_net::quality::QualityError>(())
/// ```
#[derive(Clone, Debug, Default)]
pub struct QualityDoctorConfig {
    pub(crate) mode: QualityMode,
    pub(crate) limits: QualityLimits,
    pub(crate) sampling: QualitySampling,
    pub(crate) protocols: QualityProtocols,
    pub(crate) policy: QualityPolicy,
}
impl QualityDoctorConfig {
    /// Creates a Basic-mode configuration with all other fields at their defaults. The
    /// configuration alone does not activate a collector; install it on an owner and call the
    /// owner's `quality_doctor()` accessor.
    pub fn basic() -> Self {
        Self {
            mode: QualityMode::Basic,
            ..Self::default()
        }
    }
    /// Creates a Detailed-mode configuration with default bounds and full detail sampling for
    /// both operation and connection reports. Collection still requires calling the configured
    /// owner's `quality_doctor()` accessor.
    pub fn detailed() -> Self {
        Self {
            mode: QualityMode::Detailed,
            ..Self::default()
        }
    }
    /// Replaces all memory/query/retention limits with `value`. Values are stored without
    /// validation or clamping; call [`Self::validate`] before installing custom limits.
    pub fn with_limits(mut self, value: QualityLimits) -> Self {
        self.limits = value;
        self
    }
    /// Replaces the two detail-selection rates. This does not change the collection mode; Basic
    /// mode remains Basic even with nonzero sampling rates.
    pub fn with_sampling(mut self, value: QualitySampling) -> Self {
        self.sampling = value;
        self
    }
    /// Replaces the protocol switches. Selecting a protocol that was not compiled is rejected
    /// by [`Self::validate`]; selecting none is allowed.
    pub fn with_protocols(mut self, value: QualityProtocols) -> Self {
        self.protocols = value;
        self
    }
    /// Replaces snapshot classification thresholds and minimum sample count. This affects
    /// diagnostics only and does not alter transport policy.
    pub fn with_policy(mut self, value: QualityPolicy) -> Self {
        self.policy = value;
        self
    }
    /// Returns the configured collection mode. `Default` is Off; [`Self::basic`] and
    /// [`Self::detailed`] choose the enabled modes. It is configuration, not the collector's
    /// live lifecycle state.
    pub fn mode(&self) -> QualityMode {
        self.mode
    }
    /// Borrows the configured finite capacities and retention/window settings. Defaults and
    /// valid ranges are documented on [`QualityLimits`].
    pub fn limits(&self) -> &QualityLimits {
        &self.limits
    }
    /// Returns the configured operation/connection detail-selection fractions. The default
    /// selects every eligible report for detail when Detailed mode is used.
    pub fn sampling(&self) -> QualitySampling {
        self.sampling
    }
    /// Returns the configured protocol switches. Defaults follow compiled HTTP/WS features;
    /// this is independent of whether collection has been activated.
    pub fn protocols(&self) -> QualityProtocols {
        self.protocols
    }
    /// Borrows the configured advisory thresholds. Defaults are a 500 ms slow phase, 0.2
    /// failure ratio, and 20 minimum samples.
    pub fn policy(&self) -> &QualityPolicy {
        &self.policy
    }
    /// Checks all configuration fields without allocating or activating a collector.
    ///
    /// Capacities must be nonzero and at most: ingress 65,536; active/history 4,096 each;
    /// phases 512; subscriptions 64; subscription slots 4,096; query results 8,192.
    /// Additionally `(active + history) * phases` and `subscriptions * subscription_capacity *
    /// phases` must each be at most 262,144 logical phase slots. Detailed reports can retain
    /// both a completed-phase and lifecycle view per logical phase.
    ///
    /// Retention must be nonzero and at most 24 hours; window must be nonzero and no greater
    /// than retention. Sampling rates and failure ratio must be finite fractions in
    /// `0.0..=1.0`. The slow-phase threshold must be nonzero and minimum samples must be in
    /// `1..=8192`. Selected protocols must have been compiled. These checks also apply to an
    /// Off configuration.
    ///
    /// Returns [`QualityError::InvalidConfig`] with the first invalid field/category. Validity
    /// does not imply the protocol set is nonempty or any observations have been collected.
    pub fn validate(&self) -> Result<(), QualityError> {
        let limits = &self.limits;
        for (name, value, max) in [
            ("ingress_capacity", limits.ingress, 65_536),
            ("active_capacity", limits.active, 4096),
            ("history_capacity", limits.history, 4096),
            ("max_phases", limits.phases, 512),
            ("max_subscriptions", limits.subscriptions, 64),
            ("subscription_capacity", limits.subscription_capacity, 4096),
            ("max_query_results", limits.query_results, 8192),
        ] {
            if value == 0 || value > max {
                return Err(QualityError::InvalidConfig(name));
            }
        }
        // These are logical phase budgets. Detailed reports retain at most one
        // legacy phase and one lifecycle entry per logical phase (two bounded
        // views), including copies held in subscription slots.
        if limits
            .active
            .saturating_add(limits.history)
            .saturating_mul(limits.phases)
            > 262_144
        {
            return Err(QualityError::InvalidConfig("report_phase_budget"));
        }
        if limits
            .subscriptions
            .saturating_mul(limits.subscription_capacity)
            .saturating_mul(limits.phases)
            > 262_144
        {
            return Err(QualityError::InvalidConfig("subscription_phase_budget"));
        }
        if limits.ttl.is_zero()
            || limits.window.is_zero()
            || limits.ttl > Duration::from_secs(86_400)
            || limits.window > limits.ttl
        {
            return Err(QualityError::InvalidConfig("retention/window"));
        }
        for rate in [
            self.sampling.operations,
            self.sampling.connections,
            self.policy.failure_ratio,
        ] {
            if !rate.is_finite() || !(0.0..=1.0).contains(&rate) {
                return Err(QualityError::InvalidConfig("sampling/policy_ratio"));
            }
        }
        if self.policy.slow_phase.is_zero()
            || self.policy.min_samples == 0
            || self.policy.min_samples > 8192
        {
            return Err(QualityError::InvalidConfig("policy_threshold"));
        }
        if (self.protocols.http && !cfg!(feature = "http-client"))
            || (self.protocols.ws && !cfg!(feature = "ws-client"))
        {
            return Err(QualityError::InvalidConfig("protocol_not_compiled"));
        }
        Ok(())
    }
}

/// Diagnostic configuration, query, subscription, or export failure.
///
/// These errors describe the observer rather than the outcome of a network operation. Static
/// string payloads identify a validation category and contain no request data or arbitrary
/// dependency error message. The enum is non-exhaustive; downstream matches need a fallback.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QualityError {
    /// A configuration value or combined budget is invalid; the static string names the failed
    /// validation category.
    InvalidConfig(&'static str),
    /// A query window or limit is outside the owner's allowed range; the static string
    /// identifies `window` or `limit`.
    InvalidQuery(&'static str),
    /// No additional subscription can be registered because the subscriber count or subscriber
    /// identity space is exhausted.
    Capacity,
    /// A new subscription was requested after the diagnostic owner closed. Retained-data
    /// queries can still be possible.
    OwnerClosed,
    /// Diagnostic storage cannot be accessed or a subscription was requested from a failed
    /// collector.
    CollectorFailed,
    /// The JSON serializer could not encode the report or snapshot. No transport operation is
    /// retried or changed.
    ExportFailed,
    /// An empty asynchronous subscription receive could not create/poll a Tokio timer. Use a
    /// runtime with a time driver or the synchronous `try_recv` method.
    RuntimeUnavailable,
}
impl fmt::Display for QualityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(field) => write!(f, "invalid quality configuration: {field}"),
            Self::InvalidQuery(field) => write!(f, "invalid quality query: {field}"),
            Self::Capacity => f.write_str("quality diagnostic capacity reached"),
            Self::OwnerClosed => f.write_str("quality owner is closed"),
            Self::CollectorFailed => f.write_str("quality collector is unavailable"),
            Self::ExportFailed => f.write_str("quality JSON export failed"),
            Self::RuntimeUnavailable => {
                f.write_str("quality subscription requires an active Tokio time driver")
            }
        }
    }
}
impl std::error::Error for QualityError {}
/// Opaque, copyable identity of a diagnostic observation.
///
/// The collector allocates the scope and report ID; the adapter supplies the operation kind and
/// the sampler supplies detail selection. It contains no URL, request ID, payload, or
/// credentials. Use it with [`QualityDoctor::lookup`] on the same owner. Copying a reference
/// does not retain history, and numeric values have no meaning across process restarts.
/// Serialized fields are `scope`, `id`, `kind`, and `detailed`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
pub struct QualityReportRef {
    pub(crate) scope: u64,
    pub(crate) id: u64,
    pub(crate) kind: QualityOperationKind,
    pub(crate) detailed: bool,
}
impl QualityReportRef {
    /// Returns the process-local collector identity, allocated monotonically from a
    /// process-wide counter when the collector is activated. Compare this before combining
    /// report IDs or offsets. A successful collector has a nonzero scope; it is not stable
    /// across process restarts.
    pub fn scope(&self) -> u64 {
        self.scope
    }
    /// Returns the monotonically allocated observation ID within this scope, starting at one.
    /// Gaps are normal when admissions/events are lost; the ID is not the application request
    /// ID and does not reveal record availability.
    pub fn id(&self) -> u64 {
        self.id
    }
    /// Returns the report population chosen by the adapter at observation creation. It remains
    /// fixed for this reference; use it with protocol filters to compare like observation
    /// boundaries.
    pub fn kind(&self) -> QualityOperationKind {
        self.kind
    }
    /// Returns the once-per-report sampling decision. True requires Detailed mode and selection
    /// by the applicable operation/connection rate. False can mean Basic mode or an unsampled
    /// Detailed report; see [`QualityReport::detail_state`]. True does not guarantee that all
    /// detailed events were retained. Serialized as `detailed`.
    pub fn detail_selected(&self) -> bool {
        self.detailed
    }
}

/// Retention/lifecycle state of a report, separate from its SDK result.
///
/// Terminal observation states do not prove that network activity stopped. Expiration or owner
/// closure terminates diagnostic tracking and marks missing interval ends without fabricating
/// transport outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityObservationState {
    /// A Begin was retained and no terminal observation boundary has been processed. Missing
    /// facts can leave this state stale.
    InProgress,
    /// A Finish fact was retained. Its SDK outcome may still be Unknown or absent; inspect
    /// completeness and outcome separately.
    Completed,
    /// An active observation reached retention age without a retained Finish. The maintenance
    /// read supplies its terminal timestamp and marks it incomplete.
    Expired,
    /// Collection closed or failed while this report was active. Its terminal timestamp comes
    /// from maintenance, not a transport completion event.
    OwnerClosed,
}

/// Why a report does or does not retain detailed interval lifecycle.
///
/// Coarse phases may still be present in any state. This field describes
/// configuration/sampling, not completeness: selected reports can still lose facts to
/// diagnostic capacity or contention.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityDetailState {
    /// The owner uses Basic mode; detailed interval lifecycle/detail-only phases were not
    /// requested.
    BasicMode,
    /// Detailed mode selected this entire report for extra detail using the applicable sampling
    /// rate.
    Selected,
    /// The owner uses Detailed mode, but this report was not selected; coarse observations are
    /// still attempted.
    NotSampled,
}

/// A retained completed interval or instantaneous SDK milestone.
///
/// Intervals are appended when their guard ends, including guard drop, and contain both
/// timestamps. Milestones contain only `start`; their `end` and [`Self::duration`] are `None`.
/// Detailed reports also expose interval starts and lifecycle through
/// [`QualityReport::phase_activity`]. Entries follow retained event order rather than
/// necessarily start-time order.
///
/// JSON fields `start` and `end` are collector-epoch offsets, not report-relative offsets.
/// Associations are captured at interval start or milestone creation, so they can differ from
/// the report's latest associations.
#[derive(Clone, Debug, Serialize)]
pub struct QualityPhase {
    pub(crate) instance_id: u64,
    pub(crate) kind: PhaseKind,
    pub(crate) start: Duration,
    pub(crate) end: Option<Duration>,
    pub(crate) associations: QualityAssociations,
}
impl QualityPhase {
    /// Returns the nonzero logical phase sequence allocated within this report. Repeated kinds
    /// and milestones receive distinct IDs; a detailed lifecycle entry for the same interval
    /// shares this ID. It is not globally unique and gaps do not by themselves identify a lost
    /// network operation.
    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }
    /// Returns the SDK phase category supplied by the emitting instrumentation. The same kind
    /// may occur several times due to retries, redirect hops, or repeated work; use the
    /// instance ID and associations to distinguish entries.
    pub fn kind(&self) -> PhaseKind {
        self.kind
    }
    /// Returns the interval start or milestone timestamp measured from collector activation
    /// using its monotonic clock. Serialized as `start`; it is not relative to the report
    /// start. Subtract [`QualityReport::started_offset`] only when a report-relative
    /// visualization is needed.
    pub fn start_offset(&self) -> Duration {
        self.start
    }
    /// Returns the retained interval end in the same collector-epoch clock as `start_offset`.
    /// `None` identifies a milestone, not an unfinished interval in this view; use
    /// [`QualityReport::phase_activity`] for unfinished intervals. Serialized as `end`.
    pub fn end_offset(&self) -> Option<Duration> {
        self.end
    }
    /// Returns `end.saturating_sub(start)` for a retained completed interval. `None` is a
    /// milestone, not a zero-duration interval; a real interval can have zero duration at clock
    /// resolution. Guard drop also ends an interval and does not prove success.
    pub fn duration(&self) -> Option<Duration> {
        self.end.map(|end| end.saturating_sub(self.start))
    }
    /// Borrows associations captured at interval start or milestone emission. They do not
    /// update when the report later changes attempt/hop/connection. Current phase snapshots do
    /// not carry parent references; parent links are stored on the report.
    pub fn associations(&self) -> &QualityAssociations {
        &self.associations
    }
}
/// State of a retained detailed interval observation.
///
/// An interval ending, including by guard drop, is never proof of transport success or
/// cancellation. Terminal report states update unfinished activities without inventing an end
/// timestamp; consult report completeness when interpreting every state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityPhaseState {
    /// The last retained lifecycle fact is a start with no retained end. Data loss can make
    /// this stale even when the underlying work has ended.
    Running,
    /// The interval guard ended, including on drop. The end was retained; no business outcome
    /// is implied.
    Ended,
    /// The report completed without this interval's retained end. The end timestamp remains
    /// `None` and the report is marked incomplete.
    ReportCompleted,
    /// Diagnostic retention expired without a retained interval end. This does not cancel
    /// underlying work.
    ReportExpired,
    /// The collector closed or failed without a retained interval end. This marks lost
    /// diagnostic continuation, not the network operation's outcome.
    OwnerClosed,
}
/// Detailed lifecycle of one interval, including unfinished intervals.
///
/// Only reports selected in Detailed mode populate this view. Milestones remain exclusively in
/// [`QualityReport::phases`]. Matching entries in the two views share an instance ID. JSON uses
/// `start`, `end`, `state`, `associations`, and `start_observed`; offsets use the collector
/// epoch. A retained end can reconstruct an interval after its start event was lost, without
/// restoring completeness.
#[derive(Clone, Debug, Serialize)]
pub struct QualityPhaseActivity {
    pub(crate) instance_id: u64,
    pub(crate) kind: PhaseKind,
    pub(crate) start: Duration,
    pub(crate) end: Option<Duration>,
    pub(crate) state: QualityPhaseState,
    pub(crate) associations: QualityAssociations,
    pub(crate) start_observed: bool,
}
impl QualityPhaseActivity {
    /// Returns the report-local logical interval ID. If its completed interval is also retained
    /// in [`QualityReport::phases`], both entries have this same ID. IDs distinguish repeated
    /// phase kinds.
    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }
    /// Returns the instrumented interval category. Milestones have no entries in this lifecycle
    /// view.
    pub fn kind(&self) -> PhaseKind {
        self.kind
    }
    /// Returns the interval's original start timestamp in the collector epoch, serialized as
    /// `start`. This remains the actual captured start even when reconstructed from an end
    /// event after the start event was lost.
    pub fn start_offset(&self) -> Duration {
        self.start
    }
    /// Returns the retained interval-end timestamp in the collector epoch, serialized as `end`.
    /// `None` means no retained end; unlike [`QualityPhase::end_offset`], it never denotes a
    /// milestone. Report completion/expiration/closure changes state without inventing this
    /// value.
    pub fn end_offset(&self) -> Option<Duration> {
        self.end
    }
    /// Returns the retained lifecycle state. Running means no observed end while the report is
    /// open, not proof that underlying network work is still executing. Terminal report states
    /// identify why an end can no longer be expected.
    pub fn state(&self) -> QualityPhaseState {
        self.state
    }
    /// Borrows SDK association values captured when the interval started, including when its
    /// entry was reconstructed from an end event. They are not the report's latest
    /// associations; parent references are not copied into phase snapshots.
    pub fn associations(&self) -> &QualityAssociations {
        &self.associations
    }
    /// Returns whether the detailed start event itself was retained. False means the collector
    /// reconstructed this interval from an end event that carried the original
    /// start/associations. The reconstructed timing is available, but the report remains
    /// possibly incomplete.
    pub fn start_observed(&self) -> bool {
        self.start_observed
    }
}
/// HTTP protocol version reported by the actual response headers.
///
/// The HTTP adapter maps the dependency's response version after each successful response head,
/// including intermediate redirects. This enum does not require enabling another transport
/// feature and does not describe a requested/configured version or connection reuse.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityHttpVersion {
    /// The response dependency reported HTTP/0.9.
    Http09,
    /// The response dependency reported HTTP/1.0.
    Http10,
    /// The response dependency reported HTTP/1.1.
    Http11,
    /// The response dependency reported HTTP/2.
    Http2,
    /// The response dependency reported HTTP/3; this enum does not enable HTTP/3 support.
    Http3,
    /// A response version was observed but did not match a known mapping. Distinct from `None`,
    /// which means no retained response-version fact.
    Unknown,
}

/// Optional SDK correlation values attached to an observation or phase.
///
/// A report stores the latest retained association events. An interval stores the values
/// captured when it started; a milestone stores the values at emission. These are numeric SDK
/// identifiers/counters, not remote addresses or user-supplied correlation data. `Default`
/// leaves every field `None`. Absence means no retained association, which can reflect an
/// inapplicable field, uninstrumented boundary, or lost fact; it never implies zero.
///
/// Associations are meaningful in their original collector/client/session context. Do not treat
/// an attempt, cycle, connection, or operation number as globally unique. The JSON field names
/// match the getters.
#[derive(Clone, Debug, Default, Serialize)]
pub struct QualityAssociations {
    pub(crate) attempt: Option<u64>,
    pub(crate) hop: Option<u64>,
    pub(crate) connection: Option<u64>,
    pub(crate) session: Option<u64>,
    pub(crate) cycle: Option<u64>,
    pub(crate) operation: Option<u64>,
    pub(crate) parent: Option<QualityReportRef>,
}
impl QualityAssociations {
    /// Returns the adapter's attempt index/identity, if retained. HTTP uses the SDK request
    /// attempt counter; WS send/request retry paths use their retry counter, while WS
    /// connection reports use the handshake attempt ID. Numbering is specific to the source
    /// (connection attempts can begin at zero); do not assume a universal one-based sequence.
    /// `None` means no attempt association has been retained.
    pub fn attempt(&self) -> Option<u64> {
        self.attempt
    }
    /// Returns the HTTP redirect-hop index within the current attempt: zero for the original
    /// request, incremented for each followed redirect. HTTP connector children copy the
    /// initiating hop when task-local context is available. WebSocket observations normally
    /// leave this absent. A report stores the latest retained hop; phases retain their original
    /// hop.
    pub fn hop(&self) -> Option<u64> {
        self.hop
    }
    /// Returns the WebSocket connection/generation ID attached when dispatch selects a
    /// connection or a connection attempt succeeds. Receive and heartbeat paths copy the
    /// relevant connection generation. `None` is normal before selection and for HTTP, where
    /// the dependency does not reliably expose the selected pooled connection.
    pub fn connection(&self) -> Option<u64> {
        self.connection
    }
    /// Returns the WebSocket session ID copied from the session runtime (or bound heartbeat
    /// context). It identifies SDK session context, not a server session token. HTTP
    /// observations do not populate this association; missing data also leaves `None`.
    pub fn session(&self) -> Option<u64> {
        self.session
    }
    /// Returns the WebSocket connection/reconnect cycle generation copied when a
    /// connection-attempt observation begins. Several attempt reports can share a cycle. Other
    /// report populations generally leave it absent; it is not a retry count or a globally
    /// unique ID.
    pub fn cycle(&self) -> Option<u64> {
        self.cycle
    }
    /// Returns the WebSocket operation-control ID copied when a diagnostic probe is bound to an
    /// operation. It correlates with SDK operation state, not a wire/business request ID. Paths
    /// without an operation control, such as receive or connection observations, can leave it
    /// absent.
    pub fn operation(&self) -> Option<u64> {
        self.operation
    }
    /// Returns the related parent report reference, if explicitly attached and retained. HTTP
    /// connector reports can point to the initiating request; WebSocket retry-request children
    /// can point to their logical request operation. The reference does not pin the parent or
    /// prove an HTTP connection was selected. Phase association snapshots currently leave
    /// parent as `None`.
    pub fn parent(&self) -> Option<QualityReportRef> {
        self.parent
    }
}

/// Immutable snapshot of one bounded SDK observation.
///
/// The collector creates a report from a retained Begin event, then applies accepted facts
/// until completion, expiration, or closure. Queries and subscription updates return copies: an
/// older report does not update when the collector receives more data. Correlate with
/// [`Self::reference`] and compare [`Self::revision`] when refreshing.
///
/// Result selection (`result_at`, `outcome`, `http_status`, `failure`) is separate from
/// observation completion (`finished`, `state`). The first retained result is immutable;
/// callback/cleanup activity can continue afterward. A terminal report can still be incomplete.
/// See [`Self::possibly_incomplete`], [`Self::dropped_phases`], and snapshot counters before
/// interpreting absent data.
///
/// Serialized field names include `started`, `result_at`, `finished`, `detail`, `bytes`, and
/// `phase_overflow`; their public accessors are respectively `started_offset`, `result_offset`,
/// `finished_offset`, `detail_state`, `payload_bytes`, and `dropped_phases`. All timestamp
/// fields use the collector epoch.
#[derive(Clone, Debug, Serialize)]
pub struct QualityReport {
    pub(crate) schema_version: u32,
    pub(crate) reference: QualityReportRef,
    pub(crate) protocol: QualityProtocol,
    pub(crate) client_id: u64,
    pub(crate) revision: u64,
    pub(crate) started: Duration,
    pub(crate) result_at: Option<Duration>,
    pub(crate) finished: Option<Duration>,
    pub(crate) outcome: Option<QualityOutcome>,
    pub(crate) http_status: Option<u16>,
    pub(crate) http_version: Option<QualityHttpVersion>,
    pub(crate) failure: Option<QualityFailure>,
    pub(crate) state: QualityObservationState,
    pub(crate) detail: QualityDetailState,
    pub(crate) phases: Vec<QualityPhase>,
    pub(crate) phase_activity: Vec<QualityPhaseActivity>,
    pub(crate) associations: QualityAssociations,
    pub(crate) bytes: u64,
    pub(crate) phase_overflow: u64,
    pub(crate) possibly_incomplete: bool,
    pub(crate) counters_saturated: bool,
}
impl QualityReport {
    /// Returns the diagnostic report serialization schema discriminator stored when the report
    /// is created. It describes field/semantic compatibility for consumers of JSON, not the
    /// package release, protocol version, or report revision.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }
    /// Returns the opaque scope/ID/kind/detail identity allocated at probe creation. The
    /// reference is stable across report revisions but does not keep the record retained in the
    /// collector.
    pub fn reference(&self) -> QualityReportRef {
        self.reference
    }
    /// Returns the transport family supplied in the retained Begin event. It remains fixed
    /// throughout this report and should be combined with the operation kind when grouping
    /// measurements.
    pub fn protocol(&self) -> QualityProtocol {
        self.protocol
    }
    /// Returns the numeric client identity supplied by the transport's Begin event. HTTP
    /// allocates it from the diagnostic owner's client counter at construction; zero is its
    /// fallback when that owner is unavailable or the counter is exhausted. WebSocket copies
    /// the existing client instance ID from its session runtime instead of that HTTP counter.
    /// Clones preserve their client's identity, but values can overlap across protocols.
    /// Interpret this value together with collector scope and protocol, and combine client
    /// query filters with a protocol filter. It is not a user-supplied client name, URL, or
    /// application request ID.
    pub fn client_id(&self) -> u64 {
        self.client_id
    }
    /// Returns the collector-wide store revision assigned by this report's latest retained
    /// update. It is a saturating change sequence, not the number of events in this report;
    /// gaps reflect changes elsewhere in the collector. Compare revisions only within a scope
    /// and refresh through lookup/snapshot for newer evidence.
    pub fn revision(&self) -> u64 {
        self.revision
    }
    /// Returns the Begin event's timestamp in the collector-local monotonic epoch. Serialized
    /// as `started`. It marks when the adapter successfully began diagnostic observation, not
    /// DNS start, first packet, or a wall-clock time.
    pub fn started_offset(&self) -> Duration {
        self.started
    }
    /// Returns the timestamp of the first retained result-selection event, in the collector
    /// epoch. Serialized as `result_at`. `None` means no result event has been retained, even
    /// if observation has ended. Later results, callback failures, or cleanup cannot overwrite
    /// the first retained result.
    pub fn result_offset(&self) -> Option<Duration> {
        self.result_at
    }
    /// Returns the terminal observation timestamp in the collector epoch, serialized as
    /// `finished`. For Completed it comes from the Finish event; for Expired/OwnerClosed it
    /// comes from the maintenance read that closes observation. `None` means no terminal
    /// observation has been processed. It is not necessarily a network-completion timestamp.
    pub fn finished_offset(&self) -> Option<Duration> {
        self.finished
    }
    /// Returns `finished.saturating_sub(started)`, or `None` while no terminal observation
    /// timestamp exists. This can include body consumption, callback/delivery, cleanup, or time
    /// until lazy expiration/closure. Use [`Self::result_duration`] to measure the selected SDK
    /// result boundary.
    pub fn duration(&self) -> Option<Duration> {
        self.finished.map(|end| end.saturating_sub(self.started))
    }
    /// Returns `result_at.saturating_sub(started)`, or `None` when no result was retained. This
    /// includes all observed SDK work up to result selection, such as admission/retries; it is
    /// not pure wire latency. For HTTP streams, result selection occurs at body completion,
    /// failure, or drop, so this includes caller-driven body consumption. Summary percentiles
    /// use this duration only from complete successful reports.
    pub fn result_duration(&self) -> Option<Duration> {
        self.result_at.map(|end| end.saturating_sub(self.started))
    }
    /// Returns the first retained SDK result classification, or `None` if no result fact was
    /// retained. It is independent of observation state: an InProgress report can already have
    /// Success, and a Completed report can lack an outcome if data was lost.
    pub fn outcome(&self) -> Option<QualityOutcome> {
        self.outcome
    }
    /// Returns the status code copied from the selected HTTP SDK response, as a numeric `u16`.
    /// It can include non-2xx codes even when outcome is Success, because success means the SDK
    /// returned a response. `None` covers WebSocket/connection reports, errors selected without
    /// a response, and missing result evidence; it is not status zero.
    pub fn http_status(&self) -> Option<u16> {
        self.http_status
    }
    /// Returns the most recently retained version from a successfully received HTTP response
    /// head, including redirect responses. A redirect followed by failure can leave an
    /// intermediate version here. `None` means no retained version fact (or a non-HTTP-response
    /// population); Some(Unknown) means an observed but unmapped version.
    pub fn http_version(&self) -> Option<QualityHttpVersion> {
        self.http_version
    }
    /// Borrows the sanitized error kind/stage attached to the first retained result, if any.
    /// `None` is normal for successes and can also occur for failed connectors or
    /// unknown/abandoned scopes without a NetError. It does not establish success or expose the
    /// original error message.
    pub fn failure(&self) -> Option<&QualityFailure> {
        self.failure.as_ref()
    }
    /// Returns the report's observation/retention lifecycle state. Completed, Expired, and
    /// OwnerClosed are terminal for ordinary report facts, but later loss evidence can still
    /// mark them incomplete. Inspect outcome separately for the SDK result.
    pub fn state(&self) -> QualityObservationState {
        self.state
    }
    /// Returns whether Basic mode omitted detail, Detailed mode selected it, or Detailed mode
    /// did not sample it. Serialized as `detail`. This does not certify completeness or explain
    /// every missing phase; consult capabilities and loss counters too.
    pub fn detail_state(&self) -> QualityDetailState {
        self.detail
    }
    /// Borrows retained completed intervals and milestones in consumed event order, bounded by
    /// [`QualityLimits::max_phases`]. Coarse entries can be present in Basic and unsampled
    /// reports; detail-only kinds require selection. An empty slice can mean no phase occurred,
    /// unsupported instrumentation, or lost facts. Starts of unfinished intervals live in
    /// `phase_activity`.
    pub fn phases(&self) -> &[QualityPhase] {
        &self.phases
    }
    /// Borrows detailed interval lifecycles, including Running and terminal intervals, in
    /// retained insertion order. Empty in Basic mode and unsampled reports; even selected
    /// reports can be empty through absence/loss. Milestones are excluded. Entries share
    /// instance IDs with their completed `phases` counterparts, and the slice is bounded by
    /// max_phases.
    pub fn phase_activity(&self) -> &[QualityPhaseActivity] {
        &self.phase_activity
    }
    /// Iterates over lifecycle entries whose retained state is Running. This view exists only
    /// for detail-selected reports and can be stale when an end event was lost. It is not proof
    /// that the network operation or phase remains active. Terminal report transitions remove
    /// entries from this view by changing their state.
    pub fn active_phases(&self) -> impl Iterator<Item = &QualityPhaseActivity> {
        self.phase_activity
            .iter()
            .filter(|phase| phase.state == QualityPhaseState::Running)
    }
    /// Borrows the latest retained report-level association values. An attempt/hop/connection
    /// can change as the operation progresses; individual phases retain the associations
    /// captured at their own start/emission. `None` means no retained association, not a
    /// zero-valued identity.
    pub fn associations(&self) -> &QualityAssociations {
        &self.associations
    }
    /// Returns the largest cumulative application-byte count carried by retained report events,
    /// serialized as `bytes`. HTTP counts nonempty response-body chunks observed by the
    /// adapter; WebSocket dispatch counts prepared outbound payload length and receive reports
    /// count received payload length. Connection/heartbeat or uninstrumented byte paths can
    /// remain zero. This is neither wire bytes nor a combined upload/download total. Prepared
    /// outbound bytes can be counted even if a later send fails; the counter does not measure
    /// transmission attempts or retransmissions.
    ///
    /// Byte increments themselves do not enqueue an event: the next retained non-Begin fact
    /// refreshes the report. Active streams can therefore show a stale count until another
    /// event is consumed. Zero can mean an empty payload, no instrumented bytes yet, or missing
    /// evidence. The value saturates at `u64::MAX`; see [`Self::counters_saturated`].
    pub fn payload_bytes(&self) -> u64 {
        self.bytes
    }
    /// Returns the retained cumulative count of logical phase/milestone reservations rejected
    /// by the per-report phase budget, plus any storage phase overflow. Serialized as
    /// `phase_overflow`. It does not count general ingress loss or detail omitted by sampling.
    /// Producer overflow is carried by later events, so this value can lag; nonzero marks the
    /// report possibly incomplete.
    pub fn dropped_phases(&self) -> u64 {
        self.phase_overflow
    }
    /// Returns a conservative evidence-loss flag, not a transport-error flag. It is set for
    /// observed ingress gaps, report-specific loss summaries, phase overflow, reconstructed
    /// starts, missing interval ends at terminal observation, expiration/closure, or Completed
    /// reports with absent/Unknown outcome. Collector-wide gaps can flag a report without
    /// proving its own fact was lost; false is not proof of complete traffic coverage.
    pub fn possibly_incomplete(&self) -> bool {
        self.possibly_incomplete
    }
    /// Returns whether retained evidence indicates report byte accounting or phase-overflow
    /// accounting saturated. True means numeric counts cannot be treated as exact totals; false
    /// does not rule out diagnostic loss. Saturation flags themselves arrive via retained
    /// events.
    pub fn counters_saturated(&self) -> bool {
        self.counters_saturated
    }
    /// Serializes this immutable report into compact JSON using its stored field names.
    /// Durations are `{ "secs": ..., "nanos": ... }`, optional values become `null`, and
    /// annotated enum values use snake case. It does not query or refresh collection. Returns
    /// [`QualityError::ExportFailed`] if serialization fails.
    pub fn to_json(&self) -> Result<String, QualityError> {
        serde_json::to_string(self).map_err(|_| QualityError::ExportFailed)
    }
}
/// Alias for an operation [`QualityReport`]; the representation and available getters are
/// identical. Inspect [`QualityReportRef::kind`] to distinguish operation populations.
pub type OperationReport = QualityReport;
/// Alias for a connection [`QualityReport`]; it adds no transport-specific guarantees. HTTP
/// connector invocations and WebSocket connection attempts have different boundaries; filter by
/// protocol before comparison.
pub type ConnectionReport = QualityReport;
/// Sanitized classification of a retained [`crate::error::NetError`].
///
/// The adapter copies only `error.kind()` and `error.context().stage`. Messages, sources, URLs,
/// headers, and arbitrary context values are not retained. JSON serializes kind/stage as
/// snake-case strings (or `null` for a missing stage). Some failed/abandoned observations have
/// no NetError and therefore no `QualityFailure`.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct QualityFailure {
    #[serde(serialize_with = "serialize_error_kind")]
    pub(crate) kind: crate::api::error::ErrorKind,
    #[serde(serialize_with = "serialize_error_stage")]
    pub(crate) stage: Option<crate::api::error::ErrorStage>,
}
impl QualityFailure {
    /// Returns the copied SDK error category from the first retained error result. The enum is
    /// safe classification data; source error strings and arbitrary context were not retained.
    pub fn kind(&self) -> crate::api::error::ErrorKind {
        self.kind
    }
    /// Returns the optional stage copied from `NetError::context().stage`. `None` means the
    /// original error had no stage; it does not mean that failure occurred before all phases.
    /// This is an error-stage enum, not a measured QualityDoctor phase.
    pub fn stage(&self) -> Option<crate::api::error::ErrorStage> {
        self.stage
    }
}
fn serialize_error_kind<S: serde::Serializer>(
    kind: &crate::api::error::ErrorKind,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use crate::api::error::ErrorKind::*;
    serializer.serialize_str(match kind {
        InvalidInput => "invalid_input",
        InvalidConfig => "invalid_config",
        Io => "io",
        Dns => "dns",
        Tls => "tls",
        Protocol => "protocol",
        HandshakeRejected => "handshake_rejected",
        ProviderFailed => "provider_failed",
        CallbackPanicked => "callback_panicked",
        RuntimeUnavailable => "runtime_unavailable",
        ResourceExhausted => "resource_exhausted",
        QueueFull => "queue_full",
        QueueClosed => "queue_closed",
        ItemTooLarge => "item_too_large",
        PendingLimitReached => "pending_limit_reached",
        SubscriptionLimitReached => "subscription_limit_reached",
        CallbackOverflow => "callback_overflow",
        ControlOverflow => "control_overflow",
        ObservationLagged => "observation_lagged",
        NotConnected => "not_connected",
        NotRunning => "not_running",
        Unsupported => "unsupported",
        SessionAlreadyExists => "session_already_exists",
        ConnectionClosing => "connection_closing",
        Closed => "closed",
        EngineDropped => "engine_dropped",
        Cancelled => "cancelled",
        TimedOut => "timed_out",
        DeadlineExceeded => "deadline_exceeded",
        DeliveryUnknown => "delivery_unknown",
        RetryExhausted => "retry_exhausted",
        InvalidPolicy => "invalid_policy",
        BodyNotReplayable => "body_not_replayable",
        DuplicateRequestId => "duplicate_request_id",
        RoutingDisabled => "routing_disabled",
        RoutingModeMismatch => "routing_mode_mismatch",
        ClientAlreadyExists => "client_already_exists",
        ClientNotFound => "client_not_found",
        Internal => "internal",
        HttpStatus => "http_status",
    })
}
fn serialize_error_stage<S: serde::Serializer>(
    stage: &Option<crate::api::error::ErrorStage>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use crate::api::error::ErrorStage::*;
    let name = stage.map(|stage| match stage {
        Configuration => "configuration",
        Provider => "provider",
        RequestBuild => "request_build",
        Dns => "dns",
        Tcp => "tcp",
        Proxy => "proxy",
        ProxyTls => "proxy_tls",
        Tls => "tls",
        Upgrade => "upgrade",
        Admission => "admission",
        Queue => "queue",
        Write => "write",
        Response => "response",
        Receive => "receive",
        Heartbeat => "heartbeat",
        Close => "close",
        Dispatch => "dispatch",
        NetworkMonitor => "network_monitor",
        Runtime => "runtime",
    });
    name.serialize(serializer)
}

/// Result of looking up an opaque reference in one collector.
///
/// Only `Available` yields retained data. Absence has bounded evidentiary meaning: tombstones
/// are finite, so an old eviction eventually becomes indistinguishable from an observation that
/// was never retained.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum QualityLookup {
    /// A cloned immutable report was retained at lookup time. Its state can be active or
    /// terminal; refresh explicitly to obtain later revisions.
    Available(Arc<QualityReport>),
    /// A retained tombstone proves the record was evicted. Tombstones are limited by history
    /// capacity, so this evidence can later disappear.
    Evicted,
    /// No retained report or tombstone distinguishes observation loss, an older eviction, or a
    /// never-retained record. The current collector state supplies context only.
    NotRetained {
        /// Collector state at the unsuccessful lookup. This is contextual evidence only; it
        /// does not identify why this particular report is absent.
        owner_state: QualityDoctorState,
    },
    /// The reference belongs to a different collector scope; this collector cannot resolve it.
    WrongScope,
}

/// Conjunctive filters and bounded output options for a snapshot or subscription.
///
/// `Default` has no protocol/client/kind filter and uses the owner's default window and maximum
/// query results. Repeated setters replace the corresponding option. Filter values are
/// diagnostic IDs/enums; they never contain request URLs or business request IDs.
///
/// For snapshots, window membership uses `finished` for terminal reports and `started` for
/// active ones; the collector compares their age at query time with the effective window. The
/// result limit caps the returned report and finding vectors independently, while summary
/// statistics cover every matching retained report. Subscription report notifications use only
/// protocol/client/kind filters. Window affects optional aggregate snapshots; limit is
/// validated but has no visible output effect because their report/finding vectors are omitted.
#[derive(Clone, Debug, Default)]
pub struct QualityQuery {
    pub(crate) protocol: Option<QualityProtocol>,
    pub(crate) client_id: Option<u64>,
    pub(crate) kind: Option<QualityOperationKind>,
    pub(crate) window: Option<Duration>,
    pub(crate) limit: Option<usize>,
}
impl QualityQuery {
    /// Restricts matching reports to this adapter protocol. No feature or collector is
    /// activated; an unavailable/unobserved protocol simply has no matching reports.
    pub fn with_protocol(mut self, value: QualityProtocol) -> Self {
        self.protocol = Some(value);
        self
    }
    /// Restricts matching reports to this numeric client ID, obtained from
    /// [`QualityReport::client_id`] in the same collector. HTTP diagnostic IDs and WebSocket
    /// client instance IDs have different allocation sources and can overlap; combine with
    /// [`Self::with_protocol`] to select one transport's client. The value is unrelated to any
    /// business client/request name. Unknown IDs yield an empty population.
    pub fn with_client_id(mut self, value: u64) -> Self {
        self.client_id = Some(value);
        self
    }
    /// Restricts matching reports to this operation population. Combine with a protocol filter
    /// before comparing connection durations or computing homogeneous summary statistics.
    pub fn with_kind(mut self, value: QualityOperationKind) -> Self {
        self.kind = Some(value);
        self
    }
    /// Overrides the snapshot age window. Must be nonzero and no greater than the owner's
    /// retention duration; checked when querying/subscribing. Reports qualify when age from
    /// finished (terminal) or started (active) is at most this window. It does not filter
    /// individual phases/drop summaries or report subscription notifications.
    pub fn with_window(mut self, value: Duration) -> Self {
        self.window = Some(value);
        self
    }
    /// Caps returned reports and findings independently. Must be in
    /// `1..=QualityLimits::max_query_results`; validation occurs on snapshot/subscription
    /// creation. Summary statistics still include all matching records. Subscription report
    /// notifications are not capped by this value, and aggregate subscription snapshots omit
    /// their report/finding vectors entirely.
    pub fn with_limit(mut self, value: usize) -> Self {
        self.limit = Some(value);
        self
    }
    pub(crate) fn matches(&self, report: &QualityReport) -> bool {
        self.protocol.is_none_or(|value| report.protocol == value)
            && self.client_id.is_none_or(|value| report.client_id == value)
            && self.kind.is_none_or(|value| report.reference.kind == value)
    }
}

/// Collector-wide cumulative diagnostic accounting since activation.
///
/// Counters are refreshed on diagnostic reads, include all protocols/clients, and are not
/// restricted by a snapshot's query filters or time window. They measure observer
/// events/records, not all network traffic. Before activation, filtered-out protocols and
/// unsuccessful pre-collection gates do not contribute. All counters start at zero, saturate
/// rather than wrap, and are accompanied by [`Self::saturated`].
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct QualityCounters {
    pub(crate) begin_attempted: u64,
    pub(crate) begin_enqueued: u64,
    pub(crate) records_observed: u64,
    pub(crate) ingress_dropped: u64,
    pub(crate) ingress_lock_contended_dropped: u64,
    pub(crate) ingress_queue_full_dropped: u64,
    pub(crate) ingress_closed_rejected: u64,
    pub(crate) ingress_failed_rejected: u64,
    pub(crate) ingress_high_watermark: u64,
    pub(crate) drop_summaries_lost: u64,
    pub(crate) orphan_events: u64,
    pub(crate) records_evicted: u64,
    pub(crate) records_expired: u64,
    pub(crate) subscriber_dropped: u64,
    pub(crate) id_exhausted: u64,
    pub(crate) active_capacity_skipped: u64,
    pub(crate) saturated: bool,
}
impl QualityCounters {
    /// Returns the number of observation-start attempts that passed the activated, Running
    /// collector and protocol gates. Incremented before active-capacity and report-ID checks;
    /// therefore it includes starts subsequently skipped or unable to enqueue. It excludes
    /// network work before activation and work excluded by configuration.
    pub fn begin_attempted(&self) -> u64 {
        self.begin_attempted
    }
    /// Returns the number of Begin facts successfully admitted to ingress. The counter advances
    /// after enqueue succeeds, before a diagnostic read creates the record. It is not a
    /// completed-operation count and can exceed records_observed while facts await consumption.
    pub fn begin_enqueued(&self) -> u64 {
        self.begin_enqueued
    }
    /// Returns how many distinct reports were created by consuming Begin facts since
    /// activation. Includes records later evicted or expired; it is cumulative and is not the
    /// current retained-report count.
    pub fn records_observed(&self) -> u64 {
        self.records_observed
    }
    /// Returns diagnostic fact submissions lost because the ingress lock was contended or its
    /// queue was full. This aggregate combines the corresponding two reason counters.
    /// Rejections after closure/failure are separate, and a dropped event is not a failed
    /// network operation.
    pub fn ingress_dropped(&self) -> u64 {
        self.ingress_dropped
    }
    /// Returns facts dropped when the producer's one immediate ingress try_lock could not
    /// acquire the mutex. Producers do not wait or retry for diagnostic collection; these drops
    /// also contribute to ingress_dropped.
    pub fn ingress_lock_contended_dropped(&self) -> u64 {
        self.ingress_lock_contended_dropped
    }
    /// Returns facts dropped because ingress already held its configured number of events.
    /// These drops also contribute to ingress_dropped; loss-summary retention is attempted
    /// separately.
    pub fn ingress_queue_full_dropped(&self) -> u64 {
        self.ingress_queue_full_dropped
    }
    /// Returns fact submissions rejected after the collector observed owner closure. This is
    /// distinct from contention/capacity loss and does not contribute to ingress_dropped. Start
    /// attempts stopped by the earlier closed-owner gate are not counted here.
    pub fn ingress_closed_rejected(&self) -> u64 {
        self.ingress_closed_rejected
    }
    /// Returns fact submissions rejected because collection had failed, including a poisoned
    /// ingress mutex. These do not contribute to ingress_dropped. It describes diagnostic
    /// unavailability, not a transport failure count.
    pub fn ingress_failed_rejected(&self) -> u64 {
        self.ingress_failed_rejected
    }
    /// Returns the largest accepted ingress queue length observed since activation, in events.
    /// It never exceeds configured ingress capacity and is not the current queue length. Zero
    /// means no event has been successfully queued.
    pub fn ingress_high_watermark(&self) -> u64 {
        self.ingress_high_watermark
    }
    /// Returns the number of contention/capacity losses whose best-effort per-event summary
    /// could not be retained because its independent queue/lock was unavailable. Aggregate
    /// ingress-loss counters still include the original dropped facts; this counter does not
    /// add a second lost network event.
    pub fn drop_summaries_lost(&self) -> u64 {
        self.drop_summaries_lost
    }
    /// Returns consumed non-Begin facts whose report was absent from retained storage, for
    /// example after eviction or a missing Begin. Counts diagnostic facts, not distinct
    /// reports. Events ignored because their report is already terminal are not orphan events.
    pub fn orphan_events(&self) -> u64 {
        self.orphan_events
    }
    /// Returns retained records removed by history/active capacity management or terminal
    /// retention expiry. Each removal creates a bounded tombstone. This lifetime count can
    /// remain nonzero when the query's current window has no affected records.
    pub fn records_evicted(&self) -> u64 {
        self.records_evicted
    }
    /// Returns active records transitioned to Expired because no terminal observation was
    /// retained before their retention age. This marks diagnostic incompleteness and does not
    /// cancel network work. Their later removal is counted separately as an eviction.
    pub fn records_expired(&self) -> u64 {
        self.records_expired
    }
    /// Returns update deliveries lost across all receiver queues, including report, snapshot,
    /// gap, or closure delivery attempts. One shared report update can count once per affected
    /// subscriber. This does not necessarily mean the corresponding report was lost from
    /// collector storage.
    pub fn subscriber_dropped(&self) -> u64 {
        self.subscriber_dropped
    }
    /// Returns observation-start attempts that could not allocate another report ID without
    /// wrapping. Such attempts create no report. Scope exhaustion instead fails collector
    /// creation; this counter is not a general identifier-failure total.
    pub fn id_exhausted(&self) -> u64 {
        self.id_exhausted
    }
    /// Returns observation-start attempts skipped because the configured active-probe
    /// reservation bound was reached. They increment begin_attempted but do not enqueue Begin
    /// or produce a report reference. Underlying network work still proceeds.
    pub fn active_capacity_skipped(&self) -> u64 {
        self.active_capacity_skipped
    }
    /// Returns whether any tracked diagnostic counter has reached or overflowed its
    /// representable bound. Counters saturate at `u64::MAX` rather than wrap. Once true,
    /// cumulative values cannot be treated as exact totals; the flag does not indicate
    /// transport failure.
    pub fn saturated(&self) -> bool {
        self.saturated
    }
}

/// Statistics over all retained reports matching a snapshot's filters and window.
///
/// The population includes active and terminal reports and is independent of the returned
/// report-vector limit. These counts describe retained diagnostic evidence, not traffic-wide
/// availability or an unbiased success rate. No-observation fields default to zero, empty
/// vectors, and absent percentiles.
///
/// Latency samples are [`QualityReport::result_duration`] values from successful, Completed
/// reports whose `possibly_incomplete` flag is false. Percentiles use the nearest-rank method
/// on sorted samples, require the configured minimum sample count, and are absent for mixed
/// protocols or mixed operation kinds. `latency_samples` still counts eligible individual
/// samples in mixed populations. Outcome counts need not add up to retained or completed:
/// active reports can have results, and Unknown/Abandoned/unclassified results have no
/// dedicated counter.
#[derive(Clone, Debug, Default, Serialize)]
pub struct QualitySummary {
    pub(crate) protocols: Vec<QualityProtocol>,
    pub(crate) operation_kinds: Vec<QualityOperationKind>,
    pub(crate) retained: u64,
    pub(crate) completed: u64,
    pub(crate) successes: u64,
    pub(crate) failures: u64,
    pub(crate) timeouts: u64,
    pub(crate) cancelled: u64,
    pub(crate) incomplete: u64,
    pub(crate) latency_samples: u64,
    pub(crate) p50: Option<Duration>,
    pub(crate) p95: Option<Duration>,
    pub(crate) p99: Option<Duration>,
}
impl QualitySummary {
    /// Borrows the distinct protocols represented by matching retained reports, in encounter
    /// order (not sorted). Empty when no reports match. A mixed population has no shared
    /// percentiles or failure-ratio finding because transport boundaries differ.
    pub fn protocols(&self) -> &[QualityProtocol] {
        &self.protocols
    }
    /// Returns true when more than one protocol occurs in this summary. False also covers an
    /// empty population; it does not guarantee sufficient samples for percentiles.
    pub fn mixed_protocols(&self) -> bool {
        self.protocols.len() > 1
    }
    /// Borrows distinct report kinds represented by matching retained reports, in encounter
    /// order. Mixed kinds suppress shared percentiles and failure-ratio findings even if every
    /// report uses the same transport.
    pub fn operation_kinds(&self) -> &[QualityOperationKind] {
        &self.operation_kinds
    }
    /// Returns true when more than one operation kind occurs in the retained population. False
    /// also covers zero matching reports and does not establish statistical eligibility.
    pub fn mixed_kinds(&self) -> bool {
        self.operation_kinds.len() > 1
    }
    /// Returns the number of all matching retained reports, including active, completed,
    /// expired, and owner-closed reports. It is independent of the bounded snapshot
    /// report-vector length and is not a total-traffic count.
    pub fn retained(&self) -> u64 {
        self.retained
    }
    /// Returns matching reports whose observation state is Completed. A retained Finish is
    /// required; Expired and OwnerClosed reports are excluded. Completion does not require
    /// Success or complete phase evidence.
    pub fn completed(&self) -> u64 {
        self.completed
    }
    /// Returns matching reports with the first retained outcome Success, including reports
    /// still InProgress. HTTP non-2xx responses can qualify because the SDK returned a
    /// response. This is not an application-level success count.
    pub fn successes(&self) -> u64 {
        self.successes
    }
    /// Returns matching reports whose first retained outcome is Failed. Timeout, Cancelled,
    /// Abandoned, Unknown, and absent outcomes are excluded. A report need not be Completed to
    /// contribute.
    pub fn failures(&self) -> u64 {
        self.failures
    }
    /// Returns matching reports whose first retained outcome is Timeout. Such results come from
    /// explicit timeout/deadline classification or heartbeat timeout, not from diagnostic
    /// retention expiry. A report can contribute before observation completes.
    pub fn timeouts(&self) -> u64 {
        self.timeouts
    }
    /// Returns matching reports whose first retained outcome is Cancelled. This requires
    /// explicit SDK cancellation evidence; abandoned futures are not inferred as cancelled.
    /// These outcomes are excluded from the failure-ratio denominator.
    pub fn cancelled(&self) -> u64 {
        self.cancelled
    }
    /// Returns matching reports flagged possibly_incomplete or in Expired/OwnerClosed state.
    /// Each report counts once regardless of how many facts were lost. It does not count
    /// operations whose Begin was never retained.
    pub fn incomplete(&self) -> u64 {
        self.incomplete
    }
    /// Returns the number of eligible result-duration samples: matching reports with Success,
    /// Completed state, no incompleteness flag, and a retained result timestamp. The count
    /// remains available when mixed kinds/protocols or insufficient sample count suppress
    /// percentiles.
    pub fn latency_samples(&self) -> u64 {
        self.latency_samples
    }
    /// Returns the median eligible result duration using nearest-rank selection `ceil(n * 0.50)
    /// - 1` in the sorted sample vector. `None` means insufficient samples or mixed
    /// protocols/kinds, not zero latency. See [`Self::latency_samples`] for eligibility.
    pub fn p50(&self) -> Option<Duration> {
        self.p50
    }
    /// Returns the nearest-rank 95th percentile of eligible result durations. `None` means
    /// fewer than the configured minimum samples or a mixed protocol/kind population. The
    /// sample population is retained complete successes, not all observed traffic.
    pub fn p95(&self) -> Option<Duration> {
        self.p95
    }
    /// Returns the nearest-rank 99th percentile of eligible result durations. It has the same
    /// completeness, population, and minimum-sample requirements as [`Self::p50`]. `None` must
    /// not be substituted with a zero-duration measurement.
    pub fn p99(&self) -> Option<Duration> {
        self.p99
    }
}

/// Kind of evidence-derived advisory produced during snapshot construction. Findings never
/// adjust network policy or diagnose an unobserved remote root cause.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityFindingKind {
    /// A report's longest retained completed interval meets or exceeds the configured
    /// slow-phase threshold.
    SlowObservedPhase,
    /// A single-protocol, single-kind retained population has enough classified outcomes and
    /// `(failures + timeouts) / (successes + failures + timeouts)` meets the configured
    /// threshold.
    HighObservedFailureRatio,
    /// Matching reports are incomplete, or collector-wide lifetime counters show ingress loss
    /// or record eviction. This may persist even when the current query window contains no
    /// losses.
    IncompleteCoverage,
}
/// Bounded advisory derived from retained report evidence and configured policy.
///
/// A snapshot emits at most one slow-phase finding per matching report (its longest retained
/// qualifying interval), then attempts aggregate failure-ratio and incomplete-coverage findings
/// while space remains. Missing findings can therefore mean truncation, insufficient samples,
/// missing detail, or no qualifying evidence. JSON uses `duration` for
/// [`Self::observed_duration`].
#[derive(Clone, Debug, Serialize)]
pub struct QualityFinding {
    pub(crate) kind: QualityFindingKind,
    pub(crate) phase: Option<PhaseKind>,
    pub(crate) reference: Option<QualityReportRef>,
    pub(crate) samples: u64,
    pub(crate) duration: Option<Duration>,
    pub(crate) threshold: Option<Duration>,
}
impl QualityFinding {
    /// Returns the advisory classification chosen by snapshot analysis. It describes retained
    /// evidence and never changes transport behavior.
    pub fn kind(&self) -> QualityFindingKind {
        self.kind
    }
    /// Returns the qualifying phase kind for SlowObservedPhase. Aggregate failure-ratio and
    /// coverage findings return `None` because they concern a population rather than one phase.
    pub fn phase(&self) -> Option<PhaseKind> {
        self.phase
    }
    /// Returns the report reference that supplied a slow phase, or `None` for aggregate
    /// findings. The reference does not retain the report and may no longer resolve after
    /// collector eviction.
    pub fn reference(&self) -> Option<QualityReportRef> {
        self.reference
    }
    /// Returns the evidence count for this finding: one for a slow phase; Success + Failed +
    /// Timeout for a high-failure-ratio finding; all matching retained reports for incomplete
    /// coverage. These meanings differ by kind, and coverage can have zero samples when
    /// lifetime loss/eviction evidence triggered it.
    pub fn samples(&self) -> u64 {
        self.samples
    }
    /// Returns the longest qualifying retained completed phase duration for SlowObservedPhase,
    /// otherwise `None`. Serialized as `duration`. It is an SDK interval duration, not the
    /// total report latency or measured packet RTT.
    pub fn observed_duration(&self) -> Option<Duration> {
        self.duration
    }
    /// Returns the configured slow-phase duration threshold used for a SlowObservedPhase
    /// finding; otherwise `None`. This field never contains the failure-ratio threshold, which
    /// is dimensionless and lives in [`QualityPolicy`].
    pub fn threshold(&self) -> Option<Duration> {
        self.threshold
    }
}

/// Why a diagnostic event could not enter the bounded ingress queue. Rejection after collector
/// closure/failure is counted separately and does not use these loss-summary reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityDropReason {
    /// The producer could not acquire the ingress mutex immediately and dropped its diagnostic
    /// event instead of waiting.
    LockContended,
    /// The ingress queue already contained its configured maximum number of pending diagnostic
    /// events.
    QueueFull,
}

/// Safe category of a lost internal diagnostic event.
///
/// Used by [`QualityDropSummary`] to explain missing evidence without exposing payload
/// contents. These categories describe collector facts; they are not WebSocket frame opcodes or
/// network packet types.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityEventKind {
    /// Creation of a report from a newly admitted diagnostic probe. Losing it means no report
    /// can be created for that reference.
    Begin,
    /// Start of a detailed interval lifecycle observation. A retained end can reconstruct the
    /// interval but marks coverage incomplete.
    PhaseStarted,
    /// End of an interval, including guard drop; contains the original start timestamp.
    PhaseEnded,
    /// An instantaneous SDK boundary with no interval end.
    Milestone,
    /// SDK result classification and optional sanitized failure/HTTP status.
    Result,
    /// Update to an attempt, hop, connection, session, cycle, or operation association.
    Association,
    /// Link from this report to a related parent report reference.
    Parent,
    /// Final observation boundary. Losing it can leave the report active until expiration or
    /// closure.
    Finish,
    /// The HTTP version read from a successfully received response head.
    HttpVersion,
}
/// Best-effort evidence for one event lost to contention or ingress capacity.
///
/// The summary retains only diagnostic identity, event/phase categories, and the event's
/// timestamp. Both its pending queue and retained history are capped at the smaller of 64 and
/// ingress capacity, and old entries expire with retention. Recording the loss summary can
/// itself fail; consult [`QualityCounters::drop_summaries_lost`] and aggregate loss counters
/// rather than assuming this list is exhaustive.
#[derive(Clone, Debug, Serialize)]
pub struct QualityDropSummary {
    pub(crate) reference: QualityReportRef,
    pub(crate) event_kind: QualityEventKind,
    pub(crate) phase_kind: Option<PhaseKind>,
    pub(crate) phase_instance_id: Option<u64>,
    pub(crate) reason: QualityDropReason,
    pub(crate) event_offset: Duration,
}
impl QualityDropSummary {
    /// Returns the reference carried by the lost event. It can refer to a report never created
    /// (lost Begin) or no longer retained; use lookup without assuming availability.
    pub fn reference(&self) -> QualityReportRef {
        self.reference
    }
    /// Returns the sanitized internal fact category that was lost. Phase events and
    /// association/result/finish facts have different implications for report completeness.
    pub fn event_kind(&self) -> QualityEventKind {
        self.event_kind
    }
    /// Returns the phase category for a lost PhaseStarted, PhaseEnded, or Milestone event.
    /// Other categories return `None` because they carry no phase.
    pub fn phase_kind(&self) -> Option<PhaseKind> {
        self.phase_kind
    }
    /// Returns the report-local logical phase ID for phase or milestone events, otherwise
    /// `None`. It can be used to relate loss evidence to retained phase/lifecycle entries
    /// sharing that ID; those entries may themselves be absent.
    pub fn phase_instance_id(&self) -> Option<u64> {
        self.phase_instance_id
    }
    /// Returns whether the producer lost the fact to immediate lock contention or a full
    /// ingress queue. Closed/failed rejections have separate counters and do not generate this
    /// summary.
    pub fn reason(&self) -> QualityDropReason {
        self.reason
    }
    /// Returns the original event timestamp measured from collector activation, not the time a
    /// reader discovered the loss. For PhaseStarted it is the original start; for PhaseEnded it
    /// is the end boundary. Recent-drop expiry uses this timestamp. Serialized as
    /// `event_offset`.
    pub fn event_offset(&self) -> Duration {
        self.event_offset
    }
}

/// Immutable query result captured after draining pending diagnostic facts.
///
/// The collector copies its current store, lifetime counters, recent loss evidence, bounded
/// report list, and findings. Summary statistics include every matching retained record, even
/// when report output is truncated. Capturing a snapshot performs no network I/O. Concurrent
/// producers can enqueue facts after the drain, so the result is a retained observation view
/// rather than an atomic snapshot of all network activity.
///
/// Subscription [`QualityUpdate::SnapshotUpdated`] values intentionally omit reports, findings,
/// and recent drop evidence. Use [`QualityDoctor::snapshot`] when those fields are needed. JSON
/// uses `captured_at` for [`Self::captured_at_offset`].
#[derive(Clone, Debug, Serialize)]
pub struct QualitySnapshot {
    pub(crate) schema_version: u32,
    pub(crate) revision: u64,
    pub(crate) scope: u64,
    pub(crate) captured_at: Duration,
    pub(crate) window: Duration,
    pub(crate) state: QualityDoctorState,
    pub(crate) summary: QualitySummary,
    pub(crate) counters: QualityCounters,
    pub(crate) recent_drops: Vec<QualityDropSummary>,
    pub(crate) reports: Vec<QualityReport>,
    pub(crate) findings: Vec<QualityFinding>,
    pub(crate) results_truncated: bool,
}
impl QualitySnapshot {
    /// Returns the serialization schema discriminator for this snapshot shape. It is
    /// independent of the crate release, capability-description version, and store revision;
    /// exported consumers should use it to interpret fields.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }
    /// Returns the collector-wide saturating change sequence captured from the retained store.
    /// It advances for report/counter/loss/history/closure changes, not once per query. Equal
    /// revisions do not guarantee equal snapshots: filters, capture time, and a sliding window
    /// can change the result without changing the store.
    pub fn revision(&self) -> u64 {
        self.revision
    }
    /// Returns the collector's process-local identity. Report references and timestamp offsets
    /// in this snapshot belong to this scope; do not compare them with values from another
    /// owner or process run.
    pub fn scope(&self) -> u64 {
        self.scope
    }
    /// Returns the monotonic elapsed time since collector activation at snapshot construction.
    /// Serialized as `captured_at`. It is neither a Unix timestamp nor the last report's
    /// timestamp; concurrent producers can have pending facts not yet reflected in the
    /// snapshot.
    pub fn captured_at_offset(&self) -> Duration {
        self.captured_at
    }
    /// Returns the effective lookback duration: the query override, or the owner's default. It
    /// filters report age using finished for terminal reports and started for active ones.
    /// Counters and recent drop evidence are independent of this window.
    pub fn window(&self) -> Duration {
        self.window
    }
    /// Returns collector lifecycle state at snapshot construction. It describes diagnostic
    /// availability, not network connectivity or the terminal state of every report.
    /// Closed/failed collectors can still have retained evidence.
    pub fn state(&self) -> QualityDoctorState {
        self.state
    }
    /// Borrows statistics over every matching retained report within the effective window,
    /// including records omitted by the output limit. These statistics do not represent all
    /// network traffic and can exclude work lost before report creation.
    pub fn summary(&self) -> &QualitySummary {
        &self.summary
    }
    /// Borrows collector-wide cumulative counters since activation, refreshed by diagnostic
    /// reads. Query protocol/client/kind filters and the time window do not restrict them. Use
    /// summary for the query population and counters for global observation coverage.
    pub fn counters(&self) -> &QualityCounters {
        &self.counters
    }
    /// Borrows recent best-effort ingress-loss evidence in retained consumption order. This is
    /// collector-wide and ignores report filters and the query window; retention duration still
    /// expires old entries. Both pending and retained queues are bounded by min(64, ingress
    /// capacity). Aggregate subscription snapshots omit this slice; query snapshot() for
    /// evidence.
    pub fn recent_drops(&self) -> &[QualityDropSummary] {
        &self.recent_drops
    }
    /// Borrows up to the effective query limit of matching report copies, ordered newest Begin
    /// insertion first. This is not strict completion-time or producer-timestamp order. Summary
    /// includes all matches even if this list is truncated. Aggregate subscription snapshots
    /// always leave this slice empty.
    pub fn reports(&self) -> &[QualityReport] {
        &self.reports
    }
    /// Borrows bounded advisories computed from matching report evidence and global coverage
    /// counters. The vector has its own query-limit bound; at most one slow phase per report is
    /// considered before aggregate findings. Empty does not prove good quality, and aggregate
    /// subscription snapshots omit findings entirely.
    pub fn findings(&self) -> &[QualityFinding] {
        &self.findings
    }
    /// Returns whether a direct snapshot had more matching reports than its report-output
    /// limit. It does not report finding-vector truncation. Aggregate subscription snapshots
    /// set this true whenever their summary contains any reports, because those notifications
    /// intentionally omit all report copies.
    pub fn results_truncated(&self) -> bool {
        self.results_truncated
    }
    /// Serializes the stored snapshot to compact JSON without another collector read. Stored
    /// fields include `captured_at`; durations serialize as objects with `secs` and `nanos`,
    /// options as `null`, and annotated enums as snake-case strings. Returns
    /// [`QualityError::ExportFailed`] on serialization failure.
    pub fn to_json(&self) -> Result<String, QualityError> {
        serde_json::to_string(self).map_err(|_| QualityError::ExportFailed)
    }
}

/// Coverage classification for one protocol/phase under the owner's configuration. This
/// describes instrumentation availability, not whether a particular operation produced or
/// retained the phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityCapabilityLevel {
    /// The represented capability is fully supported under its declared semantics. Current
    /// phase entries generally use Partial for SDK-boundary measurements.
    Supported,
    /// The phase is instrumented with the stated limitation, such as opaque dependency work or
    /// caller-driven consumption.
    Partial,
    /// The phase cannot be observed by this build/adapter; inspect the reason to distinguish a
    /// missing feature from unavailable instrumentation.
    Unsupported,
    /// Instrumentation exists but is disabled by protocol selection, mode, or applicable detail
    /// sampling settings.
    DisabledByConfig,
}
/// Reason for the coverage classification in a capability entry. Read together with
/// [`QualityCapability::level`]; reasons describe measurement limitations and configuration
/// rather than network failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityCapabilityReason {
    /// Only the SDK's execution boundaries are observable, including scheduling and local work.
    SdkBoundary,
    /// The underlying dependency does not expose a reliable independent boundary, as with HTTP
    /// DNS/TCP/TLS internals.
    DependencyOpaque,
    /// HTTP connector observations may represent competing candidates and cannot identify the
    /// pool-selected connection.
    CompetingConnectionCandidates,
    /// HTTP streaming body observation includes time controlled by caller polling or
    /// abandonment.
    ConsumerDrivenBody,
    /// The owner configuration excludes this compiled protocol.
    ProtocolDisabled,
    /// The corresponding HTTP/WS Cargo feature is absent.
    FeatureNotCompiled,
    /// System resolver timing aggregates opaque lookup/cache/retry behavior.
    SystemResolutionAggregate,
    /// Reserved reason for a measurement requiring packet-level timing; this collector does not
    /// capture packets.
    NotPacketCapture,
    /// Reserved reason for a phase that does not apply to the selected protocol.
    ProtocolNotApplicable,
    /// The current adapter has no producer for this phase/protocol combination.
    NotInstrumented,
    /// A detail-only phase is disabled by Basic mode or a zero applicable detail sampling rate.
    DetailDisabled,
}
/// One protocol/phase entry in the fixed capability matrix.
///
/// Generated from compiled features, selected protocols, collection mode, sampling settings,
/// and known instrumentation boundaries when the collector is activated. It is not inferred
/// from recent traffic or the presence of phases in a report.
#[derive(Clone, Debug, Serialize)]
pub struct QualityCapability {
    pub(crate) protocol: QualityProtocol,
    pub(crate) phase: PhaseKind,
    pub(crate) level: QualityCapabilityLevel,
    pub(crate) reason: QualityCapabilityReason,
}
impl QualityCapability {
    /// Returns the transport family this capability entry describes. Entries exist for both
    /// protocols even when one was not compiled or selected.
    pub fn protocol(&self) -> QualityProtocol {
        self.protocol
    }
    /// Returns the SDK phase category covered by this entry. Read level/reason before treating
    /// a missing report phase as a measured zero or a transport defect.
    pub fn phase(&self) -> PhaseKind {
        self.phase
    }
    /// Returns coverage determined from compiled features, protocol switches, detail settings,
    /// and the adapter's known instrumentation. An enabled level does not guarantee retention
    /// for every operation.
    pub fn level(&self) -> QualityCapabilityLevel {
        self.level
    }
    /// Returns the measurement/configuration limitation that explains the level. This is fixed
    /// capability metadata and is not derived from a particular network failure.
    pub fn reason(&self) -> QualityCapabilityReason {
        self.reason
    }
}
/// Immutable description of the collector's measurement coverage.
///
/// The phase matrix includes entries for both protocols, including unavailable/disabled
/// combinations. A Partial entry still denotes useful SDK evidence but does not promise pure
/// wire timing. Sampling and loss can leave an individual report empty despite enabled
/// instrumentation. Capabilities are shared by cloned doctor handles.
#[derive(Clone, Debug, Serialize)]
pub struct QualityCapabilities {
    pub(crate) version: u32,
    pub(crate) phases: Vec<QualityCapability>,
}
impl QualityCapabilities {
    /// Returns the capability-description schema discriminator. It describes interpretation of
    /// this metadata, not the crate release, an HTTP version, or the report/snapshot
    /// serialization discriminator.
    pub fn version(&self) -> u32 {
        self.version
    }
    /// Borrows the fixed protocol/phase matrix generated at collector activation. Search by
    /// protocol and phase rather than relying on positional order. Unsupported and disabled
    /// entries are included so absence of measurements has an explicit explanation.
    pub fn phases(&self) -> &[QualityCapability] {
        &self.phases
    }
    /// Returns false: the HTTP dependency does not reliably expose which pooled/competing
    /// connection actually served a request. An HTTP connector report or parent reference
    /// therefore does not establish selected connection identity or reuse.
    pub fn exact_http_connection_identity(&self) -> bool {
        false
    }
    /// Returns false: the observer records SDK boundaries and does not capture packets, TCP
    /// acknowledgements, server processing timing, or precise wire transmission/reception
    /// timestamps.
    pub fn packet_timing(&self) -> bool {
        false
    }
}

/// Bounded receiver settings for [`QualityDoctor::subscribe`].
///
/// Defaults are 64 update slots, an unfiltered [`QualityQuery`], no aggregate snapshot updates,
/// and a 25 ms receive polling interval. Setters store values; subscription creation validates
/// them against the owner's limits. Subscription creation does not replay history or create a
/// consumer task.
#[derive(Clone, Debug)]
pub struct QualitySubscriptionOptions {
    pub(crate) capacity: usize,
    pub(crate) query: QualityQuery,
    pub(crate) snapshots: bool,
    pub(crate) poll_interval: Duration,
}
impl Default for QualitySubscriptionOptions {
    fn default() -> Self {
        Self {
            capacity: 64,
            query: QualityQuery::default(),
            snapshots: false,
            poll_interval: Duration::from_millis(25),
        }
    }
}
impl QualitySubscriptionOptions {
    /// Sets the receiver's bounded update-slot count (default 64). Subscription creation
    /// requires a nonzero value no greater than [`QualityLimits::subscription_capacity`]. This
    /// is independent of ingress capacity; a full subscriber drops its own updates without
    /// backpressuring the network or other subscribers.
    pub fn with_capacity(mut self, value: usize) -> Self {
        self.capacity = value;
        self
    }
    /// Replaces the query used for this subscription. Report notifications apply
    /// protocol/client/kind matching only; window affects optional aggregate snapshots. Query
    /// limit/window are validated on subscription creation, but aggregate snapshot vectors are
    /// intentionally empty and no report-count limit is imposed on the ongoing stream.
    pub fn with_filter(mut self, value: QualityQuery) -> Self {
        self.query = value;
        self
    }
    /// Enables or disables aggregate snapshot notifications (default false). They are emitted
    /// only when collection observes relevant store/event changes, not as a periodic clock tick
    /// or history replay. They contain summary, counters, state, and metadata; reports,
    /// findings, and recent drop evidence are empty. Use [`QualityDoctor::snapshot`] for full
    /// query output.
    pub fn with_snapshot_updates(mut self, value: bool) -> Self {
        self.snapshots = value;
        self
    }
    /// Sets the delay between empty asynchronous receive polls (default 25 ms). Valid range is
    /// 1 ms through 60 seconds inclusive, checked by subscription creation. The delay runs only
    /// while the caller awaits [`QualitySubscription::recv`] on its Tokio time driver; no
    /// background timer/task is created. Producers never invoke subscriber wakers, so this
    /// affects observation latency, not transport timeouts.
    pub fn with_poll_interval(mut self, value: Duration) -> Self {
        self.poll_interval = value;
        self
    }
}
/// Location at which observations were lost. Ingress gaps affect shared collector evidence;
/// subscription gaps affect only one receiver's delivered updates.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QualityGapScope {
    /// Shared collector events were dropped because ingress was full or contended. The count is
    /// collector-wide, independent of subscription filters.
    Ingress,
    /// Updates for this receiver could not be queued. Shared collector reports may still retain
    /// the corresponding evidence.
    Subscription,
}
/// Saturating count of diagnostic events or updates missed since the preceding reported gap at
/// the same boundary.
///
/// A gap is best effort and can itself be lost to a full subscription. It does not identify a
/// particular failed network request; use lifetime counters and a fresh snapshot to assess
/// current coverage.
#[derive(Clone, Debug, Serialize)]
pub struct QualityGap {
    pub(crate) scope: QualityGapScope,
    pub(crate) dropped: u64,
}
impl QualityGap {
    /// Returns whether the missing items were shared ingress facts or updates for this
    /// particular subscriber. The scope determines the unit used by [`Self::dropped`].
    pub fn scope(&self) -> QualityGapScope {
        self.scope
    }
    /// Returns the saturating count of missed items represented by this notification: ingress
    /// events since the previous drain accounting, or subscriber updates accumulated since the
    /// preceding delivered subscription gap. Counts are not distinct operations, packets, or
    /// lifetime totals. Gap notifications are best effort; consult snapshot counters for
    /// cumulative accounting.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}
/// One bounded, best-effort subscription notification.
///
/// Collection coalesces a batch of facts into report snapshots; this is not a durable event log
/// and does not deliver one update per phase. Full subscriber queues lose updates independently
/// of the shared collector. A gap or closure notification may also be lost, so receiver
/// termination remains the definitive end of the stream.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum QualityUpdate {
    /// Latest retained immutable report after a collection batch while its observation state is
    /// InProgress. Multiple internal facts can be coalesced into this update.
    ReportUpdated(Arc<QualityReport>),
    /// Latest terminal report after a batch; includes Completed, Expired, and OwnerClosed
    /// states. Later discovered loss can cause another terminal update, so this is not an
    /// exactly-once notification.
    ReportCompleted(Arc<QualityReport>),
    /// Aggregate-only snapshot when enabled and collection state changes. Report, finding, and
    /// recent-drop vectors are empty by design; summary and lifetime counters remain available.
    SnapshotUpdated(Arc<QualitySnapshot>),
    /// Best-effort indication that ingress events or this receiver's updates were lost. It may
    /// be delayed until queue capacity becomes available.
    Gap(QualityGap),
    /// The collector has closed or failed and is ending subscription delivery. This
    /// notification can be dropped; a disconnected receiver also signals termination.
    Closed,
}
/// Nonblocking subscription receive status, unrelated to a transport error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QualityTryRecvError {
    /// Collection was driven but this subscription currently has no queued update; it may
    /// receive more later.
    Empty,
    /// The receiver was unsubscribed or disconnected and no further update can be read through
    /// this call.
    Closed,
}
/// Cloneable, read-only handle to an activated diagnostic collector.
///
/// Obtain it from a configured owner's `quality_doctor()` accessor before starting work. Clones
/// share scope, store, limits, and lifetime counters; they neither create another collector nor
/// keep the HTTP/WS network owner alive. Owner shutdown/drop closes collection, while retained
/// diagnostic state can remain queryable through surviving handles.
///
/// [`Self::snapshot`], [`Self::lookup`], and subscription reads drive collection and lazy
/// maintenance. [`Self::state`] and [`Self::capabilities`] only inspect metadata. There is no
/// background collector task or user callback registration.
#[derive(Clone)]
pub struct QualityDoctor {
    pub(crate) inner: Arc<Collector>,
}
impl fmt::Debug for QualityDoctor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QualityDoctor")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}
/// A bounded receiver for report, optional aggregate snapshot, gap, and closure updates.
///
/// Read with [`Self::try_recv`] without a timer, or await [`Self::recv`] on a Tokio runtime
/// with a time driver. Reads perform collection on the caller; producers do not invoke
/// subscriber wakers and the SDK spawns no hidden consumer task. Dropping the subscription or
/// calling [`Self::unsubscribe`] removes its registration. The handle retains diagnostic
/// storage, not the network owner.
pub struct QualitySubscription {
    pub(crate) inner: SubscriptionInner,
}
impl fmt::Debug for QualitySubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QualitySubscription")
            .finish_non_exhaustive()
    }
}

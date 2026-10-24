# QualityDoctor developer guide

QualityDoctor provides passive, bounded diagnostics for open-net HTTP and
WebSocket operations. Use it to inspect where the SDK spent time, correlate
operations with connection attempts, observe incomplete work, and export a safe
diagnostic snapshot. The public API is available through `open_net::quality`.

The observations describe SDK boundaries. They do not measure packets, prove
remote application delivery, or diagnose a root cause by themselves.
QualityDoctor does not change DNS, routes, proxies, TLS policy, connection pools,
retry decisions, deadlines, or message delivery. It sends no additional probe or
heartbeat and does not repair a connection.

## Enable collection

Collection requires three independent steps:

1. Enable the `quality-doctor` Cargo feature and the transport features you use.
2. Configure `QualityDoctorConfig::basic()` or `QualityDoctorConfig::detailed()`
   before creating the engine or standalone HTTP client.
3. Call that owner's `quality_doctor()` accessor before starting the operations
   you want to observe.

For an HTTP application:

```sh
cargo add open-net --no-default-features --features http-client,quality-doctor
cargo add tokio --features macros,rt-multi-thread,time
```

For an application using both transports, select
`http-client,ws-client,quality-doctor` instead. `quality-doctor` enables neither a
transport nor network monitoring by itself, and is not a default feature.
`QualityProtocols` selects which compiled transports to observe; it cannot
enable a missing Cargo feature.

The HTTP and WebSocket examples below explicitly select an application-owned TLS
provider. To use that provider in your application:

```sh
cargo add rustls-openssl
```

Choose the TLS provider appropriate to your application. QualityDoctor has no
separate TLS configuration. The normal transport provider requirement applies
even when the example endpoint uses an unencrypted connection.

### Owners and activation

An engine created with `OpenNetConfig::with_quality_doctor` owns one diagnostic
scope shared by its HTTP and WebSocket clients. Activate it with
`OpenNet::quality_doctor()`. An engine HTTP client's accessor reaches the same
scope. HTTP clients receive owner-allocated diagnostic IDs; WebSocket clients
use their existing `ClientId` value. Clones retain their client's identity.
These two ID populations can overlap numerically, so combine client and protocol
filters when selecting one client.

A standalone `HttpClient` uses
`HttpClientConfig::with_standalone_quality_doctor` and owns its own scope. Passing
this standalone setting to an engine-owned HTTP client is an `InvalidConfig`
error. Sharing a network-status context with an engine does not make a standalone
HTTP client share the engine's diagnostic scope.

The accessor returns `None` when collection is configured `Off`, or when an
engine-owned context can no longer reach its owner. On first activation it
allocates bounded diagnostic storage on the accessor caller's path. Later calls
share that collector. Operations started before activation are not reconstructed
or retroactively attached, including operations still running at activation.

Configuration remains fixed for the owner's lifetime. There is no runtime mode
toggle. Activating a doctor neither rebuilds HTTP clients and their pools nor
reconnects WebSocket sessions. Keep activation outside a latency-sensitive
request path.

### Standalone HTTP example

This application takes a base URL as its first argument, observes a buffered GET,
looks up the matching report if one exists, then exports a snapshot after client
shutdown. Both a successful response and an observed `NetError` can carry a
reference.

```rust,no_run
use open_net::{
    api::http::{HttpClient, HttpClientConfig, HttpTlsConfig},
    quality::{QualityDoctorConfig, QualityLookup, QualityQuery},
    BoxError,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let base_url = std::env::args().nth(1).ok_or("provide an HTTP base URL")?;
    let tls = HttpTlsConfig::default()
        .with_crypto_provider(Arc::new(rustls_openssl::default_provider()));
    let config = HttpClientConfig::new(base_url)?
        .with_tls(tls)
        .with_standalone_quality_doctor(QualityDoctorConfig::detailed());
    let client = HttpClient::new(config, "http-quality")?;
    let doctor = client.quality_doctor().ok_or("diagnostics are disabled")?;

    let result = client.get("/").await;
    let reference = match &result {
        Ok(response) => response.quality_ref(),
        Err(error) => error.quality_ref(),
    };
    if let Some(reference) = reference {
        match doctor.lookup(&reference) {
            QualityLookup::Available(report) => {
                println!("result={:?}, elapsed={:?}", report.outcome(), report.result_duration());
            }
            other => println!("diagnostic availability: {other:?}"),
        }
    }

    client.shutdown().await?;
    let snapshot = doctor.snapshot(QualityQuery::default())?;
    println!("{}", snapshot.to_json()?);
    result?;
    Ok(())
}
```

`QualityOutcome::Success` means the HTTP adapter selected a successful result;
it does not classify the HTTP status as successful. An error status returned as
a normal response still has this outcome. Apply your application's status and
body validation separately. `http_status()` contains the selected numeric status
where the adapter retained it.

For streams, ordinary `stream` and SSE APIs remain observed. Use
`stream_with_quality_ref` when you also need the reference alongside the
`HttpStreamResponse`, whose fields do not contain a quality reference:

```rust,no_run
use open_net::{
    api::http::{HttpClient, HttpRequest},
    quality::QualityDoctor,
    BoxError,
};

async fn consume_stream(client: &HttpClient, doctor: &QualityDoctor) -> Result<(), BoxError> {
    let request = HttpRequest::builder().path("/").build()?;
    let (mut response, reference) = client.stream_with_quality_ref(request).await?;
    let mut body_result = Ok(());
    while let Some(chunk) = response.next_chunk().await {
        match chunk {
            Ok(bytes) => println!("consumed {} bytes", bytes.len()),
            Err(error) => {
                body_result = Err(error);
                break;
            }
        }
    }
    drop(response);
    if let Some(reference) = reference {
        println!("{:?}", doctor.lookup(&reference));
    }
    body_result?;
    Ok(())
}
```

Receiving headers does not imply that body consumption finished or that a
diagnostic result has been selected. A transport stream normally selects its
quality result at its terminal body boundary, such as EOF, error, or drop;
`result_duration()` therefore normally includes its observed body lifetime,
rather than stopping when `stream_with_quality_ref` returns headers. The same
reference follows the body through these boundaries. Observation retention can
expire while the network stream remains open. An error before the stream opens
is returned directly and can be inspected with `NetError::quality_ref()`.

### Engine-owned WebSocket example

This application takes a WebSocket endpoint as its first argument. It activates
the engine doctor before connecting, sends a message through a receipt, and
queries only message reports. Connection attempts appear in the same scope but
have a different operation kind.

```rust,no_run
use open_net::{
    network::{NetworkConfig, TlsConfig},
    quality::{QualityDoctorConfig, QualityOperationKind, QualityProtocol, QualityQuery},
    BoxError, OpenNet, OpenNetConfig,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::args().nth(1).ok_or("provide a WebSocket endpoint")?;
    let tls = TlsConfig::default()
        .with_crypto_provider(Arc::new(rustls_openssl::default_provider()));
    let config = OpenNetConfig::default()
        .with_network_config(NetworkConfig::default().with_tls(tls))
        .with_quality_doctor(QualityDoctorConfig::detailed());
    let net = OpenNet::new_with_config(config)?;
    let doctor = net.quality_doctor().ok_or("diagnostics are disabled")?;
    let client = net.create_ws_client("ws-quality").await?;
    let session = client.connect(url).await?;

    let receipt = session.sender().enqueue("hello").await
        .map_err(|failure| failure.into_parts().1)?;
    let reference = receipt.quality_ref();
    let write_result = receipt.written().await;
    if let Some(reference) = reference {
        println!("{:?}", doctor.lookup(&reference));
    }

    session.close().await?;
    net.shutdown().await?;
    let query = QualityQuery::default()
        .with_protocol(QualityProtocol::WebSocket)
        .with_kind(QualityOperationKind::WsMessage);
    println!("{}", doctor.snapshot(query)?.to_json()?);
    write_result?;
    Ok(())
}
```

`written()` confirms the existing local write contract. It does not acknowledge
peer receipt or processing. Request APIs expose their own references on request
handles, receipts, and responses; incoming messages can expose receive-report
references. A logical retried request and each of its request attempts are
distinct reports. Select an operation kind before aggregating them.

## Configure bounded storage and sampling

`QualityDoctorConfig::default()` uses `QualityMode::Off`. `basic()` and
`detailed()` use the same default limits and policy, changing the collection mode.
All builders return the updated value; they do not validate immediately. Call
`validate()` for a diagnostic `QualityError` before construction, or let the
engine/client constructor validate and report invalid transport configuration.

| Setting | Default | Meaning and accepted values |
| --- | --- | --- |
| Mode | `Off` | `Off` has no collector. `Basic` retains major phase observations. `Detailed` also enables sampled detail and interval lifecycle starts. |
| Protocols | Compiled HTTP and WebSocket transports | `QualityProtocols::http()`, `websocket()`, `new(http, websocket)`, or `none()`. Selecting an uncompiled protocol is invalid. Selecting none is allowed. |
| Operation detail rate | `1.0` | `QualitySampling::new` first argument; a finite value in the inclusive range `0.0..=1.0`. Used for all kinds except `Connection`. |
| Connection detail rate | `1.0` | Second sampling argument; same range, applied to `Connection` reports for either transport. |
| Slow phase threshold | 500 ms | `QualityPolicy::with_slow_phase_threshold`; strictly positive. A retained completed interval at or above this duration can produce a finding. |
| Failure ratio threshold | `0.2` | `with_failure_ratio_threshold`; finite and in `0.0..=1.0`. The precise denominator is described below. |
| Minimum samples | 20 | `with_min_samples`; inclusive range `1..=8192`. Gates percentiles and failure-ratio findings, but not individual slow-phase findings. |

Sampling selects a whole report using its process-local scope and report ID.
It does not independently sample phase events. Basic observations are still
attempted for operations not selected for detail, including when the detail
rate is zero. Sampling does not guarantee an exact percentage in a small window
or identical selections across process runs.

Detailed-only phase kinds are `TcpCandidate`, `RequestGetters`,
`ResponseMatched`, `ManualDispatch`, and `ManualResolve`. A sampled Detailed
report also records interval starts for its other phase kinds. Basic and
unsampled Detailed reports retain supported completed major intervals and
milestones, but no `phase_activity()` lifecycle entries.

### Limit defaults and validation

All capacities are counts of diagnostic objects, not byte limits. Zero never
means unlimited. `QualityLimits` getters expose the configured values.

| Builder | Default | Inclusive range or constraint | What it bounds |
| --- | --- | --- | --- |
| `with_ingress_capacity` | 1024 | `1..=65536` | Pending observation events awaiting a diagnostic reader. |
| `with_active_capacity` | 256 | `1..=4096` | Concurrent producer reservations and the collector's in-progress report population. Requests can continue without a probe when this is full. |
| `with_history_capacity` | 256 | `1..=4096` | Retained terminal reports; also the bounded evidence used to recognize recent evictions. |
| `with_max_phases` | 64 | `1..=512` | Logical intervals and milestones per report. Each phase view has at most this many entries. |
| `with_max_subscriptions` | 4 | `1..=64` | Live diagnostic subscriptions. |
| `with_subscription_capacity` | 128 | `1..=4096` | Maximum queue capacity requested by any one subscription. |
| `with_max_query_results` | 128 | `1..=8192` | Maximum reports and maximum findings returned by a query. Each collection is bounded separately. |
| `with_retention` | 300 s | Strictly positive, at most 24 hours | Expiry of in-progress observation storage and retention of terminal reports. |
| `with_window` | 60 s | Strictly positive and no greater than retention | Default snapshot lookback; does not extend retention. |

Two combined budgets are also validated:

```text
(active_capacity + history_capacity) * max_phases <= 262144
max_subscriptions * subscription_capacity * max_phases <= 262144
```

These are logical phase budgets. A sampled Detailed interval can occupy an entry
in both `phases()` and `phase_activity()`, so the two views together can retain up
to twice the configured phase count. A start and its matching end consume one
logical allowance. Additional milestones consume their own allowances.

For example, configure shorter retention, a larger history capacity, and sampled
detail while retaining the default policy:

```rust
use open_net::quality::{QualityDoctorConfig, QualityLimits, QualitySampling};
use std::time::Duration;

fn diagnostics_config() -> Result<QualityDoctorConfig, open_net::quality::QualityError> {
    let config = QualityDoctorConfig::detailed()
        .with_limits(QualityLimits::default()
            .with_ingress_capacity(2048)
            .with_history_capacity(512)
            .with_retention(Duration::from_secs(120))
            .with_window(Duration::from_secs(30)))
        .with_sampling(QualitySampling::new(0.1, 1.0));
    config.validate()?;
    Ok(config)
}
```

Finishing a producer releases its active reservation even if the application
keeps a receipt. Expiring its stored report alone does not release a reservation
for a producer that has not finished. Retention and history maintenance occur
when a diagnostic reader drives collection; no expiry worker runs in the
background. An in-progress record becomes `Expired` when its age reaches
retention. Its diagnostic finish time is then recorded, after which it is subject
to terminal-history retention and capacity eviction. A finished record's
retention age uses its diagnostic finish time.

## Query, look up, and subscribe

`QualityDoctor` is a cloneable read-only handle. It retains diagnostic storage,
not the engine, network workers, or sessions. Snapshots and reports are immutable
copies of retained observations; holding one does not cause it to update.

`snapshot`, same-scope `lookup`, `QualitySubscription::try_recv`, and `recv` drain
pending events, maintain retention, and publish subscription updates on their
caller's execution path. `state()` and `capabilities()` do not drain events.
Multiple readers share one collector and its bounded storage.

### Snapshot filters and ordering

`snapshot(QualityQuery::default())` includes all retained kinds and protocols in
the configured default window, subject to the output limit. Use explicit filters
for comparable statistics:

```rust
use open_net::quality::{
    QualityDoctor, QualityError, QualityOperationKind, QualityProtocol, QualityQuery,
};
use std::time::Duration;

fn inspect_http(doctor: &QualityDoctor) -> Result<(), QualityError> {
    let snapshot = doctor.snapshot(QualityQuery::default()
        .with_protocol(QualityProtocol::Http)
        .with_kind(QualityOperationKind::HttpRequest)
        .with_window(Duration::from_secs(30))
        .with_limit(32))?;
    println!("retained={}, returned={}, truncated={}",
        snapshot.summary().retained(), snapshot.reports().len(), snapshot.results_truncated());
    println!("successful result latency: {:?}", snapshot.summary().p95());
    Ok(())
}
```

`with_client_id` accepts the numeric client ID found in a report from the same
doctor. For WebSocket it is also available from `WebSocketClient::id().as_u64()`;
HTTP uses a separate diagnostic allocator. Combine this filter with
`with_protocol` to avoid selecting the same number from both populations. The
number is not a client name, URL, application ID, or cross-process key. All
specified filters are combined. Querying a protocol with no retained reports
returns an empty population.

The window includes a report when the age of its `finished_offset()` is no greater
than the window, or uses `started_offset()` while no finish exists. Thus a
long-running report can fall outside a short window and become visible again
when it finishes. The window is positive and cannot exceed configured retention.
The requested limit must be between one and `max_query_results`; invalid values
return `InvalidQuery` instead of being silently clamped.

Reports are returned in reverse order of their retained begin records, not by
finish time or latency. `summary()` covers **all matching retained reports**,
including those omitted from `reports()` by the limit. Findings are calculated
over that same population and separately limited. `results_truncated()` indicates
that matching reports exceeded the report limit; it does not separately flag
discarded findings. Counters and recent loss summaries are collector-wide and
are never narrowed by report filters or the query window.

### Reference lookup

`quality_ref()` returns `Option<QualityReportRef>`. Absence can mean Off or dormant
collection, a disabled protocol, an operation that failed before instrumentation,
exhausted diagnostic capacity, or a dropped begin event. It does not imply
network success or failure.

A reference contains no request data and does not own or cancel an operation.
`lookup(&reference)` returns a `QualityLookup`, not a `Result`:

| Variant | Interpretation |
| --- | --- |
| `Available(report)` | A copy of the retained report after pending observations were processed. Check its observation state and completeness. |
| `Evicted` | Bounded retained evidence proves that this record was removed from history. The evidence itself can later be evicted. |
| `NotRetained { owner_state }` | No retained evidence distinguishes event loss, older eviction, or absence of a record. Also inspect the supplied collector state. |
| `WrongScope` | The reference belongs to another doctor scope; this collector is not searched. |

Do not poll `NotRetained` as proof that an operation is still running, and do not
treat it as a duration of zero. A previously returned `Arc<QualityReport>` remains
usable after collector eviction but does not make subsequent lookups available.

### Subscription delivery

`subscribe` returns a bounded receiver. Create it before the activity you want to
watch; it does not replay already-processed history or emit an initial snapshot.
Pending events not yet processed can still produce updates after subscription.

`QualitySubscriptionOptions::default()` uses a capacity of 64, all report kinds
and protocols, no snapshot updates, and a 25 ms poll interval. The requested
capacity must be positive and no greater than the configured subscription
capacity. If you lower that configured limit below the receiver default, also
set `with_capacity` on the subscription. The poll interval must be between 1 ms
and 60 s, inclusive.

```rust,no_run
use open_net::quality::{
    QualityDoctor, QualityError, QualityOperationKind, QualityQuery,
    QualitySubscriptionOptions, QualityUpdate,
};
use std::time::Duration;

async fn watch_requests(doctor: &QualityDoctor) -> Result<(), QualityError> {
    let options = QualitySubscriptionOptions::default()
        .with_capacity(32)
        .with_filter(QualityQuery::default().with_kind(QualityOperationKind::HttpRequest))
        .with_snapshot_updates(true)
        .with_poll_interval(Duration::from_millis(50));
    let mut updates = doctor.subscribe(options)?;
    while let Some(update) = updates.recv().await? {
        match update {
            QualityUpdate::ReportCompleted(report) => {
                println!("{:?}: {:?}", report.reference(), report.state());
            }
            QualityUpdate::Gap(gap) => {
                println!("diagnostic loss: {:?}, {} items", gap.scope(), gap.dropped());
            }
            QualityUpdate::Closed => break,
            _ => {}
        }
    }
    Ok(())
}
```

Run a consumer such as this concurrently with network work if you need ongoing
updates. QualityDoctor does not spawn that application task for you.

| Update | Meaning |
| --- | --- |
| `ReportUpdated` | Latest retained report after a processed batch while its observation remains in progress. Individual events can be coalesced. |
| `ReportCompleted` | Report observation is terminal, including normal completion, expiry, or owner closure. This is not necessarily a successful network operation. Later loss evidence can emit another revision of a terminal report. |
| `SnapshotUpdated` | Aggregate snapshot produced when collection changes, if requested. `reports`, `findings`, and `recent_drops` are empty; `results_truncated` is true whenever the summary retains any reports. Call `snapshot` for details. |
| `Gap` | Some ingress events or subscription updates were lost. `scope()` distinguishes those two populations; `dropped()` is their count, not a request count. |
| `Closed` | The collector stopped accepting observations. A full subscriber queue can prevent delivery of this marker; eventual receiver closure still terminates reads. |

The subscription's protocol, client, and kind filters select report updates.
Its query window selects the aggregate snapshot population, not individual
report delivery. The query limit is validated but does not limit report
notifications; aggregate updates omit their report/finding lists regardless of
the limit. Gap and close notifications describe the collector/receiver and are
not filtered to a particular request population.

`try_recv()` drives collection synchronously and returns either an update,
`QualityTryRecvError::Empty`, or `Closed`. It needs no application timer.
`recv().await` returns `Ok(Some(update))`, `Ok(None)` after closure and draining,
or a diagnostic error. While an empty receiver waits, it uses the caller's Tokio
time driver and configured polling interval. No persistent SDK consumer task is
created, and producers do not invoke subscriber wakers. A missing runtime or
time driver returns `RuntimeUnavailable` when waiting is needed. Timer-panic
recovery requires Rust's unwinding behavior; an aborting panic cannot be caught.

Calling `unsubscribe()` or dropping the receiver removes it from the collector;
this never cancels network operations. Explicit `unsubscribe()` makes subsequent
reads closed rather than offering a queue-drain API. A slow receiver loses
diagnostic updates instead of applying backpressure to network operations. A
subscription gap is best-effort notification, not guaranteed delivery of every
loss count.

### Diagnostic errors and owner state

| `QualityError` | When to handle it |
| --- | --- |
| `InvalidConfig(field)` | Invalid diagnostic limits, sampling/policy values, protocol selection, or subscription options. The string names a safe validation category. |
| `InvalidQuery(field)` | Invalid query window or result limit. |
| `Capacity` | No subscription slot or subscription identity is available. Producer capacity loss is counted separately and does not fail a network request. |
| `OwnerClosed` | Attempt to create a new subscription after owner closure. |
| `CollectorFailed` | The collector cannot safely provide the requested operation, for example after an internal synchronization failure. |
| `ExportFailed` | Explicit JSON serialization failed. |
| `RuntimeUnavailable` | An asynchronous subscription wait requires an available Tokio runtime and time driver. |

`QualityDoctorState::Running` means collection accepts observations. `Closed`
means owner shutdown or destruction closed ingress; `Failed` means collection
became unavailable. Failure takes precedence if both conditions exist. `Dormant`
is a public state concept for unactivated diagnostics; obtaining a doctor through
an accessor activates it, so normal handles report Running, Closed, or Failed.

Shutdown does not wait for subscribers. A surviving doctor can still read
retained facts after closure; `snapshot()` does not reject solely because the
owner is closed. Reading drains accepted events and closes outstanding
observations, including accepted events processed in a later batch. `lookup`
reports insufficient evidence through `NotRetained` and its owner state.

## Report data dictionary

All time offsets use a monotonic collector epoch established when that doctor is
first activated. They are `Duration` values, not wall-clock timestamps. Reports,
phases, and loss summaries in the same scope share that epoch. To display a
report-relative phase position, subtract `report.started_offset()` from the phase
offset. Do not compare offsets from different scopes or process runs.

### Identity and observation population

`QualityReportRef::scope()` is a process-local collector identity; `id()` is the
record identity allocated within it. Use both to identify a report.
`kind()` identifies the population below. `detail_selected()` records whether
this whole report was selected for Detailed capture; it does not guarantee that
all selected facts survived. References are not application request IDs or
physical-connection IDs.

| `QualityOperationKind` | Source and boundary |
| --- | --- |
| `HttpRequest` | Buffered or queued HTTP operation, including its SDK retry/redirect activity and observed result-delivery cleanup. |
| `HttpStream` | HTTP streaming operation, including observed body lifecycle after headers. |
| `WsMessage` | One WebSocket message operation observed from admission through its existing result and delivery boundaries. |
| `WsRequest` | One WebSocket request attempt, with existing response matching and local writing evidence. |
| `WsRequestOperation` | Logical WebSocket request operation that can manage multiple request attempts. Child request reports can refer to it through `parent()`. |
| `WsHeartbeat` | Observation of an existing WebSocket heartbeat exchange; it does not initiate another heartbeat. |
| `WsReceive` | One incoming WebSocket text or binary message through SDK routing/dispatch boundaries. Ping, Pong, and Close frames do not create this report kind. |
| `Connection` | One HTTP connector invocation or WebSocket connection-attempt observation, not a persistent connection's full lifetime. |
| `Heartbeat` | Generic public heartbeat category; the WebSocket adapter emits `WsHeartbeat`. Do not assume every enum variant has an active producer. |

`OperationReport` and `ConnectionReport` are aliases of `QualityReport`; their
layout does not impose a population filter. Always inspect the reference kind
and protocol when consuming them.

### Report getters, sources, and absence

| Getter | Value source and interpretation |
| --- | --- |
| `reference()` | Opaque reference allocated when a producer begins an observed operation. |
| `protocol()` | HTTP or WebSocket adapter that emitted the begin observation. |
| `client_id()` | HTTP: owner-allocated diagnostic identity. WebSocket: existing `ClientId::as_u64()` captured by the session runtime. Interpret with scope and protocol because these allocation domains can overlap; no client name or address is retained. |
| `revision()` | Collector-wide revision assigned when this report last changed. Revisions can skip values because other reports, counters, or retention also change. They are not per-report event counts. |
| `started_offset()` | Timestamp of the accepted producer begin event, measured from the collector epoch. Earlier API work outside that instrumentation is not included. |
| `result_offset()` | Timestamp of the first retained result-selection event. `None` means no such event was retained, regardless of the underlying network result. Even `QualityOutcome::Unknown` can have a retained result timestamp. A transport stream normally selects its result at body termination rather than header return. |
| `finished_offset()` | Producer finish timestamp for normal completion; reader-side closure/expiry time for `OwnerClosed` or `Expired`. `None` means the observation has not acquired a finish boundary. |
| `result_duration()` | `result_offset - started_offset`, using saturating subtraction. `None` follows missing result evidence. This excludes later delivery and cleanup. |
| `duration()` | `finished_offset - started_offset`, using saturating subtraction. For expired/closed observations this is diagnostic age, not verified network duration. |
| `outcome()` | First retained selected outcome; never replaced by later callbacks, cleanup, or late write errors. `None` means no retained result event. |
| `http_status()` | Numeric status attached to the selected HTTP result, not merely to receipt of headers. `None` for non-HTTP reports or when no status-bearing result was retained. |
| `http_version()` | `QualityHttpVersion` mapped from the most recently observed dependency response head. It describes an actual observation, not configured protocol preference. It may describe a redirect response if a later hop fails. `None` means no retained response-head protocol observation; `Unknown` means an observed protocol value was not recognized. |
| `failure()` | Optional `QualityFailure` copied from safe `NetError` kind/stage fields. Some failed observations, such as an opaque connector failure, have no structured failure detail. |
| `state()` | Observation lifecycle described below; independent of the selected network outcome. |
| `detail_state()` | `BasicMode`, `Selected`, or `NotSampled`, derived from mode and whole-report sampling at begin. |
| `phases()` | Retained completed intervals and milestones in observation application order. They are not guaranteed to be chronological by start time. |
| `phase_activity()` | Detailed sampled interval lifecycle entries. Milestones are excluded. Empty for Basic and unsampled reports. |
| `active_phases()` | Iterator over lifecycle entries whose retained state is `Running`; it can be stale when evidence was lost. |
| `associations()` | Latest retained report-level identities supplied by transport adapters, described below. |
| `payload_bytes()` | Latest retained cumulative adapter payload counter. HTTP counts consumed response-body bytes; WebSocket send/request attempts count prepared outbound message bytes and receive reports count incoming message payload bytes. This excludes headers, frame overhead, TLS, retransmissions, and request-upload accounting for HTTP. Connection/heartbeat/logical-parent records may have no byte contribution. |
| `dropped_phases()` | Retained count of logical phase observations omitted because the phase allowance or view capacity was exhausted. It does not count ingress drops, sampling exclusions, or unsupported phases. |
| `possibly_incomplete()` | Conservative evidence that expected observations may be missing: ingress loss, phase exhaustion, unclosed intervals, expiry, owner closure, or completion without a known result can set it. It is not a request-failure flag. |
| `counters_saturated()` | A retained report-local numeric accumulation overflowed and was clamped. Treat affected counts as lower bounds. |
| `schema_version()` | Export-format discriminator supplied by the library. Consumers should inspect it rather than infer the format from package metadata. |

Byte increments do not emit an event per chunk. The cumulative count is carried
by subsequent observations and the report takes the greatest retained value.
Consequently a queried count can lag active I/O, and lost terminal events can
leave a partial total. Phase-overflow counts likewise arrive with subsequent
retained events. Zero means no byte contribution was retained; it cannot prove
no network bytes were sent. Never add parent and child reports as though they
represent disjoint business traffic.

`QualityFailure::kind()` is the retained `ErrorKind`; `stage()` is the optional
stage already attached to that error. A missing stage is unclassified, not a
successful stage. Failure does not retain an arbitrary error message, nested
source, panic payload, or application context.

### Outcome and observation state are different

| Outcome | Interpretation |
| --- | --- |
| `Success` | The adapter selected success according to its API boundary. HTTP status and remote WebSocket processing still require application interpretation. |
| `Failed` | An observed failure other than the recognized cancellation/timeout cases. |
| `Cancelled` | An explicit `ErrorKind::Cancelled` result was observed. Dropping an arbitrary future does not automatically prove this outcome. |
| `Timeout` | A timeout or deadline-exceeded result was observed, or an existing heartbeat expired. |
| `Abandoned` | An HTTP connector observation was dropped before its result was selected. It does not prove that the initiating request failed. |
| `Unknown` | The adapter closed an observation without establishing its network result, such as a dropped WebSocket observation scope. |

`QualityOutcome::from_error` maps cancellation to `Cancelled`, timeout and
deadline-exceeded errors to `Timeout`, and other kinds to `Failed`.

| Observation state | Interpretation |
| --- | --- |
| `InProgress` | Begin was retained but no terminal observation boundary has been processed. A selected result can already exist while delivery or cleanup continues. |
| `Completed` | Producer finish was retained. Check outcome and completeness separately; completion does not promise all phases or a known result. |
| `Expired` | A reader found an unfinished observation older than retention. No network cancellation is implied. |
| `OwnerClosed` | A reader closed an unfinished observation after collector closure or failure. Check the doctor's state to distinguish those causes. |

### Attempt, connection, and parent associations

All `QualityAssociations` fields are optional. `None` means not applicable, not
yet observed, unsupported, or lost; it does not encode an attempt count of zero.
Values come from the existing transport lifecycle, not from parsing URLs,
payloads, application request IDs, or network addresses.

| Getter | Source and scope |
| --- | --- |
| `attempt()` | HTTP SDK attempt number, starting at one; WebSocket raw-message write-attempt or logical-request retry number, also starting at one; WebSocket connection handshake-attempt identity, starting at zero within a connection cycle. A child `WsRequest` report need not contain the parent's retry number. These populations do not share one numbering scheme. |
| `hop()` | HTTP redirect hop within an attempt: the original request uses zero and followed redirects increase it. Retries reset the hop. HTTP connector reports copy the initiating operation's attempt/hop when available. |
| `connection()` | Existing WebSocket connection generation/identity associated with the operation or successful connection attempt. It is scoped to the relevant client/session lifecycle. Exact selected HTTP connection identity is unavailable. |
| `session()` | Existing WebSocket session identity attached by its runtime, including receive, send, connection, and heartbeat observations where applicable. |
| `cycle()` | WebSocket connection/reconnection cycle generation. Multiple handshake attempts may belong to one cycle. |
| `operation()` | Existing WebSocket operation ID when bound to an observed operation control. It is local to its session and not an application request ID. |
| `parent()` | Related report reference: for example the HTTP operation that initiated a connector invocation, or the logical WebSocket request owning a child attempt. Parentage is diagnostic association, not proof that a connector carried the request. |

Report associations keep the latest retained value for each field. Phase
associations capture the numeric identities when the interval starts or the
milestone is emitted. A phase therefore retains its original attempt/hop even
after a retry or redirect updates the report. Parent linkage is recorded at the
report level; phase association snapshots do not copy that parent reference.

## Read phases and current activity

`QualityPhase::instance_id()` is unique within its report, including repeated
intervals and milestones. Use `(report reference, instance_id)` to identify a
phase across snapshots. Two phases of the same kind can belong to different
attempts or overlap, and have different IDs.

In `phases()`, `start_offset()` and `end_offset()` use the collector epoch.
`end_offset() == None` identifies a **milestone**; its `duration()` is also
`None`. It is neither a running interval nor an interval of zero length. An
interval has a retained end and `duration()` is its saturating end-minus-start.

In `phase_activity()`, the same instance ID identifies the lifecycle of a
sampled interval. Here `end_offset() == None` means **no retained interval end**.
`start_observed()` records whether its start event was retained. An end event
carries the original start time and associations, so an interval can be recovered
from its end with `start_observed() == false`; the report remains incomplete.

| `QualityPhaseState` | Meaning |
| --- | --- |
| `Running` | A start was retained without an end or report closure. This is a diagnostic view, not proof of live underlying work. |
| `Ended` | The interval's observation guard ended, including by error, cancellation, or future drop. The state says nothing about success. |
| `ReportCompleted` | The report finished without a retained end for this interval. |
| `ReportExpired` | Retention expired without a retained end for this interval. |
| `OwnerClosed` | Collection closed or failed without a retained end for this interval. |

The last three states do not synthesize an end timestamp or establish that the
underlying operation stopped. Both phase views describe the same work and must
not be added together. Nested or parallel phases also must not be summed as an
end-to-end duration. Empty activity can result from mode, sampling, limits, event
loss, or unsupported stages.

### Phase boundaries by transport

The following map explains phase names. Actual availability is determined by
`doctor.capabilities()` and the execution path; not every supported phase occurs
in every operation.

| Phase(s) | Observed boundary |
| --- | --- |
| `Admission` | SDK admission scope before work is accepted; errors and cancellation can end it. |
| `PermitWait`, `QueueWait` | HTTP waits for admission permits and queue capacity, including the streaming supervisor lane where applicable. They do not measure socket-pool acquisition. |
| `Published`, `Dequeued`, `WorkerStarted` | SDK publication boundary, removal from a queue, and HTTP worker start milestones. Published can be emitted immediately before the reserved queue slot is filled. Their gaps include local scheduling. |
| `Preparation`, `RequestGetters`, `RequestBuild` | HTTP preparation, calls to request getters, and request construction. Getters are detail-only. WebSocket request construction also uses RequestBuild. |
| `Provider`, `Prepared` | WebSocket connection/request provider work and prepared-work milestone. Provider time can include application code. |
| `ExecuteToHeaders`, `ResponseHeaders` | HTTP dependency execution until a response head is returned, and its arrival milestone. Execution includes acquisition, sending, peer delay, and local scheduling. |
| `FirstBodyChunk`, `ResponseBody` | First nonempty HTTP SDK body chunk and the body-consumption interval. Consumer pauses and scheduling can contribute. |
| `ReportFrozen`, `ResponseQueue`, `CallbackQueue`, `Callback` | HTTP result/report freeze and response/callback dispatch boundaries. CallbackQueue marks a queueing attempt, not proof that delivery succeeded. Callback timing covers the SDK-invoked callback boundary, not unrelated asynchronous application work. |
| `ResourcesReleased`, `CompletionPublished`, `DeliveryFinished` | Observable cleanup, HTTP completion publication, and result-delivery milestones. They can occur after result selection. |
| `ResultSelected` | WebSocket selection of an operation result. The first accepted result is preserved despite later delivery/write observations. |
| `Connector`, `ConnectorPolled` | Total HTTP connector invocation and first polling of its observed future. A connector can compete with another connection or pool reuse. |
| `NetworkWait`, `Backoff`, `ConnectionAdmission` | WebSocket network-availability, existing retry/reconnect delay, and connection-attempt admission. HTTP retry backoff is also observed. |
| `Dns` | Overall WebSocket system lookup call; resolver queueing and actual resolution are not separated. A literal address can still pass through this boundary without a remote DNS exchange. |
| `TcpConnect`, `TcpCandidate` | WebSocket aggregate TCP connection race and sampled candidate intervals. Candidate success alone does not mean it was selected. |
| `ProxyTls`, `ProxyConnect`, `Tls`, `WebSocketUpgrade` | WebSocket proxy TLS, proxy tunnel negotiation, target TLS, and WebSocket handshake boundaries when required by configured transport policy. |
| `SinkReadiness`, `StartSend`, `Flush`, `Written` | WebSocket sink readiness interval, local submission/flush, and existing written milestone. SinkReadiness can include the synchronous start-send attempt; StartSend marks that attempt rather than confirming delivery. These do not acknowledge remote processing. |
| `ResponseMatched`, `ManualDispatch`, `ManualResolve` | Detailed WebSocket protocol matching, manual response dispatch, and resolution-retirement milestones. ManualDispatch can occur when no receiver is present and does not guarantee delivery. ManualResolve can occur for a stale or failed resolution path and does not certify successful matching. Application processing can contribute to manual-resolution delay. |
| `Requeue` | WebSocket operation transition toward requeueing; the subsequent queue push can still fail. This does not introduce a diagnostic retry. |
| `Heartbeat` | Existing WebSocket heartbeat response-match milestone; no extra heartbeat is sent. |
| `PayloadReceived`, `ResponseRouting` | Complete WebSocket text/binary message observation and synchronous SDK response-routing boundary. This is not the arrival time of the first TCP byte. |
| `ResponseWait` | Public phase vocabulary without an instrumented boundary in the adapters. Consult capabilities rather than assuming it is emitted. |

WebSocket response matching can win before local flush finishes. A later write
error must not rewrite that accepted response. Connection reports observe
attempts, not how long an established connection stays open. Manual dispatch and
manual resolution can be milestones on different receive/request reports, so
their timestamps require explicit association and appropriate interpretation.

## Summary calculations and findings

Summary counts are reports, not business requests. A single operation can
produce a logical parent, attempts, receive records, and connection records.
`protocols()` and `operation_kinds()` identify the distinct populations present;
`mixed_protocols()` and `mixed_kinds()` report whether more than one is present.

| Summary getter | Calculation over all retained reports matching the query |
| --- | --- |
| `retained()` | Number of matching records, including observations still in progress. |
| `completed()` | Records in observation state `Completed`; excludes Expired and OwnerClosed. |
| `successes()`, `failures()`, `timeouts()`, `cancelled()` | Counts of the corresponding selected outcomes, even if diagnostic delivery/cleanup has not completed or the report is incomplete. Unknown, abandoned, and absent outcomes are not assigned to these counters. |
| `incomplete()` | Records marked `possibly_incomplete`, or in Expired/OwnerClosed state. An ordinary in-progress record is not automatically incomplete. |
| `latency_samples()` | Successful records that are Completed, are not marked incomplete, and have a result duration. Sampling of phase detail does not by itself remove a report from this population. |
| `p50()`, `p95()`, `p99()` | Nearest-rank percentiles of those successful result durations. They are `None` for mixed protocols, mixed kinds, or fewer than the configured minimum eligible samples. |

For `n` sorted eligible durations, percentile `p` uses index
`ceil(n * p / 100) - 1`. This calculation uses result latency, not total
observation duration; callback and cleanup latency after selection is excluded.
The sample count remains visible even when mixed populations prevent percentiles.
A missing percentile never means zero latency. Counters describe retained
coverage rather than a traffic-wide success rate: loss, expiry, and eviction can
bias the population.

`QualityPolicy` creates descriptive findings only. It cannot change timeouts,
retries, routes, or connection settings.

| Finding kind | Rule and fields |
| --- | --- |
| `SlowObservedPhase` | At most one per matching report: the longest retained completed interval whose duration is at least the configured slow threshold. `reference` and `phase` identify it; `samples` is one; `observed_duration` and `threshold` are populated. Milestones and unfinished intervals do not qualify. Incomplete reports can still produce this finding. |
| `HighObservedFailureRatio` | For a single protocol/kind population, `(failures + timeouts) / (successes + failures + timeouts)` meets or exceeds the policy threshold and the denominator meets the minimum samples. Cancellation, abandonment, unknown, and absent outcomes are excluded. Completion and completeness are not required for these selected-outcome counts. `samples` is the denominator; phase, reference, duration, and duration threshold are absent. |
| `IncompleteCoverage` | Matching reports include incomplete observations, or collector-wide cumulative ingress loss or eviction is nonzero. `samples` is the matched retained count; other optional fields are absent. It can appear even when the particular query has no incomplete records. |

Findings have a bounded output list. Slow-phase findings are considered while
reports are traversed; aggregate findings are appended if space remains. Absence
of a finding is therefore not proof of good coverage or healthy traffic. Inspect
the summary, counters, configured policy, and capabilities as well.

## Loss counters and recent drop evidence

`snapshot.counters()` contains cumulative collector-wide counts since activation,
not deltas for the query window. Compare snapshots from the same scope for
interval trends. No counter is a substitute for a transport error counter.

| Counter | Source and meaning |
| --- | --- |
| `begin_attempted()` | Attempts to begin observations after active collector/protocol gating. Includes attempts later skipped by diagnostic capacity or identity exhaustion. |
| `begin_enqueued()` | Begin events successfully placed on ingress, after reserving active capacity and a report ID. |
| `records_observed()` | Begin events processed into retained records by a reader. Can lag enqueued begins. |
| `active_capacity_skipped()` | Begin attempts refused because all producer reservations were in use. No report/reference is created for that attempt. |
| `id_exhausted()` | Begin attempts unable to allocate another report identity. Identities never intentionally wrap. |
| `ingress_dropped()` | Sum of lock-contention and full-queue event drops. It excludes closed/failed rejections. |
| `ingress_lock_contended_dropped()` | Event submissions whose non-waiting ingress lock acquisition failed because another user held the lock. |
| `ingress_queue_full_dropped()` | Event submissions rejected because the ingress queue was full. |
| `ingress_closed_rejected()` | Actual event submissions rejected after owner closure. |
| `ingress_failed_rejected()` | Actual event submissions rejected because the collector failed, including a poisoned ingress lock. |
| `ingress_high_watermark()` | Largest observed ingress queue length since activation. It is neither current backlog nor a byte count and cannot exceed ingress capacity. |
| `drop_summaries_lost()` | Best-effort loss summaries that could not be enqueued separately. Aggregate ingress counts still apply. |
| `orphan_events()` | Non-begin events processed after their report was absent, for example after eviction. |
| `records_evicted()` | Reports removed by retention/capacity maintenance. Expiring an in-progress observation and evicting a report are distinct events. |
| `records_expired()` | In-progress observations changed to Expired when a reader applied retention. |
| `subscriber_dropped()` | Updates that could not be delivered to subscriber queues. Counts update delivery losses, not ingress events or unique reports. |
| `saturated()` | A collector counter reached its representable bound or an accumulation overflowed. Treat cumulative values as lower bounds thereafter. |

Inactive collection and facts suppressed after a producer finished do not count
as rejected event submissions. Counter reads and event draining are separate
operations, so a snapshot is not an atomic census of all concurrent producers.

`recent_drops()` provides additional best-effort evidence only for lock contention
and full-queue loss. Each `QualityDropSummary` contains:

| Getter | Source |
| --- | --- |
| `reference()` | The dropped event's opaque report reference. A dropped Begin can reference a report that was never retained. |
| `event_kind()` | Safe classification: Begin, PhaseStarted, PhaseEnded, Milestone, Result, Association, Parent, Finish, or HttpVersion. |
| `phase_kind()` | Phase name when the dropped event describes a phase; otherwise `None`. |
| `phase_instance_id()` | Per-report interval/milestone ID when applicable; use it with the reference. |
| `reason()` | `LockContended` or `QueueFull`, never inferred from a network error. |
| `event_offset()` | Original event timestamp in the collector epoch, not the time loss was detected or the snapshot was captured. |

Pending and retained loss-summary queues each hold at most
`min(64, ingress_capacity)` entries. Retained summaries expire using the original
event offset and configured retention. They are collector-wide and independent
of snapshot filters and query window. Closed/failed submissions have counters
only. These summaries never re-submit the original event and are not a complete
loss ledger.

Detected ingress loss conservatively marks open reports incomplete and emits an
ingress Gap. Because producers and readers race, this conservative marking also
covers begins in the next drained batch. A marked report need not have lost an
event itself. A later summary can identify loss on an already-completed report,
advance its revision, and mark it incomplete; it cannot replace its selected
network outcome or invent a missing phase end.

## Capability boundaries

`doctor.capabilities()` returns the doctor's protocol/phase capability matrix.
Each `QualityCapability` supplies `protocol()`, `phase()`, `level()`, and
`reason()`. This is a configuration/adapter declaration rather than a guarantee
that a specific operation contains every phase.

Levels distinguish `Supported`, `Partial`, `Unsupported`, and
`DisabledByConfig`. Instrumented SDK boundaries are generally Partial because
their duration includes scheduling or another boundary-specific limitation.
Reasons distinguish SDK boundaries, opaque dependencies, competing connection
candidates, consumer-driven bodies, protocol/feature selection, aggregate system
resolution, detail selection, and phases that are not instrumented. Treat these
non-exhaustive enums as extensible and include fallback match arms.

For HTTP, exact physical-connection selection, pool reuse/acquisition, internal
dependency recovery, and separate DNS/TCP/proxy/TLS timings are unavailable from
the supported dependency hooks. `exact_http_connection_identity()` returns
false. The connector layer observes total connector invocations. An invocation
can lose a pool race or finish after its initiating request was cancelled.
Its parent reference establishes the initiator only. Absence of a connector
report does not establish reuse because collection can also miss data.

`ExecuteToHeaders` minus connector duration is not pure server processing time.
The intervals can overlap with scheduling and other acquisition work. HTTP body
time and first-chunk observation include consumer polling behavior. The phase
lifecycle view cannot supply dependency boundaries that were never observed.

For WebSocket, system lookup timing includes unresolved system work, TCP candidate
races can overlap, and received messages are complete protocol observations.
Local flush, a matched application response, and a heartbeat response are
different delivery evidence. Application processing can contribute to response
matching/resolution timing. `packet_timing()` returns false for all transports.

No report proves use of a particular network interface or a VPN/TUN route.
A direct proxy decision and `NO_PROXY` do not bypass operating-system routing.
Likewise TCP keepalive and HTTP pool idle retention are separate policies; a new
connector after an idle period alone is not evidence of incorrect keepalive.

## Export and privacy

`QualityReport::to_json()` and `QualitySnapshot::to_json()` explicitly serialize
retained diagnostic data and can return `ExportFailed`. There is no automatic
upload, file write, telemetry sink, or application callback. The caller chooses
where exported data is stored and who can access it.

The report JSON keys generally match stored field names rather than getter names:

| Rust accessor | JSON key |
| --- | --- |
| `started_offset()`, `result_offset()`, `finished_offset()` | `started`, `result_at`, `finished` |
| `payload_bytes()`, `dropped_phases()` | `bytes`, `phase_overflow` |
| `detail_state()` | `detail` |
| Phase `start_offset()`, `end_offset()` | `start`, `end` |
| Snapshot `captured_at_offset()` | `captured_at` |
| Finding `observed_duration()` | `duration` |
| Capabilities `version()` | `version` |

`schema_version` identifies report/snapshot export format; capability metadata
has its own format discriminator. Inspect these fields without hardcoding a
package release into your export pipeline. Safe enum values are serialized in
snake case. Optional absent fields serialize as `null`; they do not become zero
or empty strings. `Duration` serializes as an object containing integer `secs`
and `nanos`, with nanoseconds representing the fractional part of a second.
Derived getters such as `result_duration()`, `active_phases()`, and
`mixed_protocols()` do not create extra JSON fields; compute them from the
serialized data if needed. Allow additional fields and handle unknown enum
values according to your consumer's compatibility policy.

Snapshot metadata includes the scope, store revision, captured collector offset,
effective query window, and owner state. A store revision can advance for
counter/retention changes without a network result changing. Capture time is
when the reader constructs the snapshot, not a remote timestamp.

Diagnostic reports, snapshots, and their Debug output contain SDK-generated
identities, numeric timings/counts, HTTP status/protocol observations, and safe
enums. They exclude URLs and request paths/queries, headers, tokens, cookies,
payload contents, proxy credentials, application request IDs, and arbitrary
error/source/panic text. A numeric payload byte count is not a copy of payload
data. This privacy boundary applies to QualityDoctor artifacts; application
logs, transport objects, or separately enabled diagnostics have their own data
contracts. Review any additional application metadata before joining it to an
export.

## Operational cost and tuning

| Collection condition | Cost model |
| --- | --- |
| Feature omitted | Diagnostic API, fields, and probes are compiled out. |
| Feature present, mode Off | No collector, diagnostic queue, per-operation probe, clock sampling, worker, timer, or diagnostic connector observer. Feature-gated conditional fields/checks still exist. |
| Configured but not activated | Lightweight owner/context only. Request paths do not allocate the diagnostic store or start probes. Configured HTTP observation installs a transparent connector wrapper at client construction, which has a small dispatch cost even before activation. |
| Activated Basic/Detailed | Bounded operation-local storage, measurements, atomic updates, and non-waiting ingress submission. Readers perform aggregation, retention, subscription fanout, and export work. Detailed lifecycle capture increases the bounded event/storage cost. |

An enabled observer has measurable CPU, allocation, and timing overhead; it does
not guarantee zero added latency. Producer submission never waits for a reader,
queue capacity, or a user callback. Reader methods can take locks and perform
bounded computation, so place high-frequency queries and export on an
application-selected diagnostic execution path.

Tune against the actual workload:

1. Start with Basic or a low operation detail rate; raise connection detail when
   investigating connection setup. Sampling reduces detail, not the number of
   basic report attempts.
2. Poll or query often enough to drain expected event bursts. Queue fullness
   suggests increasing ingress capacity or reader frequency; lock-contention
   loss can instead indicate overlapping producers/readers. A larger queue does
   not eliminate lock contention.
3. Size active capacity for simultaneous observed operations **and** connection
   attempts, including long streams. `active_capacity_skipped` identifies omitted
   probes; shortening stored retention alone does not release active producers.
4. Increase the phase allowance for retries, redirects, or rich Detailed traces
   only after checking the combined budgets. Use `dropped_phases` separately from
   ingress counters to identify the limiting resource.
5. Size history and retention for the delay between a result and diagnostic
   lookup. A larger query limit cannot recover already-evicted reports, and a
   longer query window cannot extend retention.
6. Keep subscription readers moving. Increase their capacity or reduce snapshot
   notifications when subscriber loss grows; this is independent of ingress
   loss. A faster polling interval consumes more reader CPU and timer activity.
7. Filter by protocol and operation kind before interpreting percentiles or
   failure ratios. Check incompleteness, truncation, and sample count before
   comparing runs.

Bounds cover SDK-held diagnostic data. Application code that retains unbounded
numbers of immutable snapshots, reports, or serialized exports can still use
unbounded memory. Release those artifacts according to your application's own
retention policy. Observe real application latency and resource use when tuning;
diagnostic completeness and transport performance are separate measurements.

# WebSocket message and request retries

[WebSocket setup](README.md) · [Shared retry schedules](../retry.md)

Required feature: `ws-client`. HTTP and WebSocket use the same schedules,
backoff, jitter, decisions, observers and reports, but keep their protocol
classification and replay safety separate. These APIs ship inside `open-net`.
Neither client default retries messages or requests unless configured.
The packaged [ws_request_retry example](../../examples/ws_request_retry.rs)
implements a smaller pipe-separated demonstration protocol and accepts a server
URL plus an optional idempotency key. Use the server contract described in that
file when running it:

These commands run in the matching library source directory containing its
`Cargo.toml`, after obtaining that source. Adding a dependency to an application
does not install the library's examples into that application.

```sh
cargo run --manifest-path Cargo.toml --no-default-features --features ws-client --example ws_request_retry -- ws://127.0.0.1:8081 lookup-42
```

Every complete program below uses these dependency settings:

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false, features = ["ws-client"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

Client examples explicitly select their TLS provider. Configuration-only
examples do not create an engine or connect to a server.

## Choose the operation policy

| Operation | Policy | What shares one attempt budget |
| --- | --- | --- |
| Ordinary message write | `WsSendRetryPolicy` | Initial write and permitted repeated writes |
| Factory-backed request | `WsRequestRetryPolicy` | Initial request, write failure, response/attempt timeout and transient business response retries |
| Connection establishment/recovery | Existing `ReconnectPolicy` | Its existing connection budget, independently of message/request attempts |

`RetrySchedule::max_attempts` includes the first attempt. The first attempt is
immediate; each operation owns its own counters and jitter stream. Waiting for
reconnection still consumes an operation's remaining total time. A factory
request does not nest an independent message-write retry loop: one scheduler
owns its entire budget.

`RetrySetting<P>` is `Inherit`, `Disabled`, or `Policy(P)`. Inherit selects the
matching client default; Disabled permits at most one attempt; Policy replaces
all policy fields. The settings are not merged. `WebSocketClientConfig` has
separate `default_send_retry_policy` and `default_request_retry_policy` fields.

| `WsReplaySafety` | Application guarantee |
| --- | --- |
| `Never` | Default; do not repeat a potentially delivered operation |
| `IdempotentByContract` | Repeating this operation is idempotent by the application's protocol |
| `IdempotencyKey` | The peer deduplicates a stable key encoded in the application's payload |

The SDK cannot infer or inject a key into an opaque message. A declaration does
not verify server behavior. A local write result does not prove application
processing, and cancellation cannot undo a server-side effect.

`WsSendRetryOn` selects write errors and write timeout. `with_write_errors(true)`
selects Io, DeliveryUnknown and NotConnected. `with_errors` replaces that list;
`with_write_timeout` is separate. `WsRequestRetryOn` adds independent response
and whole-attempt timeout switches. `with_attempt_timeout` defaults to false;
enabling write or response timeout retries does not enable attempt timeout retries.
Default reason sets are empty. Configuration, admission, request-build,
cancellation and terminal session/shutdown failures cannot be made retryable by
selecting an error kind or returning Retry.

## Configure policies offline

The example configures explicit three-attempt schedules. Backoff values are
examples, not changed SDK defaults. The request classifier recognizes only a
small illustrative `BUSY` byte response; adapt it to the actual application
protocol.

```rust
use open_net::{
    api::retry::{Backoff, RetryDecision, RetryEvent, RetrySchedule},
    error::ErrorKind,
    ws::{WebSocketClientConfig, WsReplaySafety, WsSendRetryOn, WsSendRetryPolicy,
        WsSendRetryContext, WsRequestRetryOn, WsRequestRetryPolicy, WsRequestRetryContext,
        WsTimeoutPhase},
    NetError,
};
use std::{error::Error, time::Duration};

fn print_timeout(error: &NetError) {
    let label = match error.ws_timeout_phase() {
        Some(WsTimeoutPhase::Admission) => "admission",
        Some(WsTimeoutPhase::Write) => "write",
        Some(WsTimeoutPhase::Response) => "response",
        Some(WsTimeoutPhase::Attempt) => "whole attempt",
        Some(WsTimeoutPhase::Total) => "whole operation",
        Some(_) => "future timeout phase",
        None => return,
    };
    println!("expired {label}; context phase={:?}", error.context().ws_timeout_phase);
}

fn main() -> Result<(), Box<dyn Error>> {
    let schedule = RetrySchedule::builder().max_attempts(3)
        .backoff(Backoff::sequence([
            Duration::from_millis(200), Duration::from_millis(500),
        ])).build()?;
    let send_reasons = WsSendRetryOn::none()
        .with_write_errors(true)
        .with_errors([ErrorKind::Io, ErrorKind::DeliveryUnknown, ErrorKind::NotConnected])
        .with_write_timeout(true);
    println!("selected write errors: {:?}", send_reasons.errors());
    let send = WsSendRetryPolicy::builder().schedule(schedule.clone())
        .retry_on(send_reasons)
        .safety(WsReplaySafety::IdempotentByContract)
        .classifier(|context: &WsSendRetryContext<'_>| {
            if context.error.kind() == ErrorKind::Protocol {
                RetryDecision::Stop
            } else {
                RetryDecision::Default
            }
        })
        .observer(|event: &RetryEvent| println!("write {event:?}"))
        .build()?;
    let requests = WsRequestRetryPolicy::builder().schedule(schedule)
        .retry_on(WsRequestRetryOn::none().with_write_errors(true)
            .with_errors([ErrorKind::Io, ErrorKind::DeliveryUnknown, ErrorKind::NotConnected])
            .with_write_timeout(true).with_response_timeout(true)
            .with_attempt_timeout(true))
        .safety(WsReplaySafety::IdempotencyKey)
        .classifier(|context: &WsRequestRetryContext<'_>| match context.outcome {
            Ok(response) if response.message().as_bytes() == b"BUSY" => RetryDecision::Retry,
            Err(error) => {
                print_timeout(error);
                RetryDecision::Default
            }
            _ => RetryDecision::Default,
        })
        .observer(|event: &RetryEvent| println!("request {event:?}"))
        .build()?;
    println!("write attempts={}, safety={:?}, reasons={:?}, observer={:?}",
        send.schedule().max_attempts(), send.safety(), send.retry_on(), send.observer());
    println!("request attempts={}, safety={:?}, reasons={:?}, observer={:?}",
        requests.schedule().max_attempts(), requests.safety(), requests.retry_on(), requests.observer());
    let config = WebSocketClientConfig::default()
        .with_default_send_retry_policy(send)
        .with_default_request_retry_policy(requests);
    config.validate()?;
    if WsSendRetryPolicy::no_retry().schedule().max_attempts() != 1
        || WsRequestRetryPolicy::no_retry().schedule().max_attempts() != 1
    {
        return Err(std::io::Error::other("default retry budget changed").into());
    }
    Ok(())
}
```

`WsSendRetryContext` contains `error: &NetError` and `retry: &RetryContext`.
`WsRequestRetryContext` contains `outcome: Result<&Response, &NetError>` and
`retry: &RetryContext`. The common context exposes one-based attempt, elapsed time
and remaining total time. Classifiers are synchronous and read-only. Return
Default for built-in classification, Retry for a transient application outcome,
or Stop to veto retries. A classifier may return Retry for an Attempt timeout
even when its built-in switch is false. Retry cannot bypass safety, the attempt
count, Admission or Total expiry, cancellation or shutdown.
Classifier failure terminates the operation; observers are isolated and
cannot change its result. Keep both hooks short and nonblocking.

## Send one ordinary message

A normal message retains one operation identity while its configured write
attempts run. A long retry delay does not occupy the writer or block unrelated
ready messages. Send queue limits, priority, cancellation and disconnection
policy continue to apply.

This example declares a replace-current-status message idempotent by its server
contract. It requires a server that understands that message. The absolute
30-second deadline is computed with checked arithmetic and includes queue and
reconnect waiting; the per-write timeout remains separate.

```rust,no_run
use open_net::{
    api::retry::{Backoff, RetrySchedule, RetrySetting},
    network::{NetworkConfig, TlsConfig},
    ws::{SendOptions, WsReplaySafety, WsSendRetryOn, WsSendRetryPolicy},
    OpenNet,
};
use std::{sync::Arc, time::{Duration, Instant}};

#[tokio::main]
async fn main() -> Result<(), open_net::BoxError> {
    let network = NetworkConfig::default().with_tls(
        TlsConfig::default().with_crypto_provider(Arc::new(rustls_openssl::default_provider())));
    let net = OpenNet::new_with_network_config(network)?;
    let work = async {
        let client = net.create_ws_client("retry-writes").await?;
        let session = client.connect("ws://127.0.0.1:8081").await?;
        session.wait_connected().await?;
        let policy = WsSendRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2)
                .backoff(Backoff::constant(Duration::from_millis(200))).build()?)
            .retry_on(WsSendRetryOn::none().with_write_errors(true))
            .safety(WsReplaySafety::IdempotentByContract).build()?;
        let deadline = Instant::now().checked_add(Duration::from_secs(30))
            .ok_or_else(|| std::io::Error::other("deadline exceeds clock range"))?;
        let receipt = session.sender().message("set-current-status:online")
            .options(SendOptions {
                retry_setting: RetrySetting::Policy(policy),
                deadline: Some(deadline),
                ..Default::default()
            }).enqueue().await.map_err(|rejected| rejected.into_error())?;
        let written = receipt.written().await;
        println!("write report: {:?}", receipt.retry_report());
        written?;
        session.close().await?;
        Ok::<(), open_net::BoxError>(())
    }.await;
    let cleanup = net.shutdown().await;
    work?;
    cleanup?;
    Ok(())
}
```

## Send a logical request with a factory

`session.requests()?` requires Protocol or Manual response routing. A factory
receives `&RequestAttemptContext` with `operation_id()`, `attempt()`,
`request_id()` and `idempotency_key()`. It returns `Result<Message>`. The attempt
number is one-based; the wire request ID changes every attempt, while operation
ID and key stay stable.

This complete program uses an illustrative JSON protocol. Requests carry
`operation_id`, `request_id`, `idempotency_key` and `command`; responses carry the
current `request_id` and `status`. A BUSY response requests another attempt. A
real server must implement that contract and deduplicate by the stable key.
This example does not define a protocol for your application.

```rust,no_run
use open_net::{
    api::retry::{Backoff, RetryDecision, RetrySchedule, RetrySetting},
    network::{NetworkConfig, TlsConfig},
    ws::{ConnectOptions, IncomingMessage, Message, RequestId, ResponseProtocol,
        ResponseRoute, ResponseRouting, WsReplaySafety, WsRequestOperationOptions,
        WsRequestRetryOn, WsRequestRetryPolicy},
    BoxError, NetError, OpenNet,
};
use std::{sync::Arc, time::Duration};

struct JsonReplies;
impl ResponseProtocol for JsonReplies {
    fn route(&self, incoming: &IncomingMessage) -> Result<ResponseRoute, BoxError> {
        let Some(message) = incoming.message() else {
            return Ok(ResponseRoute::Unmatched);
        };
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(message.as_bytes()) else {
            return Ok(ResponseRoute::Unmatched);
        };
        match value.get("request_id").and_then(serde_json::Value::as_str) {
            Some(id) => Ok(ResponseRoute::Final { request_id: RequestId::new(id)? }),
            None => Ok(ResponseRoute::Unmatched),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let network = NetworkConfig::default().with_tls(
        TlsConfig::default().with_crypto_provider(Arc::new(rustls_openssl::default_provider())));
    let net = OpenNet::new_with_network_config(network)?;
    let work = async {
        let client = net.create_ws_client("retry-requests").await?;
        let mut connect = ConnectOptions::new("ws://127.0.0.1:8081");
        connect.routing = ResponseRouting::protocol(JsonReplies);
        let session = client.connect(connect).await?;
        session.wait_connected().await?;
        let policy = WsRequestRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(3)
                .backoff(Backoff::sequence([
                    Duration::from_millis(200), Duration::from_millis(500),
                ])).build()?)
            .retry_on(WsRequestRetryOn::none().with_write_errors(true)
                .with_response_timeout(true))
            .safety(WsReplaySafety::IdempotencyKey)
            .classifier(|context| match context.outcome {
                Ok(response) => match serde_json::from_slice::<serde_json::Value>(response.message().as_bytes()) {
                    Ok(value) if value.get("status").and_then(serde_json::Value::as_str) == Some("BUSY") => RetryDecision::Retry,
                    _ => RetryDecision::Default,
                },
                Err(_) => RetryDecision::Default,
            }).build()?;
        let requests = session.requests()?;
        let builder = requests.request_with_factory("docs-operation-001", |attempt| {
            let value = serde_json::json!({
                "operation_id": attempt.operation_id().to_string(),
                "request_id": attempt.request_id().as_str(),
                "idempotency_key": attempt.idempotency_key(),
                "attempt": attempt.attempt(),
                "command": "query-status",
            });
            serde_json::to_string(&value).map(Message::text).map_err(NetError::protocol)
        })?;
        let options = WsRequestOperationOptions {
            admission_timeout: Some(Duration::from_secs(5)),
            attempt_timeout: Some(Duration::from_secs(8)),
            total_timeout: Some(Duration::from_secs(45)),
            ..Default::default()
        };
        options.validate()?;
        let receipt = builder.options(options).retry_setting(RetrySetting::Policy(policy)).enqueue().await
            .map_err(|rejected| rejected.into_error())?;
        let handle = receipt.handle().clone();
        let result = receipt.response().await;
        println!("operation={}, report={:?}", handle.id(), handle.retry_report());
        let close = session.close().await;
        let response = result?;
        println!("final wire ID length={}, bytes={}, report={:?}",
            response.request_id().as_str().len(), response.message().len(), response.retry_report());
        close?;
        Ok::<(), BoxError>(())
    }.await;
    let cleanup = net.shutdown().await;
    work?;
    cleanup?;
    Ok(())
}
```

The 5/8/45-second limits above are example choices. Existing defaults remain a
10-second write timeout, a 30-second response timeout, and no total timeout.
`WsRequestOperationOptions` owns request timing and scheduling directly; it does
not embed an independently retrying SendOptions. The response timeout origin is
Written by default; Registered includes queue/write time for that attempt.
An optional absolute deadline and relative total timeout both constrain the
operation; the earlier limit wins. Admission, preparation, attempts, backoff and
reconnect waits all consume total time. An asynchronous submission starts its
budget at first poll, not when its unpolled future is created.

| `WsRequestOperationOptions` field | Default and meaning |
| --- | --- |
| `priority: Priority` | Normal; logical requests use the normal message lane |
| `admission_timeout: Option<Duration>` | None; optional SDK admission limit |
| `write_timeout: Duration` | 10 seconds per attempt write |
| `response_timeout: Duration` | 30 seconds per attempt response wait |
| `response_timeout_origin: ResponseTimeoutOrigin` | Written; Registered also includes queue and write time |
| `attempt_timeout: Option<Duration>` | None; optional whole-attempt limit including factory/preparation |
| `total_timeout: Option<Duration>` | None; relative logical-operation budget from first submission poll |
| `deadline: Option<Instant>` | None; optional absolute operation limit; earlier than relative total wins |
| `disconnected: DisconnectedPolicy` | Reject; select WaitForReconnect explicitly if unsent work may wait |
| `cancellation: Option<CancellationGroup>` | None; optional group cancellation |
| `metadata: Metadata` | Empty local metadata; not injected into the wire payload |
| `retry_setting: RetrySetting<WsRequestRetryPolicy>` | Inherit the client logical-request default |

`WsTimeoutPhase` identifies the budget that expired, independently of the work
stage where it expired. It is non-exhaustive: consumers matching variants must
keep a fallback arm. Read it through `NetError::ws_timeout_phase()` or the
`ErrorContext::ws_timeout_phase` field; non-timeout errors may return None.

| `WsTimeoutPhase` | Budget and retry classification |
| --- | --- |
| `Admission` | Admission limit; always terminal |
| `Write` | Per-write limit; selected by `with_write_timeout` |
| `Response` | Per-response limit; selected by `with_response_timeout` |
| `Attempt` | Entire attempt, including factory, queue, write and response; selected only by `with_attempt_timeout` in the built-in classifier |
| `Total` | Relative total limit or absolute deadline; always terminal |

All built-in timeout retry switches default to false. An Attempt timeout keeps
that phase wherever it occurs; it does not become Write or Response according to
the current work stage. Enabling its retry switch does not reset the total budget
or bypass replay safety and the maximum attempt count. A custom classifier may
request an Attempt retry, while Admission and Total remain terminal regardless
of classifier output.

Once the underlying write or response layer has settled a result, a delay in
scheduling the logical runner does not replace that result with an Attempt
timeout. This preserves both successful results and the original error or
timeout phase. For example, if the write layer records a Write timeout at 10 ms,
the attempt deadline is 20 ms, and the logical runner resumes at 30 ms, the
result remains Write while the total budget is still available. An already
settled successful response is likewise not converted into Attempt merely
because the runner resumed late.

The total budget includes logical result processing. If Total actually expires
before the logical operation finishes, it still terminates the operation even
when a lower layer has a ready result. Explicit cancellation also remains
effective until the logical operation reaches its terminal state; preserving a
lower-layer result does not bypass Total or cancellation.

A natural disconnect that ends the session also preserves a result already
selected by the write or response layer, including while its publication is
still pending. It cannot start another attempt on that ended session; a result
that would otherwise retry is retained with `NotRetryable`. Explicit session
closure or client shutdown still terminates the logical operation, even if a
natural-disconnect notification arrives afterward. Total and cancellation remain
effective throughout result processing and publication.

`enqueue()` returns either an accepted receipt or `EnqueueError<Builder>` retaining
the unaccepted factory/options. `send()` is the convenience operation that awaits
the final response directly. Exhaustion retains the last response/error and
report; it does not replace the outcome with an uninformative retry-exhausted error.

## Manual routing and cancellation

When routing manually, compare the wire ID decoded from the incoming response
with the current registration before resolving. A response for an older attempt
must not be paired with a newer registration. The following helpers are complete
program code; call them with an accepted logical handle and its session resolver.

```rust
use open_net::{
    ws::{IncomingMessage, RequestId, ResolveOutcome, ResponseResolver,
        TerminationOutcome, WsRequestOperationHandle},
    Result,
};

fn resolve_current(
    handle: &WsRequestOperationHandle,
    resolver: &ResponseResolver,
    wire_id: &RequestId,
    incoming: &IncomingMessage,
) -> Result<Option<ResolveOutcome>> {
    let Some(registration) = handle.current_registration() else {
        return Ok(None);
    };
    if registration.request_id() != wire_id {
        return Ok(None);
    }
    resolver.resolve(&registration, incoming).map(Some)
}

fn cancel_operation(handle: &WsRequestOperationHandle) -> Result<TerminationOutcome> {
    handle.cancel()
}

async fn send_direct(
    builder: open_net::ws::WsRequestOperationBuilder,
) -> Result<open_net::ws::Response> {
    builder.send().await
}

fn main() {}
```

`current_registration()` returns an owned snapshot, never a borrowed mutable
slot. It is None during backoff or after final retirement. If an attempt retires
between the snapshot and `resolve`, the token is rejected as stale. Cloning a
logical handle preserves its operation identity. Dropping the receipt or response future stops waiting without cancelling the
operation. Use `cancel()` to request cancellation explicitly.

## Keep fixed requests single-attempt

The fixed-payload API cannot rewrite a wire ID encoded inside an arbitrary
Message. It preserves its existing `Request`, `RequestBuilder`, `RequestReceipt`
and `RequestHandle` signatures and performs one attempt. A multi-attempt
send-policy setting is rejected for this API; move replayable operations to the
factory entry point. Existing fixed registration tokens retain their meaning.

```rust
use open_net::{
    api::retry::RetrySetting,
    ws::{Message, Request, RequestClient, RequestId, RequestOptions, Response, SendOptions},
    Result,
};

async fn fixed_request(requests: &RequestClient) -> Result<Response> {
    let id = RequestId::random()?;
    let message = Message::text(format!("request:{}:query-status", id.as_str()));
    requests.request(Request::new(id, message)).options(RequestOptions {
        send: SendOptions { retry_setting: RetrySetting::Disabled, ..Default::default() },
        ..Default::default()
    }).send().await
}

fn main() {}
```

## Identity, delivery and lifecycle

A logical factory request keeps a stable SDK OperationId and a stable application
idempotency key. Every attempt receives a fresh wire RequestId. The factory must
encode that wire ID and stable key; the response router must extract the wire ID
returned by the peer. A server returning a cached deduplicated result must echo
the **current attempt's wire ID**, not the ID of the first attempt it cached.

A retired attempt cannot complete a later attempt. Manual-routing registrations
are credentials for exactly one attempt; keep the supplied registration token
with the response being handled. The logical handle exposes a current owned
registration snapshot, or None between attempts. An old token remains stale.

One logical operation publishes one terminal TaskEvent and one final response or
error. Per-attempt details belong to the retry observer/report. Errors returned by the factory
are RequestBuild failures and stop the operation. The factory executes outside
SDK internal locks. Terminal TaskEvents use `TaskSource::RequestOperation` with
local metadata; they do not expose the factory or idempotency key.

Logical tasks belong to the session runtime and are cancelled/joined by SDK
shutdown. Cancelling an operation ends its active attempt and pending backoff;
late responses cannot restart it. Backoff and reconnect waits remain bounded by
the total operation budget. Request capacity stays owned across retries; a
single available request slot does not need a second slot for its next attempt.

## API migration

| Previous entry | Current behavior/API |
| --- | --- |
| Common retry imports previously used from `open_net::api::http` | Import `Backoff`, `RetrySetting`, `RetryContext`, `RetryEvent`, `RetryObserver`, `RetryObserverHandle` and `RetryReport` from `open_net::api::retry`; no HTTP feature is required for these types |
| `SendRetryPolicy` | `WsSendRetryPolicy` composed with `RetrySchedule` |
| `SendOptions.retry` | `retry_setting: RetrySetting<WsSendRetryPolicy>` |
| Client configuration struct literals | Include two new default-policy fields or use `..Default::default()` |
| New logical operation API | `RequestClient::request_with_factory`, `RequestAttemptContext`, `WsRequestOperationOptions`, `WsRequestOperationBuilder`, `WsRequestOperationReceipt`, `WsRequestOperationHandle` |
| Response reporting | New `MessageReceipt::retry_report()` and `Response::retry_report()`; operation handle supplies a live snapshot; `NetError::retry_report()` retains final failure statistics |
| WebSocket timeout diagnostics | New non-exhaustive `WsTimeoutPhase`, `NetError::ws_timeout_phase()` and `ErrorContext::ws_timeout_phase`; `WsRequestRetryOn::with_attempt_timeout` controls whole-attempt expiry independently |
| `ErrorContext` under `ws-client` | Adds public `ws_timeout_phase: Option<WsTimeoutPhase>`. The struct was already non-exhaustive; external consumers still construct it with `ErrorContext::default()` and then assign public fields, not a struct literal or struct-update expression |
| Exhaustive `TaskSource` matches | Add `TaskSource::RequestOperation(metadata)` for logical factory requests; existing Message and Request variants remain. The new payload is `Arc<Metadata>` and excludes the factory and idempotency key |
| Fixed `Request::new(id, message)`, `RequestClient::request/execute` | Existing signatures remain single-attempt; use the factory entry point for multiple attempts |
| Fixed `RequestHandle::registration()` | Remains one registration; logical operations use `current_registration()` snapshots |
| `ReconnectPolicy` and `BackoffConfig` | Existing public behavior and defaults remain independent |
| `open_net::ws::RetryDecision` | Remains the connection-event type; import common classifier decisions from `open_net::api::retry::RetryDecision` |

## `WsSendRetryPolicy::no_retry()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicy::no_retry() -> Self
```

### Steps

Construct a single-attempt policy for ordinary message writes.

### Parameters

None.

### Callback parameters

None.

### Return values

One attempt, no reasons, Never safety and no callbacks.

### Errors and behavior

This is the default policy. It does not change session reconnect configuration.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryPolicy::builder()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicy::builder() -> WsSendRetryPolicyBuilder
```

### Steps

Create a builder, configure a schedule and replay contract, then build.

### Parameters

None.

### Callback parameters

None.

### Return values

A builder starting from no_retry().

### Errors and behavior

A multi-attempt schedule alone does not select reasons or authorize replay.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryPolicy::schedule()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicy::schedule(&self) -> &RetrySchedule
```

### Steps

Inspect immutable attempt/backoff/jitter configuration.

### Parameters

None.

### Callback parameters

None.

### Return values

The borrowed schedule.

### Errors and behavior

Operation counters are separate and never shared by policy clones.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryPolicy::retry_on()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicy::retry_on(&self) -> &WsSendRetryOn
```

### Steps

Inspect protocol reason selection.

### Parameters

None.

### Callback parameters

None.

### Return values

The borrowed reason set.

### Errors and behavior

Classifiers may veto built-in decisions; safety and budgets remain mandatory.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryPolicy::safety()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicy::safety(&self) -> WsReplaySafety
```

### Steps

Read the replay declaration.

### Parameters

None.

### Callback parameters

None.

### Return values

The configured safety value.

### Errors and behavior

The SDK does not verify application idempotency or insert a key into payloads.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryPolicy::observer()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicy::observer(&self) -> Option<RetryObserverHandle>
```

### Steps

Clone the configured observer handle if present.

### Parameters

None.

### Callback parameters

None.

### Return values

An optional common observer handle.

### Errors and behavior

Clones share the observer, not runtime retry state.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryPolicyBuilder::schedule()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicyBuilder::schedule(self, schedule: RetrySchedule) -> Self
```

### Steps

Install a previously validated shared schedule.

### Parameters

`schedule`: complete attempt, backoff and jitter settings.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

Replaces the schedule without merging. Timeouts remain on operation options.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryPolicyBuilder::retry_on()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicyBuilder::retry_on(self, retry_on: WsSendRetryOn) -> Self
```

### Steps

Select protocol retry reasons.

### Parameters

`retry_on`: a complete reason set.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

Replaces prior reason selection. Terminal errors cannot be enabled for retry.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryPolicyBuilder::safety()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicyBuilder::safety(self, safety: WsReplaySafety) -> Self
```

### Steps

Declare the server replay contract.

### Parameters

`safety`: Never, IdempotentByContract or IdempotencyKey.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

IdempotencyKey requires an actual peer deduplication contract and an encoded stable key.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryPolicyBuilder::observer()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicyBuilder::observer(self, observer: impl RetryObserver) -> Self
```

### Steps

Install a short synchronous observer.

### Parameters

`observer`: a thread-safe common observer or closure.

### Callback parameters

`&RetryEvent`; returns `()`.

### Return values

The updated builder.

### Errors and behavior

Observer failure is isolated/logged and cannot alter the operation result.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryPolicyBuilder::classifier()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicyBuilder::classifier<F>(self, classifier: F) -> Self
where F: for<'a> Fn(&WsSendRetryContext<'a>) -> RetryDecision + Send + Sync + 'static
```

### Steps

Install a read-only attempt classifier.

### Parameters

`classifier`: returns Default, Retry or Stop.

### Callback parameters

A borrowed protocol context; returns common `RetryDecision`.

### Return values

The updated builder.

### Errors and behavior

Stop vetoes retry. Retry is still subject to replay safety, attempt/total budget, cancellation and shutdown. Classifier failure is terminal.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryPolicyBuilder::build()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryPolicyBuilder::build(self) -> Result<WsSendRetryPolicy, RetryConfigError>
```

### Steps

Finish policy construction and install it on a client or operation.

### Parameters

None.

### Callback parameters

None.

### Return values

The configured policy.

### Errors and behavior

The shared schedule is already validated. This method does not begin an operation or validate the server contract.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicy::no_retry()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicy::no_retry() -> Self
```

### Steps

Construct a single-attempt policy for factory-backed logical requests.

### Parameters

None.

### Callback parameters

None.

### Return values

One attempt, no reasons, Never safety and no callbacks.

### Errors and behavior

This is the default policy. It does not change session reconnect configuration.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicy::builder()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicy::builder() -> WsRequestRetryPolicyBuilder
```

### Steps

Create a builder, configure a schedule and replay contract, then build.

### Parameters

None.

### Callback parameters

None.

### Return values

A builder starting from no_retry().

### Errors and behavior

A multi-attempt schedule alone does not select reasons or authorize replay.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicy::schedule()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicy::schedule(&self) -> &RetrySchedule
```

### Steps

Inspect immutable attempt/backoff/jitter configuration.

### Parameters

None.

### Callback parameters

None.

### Return values

The borrowed schedule.

### Errors and behavior

Operation counters are separate and never shared by policy clones.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicy::retry_on()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicy::retry_on(&self) -> &WsRequestRetryOn
```

### Steps

Inspect protocol reason selection.

### Parameters

None.

### Callback parameters

None.

### Return values

The borrowed reason set.

### Errors and behavior

Classifiers may veto built-in decisions; safety and budgets remain mandatory.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicy::safety()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicy::safety(&self) -> WsReplaySafety
```

### Steps

Read the replay declaration.

### Parameters

None.

### Callback parameters

None.

### Return values

The configured safety value.

### Errors and behavior

The SDK does not verify application idempotency or insert a key into payloads.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicy::observer()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicy::observer(&self) -> Option<RetryObserverHandle>
```

### Steps

Clone the configured observer handle if present.

### Parameters

None.

### Callback parameters

None.

### Return values

An optional common observer handle.

### Errors and behavior

Clones share the observer, not runtime retry state.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicyBuilder::schedule()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicyBuilder::schedule(self, schedule: RetrySchedule) -> Self
```

### Steps

Install a previously validated shared schedule.

### Parameters

`schedule`: complete attempt, backoff and jitter settings.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

Replaces the schedule without merging. Timeouts remain on operation options.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicyBuilder::retry_on()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicyBuilder::retry_on(self, retry_on: WsRequestRetryOn) -> Self
```

### Steps

Select protocol retry reasons.

### Parameters

`retry_on`: a complete reason set.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

Replaces prior reason selection. Terminal errors cannot be enabled for retry.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicyBuilder::safety()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicyBuilder::safety(self, safety: WsReplaySafety) -> Self
```

### Steps

Declare the server replay contract.

### Parameters

`safety`: Never, IdempotentByContract or IdempotencyKey.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

IdempotencyKey requires an actual peer deduplication contract and an encoded stable key.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicyBuilder::observer()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicyBuilder::observer(self, observer: impl RetryObserver) -> Self
```

### Steps

Install a short synchronous observer.

### Parameters

`observer`: a thread-safe common observer or closure.

### Callback parameters

`&RetryEvent`; returns `()`.

### Return values

The updated builder.

### Errors and behavior

Observer failure is isolated/logged and cannot alter the operation result.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicyBuilder::classifier()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicyBuilder::classifier<F>(self, classifier: F) -> Self
where F: for<'a> Fn(&WsRequestRetryContext<'a>) -> RetryDecision + Send + Sync + 'static
```

### Steps

Install a read-only attempt classifier.

### Parameters

`classifier`: returns Default, Retry or Stop.

### Callback parameters

A borrowed protocol context; returns common `RetryDecision`.

### Return values

The updated builder.

### Errors and behavior

Stop vetoes retry. Retry is still subject to replay safety, attempt/total budget, cancellation and shutdown. Classifier failure is terminal.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryPolicyBuilder::build()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryPolicyBuilder::build(self) -> Result<WsRequestRetryPolicy, RetryConfigError>
```

### Steps

Finish policy construction and install it on a client or operation.

### Parameters

None.

### Callback parameters

None.

### Return values

The configured policy.

### Errors and behavior

The shared schedule is already validated. This method does not begin an operation or validate the server contract.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryOn::none()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryOn::none() -> Self
```

### Steps

Begin with no protocol errors selected.

### Parameters

None.

### Callback parameters

None.

### Return values

An empty reason set.

### Errors and behavior

This is also the default; no timeout category is implicitly selected.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryOn::with_write_errors()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryOn::with_write_errors(self, enabled: bool) -> Self
```

### Steps

Enable or clear the standard write-error list.

### Parameters

`enabled`: true selects Io, DeliveryUnknown and NotConnected.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

Replaces the write-error list while retaining timeout flags. Safety and terminal-error checks still apply.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryOn::with_errors()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryOn::with_errors(self, errors: impl IntoIterator<Item = ErrorKind>) -> Self
```

### Steps

Replace the write-error kind list.

### Parameters

`errors`: eligible error kinds.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

Cannot make admission, configuration, cancellation, request-build or shutdown errors retryable.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryOn::with_write_timeout()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryOn::with_write_timeout(self, enabled: bool) -> Self
```

### Steps

Choose whether a write-stage timeout may retry.

### Parameters

`enabled`: whether write timeout is selected.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

Does not change write timeout duration or make total expiry retryable.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryOn::none()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryOn::none() -> Self
```

### Steps

Begin with no protocol errors selected.

### Parameters

None.

### Callback parameters

None.

### Return values

An empty reason set.

### Errors and behavior

This is also the default; no timeout category is implicitly selected.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryOn::with_write_errors()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryOn::with_write_errors(self, enabled: bool) -> Self
```

### Steps

Enable or clear the standard write-error list.

### Parameters

`enabled`: true selects Io, DeliveryUnknown and NotConnected.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

Replaces the write-error list while retaining timeout flags. Safety and terminal-error checks still apply.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryOn::with_errors()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryOn::with_errors(self, errors: impl IntoIterator<Item = ErrorKind>) -> Self
```

### Steps

Replace the write-error kind list.

### Parameters

`errors`: eligible error kinds.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

Cannot make admission, configuration, cancellation, request-build or shutdown errors retryable.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryOn::with_write_timeout()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryOn::with_write_timeout(self, enabled: bool) -> Self
```

### Steps

Choose whether a write-stage timeout may retry.

### Parameters

`enabled`: whether write timeout is selected.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

Does not change write timeout duration or make total expiry retryable.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsSendRetryOn::errors()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsSendRetryOn::errors(&self) -> &[ErrorKind]
```

### Steps

Inspect the selected error-kind list.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed error slice.

### Errors and behavior

Read-only; independent timeout flags are not represented in this slice.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryOn::with_response_timeout()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WsRequestRetryOn::with_response_timeout(self, enabled: bool) -> Self
```

### Steps

Choose whether an attempt response timeout may retry.

### Parameters

`enabled`: whether response-stage timeout is selected.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

Shares the request schedule with write and business retries. Selects only
Response expiry, not Attempt expiry; Admission and Total expiry remain terminal.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WsRequestRetryOn::with_attempt_timeout()`

### Required features

`ws-client`. Import `WsRequestRetryOn` from `open_net::ws`.

### Function signature

```text
WsRequestRetryOn::with_attempt_timeout(self, enabled: bool) -> Self
```

### Steps

Select whole-attempt timeout retries and pass the returned reason set to a
`WsRequestRetryPolicy` builder.

### Parameters

`enabled`: whether the built-in classifier selects `WsTimeoutPhase::Attempt`.
The default is false.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

This switch is independent of write and response timeout switches. An attempt
covers factory, queue, write and response work. Retrying still requires replay
safety, remaining attempts and remaining total time. A custom classifier can
request an Attempt retry even when this switch is false, but cannot retry
Admission or Total expiry.

### Example

See the complete [offline policy example](#configure-policies-offline), which
enables all three independent timeout switches explicitly.

## `NetError::ws_timeout_phase()`

### Required features

`ws-client`. Import `NetError` from `open_net` and `WsTimeoutPhase` from
`open_net::ws`.

### Function signature

```text
NetError::ws_timeout_phase(&self) -> Option<WsTimeoutPhase>
```

### Steps

Read the expired WebSocket budget from a returned error. Match the optional
non-exhaustive enum with a fallback arm.

### Parameters

None beyond the borrowed error.

### Callback parameters

None. The getter can also be called from a request retry classifier's error arm.

### Return values

The optional WebSocket timeout phase copied from
`error.context().ws_timeout_phase`. None means no WebSocket budget was recorded.

### Errors and behavior

Read-only and does not change classification or retry policy. Attempt identifies
the whole-attempt budget wherever it expires. Admission and Total are terminal.
HTTP timeout metadata remains available separately through `timeout_phase()`.

### Example

The complete [offline policy example](#configure-policies-offline) includes
`print_timeout`, which reads both the getter and context field and handles every
current variant with a fallback for future variants.

## `WebSocketClientConfig::with_default_send_retry_policy()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WebSocketClientConfig::with_default_send_retry_policy(self, policy: WsSendRetryPolicy) -> Self
```

### Steps

Install the client default for ordinary message writes.

### Parameters

`policy`: a configured send retry policy.

### Callback parameters

None.

### Return values

The updated configuration.

### Errors and behavior

Used by SendOptions selecting Inherit. Does not affect the separate factory-request default.

### Example

See the complete [offline policy example](#configure-policies-offline).

## `WebSocketClientConfig::with_default_request_retry_policy()`

### Required features

`ws-client`. Import protocol types from `open_net::ws` and shared types from
`open_net::api::retry`.

### Function signature

```text
WebSocketClientConfig::with_default_request_retry_policy(self, policy: WsRequestRetryPolicy) -> Self
```

### Steps

Install the client default for factory-backed requests.

### Parameters

`policy`: a configured logical-request retry policy.

### Callback parameters

None.

### Return values

The updated configuration.

### Errors and behavior

Used by logical operations selecting Inherit. Fixed Request payloads are not automatically rewritten or retried.

### Example

See the complete [offline policy example](#configure-policies-offline).


## `RequestClient::request_with_factory()`

### Required features

`ws-client`.

### Function signature

```text
RequestClient::request_with_factory<F>(&self, key: impl Into<String>, factory: F) -> Result<WsRequestOperationBuilder>
where F: Fn(&RequestAttemptContext) -> Result<Message> + Send + Sync + 'static
```

### Steps

Create the logical builder, configure operation options, then enqueue or send.

### Parameters

`key`: stable application idempotency key with a non-whitespace character and at most 1024 UTF-8 bytes; `factory`: recreates the wire message for each attempt.

### Callback parameters

`&RequestAttemptContext`; the factory returns `Result<Message>` synchronously.

### Return values

A logical request builder or a validation error.

### Errors and behavior

The factory receives a fresh wire ID for each attempt and must encode it along with the stable key. Constructing the builder does not grant replay permission.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `RequestAttemptContext::operation_id()`

### Required features

`ws-client`.

### Function signature

```text
RequestAttemptContext::operation_id(&self) -> OperationId
```

### Steps

Read the stable logical identity while constructing an attempt.

### Parameters

None.

### Callback parameters

None.

### Return values

The operation ID shared by every attempt.

### Errors and behavior

This is distinct from the per-attempt wire request ID.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `RequestAttemptContext::attempt()`

### Required features

`ws-client`.

### Function signature

```text
RequestAttemptContext::attempt(&self) -> u32
```

### Steps

Read which attempt the factory is building.

### Parameters

None.

### Callback parameters

None.

### Return values

The one-based attempt number.

### Errors and behavior

The attempt budget includes the first attempt and all selected write, response-timeout and business retries.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `RequestAttemptContext::request_id()`

### Required features

`ws-client`.

### Function signature

```text
RequestAttemptContext::request_id(&self) -> &RequestId
```

### Steps

Encode this ID in the new attempt payload.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed fresh wire request ID.

### Errors and behavior

A response router must extract the current wire ID echoed by the peer. Cached business results must also use the current attempt ID.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `RequestAttemptContext::idempotency_key()`

### Required features

`ws-client`.

### Function signature

```text
RequestAttemptContext::idempotency_key(&self) -> &str
```

### Steps

Encode the stable business key required by the server contract.

### Parameters

None.

### Callback parameters

None.

### Return values

The operation key shared by all attempts.

### Errors and behavior

The SDK neither inserts this key into opaque messages nor implements server-side deduplication.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `WsRequestOperationBuilder::options()`

### Required features

`ws-client`.

### Function signature

```text
WsRequestOperationBuilder::options(self, options: WsRequestOperationOptions) -> Self
```

### Steps

Install complete operation scheduling and timeout settings.

### Parameters

`options`: priority, timeouts, disconnection/cancellation settings, metadata and retry selection.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

Replaces the entire options layer; does not create a nested SendOptions retry loop.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `WsRequestOperationBuilder::enqueue()`

### Required features

`ws-client`.

### Function signature

```text
WsRequestOperationBuilder::enqueue(self) -> impl Future<Output = Result<WsRequestOperationReceipt, EnqueueError<Self>>>
```

### Steps

Poll admission, retain the accepted receipt, then obtain its handle or response.

### Parameters

None.

### Callback parameters

None.

### Return values

An accepted receipt, or an error retaining the unaccepted builder.

### Errors and behavior

SDK admission participates in the total budget from first poll. The retained builder owns its factory and options on returned rejection.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `WsRequestOperationBuilder::send()`

### Required features

`ws-client`.

### Function signature

```text
WsRequestOperationBuilder::send(self) -> impl Future<Output = Result<Response>>
```

### Steps

Use when only the final response is needed.

### Parameters

None.

### Callback parameters

None.

### Return values

The final response or original terminal error with its report.

### Errors and behavior

Convenience submission consumes the builder. Use enqueue when the application needs ownership-preserving admission rejection or a separate handle.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `WsRequestOperationReceipt::handle()`

### Required features

`ws-client`.

### Function signature

```text
WsRequestOperationReceipt::handle(&self) -> &WsRequestOperationHandle
```

### Steps

Clone the handle before consuming the receipt if later inspection/cancellation is needed.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed logical-operation handle.

### Errors and behavior

The handle identifies the entire operation rather than one wire attempt.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `WsRequestOperationReceipt::response()`

### Required features

`ws-client`.

### Function signature

```text
WsRequestOperationReceipt::response(self) -> impl Future<Output = Result<Response>>
```

### Steps

Await one terminal outcome after acceptance.

### Parameters

None.

### Callback parameters

None.

### Return values

The final response or terminal error.

### Errors and behavior

Intermediate attempt outcomes are not delivered here. Exhaustion preserves the last outcome; final reports remain available.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `WsRequestOperationHandle::id()`

### Required features

`ws-client`.

### Function signature

```text
WsRequestOperationHandle::id(&self) -> OperationId
```

### Steps

Correlate the logical operation independently of its attempt IDs.

### Parameters

None.

### Callback parameters

None.

### Return values

The stable operation ID.

### Errors and behavior

Cloned handles refer to the same operation; reconnecting does not create another logical operation.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `WsRequestOperationHandle::current_registration()`

### Required features

`ws-client`.

### Function signature

```text
WsRequestOperationHandle::current_registration(&self) -> Option<RequestRegistration>
```

### Steps

Take an owned snapshot when manually resolving an incoming response.

### Parameters

None.

### Callback parameters

None.

### Return values

The current attempt registration, or None between attempts/after retirement.

### Errors and behavior

Compare the incoming wire ID with this registration. Retirement after a snapshot makes the token stale; it cannot complete a newer attempt.

### Example

See the complete [example](#manual-routing-and-cancellation).

## `WsRequestOperationHandle::retry_report()`

### Required features

`ws-client`.

### Function signature

```text
WsRequestOperationHandle::retry_report(&self) -> RetryReport
```

### Steps

Inspect an owned live or final retry snapshot.

### Parameters

None.

### Callback parameters

None.

### Return values

A common report snapshot.

### Errors and behavior

Counts started attempts and protocol sends separately. A running report has no stop reason; final exhaustion preserves the last outcome.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `WsRequestOperationHandle::cancel()`

### Required features

`ws-client`.

### Function signature

```text
WsRequestOperationHandle::cancel(&self) -> Result<TerminationOutcome>
```

### Steps

Request cancellation of the whole logical operation.

### Parameters

None.

### Callback parameters

None.

### Return values

The cancellation arbitration outcome, or an error.

### Errors and behavior

Cancels active work/backoff and prevents further attempts. It cannot reverse server processing; late responses do not overwrite a terminal result.

### Example

See the complete [example](#manual-routing-and-cancellation).

## `Response::retry_report()`

### Required features

`ws-client`.

### Function signature

```text
Response::retry_report(&self) -> Option<&RetryReport>
```

### Steps

Inspect logical retry statistics on a received response.

### Parameters

None.

### Callback parameters

None.

### Return values

The attached report when available.

### Errors and behavior

The report describes SDK scheduling and does not determine business success. Inspect the actual response payload separately.

### Example

See the complete [example](#send-a-logical-request-with-a-factory).

## `WsRequestOperationOptions::validate()`

### Required features

`ws-client`.

### Function signature

```text
WsRequestOperationOptions::validate(&self) -> Result<()>
```

### Steps

Validate options before installing them on the logical builder; enqueue also
validates before accepting the operation.

### Parameters

None.

### Callback parameters

None.

### Return values

Ok when duration ranges and metadata limits are valid, otherwise a configuration
or input error.

### Errors and behavior

Checks admission, attempt, total, write and response durations against the
monotonic clock. Zero durations represent immediate operational expiry rather
than invalid configuration. It does not start a timer, move an absolute deadline,
connect to a server or verify the idempotency contract.

### Example

See the complete [factory request program](#send-a-logical-request-with-a-factory).

## `MessageReceipt::retry_report()`

### Required features

`ws-client`.

### Function signature

```text
MessageReceipt::retry_report(&self) -> RetryReport
```

### Steps

Retain the message receipt, await its local write outcome, then inspect retry
statistics. It can also be sampled while the operation runs.

### Parameters

None.

### Callback parameters

None.

### Return values

An owned common retry-report snapshot for the ordinary message operation.

### Errors and behavior

The report tracks local writes, not server application processing. It shares the
receipt's stable operation identity across retry delays. Error results retain
their own final report through NetError as well.

### Example

See the complete [ordinary-message example](#send-one-ordinary-message).

## `WsRequestOperationBuilder::retry_setting()`

### Required features

`ws-client`.

### Function signature

```text
WsRequestOperationBuilder::retry_setting(self, setting: RetrySetting<WsRequestRetryPolicy>) -> Self
```

### Steps

Select Inherit, Disabled or a complete policy after setting other operation
options, then submit the builder.

### Parameters

`setting`: the complete logical-request retry selection.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

Replaces only the options retry setting, without merging policy fields or
resetting an established submission start time. A later options() call replaces
the whole options layer, including this selection.

### Example

See the complete [factory request program](#send-a-logical-request-with-a-factory).

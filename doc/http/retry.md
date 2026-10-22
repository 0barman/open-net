# HTTP retries

Required features: `http-client`. Retry support has no additional feature and
ships in the `open-net` crate. See [shared schedules and observations](../retry.md)
for `RetrySchedule`, `Backoff`, `Jitter`, `RetrySetting`, decisions and reports.
The packaged [http_retry example](../../examples/http_retry.rs) accepts a base URL
and optional relative path:

These commands run in the matching library source directory containing its
`Cargo.toml`, after obtaining that source. Adding a dependency to an application
does not install the library's examples into that application.

```sh
cargo run --manifest-path Cargo.toml --no-default-features --features http-client --example http_retry -- http://127.0.0.1:8080 /status
```

Every complete program on this page uses these dependency settings:

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false, features = ["http-client"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1.53.1", features = ["macros", "rt-multi-thread"] }
```

Client examples initialize an application-selected TLS provider, including for
plain HTTP. Configuration-only examples need no provider. See [TLS](../tls.md)
for provider ownership and prerequisites; the SDK does not install one for you.

## Policy and execution contract

`HttpRetryPolicy` combines a shared schedule with HTTP classification, replay
safety, `Retry-After`, an optional synchronous classifier and an observer. Its
default performs **one attempt and no retries**. Building a schedule with several
attempts alone does not select transient HTTP reasons: configure `RetryOn` or a
classifier explicitly. The policy stores configuration; each request creates
independent execution state.

`max_attempts` includes the first attempt and any 401 credential-refresh
replay. `Disabled` or a one-attempt schedule permits no refresh replay. Credential
loading/refresh during request preparation is distinct from replaying a received
401. Redirects stay within an attempt and retain their own hop budget; disabling
retries does not disable redirects. `transport_sends` counts redirect dispatches
separately from attempts.

The SDK classifies an entire buffered attempt, including body reading. HTTP 200
followed by a body-read failure remains `Err(NetError)` with its received status
and headers. Exhaustion preserves the last response/error plus its retry report.
A final HTTP error status is still a response value unless the caller applies a
[status policy](requests_and_responses.md). Scheduling completion does not imply
business success.

[Timeouts](timeouts_and_admission.md) belong to `HttpTimeouts`, independently of
retry configuration. The total budget covers admission, preparation, attempts,
authentication, body reading and backoff. It starts at a synchronous submission
call, or the first poll of an asynchronous submission future. An unpolled future
and an application's outer queue are outside that budget. Backoff waits observe
cancellation, total expiry and shutdown. Once the network operation has a terminal result, both final callback queueing
and callback execution are outside `total_timeout` and the report's `elapsed`.
The queued result is not changed into a timeout while waiting for the callback.
`finished()` still waits for callback execution and SDK resource cleanup.
Concrete buffered middleware receives its internal network result without waiting
on the application callback queue. Middleware's own work before and after
`next.run()` remains subject to cancellation and the total budget.

The classifier applies to buffered responses and attempt errors. Streaming never
buffers a response just for classification, and a delivered stream is never
replayed. Built-in status/transport policy can retry before returning the stream.

## Retry a transient endpoint

This example permits three attempts with two explicit delays. The 10-second
total timeout is an example setting, not a new SDK default. Start a local server
at `127.0.0.1:8080` with `/status`; the smoke fixture returns 503 twice and then
200. Final status/result and report remain available after completion.

```rust
use open_net::api::{
    http::{HttpClient, HttpClientConfig, HttpRequest, HttpRetryPolicy, HttpTimeouts, RetryOn},
    retry::{Backoff, RetryEvent, RetrySchedule},
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls_openssl::default_provider().install_default()
        .map_err(|_| std::io::Error::other("application TLS provider already installed"))?;
    let schedule = RetrySchedule::builder()
        .max_attempts(3)
        .backoff(Backoff::sequence([
            Duration::from_millis(100), Duration::from_millis(200),
        ]))
        .build()?;
    let policy = HttpRetryPolicy::builder()
        .schedule(schedule)
        .retry_on(RetryOn::standard())
        .observer(|event: &RetryEvent| println!("{event:?}"))
        .build()?;
    let config = HttpClientConfig::new("http://127.0.0.1:8080")?
        .with_default_retry_policy(policy)
        .with_timeouts(HttpTimeouts::new().with_total_timeout(Duration::from_secs(10)))?;
    let client = HttpClient::new(config, "retry-example")?;
    let result = client.request(HttpRequest::builder().path("/status").build()?).await;
    client.shutdown().await?;
    let response = result?;
    let report = response.retry_report()
        .ok_or_else(|| std::io::Error::other("completed response has no retry report"))?;
    println!("HTTP retry completed: {}", response.status());
    println!("attempts={}, sends={}, stop={:?}",
        report.attempts(), report.transport_sends(), report.stop_reason());
    Ok(())
}
```

## Built-in reasons and replay safety

`RetryOn::standard()` enables statuses 408, 425, 429, 500, 502, 503 and 504;
transport categories DNS, connect, pre-send timeout, read reset and body read;
and buffered body-read failures. `RetryOn::none()` starts empty. Setters replace
one category while retaining the others. `RetryReason::Application` is selected
by the classifier, and `RetryAfter` is a delay hint, so neither is enabled by
`RetryOn::allows` alone.

`TransportErrorKind` also exposes `Tls`, `Write`, `Protocol` and `DeliveryUnknown`
for explicit endpoint policy. These categories do not override request safety.
The available categories are not a promise that the backend can distinguish
every transport phase:

- `DeliveryUnknown` is produced when a request execution transport failure
  occurs before response headers and reliable send/receive evidence is absent.
  The peer may have received part or all of the request. This includes ambiguous
  upload failures, disconnects while awaiting headers, and HTTP/2 keepalive
  timeouts. It does not establish whether the server performed the operation.
  `NetError::kind()` remains `Io` or `TimedOut`; the retry category does not change
  it to `ErrorKind::DeliveryUnknown`. `context().stage` is `None` when the phase
  cannot be established.
- `Write` requires reliable evidence that writing failed. The current reqwest
  backend does not reliably expose that distinction and does not emit this
  category for execution failures. Ambiguous failures use `DeliveryUnknown`.
- `TimeoutBeforeSend` requires positive evidence that the timeout preceded
  sending. A `Write` stage, an elapsed timer, or a failed execution future does
  not provide that evidence. The current backend does not emit this category
  for execution failures.

`RetryOn::standard()` does not select `DeliveryUnknown` or `Write`. Correctly
classifying ambiguous execution failures as `DeliveryUnknown` narrows its retry
behavior: failures previously grouped into `ReadReset` or `TimeoutBeforeSend`
are no longer retried by `standard()` alone. With built-in classification, an
endpoint must explicitly select `DeliveryUnknown` and still satisfy request
safety and body replayability.

HTTP/2 keepalive timeout before response headers retains `ErrorKind::TimedOut` and
`http_timeout_phase = None`: a transport keepalive timer is not an SDK-owned
phase budget. It is not a read reset or evidence that the request was unsent.
Deadline phases are selected separately using `with_timeout_phases`; no phase is selected by default. Connect, Headers, IdleRead and Attempt can be selected. Admission and Total are filtered out of this setter and remain terminal. Cancellation and shutdown are never retried.

| `RequestSafety` | Replay permission |
| --- | --- |
| `Never` | Never replay, including auth refresh |
| `Automatic` | Only the SDK's idempotent HTTP methods: GET, HEAD, OPTIONS, PUT and DELETE |
| `IdempotentByContract` | The endpoint contract explicitly guarantees idempotency |
| `IdempotencyKey(String)` | The endpoint deduplicates using this nonempty key |

A body must also be replayable. `IdempotencyKey` is a safety declaration; it does
not inject a header or JSON field. The caller must encode the same key as required
by its server. Do not declare an ordinary POST safe without that server contract.
Debug formatting redacts the key.

```rust
use open_net::{StatusCode, api::{
    http::{HttpRequest, HttpRetryPolicy, Method, RequestSafety, RetryOn, RetryReason, TimeoutPhase, TransportErrorKind},
    retry::{RetrySchedule, RetrySetting},
}};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reasons = RetryOn::none()
        .with_statuses([StatusCode::SERVICE_UNAVAILABLE])
        .with_transport([TransportErrorKind::Connect])
        .with_body_read(true)
        .with_timeout_phases([TimeoutPhase::Connect, TimeoutPhase::Headers, TimeoutPhase::IdleRead, TimeoutPhase::Attempt]);
    if !reasons.allows(RetryReason::HttpStatus(StatusCode::SERVICE_UNAVAILABLE))
        || !reasons.retries_body_read()
    {
        return Err(std::io::Error::other("configured retry reasons missing").into());
    }
    println!("statuses={:?}, transport={:?}, timeout phases={:?}",
        reasons.statuses(), reasons.transport(), reasons.timeout_phases());
    let policy = HttpRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(2).build()?)
        .retry_on(reasons)
        .safety(RequestSafety::IdempotencyKey("demo-operation-001".into()))
        .build()?;
    let request = HttpRequest::builder().method(Method::POST).path("/deduplicated-operation")
        .header("idempotency-key", "demo-operation-001")?
        .body("payload".into())
        .retry_setting(RetrySetting::Policy(policy))
        .build()?;
    println!("request method: {:?}", request.method);
    Ok(())
}
```

## Classify a business response

`HttpRetryContext<'a>` has public fields `outcome: Result<&'a HttpResponse,
&'a NetError>`, `attempt: u32`, `elapsed: Duration` and
`remaining: Option<Duration>`. It borrows the completed attempt; it does not own
or invoke the final application callback.

Return `Default` to apply built-in rules, `Retry` for a transient business result,
or `Stop` to veto all retries for that attempt, including an eligible 401 auth
replay. `Retry` cannot bypass safety, attempt/total limits, cancellation or
shutdown. A classifier unwind becomes a provider failure. Keep classifiers
synchronous, read-only and short; asynchronous work belongs elsewhere.

This demonstrates a small byte protocol. Production JSON code should parse the
application's documented business schema. No application-specific business code
or server contract is built into the SDK.

```rust
use open_net::api::{
    http::{HttpRetryContext, HttpRetryPolicy, RetryOn},
    retry::{RetryDecision, RetrySchedule},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let policy = HttpRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(3).build()?)
        .retry_on(RetryOn::standard())
        .classifier(|context: HttpRetryContext<'_>| match context.outcome {
            Ok(response) if response.body().as_ref() == b"BUSY" => RetryDecision::Retry,
            Ok(response) if response.body().as_ref() == b"PERMANENT_FAILURE" => RetryDecision::Stop,
            _ => RetryDecision::Default,
        })
        .build()?;
    println!("business retry attempt limit: {}", policy.schedule().max_attempts());
    Ok(())
}
```

## Respect Retry-After

`RetryAfterMode::Ignore` uses the local delay. `Respect` takes the maximum of the
already-jittered local delay and a valid server delay. It never caps or shortens a
server minimum. Missing/invalid hints use the local delay. A hint longer than the
remaining total budget causes a cancellable wait until total expiry and a timeout;
it must not trigger an early retry. There is no `PreferServer`, `RespectWithCap`,
or retry-after cap API.

```rust
use open_net::api::http::{HttpRetryPolicy, RetryAfterMode};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let local = Duration::from_secs(2);
    let selected = RetryAfterMode::Respect.select(Some(Duration::from_secs(8)), local);
    if selected != Duration::from_secs(8) {
        return Err(std::io::Error::other("server minimum was shortened").into());
    }
    let policy = HttpRetryPolicy::builder().retry_after(RetryAfterMode::Respect).build()?;
    println!("mode={:?}, selected={selected:?}", policy.retry_after());
    Ok(())
}
```

## Request settings and callback adapters

`RetrySetting<HttpRetryPolicy>` is the single source of retry selection on a
request/options value. `Inherit` chooses the client default, `Disabled` explicitly
uses one attempt, and `Policy(policy)` replaces the whole policy. No hidden
boolean or separate public `retry_policy` field competes with that choice.

`HttpRequestTrait::retry_setting()` defaults to `Inherit`. `send` and `send_wait`
use that selection. `send_with_options` and `send_wait_with_options` use the
explicit options as authoritative; options `Inherit` means the client default,
not the trait policy. An adapter can deliberately copy `request.retry_setting()`
into its options before submission. All existing enqueue ownership and once-only
callback contracts remain in force.

```rust
use open_net::api::{
    http::{HttpClientConfig, HttpRequest, HttpRequestId, HttpRequestOptions, HttpRetryPolicy, HttpTimeouts, RetryOn},
    retry::{RetrySchedule, RetrySetting},
};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let policy = HttpRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(3).build()?)
        .retry_on(RetryOn::standard()).build()?;
    let config = HttpClientConfig::new("http://127.0.0.1:8080")?
        .with_default_retry_policy(policy);
    let request = HttpRequest::builder().path("/health")
        .retry_setting(RetrySetting::Inherit).build()?;
    let options = HttpRequestOptions::new()
        .with_retry_setting(RetrySetting::Disabled)
        .with_replayable_body(true)
        .with_request_id(HttpRequestId(42))
        .with_timeouts(HttpTimeouts::new().with_total_timeout(Duration::from_secs(5)))?;
    if config.default_retry_policy.schedule().max_attempts() != 3
        || !matches!(request.retry_setting(), RetrySetting::Inherit)
        || !matches!(options.retry_setting(), RetrySetting::Disabled)
    {
        return Err(std::io::Error::other("retry setting precedence changed").into());
    }
    println!("callback options: {:?}", options.timeouts());
    let disabled = HttpRetryPolicy::no_retry();
    println!("safety={:?}, reasons={:?}, observer={:?}",
        disabled.safety(), disabled.retry_on(), disabled.observer());
    Ok(())
}
```

`HttpRequestOptions` public fields remain `replayable_body: bool` (default `true`)
and `request_id: Option<HttpRequestId>` (default `None`). Retry selection and
`HttpTimeouts` are accessed through methods. Durations, custom request IDs,
headers and admission errors follow their existing validation rules.

## Migration from the previous API

| Previous API | Current API |
| --- | --- |
| Imports from `open_net::api::http` for `Backoff`, `RetrySetting`, `RetryContext`, `RetryEvent`, `RetryObserver`, `RetryObserverHandle`, `RetryReport` | Import these common types from `open_net::api::retry`; the old HTTP re-exports are removed |
| `RetryPolicy`, `RetryPolicyBuilder`, `RetryPolicyError` | `HttpRetryPolicy`, `HttpRetryPolicyBuilder`, common `RetryConfigError` |
| `RetryPolicy::new(retries)` / policy `max_attempts`, backoff and max-delay setters | `RetrySchedule::builder().max_attempts(retries + 1).backoff(...).build()?`, then policy `.schedule(...)`; validate arithmetic in caller code |
| Construction: `RetryPolicy::none()` / `no_retry()` | `HttpRetryPolicy::no_retry()`; the `none` alias is removed |
| Construction: `RetryPolicy::standard()` | No direct constructor. Build a three-attempt schedule with `Backoff::exponential(Duration::from_millis(50), 2.0, Duration::from_secs(30))`, then select `.retry_on(RetryOn::standard())`; the policy's default safety remains Automatic |
| Configuration: `with_retryable_statuses(statuses)` | Configure the policy builder with `.retry_on(RetryOn::none().with_statuses(statuses))`, or start from the desired existing reason set to preserve other categories |
| Configuration: `with_transport_errors(enabled)` | Select categories through `RetryOn::with_transport(...)` and buffered body failures through `with_body_read(...)`; no direct boolean shortcut remains |
| Configuration: `with_non_idempotent(enabled)` | Set builder `.safety(RequestSafety::IdempotentByContract)` for the former true setting, or `.safety(RequestSafety::Automatic)` for false; body replayability still applies |
| Queries: `RetryPolicy::max_attempts()`, `max_retries()`, `backoff()` | `policy.schedule().max_attempts()`, its value minus one, and `policy.schedule().backoff()`; the validated attempt count is always at least one |
| Delay queries: `RetryPolicy::backoff_delay(...)`, `retry_delay(...)` | Use `policy.schedule().delay_for(ordinal)?` for the deterministic local delay, then `policy.retry_after().select(server_delay, local_delay)` when needed; there is no single public replacement for the complete runtime jitter/deadline calculation |
| Reason queries: `should_retry_status(status)`, `should_retry_reason(reason)` | `policy.retry_on().allows(RetryReason::HttpStatus(status))` and `policy.retry_on().allows(reason)` inspect the reason selection; they do not authorize a complete replay |
| Runtime eligibility queries: `allows_method`, `should_retry_error`, `can_retry_attempt` | Removed without direct public equivalents; runtime combines safety, replayability, error classification, attempt count and deadlines. Inspect `safety()`, `retry_on()` and `schedule()` for configuration only |
| `Backoff::exponential(initial, max)` / `Backoff::Exponential { initial, max }` | Add the `multiplier` argument/field; `2.0` preserves the previous doubling factor |
| `Backoff::delay_for(ordinal) -> Duration` | Returns `Result<Duration, RetryConfigError>`; ordinal zero and sequence overflow are errors, and sequence overflow no longer repeats the last value |
| `RetrySetting::None` | `RetrySetting::Disabled` |
| `RetrySetting::resolve(&default)` | `RetrySetting<HttpRetryPolicy>::resolve(&default, HttpRetryPolicy::no_retry())` |
| Request `.retry_policy(...)` / options `.with_retry_policy(...)` | `.retry_setting(RetrySetting::Policy(...))` / `.with_retry_setting(...)` |
| Trait `retry_policy()` | `retry_setting() -> RetrySetting<HttpRetryPolicy>` |
| `ApplicationPredicate` and `with_application` | Three-way `HttpRetryPolicyBuilder::classifier` |
| Policy attempt timeout / total deadline | `HttpTimeouts` |
| `PreferServer`, `RespectWithCap`, retry-after cap | `Ignore` or `Respect` |
| HTTP-specific `RetryContext`, report reason/error fields | Common timing context/report; inspect final response/error separately |
| `HttpCompletion` structure | Adds `retry_report: RetryReport` and `retry_report()`; callers constructing literals must provide it |
| Response/error report access | New `HttpResponse::retry_report()` and `NetError::retry_report()` return optional borrowed reports |

Old compatibility aliases are intentionally removed. The client setter remains
`with_default_retry_policy`, now accepting `HttpRetryPolicy`. `request`, `send`,
`send_wait`, and the explicit-options entry points retain their result/callback
signatures. [Responses](requests_and_responses.md), completion handles and
[`NetError`](../retry.md#neterrorretry_report) expose the common report.

## `HttpRetryPolicy::no_retry()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicy::no_retry() -> Self
```

### Steps

Construct a one-attempt policy.

### Parameters

None.

### Callback parameters

None.

### Return values

One attempt, no built-in retry reasons, Automatic safety, Ignore server delay, and no classifier/observer.

### Errors and behavior

This is also the default. Authentication replay requires another available attempt.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRetryPolicy::builder()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicy::builder() -> HttpRetryPolicyBuilder
```

### Steps

Create a builder, select schedule and reasons, then build.

### Parameters

None.

### Callback parameters

None.

### Return values

A builder initialized from `no_retry()`.

### Errors and behavior

Selecting a multi-attempt schedule alone does not enable HTTP reasons.

### Example

See the complete [example](#retry-a-transient-endpoint).

## `HttpRetryPolicy::schedule()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicy::schedule(&self) -> &RetrySchedule
```

### Steps

Read the immutable schedule.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed shared retry schedule.

### Errors and behavior

Configuration only; running counters are available in a report.

### Example

See the complete [example](#classify-a-business-response).

## `HttpRetryPolicy::retry_on()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicy::retry_on(&self) -> &RetryOn
```

### Steps

Read built-in reason selection.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed configured reason set.

### Errors and behavior

Classifier decisions can override built-in classification within safety and budget limits.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRetryPolicy::retry_after()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicy::retry_after(&self) -> RetryAfterMode
```

### Steps

Read server-delay handling.

### Parameters

None.

### Callback parameters

None.

### Return values

`Ignore` or `Respect`.

### Errors and behavior

The default is `Ignore`.

### Example

See the complete [example](#respect-retry-after).

## `HttpRetryPolicy::safety()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicy::safety(&self) -> &RequestSafety
```

### Steps

Read the replay-safety declaration.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed safety value.

### Errors and behavior

Debug output redacts an idempotency key. The declaration does not inject it into the request.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRetryPolicy::observer()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicy::observer(&self) -> Option<RetryObserverHandle>
```

### Steps

Clone a handle to the installed observer when needed.

### Parameters

None.

### Callback parameters

None.

### Return values

An optional common observer handle.

### Errors and behavior

Clones share the observer only; they do not share request counters.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRetryPolicyBuilder::schedule()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicyBuilder::schedule(self, schedule: RetrySchedule) -> Self
```

### Steps

Build a validated shared schedule and install it.

### Parameters

`schedule`: complete attempt/backoff/jitter settings.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

Replaces the schedule; reason selection, safety and timeout configuration remain separate.

### Example

See the complete [example](#retry-a-transient-endpoint).

## `HttpRetryPolicyBuilder::retry_on()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicyBuilder::retry_on(self, retry_on: RetryOn) -> Self
```

### Steps

Choose built-in retry reasons.

### Parameters

`retry_on`: the complete reason set.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

The last setter replaces the prior reason set. Safety and budgets still apply.

### Example

See the complete [example](#built-in-reasons-and-replay-safety).

## `HttpRetryPolicyBuilder::retry_after()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicyBuilder::retry_after(self, mode: RetryAfterMode) -> Self
```

### Steps

Select whether valid server delay hints are respected.

### Parameters

`mode`: `Ignore` or `Respect`.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

Respect uses a lower bound after jitter and never shortens a server minimum.

### Example

See the complete [example](#respect-retry-after).

## `HttpRetryPolicyBuilder::safety()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicyBuilder::safety(self, safety: RequestSafety) -> Self
```

### Steps

Declare the endpoint replay contract.

### Parameters

`safety`: Never, Automatic, contract-idempotent or a nonempty idempotency key.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

An empty key fails `build()`. Replay still requires a replayable body.

### Example

See the complete [example](#built-in-reasons-and-replay-safety).

## `HttpRetryPolicyBuilder::observer()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicyBuilder::observer(self, observer: impl RetryObserver) -> Self
```

### Steps

Install a short read-only event observer.

### Parameters

`observer`: a thread-safe `RetryObserver` or compatible closure.

### Callback parameters

`&RetryEvent`; returns `()`.

### Return values

The updated builder.

### Errors and behavior

Notifications are synchronous; unwinds are isolated/logged. The observer cannot change the result.

### Example

See the complete [example](#retry-a-transient-endpoint).

## `HttpRetryPolicyBuilder::classifier()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicyBuilder::classifier<F>(self, classifier: F) -> Self
where F: for<'a> Fn(HttpRetryContext<'a>) -> RetryDecision + Send + Sync + 'static
```

### Steps

Install a read-only buffered-attempt classifier.

### Parameters

`classifier`: returns Default, Retry or Stop.

### Callback parameters

`HttpRetryContext` by value with borrowed outcome; returns `RetryDecision`.

### Return values

The updated builder.

### Errors and behavior

Stop vetoes built-in/auth retry. Retry cannot bypass safety or budgets. A panic is converted to provider failure. Streaming bodies are not buffered for this hook.

### Example

See the complete [example](#classify-a-business-response).

## `HttpRetryPolicyBuilder::build()`

### Required features

`http-client`.

### Function signature

```text
HttpRetryPolicyBuilder::build(self) -> Result<HttpRetryPolicy, RetryConfigError>
```

### Steps

Validate protocol-specific settings and retain the completed policy.

### Parameters

None.

### Callback parameters

None.

### Return values

An HTTP policy or common configuration error.

### Errors and behavior

An empty declared idempotency key is rejected. Schedule validation has already occurred in `RetryScheduleBuilder::build`.

### Example

See the complete [example](#built-in-reasons-and-replay-safety).

## `RetryAfterMode::select()`

### Required features

`http-client`.

### Function signature

```text
RetryAfterMode::select(self, server_delay: Option<Duration>, backoff_delay: Duration) -> Duration
```

### Steps

Combine a parsed server hint with the already-jittered local delay.

### Parameters

`server_delay`: optional hint; `backoff_delay`: computed local delay.

### Callback parameters

None.

### Return values

The local delay for Ignore/no hint, otherwise their maximum.

### Errors and behavior

Pure calculation. The executor separately enforces cancellation and the total deadline; it never sends early when the selected delay exceeds remaining time.

### Example

See the complete [example](#respect-retry-after).

## `RetryOn::standard()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::standard() -> Self
```

### Steps

Use the documented conservative transient reason set.

### Parameters

None.

### Callback parameters

None.

### Return values

Standard statuses, transport categories and body-read selection.

### Errors and behavior

Does not supply an attempt budget or safety declaration.

### Example

See the complete [example](#retry-a-transient-endpoint).

## `RetryOn::none()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::none() -> Self
```

### Steps

Start an explicit reason configuration from empty sets.

### Parameters

None.

### Callback parameters

None.

### Return values

No statuses, transport categories or body-read retry.

### Errors and behavior

This is the policy builder default; classifiers remain independently configurable.

### Example

See the complete [example](#built-in-reasons-and-replay-safety).

## `RetryOn::with_statuses()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::with_statuses(self, statuses: impl IntoIterator<Item = StatusCode>) -> Self
```

### Steps

Replace the HTTP status selection.

### Parameters

`statuses`: statuses eligible for retry.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

Other categories are preserved. A retry still requires remaining budget and replay permission.

### Example

See the complete [example](#built-in-reasons-and-replay-safety).

## `RetryOn::with_transport()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::with_transport(self, reasons: impl IntoIterator<Item = TransportErrorKind>) -> Self
```

### Steps

Replace transport reason selection.

### Parameters

`reasons`: eligible transport categories.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

Other categories are preserved. DeliveryUnknown does not override request safety.

### Example

See the complete [example](#built-in-reasons-and-replay-safety).

## `RetryOn::with_body_read()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::with_body_read(self, enabled: bool) -> Self
```

### Steps

Enable or disable buffered body-read retry.

### Parameters

`enabled`: whether body-read failures are eligible.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

A failed body remains an error even if HTTP status was 200. A returned stream is never replayed.

### Example

See the complete [example](#built-in-reasons-and-replay-safety).

## `RetryOn::allows()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::allows(&self, reason: RetryReason) -> bool
```

### Steps

Inspect reason membership.

### Parameters

`reason`: an HTTP status, transport, body-read or other classification.

### Callback parameters

None.

### Return values

Whether that reason belongs to the set.

### Errors and behavior

Application and RetryAfter return false; application scheduling uses a classifier and RetryAfter only adjusts delay.

### Example

See the complete [example](#built-in-reasons-and-replay-safety).

## `RetryOn::statuses()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::statuses(&self) -> &[StatusCode]
```

### Steps

Inspect the selected HTTP statuses.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed status slice.

### Errors and behavior

No mutation or execution occurs.

### Example

See the complete [example](#built-in-reasons-and-replay-safety).

## `RetryOn::transport()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::transport(&self) -> &[TransportErrorKind]
```

### Steps

Inspect the selected transport categories.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed category slice.

### Errors and behavior

No mutation or execution occurs.

### Example

See the complete [example](#built-in-reasons-and-replay-safety).

## `RetryOn::retries_body_read()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::retries_body_read(&self) -> bool
```

### Steps

Read the explicit body-read toggle.

### Parameters

None.

### Callback parameters

None.

### Return values

Whether the body-read category is selected.

### Errors and behavior

The transport category BodyRead can also select a body-read retry; replay safety and budgets still apply.

### Example

See the complete [example](#built-in-reasons-and-replay-safety).

## `HttpRequestOptions::new()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::new() -> Self
```

### Steps

Create options for an explicit-options callback submission.

### Parameters

None.

### Callback parameters

None.

### Return values

Inherit retry/timeouts, replayable body true, and no custom request ID.

### Errors and behavior

Explicit options are authoritative over the trait retry setting.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRequestOptions::with_retry_setting()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::with_retry_setting(self, setting: RetrySetting<HttpRetryPolicy>) -> Self
```

### Steps

Choose inherit, disable or full policy replacement.

### Parameters

`setting`: complete selection for this operation.

### Callback parameters

None.

### Return values

The updated options.

### Errors and behavior

Inherit selects the client default, not the trait policy.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRequestOptions::retry_setting()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::retry_setting(&self) -> RetrySetting<HttpRetryPolicy>
```

### Steps

Read the authoritative selection.

### Parameters

None.

### Callback parameters

None.

### Return values

A cloned selection.

### Errors and behavior

Cloning the policy never shares operation counters.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRequestOptions::with_replayable_body()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::with_replayable_body(self, replayable: bool) -> Self
```

### Steps

Declare whether callback request content can be recreated.

### Parameters

`replayable`: false prevents all replay.

### Callback parameters

None.

### Return values

The updated options.

### Errors and behavior

A true value does not authorize replay of an unsafe method by itself.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRequestOptions::with_request_id()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::with_request_id(self, id: HttpRequestId) -> Self
```

### Steps

Choose a local operation identifier before admission.

### Parameters

`id`: custom HTTP request ID.

### Callback parameters

None.

### Return values

The updated options.

### Errors and behavior

Duplicate active IDs fail admission and return the unaccepted input. This is unrelated to server idempotency keys.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRequestOptions::with_timeouts()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::with_timeouts(self, timeouts: HttpTimeouts) -> Result<Self, NetError>
```

### Steps

Validate and install request-level timeout settings.

### Parameters

`timeouts`: phase overrides or inherited defaults.

### Callback parameters

None.

### Return values

Updated options, or an invalid-input error.

### Errors and behavior

Zero or unrepresentable enabled durations are rejected. Retry policies do not carry timeouts.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRequestOptions::timeouts()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::timeouts(&self) -> &HttpTimeouts
```

### Steps

Inspect configured timeout overrides.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed timeout settings value.

### Errors and behavior

Shows overrides rather than a resolved running deadline.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpClientConfig::with_default_retry_policy()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_default_retry_policy(self, policy: HttpRetryPolicy) -> Self
```

### Steps

Install a default policy before creating the client.

### Parameters

`policy`: already validated HTTP retry policy.

### Callback parameters

None.

### Return values

The updated configuration.

### Errors and behavior

Used only for operations selecting Inherit. The default configuration uses `HttpRetryPolicy::no_retry()`.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRequestBuilder::retry_setting()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestBuilder::retry_setting(self, setting: RetrySetting<HttpRetryPolicy>) -> Self
```

### Steps

Select the request policy, then build and submit.

### Parameters

`setting`: Inherit, Disabled or Policy.

### Callback parameters

None.

### Return values

The updated request builder.

### Errors and behavior

The last setter wins; there is no parallel retry_policy field.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpRequest::retry_setting()`

### Required features

`http-client`.

### Function signature

```text
HttpRequest::retry_setting(&self) -> RetrySetting<HttpRetryPolicy>
```

### Steps

Inspect the request selection before submission.

### Parameters

None.

### Callback parameters

None.

### Return values

A cloned selection.

### Errors and behavior

Read-only and independent of runtime state.

### Example

See the complete [example](#request-settings-and-callback-adapters).

## `HttpResponse::retry_report()`

### Required features

`http-client`.

### Function signature

```text
HttpResponse::retry_report(&self) -> Option<&RetryReport>
```

### Steps

Inspect the completed operation report before consuming the response.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed report when attached by the SDK.

### Errors and behavior

Manual/synthetic responses may initially have no report. Retrying exhaustion preserves the response and its report.

### Example

See the complete [example](#retry-a-transient-endpoint).


## `RetryOn::with_timeout_phases()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::with_timeout_phases(self, phases: impl IntoIterator<Item = TimeoutPhase>) -> Self
```

### Steps

Replace the selected attempt timeout phases.

### Parameters

`phases`: Connect, Headers, IdleRead or Attempt.

### Callback parameters

None.

### Return values

The updated reason set.

### Errors and behavior

No phases are selected by default. Admission and Total entries are discarded and cannot make those timeouts retryable. Other retry categories are preserved.

### Example

See the complete [reason-selection example](#built-in-reasons-and-replay-safety).

## `RetryOn::timeout_phases()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::timeout_phases(&self) -> &[TimeoutPhase]
```

### Steps

Inspect selected attempt timeout phases.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed phase slice.

### Errors and behavior

This shows policy configuration rather than elapsed/running deadline state.

### Example

See the complete [reason-selection example](#built-in-reasons-and-replay-safety).

# Retries

Required features: `http-client`. Retry support has no additional feature.
Create a project with `cargo new retry-example` and add the following
dependencies to its `Cargo.toml`, adjusting the source path to your checkout.
Every complete program on this page uses these dependencies:

```toml
[dependencies]
open-net = { path = "../open-net", default-features = false, features = ["http-client"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1.53.1", features = ["macros", "rt-multi-thread"] }
```

## Application TLS provider

open-net does not choose a cryptographic provider. Each complete program that
creates an HTTP client below initializes the example application's provider in
`main`; copy the program as written. Configuration-only examples need no provider
until a client is built. Client creation requires a provider even for plain HTTP
and otherwise returns `InvalidConfig` for `tls.provider`.

The examples use OpenSSL through `rustls-openssl`, an application dependency.
Another compatible rustls 0.23 provider can replace it. Install a process provider
once in the host application, or pass an explicit per-client provider. A library
using open-net should accept the host's choice rather than install its own global
provider. See [TLS providers](../tls.md) for build prerequisites, provider
precedence and complete examples of both approaches.

See [timeouts](timeouts_and_admission.md) for deadline inheritance and
[callback requests](callback_requests.md) for submitting custom request traits.
A retry replays an operation, so configure it according to the endpoint's actual
contract. The default client policy performs **one attempt and no retries**.

## RetryPolicy and RetryPolicyBuilder

### Required features

`http-client`.

### Function signatures and parameters

Factory functions have no receiver. Other builder functions consume `self`;
accessors borrow `&self`. The tables list every public inherent method. Parameters
shown in a signature are required; a missing setter keeps its default.

| Factory signature | Return value and defaults |
| --- | --- |
| `RetryPolicy::no_retry() -> Self` | One attempt, no selected reasons, no explicit deadlines, Automatic safety |
| `RetryPolicy::none() -> Self` | Alias of `no_retry()` |
| `RetryPolicy::new(max_retries: u32) -> Self` | Retry count after the first attempt, clamped to 32; zero gives no retries |
| `RetryPolicy::standard() -> Self` | `new(2)`: three total attempts |
| `RetryPolicy::builder() -> RetryPolicyBuilder` | Same as `RetryPolicyBuilder::default()`: one attempt, no reasons, no backoff, no deadlines, Ignore server delay, Automatic safety, no observer or predicate |

`new(n)` for positive `n` selects standard transient reasons and exponential
backoff starting at 50 milliseconds and capped at 30 seconds. It adds no jitter.
`RetryPolicy::default()` is `no_retry()`, not `standard()`.

| Builder signature | Parameter meaning and validation |
| --- | --- |
| `max_attempts(self, max_attempts: u32) -> Self` | Total attempts including the first; legal range 1–33 |
| `attempt_timeout(self, timeout: Duration) -> Self` | Positive legacy per-attempt limit |
| `total_deadline(self, deadline: Duration) -> Self` | Positive legacy operation-wide limit |
| `backoff(self, backoff: Backoff) -> Self` | Retry delay selection |
| `retry_on(self, retry_on: RetryOn) -> Self` | Replace all reason categories |
| `retry_after(self, mode: RetryAfterMode) -> Self` | Select server delay handling |
| `retry_after_cap(self, cap: Duration) -> Self` | Positive cap, used in RespectWithCap mode |
| `safety(self, safety: RequestSafety) -> Self` | Replay permission |
| `observer(self, observer: impl RetryObserver) -> Self` | Retained thread-safe lifecycle observer |
| `application_predicate(self, predicate: impl Fn(&HttpResponse) -> bool + Send + Sync + 'static) -> Self` | Inspect a fully buffered response; also enables application retries |
| `build(self) -> Result<RetryPolicy, RetryPolicyError>` | Validate and construct the policy |

Each setter also has an exact `with_` alias with the same argument and return type:
`with_max_attempts`, `with_attempt_timeout`, `with_total_deadline`, `with_backoff`,
`with_retry_on`, `with_retry_after`, `with_retry_after_cap`, `with_safety`,
`with_observer`, `with_application_predicate`.
Repeated setters replace the previous value. Calling `retry_on` after
`application_predicate` can disable its application reason again.

### Steps

1. Start with `standard()`, `new(n)`, or the explicit builder.
2. Select reason categories, replay safety, delays, and optional deadlines.
3. Validate a builder with `build()` and handle `RetryPolicyError`.
4. Install the policy using `HttpClientConfig::with_default_retry_policy`,
   `HttpRequestBuilder::retry_policy`, or `HttpRequestOptions::with_retry_policy`.
5. Execute and release the response. Shut down the client when finished.

### Callback parameters

An application predicate receives `&HttpResponse` and returns `bool`: `true`
requests another attempt only when the other retry checks also allow it. The
predicate must inspect without side effects. `Send + Sync + 'static` permits
storage and invocation by background work: move owned data or `Arc` into it,
not a reference to a short-lived stack value. It applies to buffered responses,
not to a delivered stream. Observers receive `&RetryEvent` (see below).

### Return values and errors

A built policy is cloneable configuration, not an active task. `build()` returns
`Err(RetryPolicyError)` for these exact cases:

| Error | Cause |
| --- | --- |
| `InvalidMaxAttempts` | Zero or more than 33 total attempts |
| `EmptyBackoffSequence` | Empty sequence |
| `InvalidBackoff` | Exponential maximum below initial delay |
| `ZeroAttemptTimeout` | Zero attempt timeout |
| `ZeroTotalDeadline` | Zero total deadline |
| `ZeroRetryAfterCap` | Zero server-delay cap |

Builder validation checks these rules; timeout resolution additionally checks
monotonic-clock representability. `RetryPolicyError` implements `Display` and
`std::error::Error`. Request execution still returns `NetError` for transport,
cancellation, timeout, or shutdown failures. A final HTTP 4xx/5xx response is
still `Ok(HttpResponse)` unless explicitly checked with a status policy.

### Errors and behavior

Replay requires remaining attempt budget, an allowed reason and acceptable
method safety. A classification accessor alone does not imply a retry will run.
Admission, preparation, backoff and attempts remain part of one logical request;
cancellation and applicable deadlines terminate that operation. A stream is
never replayed after it has been returned to the caller. Application predicates
only run on fully buffered responses.

For a final buffered-body failure, `NetError::http_response_head()` may retain
that attempt's status and headers while the original failure classification,
source and timeout phase remain authoritative. A later retry never reuses an
earlier attempt's head, and explicit cancellation carries no head. Do not treat
retained status as a successful result or as automatic permission to replay a
request. See [response-head diagnostics](requests_and_responses.md#neterrorhttp_response_head).

### Example

Start a server at `127.0.0.1:8080` serving `/status`. A transient response such as
503 can trigger up to two retries; expect observer events and a final status or
error. The program shuts down even when the request fails. `.await` waits without
blocking a runtime thread; `?` propagates configuration errors.

```rust
use open_net::api::http::{Backoff, HttpClient, HttpClientConfig, HttpRequest, RetryEvent, RetryOn, RetryPolicy};
use std::time::Duration;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let policy = RetryPolicy::builder()
        .max_attempts(3)
        .backoff(Backoff::exponential(Duration::from_millis(100), Duration::from_secs(1)))
        .retry_on(RetryOn::standard())
        .total_deadline(Duration::from_secs(10))
        .observer(|event: &RetryEvent| println!("{event:?}"))
        .build()?;
    let config = HttpClientConfig::new("http://127.0.0.1:8080")?.with_default_retry_policy(policy);
    let client = HttpClient::new(config, "retry-example")?;
    let result = client.request(HttpRequest::builder().path("/status").build()?).await;
    client.shutdown().await?;
    println!("{:?}", result.map(|response| response.status));
    Ok(())
}
```

### Policy accessors and compatibility setters

These methods are infallible. No callbacks apply except the predicate setter.
All `with_*` methods consume and return `Self`. Durations have explicit Rust units.

| Signature | Return value / behavior |
| --- | --- |
| `max_retries(&self) -> u32` | Retry budget excluding the initial attempt |
| `max_attempts(&self) -> u32` | Total attempt budget |
| `attempt_timeout(&self) -> Option<Duration>` | Explicit legacy attempt limit; `None` means unset |
| `total_deadline(&self) -> Option<Duration>` | Explicit legacy total limit; `None` means unset |
| `backoff(&self) -> &Backoff` | Configured delay rule |
| `retry_after(&self) -> RetryAfterMode` | Server-delay mode |
| `retry_after_cap(&self) -> Option<Duration>` | Explicit cap; `None` uses the policy maximum in RespectWithCap |
| `safety(&self) -> &RequestSafety` | Replay declaration |
| `retry_on(&self) -> &RetryOn` | Reason classifier |
| `observer(&self) -> Option<RetryObserverHandle>` | Cloned handle or `None` if absent |
| `max_delay(&self) -> Duration` | Backoff upper bound |
| `with_max_retries(self, max_retries: u32) -> Self` | Clamp to 32 and update total attempts |
| `with_retryable_statuses(self, statuses: impl IntoIterator<Item = StatusCode>) -> Self` | Replace status set, retaining other categories; empty disables status retries |
| `with_transport_errors(self, enabled: bool) -> Self` | False clears transport categories and body-read flag; true restores standard transport categories only if empty, without re-enabling the separate body-read flag |
| `with_non_idempotent(self, enabled: bool) -> Self` | Set IdempotentByContract if true, Automatic if false; does not bypass body safety |
| `with_max_delay(self, max_delay: Duration) -> Self` | Nonzero changes delay bound; zero leaves it unchanged |
| `with_application_predicate(self, predicate: impl Fn(&HttpResponse) -> bool + Send + Sync + 'static) -> Self` | Store predicate and enable application reason |
| `backoff_delay(&self, retry_ordinal: u32) -> Duration` | Configured delay capped by max_delay; ordinal zero returns zero |
| `retry_delay(&self, retry_ordinal: u32, server_delay: Option<Duration>) -> Duration` | Combine backoff, server mode and optional cap |
| `allows_method(&self, method: &Method, replayable_body: bool) -> bool` | Method-safety check only; does not check attempt budget |
| `should_retry_status(&self, status: StatusCode) -> bool` | Status membership |
| `should_retry_reason(&self, reason: RetryReason) -> bool` | Reason membership |
| `should_retry_error(&self, error: &NetError) -> bool` | Classify retained typed error evidence, otherwise fallback kind/stage |
| `can_retry_attempt(&self, attempt: u32) -> bool` | Here `attempt` is a retry ordinal: 1 through max_retries; zero is false |

## Backoff and RetryAfterMode

### Required features

`http-client`.

### Function signatures and parameters

| Signature / variant | Parameters and meaning |
| --- | --- |
| `Backoff::None` | No delay |
| `Backoff::constant(delay: Duration) -> Self` / `Constant(Duration)` | Same delay each time |
| `Backoff::exponential(initial: Duration, max: Duration) -> Self` / `Exponential { initial, max }` | Double initial by retry ordinal, saturating at max |
| `Backoff::sequence(delays: impl IntoIterator<Item = Duration>) -> Self` / `Sequence(Vec<Duration>)` | One delay per retry; reuse last after the end |
| `Backoff::delay_for(&self, retry_ordinal: u32) -> Duration` | One-based retry ordinal; zero returns zero |
| `RetryAfterMode::select(self, server_delay: Option<Duration>, backoff_delay: Duration, cap: Option<Duration>) -> Duration` | Choose delay; `None` server value falls back to backoff |

### Steps, return values, and errors

Construct a backoff, install it on a policy builder, then call `build()` to validate.
Constructors and delay readers are infallible; empty sequences and reversed
exponential bounds fail when building the policy. Zero delay is allowed. There
are no callbacks. `delay_for` returns a duration, not a sleep future.

`Ignore` always chooses backoff. `PreferServer` prefers a valid server delay and
does not apply the cap. `RespectWithCap` clips the selected delay using `cap` if
present. `RetryPolicy::retry_delay` supplies its `max_delay` when no explicit cap
exists in RespectWithCap mode. `Retry-After` does not itself authorize a retry;
the status/error must qualify. The transport handles supported delta-seconds and
HTTP-date values; malformed hints fall back to configured backoff.

Date parsing uses `httpdate` for IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`),
obsolete RFC 850, and asctime forms. A compatibility adapter also accepts the
previous parser's common RFC 2822 forms: numeric offsets such as `+0000`,
`-0000`, `+0200`, and `-0400`; `GMT`, `UT`, `UTC`, and the US timezone names
`EST`/`EDT`, `CST`/`CDT`, `MST`/`MDT`, `PST`/`PDT`. It preserves case-insensitive
names, extra ASCII whitespace, a missing weekday or seconds, a one-digit day,
and one simple trailing parenthesized comment. Explicit weekdays and calendar
dates must agree. Past dates select zero delay, including dates before 1970;
`:60` seconds normalize to the following second.

The compatibility adapter supports years `0000` through `9999`; RFC 2822
two-digit years retain the `1950..2049` interpretation. Obsolete RFC 850 dates
use `httpdate`'s fixed `1970..2069` interpretation, rather than a rolling
50-year window. Nested, escaped, or multiple comments and other unsupported
date syntax fall back to backoff. Applications should send IMF-fixdate or
delta-seconds for interoperable HTTP headers.

### Example

This program runs offline and checks delay selection.

```rust
use open_net::api::http::{Backoff, RetryAfterMode};
use std::time::Duration;
fn main() {
    let constant = Backoff::constant(Duration::from_millis(100));
    let exponential = Backoff::exponential(Duration::from_millis(100), Duration::from_secs(1));
    let sequence = Backoff::sequence([Duration::ZERO, Duration::from_millis(200)]);
    assert_eq!(constant.delay_for(2), Duration::from_millis(100));
    assert_eq!(exponential.delay_for(3), Duration::from_millis(400));
    assert_eq!(sequence.delay_for(9), Duration::from_millis(200));
    assert_eq!(Backoff::None.delay_for(1), Duration::ZERO);
    let delay = RetryAfterMode::RespectWithCap.select(Some(Duration::from_secs(8)),
        Duration::from_secs(1), Some(Duration::from_secs(2)));
    assert_eq!(delay, Duration::from_secs(2));
}
```

## RetryOn, RetryReason, TransportErrorKind, and RequestSafety

### Required features

`http-client`.

### Function signatures and parameters

| RetryOn signature | Behavior |
| --- | --- |
| `standard() -> Self` | Statuses 408, 425, 429, 500, 502, 503, 504; transport Dns, Connect, TimeoutBeforeSend, ReadReset, BodyRead; body-read flag true, application false |
| `none() -> Self` | Empty reason sets and false flags |
| `with_statuses(self, statuses: impl IntoIterator<Item = StatusCode>) -> Self` | Replace statuses |
| `with_transport(self, reasons: impl IntoIterator<Item = TransportErrorKind>) -> Self` | Replace transport categories |
| `with_body_read(self, enabled: bool) -> Self` | Set dedicated body-read flag |
| `with_application(self, enabled: bool) -> Self` | Set application predicate flag |
| `allows(&self, reason: RetryReason) -> bool` | Test a reason; RetryAfter is always false |
| `statuses(&self) -> &[StatusCode]` | Borrow status set |
| `transport(&self) -> &[TransportErrorKind]` | Borrow transport set |
| `retries_body_read(&self) -> bool` | Read body flag |
| `retries_application(&self) -> bool` | Read application flag |

`RetryReason` variants are `HttpStatus(StatusCode)`, `Transport(TransportErrorKind)`,
`BodyRead`, `Application`, `RetryAfter`. Transport categories are `Dns`, `Connect`,
`Tls`, `TimeoutBeforeSend`, `Write`, `ReadReset`, `BodyRead`, `Protocol`,
`DeliveryUnknown`. Standard reasons exclude TLS, write, framing/protocol and unknown
delivery; opt-in to those categories requires careful endpoint replay guarantees.

| RequestSafety variant | Meaning |
| --- | --- |
| `Never` | Never replay |
| `Automatic` (default) | This implementation recognizes GET, HEAD, OPTIONS, PUT, DELETE as idempotent |
| `IdempotentByContract` | Application asserts server-side idempotence; non-idempotent methods still need replayable bodies |
| `IdempotencyKey(String)` | Application asserts server-side deduplication; body safety still applies |

### Steps, callback parameters, and return values

Construct reason sets, set them on the policy builder, and select safety.
No callbacks apply here. Constructors/setters return configuration; readers return
slices or booleans. All methods above are infallible.

### Errors and behavior

A safety declaration is not a server guarantee. `IdempotencyKey` does **not** add a
header or body field: provide the key exactly as your service requires. Policy
checks do not manufacture replayable content. Classification is separate from
remaining budget, cancellation and deadlines. The method-only `allows_method`
query accepts recognized idempotent methods even when `replayable_body` is false;
the worker separately refuses a required replay with `BodyNotReplayable` before
a second send. A replayability declaration must match the actual request provider.

### Example

This offline example demonstrates explicit reason selection and an idempotency
header. Replace the key with a stable per-operation value for a real service.

```rust
use open_net::{StatusCode, api::http::{HttpRequest, Method, RequestSafety, RetryOn, RetryPolicy, RetryReason, TransportErrorKind}};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reasons = RetryOn::none()
        .with_statuses([StatusCode::SERVICE_UNAVAILABLE])
        .with_transport([TransportErrorKind::Connect])
        .with_body_read(false).with_application(false);
    assert!(reasons.allows(RetryReason::HttpStatus(StatusCode::SERVICE_UNAVAILABLE)));
    assert_eq!(reasons.statuses().len(), 1);
    assert_eq!(reasons.transport().len(), 1);
    assert!(!reasons.retries_body_read() && !reasons.retries_application());
    let policy = RetryPolicy::builder().max_attempts(2).retry_on(reasons)
        .safety(RequestSafety::IdempotencyKey("operation-42".into())).build()?;
    let request = HttpRequest::builder().method(Method::POST).path("/jobs")
        .header("Idempotency-Key", "operation-42")?
        .body("payload".into()).retry_policy(policy).build()?;
    assert_eq!(request.method, Method::POST);
    Ok(())
}
```

## RetrySetting and HttpRequestOptions

### Required features

`http-client`.

### Function signatures and parameters

`RetrySetting::{Inherit, None, Policy(RetryPolicy)}` chooses the client default,
explicitly disables retries, or replaces the client policy. Default is `Inherit`.
`resolve(&self, default: &RetryPolicy) -> RetryPolicy` returns a policy clone;
`None` returns `no_retry()`. It does not modify the supplied default.

| HttpRequestOptions signature | Meaning / return |
| --- | --- |
| `new() -> Self` | Same as default: inherited policy, replayable body true, no request ID, inherited timeouts |
| `with_retry_policy(self, retry_policy: RetryPolicy) -> Self` | Explicit policy, including explicit no-retry |
| `with_retry_setting(self, setting: RetrySetting) -> Self` | Select inheritance/disable/replacement |
| `retry_setting(&self) -> RetrySetting` | Clone of selection |
| `with_replayable_body(self, replayable_body: bool) -> Self` | Declare body replayability |
| `with_request_id(self, request_id: HttpRequestId) -> Self` | Use client-local cancellation/correlation ID |
| `with_timeouts(self, timeouts: HttpTimeouts) -> Result<Self, NetError>` | Replace all request timeout overrides, validating durations |
| `timeouts(&self) -> &HttpTimeouts` | Borrow explicit overrides |

Public fields are `retry_policy: RetryPolicy`, `replayable_body: bool`, and
`request_id: Option<HttpRequestId>`. `None` ID requests automatic allocation.
Private inheritance markers prevent external struct literals; use `new()` and
setters to keep policy selection consistent. Values are copied into accepted work.

### Steps, return values, and callbacks

Build options, pass them to `send_with_options` or `send_wait_with_options`, and
keep the returned request handle if you need completion. For concrete requests,
use builder `retry_policy` or `retry_setting` instead. There are no callbacks on
option construction; request callbacks are described in the callback guide.

### Errors and behavior

Only `with_timeouts` above returns validation errors (`InvalidInput`). Request
submission can reject a duplicate active ID or invalid request. The original
request is retained in `EnqueueError` on ordinary rejection. Option configuration
does not itself submit or retry anything. Last selection wins; `Inherit` restores
the client's policy even after a previous explicit override.

### Example

This program inspects options offline; the handle flow is in the callback guide.

```rust
use open_net::api::http::{HttpRequestOptions, HttpRequestId, HttpTimeouts, RetryPolicy, RetrySetting};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let inherited = RetrySetting::Inherit.resolve(&RetryPolicy::standard());
    assert_eq!(inherited.max_attempts(), 3);
    let options = HttpRequestOptions::new()
        .with_retry_policy(RetryPolicy::standard())
        .with_retry_setting(RetrySetting::None)
        .with_replayable_body(false)
        .with_request_id(HttpRequestId(42))
        .with_timeouts(HttpTimeouts::new())?;
    assert_eq!(options.retry_setting(), RetrySetting::None);
    assert!(!options.replayable_body);
    assert_eq!(options.timeouts(), &HttpTimeouts::new());
    Ok(())
}
```

## ApplicationPredicate and retry observation

### Required features

`http-client`.

### Function signatures

```text
ApplicationPredicate::new(predicate: impl Fn(&HttpResponse) -> bool + Send + Sync + 'static) -> Self
RetryContext::new(method: Method, host: impl Into<String>, path: impl AsRef<str>) -> Self
RetryReport::new() -> Self
trait RetryObserver: Send + Sync + 'static {
    fn observe(&self, event: &RetryEvent);
}
RetryObserverHandle::new(observer: impl RetryObserver) -> Self
RetryObserverHandle::notify(&self, event: RetryEvent)
```

### Steps and parameters

Supply a `Fn(&RetryEvent) + Send + Sync + 'static` closure directly to
`RetryPolicyBuilder::observer`, or implement `RetryObserver`. The policy keeps a
cloneable handle. `notify` manually delivers an owned event; it does not cause a
request attempt. `ApplicationPredicate::new` builds a cloneable predicate wrapper;
install a closure using the policy's `application_predicate` setter. Its evaluation
method is internal. `RetryContext` and `RetryReport` constructors build data values.

### Callback parameters and return values

| RetryEvent | Payload |
| --- | --- |
| `AttemptStarted { attempt: u32 }` | One-based attempt number |
| `AttemptFinished { attempt: u32, final_attempt: bool }` | Attempt number and whether terminal |
| `RetryScheduled { attempt: u32, delay: Duration }` | Number of the attempt that just completed and the delay before its retry; for example, `attempt: 1` schedules attempt 2 |
| `Completed(RetryReport)` | Terminal operation summary |

Observers return `()`. Keep them short and nonblocking: notification is synchronous
where execution calls it, not a dedicated application thread. The handle catches
observer unwinding panics and logs the failure. No drop-based unsubscribe handle
is provided for observers; they live with the policy and its clones. Predicate
callbacks return a boolean; do not assume observer panic isolation also applies
to arbitrary application predicate code.

| RetryContext public field | Type / initial value |
| --- | --- |
| `attempt` | `u32`, 1 |
| `method` | `Method`, supplied method |
| `host` | `String`, strips scheme, credentials, path/query/fragment |
| `path` | `String`, removes query and fragment |
| `status` | `Option<StatusCode>`, None |
| `reason` | `Option<RetryReason>`, None |
| `elapsed` | `Duration`, zero |
| `remaining` | `Option<Duration>`, None (no supplied remaining budget) |

The observer signature receives an event, not a `RetryContext` parameter.

| RetryReport public field | Type / new or default value |
| --- | --- |
| `attempts` | `u32`, 0 |
| `retry_count` | `u32`, 0 |
| `elapsed` | `Duration`, zero |
| `final_reason` | `Option<RetryReason>`, None |
| `exhausted` | `bool`, false |
| `last_error` | `Option<String>`, None |

### Errors and behavior

Constructors and `notify` have no Result return. Report fields distinguish the
actual attempts from the configured maximum. `retry_count` counts scheduled
retries, including a retry cancelled during its backoff before the next attempt
starts. The current `exhausted` flag records reaching the configured ordinary
attempt boundary; it is not a success/failure flag and can be true for a successful
response, including a one-attempt policy. `last_error` contains the terminal error
kind label when execution records an error, rather than the complete error source.
`None` reason or error means none was recorded. With `http-client-auth`, a permitted
credential-refresh replay has its own limit and can add an attempt beyond the
ordinary policy maximum; see [authentication](authentication.md). Debug output redacts idempotency keys and error contents, but public
fields can hold application data; do not treat arbitrary manual values as sanitized.

### Example

This offline example constructs a predicate, verifies context sanitization and
delivers a synthetic observer event. Synthetic events do not make network calls.

```rust
use open_net::api::http::{ApplicationPredicate, HttpResponse, Method, RetryContext, RetryEvent, RetryObserverHandle, RetryReport};
fn main() {
    let _predicate = ApplicationPredicate::new(|response: &HttpResponse| response.body.is_empty());
    let context = RetryContext::new(Method::GET, "https://name:secret@example.com", "/status?token=secret#detail");
    assert_eq!(context.host, "example.com");
    assert_eq!(context.path, "/status");
    let observer = RetryObserverHandle::new(|event: &RetryEvent| println!("{event:?}"));
    observer.notify(RetryEvent::Completed(RetryReport::new()));
}
```

## Offline policy inspection example

This complete program checks the compatibility setters, factory defaults and
classification accessors without submitting any request. Classifying a reason
never authorizes replay on its own.

```rust
use open_net::{error::ErrorKind, NetError, StatusCode, api::http::{HttpResponse, Method, RetryPolicy, RetryReason}};
use std::time::Duration;
fn main() {
    assert_eq!(RetryPolicy::no_retry(), RetryPolicy::none());
    assert_eq!(RetryPolicy::new(99).max_retries(), 32);
    assert_eq!(RetryPolicy::standard().max_attempts(), 3);
    let policy = RetryPolicy::standard()
        .with_max_retries(2)
        .with_retryable_statuses([StatusCode::SERVICE_UNAVAILABLE])
        .with_transport_errors(false)
        .with_non_idempotent(true)
        .with_max_delay(Duration::from_secs(1))
        .with_application_predicate(|response: &HttpResponse| response.body.is_empty());
    assert_eq!(policy.attempt_timeout(), None);
    assert_eq!(policy.total_deadline(), None);
    let _configured = (policy.backoff(), policy.retry_after(), policy.retry_after_cap(),
        policy.safety(), policy.retry_on(), policy.observer(), policy.max_delay());
    assert_eq!(policy.backoff_delay(0), Duration::ZERO);
    assert_eq!(policy.retry_delay(1, None), Duration::from_millis(50));
    assert!(policy.allows_method(&Method::POST, true));
    assert!(policy.should_retry_status(StatusCode::SERVICE_UNAVAILABLE));
    assert!(policy.should_retry_reason(RetryReason::Application));
    assert!(!policy.should_retry_error(&NetError::from(ErrorKind::InvalidInput)));
    assert!(policy.can_retry_attempt(2));
    assert!(!policy.can_retry_attempt(0));
}
```

## Builder and alias example

Every alias below replaces the same field as its unprefixed counterpart. This
program validates the final settings and checks one invalid attempt count.

```rust
use open_net::api::http::{Backoff, HttpResponse, RequestSafety, RetryAfterMode, RetryEvent, RetryOn, RetryPolicy, RetryPolicyError};
use std::time::Duration;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let policy = RetryPolicy::builder()
        .max_attempts(2).with_max_attempts(3)
        .attempt_timeout(Duration::from_secs(1)).with_attempt_timeout(Duration::from_secs(2))
        .total_deadline(Duration::from_secs(5)).with_total_deadline(Duration::from_secs(6))
        .backoff(Backoff::None).with_backoff(Backoff::constant(Duration::from_millis(10)))
        .retry_on(RetryOn::none()).with_retry_on(RetryOn::standard())
        .retry_after(RetryAfterMode::Ignore).with_retry_after(RetryAfterMode::RespectWithCap)
        .retry_after_cap(Duration::from_secs(1)).with_retry_after_cap(Duration::from_secs(2))
        .safety(RequestSafety::Never).with_safety(RequestSafety::Automatic)
        .observer(|event: &RetryEvent| println!("{event:?}"))
        .with_observer(|event: &RetryEvent| println!("{event:?}"))
        .application_predicate(|response: &HttpResponse| response.body.is_empty())
        .with_application_predicate(|response: &HttpResponse| response.body.is_empty())
        .build()?;
    assert_eq!(policy.max_attempts(), 3);
    assert_eq!(policy.attempt_timeout(), Some(Duration::from_secs(2)));
    assert_eq!(policy.total_deadline(), Some(Duration::from_secs(6)));
    assert_eq!(RetryPolicy::builder().max_attempts(0).build().unwrap_err(), RetryPolicyError::InvalidMaxAttempts);
    Ok(())
}
```

## Custom observer example

The trait callback is synchronous. This offline example calls it directly and
then through the panic-isolating handle; neither call performs a request.

```rust
use open_net::api::http::{RetryEvent, RetryObserver, RetryObserverHandle, RetryReport};
struct Observer;
impl RetryObserver for Observer {
    fn observe(&self, event: &RetryEvent) { println!("{event:?}"); }
}
fn main() {
    Observer.observe(&RetryEvent::AttemptStarted { attempt: 1 });
    RetryObserverHandle::new(Observer).notify(RetryEvent::Completed(RetryReport::new()));
}
```

## Installing retry selections example

This offline program constructs configuration and concrete requests. The final
selection replaces the earlier selection at the same layer.

```rust
use open_net::api::http::{HttpClientConfig, HttpRequest, RetryPolicy, RetrySetting};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = HttpClientConfig::new("http://127.0.0.1:8080")?
        .with_default_retry_policy(RetryPolicy::standard());
    let request = HttpRequest::builder().path("/status")
        .retry_policy(RetryPolicy::new(1)).retry_setting(RetrySetting::Inherit).build()?;
    assert_eq!(config.default_retry_policy.max_attempts(), 3);
    assert_eq!(request.path, "/status");
    Ok(())
}
```

## Individual retry functions

The family tables above provide an index and shared behavior. The entries below
specify each function separately and link to a complete program that demonstrates
its call. All configuration is local until installed and a request is submitted.

## `RetryPolicy::no_retry()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::no_retry() -> Self
```

### Steps

Create the policy or builder, configure if needed, and install the result before sending.

### Parameters

None.

### Callback parameters

None.

### Return values

A policy with one attempt, empty reasons, no delay or deadlines, and Automatic safety.

### Errors and behavior

Infallible. The first attempt still runs.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::none()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::none() -> Self
```

### Steps

Create the policy or builder, configure if needed, and install the result before sending.

### Parameters

None.

### Callback parameters

None.

### Return values

The same disabled policy as no_retry().

### Errors and behavior

Infallible alias of no_retry().

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::new()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::new(max_retries: u32) -> Self
```

### Steps

Create the policy or builder, configure if needed, and install the result before sending.

### Parameters

`max_retries`: retries after the first attempt. Zero disables retries; values above 32 are clamped.

### Callback parameters

None.

### Return values

A policy with at most 33 total attempts.

### Errors and behavior

Infallible. Positive values use standard transient reasons and exponential delay from 50 ms to 30 s without jitter. No deadlines are installed.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::standard()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::standard() -> Self
```

### Steps

Create the policy or builder, configure if needed, and install the result before sending.

### Parameters

None.

### Callback parameters

None.

### Return values

new(2): three total attempts.

### Errors and behavior

Infallible. Default::default() instead returns no_retry().

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::builder()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::builder() -> RetryPolicyBuilder
```

### Steps

Create the policy or builder, configure if needed, and install the result before sending.

### Parameters

None.

### Callback parameters

None.

### Return values

An unvalidated builder with one attempt, no reasons or delay, no deadlines, Ignore server delay and Automatic safety.

### Errors and behavior

Infallible. Call build() to validate after setting fields.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicy::max_retries()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::max_retries(&self) -> u32
```

### Steps

Borrow an existing policy and read the configured value.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Retry budget excluding the first attempt.

### Errors and behavior

Infallible; reading configuration does not schedule retries or reserve capacity.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::max_attempts()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::max_attempts(&self) -> u32
```

### Steps

Borrow an existing policy and read the configured value.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Total attempt budget including the first attempt.

### Errors and behavior

Infallible; reading configuration does not schedule retries or reserve capacity.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::attempt_timeout()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::attempt_timeout(&self) -> Option<Duration>
```

### Steps

Borrow an existing policy and read the configured value.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Explicit legacy per-attempt duration, or None when unset.

### Errors and behavior

Infallible; reading configuration does not schedule retries or reserve capacity.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::total_deadline()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::total_deadline(&self) -> Option<Duration>
```

### Steps

Borrow an existing policy and read the configured value.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Explicit legacy operation deadline, or None when unset.

### Errors and behavior

Infallible; reading configuration does not schedule retries or reserve capacity.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::backoff()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::backoff(&self) -> &Backoff
```

### Steps

Borrow an existing policy and read the configured value.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Borrowed delay rule.

### Errors and behavior

Infallible; reading configuration does not schedule retries or reserve capacity.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::retry_after()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::retry_after(&self) -> RetryAfterMode
```

### Steps

Borrow an existing policy and read the configured value.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Copied server-delay mode.

### Errors and behavior

Infallible; reading configuration does not schedule retries or reserve capacity.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::retry_after_cap()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::retry_after_cap(&self) -> Option<Duration>
```

### Steps

Borrow an existing policy and read the configured value.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Explicit server-delay cap, or None; RespectWithCap then falls back to max_delay in retry_delay().

### Errors and behavior

Infallible; reading configuration does not schedule retries or reserve capacity.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::safety()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::safety(&self) -> &RequestSafety
```

### Steps

Borrow an existing policy and read the configured value.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Borrowed replay declaration.

### Errors and behavior

Infallible; reading configuration does not schedule retries or reserve capacity.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::retry_on()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::retry_on(&self) -> &RetryOn
```

### Steps

Borrow an existing policy and read the configured value.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Borrowed reason classifier.

### Errors and behavior

Infallible; reading configuration does not schedule retries or reserve capacity.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::observer()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::observer(&self) -> Option<RetryObserverHandle>
```

### Steps

Borrow an existing policy and read the configured value.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Cloned shared observer handle, or None if absent.

### Errors and behavior

Infallible; reading configuration does not schedule retries or reserve capacity.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::max_delay()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::max_delay(&self) -> Duration
```

### Steps

Borrow an existing policy and read the configured value.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Configured upper bound for backoff_delay(). PreferServer can select a larger server delay.

### Errors and behavior

Infallible; reading configuration does not schedule retries or reserve capacity.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::with_max_retries()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::with_max_retries(self, max_retries: u32) -> Self
```

### Steps

Apply to an existing policy and retain the returned value.

### Parameters

`max_retries`: retries after the first attempt.

### Callback parameters

None.

### Return values

The updated policy.

### Errors and behavior

Infallible configuration. Clamps to 32 and updates total attempts; does not populate reason sets or change backoff.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::with_retryable_statuses()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::with_retryable_statuses(self, statuses: impl IntoIterator<Item = StatusCode>) -> Self
```

### Steps

Apply to an existing policy and retain the returned value.

### Parameters

`statuses`: the complete replacement status set.

### Callback parameters

None.

### Return values

The updated policy.

### Errors and behavior

Infallible configuration. Empty disables status retries while preserving other reasons.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::with_transport_errors()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::with_transport_errors(self, enabled: bool) -> Self
```

### Steps

Apply to an existing policy and retain the returned value.

### Parameters

`enabled`: whether to enable compatibility transport retries.

### Callback parameters

None.

### Return values

The updated policy.

### Errors and behavior

Infallible configuration. False clears transport categories and the body-read flag. True restores standard transport categories only if empty; it does not re-enable the separate body-read flag.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::with_non_idempotent()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::with_non_idempotent(self, enabled: bool) -> Self
```

### Steps

Apply to an existing policy and retain the returned value.

### Parameters

`enabled`: whether the application asserts idempotence for other methods.

### Callback parameters

None.

### Return values

The updated policy.

### Errors and behavior

Infallible configuration. True selects IdempotentByContract, false Automatic. This does not bypass body replayability checks or add a server idempotency key.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::with_max_delay()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::with_max_delay(self, max_delay: Duration) -> Self
```

### Steps

Apply to an existing policy and retain the returned value.

### Parameters

`max_delay`: backoff bound with explicit Rust units.

### Callback parameters

None.

### Return values

The updated policy.

### Errors and behavior

Infallible configuration. Zero is ignored. Nonzero replaces the bound; PreferServer can still select a larger delay.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::with_application_predicate()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::with_application_predicate(self, predicate: impl Fn(&HttpResponse) -> bool + Send + Sync + 'static) -> Self
```

### Steps

Apply to an existing policy and retain the returned value.

### Parameters

`predicate`: owned thread-safe closure retained by the policy.

### Callback parameters

`predicate: Fn(&HttpResponse) -> bool + Send + Sync + 'static` receives a fully buffered response; true selects an application retry only if reason, attempt and safety checks permit it. It is not evaluated on a delivered stream. Keep it nonblocking and do not assume panic isolation.

### Return values

The updated policy.

### Errors and behavior

Infallible configuration. Replaces the predicate and enables the Application reason. Does not change method safety or attempt budget.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::backoff_delay()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::backoff_delay(&self, retry_ordinal: u32) -> Duration
```

### Steps

Call on a configured policy before interpreting a candidate retry.

### Parameters

`retry_ordinal`: one-based retry number; zero denotes no retry.

### Callback parameters

None.

### Return values

A Duration capped by max_delay.

### Errors and behavior

Infallible. Zero ordinal returns zero; computes only, without sleeping.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::retry_delay()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::retry_delay(&self, retry_ordinal: u32, server_delay: Option<Duration>) -> Duration
```

### Steps

Call on a configured policy before interpreting a candidate retry.

### Parameters

`retry_ordinal`: retry number; `server_delay`: parsed server hint or None.

### Callback parameters

None.

### Return values

Duration selected from backoff and server mode.

### Errors and behavior

Infallible. Ignore uses backoff; PreferServer uses a present server hint without a cap; RespectWithCap uses its explicit cap or max_delay. The hint does not authorize replay.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::allows_method()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::allows_method(&self, method: &Method, replayable_body: bool) -> bool
```

### Steps

Call on a configured policy before interpreting a candidate retry.

### Parameters

`method`: candidate method; `replayable_body`: whether the body can be recreated.

### Callback parameters

None.

### Return values

Whether the method-safety declaration allows replay.

### Errors and behavior

Infallible. This checks safety only. Automatic recognizes GET/HEAD/OPTIONS/PUT/DELETE even with replayable_body=false; the worker separately refuses a required non-replayable body.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::should_retry_status()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::should_retry_status(&self, status: StatusCode) -> bool
```

### Steps

Call on a configured policy before interpreting a candidate retry.

### Parameters

`status`: response code to classify.

### Callback parameters

None.

### Return values

Whether the code belongs to the policy status set.

### Errors and behavior

Infallible. Does not inspect attempt budget, body safety or deadlines.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::should_retry_reason()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::should_retry_reason(&self, reason: RetryReason) -> bool
```

### Steps

Call on a configured policy before interpreting a candidate retry.

### Parameters

`reason`: category to classify.

### Callback parameters

None.

### Return values

Whether the reason classifier permits the category.

### Errors and behavior

Infallible. RetryAfter is never sufficient by itself; classification does not execute work.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::should_retry_error()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::should_retry_error(&self, error: &NetError) -> bool
```

### Steps

Call on a configured policy before interpreting a candidate retry.

### Parameters

`error`: HTTP worker error or another NetError.

### Callback parameters

None.

### Return values

Whether the retained typed error evidence or fallback kind/stage matches allowed categories.

### Errors and behavior

Infallible. Application and RetryAfter do not become retryable transport evidence. Classification alone is not a replay decision.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicy::can_retry_attempt()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicy::can_retry_attempt(&self, attempt: u32) -> bool
```

### Steps

Call on a configured policy before interpreting a candidate retry.

### Parameters

`attempt`: retry ordinal, despite the parameter name; 1 means the first retry.

### Callback parameters

None.

### Return values

True for 1 through max_retries; false for zero or beyond the budget.

### Errors and behavior

Infallible. This is not the one-based attempt number used in RetryEvent; it checks budget only.

### Example

Run the complete [policy inspection example](#offline-policy-inspection-example), which calls this method.

## `RetryPolicyBuilder::max_attempts()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::max_attempts(self, max_attempts: u32) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`max_attempts`: total attempts, legal range 1 through 33.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Default 1. build() returns InvalidMaxAttempts outside that range.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::with_max_attempts()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::with_max_attempts(self, max_attempts: u32) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`max_attempts`: total attempts, legal range 1 through 33.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Default 1. build() returns InvalidMaxAttempts outside that range. Exact alias of `max_attempts`.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::attempt_timeout()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::attempt_timeout(self, timeout: Duration) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`timeout`: positive legacy per-attempt duration.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Initially unset. Zero fails build() with ZeroAttemptTimeout; clock range is checked at timeout resolution. Explicit HttpTimeouts overrides or disables this fallback.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::with_attempt_timeout()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::with_attempt_timeout(self, timeout: Duration) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`timeout`: positive legacy per-attempt duration.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Initially unset. Zero fails build() with ZeroAttemptTimeout; clock range is checked at timeout resolution. Explicit HttpTimeouts overrides or disables this fallback. Exact alias of `attempt_timeout`.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::total_deadline()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::total_deadline(self, deadline: Duration) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`deadline`: positive legacy operation-wide duration.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Initially unset. Zero fails build() with ZeroTotalDeadline; explicit HttpTimeouts can replace or disable the fallback.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::with_total_deadline()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::with_total_deadline(self, deadline: Duration) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`deadline`: positive legacy operation-wide duration.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Initially unset. Zero fails build() with ZeroTotalDeadline; explicit HttpTimeouts can replace or disable the fallback. Exact alias of `total_deadline`.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::backoff()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::backoff(self, backoff: Backoff) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`backoff`: complete replacement delay rule.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Default None. build() rejects empty sequences and exponential maximum below initial. Zero delay is allowed.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::with_backoff()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::with_backoff(self, backoff: Backoff) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`backoff`: complete replacement delay rule.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Default None. build() rejects empty sequences and exponential maximum below initial. Zero delay is allowed. Exact alias of `backoff`.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::retry_on()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::retry_on(self, retry_on: RetryOn) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`retry_on`: complete replacement reason classifier.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Default none(). This replaces every category, so calling after application_predicate can disable Application again.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::with_retry_on()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::with_retry_on(self, retry_on: RetryOn) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`retry_on`: complete replacement reason classifier.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Default none(). This replaces every category, so calling after application_predicate can disable Application again. Exact alias of `retry_on`.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::retry_after()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::retry_after(self, mode: RetryAfterMode) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`mode`: Ignore, PreferServer, or RespectWithCap.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Default Ignore. A server delay never independently authorizes replay.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::with_retry_after()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::with_retry_after(self, mode: RetryAfterMode) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`mode`: Ignore, PreferServer, or RespectWithCap.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Default Ignore. A server delay never independently authorizes replay. Exact alias of `retry_after`.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::retry_after_cap()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::retry_after_cap(self, cap: Duration) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`cap`: positive server-delay cap.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Initially unset. build() rejects zero with ZeroRetryAfterCap even if another mode is selected. Applied only by RespectWithCap.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::with_retry_after_cap()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::with_retry_after_cap(self, cap: Duration) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`cap`: positive server-delay cap.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Initially unset. build() rejects zero with ZeroRetryAfterCap even if another mode is selected. Applied only by RespectWithCap. Exact alias of `retry_after_cap`.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::safety()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::safety(self, safety: RequestSafety) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`safety`: application replay declaration.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Default Automatic. IdempotencyKey does not insert a header; supply the server key yourself.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::with_safety()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::with_safety(self, safety: RequestSafety) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`safety`: application replay declaration.

### Callback parameters

None.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Default Automatic. IdempotencyKey does not insert a header; supply the server key yourself. Exact alias of `safety`.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::observer()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::observer(self, observer: impl RetryObserver) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`observer`: owned thread-safe observer.

### Callback parameters

`observer` implements `RetryObserver: Send + Sync + 'static` and receives `&RetryEvent`, returning (). The policy retains it; invocation is synchronous and handle notification catches unwinding panics. Keep it short and nonblocking.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Initially absent. Replacement drops this builder's old reference; retained policy clones may still share it.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::with_observer()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::with_observer(self, observer: impl RetryObserver) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`observer`: owned thread-safe observer.

### Callback parameters

`observer` implements `RetryObserver: Send + Sync + 'static` and receives `&RetryEvent`, returning (). The policy retains it; invocation is synchronous and handle notification catches unwinding panics. Keep it short and nonblocking.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Initially absent. Replacement drops this builder's old reference; retained policy clones may still share it. Exact alias of `observer`.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::application_predicate()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::application_predicate(self, predicate: impl Fn(&HttpResponse) -> bool + Send + Sync + 'static) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`predicate`: owned thread-safe predicate.

### Callback parameters

`predicate: Fn(&HttpResponse) -> bool + Send + Sync + 'static` receives a fully buffered response; true selects an application retry only if reason, attempt and safety checks permit it. It is not evaluated on a delivered stream. Keep it nonblocking and do not assume panic isolation.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Initially absent. Replaces the callback and enables the Application reason.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::with_application_predicate()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::with_application_predicate(self, predicate: impl Fn(&HttpResponse) -> bool + Send + Sync + 'static) -> Self
```

### Steps

Apply the setter, retain the returned builder, then call build().

### Parameters

`predicate`: owned thread-safe predicate.

### Callback parameters

`predicate: Fn(&HttpResponse) -> bool + Send + Sync + 'static` receives a fully buffered response; true selects an application retry only if reason, attempt and safety checks permit it. It is not evaluated on a delivered stream. Keep it nonblocking and do not assume panic isolation.

### Return values

The updated unvalidated builder.

### Errors and behavior

No immediate Result error; the last setter of this field wins. Initially absent. Replaces the callback and enables the Application reason. Exact alias of `application_predicate`.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryPolicyBuilder::build()`

### Required features

`http-client`.

### Function signature

```text
RetryPolicyBuilder::build(self) -> Result<RetryPolicy, RetryPolicyError>
```

### Steps

Finish the builder and handle the result before installation.

### Parameters

Consumes the builder; no explicit arguments.

### Callback parameters

None.

### Return values

Validated RetryPolicy, or RetryPolicyError.

### Errors and behavior

Rejects InvalidMaxAttempts, EmptyBackoffSequence, InvalidBackoff, ZeroAttemptTimeout, ZeroTotalDeadline and ZeroRetryAfterCap as listed above. No request is started.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `Backoff::constant()`

### Required features

`http-client`.

### Function signature

```text
Backoff::constant(delay: Duration) -> Self
```

### Steps

Construct the delay rule, install on a policy builder and validate with build().

### Parameters

`delay`: fixed duration; zero is permitted.

### Callback parameters

None.

### Return values

Constant(delay).

### Errors and behavior

Infallible constructor. No validation failure.

### Example

Run the complete [delay selection program](#backoff-and-retryaftermode).

## `Backoff::exponential()`

### Required features

`http-client`.

### Function signature

```text
Backoff::exponential(initial: Duration, max: Duration) -> Self
```

### Steps

Construct the delay rule, install on a policy builder and validate with build().

### Parameters

`initial`: first delay; `max`: cap, both explicit durations.

### Callback parameters

None.

### Return values

Exponential { initial, max }.

### Errors and behavior

Infallible constructor. Reversed bounds fail later at policy build, not here. Delay doubles with saturation.

### Example

Run the complete [delay selection program](#backoff-and-retryaftermode).

## `Backoff::sequence()`

### Required features

`http-client`.

### Function signature

```text
Backoff::sequence(delays: impl IntoIterator<Item = Duration>) -> Self
```

### Steps

Construct the delay rule, install on a policy builder and validate with build().

### Parameters

`delays`: finite sequence consumed into owned storage.

### Callback parameters

None.

### Return values

Sequence of collected durations.

### Errors and behavior

Infallible constructor. An empty sequence fails policy build. Reuses the last value after the sequence ends; zero values are permitted.

### Example

Run the complete [delay selection program](#backoff-and-retryaftermode).

## `Backoff::delay_for()`

### Required features

`http-client`.

### Function signature

```text
Backoff::delay_for(&self, retry_ordinal: u32) -> Duration
```

### Steps

Read the chosen delay for an upcoming retry.

### Parameters

`retry_ordinal`: one-based retry number.

### Callback parameters

None.

### Return values

The raw backoff delay.

### Errors and behavior

Infallible; ordinal zero returns zero. Exponential calculation caps its shift at 31, so ordinals above 32 reuse the 32nd exponential delay even if below the configured maximum. An unvalidated empty Sequence returns zero here, although policy build rejects it. Computes a Duration without sleeping. The policy may additionally cap this value.

### Example

Run the complete [delay selection program](#backoff-and-retryaftermode).

## `RetryAfterMode::select()`

### Required features

`http-client`.

### Function signature

```text
RetryAfterMode::select(self, server_delay: Option<Duration>, backoff_delay: Duration, cap: Option<Duration>) -> Duration
```

### Steps

Choose a mode, then pass the parsed hint and fallback delay.

### Parameters

`server_delay`: valid hint or None; `backoff_delay`: fallback; `cap`: optional bound.

### Callback parameters

None.

### Return values

The selected Duration.

### Errors and behavior

Infallible. Ignore selects backoff. PreferServer selects a present hint without clipping. RespectWithCap clips the selected hint or fallback if cap is Some. This function does not parse HTTP headers or sleep.

### Example

Run the complete [delay selection program](#backoff-and-retryaftermode).

## `RetryOn::standard()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::standard() -> Self
```

### Steps

Construct the standard reasons and install them on a policy builder.

### Parameters

None.

### Callback parameters

None.

### Return values

Statuses 408/425/429/500/502/503/504; transport Dns/Connect/TimeoutBeforeSend/ReadReset/BodyRead; body-read enabled, application disabled.

### Errors and behavior

Infallible. Excludes TLS, write, protocol and unknown-delivery categories.

### Example

Run the complete [builder and alias example](#builder-and-alias-example), which calls this method and validates the final policy.

## `RetryOn::none()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::none() -> Self
```

### Steps

Start from no reasons and add only the categories required by the endpoint.

### Parameters

None.

### Callback parameters

None.

### Return values

Empty status/transport sets and false body/application flags.

### Errors and behavior

Infallible. Setting a reason later still does not increase the attempt budget.

### Example

Run the complete [reason selection program](#retryon-retryreason-transporterrorkind-and-requestsafety).

## `RetryOn::with_statuses()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::with_statuses(self, statuses: impl IntoIterator<Item = StatusCode>) -> Self
```

### Steps

Apply to a reason set and retain the returned value.

### Parameters

`statuses`: complete status list.

### Callback parameters

None.

### Return values

Updated reason set.

### Errors and behavior

Infallible. Replaces status membership; empty disables status retries.

### Example

Run the complete [reason selection program](#retryon-retryreason-transporterrorkind-and-requestsafety).

## `RetryOn::with_transport()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::with_transport(self, reasons: impl IntoIterator<Item = TransportErrorKind>) -> Self
```

### Steps

Apply to a reason set and retain the returned value.

### Parameters

`reasons`: complete transport category list.

### Callback parameters

None.

### Return values

Updated reason set.

### Errors and behavior

Infallible. Replaces transport membership independently of the body-read flag.

### Example

Run the complete [reason selection program](#retryon-retryreason-transporterrorkind-and-requestsafety).

## `RetryOn::with_body_read()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::with_body_read(self, enabled: bool) -> Self
```

### Steps

Apply to a reason set and retain the returned value.

### Parameters

`enabled`: dedicated body-read reason flag.

### Callback parameters

None.

### Return values

Updated reason set.

### Errors and behavior

Infallible. Controls BodyRead reason; a separately selected Transport(BodyRead) can also be considered by worker error classification.

### Example

Run the complete [reason selection program](#retryon-retryreason-transporterrorkind-and-requestsafety).

## `RetryOn::with_application()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::with_application(self, enabled: bool) -> Self
```

### Steps

Apply to a reason set and retain the returned value.

### Parameters

`enabled`: application reason flag.

### Callback parameters

None.

### Return values

Updated reason set.

### Errors and behavior

Infallible. True also needs an installed predicate and other retry checks. False disables that reason even with a stored predicate.

### Example

Run the complete [reason selection program](#retryon-retryreason-transporterrorkind-and-requestsafety).

## `RetryOn::allows()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::allows(&self, reason: RetryReason) -> bool
```

### Steps

Inspect the configured reason set.

### Parameters

`reason`: candidate reason.

### Callback parameters

None.

### Return values

Whether it matches the configured categories.

### Errors and behavior

Infallible. RetryAfter always returns false.

### Example

Run the complete [reason selection program](#retryon-retryreason-transporterrorkind-and-requestsafety).

## `RetryOn::statuses()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::statuses(&self) -> &[StatusCode]
```

### Steps

Inspect the configured reason set.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Borrowed status slice.

### Errors and behavior

Infallible. An empty slice means no status membership.

### Example

Run the complete [reason selection program](#retryon-retryreason-transporterrorkind-and-requestsafety).

## `RetryOn::transport()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::transport(&self) -> &[TransportErrorKind]
```

### Steps

Inspect the configured reason set.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Borrowed transport category slice.

### Errors and behavior

Infallible. The dedicated body-read flag is separate.

### Example

Run the complete [reason selection program](#retryon-retryreason-transporterrorkind-and-requestsafety).

## `RetryOn::retries_body_read()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::retries_body_read(&self) -> bool
```

### Steps

Inspect the configured reason set.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

The dedicated body-read flag.

### Errors and behavior

Infallible. Transport(BodyRead) is configured separately.

### Example

Run the complete [reason selection program](#retryon-retryreason-transporterrorkind-and-requestsafety).

## `RetryOn::retries_application()`

### Required features

`http-client`.

### Function signature

```text
RetryOn::retries_application(&self) -> bool
```

### Steps

Inspect the configured reason set.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

The application reason flag.

### Errors and behavior

Infallible. A true flag alone does not supply a predicate.

### Example

Run the complete [reason selection program](#retryon-retryreason-transporterrorkind-and-requestsafety).

## `RetrySetting::resolve()`

### Required features

`http-client`.

### Function signature

```text
RetrySetting::resolve(&self, default: &RetryPolicy) -> RetryPolicy
```

### Steps

Resolve a selection against the client policy.

### Parameters

`default`: borrowed client policy used for Inherit.

### Callback parameters

None.

### Return values

A cloned default for Inherit, no_retry() for None, or cloned explicit Policy.

### Errors and behavior

Infallible; resolution neither changes the source policy nor sends a request.

### Example

Run the complete [request options program](#retrysetting-and-httprequestoptions).

## `HttpRequestOptions::new()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::new() -> Self
```

### Steps

Construct options before passing them to send_with_options or send_wait_with_options.

### Parameters

None.

### Callback parameters

None.

### Return values

Options with Inherit, replayable_body=true, no request ID and inheriting timeouts.

### Errors and behavior

Infallible. Use setters rather than struct literals because inheritance metadata is private.

### Example

Run the complete [request options program](#retrysetting-and-httprequestoptions).

## `HttpRequestOptions::with_retry_policy()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::with_retry_policy(self, retry_policy: RetryPolicy) -> Self
```

### Steps

Apply before submitting a callback request and retain the returned options.

### Parameters

`retry_policy`: explicit replacement, including no_retry().

### Callback parameters

None.

### Return values

Updated request options.

### Errors and behavior

Infallible configuration. Selects Policy and disables client-policy inheritance.

### Example

Run the complete [request options program](#retrysetting-and-httprequestoptions).

## `HttpRequestOptions::with_retry_setting()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::with_retry_setting(self, setting: RetrySetting) -> Self
```

### Steps

Apply before submitting a callback request and retain the returned options.

### Parameters

`setting`: Inherit, None or Policy.

### Callback parameters

None.

### Return values

Updated request options.

### Errors and behavior

Infallible configuration. Last selection wins. Inherit restores client-policy inheritance even after an explicit replacement.

### Example

Run the complete [request options program](#retrysetting-and-httprequestoptions).

## `HttpRequestOptions::with_replayable_body()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::with_replayable_body(self, replayable_body: bool) -> Self
```

### Steps

Apply before submitting a callback request and retain the returned options.

### Parameters

`replayable_body`: whether a fresh body can be produced.

### Callback parameters

None.

### Return values

Updated request options.

### Errors and behavior

Infallible configuration. Must reflect the provider contract. Declaring true cannot manufacture a replayable body.

### Example

Run the complete [request options program](#retrysetting-and-httprequestoptions).

## `HttpRequestOptions::with_request_id()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::with_request_id(self, request_id: HttpRequestId) -> Self
```

### Steps

Apply before submitting a callback request and retain the returned options.

### Parameters

`request_id`: caller-supplied client-local ID.

### Callback parameters

None.

### Return values

Updated request options.

### Errors and behavior

Infallible configuration. Sets Some(id); duplicate active IDs can later reject admission. Automatic allocation is the default when absent.

### Example

Run the complete [request options program](#retrysetting-and-httprequestoptions).

## `HttpRequestOptions::retry_setting()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::retry_setting(&self) -> RetrySetting
```

### Steps

Inspect the current options selection.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

A clone of the retry selection.

### Errors and behavior

Infallible; this is not the policy resolved against a client.

### Example

Run the complete [request options program](#retrysetting-and-httprequestoptions).

## `HttpRequestOptions::with_timeouts()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::with_timeouts(self, timeouts: HttpTimeouts) -> Result<Self, NetError>
```

### Steps

Construct overrides and install before callback submission.

### Parameters

`timeouts`: complete request override layer.

### Callback parameters

None.

### Return values

Updated options or NetError.

### Errors and behavior

InvalidInput for zero or clock-unrepresentable explicit durations. Replaces the whole layer; unspecified phases inherit.

### Example

Run the complete [request options program](#retrysetting-and-httprequestoptions).

## `HttpRequestOptions::timeouts()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestOptions::timeouts(&self) -> &HttpTimeouts
```

### Steps

Borrow the installed options layer.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

A reference to explicit overrides.

### Errors and behavior

Infallible; does not expose resolved deadlines.

### Example

Run the complete [request options program](#retrysetting-and-httprequestoptions).

## `ApplicationPredicate::new()`

### Required features

`http-client`.

### Function signature

```text
ApplicationPredicate::new(predicate: impl Fn(&HttpResponse) -> bool + Send + Sync + 'static) -> Self
```

### Steps

Wrap a thread-safe owned closure. Install predicates using a policy setter accepting a closure.

### Parameters

`predicate`: callback to retain.

### Callback parameters

`predicate: Fn(&HttpResponse) -> bool + Send + Sync + 'static` receives a fully buffered response; true selects an application retry only if reason, attempt and safety checks permit it. It is not evaluated on a delivered stream. Keep it nonblocking and do not assume panic isolation.

### Return values

Cloneable callback wrapper.

### Errors and behavior

Infallible; evaluation is internal. Construction does not perform a request.

### Example

Run the complete [predicate and observation program](#applicationpredicate-and-retry-observation).

## `RetryContext::new()`

### Required features

`http-client`.

### Function signature

```text
RetryContext::new(method: Method, host: impl Into<String>, path: impl AsRef<str>) -> Self
```

### Steps

Construct a sanitized context value for application bookkeeping.

### Parameters

`method`: method; `host`: URL or host; `path`: request path, optionally with query/fragment.

### Callback parameters

None.

### Return values

Context with attempt=1, supplied method, sanitized host/path, empty status/reason/remaining and zero elapsed.

### Errors and behavior

Infallible. Host excludes scheme/credentials/path/query/fragment; path excludes query/fragment. Observers receive RetryEvent, not this context.

### Example

Run the complete [predicate and observation program](#applicationpredicate-and-retry-observation).

## `RetryReport::new()`

### Required features

`http-client`.

### Function signature

```text
RetryReport::new() -> Self
```

### Steps

Construct a blank report before filling application-owned fields.

### Parameters

None.

### Callback parameters

None.

### Return values

Zero attempts/retry_count/elapsed, exhausted=false, no reason or last_error.

### Errors and behavior

Infallible; constructing a report does not observe a live operation.

### Example

Run the complete [custom observer example](#custom-observer-example).

## `RetryObserver::observe()`

### Required features

`http-client`.

### Function signature

```text
RetryObserver::observe(&self, event: &RetryEvent)
```

### Steps

Implement the trait, then install the observer using a policy builder or handle.

### Parameters

`event`: borrowed attempt/retry/completion event; clone data if retaining it.

### Callback parameters

Receives &RetryEvent. RetryScheduled.attempt identifies the attempt just completed, not the upcoming retry. Return promptly to avoid delaying the request.

### Return values

() when the callback returns.

### Errors and behavior

Synchronous callback. Direct calls have no wrapper panic protection; RetryObserverHandle::notify catches unwinding panics. A Completed report exhausted flag is not a success/failure signal.

### Example

Run the complete [custom observer example](#custom-observer-example).

## `RetryObserverHandle::new()`

### Required features

`http-client`.

### Function signature

```text
RetryObserverHandle::new(observer: impl RetryObserver) -> Self
```

### Steps

Wrap an implementation or compatible closure.

### Parameters

`observer`: owned thread-safe callback implementation.

### Callback parameters

`observer` implements `RetryObserver: Send + Sync + 'static` and receives `&RetryEvent`, returning (). The policy retains it; invocation is synchronous and handle notification catches unwinding panics. Keep it short and nonblocking.

### Return values

A cloneable shared handle.

### Errors and behavior

Infallible; clones share the observer and there is no unsubscribe API.

### Example

Run the complete [custom observer example](#custom-observer-example).

## `RetryObserverHandle::notify()`

### Required features

`http-client`.

### Function signature

```text
RetryObserverHandle::notify(&self, event: RetryEvent)
```

### Steps

Deliver an owned event to the retained observer.

### Parameters

`event`: synthetic or recorded RetryEvent.

### Callback parameters

Invokes the retained observer with &event during this call.

### Return values

().

### Errors and behavior

Infallible Result contract; catches/logs unwinding observer panics. Synchronous notification never submits or retries a request.

### Example

Run the complete [custom observer example](#custom-observer-example).

## `HttpClientConfig::with_default_retry_policy()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_default_retry_policy(self, policy: RetryPolicy) -> Self
```

### Steps

Set before client construction or request build, retaining the returned value.

### Parameters

`policy`: default policy for inherited requests.

### Callback parameters

None.

### Return values

Updated `HttpClientConfig`.

### Errors and behavior

Infallible setting. Initial default is no_retry(). Replaces the client policy; explicit request selections override it.

### Example

Run the complete [retry installation program](#installing-retry-selections-example).

## `HttpRequestBuilder::retry_policy()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestBuilder::retry_policy(self, policy: RetryPolicy) -> Self
```

### Steps

Set before client construction or request build, retaining the returned value.

### Parameters

`policy`: explicit policy for one concrete request.

### Callback parameters

None.

### Return values

Updated `HttpRequestBuilder`.

### Errors and behavior

Infallible setting. Selects Policy, including explicit no_retry(); call build() to finish the request.

### Example

Run the complete [retry installation program](#installing-retry-selections-example).

## `HttpRequestBuilder::retry_setting()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestBuilder::retry_setting(self, setting: RetrySetting) -> Self
```

### Steps

Set before client construction or request build, retaining the returned value.

### Parameters

`setting`: Inherit, None or Policy.

### Callback parameters

None.

### Return values

Updated `HttpRequestBuilder`.

### Errors and behavior

Infallible setting. Last selection wins; Inherit restores client-policy inheritance. Call build() to finish the request.

### Example

Run the complete [retry installation program](#installing-retry-selections-example).

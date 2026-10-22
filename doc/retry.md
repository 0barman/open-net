# Shared retry schedules and observations

Required features: none. The public API is `open_net::api::retry`, also available
as `open_net::retry`. It ships inside the `open-net` crate. A schedule is immutable
configuration; each operation owns its counters, random state, deadline and
cancellation. Sharing a policy never shares an attempt budget between requests.

Every complete program on this page uses this dependency:

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false }
```

Use a schedule inside [HTTP retry policies](http/retry.md) or
[WebSocket retry policies](websocket/retry.md). The shared API does not send
requests, own a runtime, classify protocol errors, or authorize replay.
The packaged [retry_schedule example](../examples/retry_schedule.rs) also runs
without transport features:

These commands run in the matching library source directory containing its
`Cargo.toml`, after obtaining that source. Adding a dependency to an application
does not install the library's examples into that application.

```sh
cargo run --manifest-path Cargo.toml --no-default-features --example retry_schedule
```

## API migration map

| Area | Source/API change | Usage and reference |
| --- | --- | --- |
| Shared retry types | New `open_net::api::retry` module; no separate crate or transport feature | [Schedules, settings and observations](#configure-a-schedule) |
| Common imports | `Backoff`, `RetrySetting`, `RetryContext`, `RetryEvent`, `RetryObserver`, `RetryObserverHandle` and `RetryReport` move from `open_net::api::http` to `open_net::api::retry`; the old HTTP re-exports are removed | [HTTP migration table](http/retry.md#migration-from-the-previous-api) |
| Exponential backoff | `Backoff::exponential(initial, max)` becomes `Backoff::exponential(initial, multiplier, max)`; use `2.0` for the previous doubling factor. `Backoff::Exponential` also gains the `multiplier` field | [Exponential construction](#backoffexponential) |
| Backoff lookup | `Backoff::delay_for(ordinal)` now returns `Result<Duration, RetryConfigError>`; handle the result. Ordinal zero and sequence indices beyond the configured delays are errors; the last sequence delay is no longer repeated | [Delay lookup](#backoffdelay_for) |
| Setting resolution | `RetrySetting::resolve(&default)` becomes `RetrySetting<P>::resolve(&default, disabled)`; pass the protocol's no-retry policy as the owned second argument | [Setting resolution](#retrysettingresolve) |
| HTTP policy | `RetryPolicy` becomes `HttpRetryPolicy` composed with `RetrySchedule`; old count cap and compatibility setters removed | [HTTP migration table](http/retry.md#migration-from-the-previous-api) |
| HTTP request configuration | Request builder, options and trait use one `RetrySetting<HttpRetryPolicy>`; duplicate retry_policy fields/setters removed | [Request settings](http/retry.md#request-settings-and-callback-adapters) |
| HTTP completion/reporting | `HttpCompletion` gains a common report field/getter; `HttpResponse` and `NetError` gain optional report getters | [Completion](http/callback_requests.md#httpcompletionretry_report) |
| HTTP classification/timeouts | Boolean predicate becomes a three-way classifier; timeouts live only in `HttpTimeouts`; Retry-After has Ignore/Respect | [HTTP classifier](http/retry.md#classify-a-business-response) |
| WS message policy | `SendRetryPolicy` becomes `WsSendRetryPolicy`; `SendOptions.retry` becomes `retry_setting` | [WS migration table](websocket/retry.md#api-migration) |
| WS defaults | `WebSocketClientConfig` gains separate default send/request policy fields and setters | [WS policy setup](websocket/retry.md#configure-policies-offline) |
| WS logical requests | New factory, attempt context, operation options/builder/receipt/handle APIs; fixed `Request` remains single-attempt | [WS identity and lifecycle](websocket/retry.md#identity-delivery-and-lifecycle) |
| WS task observers | `TaskSource` gains `RequestOperation(Arc<Metadata>)`; exhaustive external matches must handle the new variant | [WS migration table](websocket/retry.md#api-migration) |
| WS timeout classification | New non-exhaustive `WsTimeoutPhase`, `NetError::ws_timeout_phase()` and `ErrorContext::ws_timeout_phase`; whole-attempt retry has its own opt-in switch | [WS timeout getter](websocket/retry.md#neterrorws_timeout_phase) |
| WS RetryDecision name | Existing `open_net::ws::RetryDecision` still describes connection retry events; use `open_net::api::retry::RetryDecision` for classifiers | [WS imports](websocket/retry.md#api-migration) |

HTTP request/result and consuming callback signatures remain unchanged. Request
retry configuration, policy names and configuration struct literals require the
source updates listed above. New API examples are complete programs in the linked
guides; existing connection-retry policy semantics are retained.

## Attempt and delay rules

`max_attempts` is a positive `u32` including the first attempt. The first attempt
starts without a backoff delay. There is no legacy limit of 33 attempts. The SDK
creates runtime state lazily; a large attempt limit does not allocate a delay
vector. `Backoff::Sequence` requires exactly `max_attempts - 1` entries and never
repeats the last entry. Zero delays are valid.

| Backoff | Delay before retry ordinal `n`, starting at one |
| --- | --- |
| `None` | Zero |
| `Constant(Duration)` | The same duration for every retry |
| `Sequence(Vec<Duration>)` | Entry `n - 1` |
| `Exponential { initial, multiplier, max }` | `initial * multiplier^(n - 1)`, capped at `max` |

The exponential multiplier must be finite and at least one; its maximum must be
at least its initial duration. Duration values must fit the monotonic clock.
Only these built-in backoff strategies are supported.

| Jitter | Behavior |
| --- | --- |
| `None` | Default; preserve the configured delay |
| `Full` | Sample uniformly between zero and the configured delay |
| `Proportional { ratio }` | Sample a factor between `1 - ratio` and `1 + ratio`; ratio must be finite and in `0..=1` |

Exponential jitter still respects the exponential maximum. Jitter applies to the
local delay before any protocol rule such as HTTP `Retry-After`. The public
`delay_for` methods return the deterministic delay without jitter.

## Configure a schedule

```rust
use open_net::api::retry::{Backoff, Jitter, RetrySchedule};
use std::{error::Error, time::Duration};

fn main() -> Result<(), Box<dyn Error>> {
    let schedule = RetrySchedule::builder()
        .max_attempts(3)
        .backoff(Backoff::sequence([
            Duration::from_millis(200),
            Duration::from_millis(500),
        ]))
        .jitter(Jitter::None)
        .build()?;
    if schedule.max_attempts() != 3
        || schedule.delay_for(2)? != Duration::from_millis(500)
    {
        return Err(std::io::Error::other("unexpected schedule").into());
    }
    println!("attempts={}, jitter={:?}, backoff={:?}",
        schedule.max_attempts(), schedule.jitter(), schedule.backoff());
    let _constant = Backoff::constant(Duration::from_millis(100));
    let exponential = Backoff::exponential(
        Duration::from_millis(100), 2.0, Duration::from_secs(2));
    println!("third retry delay: {:?}", exponential.delay_for(3)?);
    let disabled = RetrySchedule::no_retry();
    if disabled.max_attempts() != 1 {
        return Err(std::io::Error::other("single attempt expected").into());
    }
    Ok(())
}
```

## Reject invalid configuration

```rust
use open_net::api::retry::{RetryConfigError, RetrySchedule};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    match RetrySchedule::builder().max_attempts(0).build() {
        Err(error) if error.field() == "max_attempts" => {
            println!("{}: {}", error.field(), error.reason());
        }
        Err(error) => return Err(error.into()),
        Ok(_) => return Err(std::io::Error::other("zero attempts accepted").into()),
    }
    let application_error = RetryConfigError::new("application.retry", "invalid value");
    println!("{application_error}");
    Ok(())
}
```

## Policy inheritance and classification

`RetrySetting<P>` has three choices. `Inherit` uses the client policy, `Disabled`
allows at most one attempt, and `Policy(P)` replaces the complete policy. Policy
fields are never merged. Its default is `Inherit`.

A protocol classifier returns `RetryDecision::Default`, `Retry`, or `Stop`.
`Stop` vetoes built-in retry reasons, including an eligible HTTP auth replay.
`Retry` still obeys replay safety, cancellation, shutdown, the total deadline and
the shared attempt budget. A classifier is synchronous and read-only; keep it
short. Returning `Stop` controls scheduling, not the interpretation of a response
as application success.

```rust
use open_net::api::retry::{RetryDecision, RetrySchedule, RetrySetting};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let client = RetrySchedule::builder().max_attempts(4).build()?;
    let selection = RetrySetting::<RetrySchedule>::Inherit;
    let resolved = selection.resolve(&client, RetrySchedule::no_retry());
    let disabled = RetrySetting::<RetrySchedule>::Disabled
        .resolve(&client, RetrySchedule::no_retry());
    if resolved.max_attempts() != 4 || disabled.max_attempts() != 1 {
        return Err(std::io::Error::other("incorrect inheritance").into());
    }
    let _decision = RetryDecision::Default;
    Ok(())
}
```

## Reports and observation

`RetryContext` contains `attempt: u32`, `elapsed: Duration`, and
`remaining: Option<Duration>`. Attempt zero means no attempt has started. The
HTTP and WS classifiers add their own response/error metadata to this context.

| `RetryReport` field and getter | Meaning |
| --- | --- |
| `attempts` / `attempts()` | Started attempts, including the first and HTTP authentication replays |
| `retry_count` / `retry_count()` | Started attempts after the first |
| `transport_sends` / `transport_sends()` | Protocol send dispatches; HTTP redirect hops count separately; preparation failure adds no send |
| `elapsed` / `elapsed()` | Time since SDK submission started when the report was sampled |
| `stop_reason` / `stop_reason()` | `None` while running, otherwise the terminal scheduling reason |

`RetryStopReason` is non-exhaustive. Its current variants are `Completed`,
`NotRetryable`, `ClassifierStopped`, `AttemptsExhausted`, `Disabled`,
`UnsafeToReplay`, `Cancelled`, `TimedOut`, `Shutdown`, and `PolicyFailed`.
`Completed` describes SDK completion; a received response can still contain a
business failure. Exhaustion preserves the final response or error together with
the report instead of replacing it with a generic retry error.

| `RetryEvent` variant | Meaning |
| --- | --- |
| `AttemptStarted { attempt }` | A logical attempt started |
| `AttemptFinished { attempt, final_attempt }` | That attempt finished; `final_attempt` marks the final one |
| `RetryScheduled { attempt, delay }` | Delay after the identified completed attempt, before the next attempt |
| `Completed(RetryReport)` | One terminal report for the logical operation |

Reports/events contain no payload, credentials or arbitrary error text. Inspect
the response/error separately when needed. Observers execute synchronously and
should not block. Observer unwinds are isolated and logged; they do not change
the request result. Cloning `RetryObserverHandle` shares the observer, but never
shares runtime retry counters.

```rust
use open_net::api::retry::{
    RetryContext, RetryEvent, RetryObserver, RetryObserverHandle, RetryReport,
};
use std::time::Duration;

struct Observer;
impl RetryObserver for Observer {
    fn observe(&self, event: &RetryEvent) {
        println!("{event:?}");
    }
}

fn main() {
    let context = RetryContext {
        attempt: 0, elapsed: Duration::ZERO, remaining: None,
    };
    println!("attempt={}, remaining={:?}", context.attempt, context.remaining);
    let report = RetryReport::new();
    println!("attempts={}, retries={}, sends={}, elapsed={:?}, stop={:?}",
        report.attempts(), report.retry_count(), report.transport_sends(),
        report.elapsed(), report.stop_reason());
    Observer.observe(&RetryEvent::AttemptStarted { attempt: 1 });
    RetryObserverHandle::new(Observer).notify(RetryEvent::Completed(report));
}
```

The last example emits synthetic events solely to demonstrate the observer API.
Installing an observer on a protocol policy receives actual operation events.

## `Backoff::constant()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
Backoff::constant(delay: Duration) -> Self
```

### Steps

Choose one delay and install this value on a schedule.

### Parameters

`delay`: the interval before every retry.

### Callback parameters

None.

### Return values

A constant built-in backoff.

### Errors and behavior

Construction stores the duration; schedule construction validates its clock range.

### Example

See the complete [example](#configure-a-schedule).

## `Backoff::sequence()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
Backoff::sequence(delays: impl IntoIterator<Item = Duration>) -> Self
```

### Steps

Provide one delay for each possible retry, then build the schedule.

### Parameters

`delays`: exactly `max_attempts - 1` entries once installed.

### Callback parameters

None.

### Return values

An owned explicit delay sequence.

### Errors and behavior

Construction collects entries. Schedule construction rejects a length mismatch or unrepresentable duration. Entries are never repeated implicitly.

### Example

See the complete [example](#configure-a-schedule).

## `Backoff::exponential()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
Backoff::exponential(initial: Duration, multiplier: f64, max: Duration) -> Self
```

### Steps

Choose an initial delay, multiplier and cap, then install on a schedule.

### Parameters

`initial`: first retry delay; `multiplier`: finite and at least one; `max`: at least `initial`.

### Callback parameters

None.

### Return values

An exponential built-in backoff.

### Errors and behavior

Construction stores values. Building a schedule validates values and clock range; calculation saturates at the configured cap without overflowing.

### Example

See the complete [example](#configure-a-schedule).

## `Backoff::delay_for()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
Backoff::delay_for(&self, retry_ordinal: u32) -> Result<Duration, RetryConfigError>
```

### Steps

Calculate a deterministic delay for inspection.

### Parameters

`retry_ordinal`: one for the delay before attempt two.

### Callback parameters

None.

### Return values

The delay without jitter.

### Errors and behavior

Rejects zero, invalid backoff values, out-of-range sequence indices and arithmetic/clock overflow. This method does not enforce a schedule attempt budget.

### Example

See the complete [example](#configure-a-schedule).

## `RetrySchedule::builder()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetrySchedule::builder() -> RetryScheduleBuilder
```

### Steps

Create the builder, select settings and call `build()`.

### Parameters

None.

### Callback parameters

None.

### Return values

A builder with one attempt, `Backoff::None` and `Jitter::None`.

### Errors and behavior

No transport work or runtime allocation occurs.

### Example

See the complete [example](#configure-a-schedule).

## `RetrySchedule::no_retry()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetrySchedule::no_retry() -> Self
```

### Steps

Use when constructing a protocol policy with one attempt.

### Parameters

None.

### Callback parameters

None.

### Return values

A schedule with one immediate attempt and no retry delay.

### Errors and behavior

Equivalent to the default schedule; it does not disable protocol redirects.

### Example

See the complete [example](#configure-a-schedule).

## `RetrySchedule::max_attempts()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetrySchedule::max_attempts(&self) -> u32
```

### Steps

Read the validated limit when displaying policy configuration.

### Parameters

None.

### Callback parameters

None.

### Return values

Total attempts including the first.

### Errors and behavior

Always positive; this is a configuration limit, not the running attempt count.

### Example

See the complete [example](#configure-a-schedule).

## `RetrySchedule::backoff()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetrySchedule::backoff(&self) -> &Backoff
```

### Steps

Borrow the installed built-in backoff.

### Parameters

None.

### Callback parameters

None.

### Return values

A shared reference to the backoff configuration.

### Errors and behavior

Read-only; changing a policy requires building another schedule.

### Example

See the complete [example](#configure-a-schedule).

## `RetrySchedule::jitter()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetrySchedule::jitter(&self) -> Jitter
```

### Steps

Read the configured randomization mode.

### Parameters

None.

### Callback parameters

None.

### Return values

A copy of the jitter configuration.

### Errors and behavior

Does not sample randomness or expose an operation random stream.

### Example

See the complete [example](#configure-a-schedule).

## `RetrySchedule::delay_for()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetrySchedule::delay_for(&self, retry_ordinal: u32) -> Result<Duration, RetryConfigError>
```

### Steps

Inspect the deterministic delay before a retry within the budget.

### Parameters

`retry_ordinal`: in `1..max_attempts`.

### Callback parameters

None.

### Return values

The delay without jitter.

### Errors and behavior

Zero or an ordinal greater than or equal to `max_attempts` returns `RetryConfigError`. Calculation does not consume an attempt.

### Example

See the complete [example](#configure-a-schedule).

## `RetryScheduleBuilder::max_attempts()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryScheduleBuilder::max_attempts(self, count: u32) -> Self
```

### Steps

Set the complete attempt limit, retain the returned builder, then build.

### Parameters

`count`: first attempt plus retries; must be positive.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

Validation occurs at `build()`. There is no clamping to the old 33-attempt limit.

### Example

See the complete [example](#configure-a-schedule).

## `RetryScheduleBuilder::backoff()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryScheduleBuilder::backoff(self, backoff: Backoff) -> Self
```

### Steps

Replace the built-in delay rule and retain the returned builder.

### Parameters

`backoff`: the complete replacement configuration.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

The last setter wins. A sequence is validated against the final attempt count at `build()`.

### Example

See the complete [example](#configure-a-schedule).

## `RetryScheduleBuilder::jitter()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryScheduleBuilder::jitter(self, jitter: Jitter) -> Self
```

### Steps

Replace the randomization setting and retain the returned builder.

### Parameters

`jitter`: `None`, `Full`, or `Proportional { ratio }`.

### Callback parameters

None.

### Return values

The updated builder.

### Errors and behavior

The default is `None`. Invalid ratios and unrepresentable randomized delays are rejected by `build()`.

### Example

See the complete [example](#configure-a-schedule).

## `RetryScheduleBuilder::build()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryScheduleBuilder::build(self) -> Result<RetrySchedule, RetryConfigError>
```

### Steps

Validate the builder and install the returned schedule in a protocol policy.

### Parameters

None.

### Callback parameters

None.

### Return values

An immutable schedule or a structured configuration error.

### Errors and behavior

Rejects zero attempts, sequence length mismatch, invalid exponential settings/jitter ratios and unrepresentable durations. No per-attempt vector is allocated for constant or exponential strategies.

### Example

See the complete [example](#reject-invalid-configuration).

## `RetrySetting::resolve()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetrySetting<P>::resolve(&self, default: &P, disabled: P) -> P where P: Clone
```

### Steps

Supply the client policy and the protocol-specific one-attempt policy.

### Parameters

`default`: inherited policy; `disabled`: the policy to use for `Disabled`.

### Callback parameters

None.

### Return values

A cloned inherited/explicit policy, or the supplied disabled policy.

### Errors and behavior

Resolves the whole policy without merging fields. Protocol executors ordinarily perform this step for callers.

### Example

See the complete [example](#policy-inheritance-and-classification).

## `RetryConfigError::new()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryConfigError::new(field: impl Into<String>, reason: impl Into<String>) -> Self
```

### Steps

Create a structured configuration failure when implementing policy validation.

### Parameters

`field`: rejected configuration key; `reason`: explanation.

### Callback parameters

None.

### Return values

An owned error implementing `std::error::Error`.

### Errors and behavior

Stores supplied strings without validation. Keep credentials and request payloads out of configuration error text.

### Example

See the complete [example](#reject-invalid-configuration).

## `RetryConfigError::field()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryConfigError::field(&self) -> &str
```

### Steps

Inspect the failing key when handling a build error.

### Parameters

None.

### Callback parameters

None.

### Return values

The stored configuration field name.

### Errors and behavior

Borrowed from the error; does not alter its classification.

### Example

See the complete [example](#reject-invalid-configuration).

## `RetryConfigError::reason()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryConfigError::reason(&self) -> &str
```

### Steps

Read the validation explanation when presenting configuration feedback.

### Parameters

None.

### Callback parameters

None.

### Return values

The stored explanation.

### Errors and behavior

Borrowed from the error. It describes configuration validation, not a network result.

### Example

See the complete [example](#reject-invalid-configuration).

## `RetryReport::new()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryReport::new() -> Self
```

### Steps

Construct an empty report for synthetic observations or local state.

### Parameters

None.

### Callback parameters

None.

### Return values

Zero attempts/retries/sends/elapsed and no stop reason.

### Errors and behavior

Creating a report does not create an operation or cause callbacks.

### Example

See the complete [example](#reports-and-observation).

## `RetryReport::attempts()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryReport::attempts(&self) -> u32
```

### Steps

Read how many attempts have started.

### Parameters

None.

### Callback parameters

None.

### Return values

The attempt count including the initial attempt.

### Errors and behavior

Zero is possible before transport work starts. It is distinct from actual send dispatches.

### Example

See the complete [example](#reports-and-observation).

## `RetryReport::retry_count()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryReport::retry_count(&self) -> u32
```

### Steps

Read how many additional attempts have started.

### Parameters

None.

### Callback parameters

None.

### Return values

The retry count excluding the first attempt.

### Errors and behavior

Scheduling a delay alone does not count as starting another attempt.

### Example

See the complete [example](#reports-and-observation).

## `RetryReport::transport_sends()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryReport::transport_sends(&self) -> u64
```

### Steps

Read actual protocol dispatch count separately from logical attempts.

### Parameters

None.

### Callback parameters

None.

### Return values

The number of protocol-level send dispatches.

### Errors and behavior

HTTP redirect hops count; request preparation failures do not. This does not count network packets or WebSocket frames.

### Example

See the complete [example](#reports-and-observation).

## `RetryReport::elapsed()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryReport::elapsed(&self) -> Duration
```

### Steps

Read elapsed duration when displaying a sampled report.

### Parameters

None.

### Callback parameters

None.

### Return values

Duration from SDK submission until sampling.

### Errors and behavior

This is a snapshot, not a clock that continues advancing after the report is cloned.

### Example

See the complete [example](#reports-and-observation).

## `RetryReport::stop_reason()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryReport::stop_reason(&self) -> Option<RetryStopReason>
```

### Steps

Inspect the terminal scheduling outcome.

### Parameters

None.

### Callback parameters

None.

### Return values

A stop reason, or `None` while running/before an operation exists.

### Errors and behavior

The reason does not decide application business success. Match non-exhaustive reasons with a fallback.

### Example

See the complete [example](#reports-and-observation).

## `RetryObserver::observe()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryObserver::observe(&self, event: &RetryEvent)
```

### Steps

Implement a short, synchronous observer or use a thread-safe closure.

### Parameters

`event`: the current read-only retry event.

### Callback parameters

`event: &RetryEvent`; observer implementations return `()`.

### Return values

No return value.

### Errors and behavior

Notification through the SDK handle isolates and logs unwinding callback failures. A direct user call to a custom implementation has ordinary Rust call semantics.

### Example

See the complete [example](#reports-and-observation).

## `RetryObserverHandle::new()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryObserverHandle::new(observer: impl RetryObserver) -> Self
```

### Steps

Wrap a thread-safe observer for shared policy ownership.

### Parameters

`observer`: implements `RetryObserver`, including compatible closures.

### Callback parameters

`event: &RetryEvent`; observer implementations return `()`.

### Return values

A cloneable observer handle.

### Errors and behavior

Clones share observer ownership; they do not share retry state. Debug formatting omits the callback.

### Example

See the complete [example](#reports-and-observation).

## `RetryObserverHandle::notify()`

### Required features

None. Import from `open_net::api::retry`.

### Function signature

```text
RetryObserverHandle::notify(&self, event: RetryEvent)
```

### Steps

Deliver a recorded or synthetic observation.

### Parameters

`event`: an owned event passed by reference to the observer.

### Callback parameters

`event: &RetryEvent`; observer implementations return `()`.

### Return values

No return value.

### Errors and behavior

Observer unwinds are isolated and logged. The observer must remain short and nonblocking because delivery is synchronous.

### Example

See the complete [example](#reports-and-observation).

## `NetError::retry_report()`

### Required features

None. Import `NetError` from `open_net`.

### Function signature

```text
NetError::retry_report(&self) -> Option<&RetryReport>
```

### Steps

When a logical operation returns an error, inspect the retained report alongside
its original kind, context and source.

### Parameters

None.

### Callback parameters

None.

### Return values

The report attached by the retry executor, or `None` for errors without an
operation report, including manually constructed errors.

### Errors and behavior

The getter does not replace or wrap the underlying failure. Cloning `NetError`
retains the report and error source. A post-header HTTP body failure can retain
HTTP status 200 while still being an error; do not treat status alone as success.

### Example

```rust
use open_net::NetError;

fn main() {
    let error = NetError::protocol("example protocol error");
    match error.retry_report() {
        Some(report) => println!("attempts={}, stop={:?}",
            report.attempts(), report.stop_reason()),
        None => println!("No retry operation is attached to this error"),
    }
}
```

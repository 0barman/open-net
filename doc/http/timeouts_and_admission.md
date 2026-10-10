# Timeouts and admission

Required features: `http-client`. There is no separate timeout or admission feature.
Every Rust block below is a complete program. Use this consumer configuration,
with the path adjusted to your checkout:

```toml
[package]
name = "timeouts-example"
version = "0.1.0"
edition = "2021"

[dependencies]
open-net = { path = "../open-net", default-features = false, features = ["http-client"] }
tokio = { version = "1.53.1", features = ["macros", "rt-multi-thread"] }
```

See [configuration](configuration.md) for queue defaults and [callback requests](callback_requests.md)
for ownership when a submission is rejected. `async` creates a future;
`.await` lets the runtime make progress while waiting. `?` returns an error to the
caller. Poll request/admission and shutdown futures inside an active Tokio runtime
with its drivers enabled; a bounded admission wait creates a Tokio timer. See
[the runtime contract and synchronous bridge](README.md) for the full boundary.
Durations below use `std::time::Duration`, not an implicit integer unit.

## HttpTimeouts

Use phase budgets to bound admission, response headers, body progress, individual
attempts, or the entire logical operation.

### Required features

`http-client`.

### Function signatures and parameters

All builders consume `self` and return `Self`; keep the returned value. They do
not start a timer. Each `timeout` argument is a required `Duration`, nonzero and
representable by the monotonic clock. No callback parameters apply.

| Function signature | Parameters | Effect |
| --- | --- | --- |
| `HttpTimeouts::new() -> Self` | None | Every field inherits; also `Default::default()` |
| `with_admission_timeout(self, timeout: Duration) -> Self` | `timeout`, required | Bound the entire wait for capacity and a queue slot |
| `without_admission_timeout(self) -> Self` | None | Explicitly disable admission, including its 30-second fallback |
| `with_headers_timeout(self, timeout: Duration) -> Self` | `timeout`, required | Bound transport execution until final response headers, including redirects |
| `without_headers_timeout(self) -> Self` | None | Disable the headers budget |
| `with_idle_read_timeout(self, timeout: Duration) -> Self` | `timeout`, required | Bound waiting for nonempty body bytes, including the first byte |
| `without_idle_read_timeout(self) -> Self` | None | Disable the body progress budget |
| `with_attempt_timeout(self, timeout: Duration) -> Self` | `timeout`, required | Bound preparation and execution of one attempt through body completion |
| `without_attempt_timeout(self) -> Self` | None | Disable attempt timeout even if legacy settings supply one |
| `with_total_timeout(self, timeout: Duration) -> Self` | `timeout`, required | Bound the operation including admission, middleware, retries and body |
| `without_total_timeout(self) -> Self` | None | Disable total timeout even if the retry policy supplies one |

### Steps

1. Construct overrides with `new()` and the desired phase setters.
2. Install them using `HttpClientConfig::with_timeouts`,
   `HttpRequest::builder().timeouts(...)`, or `HttpRequestOptions::with_timeouts`.
3. Build the client or request and handle validation errors before sending.
4. Inspect `NetError::timeout_phase()` when a request fails.
5. Release response streams promptly and shut down the client after use.

### Return values and errors

The override builders return settings and cannot fail immediately. Installing
client settings returns `Result<HttpClientConfig, NetError>`; invalid durations
are `InvalidConfig`. Installing callback options returns
`Result<HttpRequestOptions, NetError>`; invalid durations are `InvalidInput`.
The request builder returns itself and checks durations in `build()`, returning
`InvalidInput`. Zero does not mean disabled: use `without_*_timeout()`.

`config.timeouts()`, `request.timeouts()` and `options.timeouts()` return
`&HttpTimeouts`, containing explicit overrides, not a computed runtime deadline.
There are no public individual phase getters or public `validate` on `HttpTimeouts`.

### Defaults, precedence, and behavior

For each field, an explicit request value or explicit disable wins; an inherited
request field uses the client field. If both inherit:

| Budget | Fallback |
| --- | --- |
| Admission | 30 seconds, independent of the legacy request timeout |
| Headers | Disabled |
| IdleRead | Disabled |
| Attempt | Client `timeout` (30 seconds initially), or the smaller of it and the retry policy's attempt timeout |
| Total | Retry policy `total_deadline`, otherwise disabled |
| Connect | Separate client connection setting, initially disabled; not a field of `HttpTimeouts` |

Replacing a `HttpTimeouts` object replaces that entire layer, not just the fields
you set on the new object. Within one object the last setter for a phase wins.
An explicit disable overrides inherited fallback values. Disabling Admission does
not disable Total. Unrelated budgets continue running and any can end the operation.
For response futures, budgets and admission policy are resolved at first poll,
before middleware. Middleware cannot retroactively change admission.

Connect includes DNS, proxy setup and TLS for a new connection. Configure it with
`HttpClientConfig::with_connect_timeout(Duration) -> Result<Self, NetError>`;
`without_connect_timeout() -> Self` disables it and
`connect_timeout() -> Option<Duration>` reads it (`None` means disabled).

IdleRead resets only on delivered nonempty body bytes. Keeping a stream without
polling it still consumes its time budgets. The stream supervisor can retire
capacity on timeout without waiting for the next application poll. A timeout
reports the budget that expired; it is not necessarily the transport stage that
was executing at that moment.

### Example

This program exercises construction, replacement, disabling, and accessors
without contacting a server. It should exit successfully.

```rust
use open_net::api::http::{HttpClientConfig, HttpRequest, HttpRequestOptions, HttpTimeouts};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let overrides = HttpTimeouts::new()
        .with_admission_timeout(Duration::from_secs(2))
        .with_headers_timeout(Duration::from_secs(4))
        .with_idle_read_timeout(Duration::from_secs(3))
        .with_attempt_timeout(Duration::from_secs(8))
        .with_total_timeout(Duration::from_secs(12));
    let config = HttpClientConfig::new("http://127.0.0.1:8080")?
        .with_timeouts(overrides.clone())?
        .with_connect_timeout(Duration::from_secs(1))?;
    assert_eq!(config.connect_timeout(), Some(Duration::from_secs(1)));
    assert_eq!(config.timeouts(), &overrides);
    let config = config.without_connect_timeout();
    assert_eq!(config.connect_timeout(), None);
    let disabled = HttpTimeouts::new()
        .without_admission_timeout()
        .without_headers_timeout()
        .without_idle_read_timeout()
        .without_attempt_timeout()
        .without_total_timeout();
    let request = HttpRequest::builder().path("/status").timeouts(disabled.clone()).build()?;
    let options = HttpRequestOptions::new().with_timeouts(disabled.clone())?;
    assert_eq!(request.timeouts(), &disabled);
    assert_eq!(options.timeouts(), &disabled);
    Ok(())
}
```

## TimeoutPhase and error inspection

### Required features

`http-client`.

### Function signature

```text
NetError::timeout_phase(&self) -> Option<TimeoutPhase>
```

### Steps and parameters

After receiving an error, call the method on it; no explicit parameters and no
callbacks. `Some(phase)` identifies the expired HTTP budget; `None` means the error
has no HTTP timeout classification. Reading the error does not cancel other work.

### Return values and errors

| Variant | Budget |
| --- | --- |
| `Admission` | Capacity and queue-slot waiting |
| `Connect` | New-connection setup |
| `Headers` | Transport through final headers |
| `IdleRead` | Time between nonempty byte deliveries |
| `Attempt` | One attempt including body |
| `Total` | Logical operation including retries |

`TimeoutPhase` is non-exhaustive. Keep a wildcard arm in downstream matches.
Error inspection is infallible. Total expiry uses `ErrorKind::DeadlineExceeded`;
other HTTP phase budgets use `ErrorKind::TimedOut`. When budgets overlap, the
earliest absolute deadline wins. Exact deadline ties are resolved in this order:
Total, Admission, Attempt, Connect, Headers, IdleRead. Transport, cancellation, and
shutdown failures can occur instead of a timeout.

### Example

This program compiles an error-reporting helper without making a request.

```rust
use open_net::{NetError, api::http::TimeoutPhase};
fn describe(error: &NetError) -> &'static str {
    match error.timeout_phase() {
        Some(TimeoutPhase::Admission) => "capacity wait",
        Some(TimeoutPhase::Connect) => "connection",
        Some(TimeoutPhase::Headers) => "response headers",
        Some(TimeoutPhase::IdleRead) => "body progress",
        Some(TimeoutPhase::Attempt) => "attempt",
        Some(TimeoutPhase::Total) => "whole operation",
        Some(_) => "another HTTP budget",
        None => "not an HTTP timeout",
    }
}
fn main() { let _report = describe; }
```

## HttpAdmissionPolicy

### Required features

`http-client`.

### Function signatures

```text
HttpClientConfig::with_admission_policy(self, policy: HttpAdmissionPolicy) -> Self
HttpClientConfig::admission_policy(&self) -> HttpAdmissionPolicy
HttpRequestBuilder::admission_policy(self, policy: HttpAdmissionPolicy) -> Self
HttpRequest::admission_policy_override(&self) -> Option<HttpAdmissionPolicy>
```

### Steps and parameters

Set the required `policy` argument on a config for the default, or on a concrete
request builder for one operation. There are no callback parameters. Accessors
have no arguments. Policies take effect when response futures are first polled.

### Return values

The builders return their updated values. Config reads return the policy;
request reads return `Some(override)` or `None` to inherit the client default.

| Policy | Behavior |
| --- | --- |
| `Wait` (default) | Asynchronously wait for in-flight capacity and queue reservation within Admission and Total budgets |
| `FailFast` | Immediately return `QueueFull` if any required admission resource is unavailable |

### Errors and behavior

The setting itself is infallible. Response futures (`request`, convenience
GET/POST, `stream`, `sse`) obey it. Callback `send*` always tries immediate
admission; callback `send_wait*` always waits, regardless of this setting.
Shutdown can reject a waiter with `Closed`. An elapsed Admission budget reports
`TimedOut`; an elapsed Total budget reports `DeadlineExceeded`, each with its
applicable timeout phase. Dropping an unaccepted future releases its waiter
and owned request; no callback executes. For callback ownership on ordinary
rejection versus cancellation of a wait, see [callback requests](callback_requests.md).

The request queue capacity also bounds in-flight operations. Queue sizes are
positive counts, not byte limits. Permits stay occupied through callback
completion, or stream completion/error/drop/timeout; retries belong to the same
logical operation. `response_queue_capacity` and `callback_queue_capacity` bound
the downstream queues, not an additional unlimited source of admission.

Waiting callers retain request bodies: bound application concurrency too.
An inner `Wait` request may wait for capacity held by its outer middleware or
callback. A clone shares the same capacity. Use an independent client or handle
`FailFast` for nested work. Disabling Admission and Total permits an unbounded
admission wait until capacity, cancellation or shutdown.

### Example

Start an HTTP server on `127.0.0.1:8080` serving `/status` before running. Expect a
status code or a printed request error. The per-request policy overrides FailFast.
Shutdown runs even if the request fails.

```rust
use open_net::api::http::{HttpAdmissionPolicy, HttpClient, HttpClientConfig, HttpRequest, HttpTimeouts};
use std::time::Duration;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = HttpClientConfig::new("http://127.0.0.1:8080")?
        .with_request_queue_capacity(1)?
        .with_admission_policy(HttpAdmissionPolicy::FailFast);
    assert_eq!(config.admission_policy(), HttpAdmissionPolicy::FailFast);
    let client = HttpClient::new(config, "timeouts-example")?;
    let request = HttpRequest::builder().path("/status")
        .admission_policy(HttpAdmissionPolicy::Wait)
        .timeouts(HttpTimeouts::new().with_admission_timeout(Duration::from_secs(2)))
        .build()?;
    assert_eq!(request.admission_policy_override(), Some(HttpAdmissionPolicy::Wait));
    let result = client.request(request).await;
    client.shutdown().await?;
    match result { Ok(response) => println!("{}", response.status), Err(error) => eprintln!("{error}") }
    Ok(())
}
```

## Individual timeout and admission functions

The examples above are shared complete programs. Each entry below points to the
program that demonstrates the named call and records its own contract.

## `HttpTimeouts::new()`

### Required features

`http-client`.

### Function signature

```text
HttpTimeouts::new() -> Self
```

### Steps

Create overrides, set the phases you need, then install the returned object.

### Parameters

None.

### Callback parameters

None.

### Return values

A new override object with every phase set to inherit.

### Errors and behavior

Infallible; inheriting does not mean disabled. Admission falls back to 30 seconds and Attempt to the legacy client/policy setting.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpTimeouts::with_admission_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpTimeouts::with_admission_timeout(self, timeout: Duration) -> Self
```

### Steps

Set the budget for capacity and queue-slot waiting, retain the returned object, then install it.

### Parameters

`timeout`: required positive `Duration` that fits the monotonic clock; use `Duration::from_secs` or `from_millis` for explicit units.

### Callback parameters

None.

### Return values

The updated override object.

### Errors and behavior

No immediate validation error. Installation validates durations: client configuration returns InvalidConfig, callback options or request build return InvalidInput. The last setting for this phase wins. Admission still has a 30-second fallback when inherited. Immediate callback submission does not wait.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpTimeouts::without_admission_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpTimeouts::without_admission_timeout(self) -> Self
```

### Steps

Disable the budget for capacity and queue-slot waiting and install the returned object.

### Parameters

No explicit arguments; consumes the current overrides.

### Callback parameters

None.

### Return values

The updated object with this phase explicitly disabled.

### Errors and behavior

Infallible. Explicit disable overrides client and fallback values, while other phase budgets remain active. It does not mean an operation can never time out. Admission still has a 30-second fallback when inherited. Immediate callback submission does not wait.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpTimeouts::with_headers_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpTimeouts::with_headers_timeout(self, timeout: Duration) -> Self
```

### Steps

Set the budget for transport execution through final response headers, including redirects, retain the returned object, then install it.

### Parameters

`timeout`: required positive `Duration` that fits the monotonic clock; use `Duration::from_secs` or `from_millis` for explicit units.

### Callback parameters

None.

### Return values

The updated override object.

### Errors and behavior

No immediate validation error. Installation validates durations: client configuration returns InvalidConfig, callback options or request build return InvalidInput. The last setting for this phase wins. This is not an application callback timer; buffered header hooks execute later, after body success.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpTimeouts::without_headers_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpTimeouts::without_headers_timeout(self) -> Self
```

### Steps

Disable the budget for transport execution through final response headers, including redirects and install the returned object.

### Parameters

No explicit arguments; consumes the current overrides.

### Callback parameters

None.

### Return values

The updated object with this phase explicitly disabled.

### Errors and behavior

Infallible. Explicit disable overrides client and fallback values, while other phase budgets remain active. It does not mean an operation can never time out. This is not an application callback timer; buffered header hooks execute later, after body success.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpTimeouts::with_idle_read_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpTimeouts::with_idle_read_timeout(self, timeout: Duration) -> Self
```

### Steps

Set the budget for waiting for nonempty body bytes, including the first byte, retain the returned object, then install it.

### Parameters

`timeout`: required positive `Duration` that fits the monotonic clock; use `Duration::from_secs` or `from_millis` for explicit units.

### Callback parameters

None.

### Return values

The updated override object.

### Errors and behavior

No immediate validation error. Installation validates durations: client configuration returns InvalidConfig, callback options or request build return InvalidInput. The last setting for this phase wins. Each delivered nonempty chunk resets this budget. Pauses while retaining an unpolled stream consume it.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpTimeouts::without_idle_read_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpTimeouts::without_idle_read_timeout(self) -> Self
```

### Steps

Disable the budget for waiting for nonempty body bytes, including the first byte and install the returned object.

### Parameters

No explicit arguments; consumes the current overrides.

### Callback parameters

None.

### Return values

The updated object with this phase explicitly disabled.

### Errors and behavior

Infallible. Explicit disable overrides client and fallback values, while other phase budgets remain active. It does not mean an operation can never time out. Each delivered nonempty chunk resets this budget. Pauses while retaining an unpolled stream consume it.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpTimeouts::with_attempt_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpTimeouts::with_attempt_timeout(self, timeout: Duration) -> Self
```

### Steps

Set the budget for one attempt from preparation through body completion, retain the returned object, then install it.

### Parameters

`timeout`: required positive `Duration` that fits the monotonic clock; use `Duration::from_secs` or `from_millis` for explicit units.

### Callback parameters

None.

### Return values

The updated override object.

### Errors and behavior

No immediate validation error. Installation validates durations: client configuration returns InvalidConfig, callback options or request build return InvalidInput. The last setting for this phase wins. An explicit setting replaces the inherited legacy client/retry attempt limit.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpTimeouts::without_attempt_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpTimeouts::without_attempt_timeout(self) -> Self
```

### Steps

Disable the budget for one attempt from preparation through body completion and install the returned object.

### Parameters

No explicit arguments; consumes the current overrides.

### Callback parameters

None.

### Return values

The updated object with this phase explicitly disabled.

### Errors and behavior

Infallible. Explicit disable overrides client and fallback values, while other phase budgets remain active. It does not mean an operation can never time out. An explicit setting replaces the inherited legacy client/retry attempt limit.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpTimeouts::with_total_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpTimeouts::with_total_timeout(self, timeout: Duration) -> Self
```

### Steps

Set the budget for the whole operation, including admission, middleware, retries and body, retain the returned object, then install it.

### Parameters

`timeout`: required positive `Duration` that fits the monotonic clock; use `Duration::from_secs` or `from_millis` for explicit units.

### Callback parameters

None.

### Return values

The updated override object.

### Errors and behavior

No immediate validation error. Installation validates durations: client configuration returns InvalidConfig, callback options or request build return InvalidInput. The last setting for this phase wins. Total expiry reports DeadlineExceeded; other phase expiry reports TimedOut.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpTimeouts::without_total_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpTimeouts::without_total_timeout(self) -> Self
```

### Steps

Disable the budget for the whole operation, including admission, middleware, retries and body and install the returned object.

### Parameters

No explicit arguments; consumes the current overrides.

### Callback parameters

None.

### Return values

The updated object with this phase explicitly disabled.

### Errors and behavior

Infallible. Explicit disable overrides client and fallback values, while other phase budgets remain active. It does not mean an operation can never time out. Total expiry reports DeadlineExceeded; other phase expiry reports TimedOut.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpClientConfig::with_connect_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_connect_timeout(self, timeout: Duration) -> Result<Self, NetError>
```

### Steps

Set before constructing the client.

### Parameters

`timeout`: positive monotonic-clock-representable Duration.

### Callback parameters

None.

### Return values

Updated configuration, or a validation error.

### Errors and behavior

InvalidConfig for zero or out-of-range duration. Applies to new-connection DNS/proxy/TLS setup; initially disabled.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpClientConfig::without_connect_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::without_connect_timeout(self) -> Self
```

### Steps

Apply to configuration before constructing the client.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Updated configuration.

### Errors and behavior

Infallible; disables only Connect, preserving phase budgets.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpClientConfig::connect_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::connect_timeout(&self) -> Option<Duration>
```

### Steps

Read the configured connection budget.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Some(duration), or None when disabled.

### Errors and behavior

Infallible; returns a setting, not a remaining deadline.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpClientConfig::with_timeouts()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_timeouts(self, timeouts: HttpTimeouts) -> Result<Self, NetError>
```

### Steps

Construct phase overrides and install them before creating the client.

### Parameters

`timeouts`: required complete client override layer.

### Callback parameters

None.

### Return values

Updated configuration or NetError.

### Errors and behavior

InvalidConfig for invalid explicit durations. Replaces the entire layer; inherited fields still use fallbacks.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpClientConfig::timeouts()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::timeouts(&self) -> &HttpTimeouts
```

### Steps

Borrow the client override layer for inspection.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

A reference valid while the config is borrowed.

### Errors and behavior

Infallible. Returns explicit settings rather than computed runtime deadlines.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpRequestBuilder::timeouts()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestBuilder::timeouts(self, timeouts: HttpTimeouts) -> Self
```

### Steps

Install request overrides, then call build() before sending.

### Parameters

`timeouts`: required complete request override layer.

### Callback parameters

None.

### Return values

Updated request builder.

### Errors and behavior

Validation is deferred to build(): invalid explicit durations produce InvalidInput. Replaces the whole request layer, with per-phase inheritance from the client.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpRequest::timeouts()`

### Required features

`http-client`.

### Function signature

```text
HttpRequest::timeouts(&self) -> &HttpTimeouts
```

### Steps

Read overrides from the built request.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Borrowed explicit request overrides.

### Errors and behavior

Infallible; inherited values are not resolved here.

### Example

Run the complete offline [timeout construction program](#httptimeouts); it calls this method explicitly.

## `HttpClientConfig::with_admission_policy()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_admission_policy(self, policy: HttpAdmissionPolicy) -> Self
```

### Steps

Select the client default before construction.

### Parameters

`policy`: Wait (default) or FailFast.

### Callback parameters

None.

### Return values

Updated configuration.

### Errors and behavior

Infallible; affects response-future admission, not the fixed admission behavior of callback send/send_wait. Wait obeys Admission and Total budgets.

### Example

Run the complete [admission program](#httpadmissionpolicy), which configures FailFast.

## `HttpClientConfig::admission_policy()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::admission_policy(&self) -> HttpAdmissionPolicy
```

### Steps

Read the configured response-future admission default.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

The copied admission policy.

### Errors and behavior

Infallible; does not report queue capacity or reserve a slot.

### Example

Run the complete [admission program](#httpadmissionpolicy), which checks the configured value.

## `HttpRequestBuilder::admission_policy()`

### Required features

`http-client`.

### Function signature

```text
HttpRequestBuilder::admission_policy(self, policy: HttpAdmissionPolicy) -> Self
```

### Steps

Set the override, build the request, then submit it.

### Parameters

`policy`: required Wait or FailFast.

### Callback parameters

None.

### Return values

Updated request builder.

### Errors and behavior

Infallible setting; overrides the client at first poll. A full queue can later produce QueueFull under FailFast.

### Example

Run the complete [admission program](#httpadmissionpolicy), which selects Wait for one request.

## `HttpRequest::admission_policy_override()`

### Required features

`http-client`.

### Function signature

```text
HttpRequest::admission_policy_override(&self) -> Option<HttpAdmissionPolicy>
```

### Steps

Read the built request before submission.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Some(policy) for an explicit override, None to inherit.

### Errors and behavior

Infallible; no capacity is acquired.

### Example

Run the complete [admission program](#httpadmissionpolicy), which asserts the override.

## `NetError::timeout_phase()`

### Required features

`http-client`.

### Function signature

```text
NetError::timeout_phase(&self) -> Option<TimeoutPhase>
```

### Steps

Call on a request error and match the returned phase with a wildcard for future variants.

### Parameters

No explicit arguments.

### Callback parameters

None.

### Return values

Some(expired_budget), or None if no HTTP timeout classification is attached.

### Errors and behavior

Infallible. A budget identifies what expired, not necessarily the transport stage being executed. Total uses DeadlineExceeded; the other phase budgets use TimedOut.

### Example

Use the complete [error-reporting helper](#timeoutphase-and-error-inspection).

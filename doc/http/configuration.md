# HTTP client configuration

Required feature: `http-client`. This guide describes the checked-out source. In a consumer project beside the `open-net` directory, use:

```toml
[dependencies]
open-net = { path = "../open-net", default-features = false, features = ["http-client"] }
```

Adjust the local path to your checkout. All types below are in `open_net::api::http`; `Duration` is `std::time::Duration`. Configuration builders consume and return ownership of the configuration. Fallible setters consume the original configuration even on `Err(NetError)`; the error does not return it. Clone first if a previous configuration must remain available. `?` returns early on failure. Create a new client to apply changed settings; changing a retained configuration does not reconfigure a running client. `Clone` copies ordinary settings and shares explicitly installed `Arc` providers.

## Construct, validate, and inspect a configuration

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::new(base_url: impl AsRef<str>) -> Result<Self, NetError>
HttpClientConfig::validate(&self) -> Result<(), NetError>
```

### Steps

1. Choose an absolute HTTP or HTTPS base URL.
2. Call `new`, then the relevant `with_*` methods.
3. If you change public fields directly, call `validate` before use. Client construction also validates.
4. Pass the completed value to `HttpClient::new` or the engine's HTTP factory.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `base_url` | `impl AsRef<str>` | Yes | Absolute `http` or `https` URL with a host; no credentials or fragment. A path and query are allowed. |
| `self` for `validate` | `&HttpClientConfig` | Yes | Configuration to inspect, without mutation. |

Public fields and initial values:

| Field | Type | Default and bounds |
| --- | --- | --- |
| `base_url` | URL value (`reqwest::Url`) | Parsed input; normally construct via `new` without adding reqwest as a dependency. |
| `common_headers` | `HeaderMap` | Empty. SDK-managed transport headers are rejected. |
| `timeout` | `Duration` | 30 seconds per attempt; positive and representable on the monotonic clock. |
| `tcp_keepalive` | `Option<Duration>` | `Some(15 seconds)`; `None` disables it, direct `Some(Duration::ZERO)` is invalid. |
| `max_response_bytes` | `usize` | 1,073,741,824 bytes (1024 MiB); must be positive. Applies to buffered response collection; streaming consumers must enforce their own cumulative byte limit. |
| `request_queue_capacity` | `usize` | 128; `1..=tokio::sync::Semaphore::MAX_PERMITS`. |
| `response_queue_capacity` | `usize` | 128; same range. |
| `callback_queue_capacity` | `usize` | 128; same range. |
| `default_retry_policy` | `RetryPolicy` | `RetryPolicy::no_retry()`. |
| `tls` | `HttpTlsConfig` | Built-in WebPKI roots, no custom roots or client identity. |
| `proxy` | `Option<HttpProxyConfig>` | `None`. |
| `environment_proxy` | `bool` | `false`. |

The remaining settings are private. Defaults are: no dedicated connect timeout, inherited phase timeouts, `HttpAdmissionPolicy::Wait`, no Cookie store, no authentication provider, no middleware, and, with HTTP/2 enabled, `HttpVersionPolicy::Auto` and default `Http2Config`. Optional settings exist only with their respective features.

### Callback parameters

None.

### Return values

`new`: `Ok(HttpClientConfig)` or `Err(NetError)`. `validate`: `Ok(())` or `Err(NetError)`. Neither starts network work. Validation does not test server reachability, proxy credentials, or the remote TLS handshake.

### Errors and behavior

Invalid configuration returns `InvalidConfig`; inspect `error.config_error()` and its `field()`. A directly mutated public field is checked again at installation. Certificate parsing is performed by the TLS configuration methods; transport initialization performs additional TLS setup.

Relative request paths are resolved using URL joining. For base `https://example.com/api/`, `users` becomes `/api/users`; `/users` becomes `/users`. A base without a trailing slash treats its last component as a file. `..` may traverse the base path while remaining on the same origin. The client trims outer whitespace and rejects empty paths, absolute URLs, `//`, backslashes, fragments, control characters and origin changes. These rules apply to the initial request target; redirects can cross origins. See [authentication](authentication.md) and [cookies](cookies.md) for provider-aware redirects.

### Example

This is an offline configuration example.

```rust
use open_net::api::http::HttpClientConfig;
use open_net::NetError;

fn main() -> Result<(), NetError> {
    let mut config = HttpClientConfig::new("https://example.com/api/")?;
    config.max_response_bytes = 2 * 1024 * 1024;
    config.validate()?;
    assert_eq!(config.request_queue_capacity, 128);
    Ok(())
}
```

## Headers: `with_common_headers` and `merge_headers`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_common_headers(self, headers: HeaderMap) -> Result<Self, NetError>
HttpClientConfig::merge_headers(&self, request_headers: &HeaderMap) -> Result<HeaderMap, NetError>
```

### Steps

Build a `HeaderMap`, install it as common defaults, and put overrides in each request's header map. `merge_headers` lets you inspect the public merge rule before sending.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `headers` | `HeaderMap` | Yes | Replaces the entire common map, including when empty. |
| `request_headers` | `&HeaderMap` | Yes | Each present name replaces every common value for that name. Repeated values retain their per-name order. |

### Callback parameters

None.

### Return values

`with_common_headers` returns the updated configuration; `merge_headers` returns a newly owned map. Both return `Err(NetError)` for invalid settings. Neither input to `merge_headers` is modified.

### Errors and behavior

The forbidden names are `connection`, `keep-alive`, `proxy-connection`, `proxy-authenticate`, `proxy-authorization`, `te`, `trailer`, `transfer-encoding`, `upgrade`, `host`, and `content-length`. They produce `InvalidConfig` for `headers`. Typed `HeaderName` and `HeaderValue` construction can separately fail before this API is called. Empty request values still override common values. `merge_headers` first validates the entire configuration.

### Example

```rust
use open_net::api::http::{HeaderMap, HeaderValue, HttpClientConfig};
use open_net::NetError;

fn main() -> Result<(), NetError> {
    let mut common = HeaderMap::new();
    common.insert("accept", HeaderValue::from_static("application/json"));
    common.insert("x-client", HeaderValue::from_static("demo"));
    let config = HttpClientConfig::new("https://example.com/")?
        .with_common_headers(common)?;
    let mut request = HeaderMap::new();
    request.append("accept", HeaderValue::from_static("text/plain"));
    let merged = config.merge_headers(&request)?;
    assert_eq!(merged["accept"], "text/plain");
    assert_eq!(merged["x-client"], "demo");
    Ok(())
}
```

## Time budgets, admission and resource limits

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_timeout(self, timeout: Duration) -> Result<Self, NetError>
HttpClientConfig::with_connect_timeout(self, timeout: Duration) -> Result<Self, NetError>
HttpClientConfig::without_connect_timeout(self) -> Self
HttpClientConfig::connect_timeout(&self) -> Option<Duration>
HttpClientConfig::with_timeouts(self, timeouts: HttpTimeouts) -> Result<Self, NetError>
HttpClientConfig::timeouts(&self) -> &HttpTimeouts
HttpClientConfig::with_admission_policy(self, policy: HttpAdmissionPolicy) -> Self
HttpClientConfig::admission_policy(&self) -> HttpAdmissionPolicy
HttpClientConfig::with_tcp_keepalive(self, keepalive: Duration) -> Self
HttpClientConfig::without_tcp_keepalive(self) -> Self
HttpClientConfig::with_max_response_bytes(self, max_response_bytes: usize) -> Result<Self, NetError>
HttpClientConfig::with_request_queue_capacity(self, request_queue_capacity: usize) -> Result<Self, NetError>
HttpClientConfig::with_response_queue_capacity(self, response_queue_capacity: usize) -> Result<Self, NetError>
HttpClientConfig::with_callback_queue_capacity(self, callback_queue_capacity: usize) -> Result<Self, NetError>
```

### Steps

Set limits before constructing a client. Use request-level timeouts or admission overrides for individual operations. See [timeouts and admission](timeouts_and_admission.md) for phase timing, precedence, and the individual references for `with_connect_timeout`, `without_connect_timeout`, `connect_timeout`, `with_timeouts`, `timeouts`, `with_admission_policy` and `admission_policy`.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `timeout` | `Duration` | Yes for setters | Positive, monotonic-clock-representable duration; use `from_secs` or `from_millis` for explicit units. |
| `timeouts` | `HttpTimeouts` | Yes | Replaces the complete client override layer; omitted phases inherit fallback values, not the previous layer. |
| `policy` | `HttpAdmissionPolicy` | Yes | `Wait` (default) or `FailFast` for response-returning futures. Does not change `send`/`send_wait`. |
| `keepalive` | `Duration` | Yes | TCP keepalive interval; zero means disable. This is not an HTTP request timeout. |
| `max_response_bytes` | `usize` | Yes | Positive buffered-response body-byte limit. |
| Each queue capacity | `usize` | Yes | Positive number of slots, no greater than `Semaphore::MAX_PERMITS`. |
| Getters and `without_*` methods | None | — | Getters borrow; disabling methods consume and return configuration. |

### Callback parameters

None.

### Return values

Setters return updated configuration, with `Result` where validation can fail. `connect_timeout()` returns `None` when disabled. `timeouts()` returns explicit overrides before inheritance resolution. `admission_policy()` returns the current client default.

### Errors and behavior

Invalid durations, zero body limits, or out-of-range capacities return `InvalidConfig`. The legacy attempt timeout excludes admission, resets on retry, includes preparation and body consumption, and loses precedence to explicit phase settings regardless of setter order. Dedicated connect timeout includes DNS, proxy negotiation and TLS for new connections. Reused connections do not start that budget. TCP keepalive accepts nonzero durations without an additional SDK upper-bound check.

The request quota remains occupied through callback completion or stream completion/drop. Retries retain it while waiting. A buffered response larger than the body limit fails with `ItemTooLarge`; it is not silently truncated. Streaming and SSE consumers must count delivered bytes themselves and drop/cancel the stream when their application limit is reached. Pending callers also retain their own request bodies, so queue capacity alone does not bound all application memory.

### Example

```rust
use open_net::api::http::{HttpAdmissionPolicy, HttpClientConfig, HttpTimeouts};
use open_net::NetError;
use std::time::Duration;

fn main() -> Result<(), NetError> {
    let config = HttpClientConfig::new("https://example.com/")?
        .with_timeout(Duration::from_secs(20))?
        .with_connect_timeout(Duration::from_secs(3))?
        .without_connect_timeout()
        .with_timeouts(HttpTimeouts::new().with_total_timeout(Duration::from_secs(30)))?
        .with_admission_policy(HttpAdmissionPolicy::FailFast)
        .with_tcp_keepalive(Duration::from_secs(15))
        .without_tcp_keepalive()
        .with_max_response_bytes(1024 * 1024)?
        .with_request_queue_capacity(16)?
        .with_response_queue_capacity(16)?
        .with_callback_queue_capacity(16)?;
    assert_eq!(config.connect_timeout(), None);
    assert_eq!(config.admission_policy(), HttpAdmissionPolicy::FailFast);
    let _overrides = config.timeouts();
    config.validate()
}
```

## Extension configuration map

Every method below consumes `self` and returns an updated configuration; later installation replaces the previous value except middleware, which appends. The complete signatures, inputs, return values, callbacks, errors and examples are in the linked guide.

| Methods | Required feature | Guide |
| --- | --- | --- |
| `with_default_retry_policy(policy: RetryPolicy) -> Self` | `http-client` | [Retry](retry.md) |
| `with_tls(tls: HttpTlsConfig) -> Self`, `with_proxy(proxy: HttpProxyConfig) -> Self`, `with_environment_proxy(enabled: bool) -> Self` | `http-client` | [TLS and proxies](tls_and_proxy.md) |
| `with_cookie_store<S: CookieStore + 'static>(store: S) -> Self`, `with_cookie_store_arc(store: Arc<dyn CookieStore>) -> Self`, `with_cookie_limits(limits: CookieLimits) -> Result<Self, NetError>` | `http-client-cookies` | [Cookies](cookies.md) |
| `with_auth<A: HttpAuth + 'static>(auth: A) -> Self`, `with_auth_arc(auth: Arc<dyn HttpAuth>) -> Self` | `http-client-auth` | [Authentication](authentication.md) |
| `with_middleware<M: HttpMiddleware + 'static>(middleware: M) -> Self`, `with_middleware_arc(middleware: Arc<dyn HttpMiddleware>) -> Self` | `http-client-middleware` | [Middleware](middleware.md) |
| `with_http_version_policy(policy: HttpVersionPolicy) -> Self`, `with_http2(http2: Http2Config) -> Result<Self, NetError>` | `http-client-http2` | [HTTP/2](http2.md) |

## Individual function reference

The examples linked below are complete programs with the dependency declaration at the top of this page. Each reference identifies the call that demonstrates the function.

## `HttpClientConfig::new()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::new(base_url: impl AsRef<str>) -> Result<Self, NetError>
```

### Steps

Pass an absolute base URL, apply setters, then create the client with the returned configuration.

### Parameters

`base_url` is required and borrowed for parsing. It must be an absolute `http` or `https` URL with a host, without credentials or a fragment. Path and query are allowed.

### Callback parameters

None.

### Return values

`Ok(HttpClientConfig)` with the defaults listed above; `Err(NetError)` for invalid input.

### Errors and behavior

Returns `InvalidConfig` for `base_url` on invalid syntax or unsupported URL components. Parsing and construction perform no network I/O; later `validate` checks changes to public fields.

### Example

See the [complete construction program](#construct-validate-and-inspect-a-configuration), which calls `HttpClientConfig::new` and `config.validate()` after changing a public field.

## `HttpClientConfig::validate()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::validate(&self) -> Result<(), NetError>
```

### Steps

Call after mutating public fields, or before handing a completed configuration to client construction.

### Parameters

Only the borrowed configuration receiver; it is not modified.

### Callback parameters

None.

### Return values

`Ok(())` when local invariants pass, otherwise `Err(NetError)`.

### Errors and behavior

Checks URL, headers, durations, queue capacities, response limit and enabled extension bounds. Errors are `InvalidConfig`; inspect the structured field. This does not prove reachability, proxy credentials or successful TLS negotiation. Client construction also validates.

### Example

See the [complete construction program](#construct-validate-and-inspect-a-configuration), which calls `HttpClientConfig::new` and `config.validate()` after changing a public field.

## `HttpClientConfig::with_common_headers()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_common_headers(self, headers: HeaderMap) -> Result<Self, NetError>
```

### Steps

Build a typed map, pass it to this setter, and retain the returned configuration.

### Parameters

`headers` is a required owned map. Empty clears all common headers. Per-request values later replace common values by name.

### Callback parameters

None.

### Return values

`Ok(Self)` with the complete replacement map, or `Err(NetError)`.

### Errors and behavior

Rejects the SDK-managed transport names listed in the headers overview with `InvalidConfig` for `headers`. The consumed input configuration is not returned on failure. This replaces the map rather than merging it.

### Example

See the [complete headers program](#headers-with_common_headers-and-merge_headers), which installs `common` with `with_common_headers(common)` and calls `config.merge_headers(&request)`.

## `HttpClientConfig::merge_headers()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::merge_headers(&self, request_headers: &HeaderMap) -> Result<HeaderMap, NetError>
```

### Steps

Prepare request headers and call this accessor to inspect the same name-based merge used by submission.

### Parameters

`request_headers` is required and borrowed; both it and the configuration remain unchanged.

### Callback parameters

None.

### Return values

`Ok(HeaderMap)` containing an owned merged map, or `Err(NetError)`.

### Errors and behavior

Validates the entire configuration first, then the request map. Invalid configuration or managed transport headers return `InvalidConfig`. A present request name replaces all common values of that name, preserving repeated request values and their order, including empty values.

### Example

See the [complete headers program](#headers-with_common_headers-and-merge_headers), which installs `common` with `with_common_headers(common)` and calls `config.merge_headers(&request)`.

## `HttpClientConfig::with_timeout()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_timeout(self, timeout: Duration) -> Result<Self, NetError>
```

### Steps

Choose the legacy per-attempt duration, call the setter, and install the returned configuration.

### Parameters

`timeout` is required, positive and representable on the monotonic clock. Use explicit seconds or milliseconds.

### Callback parameters

None.

### Return values

`Ok(Self)` with the replacement legacy timeout, or `Err(NetError)`.

### Errors and behavior

Invalid values return `InvalidConfig` for `http.timeout`. The timeout excludes admission and resets per retry; preparation and body consumption use it. Explicit `HttpTimeouts` attempt settings take precedence regardless of setter order. See [phase timeouts](timeouts_and_admission.md).

### Example

See the [complete limits program](#time-budgets-admission-and-resource-limits), which calls `with_timeout(Duration::from_secs(20))`.

## `HttpClientConfig::with_tcp_keepalive()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_tcp_keepalive(self, keepalive: Duration) -> Self
```

### Steps

Choose a TCP keepalive interval before client construction and retain the returned configuration.

### Parameters

`keepalive` is required. Zero disables TCP keepalive; any nonzero duration is retained as the interval.

### Callback parameters

None.

### Return values

The updated owned configuration.

### Errors and behavior

Infallible; no additional SDK upper-bound check is applied. This controls pooled TCP connections, independently of request deadlines and HTTP/2 PINGs. It does not send an application HTTP health check.

### Example

See the [complete limits program](#time-budgets-admission-and-resource-limits), which calls `with_tcp_keepalive(Duration::from_secs(15))` before disabling it.

## `HttpClientConfig::without_tcp_keepalive()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::without_tcp_keepalive(self) -> Self
```

### Steps

Call before client construction to remove the configured TCP keepalive interval.

### Parameters

Only the consumed configuration receiver.

### Callback parameters

None.

### Return values

The updated configuration with `tcp_keepalive = None`.

### Errors and behavior

Infallible and idempotent. It does not change HTTP phase timeouts or HTTP/2 PING settings.

### Example

See the [complete limits program](#time-budgets-admission-and-resource-limits), which calls `without_tcp_keepalive()` after setting an interval.

## `HttpClientConfig::with_max_response_bytes()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_max_response_bytes(self, max_response_bytes: usize) -> Result<Self, NetError>
```

### Steps

Choose a maximum buffered response-body size and pass it in bytes before client creation.

### Parameters

`max_response_bytes` is required and must be greater than zero; the default is 1,073,741,824 bytes.

### Callback parameters

None.

### Return values

`Ok(Self)` with the new byte limit, or `Err(NetError)`.

### Errors and behavior

Zero returns `InvalidConfig` for `max_response_bytes`. A buffered response that exceeds this limit returns `ItemTooLarge`; it is not truncated. Streaming and SSE consumers must enforce their own cumulative limit.

### Example

See the [complete limits program](#time-budgets-admission-and-resource-limits), which calls `with_max_response_bytes(1024 * 1024)`.

## `HttpClientConfig::with_request_queue_capacity()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_request_queue_capacity(self, request_queue_capacity: usize) -> Result<Self, NetError>
```

### Steps

Choose the request queue slot count before constructing the client and retain the returned configuration.

### Parameters

`request_queue_capacity` is required, measured in slots, and must be in `1..=tokio::sync::Semaphore::MAX_PERMITS`. The default is 128.

### Callback parameters

None.

### Return values

`Ok(Self)` with the replacement capacity, or `Err(NetError)`.

### Errors and behavior

Zero or an out-of-range value returns `InvalidConfig` for `request_queue_capacity`. Request quota remains occupied through callback completion or stream completion/drop, including retry waits. Caller-owned pending bodies are not bounded by this value.

### Example

See the [complete limits program](#time-budgets-admission-and-resource-limits), which calls `with_request_queue_capacity(16)`.

## `HttpClientConfig::with_response_queue_capacity()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_response_queue_capacity(self, response_queue_capacity: usize) -> Result<Self, NetError>
```

### Steps

Choose the response queue slot count before constructing the client and retain the returned configuration.

### Parameters

`response_queue_capacity` is required, measured in slots, and must be in `1..=tokio::sync::Semaphore::MAX_PERMITS`. The default is 128.

### Callback parameters

None.

### Return values

`Ok(Self)` with the replacement capacity, or `Err(NetError)`.

### Errors and behavior

Zero or an out-of-range value returns `InvalidConfig` for `response_queue_capacity`. Bounds queued buffered response work. It is independent of the request and callback queue settings.

### Example

See the [complete limits program](#time-budgets-admission-and-resource-limits), which calls `with_response_queue_capacity(16)`.

## `HttpClientConfig::with_callback_queue_capacity()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_callback_queue_capacity(self, callback_queue_capacity: usize) -> Result<Self, NetError>
```

### Steps

Choose the callback queue slot count before constructing the client and retain the returned configuration.

### Parameters

`callback_queue_capacity` is required, measured in slots, and must be in `1..=tokio::sync::Semaphore::MAX_PERMITS`. The default is 128.

### Callback parameters

None.

### Return values

`Ok(Self)` with the replacement capacity, or `Err(NetError)`.

### Errors and behavior

Zero or an out-of-range value returns `InvalidConfig` for `callback_queue_capacity`. Bounds callback queue slots. Slow callbacks still delay completion; this value does not bound application work detached by a callback.

### Example

See the [complete limits program](#time-budgets-admission-and-resource-limits), which calls `with_callback_queue_capacity(16)`.

[HTTP guide index](README.md)

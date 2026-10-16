# Requests and buffered responses

[HTTP guide](README.md) · [Library overview](../../README.md)

## Required features and example dependencies

Every example on this page uses `http-client`, which also enables `net-status`.
Use Rust 1.89 or later. Create a consumer crate with `cargo new http-example`,
then add the following to its `Cargo.toml`. Replace `/path/to/open-net` with
the library directory containing `Cargo.toml` in your checkout.

```toml
[dependencies]
open-net = { path = "/path/to/open-net", default-features = false, features = ["http-client"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
serde_json = "1"
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

Each Rust block is a separate program: copy one block to `src/main.rs` and run
`cargo check` to compile it. Examples that send requests need an HTTP server at
`http://127.0.0.1:8080` exposing the named route; `cargo run` executes them.
Compilation alone does not test server availability or platform monitoring.

## Shared request rules

Every method on this page returns a fully buffered response. Buffered bodies are
subject to the configured `max_response_bytes`; use [streaming](streaming_and_sse.md)
for incremental consumption. The request future acquires admission capacity when
polled. The default policy waits, with a 30-second admission timeout; see
[timeouts and admission](timeouts_and_admission.md). Dropping the pending request
future requests cancellation and releases any admission reservation. It cannot
undo effects already produced by the server.

With base URL `http://127.0.0.1:8080/api/`, `items?limit=2` resolves to
`/api/items?limit=2`, whereas `/items` resolves from the origin root. Paths are
trimmed and must be nonempty. Absolute URLs (even the same origin), `//host`,
backslashes, fragments and control characters are rejected. The resolved scheme,
host and effective port must match the base URL. These are initial request-path
rules, not a promise about redirects: server redirects may lead to another origin.
With [`Http2Only`](http2.md), the initial URL and followed redirect targets must
use HTTPS. The client never follows a redirect to an HTTP target or replays its
request body there. A rejected follow attempt returns an error; a redirect that
cannot be followed for another reason can remain an unfollowed 3xx response, as
described in the HTTP/2 guide. HTTPS redirects can still cross origins, subject
to the target's certificate and HTTP/2 support. `Auto` and `Http1Only` do not
impose this HTTPS-only restriction.

Request headers replace all common-header values with the same name; repeated
request values retain their order. Transport-controlled headers are rejected:
`connection`, `keep-alive`, `proxy-connection`, `proxy-authenticate`,
`proxy-authorization`, `te`, `trailer`, `transfer-encoding`, `upgrade`, `host`,
`content-length`. Let the transport provide framing and use the proxy
configuration for proxy authentication. A manual `authorization` header needs
no auth feature.

A successful HTTP exchange with a 404 or 500 status is `Ok(HttpResponse)`.
Network failures are `Err(NetError)`. Apply `check_status` separately. Retries can
change the number of attempts; do not infer application success from transport
success. See [retry policies](retry.md).

## `HttpClient::request()`

Send an owned concrete request with a custom method and body.

### Required features

`http-client`.

### Function signature

```text
pub async fn request(&self, request: HttpRequest) -> Result<HttpResponse, NetError>
```

### Steps

Run a server providing the route used below. Create the client, await the request, then inspect the buffered response. Shutdown runs before propagating the request error.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| request | HttpRequest | Yes | Owned request built with HttpRequest::builder(). |

### Callback parameters

Not applicable.

### Return values

`Ok(HttpResponse)` contains status, headers, owned bytes, attempt count and request ID. `Err(NetError)` reports admission, request validation, transport, body-size, cancellation or timeout failures.

### Errors and behavior

The shared rules above apply. HTTP error status codes do not automatically become errors.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig, HttpRequest, Method};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "http-example",
    )?;
    let request = HttpRequest::builder()
        .method(Method::PUT).path("/items/1")
        .header("content-type", "application/json")?
        .body(open_net::Bytes::from_static(br#"{"name":"example"}"#)).build()?;
    let result = client.request(request).await;
    client.shutdown().await?;
    let response = result?;
    println!("{}: {} bytes", response.status(), response.body().len());
    Ok(())
}
```

## `HttpClient::get()`

Send GET with the configured common headers.

### Required features

`http-client`.

### Function signature

```text
pub async fn get(&self, path: impl Into<String>) -> Result<HttpResponse, NetError>
```

### Steps

Run a server providing the route used below. Create the client, await the request, then inspect the buffered response. Shutdown runs before propagating the request error.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| path | impl Into<String> | Yes | A safe relative target, including an optional query. |

### Callback parameters

Not applicable.

### Return values

`Ok(HttpResponse)` contains status, headers, owned bytes, attempt count and request ID. `Err(NetError)` reports admission, request validation, transport, body-size, cancellation or timeout failures.

### Errors and behavior

The shared rules above apply. HTTP error status codes do not automatically become errors.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "http-example",
    )?;
    let result = client.get("/health").await;
    client.shutdown().await?;
    let response = result?;
    println!("{}: {} bytes", response.status(), response.body().len());
    Ok(())
}
```

## `HttpClient::get_with_headers()`

Send GET with per-request headers.

### Required features

`http-client`.

### Function signature

```text
pub async fn get_with_headers(
    &self, path: impl Into<String>, headers: HeaderMap,
) -> Result<HttpResponse, NetError>
```

### Steps

Run a server providing the route used below. Create the client, await the request, then inspect the buffered response. Shutdown runs before propagating the request error.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| path | impl Into<String> | Yes | Safe relative target. |
| headers | HeaderMap | Yes | Owned request headers merged over common headers. |

### Callback parameters

Not applicable.

### Return values

`Ok(HttpResponse)` contains status, headers, owned bytes, attempt count and request ID. `Err(NetError)` reports admission, request validation, transport, body-size, cancellation or timeout failures.

### Errors and behavior

The shared rules above apply. HTTP error status codes do not automatically become errors.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig, HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "http-example",
    )?;
    let mut headers = HeaderMap::new();
    headers.insert("accept", HeaderValue::from_static("application/json"));
    let result = client.get_with_headers("/health", headers).await;
    client.shutdown().await?;
    let response = result?;
    println!("{}: {} bytes", response.status(), response.body().len());
    Ok(())
}
```

## `HttpClient::post()`

Copy bytes into an owned POST body.

### Required features

`http-client`.

### Function signature

```text
pub async fn post(
    &self, path: impl Into<String>, body: impl AsRef<[u8]>,
) -> Result<HttpResponse, NetError>
```

### Steps

Run a server providing the route used below. Create the client, await the request, then inspect the buffered response. Shutdown runs before propagating the request error.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| path | impl Into<String> | Yes | Safe relative target. |
| body | impl AsRef<[u8]> | Yes | Bytes copied into request-owned storage. |

### Callback parameters

Not applicable.

### Return values

`Ok(HttpResponse)` contains status, headers, owned bytes, attempt count and request ID. `Err(NetError)` reports admission, request validation, transport, body-size, cancellation or timeout failures.

### Errors and behavior

The shared rules above apply. HTTP error status codes do not automatically become errors. POST is non-idempotent; replay requires an explicitly suitable retry policy. No Content-Type is inferred from raw bytes.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "http-example",
    )?;
    let result = client.post("/echo", b"hello").await;
    client.shutdown().await?;
    let response = result?;
    println!("{}: {} bytes", response.status(), response.body().len());
    Ok(())
}
```

## `HttpClient::post_with_headers()`

Send POST with explicit content metadata and bytes.

### Required features

`http-client`.

### Function signature

```text
pub async fn post_with_headers(
    &self, path: impl Into<String>, headers: HeaderMap, body: impl AsRef<[u8]>,
) -> Result<HttpResponse, NetError>
```

### Steps

Run a server providing the route used below. Create the client, await the request, then inspect the buffered response. Shutdown runs before propagating the request error.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| path | impl Into<String> | Yes | Safe relative target. |
| headers | HeaderMap | Yes | Owned request headers. |
| body | impl AsRef<[u8]> | Yes | Copied bytes, for example a byte slice or String. |

### Callback parameters

Not applicable.

### Return values

`Ok(HttpResponse)` contains status, headers, owned bytes, attempt count and request ID. `Err(NetError)` reports admission, request validation, transport, body-size, cancellation or timeout failures.

### Errors and behavior

The shared rules above apply. HTTP error status codes do not automatically become errors. POST is non-idempotent; replay requires an explicitly suitable retry policy. No Content-Type is inferred from raw bytes.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig, HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "http-example",
    )?;
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    let result = client.post_with_headers("/echo", headers, br#"{"hello":"world"}"#).await;
    client.shutdown().await?;
    let response = result?;
    println!("{}: {} bytes", response.status(), response.body().len());
    Ok(())
}
```

## `HttpClient::get_bytes()`

Compatibility alias for get(); it returns HttpResponse, not bare Bytes.

### Required features

`http-client`.

### Function signature

```text
pub async fn get_bytes(&self, path: impl Into<String>) -> Result<HttpResponse, NetError>
```

### Steps

Run a server providing the route used below. Create the client, await the request, then inspect the buffered response. Shutdown runs before propagating the request error.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| path | impl Into<String> | Yes | Safe relative target. |

### Callback parameters

Not applicable.

### Return values

`Ok(HttpResponse)` contains status, headers, owned bytes, attempt count and request ID. `Err(NetError)` reports admission, request validation, transport, body-size, cancellation or timeout failures.

### Errors and behavior

The shared rules above apply. HTTP error status codes do not automatically become errors.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "http-example",
    )?;
    let result = client.get_bytes("/bytes").await;
    client.shutdown().await?;
    let response = result?;
    println!("{}: {} bytes", response.status(), response.body().len());
    Ok(())
}
```

## `HttpClient::get_bytes_with_headers()`

Compatibility alias for get_with_headers(); it returns HttpResponse.

### Required features

`http-client`.

### Function signature

```text
pub async fn get_bytes_with_headers(
    &self, path: impl Into<String>, headers: HeaderMap,
) -> Result<HttpResponse, NetError>
```

### Steps

Run a server providing the route used below. Create the client, await the request, then inspect the buffered response. Shutdown runs before propagating the request error.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| path | impl Into<String> | Yes | Safe relative target. |
| headers | HeaderMap | Yes | Owned per-request headers. |

### Callback parameters

Not applicable.

### Return values

`Ok(HttpResponse)` contains status, headers, owned bytes, attempt count and request ID. `Err(NetError)` reports admission, request validation, transport, body-size, cancellation or timeout failures.

### Errors and behavior

The shared rules above apply. HTTP error status codes do not automatically become errors.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig, HeaderMap};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "http-example",
    )?;
    let result = client.get_bytes_with_headers("/bytes", HeaderMap::new()).await;
    client.shutdown().await?;
    let response = result?;
    println!("{}: {} bytes", response.status(), response.body().len());
    Ok(())
}
```

## `HttpRequest` and `HttpRequestBuilder`

### Required features

`http-client`; the optional codec and cookie methods are linked below.

### Function signatures and parameters

The builder takes ownership of itself and returns the updated builder. All
arguments shown are required. There are no callbacks. Start with
`HttpRequest::builder() -> HttpRequestBuilder` and finish with `build()`.

| Function signature | Purpose and defaults |
| --- | --- |
| `HttpRequest::builder() -> HttpRequestBuilder` | Defaults to GET, empty path, empty headers/body, inherited retry/admission and no timeout overrides. |
| `method(self, method: impl TryInto<Method>) -> Self` | Change method; invalid tokens cause `build()` to return `InvalidInput`. `Method` is re-exported by `open_net::api::http`. |
| `path(self, path: impl Into<String>) -> Self` | Set the target. Empty is invalid at build; full same-origin validation occurs at dispatch. |
| `headers(self, headers: HeaderMap) -> Self` | Replace the entire request-specific map. |
| `header(self, name: impl TryInto<HeaderName>, value: impl TryInto<HeaderValue>) -> Result<Self, NetError>` | Insert/replace one header; invalid conversions return `InvalidInput`. The method consumes the builder even on error. HeaderName is available at the crate root. |
| `body(self, body: Bytes) -> Self` | Set buffered, owned bytes; no automatic media type. Bytes is available at the crate root. |
| `retry_policy(self, policy: RetryPolicy) -> Self` | Explicit policy, including no retries. |
| `retry_setting(self, setting: RetrySetting) -> Self` | Inherit, disable, or replace retries; see [retry](retry.md). |
| `timeouts(self, timeouts: HttpTimeouts) -> Self` | Replace request timeout overrides; see [timeouts](timeouts_and_admission.md). |
| `admission_policy(self, policy: HttpAdmissionPolicy) -> Self` | Override client Wait/FailFast policy before admission; last call wins. |
| `build(self) -> Result<HttpRequest, NetError>` | Validate nonempty path and timeout values. Success owns the request; errors report invalid input/configuration. |
| `HttpRequest::timeouts(&self) -> &HttpTimeouts` | Borrow the stored overrides. |
| `HttpRequest::admission_policy_override(&self) -> Option<HttpAdmissionPolicy>` | Explicit override; None inherits the client. |

Public request fields are `method: Method`, `path: String`, `headers: HeaderMap`,
`body: Bytes`, and `retry_policy: RetryPolicy`. Other state is private; use the
builder instead of a struct literal. A concrete HttpRequest is cloneable but does
not implement HttpRequestTrait. Use `request()` for it, not `send()`.
Directly replacing `retry_policy` does not reset a stored explicit
`RetrySetting::None`; prefer the builder to avoid inconsistent settings.

`query`, `json`, `form` and `multipart` are covered in [request codecs](request_codecs.md).
`cookie_context` is covered in [cookies](cookies.md). Those optional methods are
unavailable with the minimal feature set on this page. Manual JSON bytes, as in
`request()` above, need no codecs feature.

### Steps and example

Build a request before submitting it. This example exercises construction without
contacting a server. Header parsing and build errors are propagated with `?`.

```rust
use open_net::api::http::{HttpAdmissionPolicy, HttpRequest, HttpTimeouts, Method, RetryPolicy, RetrySetting};
use open_net::Bytes;

fn main() -> Result<(), open_net::NetError> {
    let request = HttpRequest::builder()
        .method(Method::PATCH).path("/items/1")
        .headers(open_net::HeaderMap::new())
        .retry_policy(RetryPolicy::no_retry())
        .header("content-type", "application/json")?
        .body(Bytes::from_static(br#"{"active":true}"#))
        .retry_setting(RetrySetting::None)
        .timeouts(HttpTimeouts::new())
        .admission_policy(HttpAdmissionPolicy::Wait)
        .build()?;
    assert_eq!(request.admission_policy_override(), Some(HttpAdmissionPolicy::Wait));
    println!("{:?}", request.timeouts());
    Ok(())
}
```

## `HttpResponse`, `HttpResponseResult` and `HttpStatusPolicy`

### Required features

`http-client`. JSON response decoding requires no extra open-net feature.
This page declares `serde_json` for its example output type; an application
using its own derived type should also add `serde` with its `derive` feature.

### Fields and function signatures

All functions below have no callbacks. Accessors borrow the response and leave
it available. `HttpResponseResult` is `Result<HttpResponse, NetError>`.

| Public field | Type | Meaning |
| --- | --- | --- |
| `status` | `StatusCode` | The peer's status code. |
| `headers` | `HeaderMap` | Response headers. |
| `body` | `Bytes` | Complete, owned body. |
| `attempts` | `u32` | Transport attempts used for this response. |
| `request_id` | `HttpRequestId` | Local operation identifier. |

| Function signature | Return values and errors |
| --- | --- |
| `new(status: StatusCode, headers: HeaderMap, body: Bytes, attempts: u32, request_id: HttpRequestId) -> Self` | Construct a response from all five owned values; no network operation or validation. Useful for tests. |
| `status(&self) -> StatusCode` | Copy of status. |
| `headers(&self) -> &HeaderMap` | Borrowed response header map. |
| `body(&self) -> &Bytes` | Borrowed byte buffer. |
| `attempts(&self) -> u32` | Attempt count. |
| `request_id(&self) -> HttpRequestId` | Copy of originating ID. |
| `content_type(&self) -> Option<&str>` | Valid string header value, or None if absent or invalid. |
| `is_success(&self) -> bool` | True for 2xx, regardless of body content. |
| `text(&self) -> Result<&str, std::str::Utf8Error>` | Borrowed UTF-8 body; invalid UTF-8 is a Utf8Error, not NetError. No charset conversion. |
| `json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error>` | Owned decoded value; invalid JSON or an incompatible shape is a serde_json::Error. Does not require or check Content-Type. |
| `check_status(&self, policy: HttpStatusPolicy) -> Result<(), NetError>` | Ok for accepted status; otherwise ErrorKind::HttpStatus with `http_status()` set. Body remains accessible. |
| `HttpStatusPolicy::accepts(self, status: StatusCode) -> bool` | Classifies only the supplied status, without generating an error. |

Policies are `Any` (all codes), `Success2xx` (200–299), and `Exact(StatusCode)`.
The constructor and accessors are synchronous. Optional `synthetic()` is covered
in [middleware](middleware.md); `info()` and `protocol()` in [HTTP/2](http2.md).

### Steps, errors and example

First handle transport failure, then select a status policy, then decode the
body. Do not turn UTF-8/JSON errors into network errors accidentally. A boxed
standard error, as below, can carry all three error types. This offline example
constructs a buffered response explicitly and prints the decoded JSON.

```rust
use open_net::api::http::{HttpRequestId, HttpResponse, HttpStatusPolicy};
use open_net::{Bytes, HeaderMap, HeaderValue, StatusCode};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    let response = HttpResponse::new(
        StatusCode::OK, headers, Bytes::from_static(br#"{"ok":true}"#), 1, HttpRequestId(1),
    );
    response.check_status(HttpStatusPolicy::Success2xx)?;
    assert!(HttpStatusPolicy::Exact(StatusCode::OK).accepts(response.status()));
    assert!(response.is_success());
    println!("{:?} {} {} {:?}", response.content_type(), response.attempts(),
             response.headers().len(), response.request_id());
    println!("{} bytes: {}", response.body().len(), response.text()?);
    let json: serde_json::Value = response.json()?;
    println!("{json}");
    Ok(())
}
```

## Error inspection

`NetError` implements the standard error trait and is cloneable. Inspect
`kind() -> ErrorKind`, `context() -> &ErrorContext`,
`http_status() -> Option<StatusCode>`, `timeout_phase() -> Option<TimeoutPhase>`,
`config_error() -> Option<&ConfigError>` and `io_kind() -> Option<std::io::ErrorKind>`.
A missing Option value means that diagnostic is unavailable. Configuration errors
expose `field() -> &str` and `reason() -> &str`.

If response headers arrived but body collection failed, inspect
`error.http_response_head()` for the retained status and headers. A retained head
does not turn a failed body read into a successful response; handle the original
error as well. Failures before response headers have no head.
Explicit cancellation also has no head, even if headers arrived before the
cancellation won. Retried attempts do not reuse an earlier attempt's metadata.
See [`NetError::http_response_head()`](#neterrorhttp_response_head) for the full
buffered-response contract and an inspection example.

For HTTP-only consumers the public ErrorContext fields are
`stage: Option<ErrorStage>`, `http_status: Option<StatusCode>`,
`http_request_id: Option<HttpRequestId>`, `http_timeout_phase: Option<TimeoutPhase>`
and `http_attempt: Option<u32>` (one-based attempts; zero before the first attempt).
`ErrorKind` and `ErrorStage` are non-exhaustive; include `_` in matches.
Useful classifications include `InvalidInput`, `InvalidConfig`, `QueueFull`,
`Closed`, `Dns`, `Tls`, `Io`, `Protocol`, `Cancelled`, `TimedOut`,
`DeadlineExceeded`, `RetryExhausted`, `ItemTooLarge` and `HttpStatus`.
Do not classify all errors as HTTP status failures.

## `NetError::http_response_head()`

Inspect status and headers received before a buffered response body failed.

### Required features

`http-client`. Import `HttpResponseHead` from `open_net::api::http`; `NetError`
is also exported at the crate root.

### Function signature

```text
pub fn http_response_head(&self) -> Option<&HttpResponseHead>
```

### Steps

Match the request's `Result`. On `Err`, first inspect the original failure, then
use this optional metadata to explain which response could not be fully read.
Retain the error while borrowing its head, or clone the head to keep it separately.

### Parameters

No argument beyond `&self`, the error returned by a buffered request or delivered
to a callback's `deal_with_response` handler.

### Callback parameters

None. This accessor does not invoke `HttpRequestTrait::on_response_headers`.
That hook runs only for a final successfully buffered response.

### Return values

`Some(&HttpResponseHead)` borrows these public fields from the error:

| Field | Type | Meaning |
| --- | --- | --- |
| `status` | `StatusCode` | Actual status received from the peer for this attempt. |
| `headers` | `HeaderMap` | Received headers, including repeated and potentially sensitive values. |

`None` means no retained head is available. `HttpResponseHead` implements `Clone`;
its `Debug` output includes status and header count, omitting header values.
`error.http_status()` also reports the status when a head is retained.

### Errors and behavior

The accessor itself cannot fail. Body size limits, read errors and body-phase
timeouts can carry a head once headers have arrived. The request remains `Err`,
even when the status is 200; this is not a partially successful `HttpResponse`
and no partial body is exposed here. Continue handling `kind()`, `context()`,
`source()` and `timeout_phase()` as the authoritative failure diagnostics.

Failures before receiving a response and explicit cancellation have no head.
Each retry has its own metadata: a later attempt that fails before headers does
not inherit an earlier response. Streamed responses expose status and headers
on `HttpStreamResponse` before body consumption; this accessor describes buffered
collection failures. Read only the header values the application needs and avoid
logging secrets or arbitrary peer-supplied values.

### Example

Run a local HTTP server whose `/large` route returns a body longer than 16 bytes.
This program deliberately sets a small buffered-body limit. A size failure after
headers should print a retained status; a connection failure before headers prints
that no head is available. Cleanup runs in either case.

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};
use std::error::Error;

#[tokio::main]
async fn main() -> open_net::Result<()> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let config = HttpClientConfig::new("http://127.0.0.1:8080")?
        .with_max_response_bytes(16)?;
    let client = HttpClient::new(config, "response-head")?;
    let result = client.get("/large").await;
    let cleanup = client.shutdown().await;
    match result {
        Ok(response) => println!("Complete response: {}", response.status()),
        Err(error) => {
            eprintln!("Failure: {:?}, timeout: {:?}", error.kind(), error.timeout_phase());
            if let Some(head) = error.http_response_head() {
                eprintln!("Received HTTP {}, {} headers", head.status, head.headers.len());
            } else {
                eprintln!("No retained response head");
            }
            eprintln!("Underlying source present: {}", error.source().is_some());
        }
    }
    cleanup
}
```

## `HttpRequest::builder()`

### Required features

`http-client`.

### Function signature

```text
pub fn builder() -> HttpRequestBuilder
```

### Steps

Start a builder, set a nonempty relative path, then call build.

### Parameters

None.

### Callback parameters

Not applicable.

### Return values

A new owned HttpRequestBuilder.

### Errors and behavior

Defaults are GET, empty headers/body/path, inherited retry/admission and no timeout overrides. It performs no I/O and cannot fail; an untouched builder fails at build because its path is empty.

### Example

[Request construction](#httprequest-and-httprequestbuilder) contains the complete offline program using this method.

## `HttpRequestBuilder::method()`

### Required features

`http-client`.

### Function signature

```text
pub fn method(self, method: impl TryInto<Method>) -> Self
```

### Steps

Select a method before building the request.

### Parameters

`self` is consumed; `method` is required and converted to Method.

### Callback parameters

Not applicable.

### Return values

The updated owned builder.

### Errors and behavior

The last call replaces the previous method, including any conversion error.
Conversion follows the supplied TryInto implementation. Invalid method tokens are
reported by `build()` as `InvalidInput` at `RequestBuild`, without exposing the
supplied text or substituting another method. Choosing a non-idempotent method
changes replay safety; see the retry guide.

### Example

[Request construction](#httprequest-and-httprequestbuilder) contains the complete offline program using this method.

## `HttpRequestBuilder::path()`

### Required features

`http-client`.

### Function signature

```text
pub fn path(self, path: impl Into<String>) -> Self
```

### Steps

Choose a safe relative target, optionally including a query, then build and submit.

### Parameters

`self` is consumed; `path` is a required string-convertible target.

### Callback parameters

Not applicable.

### Return values

The updated owned builder.

### Errors and behavior

An empty path fails at build. Absolute URLs, cross-origin and unsafe targets are rejected at dispatch. With codecs enabled, previously encoded query parts are reapplied to the replacement path.

### Example

[Request construction](#httprequest-and-httprequestbuilder) contains the complete offline program using this method.

## `HttpRequestBuilder::headers()`

### Required features

`http-client`.

### Function signature

```text
pub fn headers(self, headers: HeaderMap) -> Self
```

### Steps

Prepare a request-specific map and replace the builder map before adding individual headers.

### Parameters

`self` is consumed; `headers` is a required owned HeaderMap.

### Callback parameters

Not applicable.

### Return values

The updated owned builder.

### Errors and behavior

This replaces every builder header, rather than merging. At dispatch the request map overrides client common headers; transport-controlled headers are rejected. With multipart, build also checks that its SDK-owned Content-Type was preserved.

### Example

[Request construction](#httprequest-and-httprequestbuilder) contains the complete offline program using this method.

## `HttpRequestBuilder::header()`

### Required features

`http-client`.

### Function signature

```text
pub fn header(self, name: impl TryInto<HeaderName>, value: impl TryInto<HeaderValue>) -> Result<Self, NetError>
```

### Steps

Insert one header, propagate any parsing error with ?, and continue building.

### Parameters

`self` is consumed; `name` and `value` are required and must convert to valid header values.

### Callback parameters

Not applicable.

### Return values

`Ok(builder)` returns the updated builder. `Err(error)` reports invalid input; ownership of the original builder is not returned.

### Errors and behavior

Insertion replaces the named value. Invalid name/value conversions return InvalidInput. With codecs, replacing a multipart Content-Type with a different value is rejected. This checks syntax, while dispatch also checks forbidden headers.

### Example

[Request construction](#httprequest-and-httprequestbuilder) contains the complete offline program using this method.

## `HttpRequestBuilder::body()`

### Required features

`http-client`.

### Function signature

```text
pub fn body(self, body: Bytes) -> Self
```

### Steps

Create owned bytes and attach them before build.

### Parameters

`self` is consumed; `body: Bytes` is required and moved into the builder.

### Callback parameters

Not applicable.

### Return values

The updated owned builder.

### Errors and behavior

The body is fully buffered. This method supplies no Content-Type. Repeated raw-body calls replace the bytes; combining raw bytes with a typed codec is rejected when codecs are enabled.

### Example

[Request construction](#httprequest-and-httprequestbuilder) contains the complete offline program using this method.

## `HttpRequestBuilder::build()`

### Required features

`http-client`.

### Function signature

```text
pub fn build(self) -> Result<HttpRequest, NetError>
```

### Steps

Finish construction, handle validation errors, then pass the request to request() or stream().

### Parameters

`self` is consumed; there are no additional arguments.

### Callback parameters

Not applicable.

### Return values

`Ok(request)` owns all request data. `Err(error)` reports invalid input/configuration and does not return the builder.

### Errors and behavior

Validates a nonempty path and request timeout durations; codec body conflicts and multipart headers are checked when enabled. Same-origin/path safety and request header restrictions are validated later at dispatch. A built request implements Clone but not HttpRequestTrait.

### Example

[Request construction](#httprequest-and-httprequestbuilder) contains the complete offline program using this method.

## Request policy method reference

Complete individual contracts for the remaining base request methods are in
the guides that explain their interaction with client defaults:

- [HttpRequest::timeouts](timeouts_and_admission.md#httprequesttimeouts)
- [HttpRequest::admission_policy_override](timeouts_and_admission.md#httprequestadmission_policy_override)
- [HttpRequestBuilder::timeouts](timeouts_and_admission.md#httprequestbuildertimeouts)
- [HttpRequestBuilder::admission_policy](timeouts_and_admission.md#httprequestbuilderadmission_policy)
- [HttpRequestBuilder::retry_policy](retry.md#httprequestbuilderretry_policy)
- [HttpRequestBuilder::retry_setting](retry.md#httprequestbuilderretry_setting)

## `HttpResponse::new()`

### Required features

`http-client`.

### Function signature

```text
pub fn new(status: StatusCode, headers: HeaderMap, body: Bytes, attempts: u32, request_id: HttpRequestId) -> Self
```

### Steps

Construct a response from known metadata for a test or custom integration.

### Parameters

`status`, `headers`, `body`, `attempts` and `request_id` are all required. Headers and body move into the response.

### Callback parameters

Not applicable.

### Return values

An owned HttpResponse; construction returns no Result.

### Errors and behavior

This stores the supplied values without network I/O or validation. A manually supplied attempt count or request ID does not register a real operation. Optional protocol metadata starts at its default.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## `HttpResponse::status()`

### Required features

`http-client`.

### Function signature

```text
pub fn status(&self) -> StatusCode
```

### Steps

Obtain a buffered response, then inspect it before dropping it.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

A copy of the response status.

### Errors and behavior

No status policy is applied automatically; 4xx/5xx still represent successful transport responses.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## `HttpResponse::headers()`

### Required features

`http-client`.

### Function signature

```text
pub fn headers(&self) -> &HeaderMap
```

### Steps

Obtain a buffered response, then inspect it before dropping it.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

A borrowed response header map.

### Errors and behavior

The borrow cannot outlive the response; no response data is changed.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## `HttpResponse::body()`

### Required features

`http-client`.

### Function signature

```text
pub fn body(&self) -> &Bytes
```

### Steps

Obtain a buffered response, then inspect it before dropping it.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

A borrowed fully buffered body.

### Errors and behavior

The buffer is not decoded or copied; clone the Bytes value if independently owned bytes are required.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## `HttpResponse::attempts()`

### Required features

`http-client`.

### Function signature

```text
pub fn attempts(&self) -> u32
```

### Steps

Obtain a buffered response, then inspect it before dropping it.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

The transport attempt count stored with this response.

### Errors and behavior

Synthetic middleware responses have zero transport attempts. This accessor does not count application callbacks or independently submitted operations.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## `HttpResponse::request_id()`

### Required features

`http-client`.

### Function signature

```text
pub fn request_id(&self) -> HttpRequestId
```

### Steps

Obtain a buffered response, then inspect it before dropping it.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

A copy of the originating local request ID.

### Errors and behavior

This identifier is local correlation metadata, not a server-visible ID or a retained cancellation handle.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## `HttpResponse::content_type()`

### Required features

`http-client`.

### Function signature

```text
pub fn content_type(&self) -> Option<&str>
```

### Steps

Obtain a buffered response, then inspect it before dropping it.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Some borrowed header text, or None if Content-Type is missing or not valid header text.

### Errors and behavior

This does not parse media-type parameters or verify the body matches the declared type.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## `HttpResponse::is_success()`

### Required features

`http-client`.

### Function signature

```text
pub fn is_success(&self) -> bool
```

### Steps

Obtain a buffered response, then inspect it before dropping it.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

True for status codes 200 through 299; false otherwise.

### Errors and behavior

Only the status is inspected. It does not validate or decode the body and returns no error.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## `HttpResponse::text()`

### Required features

`http-client`.

### Function signature

```text
pub fn text(&self) -> Result<&str, std::str::Utf8Error>
```

### Steps

Obtain a buffered response, then inspect it before dropping it.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Ok with a borrowed UTF-8 string, or Err(Utf8Error) for invalid UTF-8.

### Errors and behavior

No charset conversion is performed. This error is not NetError and is independent of the response status.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## `HttpResponse::json()`

### Required features

`http-client`.

### Function signature

```text
pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error>
```

### Steps

Choose an owned target type and decode a buffered body, separately handling transport/status failures.

### Parameters

None beyond `&self`; the type parameter T must implement DeserializeOwned.

### Callback parameters

Not applicable.

### Return values

`Ok(value)` owns the decoded T. `Err(serde_json::Error)` reports invalid JSON or an incompatible target shape.

### Errors and behavior

The Content-Type header is neither required nor checked. This requires http-client, not the request-codecs feature. The target cannot borrow from the response; the original body remains available.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## `HttpResponse::check_status()`

### Required features

`http-client`.

### Function signature

```text
pub fn check_status(&self, policy: HttpStatusPolicy) -> Result<(), NetError>
```

### Steps

Select which status codes are acceptable and check before decoding if that fits the application.

### Parameters

`policy: HttpStatusPolicy` is required; `&self` borrows the response.

### Callback parameters

Not applicable.

### Return values

`Ok(())` means the status is accepted. `Err(error)` has ErrorKind::HttpStatus and the rejected code in http_status().

### Errors and behavior

The response and body remain available after rejection. Any accepts every status, Success2xx accepts 200–299 and Exact accepts only its specified code.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## `HttpStatusPolicy::accepts()`

### Required features

`http-client`.

### Function signature

```text
pub fn accepts(self, status: StatusCode) -> bool
```

### Steps

Select a policy and pass a status to classify it without constructing an error.

### Parameters

`self` is the policy value; `status: StatusCode` is required.

### Callback parameters

Not applicable.

### Return values

True if accepted by the selected policy, otherwise false.

### Errors and behavior

Checks only the status and has no side effects. Any accepts all statuses, Success2xx accepts the 2xx range, and Exact accepts one code.

### Example

[Buffered response example](#httpresponse-httpresponseresult-and-httpstatuspolicy) constructs and inspects one response with this method.

## Structured error example

This offline program demonstrates diagnostic accessors. A missing optional
diagnostic is normal; do not unwrap it unless the originating contract guarantees
that field. Invalid timeout configuration supplies ConfigError details.

```rust
use open_net::api::http::{HttpRequest, HttpTimeouts};
use open_net::{NetError, StatusCode};

fn main() -> Result<(), NetError> {
    let status_error = NetError::from_http_status(StatusCode::NOT_FOUND);
    println!("{:?} {:?}", status_error.kind(), status_error.context());
    assert_eq!(status_error.http_status(), Some(StatusCode::NOT_FOUND));
    assert_eq!(status_error.timeout_phase(), None);
    let io_error = NetError::from(std::io::Error::from(std::io::ErrorKind::BrokenPipe));
    assert_eq!(io_error.io_kind(), Some(std::io::ErrorKind::BrokenPipe));
    let error = HttpRequest::builder().path("/health")
        .timeouts(HttpTimeouts::new().with_total_timeout(std::time::Duration::ZERO))
        .build().expect_err("zero timeout must fail");
    if let Some(details) = error.config_error() {
        println!("{}: {}", details.field(), details.reason());
    }
    let protocol = NetError::protocol(std::io::Error::other("invalid application frame"));
    let provider = NetError::provider(std::io::Error::other("provider unavailable"));
    println!("{:?} {:?}", protocol.kind(), provider.kind());
    Ok(())
}
```

## `NetError::kind()`

### Required features

`http-client`.

### Function signature

```text
pub fn kind(&self) -> ErrorKind
```

### Steps

Keep the error and inspect the relevant diagnostic before choosing application recovery.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

The broad error classification.

### Errors and behavior

ErrorKind is non-exhaustive; include a fallback when matching. Classification alone does not retain every diagnostic.

### Example

[Structured error example](#structured-error-example) calls this method without contacting a server.

## `NetError::context()`

### Required features

`http-client`.

### Function signature

```text
pub fn context(&self) -> &ErrorContext
```

### Steps

Keep the error and inspect the relevant diagnostic before choosing application recovery.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

A borrowed structured context.

### Errors and behavior

Fields are optional. HTTP attempt is one-based after an attempt starts and may be zero before the first attempt.

### Example

[Structured error example](#structured-error-example) calls this method without contacting a server.

## `NetError::http_status()`

### Required features

`http-client`.

### Function signature

```text
pub fn http_status(&self) -> Option<StatusCode>
```

### Steps

Keep the error and inspect the relevant diagnostic before choosing application recovery.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Some associated HTTP status, or None when no status was recorded.

### Errors and behavior

A transport failure can have no status; a received 4xx/5xx only becomes a status error after explicit checking.

### Example

[Structured error example](#structured-error-example) calls this method without contacting a server.

## `NetError::timeout_phase()`

### Required features

`http-client`.

### Function signature

```text
pub fn timeout_phase(&self) -> Option<TimeoutPhase>
```

### Steps

Keep the error and inspect the relevant diagnostic before choosing application recovery.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Some expired HTTP budget phase, or None for other failures.

### Errors and behavior

Available with http-client. Total-budget expiry is DeadlineExceeded; phase-specific expiry is TimedOut. Do not infer the phase solely from the broad kind.

### Example

[Structured error example](#structured-error-example) calls this method without contacting a server.

## `NetError::config_error()`

### Required features

`http-client`.

### Function signature

```text
pub fn config_error(&self) -> Option<&ConfigError>
```

### Steps

Keep the error and inspect the relevant diagnostic before choosing application recovery.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Some borrowed configuration detail, or None if none was recorded.

### Errors and behavior

Use field and reason to explain the invalid input. This accessor does not rerun validation.

### Example

[Structured error example](#structured-error-example) calls this method without contacting a server.

## `NetError::io_kind()`

### Required features

`http-client`.

### Function signature

```text
pub fn io_kind(&self) -> Option<std::io::ErrorKind>
```

### Steps

Keep the error and inspect the relevant diagnostic before choosing application recovery.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

The first retained I/O kind in the source chain, or None if none is found.

### Errors and behavior

Inspection traverses at most 64 sources to avoid an unbounded walk through a cyclic custom source chain. None does not prove that the original cause was unrelated to I/O.

### Example

[Structured error example](#structured-error-example) calls this method without contacting a server.

## `ConfigError::field()`

### Required features

`http-client`.

### Function signature

```text
pub fn field(&self) -> &str
```

### Steps

Obtain Some(details) from NetError::config_error, then inspect it.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

The borrowed name of the rejected configuration field.

### Errors and behavior

The accessor is infallible and does not change the parent error.

### Example

[Structured error example](#structured-error-example) calls this method without contacting a server.

## `ConfigError::reason()`

### Required features

`http-client`.

### Function signature

```text
pub fn reason(&self) -> &str
```

### Steps

Obtain Some(details) from NetError::config_error, then inspect it.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

The borrowed human-readable reason for rejection.

### Errors and behavior

Use this explanation for diagnostics; prefer structured kind/field checks to parsing prose.

### Example

[Structured error example](#structured-error-example) calls this method without contacting a server.

## `NetError::from_http_status()`

### Required features

`http-client`.

### Function signature

```text
pub fn from_http_status(status: StatusCode) -> Self
```

### Steps

Create an error from a known status or owned error source when implementing an integration.

### Parameters

`status: StatusCode` is required.

### Callback parameters

Not applicable.

### Return values

A NetError with HttpStatus kind and the supplied status recorded.

### Errors and behavior

This constructor does not decide whether the supplied code is an application failure; use a status policy first when needed.

### Example

[Structured error example](#structured-error-example) calls this method without contacting a server.

## `NetError::protocol()`

### Required features

`http-client`.

### Function signature

```text
pub fn protocol(source: impl Into<BoxError>) -> Self
```

### Steps

Create an error from a known status or owned error source when implementing an integration.

### Parameters

`source` is required and must convert to `BoxError`, the alias `Box<dyn std::error::Error + Send + Sync + 'static>`. Its owned data must be safe to transfer and share between threads.

### Callback parameters

Not applicable.

### Return values

A Protocol NetError retaining the source.

### Errors and behavior

Use for protocol failures; this does not turn an error into an HTTP status or assign a timeout phase.

### Example

[Structured error example](#structured-error-example) calls this method without contacting a server.

## `NetError::provider()`

### Required features

`http-client`.

### Function signature

```text
pub fn provider(source: impl Into<BoxError>) -> Self
```

### Steps

Create an error from a known status or owned error source when implementing an integration.

### Parameters

`source` is required and must convert to `BoxError`, the alias `Box<dyn std::error::Error + Send + Sync + 'static>`. Its owned data must be safe to transfer and share between threads.

### Callback parameters

Not applicable.

### Return values

A ProviderFailed NetError with Provider stage, retaining the source.

### Errors and behavior

This classifies a user or transport provider failure. Specific SDK provider boundaries may sanitize returned provider errors; consult the authentication guide.

### Example

[Structured error example](#structured-error-example) calls this method without contacting a server.

## Awaiting a buffered response

When application code needs one buffered response to continue an async operation,
use `client.request(request).await` directly. There is no need to implement
`HttpRequestTrait`, forward its callback through a oneshot channel, or maintain a
second cancel-on-drop guard. Owned callbacks are useful when the SDK should
run a request's response handler; see [callback requests](callback_requests.md).

This example uses waiting admission and explicitly disables retries even
if the client's defaults change. `remaining` must be positive. Disabling the
separate admission timeout leaves the total budget in force, including while
waiting for capacity. The program needs a local `/echo` endpoint and the
dependencies listed above.

```rust
use open_net::api::http::{
    HttpAdmissionPolicy, HttpClient, HttpClientConfig, HttpRequest, HttpResponse,
    HttpTimeouts, RetryPolicy,
};
use std::time::Duration;

async fn post_json(
    client: &HttpClient,
    body: String,
    remaining: Duration,
) -> open_net::Result<HttpResponse> {
    let request = HttpRequest::builder()
        .method("POST")
        .path("/echo")
        .header("content-type", "application/json")?
        .body(body.into_bytes().into())
        .admission_policy(HttpAdmissionPolicy::Wait)
        .retry_policy(RetryPolicy::no_retry())
        .timeouts(
            HttpTimeouts::new()
                .with_total_timeout(remaining)
                .without_admission_timeout(),
        )
        .build()?;
    client.request(request).await
}

#[tokio::main]
async fn main() -> open_net::Result<()> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "async-response",
    )?;
    let result = post_json(&client, r#"{"hello":"world"}"#.into(), Duration::from_secs(3)).await;
    let cleanup = client.shutdown().await;
    if let Err(error) = &result {
        if let Some(head) = error.http_response_head() {
            eprintln!("HTTP {} received before failure; {} headers", head.status, head.headers.len());
        }
    }
    let response = result?;
    cleanup?;
    println!("{}: {} bytes", response.status(), response.body().len());
    Ok(())
}
```

The request's total budget starts when it is polled for admission. It excludes
client lookup/creation and earlier application work. If those operations share
one application deadline, bound them separately, compute the remaining duration
before building the request, and reject an exhausted budget instead of passing
zero. Keep [the caller's Tokio runtime](README.md#start-here) alive while requests
or open streams use it; the Future API requires that runtime as well.
Shutdown itself can be awaited without that runtime because the SDK
owns cleanup.

Dropping a pending request future requests cancellation; it neither waits for
cleanup nor rolls back server-side effects. Session checks, 401 handling,
application error codes, accepting only status 200, and application URL construction
belong in the application's adapter. Recheck application state before applying a
delivered result. For teardown, follow [cancellation and shutdown](lifecycle.md):
HTTP request callbacks must finish, while network observation callbacks require
their own external `Subscription::close()` wait.

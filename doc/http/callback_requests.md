# Callback requests and completion handles

[HTTP guide](README.md) · [Library overview](../../README.md)

## Required features and example dependencies

Use Rust 1.89 or later and the `http-client` feature (it also enables `net-status`).
Create a consumer with `cargo new http-example` and add these dependencies.
Replace `/path/to/open-net` with the library directory containing Cargo.toml.

```toml
[dependencies]
open-net = { path = "/path/to/open-net", default-features = false, features = ["http-client"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

Each Rust block is an independent `src/main.rs`; `cargo check` compiles it.
Sending examples need a server at `http://127.0.0.1:8080` with the stated route.
Use `cargo run` only when that prerequisite is satisfied. Compilation is not a
network test. Construction and cleanup examples that send nothing need no server.

## Admission, ownership and completion

The four `send` methods consume a custom `HttpRequestTrait` implementation.
An `Ok(handle)` means the SDK accepted ownership. It does not mean the response
arrived or the callback ran. Accepted requests receive one terminal callback
under normal worker operation, including cancellation and shutdown. Worker
termination or callback panics are reflected by completion state.

An ordinary `Err(EnqueueError<R>)` preserves the original request, including any
owned application state. Rejected requests have no automatic callback. The
application may retry submission, discard the request, or invoke its handler
explicitly. Dropping a pending `send_wait*()` future drops its owned request;
there is no error value to recover and no callback. Configure an admission
timeout when you need ownership returned after a bounded wait.

`Send + 'static` allows the request to move to SDK worker threads and outlive the
submission stack. It need not be `Sync`, `Clone` or `Debug`. A request can own a
`FnOnce + Send` closure. `'static` does not mean “lives forever”; captured borrowed
stack data must be replaced with owned data or an appropriate shared owner.

## `HttpClient::send()`

Attempt immediate admission and obtain a completion handle.

### Required features

`http-client`.

### Function signature

```text
pub fn send<R>(
    &self, request: R,
) -> Result<HttpRequestHandle, EnqueueError<R>>
where
    R: HttpRequestTrait
```

### Steps

Implement HttpRequestTrait, start a server with /health, submit the request, then await handle.finished() and shut down externally.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| request | R: HttpRequestTrait | Yes | Owned custom request and its callback state. |

### Callback parameters

`deal_with_response(self: Box<Self>, result: HttpResponseResult)` consumes the request on the callback lane. `result` is the final buffered response or NetError. The optional header hook receives `&HeaderMap`. These are trait callbacks, not arguments to the send method.

### Return values

`Ok(HttpRequestHandle)` transfers ownership to the SDK. `Err(EnqueueError<R>)` returns the rejected request and its NetError. Neither return path is a buffered response.

### Errors and behavior

Does not wait for capacity. Capacity exhaustion (for example QueueFull), duplicate custom IDs, invalid options and closed admission return EnqueueError with the input preserved. A trait retry policy with max_retries() > 0 overrides the client; otherwise it inherits the client default. retry_policy() runs before admission; a panic there unwinds on the submitting thread/future and is not an ownership-preserving returned error.

### Example

```rust
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpRequestTrait, HttpRequestMethod, HttpResponseResult
};

struct Health;

#[open_net::async_trait]
impl HttpRequestTrait for Health {
    fn get_path(&self) -> String { "/health".into() }
    fn get_method(&self) -> HttpRequestMethod { HttpRequestMethod::GET }
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        match result {
            Ok(response) => println!("HTTP status: {}", response.status()),
            Err(error) => eprintln!("Request failed: {error}"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "callback-example",
    )?;
    match client.send(Health) {
        Ok(handle) => {
            let completion = handle.finished().await;
            println!("Transport summary: {:?}", completion.response);
            if let Err(error) = completion.callback {
                eprintln!("Callback failed: {error}");
            }
        }
        Err(rejected) => {
            let (request, error) = rejected.into_parts();
            // This handler call is an explicit application decision.
            Box::new(request).deal_with_response(Err(error)).await;
        }
    }
    client.shutdown().await?;
    Ok(())
}
```

## `HttpClient::send_with_options()`

Attempt immediate admission and obtain a completion handle.

### Required features

`http-client`.

### Function signature

```text
pub fn send_with_options<R>(
    &self, request: R, options: HttpRequestOptions,
) -> Result<HttpRequestHandle, EnqueueError<R>>
where
    R: HttpRequestTrait
```

### Steps

Implement HttpRequestTrait, start a server with /health, submit the request, then await handle.finished() and shut down externally.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| request | R: HttpRequestTrait | Yes | Owned custom request and its callback state. |
| options | HttpRequestOptions | Yes | Authoritative retry, timeouts, request ID and replay settings. |

### Callback parameters

`deal_with_response(self: Box<Self>, result: HttpResponseResult)` consumes the request on the callback lane. `result` is the final buffered response or NetError. The optional header hook receives `&HeaderMap`. These are trait callbacks, not arguments to the send method.

### Return values

`Ok(HttpRequestHandle)` transfers ownership to the SDK. `Err(EnqueueError<R>)` returns the rejected request and its NetError. Neither return path is a buffered response.

### Errors and behavior

Does not wait for capacity. Capacity exhaustion (for example QueueFull), duplicate custom IDs, invalid options and closed admission return EnqueueError with the input preserved. Options override the trait policy. An inherited option uses the client default, even if the trait provides a positive policy.

### Example

```rust
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpRequestTrait, HttpRequestMethod, HttpResponseResult,
    HttpRequestOptions, HttpTimeouts, RetrySetting
};

struct Health;

#[open_net::async_trait]
impl HttpRequestTrait for Health {
    fn get_path(&self) -> String { "/health".into() }
    fn get_method(&self) -> HttpRequestMethod { HttpRequestMethod::GET }
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        match result {
            Ok(response) => println!("HTTP status: {}", response.status()),
            Err(error) => eprintln!("Request failed: {error}"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "callback-example",
    )?;
    let options = HttpRequestOptions::new()
        .with_retry_setting(RetrySetting::None)
        .with_timeouts(HttpTimeouts::new().with_admission_timeout(
            std::time::Duration::from_secs(2),
        ))?;
    match client.send_with_options(Health, options) {
        Ok(handle) => {
            let completion = handle.finished().await;
            println!("Transport summary: {:?}", completion.response);
            if let Err(error) = completion.callback {
                eprintln!("Callback failed: {error}");
            }
        }
        Err(rejected) => {
            let (request, error) = rejected.into_parts();
            // This handler call is an explicit application decision.
            Box::new(request).deal_with_response(Err(error)).await;
        }
    }
    client.shutdown().await?;
    Ok(())
}
```

## `HttpClient::send_wait()`

Wait for admission and obtain a completion handle.

### Required features

`http-client`.

### Function signature

```text
pub async fn send_wait<R>(
    &self, request: R,
) -> Result<HttpRequestHandle, EnqueueError<R>>
where
    R: HttpRequestTrait
```

### Steps

Implement HttpRequestTrait, start a server with /health, submit the request, then await handle.finished() and shut down externally.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| request | R: HttpRequestTrait | Yes | Owned custom request and its callback state. |

### Callback parameters

`deal_with_response(self: Box<Self>, result: HttpResponseResult)` consumes the request on the callback lane. `result` is the final buffered response or NetError. The optional header hook receives `&HeaderMap`. These are trait callbacks, not arguments to the send method.

### Return values

`Ok(HttpRequestHandle)` transfers ownership to the SDK. `Err(EnqueueError<R>)` returns the rejected request and its NetError. Neither return path is a buffered response.

### Errors and behavior

Waits for both in-flight capacity and a queue slot using one admission deadline from the first poll. The default admission budget is 30 seconds, independent of attempt timeouts. Timeout, shutdown and other returned errors retain the request; dropping the waiting future does not. A trait retry policy with max_retries() > 0 overrides the client; otherwise it inherits the client default. retry_policy() runs before admission; a panic there unwinds on the submitting thread/future and is not an ownership-preserving returned error.

### Example

```rust
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpRequestTrait, HttpRequestMethod, HttpResponseResult
};

struct Health;

#[open_net::async_trait]
impl HttpRequestTrait for Health {
    fn get_path(&self) -> String { "/health".into() }
    fn get_method(&self) -> HttpRequestMethod { HttpRequestMethod::GET }
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        match result {
            Ok(response) => println!("HTTP status: {}", response.status()),
            Err(error) => eprintln!("Request failed: {error}"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "callback-example",
    )?;
    match client.send_wait(Health).await {
        Ok(handle) => {
            let completion = handle.finished().await;
            println!("Transport summary: {:?}", completion.response);
            if let Err(error) = completion.callback {
                eprintln!("Callback failed: {error}");
            }
        }
        Err(rejected) => {
            let (request, error) = rejected.into_parts();
            // This handler call is an explicit application decision.
            Box::new(request).deal_with_response(Err(error)).await;
        }
    }
    client.shutdown().await?;
    Ok(())
}
```

## `HttpClient::send_wait_with_options()`

Wait for admission and obtain a completion handle.

### Required features

`http-client`.

### Function signature

```text
pub async fn send_wait_with_options<R>(
    &self, request: R, options: HttpRequestOptions,
) -> Result<HttpRequestHandle, EnqueueError<R>>
where
    R: HttpRequestTrait
```

### Steps

Implement HttpRequestTrait, start a server with /health, submit the request, then await handle.finished() and shut down externally.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| request | R: HttpRequestTrait | Yes | Owned custom request and its callback state. |
| options | HttpRequestOptions | Yes | Authoritative retry, timeouts, request ID and replay settings. |

### Callback parameters

`deal_with_response(self: Box<Self>, result: HttpResponseResult)` consumes the request on the callback lane. `result` is the final buffered response or NetError. The optional header hook receives `&HeaderMap`. These are trait callbacks, not arguments to the send method.

### Return values

`Ok(HttpRequestHandle)` transfers ownership to the SDK. `Err(EnqueueError<R>)` returns the rejected request and its NetError. Neither return path is a buffered response.

### Errors and behavior

Waits for both in-flight capacity and a queue slot using one admission deadline from the first poll. The default admission budget is 30 seconds, independent of attempt timeouts. Timeout, shutdown and other returned errors retain the request; dropping the waiting future does not. Options override the trait policy. An inherited option uses the client default, even if the trait provides a positive policy.

### Example

```rust
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpRequestTrait, HttpRequestMethod, HttpResponseResult,
    HttpRequestOptions, HttpTimeouts, RetrySetting
};

struct Health;

#[open_net::async_trait]
impl HttpRequestTrait for Health {
    fn get_path(&self) -> String { "/health".into() }
    fn get_method(&self) -> HttpRequestMethod { HttpRequestMethod::GET }
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        match result {
            Ok(response) => println!("HTTP status: {}", response.status()),
            Err(error) => eprintln!("Request failed: {error}"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "callback-example",
    )?;
    let options = HttpRequestOptions::new()
        .with_retry_setting(RetrySetting::None)
        .with_timeouts(HttpTimeouts::new().with_admission_timeout(
            std::time::Duration::from_secs(2),
        ))?;
    match client.send_wait_with_options(Health, options).await {
        Ok(handle) => {
            let completion = handle.finished().await;
            println!("Transport summary: {:?}", completion.response);
            if let Err(error) = completion.callback {
                eprintln!("Callback failed: {error}");
            }
        }
        Err(rejected) => {
            let (request, error) = rejected.into_parts();
            // This handler call is an explicit application decision.
            Box::new(request).deal_with_response(Err(error)).await;
        }
    }
    client.shutdown().await?;
    Ok(())
}
```

## `HttpRequestTrait`

### Required features

`http-client`. Use the re-exported `#[open_net::async_trait]` attribute on the
implementation so the async consuming method matches the trait.

### Function signatures, parameters and defaults

```text
#[async_trait]
pub trait HttpRequestTrait: Send + 'static {
    fn get_path(&self) -> String;
    fn get_method(&self) -> HttpRequestMethod;
    fn get_req_data(&self) -> String;
    fn get_req_body(&self) -> Bytes;
    fn has_req_body(&self) -> bool;
    fn headers(&self) -> HeaderMap;
    fn retry_policy(&self) -> RetryPolicy;
    fn on_response_headers(&self, headers: &HeaderMap);
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult);
}
```

| Function | Required implementation | Return value or callback parameters |
| --- | --- | --- |
| `get_path` | Yes | Relative request path; read on attempts. Unsafe paths fail after acceptance and reach the terminal callback. |
| `get_method` | Yes | Method for the request. |
| `get_req_data` | No | Legacy textual body; defaults to empty String. |
| `get_req_body` | No | Owned Bytes; default converts get_req_data(). If overridden, this is authoritative. Return equivalent data on each retry. |
| `has_req_body` | No | Defaults false. Nonempty bytes are detected automatically. Return true to explicitly frame an empty body. |
| `headers` | No | Owned per-request HeaderMap; empty by default. Invalid/transport-controlled headers fail at dispatch. |
| `retry_policy` | No | Defaults to no_retry(), which causes send/send_wait to inherit the client. Explicit options can disable retries. |
| `on_response_headers` | No | Runs on the callback lane only for the final successfully buffered response, just before its consuming handler. No-op by default; headers are read-only. |
| `deal_with_response` | Yes | Consumes Box<Self> and the final HttpResponseResult, returns unit asynchronously. |

With `http-client-cookies`, the additional provider
`fn cookie_context(&self) -> CookieRequestContext` defaults to the default
SameSite context; see [cookies](cookies.md). `Box<T>` forwards the trait for
`T: HttpRequestTrait + ?Sized`, so a `Box<dyn HttpRequestTrait>` is accepted.
The complete examples above show the minimal implementation and cleanup.

### Callback errors and lifetime

`on_response_headers` is a completion hook, not an arrival notification. The SDK
finishes buffering the final body before invoking it. Intermediate redirect,
authentication replay and retry responses are not delivered to this hook. If
body collection fails (for example a size limit, timeout or transport error),
the handler receives `Err` and the header hook is skipped, even if headers had
already arrived. The hook cannot reject `Content-Length` before download;
use [streaming](streaming_and_sse.md) to inspect headers before consuming a body.
HTTP 4xx/5xx remain successful response values until the handler applies a status
policy. Callback panics are reported through completion, not returned by the
original send call. Keep handlers bounded: network timeouts do not interrupt
application callbacks. The handler must not await its own `finished()`, the same
client's `drain()`/`shutdown()`, or new work that requires capacity it still holds.
Use immediate `send()` or independent workers for reentrant submissions.

## `HttpRequestMethod`

Required feature: `http-client`. This enum has no callbacks and represents
`GET`, `POST`, `PUT`, `DELETE`, `HEAD`, `OPTIONS`, `CONNECT`, `PATCH`, `TRACE`,
`QUERY` and `Extension(String)`. `as_str(&self) -> &str` returns its method text.
Owned/borrowed conversions to and from `Method` are implemented; comparisons
with Method work directly. Extension(String) must contain valid method bytes:
converting an invalid extension to Method panics. Validate custom methods with
`Method::from_bytes` before converting into HttpRequestMethod.

```rust
use open_net::api::http::{HttpRequestMethod, Method};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let validated = Method::from_bytes(b"CUSTOM")?;
    let method = HttpRequestMethod::from(validated.clone());
    assert_eq!(method.as_str(), "CUSTOM");
    assert_eq!(Method::from(&method), validated);
    Ok(())
}
```

## `EnqueueError<R>`

This error lives at `open_net::EnqueueError` (also `open_net::error::EnqueueError`).
It does not require R to implement Debug. No constructor is public. There are
no callbacks on this type.

| Function signature | Return values and ownership |
| --- | --- |
| `error(&self) -> &NetError` | Inspect the admission failure while retaining the request. |
| `value(&self) -> &R` | Borrow the original request. |
| `into_parts(self) -> (R, NetError)` | Recover both owned values; demonstrated in every submission example. |
| `into_inner(self) -> R` | Recover only the request. |
| `into_error(self) -> NetError` | Keep the error and drop the owned request. |

## `HttpRequestId`, `HttpRequestHandle` and completion

An ID is `pub struct HttpRequestId(pub u64)`, supporting Copy, Clone, ordering,
hashing and equality. Automatic IDs increase without wrapping within one client;
IDs are local correlation values, not server-visible or persistent identifiers.
Custom IDs in HttpRequestOptions must be unique among active operations and may
be reused after cleanup. A handle remains bound to its original operation when
that ID is reused; `client.cancel(id)` instead looks up the currently registered ID.

| Function signature | Parameters | Return values and behavior |
| --- | --- | --- |
| `HttpRequestHandle::id(&self) -> HttpRequestId` | None | Operation correlation ID. |
| `HttpRequestHandle::cancel(&self) -> HttpCancelOutcome` | None | Requests local cancellation before callback dispatch. No server-side rollback. |
| `async HttpRequestHandle::finished(&self) -> HttpCompletion` | None | Wait for header hook, handler and SDK cleanup; cloned handles/repeated waits see cached completion. Detached application tasks are excluded. |

`HttpCancelOutcome` is `Requested` (first request), `AlreadyRequested` (already
cancelled before callback dispatch), or `TooLate` (dispatch started or operation
finished). A determined timeout remains a timeout even if cancellation is requested.

`HttpCompletion` has `request_id: HttpRequestId`,
`response: Result<HttpResponseSummary, NetError>` and
`callback: Result<(), NetError>`. `HttpResponseSummary` has `status: StatusCode`
and `attempts: u32`. The response field describes the transport result delivered
to the callback. The callback field describes execution of both hook and handler,
not whether the application accepted the response. Successful completion keeps
metadata, not body bytes. Callback faults can include CallbackPanicked or runtime
failure; inspect the error instead of assuming an HTTP status failure.

Dropping a handle does not cancel its operation; handles do not retain the client,
request, response or admission capacity. Dropping a `finished()` future also does
not cancel the operation. Dropping the final worker owner initiates shutdown even
if a completion handle still exists. Explicit `cancel()` is a separate action.

This compile-ready helper is callable with any accepted handle. Its empty main
performs no network operation; the four submission programs demonstrate where
to obtain the handle.

```rust
use open_net::api::http::{HttpCancelOutcome, HttpRequestHandle};

async fn cancel_and_wait(handle: HttpRequestHandle) -> Result<(), open_net::NetError> {
    println!("Request ID: {:?}", handle.id());
    match handle.cancel() {
        HttpCancelOutcome::Requested => println!("Cancellation requested"),
        HttpCancelOutcome::AlreadyRequested => println!("Already requested"),
        HttpCancelOutcome::TooLate => println!("Callback started or completed"),
    }
    let completion = handle.finished().await;
    completion.callback?;
    println!("{:?}", completion.response);
    Ok(())
}

fn main() {}
```

## Offline trait and rejection example

This program directly inspects the trait providers, then closes admission to
obtain recoverable rejection values without contacting a server. Direct hook and
handler invocation at the end is an explicit application decision for a locally
constructed response; the SDK never invokes callbacks for rejected submissions.

```rust
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpRequestId, HttpRequestMethod, HttpRequestTrait,
    HttpResponse, HttpResponseResult, RetryPolicy,
};
use open_net::{Bytes, EnqueueError, HeaderMap, NetError, StatusCode};

struct Payload;

#[open_net::async_trait]
impl HttpRequestTrait for Payload {
    fn get_path(&self) -> String { "/echo".into() }
    fn get_method(&self) -> HttpRequestMethod { HttpRequestMethod::POST }
    fn get_req_data(&self) -> String { "demo".into() }
    fn get_req_body(&self) -> Bytes { Bytes::from(self.get_req_data()) }
    fn has_req_body(&self) -> bool { true }
    fn headers(&self) -> HeaderMap { HeaderMap::new() }
    fn retry_policy(&self) -> RetryPolicy { RetryPolicy::no_retry() }
    fn on_response_headers(&self, headers: &HeaderMap) {
        println!("Final headers: {}", headers.len());
    }
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        println!("Application result: {:?}", result.map(|response| response.status()));
    }
}

fn reject(client: &HttpClient) -> EnqueueError<Payload> {
    match client.send(Payload) {
        Err(rejected) => rejected,
        Ok(_) => panic!("admission should already be closed"),
    }
}

#[tokio::main]
async fn main() -> Result<(), NetError> {
    let request = Payload;
    assert_eq!(request.get_path(), "/echo");
    assert_eq!(request.get_method(), HttpRequestMethod::POST);
    assert_eq!(request.get_req_data(), "demo");
    assert_eq!(request.get_req_body().as_ref(), b"demo");
    assert!(request.has_req_body());
    assert!(request.headers().is_empty());
    assert_eq!(request.retry_policy().max_retries(), 0);
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "rejected-request",
    )?;
    client.request_shutdown();
    let rejected = reject(&client);
    println!("{:?}: {}", rejected.error().kind(), rejected.value().get_path());
    let (_request, error) = rejected.into_parts();
    println!("Recovered error: {error}");
    let recovered = reject(&client).into_inner();
    assert_eq!(recovered.get_path(), "/echo");
    println!("Error only: {}", reject(&client).into_error());
    client.shutdown().await?;
    let response = HttpResponse::new(
        StatusCode::OK, HeaderMap::new(), Bytes::new(), 0, HttpRequestId(0),
    );
    request.on_response_headers(response.headers());
    Box::new(request).deal_with_response(Ok(response)).await;
    Ok(())
}
```

## `HttpRequestTrait::get_path()`

### Required features

`http-client`; use `#[open_net::async_trait]` on the trait implementation.

### Function signature

```text
fn get_path(&self) -> String
```

### Steps

Implement this provider on a request type satisfying `Send + 'static`, then submit the request with a send method. The lifetime bound permits owned data and excludes temporary borrowed captures; it does not require the request value to live forever.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

An owned request target.

### Errors and behavior

Required implementation. Return a safe relative path. The worker reads providers during attempts; invalid paths fail after admission through the terminal callback, not as a recoverable enqueue rejection.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) implements and directly calls this method; the submission programs above show SDK dispatch.

## `HttpRequestTrait::get_method()`

### Required features

`http-client`; use `#[open_net::async_trait]` on the trait implementation.

### Function signature

```text
fn get_method(&self) -> HttpRequestMethod
```

### Steps

Implement this provider on a request type satisfying `Send + 'static`, then submit the request with a send method. The lifetime bound permits owned data and excludes temporary borrowed captures; it does not require the request value to live forever.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

An owned method value.

### Errors and behavior

Required implementation. The method participates in replay safety. Validate custom method text before creating an Extension value.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) implements and directly calls this method; the submission programs above show SDK dispatch.

## `HttpRequestTrait::get_req_data()`

### Required features

`http-client`; use `#[open_net::async_trait]` on the trait implementation.

### Function signature

```text
fn get_req_data(&self) -> String
```

### Steps

Implement this provider on a request type satisfying `Send + 'static`, then submit the request with a send method. The lifetime bound permits owned data and excludes temporary borrowed captures; it does not require the request value to live forever.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

An owned textual body; empty by default.

### Errors and behavior

Legacy body provider. Default get_req_body converts this string into bytes. If get_req_body is overridden, that byte provider is authoritative.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) implements and directly calls this method; the submission programs above show SDK dispatch.

## `HttpRequestTrait::get_req_body()`

### Required features

`http-client`; use `#[open_net::async_trait]` on the trait implementation.

### Function signature

```text
fn get_req_body(&self) -> Bytes
```

### Steps

Implement this provider on a request type satisfying `Send + 'static`, then submit the request with a send method. The lifetime bound permits owned data and excludes temporary borrowed captures; it does not require the request value to live forever.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Owned buffered request bytes.

### Errors and behavior

By default converts get_req_data. Providers can be called again on retries; return equivalent replayable bytes. A nonempty byte buffer is detected as a body automatically.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) implements and directly calls this method; the submission programs above show SDK dispatch.

## `HttpRequestTrait::has_req_body()`

### Required features

`http-client`; use `#[open_net::async_trait]` on the trait implementation.

### Function signature

```text
fn has_req_body(&self) -> bool
```

### Steps

Implement this provider on a request type satisfying `Send + 'static`, then submit the request with a send method. The lifetime bound permits owned data and excludes temporary borrowed captures; it does not require the request value to live forever.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

True to explicitly frame a body even when its byte representation is empty; false by default.

### Errors and behavior

The default does not invoke body providers. Nonempty get_req_body bytes already imply a body; override this for an intentional empty body needing framing.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) implements and directly calls this method; the submission programs above show SDK dispatch.

## `HttpRequestTrait::headers()`

### Required features

`http-client`; use `#[open_net::async_trait]` on the trait implementation.

### Function signature

```text
fn headers(&self) -> HeaderMap
```

### Steps

Implement this provider on a request type satisfying `Send + 'static`, then submit the request with a send method. The lifetime bound permits owned data and excludes temporary borrowed captures; it does not require the request value to live forever.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Owned per-request headers; empty by default.

### Errors and behavior

Request headers override common client headers. Invalid or transport-controlled headers fail at dispatch. Return stable values when the request is replayable.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) implements and directly calls this method; the submission programs above show SDK dispatch.

## `HttpRequestTrait::retry_policy()`

### Required features

`http-client`; use `#[open_net::async_trait]` on the trait implementation.

### Function signature

```text
fn retry_policy(&self) -> RetryPolicy
```

### Steps

Implement this provider on a request type satisfying `Send + 'static`, then submit the request with a send method. The lifetime bound permits owned data and excludes temporary borrowed captures; it does not require the request value to live forever.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

The request policy; no_retry by default.

### Errors and behavior

send/send_wait consult this provider on the caller before admission. A positive retry count overrides the client; a no-retry value inherits the client in those shorthand methods. Explicit options are authoritative and can disable retries. Provider panics unwind on the caller; ownership recovery applies to returned errors, not panics.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) implements and directly calls this method; the submission programs above show SDK dispatch.

## `HttpRequestTrait::on_response_headers()`

### Required features

`http-client`.

### Function signature

```text
fn on_response_headers(&self, headers: &HeaderMap)
```

### Steps

Optionally override this synchronous hook to inspect final headers immediately before the consuming callback.

### Parameters

`&self` borrows the request and `headers: &HeaderMap` borrows final response headers.

### Callback parameters

`headers: &HeaderMap` contains the final response headers, borrowed only for this call. The callback returns synchronously; retain a clone if needed later.

### Return values

Unit; the default implementation is a no-op.

### Errors and behavior

Runs on the callback lane only after the complete final buffered response succeeds. There are no intermediate redirect/retry/authentication-response notifications. Body read errors, limits and timeouts skip the hook. It cannot reject headers before download or replace the result; use stream() for pre-body inspection. A panic is reflected in handle completion.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) implements and directly calls this method; the submission programs above show SDK dispatch.

## `HttpRequestTrait::deal_with_response()`

### Required features

`http-client`; use `#[open_net::async_trait]` on the implementation, which must satisfy `Send + 'static`.

### Function signature

```text
async fn deal_with_response(self: Box<Self>, result: HttpResponseResult)
```

### Steps

Implement the owned terminal callback, handle both response and transport failure, and let it finish to release operation capacity.

### Parameters

`self: Box<Self>` consumes the request and its application state; `result: HttpResponseResult` is required and moved into the callback.

### Callback parameters

`result` is `Result<HttpResponse, NetError>`: Ok owns a fully buffered final response; Err owns the failure. Application state is recovered through owned self.

### Return values

Unit after the callback future completes; application success is not inferred from the return type.

### Errors and behavior

Accepted submissions receive one terminal callback under normal SDK dispatch, including failure/cancellation. Rejected submissions receive none. HTTP 4xx/5xx remain Ok responses unless checked. The callback runs on the SDK callback lane, must not wait for its own completion or client cleanup, and is not interrupted by network timeout budgets. Completion reports callback panic or abnormal execution.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) implements and directly calls this method; the submission programs above show SDK dispatch.

## `HttpRequestMethod::as_str()`

### Required features

`http-client`.

### Function signature

```text
pub fn as_str(&self) -> &str
```

### Steps

Inspect the textual method while retaining the enum.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

A borrowed method string.

### Errors and behavior

Known variants return their method text; Extension returns its stored text without validation. Converting an invalid Extension to Method panics, so validate through Method::from_bytes first.

### Example

[HttpRequestMethod example](#httprequestmethod) validates CUSTOM and calls as_str in a complete offline program.

## `EnqueueError::error()`

### Required features

`http-client`.

### Function signature

```text
pub fn error(&self) -> &NetError
```

### Steps

Handle the Err branch of send/send_wait and choose whether to inspect or recover the rejected request.

### Parameters

None beyond `&self` or consumed `self`, as shown in the signature. R has no extra Clone, Sync or Debug requirement.

### Callback parameters

Not applicable.

### Return values

A borrowed admission error.

### Errors and behavior

Keeps the original rejected request owned by EnqueueError. No callback is scheduled by inspection.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) exercises all five accessors against explicitly closed admission.

## `EnqueueError::value()`

### Required features

`http-client`.

### Function signature

```text
pub fn value(&self) -> &R
```

### Steps

Handle the Err branch of send/send_wait and choose whether to inspect or recover the rejected request.

### Parameters

None beyond `&self` or consumed `self`, as shown in the signature. R has no extra Clone, Sync or Debug requirement.

### Callback parameters

Not applicable.

### Return values

A borrowed original rejected request.

### Errors and behavior

Neither transfers nor clones the request. Use an owning accessor when retrying submission.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) exercises all five accessors against explicitly closed admission.

## `EnqueueError::into_parts()`

### Required features

`http-client`.

### Function signature

```text
pub fn into_parts(self) -> (R, NetError)
```

### Steps

Handle the Err branch of send/send_wait and choose whether to inspect or recover the rejected request.

### Parameters

None beyond `&self` or consumed `self`, as shown in the signature. R has no extra Clone, Sync or Debug requirement.

### Callback parameters

Not applicable.

### Return values

The original request and admission error as owned values.

### Errors and behavior

Consumes the wrapper. The application can retry, retain, or manually handle the request; no SDK callback has been scheduled.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) exercises all five accessors against explicitly closed admission.

## `EnqueueError::into_inner()`

### Required features

`http-client`.

### Function signature

```text
pub fn into_inner(self) -> R
```

### Steps

Handle the Err branch of send/send_wait and choose whether to inspect or recover the rejected request.

### Parameters

None beyond `&self` or consumed `self`, as shown in the signature. R has no extra Clone, Sync or Debug requirement.

### Callback parameters

Not applicable.

### Return values

The original owned request.

### Errors and behavior

Consumes the wrapper and discards its error. This performs no retry automatically.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) exercises all five accessors against explicitly closed admission.

## `EnqueueError::into_error()`

### Required features

`http-client`.

### Function signature

```text
pub fn into_error(self) -> NetError
```

### Steps

Handle the Err branch of send/send_wait and choose whether to inspect or recover the rejected request.

### Parameters

None beyond `&self` or consumed `self`, as shown in the signature. R has no extra Clone, Sync or Debug requirement.

### Callback parameters

Not applicable.

### Return values

The owned admission error.

### Errors and behavior

Consumes the wrapper and drops its original request, including owned callback/application state. No callback is invoked.

### Example

[Offline trait and rejection example](#offline-trait-and-rejection-example) exercises all five accessors against explicitly closed admission.

## `HttpRequestHandle::id()`

### Required features

`http-client`.

### Function signature

```text
pub fn id(&self) -> HttpRequestId
```

### Steps

Retain the accepted operation handle and read its correlation ID.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

The operation ID by value.

### Errors and behavior

The handle remains bound to its original operation even if that numeric ID is later reused. This accessor does not schedule cancellation or retain request capacity.

### Example

[Handle example](#httprequestid-httprequesthandle-and-completion) provides the complete cancel_and_wait helper calling this method; the send programs supply accepted handles.

## `HttpRequestHandle::cancel()`

### Required features

`http-client`.

### Function signature

```text
pub fn cancel(&self) -> HttpCancelOutcome
```

### Steps

Request local cancellation and await finished separately if cleanup must be observed.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Requested on the first cancellation request, AlreadyRequested if previously requested, or TooLate once callback dispatch begins or the operation finishes.

### Errors and behavior

Cancellation cannot roll back server-side processing or interrupt an executing callback. An already determined timeout remains a timeout. Dropping the handle alone does not request cancellation.

### Example

[Handle example](#httprequestid-httprequesthandle-and-completion) provides the complete cancel_and_wait helper calling this method; the send programs supply accepted handles.

## `HttpRequestHandle::finished()`

### Required features

`http-client`.

### Function signature

```text
pub async fn finished(&self) -> HttpCompletion
```

### Steps

Await completion outside the operation callback, then inspect response and callback fields separately.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

A cached HttpCompletion containing request ID, transport summary/error and callback execution result.

### Errors and behavior

Waits for the header hook, consuming handler and SDK operation-resource cleanup. Repeated waits and cloned handles observe the same result. Dropping this waiting future does not cancel the operation. Detached callback tasks are outside the boundary; a callback that never returns can prevent completion. A callback must not await its own handle.

### Example

[Handle example](#httprequestid-httprequesthandle-and-completion) provides the complete cancel_and_wait helper calling this method; the send programs supply accepted handles.

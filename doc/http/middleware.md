# HTTP middleware

Required feature: `http-client-middleware` (includes `http-client`). Middleware wraps concrete `request` and `stream` operations, including convenience methods such as `get`, `post` and `sse`. The `HttpRequestTrait` callback submission methods `send*` do not run this chain.

```toml
[dependencies]
open-net = { path = "../open-net", default-features = false, features = ["http-client-middleware"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
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

Adjust the local path to your checkout. All middleware types are exported from `open_net::api::http`. Use `open_net::async_trait` for trait implementations. `.await` waits without blocking an asynchronous worker; a middleware must avoid blocking work because the request's cancellation and total timeout depend on the future making progress.

## Register and execute a middleware chain

### Required features

`http-client-middleware`.

### Function signature

```text
HttpClientConfig::with_middleware<M>(self, middleware: M) -> Self
    where M: HttpMiddleware + 'static
HttpClientConfig::with_middleware_arc(self, middleware: Arc<dyn HttpMiddleware>) -> Self
#[async_trait]
pub trait HttpMiddleware: Send + Sync + 'static {
    async fn call(&self, call: HttpCall, next: Next) -> Result<HttpMiddlewareResponse, NetError>;
}
pub async fn Next::run(self, call: HttpCall) -> Result<HttpMiddlewareResponse, NetError>
```


### Steps

1. Implement `HttpMiddleware::call`.
2. Inspect or edit the call before continuation.
3. Consume `next` with `next.run(call).await` to invoke the remaining chain.
4. Inspect or transform the result after continuation.
5. Register layers before client creation, and close the client after use.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `middleware` | `M: HttpMiddleware + 'static` | For value installation | Owned thread-safe layer; each call appends a layer. |
| `middleware` | `Arc<dyn HttpMiddleware>` | For shared installation | Explicitly shared layer; also appends. |
| `self` in trait | `&Self` | SDK supplies | Layer may run concurrently; synchronize mutable shared state. |

### Callback parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `call` | `HttpCall` | Owned admitted request, with fixed request ID and response mode. |
| `next` | `Next` | Owned one-shot continuation; no public constructor or Clone. |

### Return values

Configuration setters return `HttpClientConfig`. `call` and `run` return `Ok(HttpMiddlewareResponse)` or `Err(NetError)`. With layers A then B, execution is A-before, B-before, transport, B-after, A-after. Each layer runs once per logical operation; transport retries occur inside the continuation, not by re-entering the middleware chain.

### Errors and behavior

Admission occurs before middleware. The quota and operation remain live through post-response middleware work. The Total timeout covers pre/post middleware, admission and transport; resolved admission/time budgets are fixed before middleware runs. Attempt and phase budgets apply to the transport attempts. Returning `Err` aborts the remaining chain; unwinding panics become `CallbackPanicked/Dispatch`. Shutdown or cancellation can terminate a suspended chain.

Avoid awaiting a nested request that requires a quota permit held by this call, or waiting for this same client's `drain`/shutdown from inside middleware. Cloning a client shares its quota. Use an independent client for nested network dependencies, or handle `FailFast` capacity rejection. `Next` ownership permits one continuation call; middleware is not a mechanism to replay it repeatedly.

### Example

The complete program registers a pass-through layer and an offline buffered responder. The pass-through layer invokes `Next::run`; the second layer returns a response without connecting to the example origin. Running it prints `cached response` and closes the client.

```rust
use open_net::api::http::{
    HttpCall, HttpClient, HttpClientConfig, HttpMiddleware, HttpMiddlewareResponse,
    HttpResponse, HttpResponseMode, Next,
};
use open_net::{async_trait, Bytes, HeaderMap, HeaderValue, NetError, StatusCode};
use std::sync::Arc;

struct Trace;
#[async_trait]
impl HttpMiddleware for Trace {
    async fn call(&self, mut call: HttpCall, next: Next)
        -> Result<HttpMiddlewareResponse, NetError> {
        let metadata = call.metadata();
        assert_eq!(metadata.request_id, call.request_id());
        let _facts = (metadata.method, metadata.path, metadata.mode, metadata.body_bytes);
        let _method = call.request().method.clone();
        call.request_mut().headers.insert("x-demo", HeaderValue::from_static("middleware"));
        let expected_mode = call.mode();
        let response = next.run(call).await?;
        assert_eq!(response.mode(), expected_mode);
        Ok(response)
    }
}
struct OfflineResponse;
#[async_trait]
impl HttpMiddleware for OfflineResponse {
    async fn call(&self, call: HttpCall, next: Next)
        -> Result<HttpMiddlewareResponse, NetError> {
        if call.mode() == HttpResponseMode::Streaming {
            return next.run(call).await;
        }
        Ok(HttpMiddlewareResponse::Buffered(HttpResponse::synthetic(
            call.request_id(), StatusCode::OK, HeaderMap::new(),
            Bytes::from_static(b"cached response"),
        )))
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let config = HttpClientConfig::new("https://example.com/")?
        .with_middleware(Trace)
        .with_middleware_arc(Arc::new(OfflineResponse));
    let client = HttpClient::new(config, "middleware-example")?;
    let result = client.get("/cached").await;
    client.shutdown().await?;
    println!("{}", result?.text()?);
    Ok(())
}
```

## `HttpCall` accessors and diagnostic metadata

### Required features

`http-client-middleware`.

### Function signature

```text
HttpCall::request(&self) -> &HttpRequest
HttpCall::request_mut(&mut self) -> &mut HttpRequest
HttpCall::metadata(&self) -> HttpCallMetadata
HttpCall::request_id(&self) -> HttpRequestId
HttpCall::mode(&self) -> HttpResponseMode
HttpMiddlewareResponse::mode(&self) -> HttpResponseMode
```

### Steps

Use accessors inside `call` before moving the call into `next.run`. Prefer `metadata` for diagnostics. Use `request_mut` for intentional request changes before transport validation.

### Parameters

No arguments beyond the receiver. `request_mut` needs a mutable binding. References from getters cannot outlive the call or remain borrowed while moving it into the continuation.

### Callback parameters

The `HttpCall` is supplied by `HttpMiddleware::call`; applications do not construct one directly.

### Return values

| Method / public metadata field | Type | Meaning |
| --- | --- | --- |
| `request` / `request_mut` | Reference to `HttpRequest` | Full request, including headers, query and body; treat as potentially sensitive. |
| `metadata` | `HttpCallMetadata` | Fresh owned diagnostic copy. |
| `request_id` | `HttpRequestId` | Identifier assigned at admission; also a metadata field. |
| `mode` | `HttpResponseMode` | `Buffered` or `Streaming`; also a metadata field. |
| Metadata `method` | `Method` | Current request method. |
| Metadata `path` | `String` | Current path with query and fragment removed. |
| Metadata `body_bytes` | `usize` | Current buffered request body length. |

### Errors and behavior

Accessors return directly, not `Result`. Metadata omits headers and body content, but applications should still consider whether their path segments contain identifiers. Changes to a request do not change the fixed mode or assigned request ID. Mutated request fields still face downstream validation; middleware is not a bypass for forbidden transport headers or cross-origin initial paths.

### Example

The `Trace` implementation in the complete example above demonstrates every accessor, every metadata field, request mutation, and `HttpMiddlewareResponse::mode` with declared imports and context.

## Buffered short-circuit responses and streaming restrictions

### Required features

`http-client-middleware`.

### Function signature

```text
HttpResponse::synthetic(request_id: HttpRequestId, status: StatusCode,
    headers: HeaderMap, body: Bytes) -> Self
pub enum HttpMiddlewareResponse {
    Buffered(HttpResponse),
    Streaming(HttpStreamResponse),
}
pub enum HttpResponseMode { Buffered, Streaming }
```

### Steps

For a buffered call, create a synthetic response using the incoming `call.request_id` and return `Buffered`. For a stream, continue to transport with `next.run(call).await` and return the streaming response. If you cannot serve a streaming operation, return an appropriate error.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `request_id` | `HttpRequestId` | Yes | Preserve the ID from this call. |
| `status` | `StatusCode` | Yes | Synthetic HTTP status selected by your layer. |
| `headers` | `HeaderMap` | Yes | Owned response headers. |
| `body` | `Bytes` | Yes | Owned buffered body; your middleware is responsible for how it constructs this value. |

### Callback parameters

No additional callbacks. The supplied `call.mode()` selects the expected result variant.

### Return values

`synthetic` returns `HttpResponse` directly, with zero transport attempts. Wrap it in `HttpMiddlewareResponse::Buffered`. With HTTP/2 metadata additionally enabled, synthetic responses have `HttpProtocol::Unknown` and no flow-control snapshot.

### Errors and behavior

A buffered response with a different request ID fails with `Protocol/Response`. Wrong response mode fails with `RoutingModeMismatch/Dispatch`. Streaming middleware must reach its transport continuation; a buffered cache hit cannot satisfy a streaming call. Returning a stream does not mean its body is complete: retain normal EOF/error/drop and timeout cleanup from [streaming and SSE](streaming_and_sse.md). After `next.run` returns a stream, middleware observes response headers, not a fully consumed body.

### Example

`OfflineResponse` in the complete program above preserves the incoming ID, short-circuits only `Buffered` calls, and forwards `Streaming` calls. Its offline main exercises the buffered case. The next complete program exercises both modes through the same two layers; start a server at `http://127.0.0.1:8080` whose `/bytes` route returns a nonempty finite body. The buffered call is answered locally, while the stream reaches that server. The pass-through layer checks the incoming mode rather than assuming every response is buffered.

```rust
use open_net::api::http::{
    HttpCall, HttpClient, HttpClientConfig, HttpMiddleware, HttpMiddlewareResponse,
    HttpRequest, HttpResponse, HttpResponseMode, HttpStatusPolicy, Next,
};
use open_net::{async_trait, Bytes, HeaderMap, NetError, StatusCode};

struct Trace;
#[async_trait]
impl HttpMiddleware for Trace {
    async fn call(&self, call: HttpCall, next: Next)
        -> Result<HttpMiddlewareResponse, NetError> {
        let expected_mode = call.mode();
        let response = next.run(call).await?;
        assert_eq!(response.mode(), expected_mode);
        Ok(response)
    }
}
struct OfflineResponse;
#[async_trait]
impl HttpMiddleware for OfflineResponse {
    async fn call(&self, call: HttpCall, next: Next)
        -> Result<HttpMiddlewareResponse, NetError> {
        if call.mode() == HttpResponseMode::Streaming {
            return next.run(call).await;
        }
        Ok(HttpMiddlewareResponse::Buffered(HttpResponse::synthetic(
            call.request_id(), StatusCode::OK, HeaderMap::new(),
            Bytes::from_static(b"cached response"),
        )))
    }
}
#[tokio::main]
async fn main() -> Result<(), NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let config = HttpClientConfig::new("http://127.0.0.1:8080")?
        .with_middleware(Trace).with_middleware(OfflineResponse);
    let client = HttpClient::new(config, "middleware-stream-example")?;
    let result: Result<(), NetError> = async {
        assert_eq!(client.get("/cached").await?.body, Bytes::from_static(b"cached response"));
        let request = HttpRequest::builder().path("/bytes").build()?;
        let mut response = client.stream(request).await?;
        response.check_status(HttpStatusPolicy::Success2xx)?;
        let mut received = 0;
        while let Some(chunk) = response.next_chunk().await {
            received += chunk?.len();
        }
        assert!(received > 0);
        println!("middleware streaming response: {received} bytes");
        Ok(())
    }.await;
    client.shutdown().await?;
    result
}
```


## `HttpClientConfig::with_middleware()`

### Required features

`http-client-middleware`.

### Function signature

```text
pub fn with_middleware<M>(self, middleware: M) -> Self
where M: HttpMiddleware + 'static
```

### Steps

Implement the layer, then append its owned value before constructing the client.

### Parameters

`middleware: M` is an owned, thread-safe middleware implementation; `self` is the consumed configuration.

### Callback parameters

The SDK later supplies `HttpCall` and `Next` to the layer's `call` hook.

### Return values

The updated configuration.

### Errors and behavior

Appends rather than replaces. Registration order determines the outside-to-inside call order. Callback-style `send*` operations do not enter these layers.

### Example

See the complete [chain registration example](#register-and-execute-a-middleware-chain), including both middleware implementations and the client lifecycle.


## `HttpClientConfig::with_middleware_arc()`

### Required features

`http-client-middleware`.

### Function signature

```text
pub fn with_middleware_arc(self, middleware: Arc<dyn HttpMiddleware>) -> Self
```

### Steps

Wrap the layer in `Arc` when sharing is intentional, then append it before client construction.

### Parameters

`middleware` is the shared layer; `self` is the consumed configuration.

### Callback parameters

The SDK later supplies `HttpCall` and `Next` to `call`.

### Return values

The updated configuration.

### Errors and behavior

Appends a layer. Sharing a layer across clients does not make its mutable state automatically safe; the implementation must satisfy `Send + Sync`.

### Example

See the complete [chain registration example](#register-and-execute-a-middleware-chain), including both middleware implementations and the client lifecycle.


## `HttpMiddleware::call()`

### Required features

`http-client-middleware`.

### Function signature

```text
#[async_trait]
async fn call(&self, call: HttpCall, next: Next)
    -> Result<HttpMiddlewareResponse, NetError>
```

### Steps

Implement this hook. Inspect the admitted call, then continue once or return an allowed short-circuit response.

### Parameters

`&self` borrows the shared layer. The SDK provides the remaining arguments; applications normally invoke a client method instead of calling this hook directly.

### Callback parameters

`call: HttpCall` owns the admitted request and fixed ID/mode. `next: Next` owns the remaining continuation.

### Return values

`Ok(HttpMiddlewareResponse)` on success; `Err(NetError)` fails the logical request.

### Errors and behavior

The Total budget covers this hook, including post-continuation work. Unwinding panics become `CallbackPanicked/Dispatch`. Returning a wrong response mode fails validation. Do not block or await a nested operation requiring the permit held by this call.

### Example

See the complete [buffered and streaming example](#buffered-short-circuit-responses-and-streaming-restrictions); it checks the mode in the pass-through layer and drives the stream to EOF.


## `Next::run()`

### Required features

`http-client-middleware`.

### Function signature

```text
pub async fn run(self, call: HttpCall) -> Result<HttpMiddlewareResponse, NetError>
```

### Steps

Finish inspecting or mutating the call, then move it into `next.run(call).await`.

### Parameters

`self` consumes the one-shot continuation; `call` consumes the admitted request.

### Callback parameters

Invokes the next installed middleware, or the terminal transport when no layer remains.

### Return values

The downstream response or downstream `NetError`.

### Errors and behavior

No public constructor or `Clone` is provided. Ownership permits one continuation call. Transport retries occur inside this continuation, without re-entering the outer chain.

### Example

See the complete [buffered and streaming example](#buffered-short-circuit-responses-and-streaming-restrictions); it checks the mode in the pass-through layer and drives the stream to EOF.


## `HttpCall::request()`

### Required features

`http-client-middleware`.

### Function signature

```text
pub fn request(&self) -> &HttpRequest
```

### Steps

Borrow the concrete request before moving the call into the continuation.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpMiddleware::call` supplies the receiver.

### Return values

A shared reference to the full request.

### Errors and behavior

Contains headers and body content, which may be sensitive. End the borrow before moving the call. No `Result` or validation occurs.

### Example

See the complete [chain registration example](#register-and-execute-a-middleware-chain), including both middleware implementations and the client lifecycle.


## `HttpCall::request_mut()`

### Required features

`http-client-middleware`.

### Function signature

```text
pub fn request_mut(&mut self) -> &mut HttpRequest
```

### Steps

Bind the call as mutable, edit the request, then continue with the same call.

### Parameters

Only `&mut self`.

### Callback parameters

The enclosing middleware hook supplies the receiver.

### Return values

An exclusive reference to the concrete request.

### Errors and behavior

Mutations are still validated by downstream transport. They cannot change the call's fixed response mode or assigned request ID.

### Example

See the complete [chain registration example](#register-and-execute-a-middleware-chain), including both middleware implementations and the client lifecycle.


## `HttpCall::metadata()`

### Required features

`http-client-middleware`.

### Function signature

```text
pub fn metadata(&self) -> HttpCallMetadata
```

### Steps

Take an owned diagnostic snapshot before continuing or after mutating the request.

### Parameters

Only `&self`.

### Callback parameters

The enclosing middleware hook supplies the receiver.

### Return values

Owned `HttpCallMetadata`: request ID, current method/path, response mode and buffered body length.

### Errors and behavior

Removes query, fragment, headers and body contents. Path segments may still contain identifiers. A snapshot does not update after later mutations.

### Example

See the complete [chain registration example](#register-and-execute-a-middleware-chain), including both middleware implementations and the client lifecycle.


## `HttpCall::request_id()`

### Required features

`http-client-middleware`.

### Function signature

```text
pub fn request_id(&self) -> HttpRequestId
```

### Steps

Read the admission-assigned identifier, especially when constructing a synthetic response.

### Parameters

Only `&self`.

### Callback parameters

The enclosing middleware hook supplies the receiver.

### Return values

The copied request identifier.

### Errors and behavior

Does not allocate a new ID. A synthetic response must preserve this ID; mismatches fail with `Protocol/Response`.

### Example

See the complete [chain registration example](#register-and-execute-a-middleware-chain), including both middleware implementations and the client lifecycle.


## `HttpCall::mode()`

### Required features

`http-client-middleware`.

### Function signature

```text
pub fn mode(&self) -> HttpResponseMode
```

### Steps

Capture the expected mode before consuming the call, then choose a compatible response or continuation.

### Parameters

Only `&self`.

### Callback parameters

The enclosing middleware hook supplies the receiver.

### Return values

`Buffered` for buffered operations or `Streaming` for stream/SSE operations.

### Errors and behavior

The mode is fixed at admission and does not change when request fields are mutated. Do not assume it is always `Buffered`.

### Example

See the complete [buffered and streaming example](#buffered-short-circuit-responses-and-streaming-restrictions); it checks the mode in the pass-through layer and drives the stream to EOF.


## `HttpMiddlewareResponse::mode()`

### Required features

`http-client-middleware`.

### Function signature

```text
pub fn mode(&self) -> HttpResponseMode
```

### Steps

After continuation, compare the returned mode with the mode saved from the incoming call.

### Parameters

Only `&self`.

### Callback parameters

None.

### Return values

`Buffered` or `Streaming`, matching the enum variant.

### Errors and behavior

Does not consume the response or read a stream body. A stream still needs normal EOF/error/drop cleanup.

### Example

See the complete [buffered and streaming example](#buffered-short-circuit-responses-and-streaming-restrictions); it checks the mode in the pass-through layer and drives the stream to EOF.


## `HttpResponse::synthetic()`

### Required features

`http-client-middleware`.

### Function signature

```text
pub fn synthetic(request_id: HttpRequestId, status: StatusCode,
    headers: HeaderMap, body: Bytes) -> Self
```

### Steps

For a buffered call, preserve its request ID and wrap the constructed value in `HttpMiddlewareResponse::Buffered`.

### Parameters

`request_id` identifies this admitted call; `status`, `headers` and `body` are the owned synthetic response components.

### Callback parameters

None.

### Return values

A buffered `HttpResponse` with zero transport attempts.

### Errors and behavior

Construction returns directly. Middleware validates the ID and expected mode after return. With HTTP/2 metadata enabled, the protocol is `Unknown` and no flow-control snapshot exists. This value cannot satisfy a streaming call.

### Example

See the complete [buffered and streaming example](#buffered-short-circuit-responses-and-streaming-restrictions); it checks the mode in the pass-through layer and drives the stream to EOF.

[HTTP guide index](README.md)

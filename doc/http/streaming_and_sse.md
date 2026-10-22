# Streaming responses and raw SSE data

[HTTP guide](README.md) · [Library overview](../../README.md)

## Required features and example dependencies

Use Rust 1.89 or later and the `http-client` feature (it also enables `net-status`).
Create a consumer with `cargo new http-example` and add these dependencies.

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false, features = ["http-client"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
futures-util = "0.3"
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

Each Rust block is an independent `src/main.rs`; `cargo check` compiles it.
Sending examples need a server at `http://127.0.0.1:8080` with the stated route.
Use `cargo run` only when that prerequisite is satisfied. Compilation is not a
network test. Construction and cleanup examples that send nothing need no server.

## Stream lifetime and deadlines

SDK transport streams reserve request capacity until EOF, error, drop or SDK
termination. After a stream is returned it is never replayed, even if no body
chunk has been polled. A total deadline includes admission; attempt deadlines
cover headers and the body. Idle-read time includes application processing and
pauses between polls, and resets only when a nonempty chunk is delivered. The
SDK does not prefetch to extend this budget. See [timeouts](timeouts_and_admission.md)
for overriding or disabling budgets for long-lived streams.

Cancellation, shutdown and expired deadlines release network resources and
capacity even if you retain a stream without polling. Its next poll produces
one terminal error followed by permanent EOF. This supervision needs the SDK
runtime to stay alive and receive scheduling time, and network I/O needs the
opening runtime to stay alive. With every timeout disabled, a retained stream
can keep `drain()` waiting until consumption, drop or cancellation ends it.
Open the stream inside an active Tokio runtime with I/O and time drivers enabled.
An already opened SDK stream can be polled on another executor without entering
Tokio on that consuming thread, provided the opening runtime remains alive and
drives I/O. This does not make stream opening or client shutdown independent of
Tokio. A current-thread runtime must keep being driven; retaining its value alone
does not run its I/O driver. See the [runtime requirements](README.md#start-here).
Custom streams passed to HttpStreamResponse::new retain their own lifecycle rules.

`max_response_bytes` limits buffered responses only. Streaming and SSE consumers
that need a size bound must count delivered bytes and drop or cancel the stream
once their application limit is reached.

Dropping the future waiting for `stream()` requests cancellation. After it
returns, dropping the response (or its extracted stream) releases the SDK stream.
Dropping only an in-progress `next_chunk()` wait does not drop a separately
retained response. Keep or cancel the response deliberately.

## `HttpClient::stream()`

Return headers and an incremental body without buffering the whole response.

### Required features

`http-client`.

### Function signature

```text
pub async fn stream(&self, request: HttpRequest) -> Result<HttpStreamResponse, NetError>
```

### Steps

Start a server with /bytes returning a finite byte body. Open the response, check status, consume chunks, then shut down.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| request | HttpRequest | Yes | Owned request with method, headers, body and optional overrides. |

### Callback parameters

Not applicable.

### Return values

`Ok(HttpStreamResponse)` exposes metadata and the body stream. `Err(NetError)` reports failure before returning the stream. Body failures arrive later from next_chunk().

### Errors and behavior

The normal admission, path, header, cancellation and timeout rules apply. HTTP error statuses remain successful response values until check_status(). Built-in status/transport retries may occur before the stream is returned, subject to policy; never after return. The buffered business classifier is not applied to streaming responses, and the SDK does not buffer a stream to run it.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig, HttpStatusPolicy, HttpRequest};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "stream-example",
    )?;
    let result: Result<(), open_net::NetError> = async {
        let request = HttpRequest::builder().path("/bytes").build()?;
        let mut response = client.stream(request).await?;
        response.check_status(HttpStatusPolicy::Success2xx)?;
        while let Some(chunk) = response.next_chunk().await {
            let bytes = chunk?;
            println!("Received {} bytes", bytes.len());
        }
        // Dropping the response also handles an early exit.
        Ok(())
    }.await;
    client.shutdown().await?;
    result
}
```

## `HttpClient::sse()`

Open a GET stream intended for server-sent events.

### Required features

`http-client`.

### Function signature

```text
pub async fn sse(
    &self, path: impl Into<String>, headers: HeaderMap,
) -> Result<HttpStreamResponse, NetError>
```

### Steps

Start a server with /events returning text/event-stream. Open the response, check status, consume chunks, then shut down.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| path | impl Into<String> | Yes | Safe relative SSE route. |
| headers | HeaderMap | Yes | Explicit request headers; use Accept: text/event-stream if needed. |

### Callback parameters

Not applicable.

### Return values

`Ok(HttpStreamResponse)` exposes metadata and the body stream. `Err(NetError)` reports failure before returning the stream. Body failures arrive later from next_chunk().

### Errors and behavior

The normal admission, path, header, cancellation and timeout rules apply. HTTP error statuses remain successful response values until check_status(). This helper does not add an SSE event parser, reconnect, Last-Event-ID management or automatic Accept header. It simply builds a GET and calls stream().

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig, HttpStatusPolicy, HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "stream-example",
    )?;
    let result: Result<(), open_net::NetError> = async {
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("text/event-stream"));
        let mut response = client.sse("/events", headers).await?;
        response.check_status(HttpStatusPolicy::Success2xx)?;
        while let Some(chunk) = response.next_chunk().await {
            let bytes = chunk?;
            println!("Received {} bytes", bytes.len());
            // Stop this demonstration after its first chunk.
            break;
        }
        // Dropping the response also handles an early exit.
        Ok(())
    }.await;
    client.shutdown().await?;
    result
}
```

## SSE framing belongs to the application

A chunk can contain part of a UTF-8 code point, part of an SSE line, several events,
or no complete event. Never treat each chunk as one event or call `from_utf8`
independently on every chunk assuming success. Preserve undecoded bytes and parser
state across chunks, recognize the SSE line/event rules, and decide reconnection
and Last-Event-ID behavior in your application or an SSE parser. The example
above only counts bytes and deliberately drops the response after one chunk.
There is no separate `sse` feature.

## `HttpByteStream` and `HttpStreamResponse`

### Required features

`http-client`. No callback is installed by these types. The optional HTTP/2
metadata accessors are documented in [HTTP/2](http2.md).

### Fields and function signatures

```text
pub type HttpByteStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, NetError>> + Send + 'static>>;

pub fn HttpStreamResponse::new(
    status: StatusCode, headers: HeaderMap, request_id: HttpRequestId,
    attempts: u32, stream: HttpByteStream,
) -> Self

pub fn content_type(&self) -> Option<&str>
pub async fn next_chunk(&mut self) -> Option<Result<Bytes, NetError>>
pub fn check_status(&self, policy: HttpStatusPolicy) -> Result<(), NetError>
```

The stream is pinned, owned, sendable and independent of borrowed stack data.
Its item type is owned Bytes or NetError. The response is not a buffered
HttpResponse and has no text()/json() helper.

| Public field / constructor parameter | Type | Meaning |
| --- | --- | --- |
| `status` | `StatusCode` | Peer's status. |
| `headers` | `HeaderMap` | Owned response headers. |
| `request_id` | `HttpRequestId` | Local operation identifier for correlation/cancellation. |
| `attempts` | `u32` | Attempts used before the stream opened. |
| `stream` | `HttpByteStream` | Incremental body, optionally consumed directly with StreamExt. |

### Steps and return values

`new` requires all five values and performs no network I/O; use it for custom
streams or tests. `content_type` returns None for absent/invalid header text.
`next_chunk` borrows the response mutably: `Some(Ok(bytes))` is a chunk,
`Some(Err(error))` is terminal for an SDK transport stream, and `None` means EOF.
After a delivered SDK error it permanently returns None. `check_status` borrows
the response, returns unit for an accepted status, and otherwise returns a
NetError with kind HttpStatus and the rejected status in `http_status()`.
It leaves the stream owned by the caller.

### Errors and example

This offline custom stream demonstrates the constructor and EOF. Unlike SDK
transport streams, lifecycle and error fusion for arbitrary custom streams are
controlled by their producer. Dropping one cannot cancel an unrelated SDK request.

```rust
use open_net::api::http::{HttpByteStream, HttpRequestId, HttpStreamResponse, HttpStatusPolicy};
use open_net::{Bytes, HeaderMap, StatusCode};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let stream: HttpByteStream = Box::pin(futures_util::stream::iter(vec![
        Ok(Bytes::from_static(b"first")), Ok(Bytes::from_static(b"second")),
    ]));
    let mut response = HttpStreamResponse::new(
        StatusCode::OK, HeaderMap::new(), HttpRequestId(1), 0, stream,
    );
    response.check_status(HttpStatusPolicy::Success2xx)?;
    assert_eq!(response.content_type(), None);
    while let Some(chunk) = response.next_chunk().await {
        println!("{} bytes", chunk?.len());
    }
    Ok(())
}
```

## `HttpStreamResponse::new()`

### Required features

`http-client`.

### Function signature

```text
pub fn new(status: StatusCode, headers: HeaderMap, request_id: HttpRequestId, attempts: u32, stream: HttpByteStream) -> Self
```

### Steps

Create a pinned owned stream of byte results and move it into a response.

### Parameters

All five arguments are required: status, owned headers, local request ID, attempt count and owned HttpByteStream.

### Callback parameters

Not applicable.

### Return values

An owned streaming response, with optional transport metadata at defaults.

### Errors and behavior

Construction performs no I/O or SDK request registration. Arbitrary custom streams retain their producer-defined EOF/error/cancellation semantics. This constructor does not turn them into supervised SDK transport streams.

### Example

[Custom stream example](#httpbytestream-and-httpstreamresponse) calls this method in a complete offline program.

## `HttpStreamResponse::content_type()`

### Required features

`http-client`.

### Function signature

```text
pub fn content_type(&self) -> Option<&str>
```

### Steps

Inspect the response headers after opening and before consuming any body if desired.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Some borrowed Content-Type text, or None if absent or invalid as text.

### Errors and behavior

No body bytes are consumed and no media-type parsing or validation is performed.

### Example

[Custom stream example](#httpbytestream-and-httpstreamresponse) calls this method in a complete offline program.

## `HttpStreamResponse::next_chunk()`

### Required features

`http-client`.

### Function signature

```text
pub async fn next_chunk(&mut self) -> Option<Result<Bytes, NetError>>
```

### Steps

Repeatedly await a chunk; stop on EOF and propagate or handle an error.

### Parameters

None beyond exclusive `&mut self`; simultaneous reads through that response are not permitted.

### Callback parameters

Not applicable.

### Return values

Some(Ok(bytes)) is an owned chunk, Some(Err(error)) is a stream failure, and None is EOF.

### Errors and behavior

SDK transport streams emit at most one terminal error followed by permanent EOF. Chunk boundaries are not message, SSE event or UTF-8 boundaries. Paused consumption still spends timeout budgets. Already opened SDK streams may be consumed on another executor while the opening runtime drives I/O; custom streams follow their producer requirements.

### Example

[Custom stream example](#httpbytestream-and-httpstreamresponse) calls this method in a complete offline program.

## `HttpStreamResponse::check_status()`

### Required features

`http-client`.

### Function signature

```text
pub fn check_status(&self, policy: HttpStatusPolicy) -> Result<(), NetError>
```

### Steps

Select a status policy before reading; drop or cancel the stream if rejecting it.

### Parameters

`policy: HttpStatusPolicy` is required; `&self` borrows the response.

### Callback parameters

Not applicable.

### Return values

Ok(()) accepts the status; Err(NetError) has HttpStatus kind and the rejected http_status().

### Errors and behavior

The stream is not consumed or cancelled by this check. A rejected stream still occupies capacity until it reaches a terminal state or is dropped/cancelled.

### Example

[Custom stream example](#httpbytestream-and-httpstreamresponse) calls this method in a complete offline program.

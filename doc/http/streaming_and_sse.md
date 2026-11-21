# Streaming responses and SSE events

[HTTP guide](README.md) · [Library overview](../../README.md)

## Required features and example dependencies

Enable `http-client`, which also enables `net-status`. Create a consumer with
`cargo new http-example` and run these commands from its directory.

```sh
cargo add open-net --no-default-features --features http-client
cargo add rustls-openssl --no-default-features --features tls12
cargo add tokio --features macros,rt-multi-thread,time
cargo add futures-util
```

For the event decoder examples, follow the [checkout dependency setup](README.md#try-file-uploads-and-sse-events-from-a-checkout).

## Application TLS provider

open-net does not choose a cryptographic provider. Each complete program that
creates an HTTP client below initializes the example application's provider in
`main`; copy the program as written. Configuration-only examples need no provider
until a client is built. Client creation requires a provider even for plain HTTP
and otherwise returns `InvalidConfig` for `tls.provider`.

The examples use OpenSSL through `rustls-openssl`, an application dependency.
Another compatible rustls provider can replace it. Install a process provider
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

`max_response_bytes` limits buffered responses only. Raw stream consumers can count delivered bytes to limit the total response. The SSE decoder takes a separate per-event limit.

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

## `HttpClient::stream_with_quality_ref()`

### Required features

`http-client` and `quality-doctor`.

### Function signature

```text
pub async fn stream_with_quality_ref(
    &self,
    request: HttpRequest,
) -> Result<(HttpStreamResponse, Option<QualityReportRef>), NetError>
```

### Steps

Activate the client's doctor, build a request, then await this method. Consume
the response as usual. Use the returned reference with the same doctor's
`lookup` to inspect the operation.

### Parameters

`request` is the owned `HttpRequest` to send.

### Callback parameters

None.

### Return values

`Ok((response, reference))` returns the stream at headers. The optional
reference identifies the `HttpStream` observation. A usable response can carry
`None` when collection was inactive, HTTP was excluded or diagnostic capacity
was unavailable.

### Errors and behavior

Admission, retries and deadlines follow [the streaming API](#httpclientstream).
An opening failure returns a `NetError` that can carry its own reference. Body
errors arrive while reading. The report can remain in progress until EOF, a
terminal error or drop. Keeping the reference does not extend report retention.

### Example

[Read a response body and look up its stream observation](../quality_doctor.md#streaming-http-example).

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

Open a translation GET with `sse()` or a chat POST with `stream()`.
Set `Accept: text/event-stream` on either request and `Content-Type: application/json`
on the JSON POST. Both return an `HttpStreamResponse` that the same event loop can consume.

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

The normal admission, path, header, cancellation and timeout rules apply. HTTP error statuses remain successful response values until check_status(). This helper opens a GET through `stream()`. Supply the desired Accept header. Use `next_chunk()` for raw bytes or convert the response to events.

### Example

With a server at `127.0.0.1:8080`, use `cargo run` for the translation GET
or `cargo run -- chat` for the chat POST. For a translation response that
contains the following events, the loop receives one `segment` whose data is
`"first line\nsecond line"`, then one `end` with empty data. The blank line
after each event completes it. Chinese text uses the same loop, even when
a chunk ends in the middle of a UTF-8 character or a CRLF line ending.

```text
event: segment
data: first line
data: second line

event: end
data:

```

```rust
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpRequest, HttpStatusPolicy, HttpStreamResponse,
};
use open_net::{BoxError, HeaderMap, HeaderValue};
use std::io::Write;

async fn consume_events(response: HttpStreamResponse) -> Result<(), BoxError> {
    response.check_status(HttpStatusPolicy::Success2xx)?;
    let mut events = response.into_sse_events(256 * 1024)?;
    while let Some(event) = events.next_event().await.transpose()? {
        match event.event.as_str() {
            "message" | "segment" => writeln!(std::io::stdout(), "{}", event.data)?,
            "end" => break,
            "error" => return Err("server sent an application error event".into()),
            _ => {}
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    rustls_openssl::default_provider().install_default()
        .map_err(|_| std::io::Error::other("TLS provider installation failed"))?;
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "stream-example",
    )?;
    let result = async {
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("text/event-stream"));
        let response = if std::env::args_os().nth(1).is_some_and(|arg| arg == "chat") {
            let request = HttpRequest::builder()
                .method("POST")
                .path("/chat")
                .headers(headers)
                .header("content-type", "application/json")?
                .body(r#"{"prompt":"Hello"}"#.into())
                .build()?;
            client.stream(request).await?
        } else {
            client.sse("/translation/events", headers).await?
        };
        consume_events(response).await
    }.await;
    client.shutdown().await?;
    result
}
```

## `HttpStreamResponse::into_sse_events()`

### Required features

`http-client`.

### Function signature

```text
pub fn into_sse_events(self, max_event_bytes: usize) -> Result<SseEvents, NetError>
```

### Steps

Check the HTTP status before conversion. To cancel from another task, copy
`response.request_id` before moving the response and call `client.cancel(id)?`
on the same client while that request is active.

### Parameters

Choose a nonzero limit for one event block, including an unfinished line. Each
nonempty line counts its wire bytes plus one normalized line ending. The optional
stream-opening UTF-8 BOM and the blank separator are free. The example allows
256 KiB per block and can consume any number of blocks.

### Callback parameters

None.

### Return values

An owned decoder that accepts the text/event-stream media type and its parameters.

### Errors and behavior

A zero limit returns `InvalidInput`. A missing or incorrect media type returns
`Protocol`. Exceeding the limit during decoding returns `ItemTooLarge` from
`next_event()`. The conversion consumes the response.

### Example

See the [event-reading example](#httpclientsse).

## `SseEvents::next_event()`

### Required features

`http-client`.

### Function signature

```text
pub async fn next_event(&mut self) -> Option<Result<SseEvent, NetError>>
```

### Steps

Await successive events. Handle application-specific event names and decide when to stop.

### Parameters

A mutable borrow of the decoder.

### Callback parameters

None.

### Return values

Each event has a name, UTF-8 data, and the most recent event ID. Multiple data lines are joined with a newline. An empty data field delivers an empty string. A clean EOF discards an unfinished event.

### Errors and behavior

A named error event is delivered as data. I/O, timeout, cancellation or an oversized event yields one terminal error followed by EOF. Complete events from a previously received chunk are delivered in order before polling the transport again. Dropping the current wait preserves decoder state. Dropping the decoder releases the response.

### Example

See the [event-reading example](#httpclientsse).

## `SseEvents::last_event_id()`

### Required features

`http-client`.

### Function signature

```text
pub fn last_event_id(&self) -> &str
```

### Steps

Read after polling the decoder when retaining an ID for a later request.

### Parameters

A shared borrow of the decoder.

### Callback parameters

None.

### Return values

The last event ID committed at an event boundary, initially the empty string. A control-only block can update it.

### Errors and behavior

An empty id field resets the ID. An id containing NUL is ignored.

### Example

See the [event-reading example](#httpclientsse).

## `SseEvents::retry_delay()`

### Required features

`http-client`.

### Function signature

```text
pub fn retry_delay(&self) -> Option<std::time::Duration>
```

### Steps

Read after polling the decoder if the application handles reconnection.

### Parameters

A shared borrow of the decoder.

### Callback parameters

None.

### Return values

The most recent valid retry field in milliseconds, or None before one is received.

### Errors and behavior

A valid retry field records a delay. Reconnecting and replaying a request are application decisions.

### Example

See the [event-reading example](#httpclientsse).

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

# open-net

`open-net` is a cross-platform asynchronous networking library for Rust. It gives your application one small owner object, `OpenNet`, and lets you create named clients for HTTP, WebSocket, and network-status monitoring.

This guide is written for a developer who is new to Rust or is using an AI coding assistant. Each example uses the public API of the crate. Copy one complete example first, make it compile, and then add optional pieces one at a time.

## What you get

- **`OpenNet`** owns the library runtime and named client instances.
- **`HttpClient`** provides bounded asynchronous HTTP requests with buffered or streaming responses.
- **`WebSocketClient`** provides WebSocket sessions, text/binary messages, bounded sending, reconnects, heartbeats, subscriptions, and optional request/response routing.
- **`NetStatusClient`** monitors host reachability and IPv4/IPv6 capability.
- **Structured errors and logs** let you classify failures with `ErrorKind` and opt in to callback-based logs.

A useful mental model is:

```text
OpenNet engine
├── named HttpClient(s)
├── named WebSocketClient(s)
└── named NetStatusClient(s)
```

A named client is a reusable handle. Creating it starts its worker, but does not make an HTTP request or open a WebSocket connection. The name is reserved until you call the matching `destroy_*` method or drop the engine.

## Install it

`open-net` 0.1.0-beta.2 requires Rust 1.89 or newer. The repository also pins toolchain 1.89.0 in `rust-toolchain.toml`.

Add Tokio because your application owns the async entry point:

```toml
[dependencies]
open-net = { version = "0.1.0-beta.2", features = ["http-client"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "fs", "io-util", "time"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

Features are opt-in except for the default WebSocket feature:

| Dependency declaration | Included API |
| --- | --- |
| `open-net = "0.1.0-beta.2"` | WebSocket client plus network status |
| `open-net = { version = "0.1.0-beta.2", features = ["http-client"] }` | WebSocket, HTTP, and network status |
| `open-net = { version = "0.1.0-beta.2", default-features = false, features = ["http-client"] }` | HTTP and network status, without WebSocket |
| `open-net = { version = "0.1.0-beta.2", default-features = false }` | Network status and shared configuration only |

The feature name is **`http-client`**. There is no separate `http` feature. The default feature is **`ws-client`**.

## Your first `OpenNet` program

All clients are created from an engine. `OpenNet::new()` validates the default configuration and returns a `Result`, so use `?` in your application instead of unwrapping immediately.

```rust,no_run
use open_net::{BoxError, OpenNet};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let net = OpenNet::new()?;

    let status = net.create_net_status_client("network").await?;
    let first = status.start().await?;
    println!("network state: {:?}", first.state);

    status.shutdown().await?;
    net.destroy_net_status_client("network").await?;
    Ok(())
}
```

`OpenNet` defaults to management queue capacities of 128 for asynchronous and synchronous submissions. You can change them before creating the engine:

```rust,no_run
use open_net::{OpenNet, OpenNetConfig, Result};

fn make_engine() -> Result<OpenNet> {
    let config = OpenNetConfig::default()
        .with_runtime_worker_threads(2)
        .with_async_queue_capacity(256)
        .with_sync_queue_capacity(128);
    OpenNet::new_with_config(config)
}
```

The worker count must be between 1 and 256. Queue capacities must be non-zero and within Tokio's semaphore limit. Invalid values return an `InvalidConfig` error before workers are created.

Client names are trimmed and must not be empty. Names are unique per engine:

```rust,no_run
use open_net::{OpenNet, Result};

async fn names() -> Result<()> {
    let net = OpenNet::new()?;
    let _http = net.create_http_client_with_config(
        "api",
        open_net::api::http::HttpClientConfig::new("https://api.example.com")?,
    ).await?;

    let same_http = net.get_http_client(" api ")?;
    drop(same_http);
    net.destroy_http_client("api").await?;
    Ok(())
}
```

`destroy_http_client`, `destroy_ws_client`, and `destroy_net_status_client` wait for worker cleanup and release the name. Calling a client's own `shutdown()` closes its worker but does not release the engine registration; destroy the named client when you are finished.

## HTTP client

The HTTP API is behind the `http-client` Cargo feature and is asynchronous (`tokio`). The examples use the public API in `open_net::api::http`; they are intentionally small enough to copy into a new binary and then grow with your application.

## Add the dependency

In the application `Cargo.toml`:

```toml
[dependencies]
open-net = { version = "0.1.0-beta.2", features = ["http-client"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "fs", "io-util", "time"] }
serde = { version = "1", features = ["derive"] }
# Only needed for the explicit builder/callback snippets below:
bytes = "1"
http = "1"
async-trait = "0.1"
```

`open-net` has `ws-client` in its default feature set, but `http-client` is separate. Listing the feature explicitly makes the dependency work even when the application disables default features. The first complete program needs only `open-net`, `tokio`, and `serde`; the explicit builder and callback snippets use the public `bytes`, `http`, and `async-trait` crates, so add those dependencies when copying those snippets.

## Mental model: `OpenNet` owns named clients

Create one `OpenNet` value, then create a named `HttpClient` from a validated `HttpClientConfig`. The name is a registry key, not a URL and not a request ID. `HttpClient` is cloneable; clones share the same worker queues and shutdown state.

Requests use **relative paths** such as `/users?limit=10`. The client joins that path to `base_url` and rejects absolute URLs, `//host` paths, backslashes, fragments (`#...`), credentials, and cross-origin targets. Configure an origin such as `https://api.example.com/v1/` and keep every request under that origin.

`get`, `post`, and `request` buffer the complete response body in memory. `stream` and `sse` expose response chunks as they arrive. There is currently no request-body upload stream API: request bodies are owned `bytes::Bytes` and are sent as a complete body. For a very large upload, split it into application-level chunks or use a separate upload mechanism; do not assume `stream` streams an upload.

## Complete program: GET, POST, JSON, and cleanup

The following is a complete `src/main.rs`. It uses `https://httpbin.org`, so it needs network access when run. The important pattern is also valid for an internal API.

```rust
use open_net::api::http::{HttpClientConfig, HttpStatusPolicy};
use open_net::OpenNet;
use serde::Deserialize;
use std::time::Duration;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Deserialize)]
struct GetReply {
    url: String,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // Config::new validates that the URL is absolute http:// or https://.
    let config = HttpClientConfig::new("https://httpbin.org/")?
        .with_timeout(Duration::from_secs(15))?;

    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("demo-http", config)
        .await?;

    let get = client.get("/get?hello=world").await?;
    // A 404/500 is still an Ok(HttpResponse). Apply the policy you want.
    get.check_status(HttpStatusPolicy::Success2xx)?;
    println!("GET {}: {}", get.status, get.text()?);

    let post = client.post("/post", br#"{"name":"Ada"}"#).await?;
    post.check_status(HttpStatusPolicy::Success2xx)?;
    println!("POST status: {}", post.status);

    // Parse a JSON response after checking the status.
    let parsed: GetReply = get.json()?;
    println!("server saw URL: {}", parsed.url);

    // Destroy the named worker and wait until it has exited.
    net.destroy_http_client("demo-http").await?;
    Ok(())
}
```

`HttpResponse` contains `status`, `headers`, `body` (`bytes::Bytes`), `attempts`, and `request_id`. `text()` borrows the body as UTF-8 and returns a `Utf8Error` for binary data. `json::<T>()` uses `serde_json`; it does not check the status for you. Decode only after `check_status` unless your protocol intentionally uses error JSON.

The convenience methods are:

```rust
let response = client.get("/health").await?;
let response = client.get_with_headers("/health", headers).await?;
let response = client.post("/events", body_bytes).await?;
let response = client.post_with_headers("/events", headers, body_bytes).await?;
```

`post` accepts anything implementing `AsRef<[u8]>` (`&[u8]`, `Vec<u8>`, a byte string, and so on). `get_bytes` and `get_bytes_with_headers` are compatibility aliases for `get` and `get_with_headers`.

## Headers and request builder

Client-wide headers are copied onto every request. Request-specific headers replace a matching common header. Hop-by-hop headers such as `host`, `connection`, `content-length`, and `transfer-encoding` are rejected by configuration/dispatch.

```rust
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method};
use open_net::api::http::HttpRequest;

let mut headers = HeaderMap::new();
headers.insert("accept", HeaderValue::from_static("application/json"));

let request = HttpRequest::builder()
    .method(Method::PUT)
    .path("/v1/items/42?verbose=true")
    .headers(headers)
    .body(Bytes::from_static(br#"{"enabled":true}"#))
    .build()?;

let response = client.request(request).await?;
response.check_status(open_net::api::http::HttpStatusPolicy::Success2xx)?;
```

The builder starts with `GET`, empty headers, an empty body, and `RetryPolicy::no_retry()`. `build()` rejects an empty path; the client performs the same-origin path checks when the request is submitted. To add one header without constructing a `HeaderMap`, use `.header("x-trace-id", "abc")?` before `.build()`.

## Handling HTTP errors versus transport errors

There are two different failure layers:

* `Err(NetError)` means the request could not produce a complete buffered response (bad configuration/path, DNS/I/O failure, timeout, cancellation, body limit, or exhausted retry attempts).
* `Ok(HttpResponse)` means the peer returned an HTTP response, even when its status is 400, 404, or 500. Inspect `response.status` and call `check_status(HttpStatusPolicy::Success2xx)` (or `Any`/`Exact(code)`). The response body and headers remain available before you propagate the status error.

```rust
use open_net::api::http::HttpStatusPolicy;

match client.get("/maybe-missing").await {
    Ok(response) => {
        if let Err(status_error) = response.check_status(HttpStatusPolicy::Success2xx) {
            eprintln!("server returned {}: {status_error}", response.status);
            eprintln!("error body: {}", response.text().unwrap_or("<binary>"));
        }
    }
    Err(transport_error) => {
        eprintln!("request failed before a usable response: {transport_error}");
    }
}
```

`HttpStatusPolicy::Any` accepts every status, `Success2xx` accepts the 2xx range, and `Exact(StatusCode::NO_CONTENT)` accepts one code. `NetError::kind()` and `NetError::context().stage` are useful when logging or classifying failures; never log secrets from request headers or bodies.

## Complete program: streaming a large response

Use `stream` when buffering the complete response would be too expensive, or use `sse` for a server-sent-events endpoint. The response status and headers are available immediately; then repeatedly await `next_chunk()`.

```rust
use open_net::api::http::{HttpClientConfig, HttpRequest, HttpStatusPolicy};
use open_net::OpenNet;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = HttpClientConfig::new("https://example.com/")?
        .with_timeout(Duration::from_secs(60))?;
    let net = OpenNet::new()?;
    let client = net.create_http_client_with_config("downloader", config).await?;

    let request = HttpRequest::builder().path("/large-file.bin").build()?;
    let mut response = client.stream(request).await?;
    response.check_status(HttpStatusPolicy::Success2xx)?;

    let mut output = tokio::fs::File::create("large-file.bin").await?;
    while let Some(chunk) = response.next_chunk().await {
        let chunk = chunk?;
        output.write_all(&chunk).await?;
    }
    output.flush().await?;

    net.destroy_http_client("downloader").await?;
    Ok(())
}
```

`next_chunk()` returns `None` at clean EOF, `Some(Ok(Bytes))` for a chunk, and `Some(Err(NetError))` for a terminal stream/transport error. A stream does not replay bytes already consumed. Call `response.check_status(...)` before writing data if non-2xx responses should not be saved. `HttpClient::sse(path, headers)` is a convenience for opening a GET stream with an `Accept: text/event-stream` header supplied by you; it does not parse SSE frames. Decode lines/events in your application.

## Upload bodies and “streaming upload” expectations

The public request model is an owned body (`Bytes`). `post`, `post_with_headers`, and `HttpRequest::builder().body(...)` therefore buffer the upload before dispatch. `HttpRequestTrait::get_req_body()` is called once per transport attempt and must return equivalent bytes when retries are enabled; it is a replay hook, not a socket-level upload stream. There is no `AsyncRead`/chunk producer parameter in this API.

For a large payload, prefer a multipart/object-storage client designed for upload streaming, or divide the data into multiple application requests. If you must use this API, set a suitable `max_response_bytes` (that limit applies to received responses) and ensure your own body fits available memory.

## Timeouts, response limits, retries, and backpressure

`HttpClientConfig::new` defaults to a 30-second request timeout, an enabled 15-second TCP keepalive, 8 MiB maximum buffered response body, and request/response/callback queue capacities of 128. Override them with:

```rust
let config = HttpClientConfig::new("https://api.example.com/")?
    .with_timeout(Duration::from_secs(10))?
    .with_max_response_bytes(16 * 1024 * 1024)?
    .with_request_queue_capacity(256)?
    .with_response_queue_capacity(256)?
    .with_callback_queue_capacity(256)?;
```

A zero timeout, zero limit, or zero queue capacity is rejected. `with_tcp_keepalive(Duration::ZERO)` disables keepalive; `without_tcp_keepalive()` is equivalent.

### Retries

The safe default is no retry. `RetryPolicy::new(n)` allows up to `n` retries (capped at 32), retries transient statuses `408`, `425`, `429`, `500`, `502`, `503`, and `504`, and retries transient transport failures. GET/HEAD/OPTIONS/PUT/DELETE are considered idempotent. POST and other non-idempotent methods require both `.with_non_idempotent(true)` and a replayable body.

Attach a policy to a specific request:

```rust
use open_net::api::http::{HttpRequest, RetryPolicy};
use std::time::Duration;

let policy = RetryPolicy::new(3)
    .with_max_delay(Duration::from_secs(5));
let request = HttpRequest::builder()
    .path("/temporary")
    .retry_policy(policy)
    .build()?;
let response = client.request(request).await?;
```

For a custom request sent through `send_with_options`, use `HttpRequestOptions::with_retry_policy(...)` and, when appropriate, `.with_replayable_body(true)`. A non-idempotent request is never made retryable merely by setting a retry count; explicitly opt in and return the same body on every attempt. Retry backoff can be interrupted by cancellation. A partially consumed streaming response is not replayed.

**Important convenience-method detail:** `request(HttpRequest)` supplies explicit options derived from the request's own `retry_policy`. Therefore `get`/`post` use the builder's no-retry policy even if `HttpClientConfig.default_retry_policy` is non-empty. To use the client default or custom options, build a request and call `send`/`send_with_options`, or attach `RetryPolicy` to the request as shown above.

### Queue admission and `send_wait`

`send` fails quickly when the bounded request queue cannot admit a job. `send_wait(...).await` waits for capacity (backpressure) and returns a client-local `HttpRequestId` after admission. An accepted request owns exactly one terminal callback, including during shutdown. Keep the returned ID if you need cancellation or correlation.

## Cancellation and lifecycle

```rust
let request_id = client.send(request)?;
client.cancel(request_id)?; // best effort if a callback is already running
client.cancel_all()?;        // cancel every accepted request owned by this client
client.drain().await?;       // wait for admitted requests; admission stays open
client.shutdown_graceful().await?; // cancel accepted requests, then join workers
```

`client.shutdown().await` closes admission and waits for worker shutdown. `client.request_shutdown()` only publishes the shutdown signal and returns immediately; use it when synchronous notification is required, then await `shutdown()` if you need completion. Cloned handles observe the same shutdown. Finally, `OpenNet::destroy_http_client(name).await` permanently removes the named registry entry after the worker exits. Destroying a client is the normal application cleanup path.

## Optional advanced API: custom callback request

Most applications should use `request`, `get`, `post`, or `stream`. Implement `HttpRequestTrait` only when you need a callback that runs on the client's callback lane, custom request state, or access to a request ID without awaiting a response future. This is a fragment: add `async-trait = "0.1"` to your application dependencies and import `bytes`/`http` as shown.

```rust
use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, Method};
use open_net::api::http::{HttpRequestId, HttpRequestOptions, HttpRequestTrait, HttpResponseResult, RetryPolicy};

struct SaveReply {
    path: String,
}

#[async_trait]
impl HttpRequestTrait for SaveReply {
    fn get_path(&self) -> String { self.path.clone() }
    fn get_method(&self) -> Method { Method::GET }
    fn get_req_body(&self) -> Bytes { Bytes::new() }
    fn headers(&self) -> HeaderMap { HeaderMap::new() }
    fn retry_policy(&self) -> RetryPolicy { RetryPolicy::new(2) }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        match result {
            Ok(response) => println!("callback got {} ({} bytes)", response.status, response.body.len()),
            Err(error) => eprintln!("callback failed: {error}"),
        }
    }
}

let request_id: HttpRequestId = client.send_with_options(
    SaveReply { path: "/health".into() },
    HttpRequestOptions::new()
        .with_retry_policy(RetryPolicy::new(2))
        .with_replayable_body(true),
)?;
println!("accepted request {request_id:?}");
```

A callback is invoked once after the final attempt, including cancellation or an error. Keep callbacks short and non-blocking; they run on a bounded callback lane. The trait's `on_response_headers` hook can observe headers before the final callback. `get_req_body()` may be called again for retries, so never consume a one-shot reader there.

## TLS, proxy, and security configuration

`HttpClientConfig::new` uses reqwest's rustls transport. `HttpTlsConfig` can append or replace built-in roots and can configure a PEM client identity; all PEM is validated before worker startup. `HttpProxyConfig` configures an explicit HTTP CONNECT proxy. Environment proxy variables are **disabled by default**; opt in with `.with_environment_proxy(true)`. Keep credentials out of URLs and do not print bodies/authorization headers in logs.

The response body limit is a safety bound for buffered responses. Streaming responses are consumed incrementally, but still surface transport/framing errors through `next_chunk()`.

## WebSocket client

`WebSocketClient` is OpenNet's long-lived WebSocket API. It separates a reusable
client from a logical connection **session**:

* The client owns configuration and the background worker. It can be cloned and
  is shut down permanently with `client.shutdown().await`.
* A session owns one connection intent (including automatic reconnects). Keep the
  `Session` value alive while the feature is running. Dropping it requests
  cancellation, even if a `Sender`, receiver, or request handle was cloned from
  it. Call `session.close().await` for an orderly close, then destroy the client
  when the client name is no longer needed.
* A client has at most one active session across all of its clones. Close the old
  session and wait for it to finish before starting another target.

These examples use Tokio:

```toml
[dependencies]
open-net = { version = "0.1.0-beta.2", features = ["ws-client"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

The `ws-client` feature is enabled by default in this release. A WebSocket URL
must use `ws://` or `wss://` and include a host.

## The smallest working program (text echo)

The following is a complete program. Run it against an ordinary WebSocket echo
server: the server must send each received WebSocket data message back unchanged.
It sends the text `hello`, waits for the echoed data message, and closes cleanly.

```rust,no_run
use open_net::{BoxError, OpenNet};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::args()
        .nth(1)
        .ok_or("usage: cargo run --example echo -- ws://127.0.0.1:9000/echo")?;

    let net = OpenNet::new()?;
    let client = net.create_ws_client("echo").await?;

    // `connect` waits for the first successful handshake. The returned Session
    // must remain in scope while we use its sender and receiver.
    let mut session = client.connect(url).await?;
    let mut incoming = session.take_messages().ok_or("initial inbox missing")?;

    session.sender().send("hello").await?;
    if let Some(item) = incoming.recv().await? {
        // `message()` is Some for text/binary data and None for Ping, Pong, or
        // Close control frames. Control frames are filtered by default.
        if let Some(message) = item.message() {
            println!("echoed {} bytes: {:?}", message.len(), message.as_text());
        }
    }

    session.close().await?;
    net.destroy_ws_client("echo").await?;
    Ok(())
}
```

`sender.send(...)` waits for the local WebSocket write to finish. A successful
local write is not an acknowledgement that the server processed the application
message. For a trackable write, use `sender.enqueue(...)` and await
`receipt.written()`; for a server response, use the request API below.

## Data messages and application protocols

WebSocket defines text and binary *payloads*, not JSON, topics, authentication
messages, or request IDs. OpenNet does not add an application protocol. Convert
your own values explicitly:

```rust,no_run
use open_net::ws::Message;

let text = Message::text(r#"{"type":"chat","body":"hi"}"#);
let bytes = Message::binary(vec![0, 1, 2, 3]);
assert_eq!(text.as_text(), Some(r#"{"type":"chat","body":"hi"}"#));
assert_eq!(bytes.as_bytes(), &[0, 1, 2, 3]);
```

`IncomingMessage::message()` returns a borrowed `Message`. Match on
`message.as_text()` when the server's protocol is text, or use `as_bytes()` for
binary. Parse JSON with your chosen serde type, and validate fields before using
them. A text payload from the server is not automatically a Rust `String` or a
deserialized struct.

## Receiving with a subscription

`take_messages()` takes the one initial inbox created by default in
`ConnectOptions`. It can only be taken once. For additional independent
receivers, call `subscribe_messages` with a bounded `ReceiveOptions`:

```rust,no_run
use open_net::{OpenNet, Result};
use open_net::ws::{ReceiveOptions, ReceiveOverflow};

async fn read_prices(client: &open_net::ws::WebSocketClient) -> Result<()> {
    let session = client.connect("wss://example.test/prices").await?;
    let mut prices = session.subscribe_messages(ReceiveOptions {
        max_messages: 512,
        max_bytes: 8 * 1024 * 1024,
        overflow: ReceiveOverflow::DropOldest,
        ..ReceiveOptions::default()
    })?;

    while let Some(item) = prices.recv().await? {
        if let Some(message) = item.message() {
            println!("received {} bytes", message.len());
        }
    }
    // The stream ends when the session/connection source closes or the
    // subscription is unsubscribed. Dropping `prices` also unsubscribes it.
    session.close().await?;
    Ok(())
}
```

Every receive queue is bounded by both item count and payload bytes. The default
is 256 messages or 64 MiB, with `ReceiveOverflow::Disconnect`: if a consumer
cannot keep up, the current connection is failed rather than silently losing
business messages. `DropOldest` keeps the connection alive but `recv()` reports a
lag error, so the application must resynchronize. `include_control_frames` is
`false` by default; set it to `true` only when you need to inspect Ping/Pong/Close
frames.

For callbacks, convert a receiver into a `Subscription`. The callback must be
`Send + Sync + 'static` because it runs on OpenNet's callback pool; keep the
returned subscription alive for as long as the callback is wanted:

```rust,no_run
use open_net::{OpenNet, Result};
use open_net::ws::ReceiveOptions;

async fn callback_example(net: &OpenNet) -> Result<()> {
    let client = net.create_ws_client("callback").await?;
    let mut session = client.connect("wss://example.test/feed").await?;
    let receiver = session.subscribe_messages(ReceiveOptions::default())?;
    let subscription = receiver.into_callback(|_context, result| match result {
        Ok(item) => println!("message: {:?}", item.message().and_then(|m| m.as_text())),
        Err(error) => eprintln!("receive stream ended or lagged: {error}"),
    })?;

    // Keep both values alive. Dropping either the Session or Subscription ends
    // the work associated with it.
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    subscription.unsubscribe();
    session.close().await?;
    Ok(())
}
```

The same pattern is available for lifecycle events (`subscribe_events` or
`on_event`), state snapshots (`watch_state` or `on_state`), and terminal task
events (`subscribe_tasks` or `on_task`). `session.state()` is a point-in-time
snapshot; `session.wait_connected().await` waits for a usable physical
connection, and `session.closed().await` waits for the terminal session result.

## One-way messaging versus request/response

For notifications, telemetry, and commands that do not have a correlated reply,
leave the default `ResponseRouting::Disabled` and use `Sender`. Calling
`session.requests()` in that mode returns `RoutingDisabled`.

For request/response, your wire format must contain a request identifier and you
must teach OpenNet how to classify incoming messages. The following complete
example uses the deliberately simple protocol `request-id|payload`; an echo
server returns the same string. A production protocol should parse its real JSON
or binary envelope and validate IDs before returning `Final`.

```rust,no_run
use open_net::{BoxError, OpenNet};
use open_net::ws::{
    ConnectOptions, IncomingMessage, InitialMessages, Message, Request, RequestId,
    ResponseProtocol, ResponseRoute, ResponseRouting,
};

struct PipeProtocol;

impl ResponseProtocol for PipeProtocol {
    fn route(&self, incoming: &IncomingMessage) -> Result<ResponseRoute, BoxError> {
        let Some(text) = incoming.message().and_then(Message::as_text) else {
            return Ok(ResponseRoute::Unmatched);
        };
        let Some((id, _body)) = text.split_once('|') else {
            return Ok(ResponseRoute::Unmatched);
        };
        Ok(ResponseRoute::Final {
            request_id: RequestId::new(id)?,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::args().nth(1).ok_or("provide a WebSocket URL")?;
    let net = OpenNet::new()?;
    let client = net.create_ws_client("requests").await?;

    let mut options = ConnectOptions::new(url);
    options.routing = ResponseRouting::protocol(PipeProtocol);
    // Unmatched messages are not needed by this example. Use a receiver instead
    // when your application also needs broadcasts or progress messages.
    options.initial_messages = InitialMessages::DiscardUnmatched;
    let session = client.connect(options).await?;

    let id = RequestId::random()?;
    // RequestId is local correlation metadata. Encode it into your wire format.
    let wire = Message::text(format!("{}|hello", id.as_str()));
    let response = session.requests()?.execute(Request::new(id, wire)).await?;
    println!("response: {:?}", response.message());

    session.close().await?;
    net.destroy_ws_client("requests").await?;
    Ok(())
}
```

`RequestClient::execute` registers the request, sends it, and waits for the
`ResponseRoute::Final` message matching that `RequestId`. A successful response
means that OpenNet routed a message to this request; it does not prove that a
separate business transaction was committed. Set `ResponseRoute::Intermediate`
for progress messages and return `Final` only for the terminal response.
Request IDs are application-level values and must be encoded in the payload or
headers understood by the server.

Use `ResponseRouting::Manual` when classification needs application state or
when a dedicated reader owns the protocol. Obtain a resolver with
`session.response_resolver()`, enqueue a request, then pass each matching
`IncomingMessage` and `receipt.handle().registration()` to
`resolver.resolve(...)`. `Resolved` completes the request; `StaleOrFinished`
means it already timed out or completed; `ForeignOrigin` means the registration
belongs to another session. Manual routing requires a lossless initial inbox, so
do not combine it with `ReceiveOverflow::DropOldest`.

Here is the same `id|body` protocol with manual dispatch. This is useful when a
reader must apply application state before deciding whether a message is the
final response:

```rust,no_run
use open_net::{BoxError, OpenNet};
use open_net::ws::{
    ConnectOptions, InitialMessages, Message, ReceiveOptions,
    Request, RequestId, ResolveOutcome, ResponseRouting,
};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::args().nth(1).ok_or("provide a WebSocket URL")?;
    let net = OpenNet::new()?;
    let client = net.create_ws_client("manual-requests").await?;
    let mut options = ConnectOptions::new(url);
    options.routing = ResponseRouting::Manual;
    options.initial_messages = InitialMessages::Buffer(ReceiveOptions::default());
    let mut session = client.connect(options).await?;
    let mut inbox = session.take_messages().ok_or("initial inbox missing")?;
    let resolver = session.response_resolver()?;

    let id = RequestId::random()?;
    let receipt = session
        .requests()?
        .request(Request::new(
            id.clone(),
            Message::text(format!("{}|hello", id.as_str())),
        ))
        .enqueue()
        .await
        .map_err(|failure| failure.into_error())?;
    let registration = receipt.handle().registration().clone();

    while let Some(incoming) = inbox.recv().await? {
        let Some((wire_id, _body)) = incoming
            .message()
            .and_then(Message::as_text)
            .and_then(|text| text.split_once('|'))
        else {
            continue;
        };
        if wire_id != id.as_str() {
            continue;
        }
        // Application checks can happen immediately before this call.
        if resolver.resolve(&registration, &incoming)? == ResolveOutcome::Resolved {
            break;
        }
    }

    let response = receipt.response().await?;
    println!("manually resolved: {:?}", response.message());
    session.close().await?;
    net.destroy_ws_client("manual-requests").await?;
    Ok(())
}
```

## Reconnect, timeouts, and heartbeats

`ConnectOptions::new` uses these defaults:

* handshake timeout: 10 seconds per attempt;
* first-connection timeout: 30 seconds for all attempts and backoff (`None`
  removes this extra overall limit);
* reconnect: `BackoffConfig { max_retries: 6, initial_delay: 250 ms,
  max_delay: 8 s, max_elapsed: Some(30 s) }`. Delays use full jitter from zero to
  each exponential cap.

The same session follows its reconnect policy after a later disconnect. To fail
immediately instead, set `options.reconnect = ReconnectPolicy::Disabled`. A
session reconnect does not change its logical session ID; each physical
connection has its own connection ID in events and responses.

The client config enables WebSocket Ping/Pong heartbeats by default:
`WebSocketClientConfig::default().heartbeat` is `Some(HeartbeatConfig {
interval: 20 s, pong_timeout: 45 s })`. Set `heartbeat = None` only when the
server or deployment supplies its own liveness mechanism. Heartbeat control
frames are separate from application text messages and are excluded from receive
queues unless `include_control_frames` is enabled.

Sending has separate policies. `SendOptions::default()` rejects a message when
the session is disconnected, uses a 10-second write timeout, and never retries a
message that was already attempted. `DisconnectedPolicy::WaitForReconnect` can
hold an unsent message until the session reconnects. `SendRetryPolicy::Idempotent`
allows bounded redelivery only when your server operation is safe to repeat; do
not enable it for non-idempotent payments or mutations without an idempotency key.

## Proxy and TLS policy

Network policy is explicit and immutable for each client. The default is a direct
connection with built-in WebPKI roots; OpenNet does not read proxy environment
variables. Configure a proxy when creating the client:

```rust,no_run
use open_net::network::{NetworkConfig, ProxyBasicAuth, ProxyConfig};
use open_net::{OpenNet, Result};

async fn client_through_proxy(net: &OpenNet) -> Result<open_net::ws::WebSocketClient> {
    let credentials = ProxyBasicAuth::new("proxy-user", "proxy-password")?;
    let proxy = ProxyConfig::http_connect(
        "http://proxy.example:8080",
        Some(credentials),
    )?;
    let network = NetworkConfig::default().with_proxy(proxy);
    net.create_ws_client_with_network_config(
        "through-proxy",
        open_net::ws::WebSocketClientConfig::default(),
        network,
    )
    .await
}
```

`ProxyConfig::http_connect` accepts only an HTTP CONNECT endpoint. Basic proxy
credentials are sent to the proxy and should be protected with a trusted network
path; a plain HTTP proxy does not encrypt those credentials, even when the target
URL is `wss://`. For private CAs, build a `TlsConfig` with
`TlsConfig::with_root_certificates(pem, RootCertificateMode::Append)` (or
`Replace` when intentionally removing WebPKI roots), then pass it with
`NetworkConfig::with_tls`. Use `TlsConfig::with_client_identity` for mutual TLS.
Certificates and private keys are validated in memory; never commit private key
material to source control.

## Closing and errors

Handle every `Result`. Common lifecycle errors include `SessionAlreadyExists`,
`RoutingDisabled`, `QueueFull`, `TimedOut`, `Cancelled`, and `Closed`. A receive
error can represent lag or a source/connection failure; inspect it before deciding
whether to resubscribe or terminate. `session.cancel()` requests cooperative
cancellation and `session.closed().await` reports the final `SessionEnd`.

At application shutdown, stop producing work, await important message receipts or
responses, call `session.close().await`, then call `client.shutdown().await` (or
`net.destroy_ws_client(name).await` to also release the engine's name). Shutdown
does not promise that queued messages were processed by the peer, so use your
protocol's acknowledgement when business delivery matters.

## Network status monitoring

Network status is always available, even when `ws-client` and `http-client` are disabled. The first `start()` waits for a coherent snapshot:

```rust,no_run
use open_net::{OpenNet, Result};

async fn monitor() -> Result<()> {
    let net = OpenNet::new()?;
    let monitor = net.create_net_status_client("network").await?;
    let first = monitor.start().await?;
    println!("reachability: {:?}, IP stack: {:?}", first.reachability, first.ip_stack);

    let mut states = monitor.subscribe()?;
    while let Some(snapshot) = states.recv().await? {
        println!("revision {}: {:?}", snapshot.revision, snapshot.reachability);
        if snapshot.revision > first.revision + 2 {
            break;
        }
    }

    monitor.stop().await?;      // restartable
    monitor.shutdown().await?;  // permanent Closed state
    net.destroy_net_status_client("network").await?;
    Ok(())
}
```

`reachability` and `ip_stack` can be `None` while the monitor is stopped or before an observation exists. `on_change` is a callback alternative; retain its returned `Subscription` for as long as you need updates.

## Logging and errors

The library does not print or persist logs automatically. Register a filtered callback when debugging a problem:

```rust,no_run
use open_net::{LogType, Logger};

fn install_logs() -> std::io::Result<open_net::LogSubscription> {
    Logger::register_log_listener(
        Box::new(|record| {
            eprintln!("[{:?}] {:?}: {}", record.level, record.log_type, record.content);
        }),
        &[LogType::Engine, LogType::HTTP, LogType::WSC, LogType::Common],
    )
}
```

The returned `LogSubscription` must stay alive. It owns a bounded queue; call `dropped_count()` when diagnosing overload. Callbacks run sequentially on the subscription's worker thread.

Use `NetError` like this:

```rust,no_run
use open_net::{error::ErrorKind, Result};

fn classify(result: Result<()>) {
    if let Err(error) = result {
        match error.kind() {
            ErrorKind::TimedOut => eprintln!("the operation timed out"),
            ErrorKind::Dns => eprintln!("DNS failed"),
            ErrorKind::HttpStatus => eprintln!("the server returned an unacceptable HTTP status"),
            ErrorKind::QueueFull | ErrorKind::ResourceExhausted => eprintln!("apply backpressure"),
            _ => eprintln!("network operation failed: {error}"),
        }
        if let Some(status) = error.http_status() {
            eprintln!("HTTP status: {status}");
        }
    }
}
```

`ErrorKind` is non-exhaustive, so include a wildcard arm. `error.context()` contains the processing stage and optional protocol identifiers. `error.config_error()` exposes a configuration field and reason when the error is `InvalidConfig`. The default `Display` and `Debug` representations intentionally redact arbitrary underlying source text, response bodies, credentials, and peer details; call `source()` only when you explicitly need the underlying cause.

## Vibecoding checklist

When asking an AI coding assistant to add networking code, include these constraints in the prompt:

1. Use the public API (`OpenNet`, `open_net::api::http`, and `open_net::ws`); do not import `src/module` or `src/inner`.
2. Preserve the owning `Session` until all WebSocket sends and receives finish.
3. Treat an HTTP 4xx/5xx as `Ok(HttpResponse)` until `check_status(...)` is called.
4. Add an explicit retry policy only when the operation is safe to repeat.
5. Keep queue limits bounded and decide whether `send()` or `send_wait()` matches the desired backpressure.
6. Keep `Subscription`, `MessageReceiver`, and `LogSubscription` variables alive; dropping them unsubscribes or stops delivery.
7. Close the session/client, then destroy the named client so its name can be reused.
8. Never paste passwords, private keys, or access tokens into source code or diagnostic logs.
9. Ask the assistant to compile after each small change with `cargo check`.

## Build and test

From the crate directory:

```sh
cargo check --locked --all-targets --all-features
cargo test --locked --all-features
cargo test --locked --no-default-features
cargo doc --locked --all-features --no-deps
```

The examples shipped with the crate are useful smoke tests: [examples_messages](examples/examples_messages.rs) demonstrates one-way traffic, and [examples_requests](examples/examples_requests.rs) demonstrates protocol-routed responses.

```sh
cargo run --example examples_messages -- ws://127.0.0.1:9001/echo
cargo run --example examples_requests -- ws://127.0.0.1:9001/echo
```

Those commands require a compatible WebSocket echo service. Network tests that depend on a local server, TLS certificates, or a particular operating system need the environment described by the repository's test fixtures.

## License

`open-net` is released under the Apache-2.0 license. See [LICENSE](LICENSE).

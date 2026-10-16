# HTTP integration

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

## Start here

`HttpClient` supports buffered responses, streaming bodies and owned callback
requests. Import it from `open_net::api::http`; it is not re-exported at the crate
root. `OpenNet` optionally manages named clients and attaches a network context.
A standalone constructor is also supported. For lazy initialization shared by
multiple consumers, register a fixed configuration with
[`OpenNet::managed_http`](lifecycle.md#opennetmanaged_http), await its `get`, and
use the returned `HttpClient` normally. The managed handle owns initialization
and shutdown coordination; it does not duplicate request methods.

`async fn` creates work that progresses when its future is polled; `.await` waits
without blocking the async executor thread. `?` returns an error to the caller.
`Result<T, E>` contains either `Ok(T)` or `Err(E)`. Moving a request transfers its
ownership. `client.clone()` creates another handle to the same workers, capacity
and shutdown state, not an independent client. The SDK owns its worker lanes,
but those lanes do not replace the caller's Tokio runtime. Poll response-returning
requests and waiting admission methods inside an active Tokio runtime with time
enabled: their admission/total deadlines can create Tokio timers on the caller.
Opening `stream()` or `sse()` additionally uses the caller's network I/O driver.
`shutdown()` and `shutdown_graceful()` can be awaited on another executor without
a caller Tokio runtime. The SDK owns cleanup; cancelling a shutdown waiter or
stopping its runtime does not cancel accepted cleanup. This does not remove the
runtime requirements for requests or open streams described above.

`#[tokio::main]` provides the required runtime in the examples. Synchronous
applications can own an enabled Tokio runtime and use its `block_on`, as shown
in [Synchronous application bridge](#synchronous-application-bridge).
Immediate `send()`/`send_with_options()` and synchronous configuration accessors
do not require the caller to enter Tokio; accepted callback work runs on SDK lanes.
An already opened SDK stream can be consumed on another executor while the
opening runtime stays alive and drives I/O. A multi-thread runtime continues
driving its workers between `block_on` calls; a current-thread runtime needs to
keep being driven. See [stream lifetime](streaming_and_sse.md#stream-lifetime-and-deadlines).

Disabling default features removes the default WebSocket selection for this
dependency declaration. `features = ["http-client"]` without
`default-features = false` retains that default. Neither transport feature
selects a cryptographic provider. Configure a rustls 0.23 provider with
`HttpTlsConfig::with_crypto_provider` or initialize it in the host application
before client creation; see [TLS providers](../tls.md).
Cargo features are additive across dependencies. Response JSON decoding is
included in `http-client`; request codecs require `http-client-codecs`.

## Quick start

Start a local server with a `/health` route, then run this program. It prints the
status and UTF-8 body. A 4xx/5xx response is received successfully by the transport;
the explicit `check_status` call rejects it. Cleanup runs even if the request fails.

```rust
use open_net::api::http::{HttpClient, HttpClientConfig, HttpStatusPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "quick-start",
    )?;
    let result = client.get("/health").await;
    client.shutdown().await?;
    let response = result?;
    println!("Status: {}", response.status());
    response.check_status(HttpStatusPolicy::Success2xx)?;
    println!("{}", response.text()?);
    Ok(())
}
```

## `HttpClient::new()`

Create standalone workers without a network observation context.

### Required features

`http-client`.

### Function signature

```text
pub fn new(config: HttpClientConfig, thread_name: impl Into<String>) -> Result<Self, NetError>
```

### Steps

Build HttpClientConfig, supply a worker name, then explicitly shut down when finished. This example needs no server.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| config | HttpClientConfig | Yes | Owned, validated HTTP configuration; see [configuration](configuration.md). |
| thread_name | impl Into<String> | Yes | Worker name; must not be blank or contain NUL. |

### Callback parameters

Not applicable.

### Return values

`Ok(HttpClient)` owns a handle to shared workers. `Err(NetError)` means construction failed.

### Errors and behavior

`InvalidInput` rejects invalid worker names. Invalid configuration, runtime creation and transport setup errors propagate as `NetError`. Construction sends no HTTP request. Network observation methods return `InvalidConfig` without a context. Standalone names are not registered in an OpenNet engine.

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
    println!("HTTP workers created");
    client.shutdown().await?;
    Ok(())
}
```

## `HttpClient::new_with_network_status()`

Create standalone workers bound to one engine's network service.

### Required features

`http-client`.

### Function signature

```text
pub fn new_with_network_status(
    config: HttpClientConfig,
    thread_name: impl Into<String>,
    context: NetworkStatusContext,
) -> Result<Self, NetError>
```

### Steps

Build HttpClientConfig, supply a worker name and engine context, then explicitly shut down when finished. This example needs no server.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| config | HttpClientConfig | Yes | Owned, validated HTTP configuration; see [configuration](configuration.md). |
| thread_name | impl Into<String> | Yes | Worker name; must not be blank or contain NUL. |
| context | NetworkStatusContext | Yes | Owned context obtained from an OpenNet engine; may be cloned. |

### Callback parameters

Not applicable.

### Return values

`Ok(HttpClient)` owns a handle to shared workers. `Err(NetError)` means construction failed.

### Errors and behavior

`InvalidInput` rejects invalid worker names. Invalid configuration, runtime creation and transport setup errors propagate as `NetError`. Construction sends no HTTP request. Reading the context does not start monitoring. Keep the engine alive: engine drop closes its monitoring context.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let engine = open_net::OpenNet::new()?;
    let client = HttpClient::new_with_network_status(
        HttpClientConfig::new("http://127.0.0.1:8080")?,
        "observed-http", engine.network_status_context(),
    )?;
    println!("{:?}", client.network_snapshot()?.state);
    client.shutdown().await?;
    Ok(())
}
```

## Engine-managed clients

Use these methods when an engine should retain and name the client. Names are
trimmed; an empty name is invalid. Creation also rejects NUL. Names are unique
within the engine's HTTP registry. `HttpClient::shutdown()` closes the workers
but leaves their registry name reserved; destroy the client to release the name.

| Function signature | Parameters | Return values and errors |
| --- | --- | --- |
| `OpenNet::new() -> Result<Self, NetError>` | None | New engine, or configuration/runtime setup failure. |
| `OpenNet::new_with_config(config: OpenNetConfig) -> Result<Self, NetError>` | Engine configuration by value | New engine, or invalid configuration/runtime failure. HTTP settings still belong to HttpClientConfig. |
| `async create_http_client_with_config(&self, thread_name: &str, config: HttpClientConfig) -> Result<HttpClient, NetError>` | Name and owned configuration | Shared handle bound to the engine's network context. `ClientAlreadyExists` for a reserved name; input/configuration/runtime errors otherwise. |
| `get_http_client(&self, thread_name: &str) -> Result<HttpClient, NetError>` | Name | A clone of the same handle. `ClientNotFound` if absent; `ConnectionClosing` while creating/closing; `InvalidInput` if blank. |
| `async destroy_http_client(&self, thread_name: &str) -> Result<(), NetError>` | Name | `Ok(())` after cancellation, callbacks and worker cleanup; releases the name. Missing name returns `ClientNotFound`; a legacy creating/closing slot returns `ConnectionClosing`; a managed slot joins its shared shutdown. |

The engine constructors have complete per-function descriptions in
[OpenNet::new](../net_status/README.md#opennetnew) and
[OpenNet::new_with_config](../net_status/README.md#opennetnew_with_config).

These functions have no callbacks. After an engine creation or destruction job
has been submitted, dropping the waiting future does not undo that job. If a
create wait was cancelled, query the registry before attempting the same name.
After destroy begins, the name stays reserved until cleanup completes. Existing
clones share the closed workers and cannot reopen them.

This complete example requires no server:

```rust
use open_net::{OpenNet, OpenNetConfig};
use open_net::api::http::HttpClientConfig;

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let engine = OpenNet::new_with_config(OpenNetConfig::default())?;
    let client = engine.create_http_client_with_config(
        "api", HttpClientConfig::new("http://127.0.0.1:8080")?,
    ).await?;
    let same_workers = engine.get_http_client("api")?;
    println!("{:?}", same_workers.network_snapshot()?.state);
    engine.destroy_http_client("api").await?;
    drop(client);
    drop(same_workers);
    Ok(())
}
```

## Integration guides

- [Awaiting a buffered response](requests_and_responses.md#awaiting-a-buffered-response)
- [Requests and buffered responses](requests_and_responses.md)
- [Owned callback requests and completion handles](callback_requests.md)
- [Streaming and raw SSE data](streaming_and_sse.md)
- [Cancellation and shutdown](lifecycle.md)
- [Engine shutdown and subscription lifetime](../lifecycle.md)
- [Network observations](network_status.md)
- [Client configuration](configuration.md)
- [Request codecs](request_codecs.md)
- [Timeouts and admission](timeouts_and_admission.md)
- [Retries](retry.md)
- [TLS and proxies](tls_and_proxy.md)
- [Cookies](cookies.md)
- [Authentication](authentication.md)
- [Middleware](middleware.md)
- [HTTP/2](http2.md)

## Synchronous application bridge

This offline program creates and explicitly shuts down workers from ordinary
synchronous `main`. `enable_all()` enables the runtime's I/O and time drivers.
Keep this runtime for the lifetime of asynchronous HTTP use; do not create and
drop a temporary runtime around each streamed request. Within an existing Tokio
async function, use `.await` directly instead of nesting `block_on`.

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let client = HttpClient::new(
            HttpClientConfig::new("http://127.0.0.1:8080")?, "sync-application",
        )?;
        client.shutdown().await?;
        println!("HTTP workers stopped");
        Ok::<(), open_net::NetError>(())
    })?;
    Ok(())
}
```

## `OpenNet::create_http_client_with_config()`

### Required features

`http-client`.

### Function signature

```text
pub async fn create_http_client_with_config(&self, thread_name: &str, config: HttpClientConfig) -> Result<HttpClient, NetError>
```

### Steps

Create an engine and HTTP configuration, await creation, then destroy the named client when finished.

### Parameters

`thread_name: &str` is required and is trimmed; `config: HttpClientConfig` is required and moved into the creation job.

### Callback parameters

Not applicable.

### Return values

`Ok(client)` is a shared handle bound to the engine network context. `Err(error)` reports invalid input/configuration, a reserved name, or worker setup failure.

### Errors and behavior

Blank names or NUL are rejected. `ClientAlreadyExists` means the name is reserved, including while another job is creating it. Once the creation job is submitted, dropping the waiting future does not cancel that job; query the registry before reusing the name.

### Example

[Engine-managed clients](#engine-managed-clients) contains a complete offline program calling this method.

## `OpenNet::get_http_client()`

### Required features

`http-client`.

### Function signature

```text
pub fn get_http_client(&self, thread_name: &str) -> Result<HttpClient, NetError>
```

### Steps

Use a previously registered name to retrieve another handle.

### Parameters

`thread_name: &str` is required and is trimmed before lookup.

### Callback parameters

Not applicable.

### Return values

`Ok(client)` clones the registered handle; `Err(error)` reports lookup failure.

### Errors and behavior

`InvalidInput` rejects a blank name, `ClientNotFound` means no slot exists, and `ConnectionClosing` means its slot is not ready for retrieval. For a managed registration this includes declared-but-unused, initializing, closing and raw-client-closed states; this lookup never starts initialization. Use `ManagedHttpClient::get` to initialize it. Clones share capacity and shutdown state. Shutting down a raw client does not release its registry name.

### Example

[Engine-managed clients](#engine-managed-clients) contains a complete offline program calling this method.

## `OpenNet::destroy_http_client()`

### Required features

`http-client`.

### Function signature

```text
pub async fn destroy_http_client(&self, thread_name: &str) -> Result<(), NetError>
```

### Steps

Await destruction outside the client callback; drop remaining application handles afterward.

### Parameters

`thread_name: &str` is required and is trimmed before lookup.

### Callback parameters

Not applicable.

### Return values

`Ok(())` confirms worker cleanup and releases the name; `Err(error)` reports invalid name, registry state or cleanup failure.

### Errors and behavior

`ClientNotFound` means no registration exists; `ConnectionClosing` means a legacy registration is creating/closing. A managed registration instead begins or joins its shared shutdown, including when it has never initialized. Destruction closes admission and cancels accepted work while allowing callbacks to finish. Once submitted, the destruction job continues if its waiting future is dropped. Existing clones remain closed.

### Example

[Engine-managed clients](#engine-managed-clients) contains a complete offline program calling this method.

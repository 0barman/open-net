# Client lifetime, cancellation and shutdown

[HTTP guide](README.md) · [Library overview](../../README.md)

## Required features and example dependencies

Use Rust 1.89 or later and `http-client` (which includes `net-status`).
Each Rust block is a separate consumer program. Create one with `cargo new
http-example`, replace its `src/main.rs` with a block, and use `cargo check`.
Add these dependencies, replacing the local library path.

```toml
[dependencies]
open-net = { path = "/path/to/open-net", default-features = false, features = ["http-client"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

The examples below do not send HTTP requests and need no server. `cargo run`
executes them; network observation examples use the host platform's monitor.
Building an example alone is not a platform or network test.

## Shared ownership

Cloning a HttpClient shares connections, admission capacity, request IDs,
network observation view and shutdown state. Shutting down any clone shuts down
that shared client. Standalone clients begin cancellation/shutdown when their
last worker owner is dropped. An engine registry retains its registered client,
so dropping application clones alone does not destroy the named client.
A completion handle does not retain the client. Prefer explicit shutdown so
that you can await callbacks, resource cleanup and worker exit.

Await `shutdown()` and `shutdown_graceful()` within an active Tokio runtime.
Their worker-join path calls `tokio::task::spawn_blocking`; a generic executor
alone can panic here instead of receiving a `NetError`. A synchronous application
can use the [Tokio runtime bridge](README.md#synchronous-application-bridge).
Keep the runtime alive until cleanup finishes; these methods cannot be safely
used as executor-independent destructors.

Cancellation is local: a server may already have processed a request. It is not
a rollback. Running application callbacks are allowed to finish; network
budgets do not interrupt them. A handler that never returns can prevent completion.

| Action | Effect |
| --- | --- |
| Drop a pending request/get/post future | Requests cancellation; releases waiting admission reservation. |
| Drop a pending send_wait future before acceptance | Drops owned input; no recovery value and no callback. |
| Drop HttpRequestHandle | Does not cancel. |
| Drop a finished() wait | Does not cancel; another wait can observe the cached completion. |
| Drop HttpStreamResponse or its extracted stream | Releases SDK stream resources and capacity. |
| Call cancel/cancel_all | Requests cancellation of targeted registered work; does not close future admission. |
| Call request_shutdown | Closes admission, preserves accepted work, closes network observation view. |
| Call shutdown or shutdown_graceful | Closes admission, cancels registered work and joins workers. |

## `HttpClient::cancel()`

Cancel the operation currently registered under an ID.

### Required features

`http-client`.

### Function signature

```text
pub fn cancel(&self, request_id: HttpRequestId) -> Result<(), NetError>
```

### Steps

Keep a client handle, perform the operation, and await cleanup externally. This example has no active requests; ID 42 demonstrates the unknown-ID no-op.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| request_id | HttpRequestId | Yes | Current operation ID, such as response.request_id or handle.id(). |

### Callback parameters

Not applicable.

### Return values

`Ok(())` means lookup/cancellation processing succeeded; it does not wait for completion. An unknown or retired ID is a successful no-op. `Err(NetError)` reports internal registry failure.

### Errors and behavior

Custom IDs can be reused. An old ID can therefore cancel a newer operation. For callback requests prefer handle.cancel(), whose operation identity cannot change. Cancellation may arrive too late once callback dispatch begins.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig, HttpRequestId};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "lifecycle-example",
    )?;
    client.cancel(HttpRequestId(42))?;
    client.drain().await?;
    client.shutdown().await?;
    Ok(())
}
```

## `HttpClient::cancel_all()`

Cancel accepted requests and currently registered admission waiters.

### Required features

`http-client`.

### Function signature

```text
pub fn cancel_all(&self) -> Result<(), NetError>
```

### Steps

Keep a client handle, perform the operation, and await cleanup externally. This example has no active requests.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

`Ok(())` means cancellation was requested for the current registry snapshot. `Err(NetError)` reports registry failure.

### Errors and behavior

Running callbacks finish normally. Future submissions remain permitted, so other tasks can add work while cancellation runs. This method does not wait for cleanup. Follow with drain() when needed.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "lifecycle-example",
    )?;
    client.cancel_all()?;
    client.drain().await?;
    client.shutdown().await?;
    Ok(())
}
```

## `HttpClient::drain()`

Wait for registered operations and admission reservations to retire.

### Required features

`http-client`.

### Function signature

```text
pub async fn drain(&self) -> Result<(), NetError>
```

### Steps

Keep a client handle, perform the operation, and await cleanup externally. This example has no active requests.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

`Ok(())` means the registry was observed empty after requests/callbacks/reservations retired. `Err(NetError)` reports registry failure.

### Errors and behavior

Does not close admission. Concurrent tasks can add work during or after the wait. Call request_shutdown() first when you need to prevent new admission. An unconsumed stream with no active deadlines can keep drain waiting. Dropping this wait does not cancel active work.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "lifecycle-example",
    )?;
    client.drain().await?;
    client.shutdown().await?;
    Ok(())
}
```

## `HttpClient::request_shutdown()`

Synchronously stop accepting new work without cancelling accepted requests.

### Required features

`http-client`.

### Function signature

```text
pub fn request_shutdown(&self)
```

### Steps

Keep a client handle, perform the operation, and await cleanup externally. This example has no active requests.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Unit `()`; no Result and no completion guarantee.

### Errors and behavior

Idempotent and independent of queue capacity. Accepted HTTP requests and stream bodies remain usable until their normal terminal state. Pending unaccepted admissions fail. Network observation closes. Call drain() to await natural completion, then shutdown() to join already-retired workers.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "lifecycle-example",
    )?;
    client.request_shutdown();
    client.drain().await?;
    client.shutdown().await?;
    Ok(())
}
```

## `HttpClient::shutdown()`

Close admission, cancel current work and wait for shared shutdown completion.

### Required features

`http-client`.

### Function signature

```text
pub async fn shutdown(&self) -> Result<(), NetError>
```

### Steps

Keep a client handle, perform the operation, and await cleanup externally. This example has no active requests.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

`Ok(())` after worker cleanup; `Err(NetError)` reports cancellation registry or worker/observation cleanup failure.

### Errors and behavior

Concurrent callers share the same completion. Dropping one shutdown waiter does not reverse shutdown or allow another waiter to return before callbacks finish. Accepted callbacks still run to completion. The client cannot restart after shutdown. The caller must be inside an active Tokio runtime for worker joins.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "lifecycle-example",
    )?;
    client.shutdown().await?;
    Ok(())
}
```

## `HttpClient::shutdown_graceful()`

Close admission, cancel current work and join the worker lanes.

### Required features

`http-client`.

### Function signature

```text
pub async fn shutdown_graceful(&self) -> Result<(), NetError>
```

### Steps

Keep a client handle, perform the operation, and await cleanup externally. This example has no active requests.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

`Ok(())` after shutdown cleanup; `Err(NetError)` reports registry/worker/observation failure.

### Errors and behavior

Despite its name, the current implementation cancels requests exactly as shutdown() does. For natural completion, use request_shutdown() followed by drain(), then shutdown(). This method cannot restart the client. The caller must be inside an active Tokio runtime for worker joins.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:8080")?, "lifecycle-example",
    )?;
    client.shutdown_graceful().await?;
    Ok(())
}
```

## Finish accepted work naturally

Stop application producers first. Call `request_shutdown()` to close SDK
admission. Continue consuming any accepted streams and allow callbacks to return,
then await `drain()`. Finally call `shutdown()` to await worker joins. When the
client is engine-managed, call `engine.destroy_http_client(name).await` to
release the registry name as well. Calling shutdown before drain cancels remaining
work. Timeout budgets remain in force during natural completion.

Never await a request's own completion handle from its callback. Similarly, do
not await this client's drain/shutdown from a request callback: the callback
itself is part of the completion boundary. A callback holding the last permit
must not await send_wait on those workers. Have an external owner coordinate
cleanup. A synchronous request_shutdown call does not wait on the caller and
can be used to signal closure.

Network observation callbacks use a different lifecycle: HTTP shutdown does not
wait for an already running network callback, so a network callback may initiate
HTTP shutdown. Externally await its Subscription::close() to join that callback.
See [network status](network_status.md).

## Engine registry cleanup

Creation, retrieval and destruction signatures are listed in the
[HTTP introduction](README.md#engine-managed-clients). Destroy cancels the
client, awaits cleanup, then frees its trimmed name. Clones remain handles to a
closed instance. Once a destruction job is submitted, cancelling its waiting
future does not stop cleanup; the name remains reserved until cleanup finishes.
While a slot is creating/closing, get/destroy report ConnectionClosing. A missing
slot reports ClientNotFound. Shutting down a handle alone does not release its name.

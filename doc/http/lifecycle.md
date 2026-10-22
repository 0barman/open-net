# Client lifetime, cancellation and shutdown

[HTTP guide](README.md) · [Library overview](../../README.md)

## Required features and example dependencies

Use Rust 1.89 or later and `http-client` (which includes `net-status`).
Each Rust block is a separate consumer program. Create one with `cargo new
http-example`, replace its `src/main.rs` with a block, and use `cargo check`.
Add these dependencies, replacing the local library path.

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false, features = ["http-client"] }
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

Once an HTTP network operation has a terminal result, final callback queueing and
execution are outside `total_timeout` and retry-report `elapsed`. The terminal
result does not expire while waiting in the callback queue. Completion handles
still wait for the callback and SDK resource cleanup; a callback that never
returns can therefore keep `finished()` pending.

`shutdown()` and `shutdown_graceful()` can be awaited without a caller Tokio
runtime. The SDK owns the cleanup thread: once cleanup is accepted, cancelling
one waiter or stopping its runtime does not abandon workers. Concurrent waiters
observe shared completion. If the operating system cannot start cleanup, the
call returns `RuntimeUnavailable`, retains ownership of the workers and permits
a later shutdown call to retry. Only successful completion confirms retirement.
Requests and open streams retain their separate
[runtime requirements](README.md#start-here).

An engine also has its own [shutdown boundary](../lifecycle.md). Its successful
`shutdown().await` retires registered clients and engine-owned callbacks and
runtime. A standalone HTTP client remains its application's responsibility,
even if it observes that engine's network context.

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
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
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
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
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
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
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
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
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

Concurrent callers share the same completion. Dropping one shutdown waiter does not reverse shutdown or allow another waiter to return before callbacks finish. Accepted callbacks still run to completion. The client cannot restart after shutdown. Worker cleanup is owned by the SDK and does not require a caller Tokio runtime. Never await this client's shutdown from one of its HTTP request callbacks; signal closure there and await it externally.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
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

Despite its name, the current implementation cancels requests exactly as shutdown() does. For natural completion, use request_shutdown() followed by drain(), then shutdown(). This method cannot restart the client. Like shutdown(), its cleanup is independent of the caller's executor and continues if a waiter is cancelled.

### Example

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
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
Polling `Subscription::close()` inside any synchronous subscription callback
first unsubscribes and then returns `InvalidInput`; this includes closing a
different subscription. Use `CallbackContext::unsubscribe()` in the callback
and await retirement externally. See [network status](network_status.md) and
[shared subscription lifetime](../lifecycle.md).

## Engine registry cleanup

Creation, retrieval and destruction signatures are listed in the
[HTTP introduction](README.md#engine-managed-clients). Destroy cancels the
client, awaits cleanup, then frees its trimmed name. Clones remain handles to a
closed instance. Once a destruction job is submitted, cancelling its waiting
future does not stop cleanup; the name remains reserved until cleanup finishes.
While a slot is creating/closing, get/destroy report ConnectionClosing. A missing
slot reports ClientNotFound. Shutting down a handle alone does not release its name.
These creating/closing errors describe legacy registrations. For a managed
registration, destroy joins its shared shutdown; get returns only an already
initialized, open client.

## Choose a client ownership model

All three options ultimately expose the same `HttpClient` request API. Choose
based on who creates the workers and who must confirm cleanup, not on which
HTTP verbs the application uses.

| Choice | When workers start | Sharing and name lifetime | How to finish |
| --- | --- | --- | --- |
| `HttpClient::new` / `new_with_network_status` | During construction | Clone the client; its label is not an engine registry name. | Await the client's `shutdown`. Engine shutdown does not own these HTTP workers. |
| `OpenNet::create_http_client_with_config` | During the asynchronous factory call | The engine reserves the name. `get_http_client` retrieves clones; duplicate creation fails. | `destroy_http_client` joins workers and releases the name. Raw `shutdown` alone does not release this registration. |
| `OpenNet::managed_http` | First `ManagedHttpClient::get` | Registration reserves a fixed configuration and name without starting HTTP workers. Clone the managed handle to share initialization and permanent closure. | `begin_shutdown` signals cancellation; `shutdown` joins cleanup and releases the name. `destroy_http_client` joins the same managed cleanup. |

For a managed registration, `OpenNet::get_http_client` only retrieves an already
initialized, open client. It does not start initialization; use the managed
handle's `get` for that. After an initialization failure a later `get` retries
the same fixed configuration. After closure, no `get` can reopen that handle.
Register a new handle after successful retirement to use new settings.

Keep the engine alive while using managed clients: their handles do not retain
it. Closing the raw `HttpClient` returned by `get` also permanently closes access
through the managed handle. Application clones do not form independent shutdown
boundaries. See [engine shutdown](../lifecycle.md) for closing all registrations
together and for resources that remain application-owned.

## `OpenNet::managed_http()`

Register one fixed configuration and reserve a name before creating HTTP workers.

### Required features

`http-client`.

### Function signature

```text
pub fn managed_http(&self, name: &str, config: HttpClientConfig) -> Result<ManagedHttpClient, NetError>
```

### Steps

Register each application role once. Clone its managed handle into consumers,
then call `get().await` when that role first needs HTTP. Coordinate shutdown from
an external owner. See the [complete program](#managed-client-example).

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| name | &str | Yes | Trimmed engine registry name, shared with legacy named clients. |
| config | HttpClientConfig | Yes | Fixed configuration owned by this registration. |

### Callback parameters

Not applicable.

### Return values

`Ok(ManagedHttpClient)` reserves the name and owns a cloneable lifecycle handle.
Import the type from `open_net::api::http`.

### Errors and behavior

Invalid names/configuration and duplicate registrations return an error.
Registration validates configuration but creates no HTTP workers. TLS transport
setup and other initialization failures are reported by `get`. Registration is
not a lookup or configuration replacement operation. A name remains reserved
until managed cleanup finishes. Keep `OpenNet` alive: managed handles do not
retain their engine, and dropping it closes managed clients, including raw
`HttpClient` clones previously returned by `get`.

### Example

The [managed client example](#managed-client-example) declares business and
storage clients separately.

## `ManagedHttpClient::get()`

### Required features

`http-client`.

### Function signature

```text
pub async fn get(&self) -> Result<HttpClient, NetError>
```

### Steps

Await the first initialization, then use the returned client with the existing
HTTP request API. Repeated calls return clones sharing that client's workers.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

An initialized `HttpClient`, or its initialization/lifecycle error.

### Errors and behavior

Concurrent callers join one initialization attempt and observe that attempt's
result. After failure, a later call explicitly starts a new attempt using the
same configuration. Dropping a waiter or its application runtime does not cancel
SDK-owned initialization. After shutdown begins, `get` returns `Closed` and never
reinitializes the registration. Closing a returned raw client with
`request_shutdown` or `shutdown` also prevents subsequent managed access;
`cancel_all` only cancels requests and does not close the client.

The managed wait does not require a caller Tokio runtime. HTTP requests still
have their existing [runtime requirements](README.md#start-here). Initialization
of another named client is independent of this client's shutdown wait.

### Example

The [managed client example](#managed-client-example) lazily initializes each role.

## `ManagedHttpClient::begin_shutdown()`

### Required features

`http-client`.

### Function signature

```text
pub fn begin_shutdown(&self)
```

### Steps

Call this synchronously to close admission and request cancellation. Await
`shutdown` from an external owner when cleanup must be complete.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Unit. Repeated calls are harmless.

### Errors and behavior

Closes managed `get` immediately and, for an initialized client, closes admission
and requests cancellation before returning. Raw clones cannot bypass that gate.
If initialization is in progress, its result is cleaned up without being exposed
as a usable client. Cleanup runs independently on SDK-owned tasks; this method
does not wait for callbacks or workers. A callback may call this method to signal
closure, but must not wait for its own shutdown completion.

### Example

The [managed client example](#managed-client-example) begins closing business
HTTP while storage remains usable.

## `ManagedHttpClient::shutdown()`

### Required features

`http-client`.

### Function signature

```text
pub async fn shutdown(&self) -> Result<(), NetError>
```

### Steps

Stop application producers, begin shutdown, then await it from an external owner.
Observe cleanup errors even if the earlier application operation failed.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

The shared cleanup result, after request callbacks and HTTP workers have finished.

### Errors and behavior

Begins shutdown if needed. Concurrent waiters share completion. Dropping a waiter
or its application runtime does not cancel cleanup; the wait itself does not
require caller Tokio context. Once cleanup finishes, the name can be registered
again. Old managed/raw handles stay closed and cannot close a replacement with
the same name. `OpenNet::destroy_http_client` also joins managed cleanup.
If cleanup fails without confirming worker retirement, the error is retained
and the name stays reserved so that a replacement cannot overlap live workers.

This boundary covers HTTP request callbacks and workers. Network subscriptions
have separate lifetimes: retain their handles and await `Subscription::close`
externally. Never await HTTP shutdown inside one of that client's request
callbacks; that callback is itself part of the completion boundary.

### Example

The [managed client example](#managed-client-example) joins both cleanup results.

## Managed client example

This complete program creates workers without sending requests and requires no
server. Each role is registered once. Applications can use
`business.get().await?.get("/health").await` inside their enabled Tokio runtime
to send a request; registration itself performs no network operation.

```rust
use open_net::api::http::HttpClientConfig;
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let net = OpenNet::new()?;
    let business = net.managed_http(
        "business", HttpClientConfig::new("http://127.0.0.1:8080")?,
    )?;
    let storage = net.managed_http(
        "storage", HttpClientConfig::new("http://127.0.0.1:8081")?,
    )?;
    let work: Result<()> = async {
        let _business_client = business.get().await?;
        business.begin_shutdown();
        let _storage_client = storage.get().await?;
        Ok(())
    }.await;
    business.begin_shutdown();
    storage.begin_shutdown();
    let business_cleanup = business.shutdown().await;
    let storage_cleanup = storage.shutdown().await;
    // Await every role even if an earlier operation failed.
    work.and(business_cleanup).and(storage_cleanup)
}
```

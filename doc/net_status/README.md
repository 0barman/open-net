# NetStatusClient integration guide

[Back to the library overview](../../README.md)

`NetStatusClient` observes host network conditions. Use it to display network state, react to changes, or inspect local IPv4/IPv6 capability. `Available` does not guarantee that your server, DNS service, or an Internet endpoint can be reached. Always handle failures from the actual network operation.

Reachability describes the platform's network evidence. `Unavailable` does not
prove that every LAN peer or remote endpoint is unreachable. An absent
`snapshot.reachability` means there is no current observation; `Stopped`,
`Starting`, `Failed`, and `Closed` are monitor lifecycle states, not synthetic
`Unavailable` observations. Keep these distinctions when adapting the SDK to a
two-state application API.

Choose the observation contract your application needs:

| Need | Contract |
| --- | --- |
| Display the latest state | `snapshot()` reads cached state; `subscribe()` and `on_change()` deliver state updates and can coalesce intermediate changes. |
| Handle each committed reachability transition | `subscribe_events()` and `on_event()` preserve a bounded ordered prefix. Overflow ends that raw subscription; register again to obtain a new baseline. |
| Continue observing after event-queue overflow | [`on_recoverable_event()`](#netstatusclienton_recoverable_event) automatically installs a new baseline under the same subscription ID and reports `Reconciled`; missed events are not replayed. |
| Request a fresh Windows observation | `refresh()` samples reachability and the network name while the calling view is `Running`; `refresh_reachability()` skips the name query. IP-stack capability remains the current shared netwatch value. |
| Query from an existing synchronous interface | `refresh_blocking()` and `refresh_reachability_blocking()` expose the same query results and budgets while blocking the calling thread. |
| Inspect local LAN IPv4 addresses | [`query_lan_ipv4()`](#netstatusclientquery_lan_ipv4) makes a fresh synchronous OS query, including while monitoring is stopped. It returns candidates and an optional recommendation without changing network observations. |

The event guarantee begins at an SDK commit. An operating system or its monitor can merge notifications before the SDK samples them, so events are not a complete history of physical cable, Wi-Fi, VPN, or sleep/resume changes.

## Required features and prerequisites

Use Rust 1.89 or later and a supported platform: macOS, Windows, or Ubuntu with Linux kernel 5.0 or later. Native build tools must be available; the macOS monitor includes a C component. Validate monitoring on the platforms and kernels your application supports.

Create a consumer project with `cargo new network-observer`, then use this dependency configuration. Replace the path with the location of the `open-net` package directory in your checkout, which contains its `Cargo.toml`:

```toml
[dependencies]
# Use the local source checkout.
open-net = { path = "../open-net-test/open-net", default-features = false, features = ["net-status"] }
tokio = { version = "1.53.1", features = ["macros", "rt-multi-thread", "sync", "time"] }
```

Every Rust block below is a separate complete `src/main.rs`. Copy one block at a time and run `cargo run`. Examples that call `start()` inspect the real operating system and can fail if monitoring cannot initialize. They need no application server. Compile checks alone do not prove live monitoring on every platform.

`net-status` enables monitoring and LAN discovery. The default `ws-client` feature also enables `net-status`; `default-features = false` avoids opting into WebSocket through this dependency declaration. Cargo features are additive across dependencies: another dependency can still enable `ws-client`.

In the examples, `async fn` defines work that can suspend; `.await` waits without blocking the executor's worker thread. `#[tokio::main]` supplies an executor for the application. The library also manages its own workers. `?` returns an error to the caller rather than continuing with an invalid result. `open_net::Result<T>` means `Result<T, NetError>`.

A Rust value owns its resources until it is moved or dropped. `client.clone()` creates another handle to the **same named view**; it does not create independent monitoring. A returned subscription must stay alive for observation to continue. Keep the engine alive while using its clients; dropping `OpenNet` closes their observations.

## Quick start

Create an engine, create a named view, start it, read its snapshot, and destroy it. Save the start result before cleanup so that a failed startup still reaches the explicit destruction step. Expect `Running` plus actual host observations after a successful start; the exact reachability and IP stack depend on your machine.

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("status").await?;
    let started = client.start().await;
    match &started {
        Ok(snapshot) => println!("First observation: {snapshot:?}"),
        Err(error) => eprintln!("Monitor startup failed: {error}"),
    }
    if started.is_ok() {
        println!("Cached snapshot: {:?}", client.snapshot()?);
    }
    net.destroy_net_status_client("status").await?;
    started?;
    Ok(())
}
```

## Create, retrieve, and destroy a client

Only `OpenNet` constructs a `NetStatusClient`; its `new` constructor is internal. Each distinct name selects an independent lifecycle view over the engine's shared monitor. Names are trimmed and compared case-sensitively within the network-status registry of that engine. Creating `" status "` and then `"status"` is a duplicate. Empty names are invalid. A NUL character is rejected during creation.

| Operation | Named view | Subscriptions | Name reservation |
| --- | --- | --- | --- |
| Create | Initially `Stopped`, with no observation | Can register before starting | Reserved |
| `start()` | Acquires monitoring demand and awaits initialization | Existing observers continue | Kept |
| `stop()` | Becomes `Stopped`, clears current observation | Kept for restart | Kept |
| `shutdown()` | Permanently `Closed` | Final state, then end | Kept |
| Destroy through `OpenNet` | Permanently `Closed` for all clones | Final state, then end | Released after cleanup |
| Drop `OpenNet` | All its views become `Closed` | Terminate | Engine no longer exists |

Stopping one named view does not stop another active view. The underlying operating-system monitor can stay active while another consumer still needs it. A newly created client reusing a destroyed name is a new view; old handles remain closed.

The table describes a single client. To permanently close the entire engine and
wait for its registered clients, monitoring and SDK callback workers, use
[`OpenNet::begin_shutdown()` / `OpenNet::shutdown()`](../lifecycle.md).
Client shutdown keeps the name registered; engine shutdown closes admission for
all new work on that engine and cannot be reversed.

## OpenNet::new()

Create the engine with default settings before creating any named client.

### Required features

No feature is required for the constructor itself. This example uses `net-status`.

### Function signature

```text
pub fn new() -> Result<Self, NetError>
```

### Steps

Call the constructor, retain the engine, and create clients from it.

### Parameters

None.

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(net)` | `OpenNet` | The engine is ready to manage clients; monitoring has not been started. |
| `Err(error)` | `NetError` | Engine initialization failed. |

### Errors and behavior

Runtime or worker creation can fail, including `RuntimeUnavailable` or operating-system errors. Default async and sync submission queue capacities are each 128. Dropping the engine closes all its named views.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("status").await?;
    println!("{:?}", client.snapshot()?.state);
    net.destroy_net_status_client("status").await?;
    Ok(())
}
```

## OpenNet::new_with_config()

Set engine worker and queue limits before creation.

### Required features

No feature is required for the constructor itself. This example uses `net-status`.

### Function signature

```text
pub fn new_with_config(config: OpenNetConfig) -> Result<Self, NetError>
```

### Steps

Build `OpenNetConfig`, optionally validate it, then move it into the constructor.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `config` | `OpenNetConfig` | Yes | Immutable settings used to construct the engine. |

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(net)` | `OpenNet` | An engine using the supplied settings. |
| `Err(error)` | `NetError` | Invalid configuration or engine initialization failure. |

### Errors and behavior

`InvalidConfig` is returned before workers start if limits are invalid. Values cannot be changed on an existing engine. The setters below take `self` and return `Self`, so they can be chained; validation is deferred until `validate()` or engine creation.

| Configuration API | Default and accepted values |
| --- | --- |
| `OpenNetConfig::default() -> OpenNetConfig` | Runtime worker count is unspecified; queue capacities are 128. |
| `with_runtime_worker_threads(self, threads: usize) -> Self` | Explicit count must be 1 through 256. If omitted, Tokio defaults, including its worker-count environment setting, apply. This does not cap every I/O, callback, or blocking thread. |
| `with_async_queue_capacity(self, capacity: usize) -> Self` | 1 through `tokio::sync::Semaphore::MAX_PERMITS`; counts pending tasks, not bytes. |
| `with_sync_queue_capacity(self, capacity: usize) -> Self` | Same range; the queue is still asynchronous internally, while callers wait for a result. |
| `runtime_worker_threads(&self) -> Option<usize>` | Explicit count, or `None` for the runtime default. |
| `async_queue_capacity(&self) -> usize` | Configured async capacity. |
| `sync_queue_capacity(&self) -> usize` | Configured sync capacity. |
| `validate(&self) -> Result<(), NetError>` | `Ok(())` for valid settings; `InvalidConfig` otherwise. |

The two remaining inherent methods, `with_network_config(self, config:
NetworkConfig) -> Self` and `network_config(&self) -> &NetworkConfig`, require
`ws-client`. They set or borrow the default WebSocket network policy and do not
configure this monitor or HTTP clients. They are outside this NetStatus guide
and are unavailable under its minimal dependency declaration. See the
[WebSocket guide](../websocket/README.md) for transport configuration.

### Example

```rust
use open_net::{OpenNet, OpenNetConfig, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let config = OpenNetConfig::default()
        .with_runtime_worker_threads(2)
        .with_async_queue_capacity(256)
        .with_sync_queue_capacity(128);
    config.validate()?;
    assert_eq!(config.runtime_worker_threads(), Some(2));
    assert_eq!(config.async_queue_capacity(), 256);
    assert_eq!(config.sync_queue_capacity(), 128);
    let net = OpenNet::new_with_config(config)?;
    let client = net.create_net_status_client("configured").await?;
    println!("{:?}", client.snapshot()?.state);
    net.destroy_net_status_client("configured").await?;
    Ok(())
}
```

## OpenNet::create_net_status_client()

Reserve a name and create a stopped client without starting the detector.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub async fn create_net_status_client(
    &self,
    thread_name: &str,
) -> Result<NetStatusClient, NetError>
```

### Steps

Choose a unique name, await creation, then call `start()` when observations are needed.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `thread_name` | `&str` | Yes | Trimmed nonempty name, without NUL characters. Despite its name, this parameter identifies a client view, not a dedicated OS thread. |

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(client)` | `NetStatusClient` | A cloneable view in `Stopped` state. |
| `Err(error)` | `NetError` | No new client was created. |

### Errors and behavior

An empty trimmed name returns `InvalidInput`; a NUL character returns `InvalidConfig`. A reserved name returns `ClientAlreadyExists`, including while its old client is being destroyed. Infrastructure errors can also propagate. Creation does not initialize monitoring. Dropping the returned handle alone does not destroy the engine's registered client.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("  status  ").await?;
    assert!(matches!(client.snapshot()?.state, open_net::MonitorState::Stopped));
    net.destroy_net_status_client("status").await?;
    Ok(())
}
```

## OpenNet::get_net_status_client()

Retrieve another handle to an existing view.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn get_net_status_client(&self, thread_name: &str) -> Result<NetStatusClient, NetError>
```

### Steps

Use the creation name after trimming. Keep the engine alive while using the handle.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `thread_name` | `&str` | Yes | Name in this engine's network-status registry. |

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(client)` | `NetStatusClient` | A shared handle to the same view, including an explicitly shut-down view whose name remains reserved. |
| `Err(error)` | `NetError` | Lookup failed. |

### Errors and behavior

An empty name returns `InvalidInput`; an unknown name returns `ClientNotFound`; a name being destroyed returns `ConnectionClosing`. Internal lock failures may propagate. Retrieving or cloning a handle never starts monitoring. Lifecycle operations through any clone affect all handles to this view.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let first = net.create_net_status_client("status").await?;
    let second = net.get_net_status_client("status")?;
    second.shutdown().await?;
    assert!(matches!(first.snapshot()?.state, open_net::MonitorState::Closed));
    net.destroy_net_status_client("status").await?;
    Ok(())
}
```

## OpenNet::destroy_net_status_client()

Permanently close a named view and release its name after cleanup.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub async fn destroy_net_status_client(&self, thread_name: &str) -> Result<(), NetError>
```

### Steps

Finish application work, await destruction, and only then reuse the name if needed. Separately close callback handles when callback retirement must be awaited.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `thread_name` | `&str` | Yes | Name of an existing network-status client. |

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(())` | `()` | The view is closed, its monitoring cleanup completed, and its name is released. |
| `Err(error)` | `NetError` | Lookup, destruction, or cleanup failed. |

### Errors and behavior

Empty names return `InvalidInput`, absent names `ClientNotFound`, and simultaneous destruction `ConnectionClosing`. Cleanup or internal errors can propagate. Once destruction has been submitted, cancelling its waiting future does not cancel cleanup; the name remains reserved until cleanup completes. All old clones stay closed even if the name is reused. Successful destruction is not a substitute for `Subscription::close()` when an already running callback must finish.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let old = net.create_net_status_client("status").await?;
    net.destroy_net_status_client("status").await?;
    assert!(matches!(old.snapshot()?.state, open_net::MonitorState::Closed));
    let fresh = net.create_net_status_client("status").await?;
    assert!(matches!(fresh.snapshot()?.state, open_net::MonitorState::Stopped));
    net.destroy_net_status_client("status").await?;
    Ok(())
}
```

## NetStatusClient::start()

Start this view and wait for the first coherent network observation.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub async fn start(&self) -> Result<NetworkSnapshot>
```

### Steps

Create a client, await `start()`, inspect its returned state and observations, then stop or destroy it when finished.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(snapshot)` | `NetworkSnapshot` | Normally the first `Running` observation; concurrent `stop()` may instead complete the wait with `Stopped`. |
| `Err(error)` | `NetError` | Initialization, lifecycle, or infrastructure failure. |

### Errors and behavior

Repeated starts reuse the active shared monitor and concurrent starts share pending initialization. A recoverable initialization failure is exposed by `Err(error)` and a `Failed(error)` state; subscriptions remain usable and another explicit `start()` can retry. Failure to create the detector is classified `RuntimeUnavailable`; platform reads can return `Io`. These are examples, not an exhaustive error list.

A closed or destroyed view returns `Closed`. If `shutdown()` races startup it can terminate the wait with `Closed`; if `stop()` races it, inspect the successful snapshot's state rather than assuming `Running`. Dropping a started wait does not itself request `stop()`; perform explicit cleanup. An `Unavailable` observation is a successful monitor result, not a startup error.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("status").await?;
    let result = client.start().await;
    match &result {
        Ok(snapshot) => println!("{:?}: {:?}", snapshot.state, snapshot.reachability),
        Err(error) => eprintln!("Startup: {:?}: {error}", error.kind()),
    }
    net.destroy_net_status_client("status").await?;
    result?;
    Ok(())
}
```

## NetStatusClient::snapshot()

Read a coherent cached state without waiting for a new observation.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn snapshot(&self) -> Result<NetworkSnapshot>
```

### Steps

Read the cache, check `state`, and only interpret observation fields when they are `Some`.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(snapshot)` | `NetworkSnapshot` | An owned clone of the current local snapshot. |
| `Err(error)` | `NetError` | An internal/source failure prevents a valid read. |

### Errors and behavior

Reading never starts monitoring or actively refreshes the operating system. A new or stopped view has `Stopped` with absent observation fields, even if another named view is running. After normal shutdown or engine drop the final `Closed` snapshot remains readable. A recoverable monitor failure is normally returned as `Ok(snapshot)` whose state is `Failed(error)`; a permanent observation infrastructure failure can instead make this call return `Err`. `None` is unknown or inactive, not evidence of being offline.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("cache").await?;
    let snapshot = client.snapshot()?;
    assert!(matches!(snapshot.state, open_net::MonitorState::Stopped));
    assert!(snapshot.reachability.is_none());
    client.shutdown().await?;
    assert!(matches!(client.snapshot()?.state, open_net::MonitorState::Closed));
    net.destroy_net_status_client("cache").await?;
    Ok(())
}
```

## NetStatusClient::query_lan_ipv4()

Collect local IPv4 candidates and an optional recommendation using fresh
interface, link and default-route evidence. This helps choose an address to
show to a LAN peer; it does not prove that the peer can reach that address.

### Required features

`net-status`; use the dependency configuration at the top of this page.
The query is supported on macOS, Windows and Linux. Unlike Windows-only
`refresh()`, it does not require running network monitoring.

### Function signature

```text
pub fn query_lan_ipv4(&self) -> Result<open_net::network::LanIpv4Snapshot>
```

### Steps

Create or retrieve a named client, call the method, inspect completeness and
diagnostics, then handle `recommended()` as an `Option`. `start()` is optional.
Use a blocking worker from async or UI code because the OS query blocks its
calling thread. If no client is needed, use
[`network::lan_ipv4_snapshot()`](../lan_ipv4.md#networklan_ipv4_snapshot)
directly without creating an engine.

### Parameters

Only `&self`, a borrowed client handle. There is no timeout, destination address
or probe URL parameter.

### Callback parameters

Not applicable. The method does not register a callback, change
`NetworkSnapshot`, advance observation revisions or emit network events.

### Return values

| Value | Meaning |
| --- | --- |
| `Ok(snapshot)` with candidates | An owned inventory, including candidates excluded by the default recommendation policy. |
| `Ok(snapshot)` with no candidates | IPv4 enumeration succeeded but returned no addresses. |
| `snapshot.recommended() == None` | No default recommendation was found; do not invent a loopback or arbitrary fallback address. |
| `snapshot.completeness() == Partial` | Addresses were collected but some auxiliary evidence failed; inspect diagnostics. A recommendation can still exist. |
| `Err(error)` | The client/engine is closed, address enumeration failed, or admission failed. The original enumeration I/O error is retained as an error source. |

See the [LAN fields and selection guide](../lan_ipv4.md) for return types and
the difference between unknown evidence and a negative fact.

### Errors and behavior

Stopped, Starting, Running and ordinary monitoring Failed states permit the
query. Closed or destroyed clients and their clones, and a closing engine,
reject new queries with `Closed`. An already admitted query can finish normally;
client/engine shutdown does not wait for this caller's read-only OS query.

Internet access is unnecessary. The query neither starts monitoring nor tests
remote connectivity. Existing network events do not cover every LAN-only
address change; applications needing DHCP/interface tracking must arrange fresh
sampling. An error or Partial result is not proof that a previously healthy
application endpoint disappeared. Listener rebinding and advertisement updates
remain application responsibilities.

`network::preferred_lan_ipv4()` uses subnet-based selection. Use this method or
`lan_ipv4_snapshot()` for selection based on native interface and route evidence.

### Example

This complete program queries a stopped client on a blocking worker. It still
attempts engine cleanup if the query or blocking task fails.

```rust
use open_net::OpenNet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("lan-query").await?;
    let queried = tokio::task::spawn_blocking(move || client.query_lan_ipv4()).await;
    let cleanup = net.shutdown().await;
    let snapshot = queried??;
    cleanup?;
    println!("Collection: {:?}", snapshot.completeness());
    for diagnostic in snapshot.diagnostics() {
        eprintln!("Collection detail: {diagnostic}");
    }
    match snapshot.recommended() {
        Some(candidate) => println!(
            "Candidate for LAN use: {} on {} ({:?})",
            candidate.address(), candidate.interface_name(), candidate.recommendation_reason()
        ),
        None => println!("No default LAN recommendation; inspect candidates explicitly"),
    }
    Ok(())
}
```

## NetStatusClient::subscribe()

Receive the current state followed by later states through an owned receiver.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn subscribe(&self) -> Result<StateReceiver<NetworkSnapshot>>
```

### Steps

Keep the returned receiver, call `recv().await` repeatedly while observing, and unsubscribe or drop it when finished.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(receiver)` | `StateReceiver<NetworkSnapshot>` | A new subscription with its initial snapshot pending. |
| `Err(error)` | `NetError` | Subscription registration failed. |

### Errors and behavior

Subscription does not start monitoring. The initial snapshot is preserved; subsequent changes use the latest state, so slow consumers can skip intermediate revisions. This is a state feed, not a complete event log. `stop()` leaves it alive for later `start()`. Normal shutdown delivers the final `Closed` snapshot and then `Ok(None)`; subscribing to a normally closed view also permits reading its final state.

The current implementation allows 1,024 state subscriptions per named view, including state callbacks. Event subscriptions have a separate limit of 1,024, including event callbacks. Exhausting this limit yields `SubscriptionLimitReached`; allocation or identifier exhaustion can yield `ResourceExhausted`. Drop or unsubscribe to release capacity. Source failures are delivered through `recv()` as `Err`; a recoverable `Failed` monitor state is an ordinary snapshot.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("events").await?;
    let mut receiver = client.subscribe()?;
    println!("Initial: {:?}", receiver.recv().await?);
    client.shutdown().await?;
    while let Some(snapshot) = receiver.recv().await? {
        println!("State: {:?}", snapshot.state);
    }
    net.destroy_net_status_client("events").await?;
    Ok(())
}
```

## NetStatusClient::on_change()

Receive the same state feed through a synchronous closure dispatched on the shared callback pool.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn on_change<F>(&self, callback: F) -> Result<Subscription>
where
    F: Fn(CallbackContext, Result<NetworkSnapshot>) + Send + Sync + 'static,
```

### Steps

Create the callback, retain its returned handle, and call `close().await` outside the callback when you need to wait for its retirement.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `callback` | `F` | Yes | Called with a context and state result. `Fn` can be invoked repeatedly; use thread-safe interior mutability for shared application state. |

### Callback parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `context` | `CallbackContext` | Subscription identity and a way to unsubscribe from this callback. |
| `result` | `open_net::Result<NetworkSnapshot>` | `Ok(snapshot)` is a state value, including `Failed(error)` or `Closed`; `Err(error)` is a terminal subscription/source infrastructure failure. |

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(subscription)` | `Subscription` | Owns the callback registration; dropping it unsubscribes. |
| `Err(error)` | `NetError` | Registration or callback-pool startup failed. |

### Errors and behavior

`Send + Sync` permits the callback to cross threads safely. `'static` means captured references cannot borrow short-lived stack values; `move` owned data or an `Arc` into the closure. The closure returns `()`, not a future. Callbacks run on the engine's shared callback pool with its runtime context entered; they are not invoked on the caller's thread or a UI thread. Keep them short and dispatch UI work to the application's UI queue.

The same subscription is delivered serially; different registrations can execute independently. The initial callback can run before `on_change()` returns, so use `CallbackContext` for self-unsubscription. Registration errors include subscription limits and runtime/dispatch failures. A panic retires the callback and is observable as `CallbackPanicked` through `close()`. Do not block a callback waiting for itself to retire. Stopping the client preserves this registration.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("callbacks").await?;
    let subscription = client.on_change(|context, result| {
        match result {
            Ok(snapshot) => println!("{}: {:?}", context.id(), snapshot.state),
            Err(error) => eprintln!("Observation failed: {error}"),
        }
    })?;
    let started = client.start().await;
    client.shutdown().await?;
    subscription.close().await?;
    net.destroy_net_status_client("callbacks").await?;
    started?;
    Ok(())
}
```

## NetStatusClient::subscribe_events()

Receive each committed reachability transition through an independent bounded queue.

### Required features

`net-status` on all supported platforms. No additional Cargo feature is needed.

### Function signature

```text
pub fn subscribe_events(
    &self,
    options: NetworkEventOptions,
) -> Result<EventReceiver<NetworkEvent>>
```

### Steps

Register before or after `start()`, retain the receiver, and consume `recv().await`.
The first item is an atomic `Initial` baseline. Handle overflow by registering
again and using the new baseline; the missing interval cannot be replayed.

### Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `options` | `NetworkEventOptions` | Per-subscriber item and accounted-byte limits. Defaults are `capacity: 256` and `max_bytes: 256 * 1024`. Initial and lifecycle events use capacity. |

Both limits must be positive and within supported semaphore limits; `max_bytes`
is also limited to `u32::MAX`. Accounted bytes include the event value and owned
network-name bytes; this is not a bound on total process memory. An individual
event must fit, including the initial baseline.

### Callback parameters

Not applicable. Use `on_event()` or convert the receiver with `into_callback()`.

### Return values

| Value | Meaning |
| --- | --- |
| `Ok(receiver)` | An owned `EventReceiver<NetworkEvent>` with its baseline already installed. |
| `Err(error)` | Invalid limits, insufficient space for Initial, subscription quota, or allocation/registration failure. |

### Errors and behavior

Registration does not start monitoring. It atomically captures the baseline and
joins the same commit order as later events: no gap or duplicate is introduced
between Initial and subsequent commits. Each subscriber sees its own ordered
queue. Existing `subscribe()` and `on_change()` continue to coalesce state.

The queue uses a fixed disconnect-on-overflow policy. It first delivers the
retained ordered prefix, then `ReceiveError::Failed(error)` with
`error.kind() == ErrorKind::CallbackOverflow`, and then ends. It does not silently
drop the oldest event or block monitoring. Other subscribers and the shared
monitor continue. A byte limit can overflow even when the item count is below
`capacity`. Create a new subscription to resynchronize; this is a fresh Initial,
not recovery of the lost events. A separately cancelled or dropped subscription
discards pending deliveries rather than promising to drain them.

An individual event larger than `max_bytes` is a separate `ItemTooLarge` error.
If Initial is too large, `subscribe_events()` returns that error immediately.
If a later event is too large, the receiver delivers its retained prefix, then
`ReceiveError::Failed(error)` with `ErrorKind::ItemTooLarge`, and ends. Increase
`max_bytes` before subscribing again; repeatedly using the same undersized limit
cannot resynchronize. The example below treats this error as terminal and
cancels its application work on exit.

Each named view allows 1,024 event registrations, including event callbacks.
This quota is separate from its 1,024 state registrations. Invalid limits return `InvalidConfig`; quota
exhaustion returns `SubscriptionLimitReached`. Allocation or identifier
exhaustion can return `ResourceExhausted`.

`stop()` preserves the subscription and emits a lifecycle event. Restart uses a
new local generation and emits `ObservationReady` for its first usable facts;
it does not replay changes that occurred while this view was stopped. Normal
shutdown emits the final lifecycle event and ends after queued events, unless
this subscriber overflowed or was cancelled. A normally closed view can still
supply an Initial containing its final snapshot and then end.

`EventReceiver::recv()` returns `Ok(Some(event))`, `Ok(None)` at the end, or
`ReceiveError`; cancelling a pending receive does not consume an unreturned
item. `try_recv()` uses `TryReceiveError`, including `Empty` when nothing is
ready. `id()` identifies the registration, `unsubscribe()` detaches it, and
`into_callback()` transfers ownership to a `Subscription`. Dropping either
owner releases its queue. This feed never selects the generic receiver's
DropOldest policy, so overflow is a failure rather than `Lagged` recovery.

### Example

The [network_events example](../../examples/network_events.rs) observes the host
for 15 seconds and keeps one cancellable application task. `Unavailable` cancels
old work; a committed transition to `Available` starts recovery work.
`Initial` and `ObservationReady` reconcile the baseline separately, without
claiming a recovery transition. After overflow, it cancels the old work before
subscribing again; the new Initial determines whether to initialize work again.

The placeholder task deliberately has no server address. Replace its body with
your actual connection/request loop, and extend `cancel()` to release application
session and request ownership. Task abortion requests cancellation; an application
that must await cleanup should also await task completion outside this handler.
A host `Available` observation does not guarantee that reconnection will succeed.

```rust
use open_net::error::{ErrorKind, ReceiveError};
use open_net::{
    MonitorState, NetworkEvent, NetworkEventKind, NetworkEventOptions, NetworkSnapshot,
    NetworkStatus, OpenNet, Result,
};
use std::time::Duration;

#[derive(Default)]
struct ConnectionWork {
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ConnectionWork {
    fn cancel(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        // Also clear your application's old session and pending request ownership here.
    }

    fn start_work(&mut self, reason: &'static str) {
        self.cancel();
        self.task = Some(tokio::spawn(async move {
            println!("Starting connection work: {reason}");
            // Replace this placeholder with your connect/reconnect and request loop.
            // Available is only host evidence: handle actual connection failures here.
            std::future::pending::<()>().await;
        }));
    }

    fn establish_baseline(&mut self, snapshot: &NetworkSnapshot) {
        self.cancel();
        if matches!(snapshot.state, MonitorState::Running)
            && snapshot.reachability == Some(NetworkStatus::Available)
        {
            // Initial/ObservationReady can initialize work without claiming recovery.
            self.start_work("establish current baseline");
        }
    }

    fn apply(&mut self, event: NetworkEvent) {
        println!(
            "sequence={} generation={} kind={:?}",
            event.sequence, event.generation, event.kind
        );
        match event.kind {
            NetworkEventKind::Initial | NetworkEventKind::ObservationReady => {
                self.establish_baseline(&event.snapshot);
            }
            NetworkEventKind::ReachabilityChanged {
                current: NetworkStatus::Unavailable,
                ..
            } => {
                println!("Network became unavailable: cancel old connection work.");
                self.cancel();
            }
            NetworkEventKind::ReachabilityChanged {
                current: NetworkStatus::Available,
                ..
            } => {
                self.start_work("recover after a committed availability transition");
            }
            NetworkEventKind::LifecycleChanged => self.cancel(),
            _ => {} // NetworkEventKind is non-exhaustive.
        }
    }
}

impl Drop for ConnectionWork {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("ordered-events").await?;
    let observed = async {
        let options = NetworkEventOptions::default();
        let mut receiver = client.subscribe_events(options)?;
        let mut work = ConnectionWork::default();
        if let Some(initial) = receiver.recv().await? {
            work.apply(initial);
        }
        let started = client.start().await?;
        if !matches!(started.state, MonitorState::Running) {
            return Ok::<(), open_net::NetError>(());
        }
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        while tokio::time::Instant::now() < until {
            match tokio::time::timeout_at(until, receiver.recv()).await {
                Ok(Ok(Some(event))) => work.apply(event),
                Ok(Err(ReceiveError::Failed(error)))
                    if error.kind() == ErrorKind::CallbackOverflow =>
                {
                    // The lost interval can include a disconnection: retire old work first.
                    work.cancel();
                    eprintln!("Event history ended after overflow; awaiting a new Initial.");
                    receiver = client.subscribe_events(options)?;
                }
                Ok(Err(error)) => return Err(error.into()),
                Ok(Ok(None)) | Err(_) => break,
            }
        }
        Ok(())
    }
    .await;
    // Cleanup also runs when startup or observation failed.
    net.destroy_net_status_client("ordered-events").await?;
    observed
}
```

## NetStatusClient::on_event()

Consume the same bounded ordered event feed on the shared callback pool.

### Required features

`net-status` on all supported platforms.

### Function signature

```text
pub fn on_event<F>(
    &self,
    options: NetworkEventOptions,
    callback: F,
) -> Result<Subscription>
where
    F: Fn(CallbackContext, std::result::Result<NetworkEvent, ReceiveError>)
        + Send + Sync + 'static,
```

### Steps

Choose the queue bounds, install a short callback, and retain its `Subscription`.
Dispatch UI changes to your application's UI thread. Use `close().await` outside
the callback when you must await that callback's retirement.

### Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `options` | `NetworkEventOptions` | The same limits and overflow policy as `subscribe_events()`. |
| `callback` | `F` | A synchronous closure returning `()`; captured data must be safe to share across threads. |

### Callback parameters

| Parameter | Meaning |
| --- | --- |
| `context: CallbackContext` | Subscription identity and self-unsubscription. |
| `result: Result<NetworkEvent, ReceiveError>` | A committed event or terminal receive failure. This error type differs from the state callback's `NetError`. |

### Return values

`Ok(Subscription)` owns the callback registration. `Err(NetError)` reports
subscription setup or callback-pool startup failure.

### Errors and behavior

Each registration executes serially; independent registrations may run
concurrently. Initial can arrive before `on_event()` returns. Use the supplied
`CallbackContext` for self-unsubscription. A slow callback can exhaust its
queue and receives the retained prefix followed by the overflow error. Queue
limits do not count work your callback spawns elsewhere.

A callback panic retires the registration and is observable through `close()`.
Dropping the owner or unsubscribing cancels pending deliveries; an already
running callback may finish. `close().await` waits for callback retirement,
not delivery of every queued event. `stop()` keeps this registration alive.

### Example

```rust
use open_net::{NetworkEventOptions, OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("event-callback").await?;
    let subscription = client.on_event(NetworkEventOptions::default(), |context, result| {
        match result {
            Ok(event) => println!("{}: {} {:?}", context.id(), event.sequence, event.kind),
            Err(error) => eprintln!("Event subscription ended: {error}"),
        }
    })?;
    let started = client.start().await;
    let closed = client.shutdown().await;
    let retired = subscription.close().await;
    net.destroy_net_status_client("event-callback").await?;
    started?;
    closed?;
    retired
}
```

## NetStatusClient::refresh()

Request new Windows reachability and network-name samples and commit them to
shared network state. `snapshot()` remains a cheap cache read.

### Required features

`net-status`. The method is available on all platforms; active refresh is
implemented on Windows. After timeout and local-lifecycle validation, other
platforms return `Unsupported`.

### Function signature

```text
pub async fn refresh(&self, timeout: Duration) -> Result<NetworkRefreshResult>
```

### Steps

Start the named client and check its `Running` state. Await `refresh()` with a
positive timeout, inspect the sampling result, and handle errors independently
from network unavailability.

Use `refresh_reachability()` when only Windows' Internet-connectivity observation is needed. It uses
an independent bounded worker which never runs network-name queries. A failed,
slow, or stuck name query therefore cannot block this operation. The returned
name is the latest cached value, identified by `CachedObservation`; it is not
claimed as a newly queried SSID. Lifecycle, cancellation and timeout rules are
the same as for `refresh()`.

### Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `timeout` | `std::time::Duration` | Positive total budget covering queue admission, system queries, and commit. It is not restarted for each stage. |

### Callback parameters

Not applicable. Successful changed observations also reach existing state and
event subscriptions through the shared commit path. Successful refresh means
the sample was committed; it does not wait for subscription callbacks or for
application work those callbacks dispatch.

### Return values

| Value | Meaning |
| --- | --- |
| `Ok(result)` | New system queries completed, and their result was accepted for this view's current generation. `Unavailable` can be a successful result. |
| `Err(error)` | The request did not produce a valid committed observation. Cached data is not returned as fresh success. |

### Errors and behavior

The calling named view itself must be `Running`. Another named
client, HTTP observer, or WebSocket consumer keeping the shared monitor running
does not satisfy this condition. A new, stopped, starting, or failed view returns
`NotRunning`; a closed view returns `Closed`. A zero or unsupported timeout
returns `InvalidInput`. Deadline expiry returns `TimedOut`; an invalidated
request or generation returns `Cancelled`. System-query failures retain their
cause as `Io` with `NetworkMonitor` stage. Timeout and local-lifecycle validation
precede the platform check, so non-Windows calls can first return `InvalidInput`,
`NotRunning`, or `Closed`; a valid running request returns `Unsupported`.

Each sampling lane admits at most 16 outstanding manual queries, including a
running query and cancelled queries whose blocking work has not yet retired.
Further requests return `QueueFull`. Background monitoring has separate admission
capacity and does not consume these manual-query slots.

Every accepted full `refresh()` samples Windows Network List Manager reachability and
queries the network name again. An active Wi-Fi SSID is preferred; a freshly
queried connected NLM name is the fallback. A successful absence is represented
by `None`. A query failure is not a disconnection and does not clear the last
valid state or manufacture an `Unavailable` event. If a name-query fault is
bypassed by a successful fresh fallback, `network_name_diagnostics` retains it.

Reachability and name queries form a sampling window, not an atomic operating
system snapshot. `ip_stack_source` is `BackgroundObservation`: `ip_stack` remains
the latest background netwatch capability and is not freshly measured by this
operation. A successful result with unchanged values has a new `sample_id` and
sampling window, while `snapshot.revision` and `snapshot.observed_at` can remain
unchanged. Do not infer request freshness from the snapshot revision alone.
Reachability and name have independent sampling watermarks. If a full query's
name finishes after a newer reachability query, its fresh name is merged without
rolling reachability back. `sampled_reachability` is the result of this query's
own NLM call; `snapshot.reachability` may retain a later observation. A newer
name sample supersedes an older full query with `Cancelled`; a newer
reachability sample similarly supersedes an older reachability-only query.

The active query and background monitor share one ordered commit path. Active
views observe the same new source facts, using their own local revisions;
stopped views stay stopped. Dropping a pending refresh invalidates its
uncommitted result. Deadline expiry, stop/restart, shutdown, and engine closure
also prevent late results from committing. A blocking COM query cannot be
forcibly interrupted: it runs on a dedicated worker rather than blocking a Tokio
worker, may finish later, and its expired result is rejected. Shutdown does not
wait for an uninterruptible COM call; its worker releases resources after it
returns. The `netsh` subprocess is terminated and reaped when its query is cancelled or
expires. These cleanup limits do not extend the successful query contract.

The timeout is a validity budget for admission, sampling, and commit, not a hard
wall-clock return deadline. Expired work cannot commit a late result, but lock
contention, thread scheduling, and cleanup can delay when the caller regains control. The blocking
methods use the same budget and do not automatically return `snapshot()` after
a query failure.

### Example

The runnable [network_refresh example](../../examples/network_refresh.rs) also
prints the sampling window, sampled and merged reachability, name provenance,
cached-IP provenance, and fallback diagnostics. Pass `--blocking` to use the
synchronous API, and `--reachability-only` to skip the name query. From the
repository root:

```bash
cargo run --manifest-path open-net/Cargo.toml --no-default-features --features net-status --example network_events
cargo run --manifest-path open-net/Cargo.toml --no-default-features --features net-status --example network_refresh
cargo run --manifest-path open-net/Cargo.toml --no-default-features --features net-status --example network_refresh -- --blocking
cargo run --manifest-path open-net/Cargo.toml --no-default-features --features net-status --example network_refresh -- --blocking --reachability-only
```

```rust
use open_net::error::ErrorKind;
use open_net::{MonitorState, OpenNet, Result};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("refresh").await?;
    let queried = async {
        let started = client.start().await?;
        if !matches!(started.state, MonitorState::Running) {
            return Ok::<(), open_net::NetError>(());
        }
        match client.refresh(Duration::from_secs(3)).await {
            Ok(result) => println!(
                "Sample {} ({:?} to {:?}): sampled={:?}, merged={:?}",
                result.sample_id, result.sample_started_at, result.sampled_at,
                result.sampled_reachability, result.snapshot.reachability
            ),
            Err(error) if error.kind() == ErrorKind::Unsupported => {
                println!("Fresh queries require Windows; cached state: {started:?}");
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }.await;
    net.destroy_net_status_client("refresh").await?;
    queried
}
```

## NetStatusClient::refresh_reachability()

Query fresh Windows reachability without waiting for a network-name query.

### Required features

`net-status`. Active queries require Windows; after timeout and lifecycle
validation, a valid running request on another platform returns `Unsupported`.

### Function signature

```text
pub async fn refresh_reachability(&self, timeout: Duration) -> Result<NetworkRefreshResult>
```

### Steps

Start this named view, confirm `Running`, and await the method. Use it when the
network name does not need a fresh query.

### Parameters

`timeout: Duration` is a positive validity budget covering admission, sampling,
and commit, with no per-stage reset or hard wall-clock return guarantee.

### Callback parameters

None. Changed observations use existing subscriptions; success does not wait
for their callbacks to complete.

### Return values

`Ok(NetworkRefreshResult)` includes the fresh `sampled_reachability` and merged
`snapshot`. The network name retains `CachedObservation` provenance and the IP
stack retains `BackgroundObservation` provenance. `Err(NetError)` reports this
query's failure without substituting cached success.

### Errors and behavior

The [refresh lifecycle, budget, and error rules](#netstatusclientrefresh) apply.
This method uses an independent bounded sampling lane; a slow or failing name
query cannot delay it. A newer reachability sample supersedes an older query
with `Cancelled`. Dropping the future invalidates its uncommitted result.

### Example

The complete [async refresh example](#netstatusclientrefresh) shows lifecycle
and result handling; use `refresh_reachability(timeout).await` for its query.
Run the complete [network_refresh example](../../examples/network_refresh.rs)
with `--reachability-only` to select this async method.

## NetStatusClient::refresh_blocking()

Use these methods when an existing synchronous interface needs a fresh Windows
query. They drive the same operation as the corresponding async methods; they
do not start monitoring, select a fallback policy, or create a second cache.
Prefer `refresh().await` or `refresh_reachability().await` in async application
code, and keep blocking queries off the UI thread.

### Required features

`net-status`. Both methods are available on all platforms. In supported calling
contexts, timeout and local-lifecycle validation precede the platform check,
just as for async refresh. A valid running non-Windows request returns
`Unsupported`; no cached value is substituted.

### Function signature

```text
pub fn refresh_blocking(&self, timeout: Duration) -> Result<NetworkRefreshResult>
```

### Steps

Start the named view, check that it is `Running`, then call the method from a
supported thread. Use the reachability-only variant below when no fresh name
query is needed.

### Parameters

`timeout: Duration` is a positive validity budget covering admission, sampling,
and commit. The timeout is not a hard wall-clock return deadline; lock
contention, scheduling, and cleanup can delay return.

### Callback parameters

None. Existing subscriptions receive changed observations through the shared
commit path; success does not wait for callback completion.

### Return values

`Ok(NetworkRefreshResult)` has the same fields and provenance as `refresh()`.
`Err(NetError)` reports a query or calling-context failure. No cached result is
substituted after an error.

### Errors and behavior

The [async refresh contract](#netstatusclientrefresh) applies, including the
order of timeout, local-lifecycle, and platform checks in supported contexts.
Calls from SDK-managed runtime or sampling threads are rejected before blocking
with the context error described below. The blocking API does not expose a
generic executor or depend on progress from the caller's runtime.

### Calling contexts

| Context | Behavior |
| --- | --- |
| Ordinary application thread | Supported; the current thread waits for the query. |
| Inside an application futures executor or Tokio runtime | Supported, including nested executors and current-thread Tokio; the SDK drives its own query and timer, but the calling thread is blocked. Prefer the async method when possible. |
| SDK callback-pool thread | Supported because callbacks use separate threads. Entering the SDK runtime context alone does not make the callback a runtime worker. A long query still occupies its callback slot. |
| SDK-managed runtime thread (including its blocking pool) or sampling worker | Rejected with `InvalidInput` and error-context field `network_status.refresh_blocking.context` to avoid blocking query progress. |

A successful call reports a committed observation, not callback completion.
The sampling window's own reachability is `sampled_reachability`; the merged
`snapshot.reachability` may already retain a newer observation. If cached
fallback is appropriate for your application, handle the query error and read
`snapshot()` explicitly, retaining the distinction between cached and fresh
data.

### Example

See the complete [synchronous refresh example](#synchronous-refresh-example)
below for synchronous lifecycle integration. Run the complete
[network_refresh example](../../examples/network_refresh.rs) with `--blocking`
to select a full synchronous query.

## NetStatusClient::refresh_reachability_blocking()

Query fresh Windows reachability synchronously without querying a network name.

### Required features

`net-status`. In supported calling contexts, timeout and local-lifecycle
validation precede the platform check; a valid running non-Windows request
returns `Unsupported`.

### Function signature

```text
pub fn refresh_reachability_blocking(&self, timeout: Duration) -> Result<NetworkRefreshResult>
```

### Steps

Start this named view, check `Running`, and call from a supported thread. Prefer
`refresh_reachability().await` when the application interface is asynchronous.

### Parameters

`timeout: Duration` is a positive validity budget for admission, sampling, and
commit. It is not a hard wall-clock return deadline.

### Callback parameters

None. Committing the query does not wait for subscription callbacks or their
application work to complete.

### Return values

`Ok(NetworkRefreshResult)` includes the fresh sampled reachability and merged
snapshot. The name retains `CachedObservation` provenance, and IP-stack data
retains `BackgroundObservation` provenance. `Err(NetError)` never carries a
cached snapshot as fresh success.

### Errors and behavior

The [blocking calling-context rules](#netstatusclientrefresh_blocking) and
[reachability-only query rules](#netstatusclientrefresh_reachability) apply.
This method uses the independent reachability sampling lane. SDK-managed runtime
threads, including their blocking pool, and sampling threads return
`InvalidInput` with field `network_status.refresh_blocking.context`; independent
callback-pool threads are supported. It does not start or restart the monitor.

### Example

The complete [synchronous refresh example](#synchronous-refresh-example) calls
this method on an ordinary thread. The runnable
[network_refresh example](../../examples/network_refresh.rs) selects it with
`--blocking --reachability-only`.

## Event and refresh values

`NetworkEvent`, `NetworkEventKind`, `NetworkEventOptions`, `NetworkObservation`, `NetworkRefreshResult`,
`NetworkNameSource`, and `NetworkIpStackSource` are available from
`open_net::net_status` and the crate root. Existing `NetworkSnapshot` fields and
state-subscription behavior are unchanged.

| Event field | Meaning |
| --- | --- |
| `sequence` | Monotonic event cursor within one named view; it does not reset on stop/restart. Initial carries the current cursor without advancing it. Compare only within that view. |
| `generation` | Local activation generation. Clones share it; other named views have independent generations. |
| `source_revision` | Revision of the shared engine observation associated with this event. Local lifecycle events may retain the same source revision. Compare only within that engine. |
| `snapshot` | Coherent local snapshot at this event's commit. Its local revision is a separate counter from event sequence and source revision. |
| `kind` | The category below. |

| Event kind | Meaning |
| --- | --- |
| `Initial` | Subscription baseline, including when the client has no usable observation yet. |
| `ObservationReady` | First usable observation after activation or initialization; neither an inferred disconnection nor a replay of missed changes. |
| `ReachabilityChanged { previous, current }` | A committed change between known `Available` and `Unavailable` values. Repeated values and metadata-only changes do not emit this kind. |
| `LifecycleChanged` | Starting, stopping, failure, or permanent shutdown. Lifecycle changes do not manufacture network loss. |

Initial is delivered once per subscription. Lifecycle events and the first
usable observation consume distinct sequence entries where they occur. Metadata
changes remain observable through the state feed, even when they do not produce
a network event. A new client reusing a destroyed name has a new identity and
counter domain. `loss_epoch` is retained shared history, not the event count.

| Refresh field | Meaning |
| --- | --- |
| `snapshot` | The calling view's latest snapshot after merging the sample; a slow full query preserves later reachability observations. |
| `sampled_reachability` | This sampling window's own NLM result, which can differ from a later observation retained in `snapshot.reachability`. |
| `sample_id` | Increasing sampling-start identifier within one engine; completion can be out of order and failures/cancellations can leave gaps. It is not an event sequence or snapshot revision. |
| `sample_started_at`, `sampled_at` | Wall-clock bounds of the system-query window. Clock corrections may reverse wall time; use sample IDs for ordering. |
| `network_name_source` | `WifiSsid`, `NetworkListManager`, or `None` after a successful name query; `CachedObservation` when reachability-only sampling preserved the cached name. |
| `network_name_diagnostics` | Query faults bypassed by a successful fresh name fallback. |
| `ip_stack_source` | `BackgroundObservation`, explicitly identifying the IP-stack value as cached background evidence. |

The new enums and refresh result are non-exhaustive. Include a wildcard when
matching enums outside the library, and read result fields rather than
constructing refresh results yourself.

## NetStatusClient::stop()

Pause this named view while retaining its subscriptions and name for reuse.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub async fn stop(&self) -> Result<()>
```

### Steps

Await `stop()` to release this view's monitoring demand. Call `start()` later to resume the same view.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(())` | `()` | This stop request and its monitoring cleanup completed. |
| `Err(error)` | `NetError` | Lifecycle or cleanup infrastructure failed. |

### Errors and behavior

Normal stop publishes `Stopped`, clears reachability, IP stack, time and name, and preserves the loss epoch. Repeating it is harmless. It does not stop other active consumers of the shared detector and does not wait for application callbacks. `stop()` on a closed view cannot reopen it. Runtime cleanup failures and internal errors may propagate.

Once a polled stop has requested cleanup, cancelling its wait does not undo that request. A concurrent `start()` can activate the view again; serialize lifecycle calls when your application needs a stable stopped state.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("restart").await?;
    let mut receiver = client.subscribe()?;
    let _initial = receiver.recv().await?;
    let first = client.start().await;
    client.stop().await?;
    assert!(matches!(client.snapshot()?.state, open_net::MonitorState::Stopped));
    let second = client.start().await;
    receiver.unsubscribe();
    net.destroy_net_status_client("restart").await?;
    first?;
    second?;
    Ok(())
}
```

## NetStatusClient::shutdown()

Permanently close this view and publish its terminal state.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub async fn shutdown(&self) -> Result<()>
```

### Steps

Await shutdown, close callback handles if their completion matters, and destroy the client through `OpenNet` to release its name.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(())` | `()` | The view is closed and its monitoring cleanup completed. |
| `Err(error)` | `NetError` | Cleanup infrastructure failed; this does not make the view restartable. |

### Errors and behavior

Normal shutdown publishes `Closed` with absent observation fields, ends subscriptions after their final state, and prevents future starts. Repeated shutdown is harmless. Every clone of this named view sees closure; other named clients stay independent. The name remains registered until `destroy_net_status_client()` runs.

This method awaits monitoring cleanup, not arbitrary application work or callback retirement. Use each callback handle's `close().await` for that purpose; closing immediately can cancel a terminal callback that has not started. If you must process the final state, explicitly wait for its delivery before closing. Runtime cleanup and internal errors can propagate. Once closure is requested, dropping its waiting future does not reopen the view.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("closed").await?;
    client.shutdown().await?;
    client.shutdown().await?;
    assert!(matches!(client.snapshot()?.state, open_net::MonitorState::Closed));
    let error = client.start().await.unwrap_err();
    assert_eq!(error.kind(), open_net::error::ErrorKind::Closed);
    assert!(net.get_net_status_client("closed").is_ok());
    net.destroy_net_status_client("closed").await?;
    Ok(())
}
```

## Snapshot fields and related types

Import these types from `open_net::net_status` or the root re-exports: `NetworkSnapshot`, `MonitorState`, `NetworkStatus`, and `IpStack`. `NetworkSnapshot` implements `Clone` and `Debug`; cloning it copies data and does not subscribe or refresh.

| Public field | Type | Meaning |
| --- | --- | --- |
| `revision` | `u64` | Monotonically increasing revision within one view. Compare only snapshots from that same view; do not compare different clients or a client with the engine context. Gaps do not count missing network outages. |
| `loss_epoch` | `u64` | Shared loss/recovery epoch marker imported from the detector. It preserves loss history across coalesced updates and is not an exact count of physical disconnections. Local stop/close do not manufacture a network loss. |
| `state` | `MonitorState` | Lifecycle of this view, independent of whether a route is available. |
| `reachability` | `Option<NetworkStatus>` | `Some` for a current host-level observation; `None` when none exists. |
| `ip_stack` | `Option<IpStack>` | `Some` for observed local protocol capability; `None` for no current observation. `Some(IpStack::None)` is distinct from `None`. |
| `observed_at` | `Option<std::time::SystemTime>` | Wall-clock time of collection, or `None`. It is not a monotonic duration clock. |
| `network_name` | `Option<String>` | Best-effort platform name, or `None` when unavailable. It is not a portable Wi-Fi SSID field. |

| `MonitorState` variant | Meaning |
| --- | --- |
| `Stopped` | This view has not started or has been paused; observation fields are empty. |
| `Starting` | Startup is in progress. |
| `Running` | Monitoring is active; reachability may still be `Unavailable`. |
| `Failed(NetError)` | Startup or monitoring failed; inspect the embedded error rather than labeling it offline. Recoverable failure can be retried with `start()`. |
| `Closed` | This view is permanently closed. |

`MonitorState` is `#[non_exhaustive]`; consumer `match` expressions must contain a fallback arm. `NetworkStatus` contains `Available` and `Unavailable`. `IpStack` contains `None`, `V4Only`, `V6Only`, and `DualStack`. IP capability is derived from locally available addresses/routes and does not prove IPv4 or IPv6 Internet reachability.

```rust
use open_net::{MonitorState, NetworkSnapshot, NetworkStatus, OpenNet, Result};

fn describe(snapshot: &NetworkSnapshot) {
    match &snapshot.state {
        MonitorState::Stopped => println!("Monitoring paused"),
        MonitorState::Starting => println!("Starting monitor"),
        MonitorState::Running => match snapshot.reachability {
            Some(NetworkStatus::Available) => println!("Host network available"),
            Some(NetworkStatus::Unavailable) => println!("Host network unavailable"),
            None => println!("No current reachability observation"),
        },
        MonitorState::Failed(error) => eprintln!("Monitor failed: {error}"),
        MonitorState::Closed => println!("Monitor permanently closed"),
        _ => println!("A newer lifecycle state"),
    }
    if let Some(stack) = snapshot.ip_stack {
        println!("IPv4: {}, IPv6: {}", stack.has_ipv4(), stack.has_ipv6());
    }
    println!("Revision {}, loss epoch {}", snapshot.revision, snapshot.loss_epoch);
    println!("Time {:?}, name {:?}", snapshot.observed_at, snapshot.network_name);
}

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("display").await?;
    describe(&client.snapshot()?);
    net.destroy_net_status_client("display").await?;
    Ok(())
}
```

## NetworkStatus::name()

Get the status variant label for logging.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub const fn name(&self) -> &'static str
```

### Steps

Obtain the enum value from a snapshot, or use a variant as in the example, and call this helper.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

`"Available"` or `"Unavailable"` as `&'static str`.

### Errors and behavior

Infallible and read-only. No platform query or network probe occurs. Both enums implement `Display`; printing a value produces the same label as `name()`.

### Example

```rust
use open_net::NetworkStatus;

fn main() {
    assert_eq!(NetworkStatus::Available.name(), "Available");
    println!("{}", NetworkStatus::Unavailable);
}
```

## IpStack::name()

Get the IP capability variant label for logging.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub const fn name(&self) -> &'static str
```

### Steps

Obtain the enum value from a snapshot, or use a variant as in the example, and call this helper.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

`"None"`, `"V4Only"`, `"V6Only"`, or `"DualStack"` as `&'static str`.

### Errors and behavior

Infallible and read-only. No platform query or network probe occurs. Both enums implement `Display`; printing a value produces the same label as `name()`.

### Example

```rust
use open_net::IpStack;

fn main() {
    assert_eq!(IpStack::DualStack.name(), "DualStack");
    println!("{}", IpStack::V4Only);
}
```

## IpStack::has_ipv4()

Check whether observed local capability includes IPv4.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub const fn has_ipv4(&self) -> bool
```

### Steps

Obtain the enum value from a snapshot, or use a variant as in the example, and call this helper.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

`true` for `V4Only` or `DualStack`; `false` otherwise.

### Errors and behavior

Infallible and read-only. No platform query or network probe occurs. Both enums implement `Display`; printing a value produces the same label as `name()`.

### Example

```rust
use open_net::IpStack;

fn main() {
    assert!(IpStack::DualStack.has_ipv4());
    assert!(!IpStack::V6Only.has_ipv4());
}
```

## IpStack::has_ipv6()

Check whether observed local capability includes IPv6.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub const fn has_ipv6(&self) -> bool
```

### Steps

Obtain the enum value from a snapshot, or use a variant as in the example, and call this helper.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

`true` for `V6Only` or `DualStack`; `false` otherwise.

### Errors and behavior

Infallible and read-only. No platform query or network probe occurs. Both enums implement `Display`; printing a value produces the same label as `name()`.

### Example

```rust
use open_net::IpStack;

fn main() {
    assert!(IpStack::DualStack.has_ipv6());
    assert!(!IpStack::V4Only.has_ipv6());
}
```

## IpStack::is_dual_stack()

Check whether both local protocol families are available.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub const fn is_dual_stack(&self) -> bool
```

### Steps

Obtain the enum value from a snapshot, or use a variant as in the example, and call this helper.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

`true` for `DualStack`; `false` for all other variants.

### Errors and behavior

Infallible and read-only. No platform query or network probe occurs. Both enums implement `Display`; printing a value produces the same label as `name()`.

### Example

```rust
use open_net::IpStack;

fn main() {
    assert!(IpStack::DualStack.is_dual_stack());
    assert!(!IpStack::None.is_dual_stack());
}
```

## Subscription lifetime and cleanup

Subscription types are exported from `open_net::subscription`. The following examples use `NetworkSnapshot` as `T`; `StateReceiver<T>` is generic and also serves other state APIs.

Keep a `StateReceiver` or `Subscription` alive for as long as updates are useful. These are ownership handles, not clonable broadcast receivers. Register separately for independent consumers. `CallbackContext` is cloneable but holds a weak control reference; retaining it cannot keep a callback alive.

| Action | Effect |
| --- | --- |
| Drop or unsubscribe a receiver | Releases its registration; later `recv()` returns `Ok(None)` after unsubscribe. |
| Drop or unsubscribe a callback handle | Stops future delivery and releases capacity immediately; an already running callback can still finish. |
| `Subscription::close().await` from outside subscription callbacks | Unsubscribes and waits for that callback's execution to retire, including any retained fault. |
| Poll `close()` inside any synchronous subscription callback | Unsubscribes, then returns `InvalidInput`; this does not confirm callback retirement. |
| Drop a `recv()` future | Does not consume an update that was not returned; the receiver remains usable. |
| Drop a `close()` future after calling `close()` | The consumed ownership handle is dropped and unsubscribes; there is no completed wait or guarantee that a running callback has returned. |
| `stop()` the client | Retains both polling and callback subscriptions for restart. |
| `shutdown()` the client | Publishes final `Closed`, then subscriptions terminate. It does not await application callback work. |

The callback closure runs synchronously even though its scheduling is asynchronous.
If it spawns its own work, `close()` does not wait for that separate application
work. Never poll any subscription's `close()` from a synchronous subscription
callback, including a different or already idle subscription. Use
`CallbackContext::unsubscribe()` there and perform awaited cleanup from an
independent owner. The check cannot detect indirect waits through another
thread or task; do not block a callback waiting for work that needs it to return.

## StateReceiver::id()

Get the identity of this registration for diagnostics.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
impl<T: Clone> StateReceiver<T> {
    pub fn id(&self) -> SubscriptionId
}
```

### Steps

Register a receiver, then read or log its identifier.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

A copy of its `SubscriptionId`.

### Errors and behavior

Infallible. The ID is preserved when converting the receiver to a callback; it does not grant access to a subscription.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let receiver = client.subscribe()?;
    println!("Subscription {}", receiver.id());
    drop(receiver);
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## StateReceiver::current()

Read the most recently committed value without consuming pending delivery.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
impl<T: Clone> StateReceiver<T> {
    pub fn current(&self) -> T
}
```

### Steps

Keep the receiver, call `current()` for an immediate clone, and use `recv()` separately to advance its cursor.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

An owned `T`; here it is `NetworkSnapshot`, not `Result<NetworkSnapshot>`.

### Errors and behavior

Infallible as an API. Reading does not advance the receiver cursor, start monitoring, or contact the operating system. It can read a newer committed snapshot while the original subscription snapshot remains pending for `recv()`. Inspect `recv()` to observe terminal infrastructure errors; `current()` alone cannot report those errors. After unsubscribe it is a retained value, not a live subscription.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let mut receiver = client.subscribe()?;
    let current = receiver.current();
    let first = receiver.recv().await?.expect("Initial state exists");
    assert_eq!(current.revision, first.revision);
    receiver.unsubscribe();
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## StateReceiver::recv()

Wait for the next value or the end of the state feed.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
impl<T: Clone> StateReceiver<T> {
    pub async fn recv(&mut self) -> Result<Option<T>>
}
```

### Steps

Use a mutable receiver. Repeat until `Ok(None)` or handle an error. Use a timeout or cancellation branch if the application must stop waiting without a new update.

### Parameters

None beyond the exclusive `&mut self` receiver borrow.

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(Some(value))` | `T` | Initial snapshot or a newer state. |
| `Ok(None)` | `Option<T>` | Subscription has ended or was unsubscribed. |
| `Err(error)` | `NetError` | A source or subscription infrastructure failure. |

### Errors and behavior

Only one receive operation can borrow a receiver mutably at a time. The future is cancellation safe: cancelling a pending receive does not consume an update that was never returned. A live unchanged source can wait indefinitely; `stop()` alone does not end this receiver. Slow consumers may skip intermediate revisions.

The type also implements `futures::Stream<Item = open_net::Result<T>>`. To use `StreamExt::next()` in your own code, add a direct `futures-util` dependency. This guide uses `recv()`, which needs no extra dependency.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let mut receiver = client.subscribe()?;
    println!("Initial: {:?}", receiver.recv().await?);
    client.shutdown().await?;
    while let Some(snapshot) = receiver.recv().await? {
        println!("Final changes: {:?}", snapshot.state);
    }
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## StateReceiver::unsubscribe()

Stop this receiver and release its subscription slot synchronously.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
impl<T: Clone> StateReceiver<T> {
    pub fn unsubscribe(&self) -> bool
}
```

### Steps

Call the method when updates are no longer needed; dropping the receiver performs equivalent ownership cleanup.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

`true` if this call changed the registration from active to inactive; `false` if it was already inactive.

### Errors and behavior

Infallible and idempotent. Future `recv()` calls return `Ok(None)` and pending unread values are discarded. This only removes the subscription; it does not stop the named monitor.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let mut receiver = client.subscribe()?;
    assert!(receiver.unsubscribe());
    assert!(!receiver.unsubscribe());
    assert!(receiver.recv().await?.is_none());
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## StateReceiver::into_callback()

Move receiver ownership into a callback subscription while retaining its identity and cursor.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
impl<T: Clone> StateReceiver<T> {
    pub fn into_callback<F>(mut self, callback: F) -> Result<Subscription>
    where
        T: Send + Sync + 'static,
        F: Fn(CallbackContext, Result<T>) + Send + Sync + 'static,
}
```

### Steps

Create a receiver, optionally consume some values, then move it into this method. Retain the returned handle and close it when finished.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `self` | `StateReceiver<T>` | Yes | Consumed; the original receiver cannot be used afterward. |
| `callback` | `F` | Yes | Thread-safe closure with owned or sufficiently long-lived captures. |

### Callback parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `context` | `CallbackContext` | Subscription identity and a way to unsubscribe from this callback. |
| `result` | `open_net::Result<T>` | `Ok(value)` is a state value of the receiver's type; `Err(error)` is a terminal subscription/source infrastructure failure. |

This guide uses `T = NetworkSnapshot`. For that type, `Ok(snapshot)` can contain
`MonitorState::Failed(error)` or `Closed`; those state values are distinct from
an infrastructure failure delivered as `Err(error)`.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(subscription)` | `Subscription` | The same registration now uses callback delivery. |
| `Err(error)` | `NetError` | Callback installation failed; no replacement handle is returned. |

### Errors and behavior

`T: Clone` is required by the receiver implementation. The additional `Send + Sync + 'static` bounds allow state and callback captures to live on worker threads. Buffered state and the cursor are not reset when switching delivery mode, so an already consumed initial value is not delivered again.

The public call consumes the receiver, including when installation fails; an error does not return ownership for a retry. Register a new receiver if you want to retry. Installation can report runtime/dispatch failures or `Closed` for an inactive registration. Execution and cleanup follow the same rules as `on_change()`.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let mut receiver = client.subscribe()?;
    let id = receiver.id();
    let _initial = receiver.recv().await?;
    let subscription = receiver.into_callback(|context, result| {
        println!("{}: {:?}", context.id(), result);
    })?;
    assert_eq!(subscription.id(), id);
    client.shutdown().await?;
    subscription.close().await?;
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## Subscription::id()

Identify a callback registration without changing it.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn id(&self) -> SubscriptionId
```

### Steps

Keep the returned callback handle and read its identifier.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

A copy of the registration's `SubscriptionId`.

### Errors and behavior

Infallible. Identity remains available after the registration becomes inactive.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let subscription = client.on_change(|_, _| {})?;
    println!("Callback {}", subscription.id());
    subscription.close().await?;
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## Subscription::is_active()

Check whether the registration is still accepting deliveries.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn is_active(&self) -> bool
```

### Steps

Query the handle when displaying observation state or checking cancellation.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

`true` while accepting deliveries; `false` when inactive.

### Errors and behavior

Infallible. This is an instantaneous observation, not a synchronization barrier. `false` does not prove that an already running callback has returned; use `close()` for that guarantee.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let subscription = client.on_change(|_, _| {})?;
    println!("Active: {}", subscription.is_active());
    subscription.unsubscribe();
    assert!(!subscription.is_active());
    subscription.close().await?;
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## Subscription::unsubscribe()

Cancel future callback deliveries without waiting for a running callback.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn unsubscribe(&self) -> bool
```

### Steps

Call when future delivery must stop immediately; optionally follow with `close().await`.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

`true` when this call deactivates the subscription; `false` when already inactive.

### Errors and behavior

Infallible and idempotent. Capacity is released. An invocation that already started can still run to completion; this method does not close the client or stop its monitor.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let subscription = client.on_change(|_, _| {})?;
    println!("Cancelled now: {}", subscription.unsubscribe());
    assert!(!subscription.unsubscribe());
    subscription.close().await?;
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## Subscription::close()

Consume the callback handle, unsubscribe, and await callback retirement.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub async fn close(self) -> Result<()>
```

### Steps

Move the handle into `close()` from outside all synchronous subscription callbacks
and await completion. Inside a callback, use `CallbackContext::unsubscribe()` and
let an external owner await the retained subscription handle.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `self` | `Subscription` | Yes | Consumed; the handle cannot be reused. |

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(())` | `()` | No callback invocation for this registration is still running. |
| `Err(error)` | `NetError` | A retained callback/source/dispatch failure, or `InvalidInput` for a forbidden callback-context wait. An error does not by itself confirm retirement. |

### Errors and behavior

`close()` can return `CallbackPanicked` or a dispatch/runtime failure and
preserves the recorded fault. It does not wait for work spawned by a callback
after the closure returns. It also cancels pending, not-yet-started deliveries;
do not use it as a promise that every queued snapshot or the final `Closed`
snapshot was delivered. A callback that never returns can keep this wait
pending. Dropping the handle or calling `unsubscribe()` never provides this wait.
For `on_recoverable_event` subscriptions it also joins the recovery driver and
retains source/recovery failures.

For every subscription type, polling `close()` on a thread currently executing
any synchronous subscription callback first unsubscribes, then returns
`InvalidInput`, field `subscription.close`, stage `Dispatch`. This includes
closing another subscription and closing an already idle one. That rejection
does not mean an executing callback has retired. Use synchronous unsubscribe
inside callbacks and await closure from an external owner.

The context check occurs on every poll, not when the future is constructed.
A future created in a callback can be awaited outside it after that callback
returns. A previously pending future moved into a callback is rejected on its
next poll. The check sees only the current thread's callback stack, so it cannot
prevent an indirect deadlock through a separate application thread or task.
Cancelling the waiting future never reactivates the subscription or interrupts
an already running callback.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let subscription = client.on_change(|_, result| {
        if let Ok(snapshot) = result {
            println!("{:?}", snapshot.state);
        }
    })?;
    subscription.close().await?;
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## CallbackContext::id()

Read the identity of the callback currently executing.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn id(&self) -> SubscriptionId
```

### Steps

Read the context passed into your closure; correlate it with the owning handle.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

A copy of its `SubscriptionId`.

### Errors and behavior

Infallible. The context is cloneable and contains a weak control reference, so retaining it does not retain the callback or engine.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let subscription = client.on_change(|context, result| {
        println!("Callback {}: {:?}", context.id(), result);
    })?;
    println!("Owner {}", subscription.id());
    subscription.close().await?;
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## CallbackContext::unsubscribe()

Request self-unsubscription safely from inside a callback.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn unsubscribe(&self) -> bool
```

### Steps

Use the context argument directly; it is available even when the first callback runs before registration returns.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

`true` when this call deactivates the registration; `false` when already inactive or its owner/control is gone.

### Errors and behavior

Infallible and synchronous. It does not wait for the current callback to return and does not keep the callback alive. There is no `CallbackContext::close()` or `is_active()` method; use the owning `Subscription` for awaited cleanup and activity queries.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let subscription = client.on_change(move |context, result| {
        context.unsubscribe();
        let _ = sent.send(result);
    })?;
    if let Some(result) = received.recv().await {
        println!("One callback: {:?}", result?);
    }
    subscription.close().await?;
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## SubscriptionId::as_u64()

Convert an opaque identifier to a number for logs.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn as_u64(self) -> u64
```

### Steps

Read an ID from a receiver, subscription, or context, then convert the copied value.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `self` | `SubscriptionId` | Yes | A `Copy` identifier value. |

### Callback parameters

Not applicable.

### Return values

The identifier's numeric representation as `u64`.

### Errors and behavior

Infallible. IDs are unique within the running process while the counter has capacity, not stable across restarts. They carry no authority and cannot recreate or look up a registration. `SubscriptionId` also implements `Display`, equality, ordering, and hashing.

### Example

```rust
use open_net::{OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("observer").await?;
    let receiver = client.subscribe()?;
    println!("Numeric ID: {}", receiver.id().as_u64());
    receiver.unsubscribe();
    net.destroy_net_status_client("observer").await?;
    Ok(())
}
```

## Shared network state and LAN IPv4 discovery

The engine maintains common network facts, while each named monitor has its own start/stop/close view. A stopped client can show `Stopped` while the engine context shows `Running` due to another active consumer. Their `revision` counters are different domains.

`NetworkStatusContext` is an engine-bound, read-only capability. It implements `Clone` and `Debug`; cloning or reading it neither starts monitoring nor extends the engine owner's lifetime. It remains tied to its original engine after that engine is dropped. There is no public constructor for a context.

A standalone HTTP client can accept a context when constructed; see [HTTP network observation](../http/network_status.md). The LAN helper below is independent of both the context and named monitor lifecycle.

## OpenNet::network_status_context()

Export a read-only handle to this engine's cached network facts.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn network_status_context(&self) -> open_net::net_status::NetworkStatusContext
```

### Steps

Keep the engine alive, obtain the context, and read snapshots or pass it to a supported consumer.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

A `NetworkStatusContext` bound to this engine. The method does not return `Result`.

### Errors and behavior

Infallible and does not activate monitoring. The context does not retain the engine owner. Dropping the engine closes its network state even if the context remains alive.

### Example

```rust
use open_net::{OpenNet, Result};

fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let context = net.network_status_context();
    println!("Engine cache: {:?}", context.snapshot()?.state);
    drop(net);
    assert!(matches!(context.snapshot()?.state, open_net::MonitorState::Closed));
    Ok(())
}
```

## NetworkStatusContext::snapshot()

Read shared cached facts without creating a named view or activating monitoring.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn snapshot(&self) -> Result<NetworkSnapshot, NetError>
```

### Steps

Obtain the context from `OpenNet`, read the snapshot, and inspect its lifecycle before interpreting optional observations.

### Parameters

None beyond the borrowed `&self` receiver.

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(snapshot)` | `NetworkSnapshot` | A clone of engine-level facts, including `Stopped`, `Failed`, or final `Closed`. |
| `Err(error)` | `NetError` | The shared observation source has an infrastructure failure. |

### Errors and behavior

This method never starts or retries monitoring. If no consumer has activated monitoring, the initial state is `Stopped` with no observation. After normal engine drop, the retained terminal state is `Closed`. A recoverable monitor error belongs in `MonitorState::Failed`; permanent source errors can be returned directly. Compare revisions only within this context's state domain.

### Example

```rust
use open_net::{OpenNet, Result};

fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let context = net.network_status_context();
    let snapshot = context.snapshot()?;
    assert!(matches!(snapshot.state, open_net::MonitorState::Stopped));
    assert!(snapshot.reachability.is_none());
    Ok(())
}
```

## network::preferred_lan_ipv4()

Choose a local IPv4 address suitable for advertising to LAN peers using interface and address heuristics.

### Required features

`net-status`; the example uses the dependencies above.

### Function signature

```text
pub fn preferred_lan_ipv4() -> Result<Option<std::net::Ipv4Addr>, NetError>
```

### Steps

Call the synchronous helper when an application needs a candidate LAN address. Advertise only a returned address, and verify reachability from the actual peer.

### Parameters

None.

### Callback parameters

Not applicable.

### Return values

| Value | Type | Description |
| --- | --- | --- |
| `Ok(Some(address))` | `std::net::Ipv4Addr` | The highest-ranked eligible local IPv4 candidate. |
| `Ok(None)` | `Option<std::net::Ipv4Addr>` | Enumeration succeeded but no candidate survived filtering; do not fabricate a loopback fallback. |
| `Err(error)` | `NetError` | Interface enumeration failed; typically `Io` with the original source retained. |

### Errors and behavior

No engine or active monitor is needed. The helper synchronously enumerates interfaces instead of probing a public server. It excludes down, point-to-point and recognized virtual/VPN interfaces, loopback, link-local, unspecified and broadcast addresses, `198.18.0.0/15`, and `100.64.0.0/10`.

Candidates are ranked `192.168.0.0/16`, then `10.0.0.0/8`, then `172.16.0.0/12`, then remaining eligible addresses. Ties retain enumeration order. Linux additionally excludes common container/VM interface prefixes while allowing normal host bridges and bonds. These are selection heuristics, not a guarantee of physical-interface identity, firewall access, correct service binding, or peer reachability. Results can change when interfaces change; the function does not subscribe.

### Example

```rust
use open_net::Result;

fn main() -> Result<()> {
    match open_net::network::preferred_lan_ipv4()? {
        Some(address) => println!("LAN candidate: {address}"),
        None => println!("No eligible LAN IPv4 address"),
    }
    Ok(())
}
```

## Platform behavior and troubleshooting

These notes describe the current implementation rather than a cross-platform validation report.

| Platform | Observation behavior |
| --- | --- |
| macOS | `Available` requires netwatch to report a default-route interface and at least one IPv4/IPv6 capability. This is local route/interface evidence, not an Internet probe. A supplementary native path monitor triggers refreshes; failure to initialize that extra hint source is logged and leaves the netwatch path in use. `network_name` is currently `None`. |
| Windows | Background reachability and `refresh_reachability()` use a dedicated sampler, independent of the full `refresh()` and background name sampler. Reachability requires NetworkListManager Internet connectivity flags; a failed NLM query is not replaced with cached or netwatch reachability. Names prefer a newly queried active Wi-Fi SSID, then a connected name from that sampling round's NLM query. IP-stack capability remains background netwatch evidence. |
| Ubuntu / Linux | Uses netwatch capability flags plus a reachability query that checks operational interfaces and IPv4/IPv6 default routes from `/proc/net/route` and `/proc/net/ipv6_route`. This establishes local route/interface capability, not end-to-end Internet access. Missing route files permit a less detailed default-interface fallback; other read errors are retained as monitor errors. `network_name` is currently `None`. |

After Windows monitoring is already `Running`, background sampling that encounters
`TimedOut`, `QueueFull`, or sampling resource exhaustion retains its last
successful snapshot and retries on a later monitoring cycle. These conditions
do not themselves establish a network failure. Initial startup still requires a
successful first reachability sample; native reachability query failures can
produce `Failed`. Background names are independently retried and cannot delay
startup or fail an otherwise running reachability monitor. Stopping the monitor
cancels both sampling futures; late native work cannot update a later generation. Manual
`refresh()` always reports its own failure and never returns the retained
snapshot as fresh success.

The locally available `IpStack` uses netwatch's IPv4/IPv6 capability flags consistently across platforms. Reachability has platform-specific evidence. None of these mechanisms sends a request to your target server to prove that service healthy. A captive portal, DNS error, TLS error, or firewall can still make a request fail while the monitor says `Available`.

| Symptom | Check and next step |
| --- | --- |
| All observation fields are `None` | Inspect `state`. Call and await `start()` if the view is `Stopped`; do not label it offline. |
| Startup returns an error or state is `Failed` | Log the `NetError` kind, context, and source. Correct OS/runtime access issues, then explicitly retry `start()` for a recoverable failure. |
| `Available` but requests fail | Inspect the actual request's DNS, connection, TLS, timeout, and HTTP response errors. Monitor availability is host-level evidence. |
| Missing or unexpected network name | Names are best effort; use `None` handling and do not assume a Wi-Fi SSID on every platform. |
| A callback never appears | Retain its `Subscription` handle; dropping it unsubscribes. Check callback registration errors and ensure the engine remains alive. |
| Fewer state changes than expected | State updates are coalesced. Use `subscribe_events()` for ordered SDK commits; upstream OS notifications can still merge physical changes. |
| Event receive fails with `CallbackOverflow` | The bounded queue ended after its retained prefix. Subscribe again for a new Initial baseline; the missing interval is not replayed. |
| Event registration or receive fails with `ItemTooLarge` | One event exceeds `max_bytes`. Increase the byte limit before subscribing again; a later oversized event terminates its receiver after the retained prefix. |
| Refresh returns `NotRunning` | Start this named view, even if another consumer already keeps the shared monitor active. |
| Refresh returns `Unsupported` | Active refresh is Windows-only. Use cached snapshots and background observation on this platform. |
| Refresh succeeds without changing snapshot revision | Identical observed values are deduplicated. Inspect `sample_id` and the sampling window for this query. |
| Blocking refresh returns `InvalidInput` with field `network_status.refresh_blocking.context` | The call ran on an SDK-owned runtime thread or sampling worker. Use async refresh on a runtime worker, or move synchronous application work to a separate thread. |
| `SubscriptionLimitReached` | Close or drop unused receivers and callback owners. Each named view has separate limits of 1,024 state and 1,024 event registrations, including their respective callbacks. |
| A receive waits forever after `stop()` | The subscription intentionally survives stop/restart. Unsubscribe or shut down if the observer must end. |
| `ClientAlreadyExists` after shutdown | Destroy the named client through `OpenNet`; shutdown alone keeps the name. |
| `ConnectionClosing` during lookup/destroy | Destruction is already in progress. Await its original completion before reuse. |
| `start()` returns `Closed` | Create a new named view after destruction; a closed view cannot restart. Keep its engine alive. |
| LAN discovery returns `None` | There was no eligible candidate; VPN/virtual-only setups may be filtered. Do not advertise a guessed address. |

For diagnostics, `NetError::kind(&self) -> ErrorKind` identifies the category, `context(&self) -> &ErrorContext` exposes additional structured details, and `io_kind(&self) -> Option<std::io::ErrorKind>` provides a retained OS I/O kind when present. `NetError` implements `std::error::Error`; `source()` can expose the original cause. An error is not the same as an `Unavailable` observation.

```rust
use open_net::NetError;
use std::error::Error;

fn report(error: &NetError) {
    eprintln!("Kind: {:?}, context: {:?}", error.kind(), error.context());
    eprintln!("I/O kind: {:?}", error.io_kind());
    if let Some(source) = error.source() {
        eprintln!("Cause: {source}");
    }
}

fn main() {
    match open_net::network::preferred_lan_ipv4() {
        Ok(address) => println!("LAN candidate: {address:?}"),
        Err(error) => report(&error),
    }
}
```

## API coverage and source references

| Area | APIs covered |
| --- | --- |
| Engine ownership | `OpenNet::new`, `new_with_config`, `create_net_status_client`, `get_net_status_client`, `destroy_net_status_client` |
| Client methods | `start`, `snapshot`, `subscribe`, `on_change`, `subscribe_events`, `on_event`, `on_recoverable_event`, `refresh`, `refresh_reachability`, `refresh_blocking`, `refresh_reachability_blocking`, `stop`, `shutdown` |
| Event and refresh values | `NetworkEvent`, `NetworkEventKind`, `NetworkEventOptions`, `NetworkObservation`, `NetworkRefreshResult`, `NetworkNameSource`, `NetworkIpStackSource` |
| Snapshot values | All seven `NetworkSnapshot` fields; all current `MonitorState`, `NetworkStatus`, and `IpStack` variants |
| Value helpers | `NetworkStatus::name`; `IpStack::name`, `has_ipv4`, `has_ipv6`, `is_dual_stack` |
| Receivers | `StateReceiver::id`, `current`, `recv`, `unsubscribe`, `into_callback`; `EventReceiver::id`, `recv`, `try_recv`, `unsubscribe`, `into_callback`; stream and drop behavior |
| Callback ownership | `Subscription::id`, `is_active`, `unsubscribe`, `close`; `CallbackContext::id`, `unsubscribe`; `SubscriptionId::as_u64` |
| Shared state and discovery | `OpenNet::network_status_context`, `NetworkStatusContext::snapshot`, `network::preferred_lan_ipv4` |

The contracts were checked against the [client API](../../src/api/net_status/net_status_client.rs), [snapshot types](../../src/api/net_status/mod.rs), [engine API](../../src/api/open_net.rs), [subscription API](../../src/api/subscription/mod.rs), [view lifecycle](../../src/module/net_status/inner/facade.rs), [shared context](../../src/module/net_status/inner/shared.rs), [LAN selection](../../src/module/net_status/lan_addr.rs), and [public integration tests](../../tests/net_status_integration.rs).


## Engine configuration example

This complete offline program inspects defaults, sets and validates engine
limits, then demonstrates rejection of a zero worker count. No engine, monitor,
network connection or additional feature is required. Configuration setters
store values immediately; validation is a separate step.

```rust
use open_net::{OpenNetConfig, Result};
use open_net::error::ErrorKind;

fn main() -> Result<()> {
    let defaults = OpenNetConfig::default();
    assert_eq!(defaults.runtime_worker_threads(), None);
    assert_eq!(defaults.async_queue_capacity(), 128);
    assert_eq!(defaults.sync_queue_capacity(), 128);
    let configured = defaults
        .with_runtime_worker_threads(2)
        .with_async_queue_capacity(256)
        .with_sync_queue_capacity(64);
    configured.validate()?;
    assert_eq!(configured.runtime_worker_threads(), Some(2));
    assert_eq!(configured.async_queue_capacity(), 256);
    assert_eq!(configured.sync_queue_capacity(), 64);
    let invalid = configured.with_runtime_worker_threads(0);
    let error = invalid.validate().expect_err("zero workers must fail");
    assert_eq!(error.kind(), ErrorKind::InvalidConfig);
    println!("Engine configuration validated");
    Ok(())
}
```

## `OpenNetConfig::default()`

### Required features

No extra feature is required for this method. The example uses this page's
`net-status` dependency declaration.

### Function signature

```text
fn default() -> Self
```

### Steps

Start with defaults, apply only needed overrides, then validate or create an engine.

### Parameters

None.

### Callback parameters

Not applicable.

### Return values

An owned OpenNetConfig with no explicit runtime worker count and both queue capacities set to 128.

### Errors and behavior

This is the Default trait constructor. It creates no workers and cannot fail. With ws-client enabled, its nested WebSocket network policy also starts at its default; that policy does not configure NetStatus or HTTP.

### Example

The complete [engine configuration example](#engine-configuration-example)
calls this method and checks its result without starting any workers.

## `OpenNetConfig::with_runtime_worker_threads()`

### Required features

No extra feature is required for this method. The example uses this page's
`net-status` dependency declaration.

### Function signature

```text
pub fn with_runtime_worker_threads(self, threads: usize) -> Self
```

### Steps

Choose the engine management-runtime size before constructing OpenNet.

### Parameters

`self` is consumed; `threads: usize` is a required worker count, in threads.

### Callback parameters

Not applicable.

### Return values

The updated owned configuration, storing Some(threads).

### Errors and behavior

The last call replaces the previous value. Valid counts are 1 through 256; the setter itself does not reject invalid values. validate() or engine creation returns InvalidConfig for zero or a count above 256. None is the default and leaves Tokio defaults, including TOKIO_WORKER_THREADS, in effect. This is not a global cap on per-client I/O, callbacks, management or blocking threads.

### Example

The complete [engine configuration example](#engine-configuration-example)
calls this method and checks its result without starting any workers.

## `OpenNetConfig::with_async_queue_capacity()`

### Required features

No extra feature is required for this method. The example uses this page's
`net-status` dependency declaration.

### Function signature

```text
pub fn with_async_queue_capacity(self, capacity: usize) -> Self
```

### Steps

Choose the asynchronous engine submission queue size before constructing OpenNet.

### Parameters

`self` is consumed; `capacity: usize` is a required count of pending tasks, not bytes.

### Callback parameters

Not applicable.

### Return values

The updated owned configuration with the requested asynchronous queue capacity.

### Errors and behavior

Defaults to 128. Valid capacity is 1 through tokio::sync::Semaphore::MAX_PERMITS, inclusive. The last setter wins; validate() or engine creation rejects values outside that range with InvalidConfig. The setting is fixed after engine creation and is separate from HTTP per-client admission/queue limits.

### Example

The complete [engine configuration example](#engine-configuration-example)
calls this method and checks its result without starting any workers.

## `OpenNetConfig::with_sync_queue_capacity()`

### Required features

No extra feature is required for this method. The example uses this page's
`net-status` dependency declaration.

### Function signature

```text
pub fn with_sync_queue_capacity(self, capacity: usize) -> Self
```

### Steps

Choose the queue size used for synchronous engine calls before constructing OpenNet.

### Parameters

`self` is consumed; `capacity: usize` is a required count of pending tasks, not bytes.

### Callback parameters

Not applicable.

### Return values

The updated owned configuration with the requested synchronous-call queue capacity.

### Errors and behavior

Defaults to 128 and accepts 1 through tokio::sync::Semaphore::MAX_PERMITS. The underlying channel is still MPSC; synchronous describes callers waiting for results. Values are validated by validate() or engine creation, not this setter. This does not change an already running engine or client queue.

### Example

The complete [engine configuration example](#engine-configuration-example)
calls this method and checks its result without starting any workers.

## `OpenNetConfig::runtime_worker_threads()`

### Required features

No extra feature is required for this method. The example uses this page's
`net-status` dependency declaration.

### Function signature

```text
pub fn runtime_worker_threads(&self) -> Option<usize>
```

### Steps

Inspect the configured management-worker override before moving the configuration into an engine.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

Some(count) if a worker count was explicitly set; None means Tokio defaults apply.

### Errors and behavior

The accessor returns the stored override without validation. It does not resolve the environment-derived worker count, count live workers or expose a running engine configuration.

### Example

The complete [engine configuration example](#engine-configuration-example)
calls this method and checks its result without starting any workers.

## `OpenNetConfig::async_queue_capacity()`

### Required features

No extra feature is required for this method. The example uses this page's
`net-status` dependency declaration.

### Function signature

```text
pub fn async_queue_capacity(&self) -> usize
```

### Steps

Inspect the configured asynchronous queue limit before engine creation.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

The stored capacity in pending tasks, defaulting to 128.

### Errors and behavior

This is a configured limit, not a live queue length or byte count. The accessor does not validate values stored by setters.

### Example

The complete [engine configuration example](#engine-configuration-example)
calls this method and checks its result without starting any workers.

## `OpenNetConfig::sync_queue_capacity()`

### Required features

No extra feature is required for this method. The example uses this page's
`net-status` dependency declaration.

### Function signature

```text
pub fn sync_queue_capacity(&self) -> usize
```

### Steps

Inspect the configured synchronous-call queue limit before engine creation.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

The stored capacity in pending tasks, defaulting to 128.

### Errors and behavior

This is a configured limit, not a live queue length or thread count. It does not validate the value or mutate an existing engine.

### Example

The complete [engine configuration example](#engine-configuration-example)
calls this method and checks its result without starting any workers.

## `OpenNetConfig::validate()`

### Required features

No extra feature is required for this method. The example uses this page's
`net-status` dependency declaration.

### Function signature

```text
pub fn validate(&self) -> Result<(), NetError>
```

### Steps

Build the configuration and validate it before creating an engine or reporting settings as acceptable.

### Parameters

None beyond `&self`; validation borrows the configuration.

### Callback parameters

Not applicable.

### Return values

Ok(()) means the configuration limits are valid. Err(NetError) has InvalidConfig and field/reason details for invalid settings.

### Errors and behavior

Rejects explicit worker counts outside 1 through 256 and either queue capacity outside 1 through Semaphore::MAX_PERMITS. With ws-client enabled it also validates the nested WebSocket network configuration. It creates no runtime and does not prove that later engine/runtime creation or OS monitoring will succeed. OpenNet::new_with_config invokes validation again before starting workers.

### Example

The complete [engine configuration example](#engine-configuration-example)
calls this method and checks its result without starting any workers.

## Synchronous refresh example

This complete program uses an application runtime only for the existing async
lifecycle APIs. The refresh itself is a synchronous call on the ordinary main
thread. The runtime is not needed to drive that refresh's query or timeout.

```rust
use open_net::error::ErrorKind;
use open_net::{MonitorState, OpenNet, Result};
use std::time::Duration;

fn main() -> Result<()> {
    let lifecycle = tokio::runtime::Runtime::new()?;
    let net = OpenNet::new()?;
    let client = lifecycle.block_on(net.create_net_status_client("sync-query"))?;
    let queried: Result<()> = (|| {
        let started = lifecycle.block_on(client.start())?;
        if !matches!(started.state, MonitorState::Running) {
            return Ok(());
        }
        match client.refresh_reachability_blocking(Duration::from_secs(3)) {
            Ok(result) => println!(
                "Sample {}: sampled={:?}, merged={:?}, name source={:?}",
                result.sample_id, result.sampled_reachability,
                result.snapshot.reachability, result.network_name_source
            ),
            Err(error) if error.kind() == ErrorKind::Unsupported => {
                println!("Fresh queries require Windows; no query result returned");
            }
            Err(error) => return Err(error),
        }
        Ok(())
    })();
    let cleanup = lifecycle.block_on(net.destroy_net_status_client("sync-query"));
    queried.and(cleanup)
}
```

## NetStatusClient::on_recoverable_event()

Observe ordered events through a stable callback subscription that replaces its
underlying receiver after queue overflow.

### Required features

`net-status`.

### Function signature

```text
pub fn on_recoverable_event<F>(
    &self,
    options: NetworkEventOptions,
    callback: F,
) -> Result<Subscription>
where
    F: Fn(CallbackContext, Result<NetworkObservation>) + Send + Sync + 'static
```

### Steps

Retain the returned subscription, process each callback synchronously, and use
`Reconciled` to compare the last delivered event with a new atomic baseline.
Unsubscribe when the application no longer wants observations and await `close`
from an external owner to finish cleanup.

### Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| options | NetworkEventOptions | Existing per-receiver event and byte limits; reused on every recovery. |
| callback | F | Serial observer receiving ordinary events, a reconciliation, or a terminal error. |

### Callback parameters

`CallbackContext` exposes the stable logical subscription ID and synchronous
unsubscribe. `Ok(NetworkObservation::Event(event))` carries an original event,
including the first `Initial`. `Ok(NetworkObservation::Reconciled { previous,
current })` carries the last delivered event and the replacement receiver's
atomic `Initial` event. `Err(NetError)` reports a terminal failure when callback
delivery is still possible. The same error remains available from `close` even
if the callback executor cannot deliver it. Match the non-exhaustive enum with
a wildcard arm.

### Return values

A `Subscription` with one stable ID across receiver replacements. Retain it to
keep observation active; dropping it requests unsubscribe without waiting.

### Errors and behavior

Initial registration has the same configuration, item-size and quota errors as
`subscribe_events`. Only `CallbackOverflow` triggers recovery. Already retained events are delivered
before reconciliation; missed events are not replayed. `ItemTooLarge`, callback
panic, executor failure and other terminal errors stop observation. Callback
panic is contained and ends the subscription; it does not trigger recovery.

Callbacks for this subscription never overlap. This serial ordering ends when
the callback returns: asynchronous work it spawns needs application coordination.
`previous` is always present because a successful registration retains its first
`Initial`. Compare `snapshot.loss_epoch` only within one `generation`. An increase
records SDK-observed loss history; it cannot prove every physical outage. A
generation change requires a fresh application baseline.

The subscription can be created while the monitor is stopped and survives
stop/start. It never starts or restarts the monitor, creates a replacement named
client, or changes the raw `subscribe_events`/`on_event` overflow policy. A
`Closed` lifecycle event is a normal final observation. If closure happens in an
overflow gap, the replacement baseline reports that closed state and then ends.

Unsubscribe prevents new callback admission and receiver recovery. Callbacks
already admitted may finish. `close` waits for the driver, recovery and all
admitted callbacks; a terminal error is returned after cleanup. Polling this
subscription's `close` from any synchronous subscription callback requests
cancellation but rejects the wait with `InvalidInput` (field
`subscription.close`, stage `Dispatch`). This is the
[shared close rule](#subscriptionclose), not a recoverable-only restriction.
Use synchronous unsubscribe there and let an external owner join.

### Example

This complete program exercises lifecycle observation without requiring a
particular network transition. Real networks can change while it runs; recovery
occurs only if its bounded queue overflows.

```rust
use open_net::{NetworkEventOptions, NetworkObservation, OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let monitor = net.create_net_status_client("recoverable-events").await?;
    let subscription = monitor.on_recoverable_event(
        NetworkEventOptions::default(),
        |context, result| match result {
            Ok(NetworkObservation::Event(event)) => {
                println!("{} event {}: {:?}", context.id(), event.sequence, event.kind);
            }
            Ok(NetworkObservation::Reconciled { previous, current }) => {
                if previous.generation != current.generation {
                    println!("{}: new monitor generation; reset application baseline", context.id());
                } else if current.snapshot.loss_epoch > previous.snapshot.loss_epoch {
                    println!("{}: SDK loss history advanced during delivery gap", context.id());
                }
                println!("Reconciled to {:?}", current.snapshot);
            }
            Err(error) => eprintln!("{} stopped: {error}", context.id()),
            _ => {}
        },
    )?;
    let work: Result<()> = async {
        monitor.start().await?;
        monitor.stop().await?;
        monitor.start().await?;
        Ok(())
    }.await;
    let monitor_cleanup = net.destroy_net_status_client("recoverable-events").await;
    let observer_cleanup = subscription.close().await;
    work.and(monitor_cleanup).and(observer_cleanup)
}
```

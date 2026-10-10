# NetStatusClient integration guide

[Back to the library overview](../../README.md)

`NetStatusClient` observes host network conditions. Use it to display network state, react to changes, or inspect local IPv4/IPv6 capability. `Available` does not guarantee that your server, DNS service, or an Internet endpoint can be reached. Always handle failures from the actual network operation.

## Required features and prerequisites

Use Rust 1.89 or later and a supported platform: macOS, Windows, or Ubuntu with Linux kernel 5.0 or later. This is the project's support scope, not a claim that every platform or minimum kernel has been tested in this documentation change. Native build tools must be available; the macOS monitor includes a C component.

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
and are unavailable under its minimal dependency declaration. No WebSocket
tutorial is included in this documentation release.

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

The current implementation allows 1,024 subscriptions per named view, including callbacks. Exhausting this limit yields `SubscriptionLimitReached`; allocation or identifier exhaustion can yield `ResourceExhausted`. Drop or unsubscribe to release capacity. Source failures are delivered through `recv()` as `Err`; a recoverable `Failed` monitor state is an ordinary snapshot.

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
| `Subscription::close().await` | Unsubscribes and waits for that callback's execution to retire, including any retained fault. |
| Drop a `recv()` future | Does not consume an update that was not returned; the receiver remains usable. |
| Drop a `close()` future after calling `close()` | The consumed ownership handle is dropped and unsubscribes; there is no completed wait or guarantee that a running callback has returned. |
| `stop()` the client | Retains both polling and callback subscriptions for restart. |
| `shutdown()` the client | Publishes final `Closed`, then subscriptions terminate. It does not await application callback work. |

The callback closure runs synchronously even though its scheduling is asynchronous. If it spawns its own work, `close()` does not wait for that separate application work. Do not wait inside a callback for its own `close()`, or for application work that needs the callback to return. Perform awaited cleanup from an independent task.

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

Move the handle into `close()` from outside its own callback and await completion.

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
| `Err(error)` | `NetError` | A retained callback panic or dispatch failure. |

### Errors and behavior

`close()` can return `CallbackPanicked` or a dispatch/runtime failure and preserves the recorded fault. It does not wait for work spawned by a callback after the closure returns. It also cancels pending, not-yet-started deliveries; do not use it as a promise that every queued snapshot or the final `Closed` snapshot was delivered. A callback that never returns can keep this wait pending. Dropping the handle or calling `unsubscribe()` never provides this wait.

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
| macOS | Uses netwatch interface/default-route facts. A supplementary native path monitor triggers refreshes; failure to initialize that extra hint source is logged and leaves the netwatch path in use. `network_name` is currently `None`. |
| Windows | Tries NetworkListManager Internet connectivity flags for reachability; if that query cannot produce a result, falls back to netwatch-derived facts. Network names prefer an active Wi-Fi SSID and then a connected NetworkListManager name; either can be unavailable. |
| Ubuntu / Linux | Uses netwatch capability flags plus a reachability query that checks operational interfaces and IPv4/IPv6 default routes from `/proc/net/route` and `/proc/net/ipv6_route`. Missing route files permit a less detailed default-interface fallback; other read errors are retained as monitor errors. `network_name` is currently `None`. |

The locally available `IpStack` uses netwatch's IPv4/IPv6 capability flags consistently across platforms. Reachability has platform-specific evidence. None of these mechanisms sends a request to your target server to prove that service healthy. A captive portal, DNS error, TLS error, or firewall can still make a request fail while the monitor says `Available`.

| Symptom | Check and next step |
| --- | --- |
| All observation fields are `None` | Inspect `state`. Call and await `start()` if the view is `Stopped`; do not label it offline. |
| Startup returns an error or state is `Failed` | Log the `NetError` kind, context, and source. Correct OS/runtime access issues, then explicitly retry `start()` for a recoverable failure. |
| `Available` but requests fail | Inspect the actual request's DNS, connection, TLS, timeout, and HTTP response errors. Monitor availability is host-level evidence. |
| Missing or unexpected network name | Names are best effort; use `None` handling and do not assume a Wi-Fi SSID on every platform. |
| A callback never appears | Retain its `Subscription` handle; dropping it unsubscribes. Check callback registration errors and ensure the engine remains alive. |
| Fewer changes than expected | State updates are coalesced. Use current snapshots and revisions, not an assumed complete history. |
| `SubscriptionLimitReached` | Close or drop unused receivers and callback owners. The current per-view limit is 1,024. |
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
| All six client methods | `start`, `snapshot`, `subscribe`, `on_change`, `stop`, `shutdown` |
| Snapshot values | All seven `NetworkSnapshot` fields; all current `MonitorState`, `NetworkStatus`, and `IpStack` variants |
| Value helpers | `NetworkStatus::name`; `IpStack::name`, `has_ipv4`, `has_ipv6`, `is_dual_stack` |
| Receiver | `StateReceiver::id`, `current`, `recv`, `unsubscribe`, `into_callback`; stream and drop behavior |
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

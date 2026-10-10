# HTTP network status observations

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

## Bind HTTP to one engine's network context

Clients created by `OpenNet::create_http_client_with_config` automatically bind
to that engine's network service. For standalone workers use
`HttpClient::new_with_network_status(config, name, engine.network_status_context())`.
A client made with `HttpClient::new` has no network context and all three
observation methods return InvalidConfig, including after HTTP shutdown.

Observation is informational: a monitoring state never pauses, rejects or gates
HTTP requests. `Available` does not guarantee the target server is reachable.
An unknown observation (`None`) is not the same as an unavailable network.

Contexts remain tied to their original engine. Cloning/reading a context neither
starts monitoring nor extends the engine lifetime; engine drop closes the context.
HTTP clones share one observation view. Active registrations on that view share
one monitoring lease; the last unsubscribe/drop releases it. Other engine consumers
may keep the shared monitor active. A later registration can acquire a new lease.

## `HttpClient::network_snapshot()`

Read cached network state synchronously.

### Required features

`http-client`.

### Function signature

```text
pub fn network_snapshot(&self) -> Result<NetworkSnapshot, NetError>
```

### Steps

Create an engine, bind the standalone HTTP client to its context, observe state, then shut down and release the subscription if present.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable.

### Return values

`Ok(NetworkSnapshot)` is a cloned cached observation. `Err(NetError)` reports missing context or observation infrastructure failure.

### Errors and behavior

Never starts, refreshes or retries the monitor. A context-bound client retains a readable terminal Closed view after shutdown. A Stopped/Starting snapshot may contain no current reachability or IP-stack observation.

### Example

```rust
use open_net::OpenNet;
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let engine = OpenNet::new()?;
    let client = HttpClient::new_with_network_status(
        HttpClientConfig::new("http://127.0.0.1:8080")?,
        "observed-http", engine.network_status_context(),
    )?;
    let snapshot = client.network_snapshot()?;
    println!("Revision {}: {:?}", snapshot.revision, snapshot.state);
    client.shutdown().await?;
    println!("After shutdown: {:?}", client.network_snapshot()?.state);
    Ok(())
}
```

## `HttpClient::subscribe_network_status()`

Register a pull-based observation receiver and start monitoring as needed.

### Required features

`http-client`.

### Function signature

```text
pub fn subscribe_network_status(&self) -> Result<StateReceiver<NetworkSnapshot>, NetError>
```

### Steps

Create an engine, bind the standalone HTTP client to its context, observe state, then shut down and release the subscription if present.

### Parameters

None beyond `&self`.

### Callback parameters

Not applicable; use recv().await or convert the receiver into a callback.

### Return values

`Ok(StateReceiver<NetworkSnapshot>)` owns a subscription slot. `Err(NetError)` can report InvalidConfig, Closed, a subscription limit or registration infrastructure failure.

### Errors and behavior

Starts shared monitoring in the background. Initialization failure appears as MonitorState::Failed(error) in a successful snapshot; a new registration can request a retry. The receiver publishes current state and later changes. Slow consumers may skip intermediate states; this is not an event log. HTTP closure publishes terminal Closed before the receiver ends.

### Example

```rust
use open_net::OpenNet;
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let engine = OpenNet::new()?;
    let client = HttpClient::new_with_network_status(
        HttpClientConfig::new("http://127.0.0.1:8080")?,
        "observed-http", engine.network_status_context(),
    )?;
    let mut receiver = client.subscribe_network_status()?;
    println!("Current state: {:?}", receiver.current().state);
    // Closing gives this finite demonstration a terminal observation.
    client.shutdown().await?;
    while let Some(snapshot) = receiver.recv().await? {
        println!("Observed: {:?}", snapshot.state);
    }
    receiver.unsubscribe();
    Ok(())
}
```

## `HttpClient::on_network_status_change()`

Deliver network observations to a retained callback subscription.

### Required features

`http-client`.

### Function signature

```text
pub fn on_network_status_change<F>(&self, callback: F) -> Result<Subscription, NetError>
where
    F: Fn(CallbackContext, Result<NetworkSnapshot, NetError>) + Send + Sync + 'static
```

### Steps

Create an engine, bind the standalone HTTP client to its context, observe state, then shut down and release the subscription if present.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| callback | F with the bounds shown above | Yes | A reusable, thread-safe closure owning its captures. |

### Callback parameters

| Parameter | Type | Description |
| --- | --- | --- |
| context | CallbackContext | Subscription ID and synchronous unsubscribe control. |
| result | Result<NetworkSnapshot, NetError> | Snapshot or terminal delivery/infrastructure error. A monitor failure may instead be in snapshot.state. |

### Return values

`Ok(Subscription)` owns the callback registration. `Err(NetError)` reports missing/closed context, capacity limits or callback installation failure.

### Errors and behavior

Callbacks execute asynchronously on the shared callback pool. Keep the returned handle alive. Fn permits repeated calls; Send + Sync allows worker use; 'static requires captures without short-lived borrows. HTTP shutdown ends observation without waiting for a running network callback. Subscription::close() externally waits for its retirement and reports callback panic/dispatch errors.

### Example

```rust
use open_net::OpenNet;
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let engine = OpenNet::new()?;
    let client = HttpClient::new_with_network_status(
        HttpClientConfig::new("http://127.0.0.1:8080")?,
        "observed-http", engine.network_status_context(),
    )?;
    let subscription = client.on_network_status_change(|context, result| {
        match result {
            Ok(snapshot) => println!("Subscription {}: {:?}", context.id().as_u64(), snapshot.state),
            Err(error) => eprintln!("Observation failed: {error}"),
        }
    })?;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    client.shutdown().await?;
    subscription.close().await?;
    Ok(())
}
```

## `NetworkStatusContext` and snapshot values

### Required features

`net-status`, already enabled by `http-client`. Import the context from
`open_net::net_status::NetworkStatusContext`. All snapshot types are also available
through `open_net::net_status`; several have crate-root re-exports.

### Function signatures and parameters

```text
pub fn OpenNet::network_status_context(&self) -> NetworkStatusContext
pub fn NetworkStatusContext::snapshot(&self) -> Result<NetworkSnapshot, NetError>
```

Neither method takes arguments beyond self or installs callbacks. The first
returns a read-only, cloneable context; the second returns cached state or an
infrastructure error. Neither starts or retries monitoring. Keep the engine alive
for ongoing observation. See [NetStatusClient](../net_status/README.md) for
explicit start/stop and independent named views.

| NetworkSnapshot field | Type | Meaning |
| --- | --- | --- |
| `revision` | `u64` | Increasing revision in this view. Do not compare revisions across views. |
| `loss_epoch` | `u64` | Loss/recovery epoch marker; not an exact count of disconnects. |
| `state` | `MonitorState` | Lifecycle state. |
| `reachability` | `Option<NetworkStatus>` | Current host-level observation; None means no current observation. |
| `ip_stack` | `Option<IpStack>` | Local protocol availability; None means unknown. |
| `observed_at` | `Option<SystemTime>` | Observation timestamp when available. |
| `network_name` | `Option<String>` | Platform-supplied label, not a portable promise of Wi-Fi SSID. Current macOS/Linux implementations return None; Windows supplies a name according to its implementation. |

`MonitorState` is non-exhaustive: Stopped, Starting, Running, Failed(NetError),
Closed. Match it with a fallback arm. A monitor failure in `Ok(snapshot)` is
separate from a receiver `Err(error)`.
`NetworkStatus` is Available or Unavailable; `name(&self) -> &'static str` and
Display expose its name. `IpStack` is None, V4Only, V6Only or DualStack, with
`name(&self) -> &'static str`, `has_ipv4(&self) -> bool`,
`has_ipv6(&self) -> bool`, and `is_dual_stack(&self) -> bool` (all const methods).
`Some(IpStack::None)` is a known observation that neither stack is available;
it differs from `Option::None`. Local IP capability does not prove public Internet
connectivity for either protocol.

### Steps, errors and example

The following complete program reads without starting monitoring, handles the
non-exhaustive state enum, and needs no server or running monitor.

```rust
use open_net::{MonitorState, OpenNet};

fn main() -> Result<(), open_net::NetError> {
    let engine = OpenNet::new()?;
    let context = engine.network_status_context();
    let snapshot = context.snapshot()?;
    match snapshot.state {
        MonitorState::Running => println!("Monitoring is running"),
        MonitorState::Failed(ref error) => eprintln!("Monitor failed: {error}"),
        _ => println!("Lifecycle: {:?}", snapshot.state),
    }
    if let Some(stack) = snapshot.ip_stack {
        println!("{} IPv4={} IPv6={} dual={}", stack.name(),
                 stack.has_ipv4(), stack.has_ipv6(), stack.is_dual_stack());
    }
    if let Some(status) = snapshot.reachability {
        println!("Reachability: {}", status.name());
    }
    Ok(())
}
```

## Receivers and callback controls

Required feature: `http-client` for the source; the generic controls themselves
are publicly exported through `open_net::subscription`. StateReceiver<T> requires
`T: Clone` for these methods; callback conversion also requires
`T: Send + Sync + 'static`. NetworkSnapshot satisfies those constraints.

| Function signature | Return values, parameters and behavior |
| --- | --- |
| `StateReceiver<T>::id(&self) -> SubscriptionId` | No parameters; the registration's ID. |
| `current(&self) -> T` | Clone the latest committed state without advancing the receive cursor. |
| `async recv(&mut self) -> Result<Option<T>, NetError>` | Next state as Ok(Some), source termination as Ok(None), infrastructure failure as Err. Cancelling a receive wait does not consume an undelivered state. |
| `unsubscribe(&self) -> bool` | True only for the first transition to inactive; releases this registration. |
| `into_callback<F>(self, callback: F) -> Result<Subscription, NetError>` | Consumes the receiver and transfers the same registration to a callback on success. F is `Fn(CallbackContext, Result<T, NetError>) + Send + Sync + 'static`. No receiver is returned on error. |
| `Subscription::id(&self) -> SubscriptionId` | No parameters; controlled registration ID. |
| `is_active(&self) -> bool` | Whether new deliveries are accepted. |
| `unsubscribe(&self) -> bool` | Idempotent synchronous unsubscribe; does not wait for an executing callback. |
| `async close(self) -> Result<(), NetError>` | Consumes the handle, unsubscribes and awaits callback retirement. Preserves panic/dispatch error. Detached application tasks are outside this boundary. |
| `CallbackContext::id(&self) -> SubscriptionId` | Calling registration's ID. |
| `unsubscribe(&self) -> bool` | May be called inside callback; false if already detached/dropped. Does not await the callback. |
| `SubscriptionId::as_u64(self) -> u64` | Numeric registration ID. |

A StateReceiver also implements Stream with `Item = Result<T, NetError>`.
Dropping a receiver or Subscription unsubscribes immediately. Retaining a
CallbackContext alone does not retain the subscription because its control
reference is weak. To stop from inside a callback use context.unsubscribe();
await Subscription::close() from outside that callback, not from itself.
State observation coalesces changes, so slow receivers can miss intermediate values.

The next example converts the receiver to a callback without losing its
registration. No callback count/order is promised during the short demonstration.

```rust
use open_net::OpenNet;
use open_net::api::http::HttpClientConfig;

#[tokio::main]
async fn main() -> Result<(), open_net::NetError> {
    let engine = OpenNet::new()?;
    let client = engine.create_http_client_with_config(
        "observations", HttpClientConfig::new("http://127.0.0.1:8080")?,
    ).await?;
    let receiver = client.subscribe_network_status()?;
    println!("Receiver {}", receiver.id().as_u64());
    let subscription = receiver.into_callback(|context, result| {
        println!("{:?}", result);
        context.unsubscribe();
    })?;
    println!("Subscription {} active={}", subscription.id().as_u64(), subscription.is_active());
    subscription.close().await?;
    engine.destroy_http_client("observations").await?;
    Ok(())
}
```

## Individual shared API reference

The following links are the canonical per-function contracts, including
parameters, return values, failure behavior and complete examples. They also
apply to the HTTP network context and receivers:

- [OpenNet::network_status_context](../net_status/README.md#opennetnetwork_status_context)
- [NetworkStatusContext::snapshot](../net_status/README.md#networkstatuscontextsnapshot)
- [NetworkStatus::name](../net_status/README.md#networkstatusname)
- [IpStack::name](../net_status/README.md#ipstackname), [has_ipv4](../net_status/README.md#ipstackhas_ipv4), [has_ipv6](../net_status/README.md#ipstackhas_ipv6), [is_dual_stack](../net_status/README.md#ipstackis_dual_stack)
- [StateReceiver::id](../net_status/README.md#statereceiverid), [current](../net_status/README.md#statereceivercurrent), [recv](../net_status/README.md#statereceiverrecv), [unsubscribe](../net_status/README.md#statereceiverunsubscribe), [into_callback](../net_status/README.md#statereceiverinto_callback)
- [Subscription::id](../net_status/README.md#subscriptionid), [is_active](../net_status/README.md#subscriptionis_active), [unsubscribe](../net_status/README.md#subscriptionunsubscribe), [close](../net_status/README.md#subscriptionclose)
- [CallbackContext::id](../net_status/README.md#callbackcontextid), [unsubscribe](../net_status/README.md#callbackcontextunsubscribe)
- [SubscriptionId::as_u64](../net_status/README.md#subscriptionidas_u64)

# Engine lifecycle and shutdown

[Back to the library overview](../README.md)

`OpenNet` owns shared engine resources and the clients registered under it.
Use `begin_shutdown()` to request permanent closure immediately, and await
`shutdown()` when the application must know that the engine's running resources
have exited. Once closure begins, the engine cannot reopen; create a new engine
for a new lifecycle.

## Required features and prerequisites

The two engine shutdown methods require no Cargo feature. The examples below
also create network-status clients, so they enable `net-status`. Use Rust 1.89
or later, create a project with `cargo new engine-lifecycle-example`, and add
these dependencies to its `Cargo.toml`. Replace the local path with your
`open-net` package directory.

```toml
[dependencies]
open-net = { path = "../open-net-test/open-net", default-features = false, features = ["net-status"] }
futures = "0.3.34"
```

Every Rust block is a separate complete `src/main.rs`. Copy one at a time and
run `cargo run`. `futures::executor::block_on` waits for a future on an ordinary
application thread; the SDK owns the workers needed to perform cleanup. An
application Tokio runtime is not required for these examples. Do not call
`block_on` from an SDK callback to wait for shutdown.

If another dependency enables `ws-client`, Cargo combines those features.
Engine construction then requires an application-selected TLS provider even if
this particular code only observes network status. Follow the
[TLS provider guide](tls.md) before constructing that engine.

## Choose the scope to close

| Scope | Operation | What it means |
| --- | --- | --- |
| One observation subscription | Drop/unsubscribe; `Subscription::close().await` to wait | Stop that registration. Other subscriptions and the client remain independent. |
| One network-status view temporarily | `NetStatusClient::stop().await` | Release its monitoring demand while keeping subscriptions for a later restart. |
| One network-status view permanently | `NetStatusClient::shutdown().await` | Close the view and its clones; its registered name remains reserved. This does not wait for all application callbacks. |
| One named client | The corresponding `OpenNet::destroy_*_client(...).await` | Close that client and release its name after cleanup, following that transport's close rules. |
| A lazy managed HTTP client | `ManagedHttpClient::begin_shutdown()` / `shutdown().await` | Close that managed client's shared lifecycle; other engine clients remain independent. |
| The entire engine | `OpenNet::begin_shutdown()` / `shutdown().await` | Permanently close admission and retire resources belonging to this engine. |

These are different completion boundaries. See the [HTTP lifecycle guide](http/lifecycle.md),
[WebSocket guide](websocket/README.md), and [network-status guide](net_status/README.md)
for client-level behavior. For instance, a client close grace period does not
mean the engine can skip waiting for a callback/provider thread that is still
executing.

## Recommended application sequence

1. Stop application code from submitting new work. Signal your own tasks to
   finish or cancel according to your application's policy.
2. Call `begin_shutdown()` if closure must be requested synchronously, such as
   from a callback. This step is optional when an external owner immediately
   awaits `shutdown()`, because its first poll starts closure too.
3. Await `shutdown()` from an external application control flow. Preserve and
   handle its result; only success confirms the engine completion boundary.
4. Join application-owned tasks and close independent HTTP clients or log
   subscriptions separately. Drop retained handles/data when they are no
   longer useful.

Do not keep an SDK callback blocked while waiting for an external task to finish
step 3: shutdown may be waiting for that callback to return. A direct callback
wait is rejected, but an indirect application dependency can still deadlock.

## OpenNet::begin_shutdown()

Request permanent engine closure without waiting for running resources.

### Required features

None. The example's named observation client requires `net-status`.

### Function signature

```text
pub fn begin_shutdown(&self)
```

### Steps

Call once when closure is needed; repeated calls are harmless. Return promptly
if called inside a callback. Let an external owner await `shutdown()` to confirm
that cleanup has finished.

### Parameters

Only `&self`, a shared reference to the engine. The engine is not consumed.

### Callback parameters

None. The method may be called from an SDK callback, but it does not register a
callback or deliver a completion notification itself.

### Return values

Returns `()`. This acknowledges the synchronous closure request, not retirement
of clients, callbacks, providers, workers or the runtime.

### Errors and behavior

The call permanently closes this engine's admission and requests cancellation.
It is idempotent and does not wait for callbacks or worker termination. Existing
handles may remain allocated, but they cannot reopen the engine. Follow it with
`shutdown().await` outside SDK callback/worker contexts when completion matters.

### Example

The callback can request closure while the application retains responsibility
for the final wait. A weak engine reference avoids making callback storage own
the engine indefinitely. No monitor is started in this lifecycle-only example.

```rust
use futures::executor::block_on;
use open_net::{OpenNet, Result};
use std::sync::Arc;

fn main() -> Result<()> {
    block_on(async {
        let net = Arc::new(OpenNet::new()?);
        let monitor = net.create_net_status_client("shutdown-observer").await?;
        let weak_net = Arc::downgrade(&net);
        let subscription = monitor.on_change(move |context, _| {
            context.unsubscribe();
            if let Some(net) = weak_net.upgrade() {
                net.begin_shutdown();
            }
        })?;
        // An external owner may also request closure; repeated requests are safe.
        net.begin_shutdown();
        let engine_cleanup = net.shutdown().await;
        let subscription_cleanup = subscription.close().await;
        engine_cleanup.and(subscription_cleanup)
    })
}
```

## OpenNet::shutdown()

Request permanent closure and wait for this engine's running resources to exit.

### Required features

None. The example's named observation client requires `net-status`.

### Function signature

```text
pub async fn shutdown(&self) -> Result<(), NetError>
```

### Steps

Await the method from an external application task or ordinary control thread.
Calling an `async fn` only creates a future: closure begins on its first poll,
unless `begin_shutdown()` already requested it. Check the result before assuming
the engine has completed cleanup. Retaining a client handle does not prevent a
successful shutdown from retiring its engine-owned running resources.

### Parameters

Only `&self`, a shared reference to the engine. There is no built-in timeout
argument. An application timeout limits that caller's wait, not SDK cleanup.

### Callback parameters

None. Existing accepted callbacks and provider work within the engine's
completion boundary are awaited; the method does not add a new callback.

### Return values

| Value | Meaning |
| --- | --- |
| `Ok(())` | The engine's registered clients, SDK callbacks/providers, monitoring, cleanup workers and engine runtime have retired. Retained public handles/data may still exist. |
| `Err(error)` with `InvalidInput` | The future was polled in a forbidden SDK callback/worker context. Closure still starts, but retirement is not confirmed. |
| Another `Err(error)` | Cleanup reported an error. It does not certify that every resource has retired. |

Concurrent and repeated external waiters share the same shutdown outcome.

### Errors and behavior

No caller Tokio runtime is required. Once closure starts, dropping or timing out
one waiting future does not cancel cleanup. Another owner can wait again. A
provider or callback that never returns can prevent completion; timing out the
wait does not interrupt arbitrary user code.

Every poll rejects SDK callback/worker contexts with `InvalidInput`, error-context
field `open_net.shutdown`, stage `Close`, after requesting closure. This also
applies when the callback belongs to another engine. Use `begin_shutdown()` and
return instead. The check cannot detect indirect waits through application-created
threads or tasks.

The boundary includes registered clients, their SDK callbacks/providers, network
monitoring, engine management and callback workers, client cleanup threads, and
the engine runtime. Accepted network-observation delivery, including recoverable
subscriptions, retires within that boundary. Terminal notifications complete
dispatch before the shared callback pool closes. Earlier unsubscribe or overflow
still has its documented delivery consequences; shutdown does not restore
discarded observations.

The following remain outside this boundary:

- Standalone HTTP workers, even for `HttpClient::new_with_network_status` using
  this engine's context. Their engine-owned observations retire, while HTTP
  request workers remain independent and need their own close.
- Other engines, process-wide log subscriptions and the process-wide retirement
  reaper. Shutdown completes after its own coordinator exits; that shared reaper
  remains available for other engines.
- User-retained response data and handles, and tasks spawned onto an
  application-owned runtime. A callback returning does not join work it spawned.
- An already admitted synchronous LAN query running on its caller's thread.

Dropping `OpenNet` still initiates automatic cleanup. Drop itself is not a
completion receipt. Explicitly await `shutdown()` when a test, application exit
sequence or resource replacement needs that receipt.

### Example

This program keeps a public client handle alive across shutdown, waits from an
ordinary thread, and reads the client's final snapshot afterward. Two concurrent
waiters and a later repeated wait observe the same engine lifecycle.

```rust
use futures::executor::block_on;
use open_net::{MonitorState, OpenNet, Result};

fn main() -> Result<()> {
    block_on(async {
        let net = OpenNet::new()?;
        let monitor = net.create_net_status_client("retained-handle").await?;
        let (first, second) = futures::join!(net.shutdown(), net.shutdown());
        first?;
        second?;
        net.shutdown().await?;
        assert!(matches!(monitor.snapshot()?.state, MonitorState::Closed));
        Ok(())
    })
}
```

## Subscription cleanup is a separate API

`Subscription::close(self)` consumes an observation callback handle, unsubscribes
and waits for that registration's callback retirement. Polling it inside any
synchronous subscription callback first unsubscribes, then returns `InvalidInput`
with field `subscription.close`, stage `Dispatch`. This includes closing a
different or already idle subscription. Use `CallbackContext::unsubscribe()` in
callbacks; see the [full shared subscription rules](net_status/README.md#subscriptionclose).

`LogSubscription` is a different handle. It has no `close()` method: Drop stops
new log admission without waiting for an already running log callback. Engine
shutdown does not clear these process-wide subscriptions. See the
[logging guide](log/README.md) for ownership, filtering and bounded queues.

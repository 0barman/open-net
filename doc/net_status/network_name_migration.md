# Network-name observation migration

[Back to the NetStatusClient guide](README.md)

The published 0.3.2 release can collect Windows network names through connectivity
monitoring and `refresh` / `refresh_blocking`; it has no separate name-observer
API. The collection boundary described below applies to open-net 0.3.3 and must
not be assumed when using 0.3.2 from crates.io.

In 0.3.3, connectivity monitoring no longer collects network names. Windows Wi-Fi SSIDs
and Network List Manager names are available only through an explicit, separately
owned `NetworkNameObserver`. Creating an engine or network-status client, starting
connectivity monitoring, subscribing to ordinary state/events, observing HTTP
network status, and using WebSocket network gating do not opt into name collection.

This changes behavior while retaining the existing 0.3 public fields and method
signatures. A caller that previously displayed `snapshot.network_name` must
explicitly create a name observer; changing the refresh method alone does not
restore the old behavior.

## Existing API migration

| Existing API or field | Current contract | Migration |
| --- | --- | --- |
| `NetworkSnapshot.network_name` | Compatibility field, always `None` in SDK-produced values on every platform. | Read a separate `NetworkNameSnapshot` if names are needed. |
| `refresh(timeout)` | Deprecated alias for `refresh_reachability(timeout)`. No name query. | Call `refresh_reachability(timeout)`. |
| `refresh_blocking(timeout)` | Deprecated alias for `refresh_reachability_blocking(timeout)`. No name query. | Call `refresh_reachability_blocking(timeout)`. |
| `NetworkRefreshResult.network_name_source` | Always `NetworkNameSource::NotCollected`. | Do not interpret this as a failed name query or a missing Wi-Fi connection. |
| `NetworkRefreshResult.network_name_diagnostics` | Always empty. | Read `NetworkNameSnapshot::error()` / `diagnostics()` on an explicit observer. |
| `NetworkNameSource::CachedObservation` | Retained for source compatibility; no longer emitted by refresh. | Handle `NotCollected`; keep a wildcard because the enum is non-exhaustive. |
| Ordinary snapshot/event/HTTP subscriptions and `NetworkStatusContext` | Carry connectivity facts only, even if another observer has opted into names. | Subscribe to the returned name observer itself. |

`NotCollected` is appended after the existing `NetworkNameSource` variants. Their
numeric cast values remain `CachedObservation = 0`, `WifiSsid = 1`,
`NetworkListManager = 2`, and `None = 3`; the new variant is `4`.

Reachability refresh still requires the calling view to be `Running` and a
positive timeout. Name observation has a different lifecycle and does not require
the connectivity monitor to be running.

## Explicit observer API

The following types are available from `open_net::net_status` and the crate root
with the `net-status` feature:

```text
NetStatusClient::observe_network_names(
    &self,
    options: NetworkNameOptions,
) -> Result<NetworkNameObserver>

NetworkNameObserver::snapshot(&self) -> Result<NetworkNameSnapshot>
NetworkNameObserver::recv(&mut self) -> Result<Option<NetworkNameSnapshot>>
NetworkNameObserver::close(&mut self) -> impl Future<Output = Result<()>>
```

The constructor is synchronous: it validates and registers observation without
blocking for a native query. It supports Windows only. Invalid options produce
`InvalidInput` before lifecycle/platform checks; an already closed client or
engine produces `Closed`; otherwise an unsupported platform produces
`Unsupported`. Each engine admits at most 32 active name observers; exceeding
this limit returns `SubscriptionLimitReached`. It does not call `start()` or
change the ordinary monitor state.

Creation racing with client or engine shutdown can fail with `Closed` or return an observer
that is already closed; it never restores revoked collection. A pending `recv()`
does not retain a private copy of the previously delivered name after cleanup.

`NetworkNameOptions` has private fields and consuming setters:

| Method | Default and accepted values |
| --- | --- |
| `NetworkNameOptions::default()` | Interval 30 seconds; query timeout 10 seconds. |
| `with_interval(Duration)` / `interval()` | From 1 second through 1 hour, inclusive. |
| `with_timeout(Duration)` / `timeout()` | From 100 milliseconds through 30 seconds, inclusive. |

The interval is the delay after a sample completes before the next sample becomes
due. The query timeout starts when the serial worker begins that observer's native
query; it does not include time waiting for another observer's query. There is no
hard upper bound on that queue wait.

Retain the returned observer while names are needed. It is not `Clone`: the owner
controls one observation lifetime. Its latest-state feed can coalesce intermediate
updates; it is not a complete history of physical network changes. `recv()`
delivers an initial `Pending` state, subsequent current observations, and a cleared
`Closed` state once before returning `None`. If the observer closes before the
first receive, `recv()` skips `Pending` and delivers only `Closed`. A direct
`snapshot()` read returns the latest state and may already be `Ready` when
construction returns.

`NetworkNameSnapshot` has private fields and read-only accessors:

| Accessor | Meaning |
| --- | --- |
| `state()` | `Pending`, `Ready`, `Failed`, or `Closed`; the enum is non-exhaustive. |
| `name()` | Borrowed name, if the current successful query found one. |
| `source()` | Wi-Fi SSID, NLM network name, successful absence, or no collection. |
| `sampled_at()` | Query observation time when present; not a monotonic clock. |
| `error()` | Current sampling failure, if any. |
| `diagnostics()` | Faults bypassed by a successful name fallback, when present. |

`Ready` with `name() == None` means the query successfully found no name. `Pending`
means no current result; `Failed` means the query failed and clears the previously
retained name. Neither a failed name query nor an absent name indicates lost
connectivity. Inspect the separate reachability API for that purpose.

On Windows, name collection prefers an active Wi-Fi SSID, with a freshly queried
connected NLM name as fallback. These have different meanings and are not stable
network identifiers. The Windows subprocess path is resolved from
`GetSystemDirectoryW`; it is not searched through the application directory or
`PATH`. This path hardening does not establish that all Windows configurations
have been dynamically tested.

## Ownership, closing and retention

| Operation | Connectivity monitor | Name observer |
| --- | --- | --- |
| Create ordinary client / read snapshot | Does not start monitoring. | Does not collect names. |
| `start()` / ordinary subscriptions | Uses the existing connectivity contract. | Does not collect names. |
| `observe_network_names(options)` | No start or state change. | Explicitly starts its independent observation lifetime. |
| `client.stop()` | Stops that connectivity view. | Continues until its owner is closed or dropped. |
| `observer.close().await` | No effect. | Revokes collection, clears SDK-held current/pending name data, rejects late results, and waits for retirement. |
| Drop observer | No effect. | Starts cancellation and clearing without blocking the dropping thread. |
| Client `shutdown()` / destroy | Permanently closes the view. | Closes that client's observers and waits for cleanup. |
| Engine shutdown | Closes the engine's monitoring. | Closes its observers and waits for registered work to retire. |

Each explicit observer has an independent boundary. A name acquired by one
observer is never inserted into an ordinary snapshot or delivered to another
client's connectivity observers. Name updates do not advance connectivity
revisions, loss epochs or event sequences.

Cancellation invalidates results but cannot forcibly interrupt a synchronous COM
call that is already running. `close().await` may therefore wait for native work
to return; a query timeout is not a hard upper bound on cleanup duration.
Subprocess work is terminated and reaped when cancelled or expired. Retain and
await explicit cleanup when resource retirement matters.

Clearing SDK caches cannot erase snapshots or strings already returned to the
application. The caller owns those copies and controls their retention, display
and storage. The SDK does not promise secure erasure of heap memory. `Debug`
output for name snapshots/observers omits the name and raw error contents; code
that explicitly reads `name()` can still display or persist it.

## Example

This example opts into name observation without starting connectivity monitoring.
It reports state and whether a name exists, and does not print the name itself.
Applications should use `name()` only for their explicitly requested feature.

Create a consumer project and use the following dependency settings:

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false, features = ["net-status"] }
tokio = { version = "1.53.1", features = ["macros", "rt-multi-thread", "sync", "time"] }
```

```rust
use open_net::error::ErrorKind;
use open_net::{NetworkNameOptions, NetworkNameState, OpenNet, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("explicit-names").await?;
    let result = async {
        let mut observer = match client.observe_network_names(NetworkNameOptions::default()) {
            Ok(observer) => observer,
            Err(error) if error.kind() == ErrorKind::Unsupported => return Ok(()),
            Err(error) => return Err(error),
        };
        let observed: Result<()> = async {
            while let Some(snapshot) = observer.recv().await? {
                if snapshot.state() == NetworkNameState::Pending {
                    continue;
                }
                println!(
                    "State: {:?}; name present: {}",
                    snapshot.state(),
                    snapshot.name().is_some()
                );
                break;
            }
            Ok(())
        }.await;
        let closed = observer.close().await;
        observed.and(closed)
    }.await;
    let destroyed = net.destroy_net_status_client("explicit-names").await;
    result.and(destroyed)
}
```

Use the minimal dependency configuration in the [integration guide](README.md).
The runnable [network-name example](../../examples/network_names.rs) requires an
explicit command-line switch before collecting. Run it from the matching library
source directory containing its `Cargo.toml`; this is a source example, not an
example installed in a downstream application:

```bash
cargo run --manifest-path Cargo.toml --no-default-features --features net-status --example network_names -- --observe-network-names
```

The ordinary [refresh example](../../examples/network_refresh.rs) and network-status
demo omit network names from default output and newly written archives. Existing
application archives or previously returned values are outside this migration's
automatic cleanup.

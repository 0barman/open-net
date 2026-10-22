# LAN IPv4 candidates and selection

[Back to the library overview](../README.md)

Use this API when your application needs to inspect this machine's IPv4
addresses, for example before displaying a LAN address to another device.
The result includes the evidence behind a recommendation so you can explain
why an address was selected or choose differently for a known network topology.

| Entry point | When to use it |
| --- | --- |
| `network::lan_ipv4_snapshot()` | You only need local IPv4 information; no engine or monitoring client is required. |
| [`NetStatusClient::query_lan_ipv4()`](net_status/README.md#netstatusclientquery_lan_ipv4) | You already own a network-status client and want the same query under its admission/lifecycle rules. |
| `network::preferred_lan_ipv4()` | Subnet-based selection without native interface or route evidence. |

The snapshot function and client query run a fresh synchronous OS query and return
the same owned `LanIpv4Snapshot` type. They do not send a remote probe. In async or UI
applications, run them on an appropriate blocking worker; the complete
[client example](net_status/README.md#netstatusclientquery_lan_ipv4) demonstrates
that integration.

## Required features and prerequisites

Use Rust 1.89 or later on a supported platform. Enable `net-status`, which is
also enabled by either transport. Create a consumer crate:

```sh
cargo new lan-address-example
cd lan-address-example
```

Use the following dependency configuration. No
async runtime, TLS provider or remote server is needed for the standalone
examples.

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false, features = ["net-status"] }
```

Each Rust block below is a complete, separate `src/main.rs`. Copy one block
at a time and run `cargo run`. Actual results depend on the host's interfaces
and permissions. Running the example does not start network monitoring.

Addresses, interface names and raw collection diagnostics can reveal the host's
network environment. These examples print counts and selection state. Read an
address only for the application's requested LAN feature; keep the raw inventory
and diagnostic text out of logs, telemetry and published support reports unless
the application has explicitly reviewed and sanitized them.

## network::lan_ipv4_snapshot()

Collect a fresh inventory without creating an engine.

### Required features

`net-status`; use the dependency configuration above.

### Function signature

```text
pub fn lan_ipv4_snapshot() -> Result<LanIpv4Snapshot, NetError>
```

Import the function and LAN value types from `open_net::network`; `NetError`
is available at the crate root.

### Steps

Call the function, inspect `completeness()` and `diagnostics()`, then enumerate
`candidates()` or use `recommended()`. Handle a missing recommendation explicitly.
Before displaying or advertising a chosen endpoint, apply your application's
binding, port and peer-connectivity checks separately.

### Parameters

None. The function collects addresses from the current host/network namespace;
it has no destination, timeout or interface-filter argument.

### Callback parameters

Not applicable. There is no subscription and no callback is registered.

### Return values

| Value | Meaning |
| --- | --- |
| `Ok(snapshot)` with `Complete` | No collector failure was reported. Optional facts can still be unknown. |
| `Ok(snapshot)` with `Partial` | Address enumeration succeeded, but auxiliary platform evidence had failures. Candidates and diagnostics remain available. |
| `Ok(snapshot)` with empty candidates | Enumeration succeeded with no IPv4 addresses. |
| `recommended() == None` | No candidate passed the default recommendation policy; this is distinct from enumeration failure. |
| `Err(error)` | Address enumeration failed; the original I/O error remains an error source. |

### Errors and behavior

The call blocks its caller while reading the OS. It needs no running monitor,
engine, runtime or Internet connection. It does not modify network snapshots,
advance revisions or emit events, and it remains usable after an unrelated
engine closes. It does not guarantee a successful socket bind or access from a
phone or other LAN peer.

Do not interpret Partial, unknown metadata or a failed query as proof that an
existing healthy endpoint disappeared. The result is a collection window, not
an atomic transaction or a lease on an address. See the evidence and lifecycle
sections below before using a result to change an advertised endpoint.

### Example

```rust
fn main() -> open_net::Result<()> {
    let lan = open_net::network::lan_ipv4_snapshot()?;
    println!("collection={:?}, candidates={}, diagnostic_count={}",
        lan.completeness(), lan.candidates().len(), lan.diagnostics().len());
    if let Some(selected) = lan.recommended() {
        println!("recommendation available: {:?}", selected.recommendation_reason());
    } else {
        println!("No default recommendation; inspect the candidate evidence");
    }
    Ok(())
}
```

## Inventory and evidence

The candidate list retains every IPv4 address returned by the address enumerator,
including entries excluded by the default recommendation policy. It is not an
inventory of every OS address state: Windows, for example, omits addresses that
have not reached the preferred DAD state. It contains no IPv6 entries or interfaces
without IPv4 addresses. Multiple addresses on one interface and the same IP on
different interfaces are preserved.

Each entry exposes interface identity/name/index, address/mask/prefix length,
interface kind, operational status, point-to-point status, native link evidence,
default-route evidence, and all applicable default exclusions. The interface ID
is a Windows adapter name or Unix interface name, not a permanent hardware ID;
native indices can be reused. On Windows the mask is derived from the unicast
address's on-link prefix, not the adapter prefix list's first matching host prefix.
An invalid native on-link prefix returns an I/O `InvalidData` error.

Unknown optional facts remain `None` or `Unknown`. In particular, the Unix
`IFF_RUNNING` flag used by `if-addrs` does not prove physical carrier. A bridge
can be reported operational Up while native link evidence says inactive.

- macOS: SystemConfiguration interface types, Link/Active and IPv4 PrimaryInterface.
- Windows: IP Helper interface/media facts and lowest connected IPv4 default
  route metric plus interface metric. Ties can have multiple default interfaces.
- Linux: sysfs interface type/carrier/operstate and lowest UP, non-rejected default
  route metric in `/proc/net/route`. This is the current namespace's main IPv4
  routing table; policy routing and destination-specific paths are outside its scope.

Missing default-route information is unknown, not a negative fact. A successfully
read Linux/Windows table with no default route is a known absence. No default
Internet route is required for a LAN candidate to be recommended.

## Snapshot and candidate fields

The return values own ordinary Rust data; they do not retain an engine, native
handle or subscription. Read them through methods. `recommended()` borrows a
candidate from its snapshot; `address()` returns a copied `Ipv4Addr` if you only
need the address after dropping the snapshot.

| `LanIpv4Snapshot` method | Return type | Meaning |
| --- | --- | --- |
| `candidates()` | `&[LanIpv4Candidate]` | All enumerated entries, including excluded ones; sorted by interface identity and address, with no IP-only deduplication. |
| `recommended()` | `Option<&LanIpv4Candidate>` | Zero or one default recommendation from those same entries. Partial snapshots can still have one. |
| `completeness()` | `LanIpv4Completeness` | `Complete` or `Partial`; a collector-completion statement, not a connectivity score. |
| `diagnostics()` | `&[String]` | Human-readable collection failures. Display or log them; do not parse their wording as a stable protocol. |
| `query_started_at()` | `SystemTime` | Wall-clock start of collection. |
| `observed_at()` | `SystemTime` | Wall-clock end of collection; clock corrections can make it earlier than the start. |

| `LanIpv4Candidate` method | Return type | Meaning |
| --- | --- | --- |
| `interface_id()` / `interface_name()` | `&str` | Platform identity and display name. Identity is not a permanent hardware ID. |
| `interface_index()` | `Option<u32>` | Native index when known; the OS can reuse it. |
| `address()` / `netmask()` | `Ipv4Addr` | Assigned IPv4 address and its subnet mask. |
| `prefix_len()` | `u8` | Network prefix length, such as 24 for a `/24` subnet. |
| `interface_kind()` | `LanInterfaceKind` | `Wifi`, `Ethernet`, `Bridge`, `Tunnel`, `Virtual`, `Loopback`, or `Unknown`. |
| `is_oper_up()` | `Option<bool>` | Operational evidence from address enumeration; it is separate from physical carrier. |
| `is_point_to_point()` | `Option<bool>` | Whether the interface is reported as point-to-point. |
| `link_active()` | `Option<bool>` | Additional native link/media evidence. |
| `is_default_route()` | `Option<bool>` | Preferred IPv4 default-interface evidence; not a route lookup for a particular destination. |
| `default_exclusions()` | `&[LanIpv4Exclusion]` | Every applicable default-policy exclusion, in deterministic order. An empty list is not a reachability test. |
| `recommendation_reason()` | `Option<LanIpv4RecommendationReason>` | The strongest evidence for the selected entry; absent on all other entries. |
| `is_recommended()` | `bool` | Whether this is the entry returned by `recommended()`. |

For optional booleans, `Some(false)` is known negative evidence; `None` is
unknown. Do not silently convert `None` to `false` in diagnostics or selection
logic. The LAN enums are non-exhaustive, so downstream `match` expressions need
a wildcard arm. Applications read these snapshots; they do not construct their
private fields.

Recommendation reasons are `ActiveLanDefaultRoute`, `ActiveWifiOrEthernet`,
`ActiveBridge`, `LanLinkUnknown`, `MetadataUnknown`, and `BridgeLinkUnknown`.
The latter three identify fallbacks with incomplete auxiliary evidence; none
is a guarantee of connectivity. Exclusions distinguish interface state, virtual
or tunnel policy, link inactivity and address-range policy. Inspect the typed
variants instead of parsing Debug output.

## Default recommendation

The policy excludes explicitly down/inactive, operationally unknown, point-to-point,
name-hinted virtual, and platform-confirmed virtual/tunnel interfaces. It also
excludes loopback, link-local, unspecified, broadcast, multicast, 0/8, 240/4,
100.64/10 and 198.18/15 addresses. Exclusions express this advertisement policy;
applications with confirmed VPN/CGNAT requirements can inspect the retained entries
and make their own selection. An empty exclusion list is not a connectivity test.

Among eligible entries, selection prefers active LAN interfaces with default-route
evidence, then active Wi-Fi/Ethernet, then active bridges. Missing auxiliary
metadata allows a documented lower-evidence fallback when the basic operational
state is known Up. Active legal bridges, bonds and container LAN interfaces are
not excluded merely because they are bridges or lack a physical device record.

Interface evidence precedes address category. The three private ranges
192.168/16, 10/8 and 172.16/12 have equal preference; private addresses precede
other eligible addresses only at the same evidence level. Ties use interface ID
and address, rather than OS enumeration order. Exactly zero or one entry has
`is_recommended() == true`; `recommended()` refers to that same entry.

For example, this policy prefers an active LAN interface over an inactive
bridge when the native facts establish those roles. No particular interface
name or host address is required by the algorithm. Selection
does not guarantee a successful bind or reachability from a phone/peer; firewall,
AP isolation, routing and network changes still matter.

## Failure and lifecycle contracts

- Address enumeration failure returns `Err`, retaining its original I/O source.
- Successful empty IPv4 enumeration returns an empty snapshot with no recommendation.
- Auxiliary metadata failure retains candidates, returns `Partial`, and includes
  human-readable diagnostics. `Complete` means no reported collector failure, not
  that every optional fact is known or that all sources were read atomically.
- Query start/end wall-clock timestamps describe a collection window. Clock
  adjustments can reverse their order; a snapshot grants no validity lease.
- A `Partial` or failed query must not be treated as proof that an application's
  existing healthy endpoint disappeared. This API never rebinds a listener or
  withdraws an advertisement.

The client method works while monitoring is Stopped, Starting, Running, or in an
ordinary Failed state. It does not start monitoring, change `NetworkSnapshot`,
advance revisions, or emit network events. A closed/destroyed client and its clones,
or a closing engine, reject new queries with `Closed`. Already admitted synchronous
queries may finish; engine/client shutdown does not wait for the caller's read-only
OS query. The standalone function remains usable independently of engine lifecycle.

`on_event` notifications do not cover every LAN-only change. Applications
needing automatic DHCP/interface tracking must perform explicit fresh sampling;
the API does not provide a LAN subscription. Listener, UDP advertisement and QR
endpoint coordination remain application responsibilities.

## Choosing an address and validating behavior

`network::preferred_lan_ipv4()` uses interface-name/subnet filtering,
192.168 > 10 > 172.16/12 priority, and an enumeration-order tie break.
For selection based on native interface and route evidence, use
`lan_ipv4_snapshot()?.recommended()` (or the client method);
handle a missing recommendation explicitly instead of inventing a fallback address.

For a caller that needs only `Option<Ipv4Addr>`, the following
complete example extracts the recommended address from the snapshot.
It reports the collection state and diagnostic count without printing network
addresses or raw diagnostic text.

```rust
use open_net::network::lan_ipv4_snapshot;

fn main() -> open_net::Result<()> {
    let snapshot = lan_ipv4_snapshot()?;
    println!("Collection: {:?}; diagnostic count: {}",
        snapshot.completeness(), snapshot.diagnostics().len());
    let address = snapshot.recommended().map(|candidate| candidate.address());
    match address {
        Some(_address) => println!("A LAN address was selected for application use"),
        None => println!("No default address selected; keep application policy explicit"),
    }
    Ok(())
}
```

Deterministic SDK fixtures cover the inactive-bridge case, private-prefix parity,
VPN/default routes, active bridges, unknown/failed metadata, address boundaries,
enumeration order, and repeated IPs. Validate the public API and lifecycle on the
platforms your application supports, including failure and recovery behavior.
Platform parser tests and cross-compilation are not substitutes for native
execution or actual peer connectivity tests.

Native ownership and the exact unsafe audit records are described in
[LAN native safety](lan_ipv4_native_safety.md).

# Third-Party Dependencies

Sources: [Cargo.toml](../../Cargo.toml) and `open-net/Cargo.lock` in this checkout. The lockfile is a local, Git-ignored
resolution file; regenerate it with Cargo when using a fresh checkout.
Required features: none to read this inventory; the tables describe feature and target conditions.
Example dependencies: none; the review commands require Cargo and Python 3.11 or later.

The manifest contains **37 distinct direct dependency names**
(40 declarations across normal/build/dev kinds). The lockfile contains
**258 third-party package records**, including multiple versions of the same crate.
A Cargo version requirement is distinct from a resolved version: Tokio's declared
`1.53.1` currently resolves to `1.53.2`.

The lockfile is a repository resolution snapshot. It includes optional features,
test/build tools and other platforms; it is not the set every downstream
application compiles. Consumers resolve their own dependency graph. Normal means
library dependency, build means compilation-time dependency, and dev means this
crate's own tests/examples. Default `ws-client` enables `net-status`; HTTP extension
features enable `http-client`, which also enables `net-status`. Feature conditions
below identify direct activation; transitive feature activation also applies.
Platform conditions apply in addition to feature conditions. `All` means there is
no target selector on the declaration, not a promise of support for every target.

`libs/common` is internal source, not a third-party package. Demo, server and
fixture lockfiles are excluded. Unenabled experimental patches are not attributed
to this release graph. Registry sources identify package provenance. The source is
not modified by the commands below.

## Direct Dependencies

| Crate | Cargo requirement | Locked version | Purpose | Kind | Feature condition | Platform condition |
| --- | --- | --- | --- | --- | --- | --- |
| async-trait | 0.1.92 | 0.1.92 | Async HTTP request/authentication trait adapters | normal | `http-client` | All |
| base64 | 0.22.1 | 0.22.1 | WebSocket handshake and optional HTTP Basic authentication | normal | `ws-client`, `http-client-auth` | All |
| bytes | 1.12.1 | 1.12.1 | Shared owned byte buffers | normal | Always for this dependency kind | All |
| cc | 1.4.4 | 1.6.0 | Native monitor build support | build | `net-status` | All |
| chrono | 0.4.45 | 0.4.45 | HTTP date and retry timing support | normal | `http-client` | All |
| cookie | =0.18.2 | 0.18.2 | Cookie parsing | normal | `http-client-cookies` | All |
| cookie_store | =0.22.1 | 0.22.1 | RFC 6265 cookie storage | normal | `http-client-cookies` | All |
| futures | 0.3.34 | 0.3.34 | Async traits and utilities; test executor in dev builds | dev | Always for this dependency kind | All |
| futures | 0.3.34 | 0.3.34 | Async traits and utilities; test executor in dev builds | normal | Always for this dependency kind | All |
| futures-util | 0.3.34 | 0.3.34 | Async stream and future utilities | normal | Always for this dependency kind | All |
| getrandom | 0.3.4 | 0.3.4 | WebSocket and multipart randomness | normal | `ws-client`, `http-client-codecs` | All |
| h2 | 0.4.19 | 0.4.20 | HTTP/2 test server support | dev | Always for this dependency kind | All |
| http | 1.5.0 | 1.5.0 | HTTP methods, headers, status codes | normal | Always for this dependency kind | All |
| httparse | 1.10.1 | 1.10.1 | WebSocket HTTP upgrade parsing | normal | `ws-client` | All |
| hyper | 1.11.1 | 1.12.0 | HTTP transport error classification and protocol support | normal | `http-client` | All |
| hyper-util | =0.1.20 | 0.1.20 | HTTP transport integration | normal | `http-client` | All |
| if-addrs | 0.15.0 | 0.15.0 | Interface and LAN address discovery | normal | `net-status` | All |
| indexmap | 2.14.1 | 2.14.2 | Ordered maps | normal | Always for this dependency kind | All |
| n0-watcher | =1.0.0-rc.0 | 1.0.0-rc.0 | Network observation subscriptions | normal | `net-status` | All |
| netwatch | 0.18.0 | 0.18.0 | Native network change observation | normal | `net-status` | All |
| proc-macro2 | 1.0.107 | 1.0.107 | API syntax/span checks in tests | dev | Always for this dependency kind | All |
| psl | =2.1.240 | 2.1.240 | Public suffix checks for cookies | normal | `http-client-cookies` | All |
| reqwest | 0.12.28 | 0.12.28 | HTTP transport | normal | `http-client` | All |
| rustls | 0.23.43 | 0.23.45 | TLS configuration and AWS-LC provider | normal | `ws-client`, `http-client` | All |
| serde | 1.0.229 | 1.0.229 | Serialization traits and derives | normal | Always for this dependency kind | All |
| serde_json | 1.0.151 | 1.0.151 | JSON values, encoding, decoding, logging | normal | Always for this dependency kind | All |
| serde_qs | 1.1.3 | 1.1.3 | Query and form request encoding | normal | `http-client-codecs` | All |
| socket2 | 0.6.5 | 0.6.5 | WebSocket socket configuration | normal | `ws-client` | All |
| tokio | 1.53.1 | 1.53.2 | Async runtime, scheduling, synchronization | dev | Always for this dependency kind | All |
| tokio | 1.53.1 | 1.53.2 | Async runtime, scheduling, synchronization | normal | Always for this dependency kind | All |
| tokio-rustls | 0.26.4 | 0.26.6 | WebSocket TLS; TLS server fixtures in dev builds | dev | Always for this dependency kind | All |
| tokio-rustls | 0.26.4 | 0.26.6 | WebSocket TLS; TLS server fixtures in dev builds | normal | `ws-client` | All |
| tokio-tungstenite | 0.30.0 | 0.30.0 | Asynchronous WebSocket protocol | normal | `ws-client` | All |
| tokio-util | 0.7.19 | 0.7.20 | WebSocket cancellation support | normal | `ws-client` | All |
| tower-layer | =0.3.3 | 0.3.3 | HTTP transport connector layering | normal | `http-client` | All |
| tower-service | =0.3.3 | 0.3.3 | HTTP transport service interface | normal | `http-client` | All |
| url | 2.5.7 | 2.5.8 | Cookie URL context | normal | `http-client-cookies` | All |
| uuid | 1.26.1 | 1.27.0 | WebSocket identifiers | normal | `ws-client` | All |
| webpki-roots | 1.0.9 | 1.0.9 | Bundled TLS certificate roots | normal | `ws-client`, `http-client` | All |
| windows | 0.62.2 | 0.62.2 | Windows network-list APIs | normal | `net-status` | cfg(windows) |



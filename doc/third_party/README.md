# Third-Party Dependencies

Sources: [Cargo.toml](../../Cargo.toml) and the library crate's `Cargo.lock` in this checkout.
In the Git repository, this lockfile is a local, Git-ignored resolution file.
A fresh checkout may need Cargo to generate it and may resolve different exact versions.
Required features: none to read this inventory; the tables describe feature and target conditions.
Example dependencies: none; the review commands require Cargo and Python 3.11 or later.

The manifest contains **39 distinct direct dependency names**
(41 declarations across normal/build/dev kinds). The lockfile contains
**261 third-party package records**, including multiple versions of the same crate.
A Cargo version requirement is distinct from a resolved version: rustls's declared
`0.23.43` resolves to `0.23.45` in this checkout.
Requirements without a leading `=` allow compatible releases; a requirement such
as `=0.4.2` pins that dependency to the exact version. The Locked version column
records this checkout's resolution, not a version imposed on every consumer.

The lockfile is a repository resolution snapshot. It includes optional features,
test/build tools and other platforms; it is not the set every downstream
application compiles. Consumers resolve their own dependency graph. Normal means
library dependency, build means compilation-time dependency, and dev means this
crate's own tests/examples. Default `ws-client` enables `net-status`; HTTP extension
features enable `http-client`, which also enables `net-status`. Feature conditions
below identify direct activation; transitive feature activation also applies.
Platform conditions apply in addition to feature conditions. `All` means there is
no target selector on the declaration, not a promise of support for every target.

## Dependency roles

`net-status` uses `core-foundation` on macOS and `windows` on Windows for native
interface, link and default-route queries. The Windows dependency also supports
the network refresh helper.

Production async utilities use `futures-util` with default features disabled and
`std` enabled; `ws-client` explicitly enables `futures-util/sink`. The SDK declares
`futures` only as a dev-dependency for test executors, polling macros and async utilities.
Ordered log fields use standard-library `HashMap` and `Vec` containers with Serde
serialization, preserving insertion order and last-value replacement for repeated
field names. In the resolved inventory, `futures` supports tests and is also pulled in by
`netwatch` through `wmi` on Windows. `indexmap` is a transitive dependency
of `h2` and macOS's `netwatch` through `netdev` and `plist`.

With all features disabled, the six direct library dependencies are `bytes`,
`futures-util`, `http`, `serde`, `serde_json` and `tokio`.

HTTP and WebSocket clients require an application-selected crypto provider,
either supplied explicitly or installed as the process default. The SDK's normal
and build dependency graph does not select AWS-LC, ring or OpenSSL. The OpenSSL
packages in this inventory support SDK tests/examples through `rustls-openssl`;
the `ring` lockfile entry alone does not mean a production build compiles it.
Another application dependency can still enable provider features because Cargo
features are additive. See [TLS configuration](../tls.md) for initialization,
provider precedence, plain HTTP/WS constructor behavior and build requirements.

`libs/common` is internal source, not a third-party package. Demo, server and
fixture lockfiles are excluded. Unenabled experimental patches are excluded from
this dependency graph. Registry sources identify package provenance.
This inventory lists dependencies and sources; individual crates retain their
own license terms.

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
| core-foundation | 0.10.1 | 0.10.1 | macOS Core Foundation ownership for interface, link and default-route queries | normal | `net-status` | cfg(target_os = "macos") |
| ctor | 0.2.9 | 0.2.9 | Test-process crypto provider initialization | dev | Always for this dependency kind | All |
| futures | 0.3.34 | 0.3.34 | Test executor, polling macros and async utilities | dev | Always for this dependency kind | All |
| futures-util | 0.3.34 | 0.3.34 | Production async traits, streams, futures and WebSocket sink utilities | normal | Always for this dependency kind | All |
| getrandom | 0.3.4 | 0.3.4 | WebSocket and multipart randomness | normal | `ws-client`, `http-client-codecs` | All |
| h2 | 0.4.19 | 0.4.20 | HTTP/2 test server support | dev | Always for this dependency kind | All |
| http | 1.5.0 | 1.5.0 | HTTP methods, headers, status codes | normal | Always for this dependency kind | All |
| httparse | 1.10.1 | 1.10.1 | WebSocket HTTP upgrade parsing | normal | `ws-client` | All |
| hyper | 1.11.1 | 1.12.0 | HTTP transport error classification and protocol support | normal | `http-client` | All |
| hyper-util | =0.1.20 | 0.1.20 | HTTP transport integration | normal | `http-client` | All |
| if-addrs | 0.15.0 | 0.15.0 | Interface and LAN address discovery | normal | `net-status` | All |
| n0-watcher | =1.0.0-rc.0 | 1.0.0-rc.0 | Network observation subscriptions | normal | `net-status` | All |
| netwatch | 0.18.0 | 0.18.0 | Native network change observation | normal | `net-status` | All |
| proc-macro2 | 1.0.107 | 1.0.107 | API syntax/span checks in tests | dev | Always for this dependency kind | All |
| psl | =2.1.240 | 2.1.240 | Public suffix checks for cookies | normal | `http-client-cookies` | All |
| reqwest | 0.12.28 | 0.12.28 | HTTP transport | normal | `http-client` | All |
| rustls | 0.23.43 | 0.23.45 | TLS protocol and provider configuration | normal | `ws-client`, `http-client` | All |
| rustls-openssl | =0.4.2 | 0.4.2 | Application-selected OpenSSL provider for test peers | dev | Always for this dependency kind | All |
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
| windows | 0.62.2 | 0.62.2 | Windows network-list, refresh-helper pipes, interface/link and IPv4 route APIs | normal | `net-status` | cfg(windows) |

## Resolved Dependency Inventory

| Crate | Exact version | Source |
| --- | --- | --- |
| aho-corasick | 1.1.5 | `registry+https://github.com/rust-lang/crates.io-index` |
| android_system_properties | 0.1.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| async-trait | 0.1.92 | `registry+https://github.com/rust-lang/crates.io-index` |
| atomic-waker | 1.1.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| autocfg | 1.5.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| base64 | 0.22.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| base64 | 0.23.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| bitflags | 2.13.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| block-buffer | 0.12.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| block2 | 0.6.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| bumpalo | 3.20.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| bytes | 1.12.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| cc | 1.6.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| cfg-if | 1.0.5 | `registry+https://github.com/rust-lang/crates.io-index` |
| cfg_aliases | 0.2.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| chacha20 | 0.10.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| chrono | 0.4.45 | `registry+https://github.com/rust-lang/crates.io-index` |
| const-oid | 0.10.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| convert_case | 0.10.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| cookie | 0.18.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| cookie_store | 0.22.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| cordyceps | 0.3.5 | `registry+https://github.com/rust-lang/crates.io-index` |
| core-foundation | 0.10.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| core-foundation-sys | 0.8.7 | `registry+https://github.com/rust-lang/crates.io-index` |
| cpufeatures | 0.3.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| crypto-common | 0.2.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| ctor | 0.2.9 | `registry+https://github.com/rust-lang/crates.io-index` |
| data-encoding | 2.11.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| deranged | 0.5.8 | `registry+https://github.com/rust-lang/crates.io-index` |
| derive_more | 2.1.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| derive_more-impl | 2.1.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| diatomic-waker | 0.2.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| digest | 0.11.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| dispatch2 | 0.3.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| displaydoc | 0.2.7 | `registry+https://github.com/rust-lang/crates.io-index` |
| dlopen2 | 0.8.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| document-features | 0.2.12 | `registry+https://github.com/rust-lang/crates.io-index` |
| equivalent | 1.0.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| errno | 0.3.14 | `registry+https://github.com/rust-lang/crates.io-index` |
| fastrand | 2.5.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| find-msvc-tools | 0.1.14 | `registry+https://github.com/rust-lang/crates.io-index` |
| fnv | 1.0.7 | `registry+https://github.com/rust-lang/crates.io-index` |
| foreign-types | 0.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| foreign-types-shared | 0.1.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| form_urlencoded | 1.2.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| futures | 0.3.34 | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-buffered | 0.2.13 | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-channel | 0.3.34 | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-core | 0.3.34 | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-executor | 0.3.34 | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-io | 0.3.34 | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-lite | 2.6.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-macro | 0.3.34 | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-sink | 0.3.34 | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-task | 0.3.34 | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-util | 0.3.34 | `registry+https://github.com/rust-lang/crates.io-index` |
| generator | 0.8.10 | `registry+https://github.com/rust-lang/crates.io-index` |
| getrandom | 0.2.17 | `registry+https://github.com/rust-lang/crates.io-index` |
| getrandom | 0.3.4 | `registry+https://github.com/rust-lang/crates.io-index` |
| getrandom | 0.4.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| h2 | 0.4.20 | `registry+https://github.com/rust-lang/crates.io-index` |
| hashbrown | 0.17.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| http | 1.5.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| http-body | 1.1.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| http-body-util | 0.1.5 | `registry+https://github.com/rust-lang/crates.io-index` |
| httparse | 1.10.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| hybrid-array | 0.4.15 | `registry+https://github.com/rust-lang/crates.io-index` |
| hyper | 1.12.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| hyper-rustls | 0.27.10 | `registry+https://github.com/rust-lang/crates.io-index` |
| hyper-util | 0.1.20 | `registry+https://github.com/rust-lang/crates.io-index` |
| iana-time-zone | 0.1.65 | `registry+https://github.com/rust-lang/crates.io-index` |
| iana-time-zone-haiku | 0.1.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_collections | 2.3.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_locale_core | 2.3.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_normalizer | 2.3.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_normalizer_data | 2.3.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_properties | 2.3.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_properties_data | 2.3.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_provider | 2.3.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| idna | 1.1.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| idna_adapter | 1.2.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| if-addrs | 0.15.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| indexmap | 2.14.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| ipnet | 2.12.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| itoa | 1.0.18 | `registry+https://github.com/rust-lang/crates.io-index` |
| js-sys | 0.3.106 | `registry+https://github.com/rust-lang/crates.io-index` |
| lazy_static | 1.5.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| libc | 0.2.190 | `registry+https://github.com/rust-lang/crates.io-index` |
| litemap | 0.8.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| litrs | 1.0.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| log | 0.4.34 | `registry+https://github.com/rust-lang/crates.io-index` |
| loom | 0.7.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| mac-addr | 0.3.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| matchers | 0.2.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| memchr | 2.8.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| mio | 1.2.4 | `registry+https://github.com/rust-lang/crates.io-index` |
| n0-error | 1.0.0-rc.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| n0-error-macros | 1.0.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| n0-future | 0.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| n0-watcher | 1.0.0-rc.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| netdev | 0.43.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| netlink-packet-core | 0.8.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| netlink-packet-route | 0.29.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| netlink-packet-route | 0.30.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| netlink-proto | 0.12.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| netlink-sys | 0.8.8 | `registry+https://github.com/rust-lang/crates.io-index` |
| netwatch | 0.18.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| noq-udp | 1.0.0-rc.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| nu-ansi-term | 0.50.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| num-conv | 0.2.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| num-traits | 0.2.19 | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2 | 0.6.5 | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-core-foundation | 0.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-core-wlan | 0.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-encode | 4.1.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-foundation | 0.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-security | 0.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-security-foundation | 0.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-system-configuration | 0.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| once_cell | 1.21.4 | `registry+https://github.com/rust-lang/crates.io-index` |
| openssl | 0.10.81 | `registry+https://github.com/rust-lang/crates.io-index` |
| openssl-macros | 0.1.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| openssl-sys | 0.9.117 | `registry+https://github.com/rust-lang/crates.io-index` |
| parking | 2.2.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| paste | 1.0.15 | `registry+https://github.com/rust-lang/crates.io-index` |
| percent-encoding | 2.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| pin-project | 1.1.13 | `registry+https://github.com/rust-lang/crates.io-index` |
| pin-project-internal | 1.1.13 | `registry+https://github.com/rust-lang/crates.io-index` |
| pin-project-lite | 0.2.17 | `registry+https://github.com/rust-lang/crates.io-index` |
| pkg-config | 0.3.34 | `registry+https://github.com/rust-lang/crates.io-index` |
| plist | 1.10.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| potential_utf | 0.1.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| powerfmt | 0.2.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| proc-macro2 | 1.0.107 | `registry+https://github.com/rust-lang/crates.io-index` |
| psl | 2.1.240 | `registry+https://github.com/rust-lang/crates.io-index` |
| psl-types | 2.0.11 | `registry+https://github.com/rust-lang/crates.io-index` |
| quick-xml | 0.42.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| quote | 1.0.47 | `registry+https://github.com/rust-lang/crates.io-index` |
| r-efi | 5.3.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| r-efi | 6.0.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| rand | 0.10.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| rand_core | 0.10.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| regex-automata | 0.4.18 | `registry+https://github.com/rust-lang/crates.io-index` |
| regex-syntax | 0.8.11 | `registry+https://github.com/rust-lang/crates.io-index` |
| reqwest | 0.12.28 | `registry+https://github.com/rust-lang/crates.io-index` |
| ring | 0.17.14 | `registry+https://github.com/rust-lang/crates.io-index` |
| rustc_version | 0.4.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| rustls | 0.23.45 | `registry+https://github.com/rust-lang/crates.io-index` |
| rustls-openssl | 0.4.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| rustls-pki-types | 1.15.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| rustls-webpki | 0.103.15 | `registry+https://github.com/rust-lang/crates.io-index` |
| rustversion | 1.0.23 | `registry+https://github.com/rust-lang/crates.io-index` |
| ryu | 1.0.23 | `registry+https://github.com/rust-lang/crates.io-index` |
| scoped-tls | 1.0.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| semver | 1.0.28 | `registry+https://github.com/rust-lang/crates.io-index` |
| send_wrapper | 0.6.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| serde | 1.0.229 | `registry+https://github.com/rust-lang/crates.io-index` |
| serde_core | 1.0.229 | `registry+https://github.com/rust-lang/crates.io-index` |
| serde_derive | 1.0.229 | `registry+https://github.com/rust-lang/crates.io-index` |
| serde_json | 1.0.151 | `registry+https://github.com/rust-lang/crates.io-index` |
| serde_qs | 1.1.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| serde_urlencoded | 0.7.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| sha1 | 0.11.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| sharded-slab | 0.1.7 | `registry+https://github.com/rust-lang/crates.io-index` |
| shlex | 2.0.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| signal-hook-registry | 1.4.8 | `registry+https://github.com/rust-lang/crates.io-index` |
| slab | 0.4.12 | `registry+https://github.com/rust-lang/crates.io-index` |
| smallvec | 1.16.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| socket2 | 0.6.5 | `registry+https://github.com/rust-lang/crates.io-index` |
| spez | 0.1.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| spin | 0.10.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| stable_deref_trait | 1.2.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| subtle | 2.6.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| syn | 2.0.119 | `registry+https://github.com/rust-lang/crates.io-index` |
| syn | 3.0.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| sync_wrapper | 1.0.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| synstructure | 0.14.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| thiserror | 2.0.21 | `registry+https://github.com/rust-lang/crates.io-index` |
| thiserror-impl | 2.0.21 | `registry+https://github.com/rust-lang/crates.io-index` |
| thread_local | 1.1.10 | `registry+https://github.com/rust-lang/crates.io-index` |
| time | 0.3.55 | `registry+https://github.com/rust-lang/crates.io-index` |
| time-core | 0.1.9 | `registry+https://github.com/rust-lang/crates.io-index` |
| time-macros | 0.2.32 | `registry+https://github.com/rust-lang/crates.io-index` |
| tinystr | 0.8.4 | `registry+https://github.com/rust-lang/crates.io-index` |
| tokio | 1.53.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| tokio-macros | 2.7.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| tokio-rustls | 0.26.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| tokio-tungstenite | 0.30.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| tokio-util | 0.7.20 | `registry+https://github.com/rust-lang/crates.io-index` |
| tower | 0.5.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| tower-http | 0.6.11 | `registry+https://github.com/rust-lang/crates.io-index` |
| tower-layer | 0.3.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| tower-service | 0.3.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| tracing | 0.1.44 | `registry+https://github.com/rust-lang/crates.io-index` |
| tracing-attributes | 0.1.31 | `registry+https://github.com/rust-lang/crates.io-index` |
| tracing-core | 0.1.36 | `registry+https://github.com/rust-lang/crates.io-index` |
| tracing-log | 0.2.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| tracing-subscriber | 0.3.23 | `registry+https://github.com/rust-lang/crates.io-index` |
| try-lock | 0.2.5 | `registry+https://github.com/rust-lang/crates.io-index` |
| tungstenite | 0.30.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| typenum | 1.20.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| unicode-ident | 1.0.26 | `registry+https://github.com/rust-lang/crates.io-index` |
| unicode-segmentation | 1.13.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| unicode-xid | 0.2.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| untrusted | 0.9.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| url | 2.5.8 | `registry+https://github.com/rust-lang/crates.io-index` |
| utf8_iter | 1.0.4 | `registry+https://github.com/rust-lang/crates.io-index` |
| uuid | 1.27.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| valuable | 0.1.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| vcpkg | 0.2.15 | `registry+https://github.com/rust-lang/crates.io-index` |
| version_check | 0.9.5 | `registry+https://github.com/rust-lang/crates.io-index` |
| want | 0.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| wasi | 0.11.1+wasi-snapshot-preview1 | `registry+https://github.com/rust-lang/crates.io-index` |
| wasip2 | 1.0.4+wasi-0.2.12 | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-bindgen | 0.2.129 | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-bindgen-futures | 0.4.79 | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-bindgen-macro | 0.2.129 | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-bindgen-macro-support | 0.2.129 | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-bindgen-shared | 0.2.129 | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-streams | 0.4.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| web-sys | 0.3.106 | `registry+https://github.com/rust-lang/crates.io-index` |
| web-time | 1.1.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| webpki-roots | 0.26.11 | `registry+https://github.com/rust-lang/crates.io-index` |
| webpki-roots | 1.0.9 | `registry+https://github.com/rust-lang/crates.io-index` |
| winapi | 0.3.9 | `registry+https://github.com/rust-lang/crates.io-index` |
| winapi-i686-pc-windows-gnu | 0.4.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| winapi-x86_64-pc-windows-gnu | 0.4.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows | 0.62.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-collections | 0.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-core | 0.62.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-future | 0.3.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-implement | 0.60.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-interface | 0.59.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-link | 0.2.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-numerics | 0.3.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-result | 0.4.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-strings | 0.5.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-sys | 0.52.0 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-sys | 0.61.2 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-targets | 0.52.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-threading | 0.2.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_aarch64_gnullvm | 0.52.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_aarch64_msvc | 0.52.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_i686_gnu | 0.52.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_i686_gnullvm | 0.52.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_i686_msvc | 0.52.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_x86_64_gnu | 0.52.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_x86_64_gnullvm | 0.52.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_x86_64_msvc | 0.52.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| wit-bindgen | 0.57.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| wmi | 0.18.4 | `registry+https://github.com/rust-lang/crates.io-index` |
| writeable | 0.6.4 | `registry+https://github.com/rust-lang/crates.io-index` |
| yoke | 0.8.3 | `registry+https://github.com/rust-lang/crates.io-index` |
| yoke-derive | 0.8.4 | `registry+https://github.com/rust-lang/crates.io-index` |
| zerofrom | 0.1.8 | `registry+https://github.com/rust-lang/crates.io-index` |
| zerofrom-derive | 0.1.8 | `registry+https://github.com/rust-lang/crates.io-index` |
| zeroize | 1.9.1 | `registry+https://github.com/rust-lang/crates.io-index` |
| zerotrie | 0.2.5 | `registry+https://github.com/rust-lang/crates.io-index` |
| zerovec | 0.11.8 | `registry+https://github.com/rust-lang/crates.io-index` |
| zerovec-derive | 0.11.6 | `registry+https://github.com/rust-lang/crates.io-index` |
| zmij | 1.0.23 | `registry+https://github.com/rust-lang/crates.io-index` |

## Review commands

These commands are for a source checkout of the full repository, run from its
root. The `script/` tools are not included in the published library package.
Use Python 3.11 or later. Generate a missing library lockfile with Cargo before
running the inventory tool; the tool itself never updates the manifest or lockfile.
`--locked` refuses to change the resolution;
`--offline` can be added when all required dependencies are cached. The tree
commands help inspect conditions; the tables above remain a flat inventory.

```bash
# Regenerate the inventory from the local manifest and lockfile.
python3 script/developer_dependency_inventory.py
# Verify without rewriting it.
python3 script/developer_dependency_inventory.py --check
# Inspect normal and build dependencies for the local target and HTTP only.
cargo tree --manifest-path open-net/Cargo.toml --locked --no-default-features --features http-client -e normal,build
# Inspect all feature and target conditions, including test dependencies.
cargo tree --manifest-path open-net/Cargo.toml --locked --all-features --target all -e normal,build,dev
# Inspect Cargo's machine-readable dependency declarations.
cargo metadata --manifest-path open-net/Cargo.toml --locked --no-deps --format-version 1
```

Regenerate the inventory when dependency declarations or the local resolution change.

# Third-Party Dependencies

For direct dependency requirements, see [Cargo.toml](../../Cargo.toml).
Cargo records the exact packages selected for a build in its `Cargo.lock`.
The library lockfile is Git-ignored. A fresh checkout can generate it with
`cargo generate-lockfile` from the library crate directory. An application
can inspect its own lockfile for the versions used in its build.

The manifest contains **39 distinct direct dependency names**
(41 declarations across normal/build/dev kinds).

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

`http-client` uses the dependency-free `httpdate` crate and standard-library
`SystemTime` for Retry-After dates. The SDK no longer declares `chrono` directly;
it remains in the all-target lockfile because Windows monitoring uses it through
`netwatch` and `wmi`. Its presence there does not imply that macOS or Linux HTTP
consumers compile it.

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

| Crate | Purpose | Kind | Feature condition | Platform condition |
| --- | --- | --- | --- | --- |
| async-trait | Async HTTP request/authentication trait adapters | normal | `http-client` | All |
| base64 | WebSocket handshake and optional HTTP Basic authentication | normal | `ws-client`, `http-client-auth` | All |
| bytes | Shared owned byte buffers | normal | Always for this dependency kind | All |
| cc | Native monitor build support | build | `net-status` | All |
| cookie | Cookie parsing | normal | `http-client-cookies` | All |
| cookie_store | RFC 6265 cookie storage | normal | `http-client-cookies` | All |
| core-foundation | macOS Core Foundation ownership for interface, link and default-route queries | normal | `net-status` | cfg(target_os = "macos") |
| ctor | Test-process crypto provider initialization | dev | Always for this dependency kind | All |
| futures | Test executor, polling macros and async utilities | dev | Always for this dependency kind | All |
| futures-util | Production async traits, streams, futures and WebSocket sink utilities | normal | Always for this dependency kind | All |
| getrandom | WebSocket and multipart randomness | normal | `ws-client`, `http-client-codecs` | All |
| h2 | HTTP/2 test server support | dev | Always for this dependency kind | All |
| http | HTTP methods, headers, status codes | normal | Always for this dependency kind | All |
| httparse | WebSocket HTTP upgrade parsing | normal | `ws-client` | All |
| httpdate | HTTP Retry-After date parsing | normal | `http-client` | All |
| hyper | HTTP transport error classification and protocol support | normal | `http-client` | All |
| hyper-util | HTTP transport integration | normal | `http-client` | All |
| if-addrs | Interface and LAN address discovery | normal | `net-status` | All |
| n0-watcher | Network observation subscriptions | normal | `net-status` | All |
| netwatch | Native network change observation | normal | `net-status` | All |
| proc-macro2 | API syntax/span checks in tests | dev | Always for this dependency kind | All |
| psl | Public suffix checks for cookies | normal | `http-client-cookies` | All |
| reqwest | HTTP transport | normal | `http-client` | All |
| rustls | TLS protocol and provider configuration | normal | `ws-client`, `http-client` | All |
| rustls-openssl | Application-selected OpenSSL provider for test peers | dev | Always for this dependency kind | All |
| serde | Serialization traits and derives | normal | Always for this dependency kind | All |
| serde_json | JSON values, encoding, decoding, logging | normal | Always for this dependency kind | All |
| serde_qs | Query and form request encoding | normal | `http-client-codecs` | All |
| socket2 | WebSocket socket configuration | normal | `ws-client` | All |
| tokio | Async runtime, scheduling, synchronization | dev | Always for this dependency kind | All |
| tokio | Async runtime, scheduling, synchronization | normal | Always for this dependency kind | All |
| tokio-rustls | WebSocket TLS; TLS server fixtures in dev builds | dev | Always for this dependency kind | All |
| tokio-rustls | WebSocket TLS; TLS server fixtures in dev builds | normal | `ws-client` | All |
| tokio-tungstenite | Asynchronous WebSocket protocol | normal | `ws-client` | All |
| tokio-util | WebSocket cancellation support | normal | `ws-client` | All |
| tower-layer | HTTP transport connector layering | normal | `http-client` | All |
| tower-service | HTTP transport service interface | normal | `http-client` | All |
| url | Cookie URL context | normal | `http-client-cookies` | All |
| uuid | WebSocket identifiers | normal | `ws-client` | All |
| webpki-roots | Bundled TLS certificate roots | normal | `ws-client`, `http-client` | All |
| windows | Windows network-list, refresh-helper pipes, interface/link and IPv4 route APIs | normal | `net-status` | cfg(windows) |

## Resolved Dependency Inventory

Each row lists a crate name and source once. Cargo can resolve multiple
versions of the same crate from one source. Use the duplicate-dependency
command below to see which packages pull each one in.

| Crate | Source |
| --- | --- |
| aho-corasick | `registry+https://github.com/rust-lang/crates.io-index` |
| android_system_properties | `registry+https://github.com/rust-lang/crates.io-index` |
| async-trait | `registry+https://github.com/rust-lang/crates.io-index` |
| atomic-waker | `registry+https://github.com/rust-lang/crates.io-index` |
| autocfg | `registry+https://github.com/rust-lang/crates.io-index` |
| base64 | `registry+https://github.com/rust-lang/crates.io-index` |
| bitflags | `registry+https://github.com/rust-lang/crates.io-index` |
| block-buffer | `registry+https://github.com/rust-lang/crates.io-index` |
| block2 | `registry+https://github.com/rust-lang/crates.io-index` |
| bumpalo | `registry+https://github.com/rust-lang/crates.io-index` |
| bytes | `registry+https://github.com/rust-lang/crates.io-index` |
| cc | `registry+https://github.com/rust-lang/crates.io-index` |
| cfg-if | `registry+https://github.com/rust-lang/crates.io-index` |
| cfg_aliases | `registry+https://github.com/rust-lang/crates.io-index` |
| chacha20 | `registry+https://github.com/rust-lang/crates.io-index` |
| chrono | `registry+https://github.com/rust-lang/crates.io-index` |
| combine | `registry+https://github.com/rust-lang/crates.io-index` |
| const-oid | `registry+https://github.com/rust-lang/crates.io-index` |
| convert_case | `registry+https://github.com/rust-lang/crates.io-index` |
| cookie | `registry+https://github.com/rust-lang/crates.io-index` |
| cookie_store | `registry+https://github.com/rust-lang/crates.io-index` |
| cordyceps | `registry+https://github.com/rust-lang/crates.io-index` |
| core-foundation | `registry+https://github.com/rust-lang/crates.io-index` |
| core-foundation-sys | `registry+https://github.com/rust-lang/crates.io-index` |
| cpufeatures | `registry+https://github.com/rust-lang/crates.io-index` |
| crypto-common | `registry+https://github.com/rust-lang/crates.io-index` |
| ctor | `registry+https://github.com/rust-lang/crates.io-index` |
| data-encoding | `registry+https://github.com/rust-lang/crates.io-index` |
| deranged | `registry+https://github.com/rust-lang/crates.io-index` |
| derive_more | `registry+https://github.com/rust-lang/crates.io-index` |
| derive_more-impl | `registry+https://github.com/rust-lang/crates.io-index` |
| diatomic-waker | `registry+https://github.com/rust-lang/crates.io-index` |
| digest | `registry+https://github.com/rust-lang/crates.io-index` |
| dispatch2 | `registry+https://github.com/rust-lang/crates.io-index` |
| displaydoc | `registry+https://github.com/rust-lang/crates.io-index` |
| dlopen2 | `registry+https://github.com/rust-lang/crates.io-index` |
| document-features | `registry+https://github.com/rust-lang/crates.io-index` |
| equivalent | `registry+https://github.com/rust-lang/crates.io-index` |
| errno | `registry+https://github.com/rust-lang/crates.io-index` |
| fastrand | `registry+https://github.com/rust-lang/crates.io-index` |
| find-msvc-tools | `registry+https://github.com/rust-lang/crates.io-index` |
| fnv | `registry+https://github.com/rust-lang/crates.io-index` |
| foreign-types | `registry+https://github.com/rust-lang/crates.io-index` |
| foreign-types-shared | `registry+https://github.com/rust-lang/crates.io-index` |
| form_urlencoded | `registry+https://github.com/rust-lang/crates.io-index` |
| futures | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-buffered | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-channel | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-core | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-executor | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-io | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-lite | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-macro | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-sink | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-task | `registry+https://github.com/rust-lang/crates.io-index` |
| futures-util | `registry+https://github.com/rust-lang/crates.io-index` |
| generator | `registry+https://github.com/rust-lang/crates.io-index` |
| getrandom | `registry+https://github.com/rust-lang/crates.io-index` |
| h2 | `registry+https://github.com/rust-lang/crates.io-index` |
| hashbrown | `registry+https://github.com/rust-lang/crates.io-index` |
| http | `registry+https://github.com/rust-lang/crates.io-index` |
| http-body | `registry+https://github.com/rust-lang/crates.io-index` |
| http-body-util | `registry+https://github.com/rust-lang/crates.io-index` |
| httparse | `registry+https://github.com/rust-lang/crates.io-index` |
| httpdate | `registry+https://github.com/rust-lang/crates.io-index` |
| hybrid-array | `registry+https://github.com/rust-lang/crates.io-index` |
| hyper | `registry+https://github.com/rust-lang/crates.io-index` |
| hyper-rustls | `registry+https://github.com/rust-lang/crates.io-index` |
| hyper-util | `registry+https://github.com/rust-lang/crates.io-index` |
| iana-time-zone | `registry+https://github.com/rust-lang/crates.io-index` |
| iana-time-zone-haiku | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_collections | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_locale_core | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_normalizer | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_normalizer_data | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_properties | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_properties_data | `registry+https://github.com/rust-lang/crates.io-index` |
| icu_provider | `registry+https://github.com/rust-lang/crates.io-index` |
| idna | `registry+https://github.com/rust-lang/crates.io-index` |
| idna_adapter | `registry+https://github.com/rust-lang/crates.io-index` |
| if-addrs | `registry+https://github.com/rust-lang/crates.io-index` |
| indexmap | `registry+https://github.com/rust-lang/crates.io-index` |
| ipnet | `registry+https://github.com/rust-lang/crates.io-index` |
| itoa | `registry+https://github.com/rust-lang/crates.io-index` |
| jni | `registry+https://github.com/rust-lang/crates.io-index` |
| jni-macros | `registry+https://github.com/rust-lang/crates.io-index` |
| jni-sys | `registry+https://github.com/rust-lang/crates.io-index` |
| jni-sys-macros | `registry+https://github.com/rust-lang/crates.io-index` |
| js-sys | `registry+https://github.com/rust-lang/crates.io-index` |
| lazy_static | `registry+https://github.com/rust-lang/crates.io-index` |
| libc | `registry+https://github.com/rust-lang/crates.io-index` |
| litemap | `registry+https://github.com/rust-lang/crates.io-index` |
| litrs | `registry+https://github.com/rust-lang/crates.io-index` |
| log | `registry+https://github.com/rust-lang/crates.io-index` |
| loom | `registry+https://github.com/rust-lang/crates.io-index` |
| mac-addr | `registry+https://github.com/rust-lang/crates.io-index` |
| matchers | `registry+https://github.com/rust-lang/crates.io-index` |
| memchr | `registry+https://github.com/rust-lang/crates.io-index` |
| mio | `registry+https://github.com/rust-lang/crates.io-index` |
| n0-error | `registry+https://github.com/rust-lang/crates.io-index` |
| n0-error-macros | `registry+https://github.com/rust-lang/crates.io-index` |
| n0-future | `registry+https://github.com/rust-lang/crates.io-index` |
| n0-watcher | `registry+https://github.com/rust-lang/crates.io-index` |
| netdev | `registry+https://github.com/rust-lang/crates.io-index` |
| netlink-packet-core | `registry+https://github.com/rust-lang/crates.io-index` |
| netlink-packet-route | `registry+https://github.com/rust-lang/crates.io-index` |
| netlink-proto | `registry+https://github.com/rust-lang/crates.io-index` |
| netlink-sys | `registry+https://github.com/rust-lang/crates.io-index` |
| netwatch | `registry+https://github.com/rust-lang/crates.io-index` |
| noq-udp | `registry+https://github.com/rust-lang/crates.io-index` |
| nu-ansi-term | `registry+https://github.com/rust-lang/crates.io-index` |
| num-conv | `registry+https://github.com/rust-lang/crates.io-index` |
| num-traits | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2 | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-core-foundation | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-core-wlan | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-encode | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-foundation | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-security | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-security-foundation | `registry+https://github.com/rust-lang/crates.io-index` |
| objc2-system-configuration | `registry+https://github.com/rust-lang/crates.io-index` |
| once_cell | `registry+https://github.com/rust-lang/crates.io-index` |
| openssl | `registry+https://github.com/rust-lang/crates.io-index` |
| openssl-macros | `registry+https://github.com/rust-lang/crates.io-index` |
| openssl-probe | `registry+https://github.com/rust-lang/crates.io-index` |
| openssl-sys | `registry+https://github.com/rust-lang/crates.io-index` |
| parking | `registry+https://github.com/rust-lang/crates.io-index` |
| paste | `registry+https://github.com/rust-lang/crates.io-index` |
| percent-encoding | `registry+https://github.com/rust-lang/crates.io-index` |
| pin-project | `registry+https://github.com/rust-lang/crates.io-index` |
| pin-project-internal | `registry+https://github.com/rust-lang/crates.io-index` |
| pin-project-lite | `registry+https://github.com/rust-lang/crates.io-index` |
| pkg-config | `registry+https://github.com/rust-lang/crates.io-index` |
| plist | `registry+https://github.com/rust-lang/crates.io-index` |
| potential_utf | `registry+https://github.com/rust-lang/crates.io-index` |
| powerfmt | `registry+https://github.com/rust-lang/crates.io-index` |
| proc-macro2 | `registry+https://github.com/rust-lang/crates.io-index` |
| psl | `registry+https://github.com/rust-lang/crates.io-index` |
| psl-types | `registry+https://github.com/rust-lang/crates.io-index` |
| quick-xml | `registry+https://github.com/rust-lang/crates.io-index` |
| quote | `registry+https://github.com/rust-lang/crates.io-index` |
| r-efi | `registry+https://github.com/rust-lang/crates.io-index` |
| rand | `registry+https://github.com/rust-lang/crates.io-index` |
| rand_core | `registry+https://github.com/rust-lang/crates.io-index` |
| regex-automata | `registry+https://github.com/rust-lang/crates.io-index` |
| regex-syntax | `registry+https://github.com/rust-lang/crates.io-index` |
| reqwest | `registry+https://github.com/rust-lang/crates.io-index` |
| ring | `registry+https://github.com/rust-lang/crates.io-index` |
| rustc_version | `registry+https://github.com/rust-lang/crates.io-index` |
| rustls | `registry+https://github.com/rust-lang/crates.io-index` |
| rustls-native-certs | `registry+https://github.com/rust-lang/crates.io-index` |
| rustls-openssl | `registry+https://github.com/rust-lang/crates.io-index` |
| rustls-pki-types | `registry+https://github.com/rust-lang/crates.io-index` |
| rustls-platform-verifier | `registry+https://github.com/rust-lang/crates.io-index` |
| rustls-platform-verifier-android | `registry+https://github.com/rust-lang/crates.io-index` |
| rustls-webpki | `registry+https://github.com/rust-lang/crates.io-index` |
| rustversion | `registry+https://github.com/rust-lang/crates.io-index` |
| ryu | `registry+https://github.com/rust-lang/crates.io-index` |
| same-file | `registry+https://github.com/rust-lang/crates.io-index` |
| schannel | `registry+https://github.com/rust-lang/crates.io-index` |
| scoped-tls | `registry+https://github.com/rust-lang/crates.io-index` |
| security-framework | `registry+https://github.com/rust-lang/crates.io-index` |
| security-framework-sys | `registry+https://github.com/rust-lang/crates.io-index` |
| semver | `registry+https://github.com/rust-lang/crates.io-index` |
| send_wrapper | `registry+https://github.com/rust-lang/crates.io-index` |
| serde | `registry+https://github.com/rust-lang/crates.io-index` |
| serde_core | `registry+https://github.com/rust-lang/crates.io-index` |
| serde_derive | `registry+https://github.com/rust-lang/crates.io-index` |
| serde_json | `registry+https://github.com/rust-lang/crates.io-index` |
| serde_qs | `registry+https://github.com/rust-lang/crates.io-index` |
| sha1 | `registry+https://github.com/rust-lang/crates.io-index` |
| sharded-slab | `registry+https://github.com/rust-lang/crates.io-index` |
| shlex | `registry+https://github.com/rust-lang/crates.io-index` |
| signal-hook-registry | `registry+https://github.com/rust-lang/crates.io-index` |
| simd_cesu8 | `registry+https://github.com/rust-lang/crates.io-index` |
| simdutf8 | `registry+https://github.com/rust-lang/crates.io-index` |
| slab | `registry+https://github.com/rust-lang/crates.io-index` |
| smallvec | `registry+https://github.com/rust-lang/crates.io-index` |
| socket2 | `registry+https://github.com/rust-lang/crates.io-index` |
| spez | `registry+https://github.com/rust-lang/crates.io-index` |
| spin | `registry+https://github.com/rust-lang/crates.io-index` |
| stable_deref_trait | `registry+https://github.com/rust-lang/crates.io-index` |
| subtle | `registry+https://github.com/rust-lang/crates.io-index` |
| syn | `registry+https://github.com/rust-lang/crates.io-index` |
| sync_wrapper | `registry+https://github.com/rust-lang/crates.io-index` |
| synstructure | `registry+https://github.com/rust-lang/crates.io-index` |
| thiserror | `registry+https://github.com/rust-lang/crates.io-index` |
| thiserror-impl | `registry+https://github.com/rust-lang/crates.io-index` |
| thread_local | `registry+https://github.com/rust-lang/crates.io-index` |
| time | `registry+https://github.com/rust-lang/crates.io-index` |
| time-core | `registry+https://github.com/rust-lang/crates.io-index` |
| time-macros | `registry+https://github.com/rust-lang/crates.io-index` |
| tinystr | `registry+https://github.com/rust-lang/crates.io-index` |
| tokio | `registry+https://github.com/rust-lang/crates.io-index` |
| tokio-macros | `registry+https://github.com/rust-lang/crates.io-index` |
| tokio-rustls | `registry+https://github.com/rust-lang/crates.io-index` |
| tokio-tungstenite | `registry+https://github.com/rust-lang/crates.io-index` |
| tokio-util | `registry+https://github.com/rust-lang/crates.io-index` |
| tower | `registry+https://github.com/rust-lang/crates.io-index` |
| tower-http | `registry+https://github.com/rust-lang/crates.io-index` |
| tower-layer | `registry+https://github.com/rust-lang/crates.io-index` |
| tower-service | `registry+https://github.com/rust-lang/crates.io-index` |
| tracing | `registry+https://github.com/rust-lang/crates.io-index` |
| tracing-attributes | `registry+https://github.com/rust-lang/crates.io-index` |
| tracing-core | `registry+https://github.com/rust-lang/crates.io-index` |
| tracing-log | `registry+https://github.com/rust-lang/crates.io-index` |
| tracing-subscriber | `registry+https://github.com/rust-lang/crates.io-index` |
| try-lock | `registry+https://github.com/rust-lang/crates.io-index` |
| tungstenite | `registry+https://github.com/rust-lang/crates.io-index` |
| typenum | `registry+https://github.com/rust-lang/crates.io-index` |
| unicode-ident | `registry+https://github.com/rust-lang/crates.io-index` |
| unicode-segmentation | `registry+https://github.com/rust-lang/crates.io-index` |
| unicode-xid | `registry+https://github.com/rust-lang/crates.io-index` |
| untrusted | `registry+https://github.com/rust-lang/crates.io-index` |
| url | `registry+https://github.com/rust-lang/crates.io-index` |
| utf8_iter | `registry+https://github.com/rust-lang/crates.io-index` |
| uuid | `registry+https://github.com/rust-lang/crates.io-index` |
| valuable | `registry+https://github.com/rust-lang/crates.io-index` |
| vcpkg | `registry+https://github.com/rust-lang/crates.io-index` |
| version_check | `registry+https://github.com/rust-lang/crates.io-index` |
| walkdir | `registry+https://github.com/rust-lang/crates.io-index` |
| want | `registry+https://github.com/rust-lang/crates.io-index` |
| wasi | `registry+https://github.com/rust-lang/crates.io-index` |
| wasip2 | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-bindgen | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-bindgen-futures | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-bindgen-macro | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-bindgen-macro-support | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-bindgen-shared | `registry+https://github.com/rust-lang/crates.io-index` |
| wasm-streams | `registry+https://github.com/rust-lang/crates.io-index` |
| web-sys | `registry+https://github.com/rust-lang/crates.io-index` |
| web-time | `registry+https://github.com/rust-lang/crates.io-index` |
| webpki-root-certs | `registry+https://github.com/rust-lang/crates.io-index` |
| webpki-roots | `registry+https://github.com/rust-lang/crates.io-index` |
| winapi | `registry+https://github.com/rust-lang/crates.io-index` |
| winapi-i686-pc-windows-gnu | `registry+https://github.com/rust-lang/crates.io-index` |
| winapi-util | `registry+https://github.com/rust-lang/crates.io-index` |
| winapi-x86_64-pc-windows-gnu | `registry+https://github.com/rust-lang/crates.io-index` |
| windows | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-collections | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-core | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-future | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-implement | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-interface | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-link | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-numerics | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-result | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-strings | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-sys | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-targets | `registry+https://github.com/rust-lang/crates.io-index` |
| windows-threading | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_aarch64_gnullvm | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_aarch64_msvc | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_i686_gnu | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_i686_gnullvm | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_i686_msvc | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_x86_64_gnu | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_x86_64_gnullvm | `registry+https://github.com/rust-lang/crates.io-index` |
| windows_x86_64_msvc | `registry+https://github.com/rust-lang/crates.io-index` |
| wit-bindgen | `registry+https://github.com/rust-lang/crates.io-index` |
| wmi | `registry+https://github.com/rust-lang/crates.io-index` |
| writeable | `registry+https://github.com/rust-lang/crates.io-index` |
| yoke | `registry+https://github.com/rust-lang/crates.io-index` |
| yoke-derive | `registry+https://github.com/rust-lang/crates.io-index` |
| zerofrom | `registry+https://github.com/rust-lang/crates.io-index` |
| zerofrom-derive | `registry+https://github.com/rust-lang/crates.io-index` |
| zeroize | `registry+https://github.com/rust-lang/crates.io-index` |
| zerotrie | `registry+https://github.com/rust-lang/crates.io-index` |
| zerovec | `registry+https://github.com/rust-lang/crates.io-index` |
| zerovec-derive | `registry+https://github.com/rust-lang/crates.io-index` |
| zmij | `registry+https://github.com/rust-lang/crates.io-index` |

## Review commands

Run these commands from the library crate directory after generating a lockfile.
`--locked` keeps the recorded resolution. Add `--offline` when all required
dependencies are cached.

```bash
# Inspect normal and build dependencies for the current target and HTTP only.
cargo tree --manifest-path Cargo.toml --locked --no-default-features --features http-client -e normal,build
# Inspect all feature and target conditions, including test dependencies.
cargo tree --manifest-path Cargo.toml --locked --all-features --target all -e normal,build,dev
# Show packages resolved at multiple versions and their dependents.
cargo tree --manifest-path Cargo.toml --locked --all-features --target all --duplicates
# Inspect Cargo's machine-readable dependency declarations.
cargo metadata --manifest-path Cargo.toml --locked --no-deps --format-version 1
```

Regenerate the inventory when dependency declarations or the local resolution change.

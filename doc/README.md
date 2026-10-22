# Installation and version compatibility

These guides cover **open-net 0.3.3**. Use the matching package version or source
revision, and follow the migration notes when upgrading from 0.3.2.

## Library integration

Use Rust 1.89 or later. For an HTTP-only application, add the
[crates.io package](https://crates.io/crates/open-net) to your `Cargo.toml`:

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false, features = ["http-client"] }
```

For Git integration, use the [official repository](https://github.com/0barman/open-net)
as the dependency's `git` source and select a verified `tag` or full commit `rev`
containing the 0.3.3 APIs. Confirm that the reference exists in the repository;
do not assume the default branch matches this documentation. Preserve the
`default-features` and `features` settings from the guide. Choose one dependency
source. A downstream crate intended for crates.io publication needs
registry-resolvable dependencies; see the
[Cargo dependency rules](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html).

`version = "0.3.3"` permits later 0.3.x versions under Cargo's version rules.
Applications can retain their resolved dependency graph in `Cargo.lock`.

## Changes from 0.3.2

Version 0.3.3 changes some public retry APIs and privacy behavior. Follow the
linked migration instructions when upgrading. Do not substitute 0.3.2 and expect
the examples or security guarantees to remain valid; use the
[0.3.2 documentation](https://docs.rs/crate/open-net/0.3.2) for that version.

| Capability | open-net 0.3.3 | Published 0.3.2 boundary |
| --- | --- | --- |
| Shared retry schedules and observations | `open_net::api::retry`, generic settings and shared reports | The new module and signatures are unavailable; see the [retry migration map](retry.md#api-migration-map). |
| HTTP retry policy | `HttpRetryPolicy` with shared schedules and classifiers | Uses the previous API; see [HTTP migration](http/retry.md#migration-from-the-previous-api). |
| WebSocket message and logical-request retries | `WsSendRetryPolicy`, `WsRequestRetryPolicy` and request factories | These types and operations are unavailable; see [WebSocket retries](websocket/retry.md). |
| Network-name privacy | Ordinary monitoring omits names; explicit `NetworkNameObserver` owns collection | The observer API is unavailable and Windows monitoring/refresh can collect names; see [migration](net_status/network_name_migration.md). |
| HTTP transport and redirect policies | `HttpTransportSecurity` and `RedirectPolicy` | These policy APIs and their guarantees are unavailable; see [transport security](http/redirects.md). |
| WebSocket HTTPS proxy and authentication policy | `https_connect` and default TLS requirement for proxy Basic credentials | These APIs and guarantees are unavailable; see [proxy migration](ws_proxy_migration.md). |
| Diagnostic privacy | HTTP summaries across feature combinations, broader WebSocket business-value summaries and reduced SDK log fields | Some values already redact secrets, but the broader guarantees are new; see [logging boundaries](log/README.md). |

Add each example's application dependencies, including its selected TLS provider
and runtime. The library's development dependencies do not become dependencies of
your application. Features are additive; preserve each guide's feature selection
and consult the [library feature table](../README.md#cargo-features).

To run the library's source examples, obtain the matching source
and run the documented `cargo run --example` commands from the library directory
containing its `Cargo.toml`. This is source validation, not evidence that the same
program compiles against a published package. Maintainer checks that override a
dependency in a temporary consumer must identify that override explicitly.

## Example data and privacy

Examples use reserved documentation domains, generic application names and
clearly artificial credentials. Public project and upstream documentation links
identify required resources; they are not internal service addresses. Never put
real tokens, private keys, personal or organization names, internal endpoints,
machine-specific paths or captured network names into shared examples or logs.

Loopback HTTP/WS endpoints are local test fixtures. Run only a server you control
and use artificial data. The examples do not require a public demonstration
service. Reserved domains illustrate configuration and are not promised to serve
application routes; see [IANA example domains](https://www.iana.org/help/example-domains).

For credential-bearing requests, require HTTPS. TLS to the destination does not
protect credentials sent earlier over a plaintext proxy connection. Read the
protocol-specific proxy documentation; HTTP and WebSocket proxy capabilities
differ. Default diagnostics should report counts, states and error categories.
Raw error sources, request/response bodies, URLs, interface addresses and network
names require application-specific handling and redaction before disclosure.

[Back to the library overview](../README.md)

# open-net

This README and the linked integration guides cover **open-net 0.3.3**.
See [installation and version compatibility](doc/README.md) for setup and
the API and privacy changes from 0.3.2.

## Overview

open-net is an asynchronous networking library for Rust applications. It provides
`NetStatusClient` for observing network state, `HttpClient` for HTTP requests and
responses, and `WebSocketClient` for persistent WebSocket connections. `OpenNet`
manages named clients and shared engine resources. HTTP clients can also be
constructed independently with `HttpClient::new`.

## Supported Platforms

| Platform | Support scope |
| --- | --- |
| macOS | Native desktop applications |
| Windows | Native desktop applications |
| Ubuntu | Linux kernel 5.0 or later |

## Cargo Features

| Capability | Minimum feature | Automatically enables / additional requirements |
| --- | --- | --- |
| Engine, basic configuration, logging, errors, shared subscription types | None | Available with `default-features = false` |
| Shared retry schedules, built-in backoff, jitter and reports | None | Import from `open_net::api::retry`; transport policies remain protocol-specific |
| Network monitoring, snapshots, IPv4/IPv6 capability, state subscriptions and ordered events | `net-status` | Create a named `NetStatusClient` through `OpenNet` |
| Recoverable event callbacks | `net-status` | Stable subscription identity; queue overflow reconciles to a new atomic baseline |
| On-demand network observation | `net-status` | Windows; async and blocking reachability refresh, with cached IP-stack capability; no network-name collection |
| Explicit network-name observation | `net-status` | `NetworkNameObserver`; Windows only, separately owned and explicitly requested |
| LAN IPv4 candidates and evidence-based recommendation | `net-status` | `network::lan_ipv4_snapshot()` or `NetStatusClient::query_lan_ipv4()`; fresh synchronous OS query |
| Subnet-based LAN IPv4 selection | `net-status` | `network::preferred_lan_ipv4()` ranks candidates by subnet priority |
| WebSocket client | `ws-client` | Default; enables `net-status`; select a TLS provider separately |
| HTTP/HTTPS, GET/POST, custom methods, headers, byte request bodies | `http-client` | Enables `net-status`; import from `open_net::api::http` |
| Lazy named HTTP clients and shared shutdown | `http-client` | `OpenNet::managed_http`; fixed configuration with SDK-owned initialization and cleanup |
| Buffered responses, text/JSON parsing, explicit HTTP status checks | `http-client` | JSON response parsing does not require codecs |
| Streaming response bytes and raw SSE bytes | `http-client` | No SSE event parser is included |
| Custom request traits, callback submission, completion handles | `http-client` | No extra feature |
| Timeouts, retries, admission capacity, cancellation, shutdown | `http-client` | No separate timeout or retry feature |
| HTTP proxy, custom root certificates, mutual TLS | `http-client` | Use HTTP-specific proxy and TLS configuration |
| HTTP network snapshots and subscriptions | `http-client` | Standalone clients require a supplied network context for observation |
| Query, JSON, form, multipart request encoding | `http-client-codecs` | Enables `http-client`; manual body bytes need no codecs |
| Cookie storage, limits, persistence snapshots | `http-client-cookies` | Enables `http-client`; also install a cookie store |
| HTTP/2, version policy, flow control | `http-client-http2` | Enables `http-client`; `Http2Only` requires HTTPS for the initial URL and redirect targets, with no HTTP/1 fallback |
| HTTP middleware | `http-client-middleware` | Enables `http-client`; concrete buffered/streaming requests, not trait `send*` |
| Basic/Bearer auth, asynchronous providers, token refresh | `http-client-auth` | Enables `http-client`; manually setting `Authorization` needs no auth feature |

The default feature is `ws-client`, which also enables `net-status`. Adding
`features = ["http-client"]` keeps that default WebSocket feature enabled. For an
HTTP-only dependency, use `default-features = false, features = ["http-client"]`.
For network monitoring alone, use
`default-features = false, features = ["net-status"]`.

`ws-client` and `http-client` use rustls without bundling or selecting a
cryptographic provider. Supply a compatible rustls 0.23 `CryptoProvider`
explicitly or install one in the host application before client creation.
A missing provider returns `InvalidConfig` for `tls.provider`
when the client is built, including clients intended for plain HTTP or WS.
With `ws-client` enabled, `OpenNet` also compiles its default WebSocket connector
during engine construction, so supply the engine's provider before that step.
See [TLS providers](doc/tls.md) for complete examples and provider-specific build
requirements. open-net does not enable AWS-LC or ring, or depend on OpenSSL in
production. There are no built-in provider features.

Cargo features are additive: another dependency can enable a feature even when
one declaration disables defaults, including rustls's AWS-LC or ring features.
This library has no features named `http`,
`json`, `sse`, `retry`, or `tls`.

## Using this library from your application

Start with Rust 1.89 or later and create a consumer with `cargo new my-app`.
For an HTTP-only application, add the dependency from your application's directory:

```sh
cargo add open-net@0.3.3 --no-default-features --features http-client
```

The guides below use `version = "0.3.3"`. When upgrading from 0.3.2, follow the
retry and privacy migration instructions; the new APIs and behavior described
here must not be assumed when using the older version.

For Git integration, use a verified tag or commit from the official repository.
The [installation guide](doc/README.md) explains how to choose the matching
source version. Add each example's application
dependencies, such as Tokio and a TLS provider; the library's development
dependencies are not automatically available to your application.

## Integration Guides

- [Installation, version compatibility and example privacy](doc/README.md)
- [Engine and client lifecycle](doc/lifecycle.md)
- [Logging, subscriptions and drop statistics](doc/log/README.md)
- [NetStatusClient](doc/net_status/README.md)
- [LAN IPv4 candidates and selection](doc/lan_ipv4.md)
- [TLS providers, OpenSSL setup and custom cryptography](doc/tls.md)
- [Shared retry schedules and observations](doc/retry.md)
- [HttpClient: installation and first request](doc/http/README.md)
  - [Requests and responses](doc/http/requests_and_responses.md)
  - [Callback requests](doc/http/callback_requests.md)
  - [Streaming and SSE](doc/http/streaming_and_sse.md)
  - [Lifecycle and cancellation](doc/http/lifecycle.md)
  - [Network status](doc/http/network_status.md)
  - [Configuration](doc/http/configuration.md)
  - [Transport security and redirects](doc/http/redirects.md)
  - [Request codecs and multipart](doc/http/request_codecs.md)
  - [Timeouts and admission](doc/http/timeouts_and_admission.md)
  - [Retries](doc/http/retry.md)
  - [TLS and proxies](doc/http/tls_and_proxy.md)
  - [Cookies](doc/http/cookies.md)
  - [Authentication](doc/http/authentication.md)
  - [Middleware](doc/http/middleware.md)
  - [HTTP/2](doc/http/http2.md)
- [Network-name observation and privacy migration](doc/net_status/network_name_migration.md)
- [WebSocketClient](doc/websocket/README.md)
  - [Message and request retries](doc/websocket/retry.md)
- [WebSocket HTTPS proxies and authentication migration](doc/ws_proxy_migration.md)
- [Third-Party Dependencies](doc/third_party/README.md)

## Engine shutdown

Use `OpenNet::begin_shutdown()` to request permanent shutdown synchronously,
including from SDK callbacks. Await `OpenNet::shutdown()` from an external
control flow to confirm that the engine's owned workers, registered clients,
monitoring and callbacks/providers have retired. Cancelling or timing out the
wait leaves cleanup running. No caller Tokio runtime is required for this wait.

Standalone HTTP workers, other engines, process-wide log subscriptions and
application-created tasks have separate owners. Drop initiates cleanup but does
not confirm completion. See [engine and client lifecycle](doc/lifecycle.md) for
a comparison of shutdown APIs, callback restrictions and complete examples.

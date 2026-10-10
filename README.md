# open-net

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

This table states the project's support scope. It does not claim that this
documentation change has tested every operating system or the minimum Linux
kernel. Linux 5.0 is a kernel version, not an Ubuntu release number.

## Cargo Features

| Capability | Minimum feature | Automatically enables / additional requirements |
| --- | --- | --- |
| Engine, basic configuration, logging, errors, shared subscription types | None | Available with `default-features = false` |
| Network monitoring, snapshots, IPv4/IPv6 capability, subscriptions | `net-status` | Create a named `NetStatusClient` through `OpenNet` |
| Preferred LAN IPv4 discovery | `net-status` | `open_net::network::preferred_lan_ipv4()` |
| WebSocket client | `ws-client` | Default; enables `net-status` |
| HTTP/HTTPS, GET/POST, custom methods, headers, byte request bodies | `http-client` | Enables `net-status`; import from `open_net::api::http` |
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

Cargo features are additive: another dependency can enable a feature even when
one declaration disables defaults. This library has no features named `http`,
`json`, `sse`, `retry`, or `tls`.

## Integration Guides

- [NetStatusClient](doc/net_status/README.md)
- [HttpClient: installation and first request](doc/http/README.md)
  - [Requests and responses](doc/http/requests_and_responses.md)
  - [Callback requests](doc/http/callback_requests.md)
  - [Streaming and SSE](doc/http/streaming_and_sse.md)
  - [Lifecycle and cancellation](doc/http/lifecycle.md)
  - [Network status](doc/http/network_status.md)
  - [Configuration](doc/http/configuration.md)
  - [Request codecs and multipart](doc/http/request_codecs.md)
  - [Timeouts and admission](doc/http/timeouts_and_admission.md)
  - [Retries](doc/http/retry.md)
  - [TLS and proxies](doc/http/tls_and_proxy.md)
  - [Cookies](doc/http/cookies.md)
  - [Authentication](doc/http/authentication.md)
  - [Middleware](doc/http/middleware.md)
  - [HTTP/2](doc/http/http2.md)
- [WebSocketClient](doc/websocket/README.md)
- [Third-Party Dependencies](doc/third_party/README.md)

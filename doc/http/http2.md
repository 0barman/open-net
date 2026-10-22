# HTTP/2 protocol policy and flow-control metadata

Required feature: `http-client-http2` (includes `http-client`). Without this feature, the SDK explicitly configures HTTP/1 even when a different dependency enables HTTP/2 in the underlying transport.

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false, features = ["http-client-http2"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Application TLS provider

open-net does not choose a cryptographic provider. Each complete program that
creates an HTTP client below initializes the example application's provider in
`main`; copy the program as written. Configuration-only examples need no provider
until a client is built. Client creation requires a provider even for plain HTTP
and otherwise returns `InvalidConfig` for `tls.provider`.

The examples use OpenSSL through `rustls-openssl`, an application dependency.
Another compatible rustls 0.23 provider can replace it. Install a process provider
once in the host application, or pass an explicit per-client provider. A library
using open-net should accept the host's choice rather than install its own global
provider. See [TLS providers](../tls.md) for build prerequisites, provider
precedence and complete examples of both approaches.

Public types below are re-exported by `open_net::api::http`. Runtime examples require a reachable HTTPS server with an accepted certificate and HTTP/2 ALPN support.

## `HttpClientConfig::with_http_version_policy()`

### Required features

`http-client-http2`.

### Function signature

```text
HttpClientConfig::with_http_version_policy(self, policy: HttpVersionPolicy) -> Self
#[non_exhaustive]
pub enum HttpVersionPolicy { Auto, Http1Only, Http2Only }
```

### Steps

Select a policy on configuration before client creation. Validate it together with the base URL. Inspect the response's observed protocol instead of assuming negotiation selected HTTP/2.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `policy` | `HttpVersionPolicy` | Yes | `Auto` is the default: negotiate HTTPS h2 using ALPN with HTTP/1.1 fallback. `Http1Only` restricts transport to HTTP/1.1. `Http2Only` requires HTTPS for the initial base URL and followed redirect targets, and selects HTTP/2 without HTTP/1 fallback. |

### Callback parameters

None.

### Return values

Updated `HttpClientConfig`; the setter itself does not return a `Result`. `validate` and client creation enforce the policy/base URL combination.

### Errors and behavior

`Http2Only` with an `http` initial base URL returns `InvalidConfig` for `http_version_policy`; direct cleartext h2c base URLs are rejected. A TLS peer that cannot negotiate h2 fails a request under `Http2Only` with a TLS/protocol error; there is no HTTP/1 fallback. `Auto` does not promise that the server will choose h2. The enum is non-exhaustive, so downstream exhaustive `match` statements require a wildcard arm.

`Http2Only` never follows a redirect to an HTTP target or replays a request body there, including buffered POST bodies on 307/308 redirects. The independent [transport security policy](redirects.md) now prevents HTTPS-to-HTTP redirects in `Auto`, `Http1Only`, and builds without the HTTP/2 feature as well. Initial HTTP URLs remain supported by those modes under the default transport policy.

The SDK handles every redirect for buffered requests, callbacks, stream opening and SSE opening. Installing an authentication provider or Cookie store does not select another redirect implementation. A rejected redirect returns `Protocol/Receive` with a URL-free `HttpRedirectError` source before connecting to the target. It is not automatically retried, even when protocol transport retries are enabled. `RedirectPolicy::none()`, a missing `Location`, or a non-replayable body can leave a response as an unfollowed 3xx. Check the returned status as well as handling request errors.

HTTPS-to-HTTPS redirects must satisfy the independent redirect policy as well as certificate checks and HTTP/2 requirements. The default permits only the original origin; explicitly allow another origin when needed. Allowlisting a target never bypasses the HTTPS requirement. An explicit HTTP proxy can still carry an HTTPS request through CONNECT; see [TLS and proxies](tls_and_proxy.md).

### Example

This configuration example runs offline.

```rust
use open_net::api::http::{HttpClientConfig, HttpVersionPolicy};
use open_net::NetError;

fn main() -> Result<(), NetError> {
    let config = HttpClientConfig::new("https://example.com/")?
        .with_http_version_policy(HttpVersionPolicy::Http2Only);
    config.validate()?;
    assert!(HttpClientConfig::new("http://example.com/")?
        .with_http_version_policy(HttpVersionPolicy::Http2Only).validate().is_err());
    Ok(())
}
```

## Tune `Http2Config`

### Required features

`http-client-http2`.

### Function signature

```text
Http2Config::new() -> Self
Http2Config::adaptive_window(self, enabled: bool) -> Self
Http2Config::initial_stream_window(self, bytes: u32) -> Self
Http2Config::initial_connection_window(self, bytes: u32) -> Self
Http2Config::max_frame_size(self, bytes: u32) -> Self
Http2Config::max_header_list_size(self, bytes: u32) -> Self
Http2Config::keep_alive(self, interval: Duration, timeout: Duration, while_idle: bool) -> Self
HttpClientConfig::with_http2(self, http2: Http2Config) -> Result<Self, NetError>
```

### Steps

Construct a default tuning object, set only required overrides, then install using `with_http2`. The whole object replaces the previous HTTP/2 tuning layer. Repeated setters inside an object replace their respective values.

### Parameters

| Parameter / method | Type | Required | Default and bounds |
| --- | --- | --- | --- |
| `new` / `Default` | None | — | Adaptive windows off; window/frame/header overrides absent; no configured PING keepalive. |
| `adaptive_window(enabled)` | `bool` | For setter | `false` by default; true lets transport adapt flow-control windows and supersedes fixed window sizing. |
| `initial_stream_window(bytes)` | `u32` | For setter | Explicit initial per-stream bytes, `1..=2_147_483_647`. |
| `initial_connection_window(bytes)` | `u32` | For setter | Explicit initial connection bytes, same range. |
| `max_frame_size(bytes)` | `u32` | For setter | Frame payload bytes, `16_384..=16_777_215`. |
| `max_header_list_size(bytes)` | `u32` | For setter | Maximum received header-list size, positive `u32`. |
| `keep_alive` `interval`, `timeout` | `Duration` | Both for setter | Positive PING interval and response timeout; use explicit seconds/milliseconds. |
| `keep_alive` `while_idle` | `bool` | For setter | Whether PINGs are allowed without an active request. |
| `http2` | `Http2Config` | For installation | Entire owned tuning value; all fields are private. |

Unspecified transport settings use the locked transport's defaults; the SDK does not expose those as fixed configuration values. These windows and frames are not a replacement for the buffered-response `max_response_bytes` limit or client admission capacity. Streaming consumers must enforce their own cumulative response-size bound.

### Callback parameters

None.

### Return values

`Http2Config` methods return `Self` without immediate validation. `with_http2` returns `Ok(HttpClientConfig)` after validation or `Err(NetError)`.

### Errors and behavior

Invalid protocol ranges or zero keepalive durations return `InvalidConfig/Configuration` under `http2.initial_stream_window`, `http2.initial_connection_window`, `http2.max_frame_size`, `http2.max_header_list_size` or `http2.keep_alive`. This validation checks keepalive nonzero values; it does not promise every arbitrarily large `Duration` is practical for the transport. Configuration applies to the client's connections, not a single request. PING keepalive is independent of TCP keepalive and request phase timeouts.

### Example

```rust
use open_net::api::http::{Http2Config, HttpClientConfig};
use open_net::NetError;
use std::time::Duration;

fn main() -> Result<(), NetError> {
    let http2 = Http2Config::new()
        .adaptive_window(false)
        .initial_stream_window(1024 * 1024)
        .initial_connection_window(4 * 1024 * 1024)
        .max_frame_size(16_384)
        .max_header_list_size(64 * 1024)
        .keep_alive(Duration::from_secs(30), Duration::from_secs(10), false);
    HttpClientConfig::new("https://example.com/")?.with_http2(http2)?.validate()
}
```

## Inspect protocol and delivery counters

### Required features

`http-client-http2`.

### Function signature

```text
HttpResponse::info(&self) -> &HttpResponseInfo
HttpResponse::protocol(&self) -> HttpProtocol
HttpStreamResponse::info(&self) -> &HttpResponseInfo
HttpStreamResponse::flow_control(&self) -> Option<Http2FlowControlSnapshot>
#[non_exhaustive]
pub enum HttpProtocol { Http10, Http11, Http2, Unknown }
```

### Steps

Read `info` from a response. For a stream, call `flow_control` again after consuming chunks to see updated delivery counters. Consume or drop the stream, then close the client.

### Parameters

None beyond the borrowed response receiver.

### Callback parameters

None.

### Return values

| Public type and field | Type | Meaning |
| --- | --- | --- |
| `HttpResponseInfo.protocol` | `HttpProtocol` | Actual observed HTTP/1.0, HTTP/1.1, HTTP/2, or `Unknown` for synthetic/future versions. |
| `HttpResponseInfo.flow_control` | `Option<Http2FlowControlSnapshot>` | `Some` for observed HTTP/2; `None` otherwise. `Default` info is `Unknown` / `None`. |
| `Http2FlowControlSnapshot.configured_stream_window` | `Option<u32>` | Local explicitly configured initial bytes; `None` means no SDK override. |
| `Http2FlowControlSnapshot.configured_connection_window` | `Option<u32>` | Local explicitly configured connection bytes; same `None` meaning. |
| `Http2FlowControlSnapshot.adaptive_window` | `bool` | Whether adaptive window management was configured. |
| `Http2FlowControlSnapshot.body_bytes_delivered` | `u64` | Body bytes delivered to the SDK caller so far. |
| `Http2FlowControlSnapshot.body_chunks_delivered` | `u64` | Body chunks delivered so far, not HTTP/2 frame count. |

The snapshot is `Copy`, `Clone` and `Default` (no configured windows, false adaptive flag, zero counters). Response info is also `Copy`/`Clone`. These types describe SDK-observable configuration/delivery, not peer SETTINGS, current protocol windows or wire-frame telemetry. A stream's `info()` holds its initial metadata; use `flow_control()` for a fresh counter copy.

### Errors and behavior

These accessors are not fallible. Buffered requests can still return normal network, cancellation, size or timeout errors. Streaming chunk reads can fail with network, cancellation or timeout errors; the consumer is responsible for its own cumulative size limit. Treat a chunk error as terminal. Non-exhaustive `HttpProtocol` matches need a fallback arm.

### Example

Set `HTTP2_BASE_URL` to an HTTPS service whose `/` endpoint supports HTTP/2. The program prints the observed protocol and final stream delivery bytes. The result block ensures cleanup before propagating a network error.

```rust
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpProtocol, HttpRequest, HttpVersionPolicy,
};
use open_net::NetError;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let base = std::env::var("HTTP2_BASE_URL")?;
    let config = HttpClientConfig::new(base)?
        .with_http_version_policy(HttpVersionPolicy::Http2Only);
    let client = HttpClient::new(config, "http2-example")?;
    let result: Result<(), NetError> = async {
        let buffered = client.get("/").await?;
        let _info = buffered.info();
        match buffered.protocol() {
            HttpProtocol::Http2 => println!("HTTP/2"),
            other => println!("observed: {other:?}"),
        }
        let request = HttpRequest::builder().path("/").build()?;
        let mut response = client.stream(request).await?;
        println!("stream protocol: {:?}", response.info().protocol);
        while let Some(chunk) = response.next_chunk().await { let _bytes = chunk?; }
        if let Some(snapshot) = response.flow_control() {
            println!("delivered bytes: {}", snapshot.body_bytes_delivered);
        }
        drop(response);
        Ok(())
    }.await;
    client.shutdown().await?;
    result?;
    Ok(())
}
```

## Individual function reference

The examples linked below are complete programs with the dependency declaration at the top of this page. Each reference identifies the call that demonstrates the function.

## `Http2Config::new()`

### Required features

`http-client-http2`.

### Function signature

```text
Http2Config::new() -> Self
```

### Steps

Start with defaults, set required transport overrides, then install through `HttpClientConfig::with_http2`.

### Parameters

None.

### Callback parameters

None.

### Return values

An owned `Http2Config`, equivalent to `Default`.

### Errors and behavior

Infallible. Adaptive windows start disabled; windows/frame/header overrides and PING configuration are absent. Unspecified values use transport defaults, not fixed values promised by the SDK.

### Example

See the [complete tuning program](#tune-http2config), which starts with `Http2Config::new()`.

## `Http2Config::adaptive_window()`

### Required features

`http-client-http2`.

### Function signature

```text
Http2Config::adaptive_window(self, enabled: bool) -> Self
```

### Steps

Choose whether the transport should adapt flow-control windows and install the resulting config.

### Parameters

`enabled` is required; false is the SDK default.

### Callback parameters

None.

### Return values

The updated owned `Http2Config`.

### Errors and behavior

Infallible. When true, adaptive management supersedes fixed flow-control window sizing. It does not change response-byte limits or admission capacity.

### Example

See the [complete tuning program](#tune-http2config), which calls `.adaptive_window(false)`.

## `Http2Config::initial_stream_window()`

### Required features

`http-client-http2`.

### Function signature

```text
Http2Config::initial_stream_window(self, bytes: u32) -> Self
```

### Steps

Set the per-stream receive window and install the config using `with_http2` to validate it.

### Parameters

`bytes` is required, in `1..=2_147_483_647`. No override is configured by default.

### Callback parameters

None.

### Return values

The updated owned tuning object without immediate validation.

### Errors and behavior

Invalid values are rejected on `with_http2`/client configuration validation with `InvalidConfig` for `http2.initial_stream_window`. Adaptive windows may supersede this setting. This is not an accumulated response-size limit.

### Example

See the [complete tuning program](#tune-http2config), which calls `.initial_stream_window(1024 * 1024)`.

## `Http2Config::initial_connection_window()`

### Required features

`http-client-http2`.

### Function signature

```text
Http2Config::initial_connection_window(self, bytes: u32) -> Self
```

### Steps

Set the connection receive window, then validate by installing with `with_http2`.

### Parameters

`bytes` is required, in `1..=2_147_483_647`; default has no SDK override.

### Callback parameters

None.

### Return values

The updated owned tuning object without immediate validation.

### Errors and behavior

Invalid values later return `InvalidConfig` for `http2.initial_connection_window`. Applies to the connection across streams; adaptive management can supersede the fixed window.

### Example

See the [complete tuning program](#tune-http2config), which calls `.initial_connection_window(4 * 1024 * 1024)`.

## `Http2Config::max_frame_size()`

### Required features

`http-client-http2`.

### Function signature

```text
Http2Config::max_frame_size(self, bytes: u32) -> Self
```

### Steps

Set the maximum frame payload and install using `with_http2`.

### Parameters

`bytes` is required, in `16_384..=16_777_215`; no override by default.

### Callback parameters

None.

### Return values

The updated owned tuning object without immediate validation.

### Errors and behavior

Out-of-range values later return `InvalidConfig` for `http2.max_frame_size`. A transport frame is not the same as an SDK-delivered body chunk.

### Example

See the [complete tuning program](#tune-http2config), which calls `.max_frame_size(16_384)`.

## `Http2Config::max_header_list_size()`

### Required features

`http-client-http2`.

### Function signature

```text
Http2Config::max_header_list_size(self, bytes: u32) -> Self
```

### Steps

Choose a bound for received HTTP/2 headers and install the tuning object.

### Parameters

`bytes` is required and must be positive; default has no SDK override.

### Callback parameters

None.

### Return values

The updated owned tuning object without immediate validation.

### Errors and behavior

Zero later returns `InvalidConfig` for `http2.max_header_list_size`. This setting limits received header lists independently of buffered body bytes.

### Example

See the [complete tuning program](#tune-http2config), which calls `.max_header_list_size(64 * 1024)`.

## `Http2Config::keep_alive()`

### Required features

`http-client-http2`.

### Function signature

```text
Http2Config::keep_alive(self, interval: Duration, timeout: Duration, while_idle: bool) -> Self
```

### Steps

Select a PING interval/response budget and whether idle connections may send PINGs, then install.

### Parameters

`interval` and `timeout` are required positive durations; use explicit units. Required `while_idle` selects whether PINGs may run without an active request. Default has no configured PING keepalive.

### Callback parameters

None.

### Return values

The updated owned tuning object replacing any prior PING configuration.

### Errors and behavior

Zero interval or timeout later returns `InvalidConfig` for `http2.keep_alive`. No further SDK upper-duration bound is checked. These protocol PINGs are independent of TCP keepalive and application request budgets.

### Example

See the [complete tuning program](#tune-http2config), which calls `.keep_alive(Duration::from_secs(30), Duration::from_secs(10), false)`.

## `HttpClientConfig::with_http2()`

### Required features

`http-client-http2`.

### Function signature

```text
HttpClientConfig::with_http2(self, http2: Http2Config) -> Result<Self, NetError>
```

### Steps

Construct the tuning object, install it on a client configuration, then create a new client.

### Parameters

`http2` is the required owned complete tuning layer, replacing the previous layer.

### Callback parameters

None.

### Return values

`Ok(HttpClientConfig)` after tuning validation, or `Err(NetError)`.

### Errors and behavior

Rejects invalid ranges and zero PING durations with `InvalidConfig` and the corresponding `http2.*` field. The consumed configuration is not returned on error; clone first if needed. This does not select `Http2Only` or reconfigure an existing client.

### Example

See the [complete tuning program](#tune-http2config), which calls `.with_http2(http2)?.validate()`.

## `HttpResponse::info()`

### Required features

`http-client-http2`.

### Function signature

```text
HttpResponse::info(&self) -> &HttpResponseInfo
```

### Steps

Complete a buffered request and borrow its metadata before dropping the response.

### Parameters

Only the borrowed buffered response.

### Callback parameters

None.

### Return values

A borrowed `HttpResponseInfo` containing observed protocol and optional HTTP/2 delivery counters.

### Errors and behavior

Infallible. `flow_control` is absent for non-HTTP/2 responses. Synthetic metadata may report Unknown. Counters describe delivery, not peer SETTINGS or current wire windows.

### Example

See the [complete response metadata program](#inspect-protocol-and-delivery-counters), which calls `buffered.info()` after `client.get("/").await`.

## `HttpResponse::protocol()`

### Required features

`http-client-http2`.

### Function signature

```text
HttpResponse::protocol(&self) -> HttpProtocol
```

### Steps

Complete a buffered request and inspect its actual observed protocol.

### Parameters

Only the borrowed buffered response.

### Callback parameters

None.

### Return values

A copied `HttpProtocol`: Http10, Http11, Http2 or Unknown.

### Errors and behavior

Infallible. Do not infer protocol from requested policy alone; synthetic or future versions may be Unknown. Matches on this non-exhaustive enum need a fallback arm.

### Example

See the [complete response metadata program](#inspect-protocol-and-delivery-counters), which matches `buffered.protocol()` with a wildcard branch.

## `HttpStreamResponse::info()`

### Required features

`http-client-http2`.

### Function signature

```text
HttpStreamResponse::info(&self) -> &HttpResponseInfo
```

### Steps

Open a stream, then borrow its initial response metadata before consuming chunks.

### Parameters

Only the borrowed streaming response.

### Callback parameters

None.

### Return values

A reference to initial `HttpResponseInfo` stored on the stream.

### Errors and behavior

Infallible. This metadata is not refreshed as the stream delivers data. Call `flow_control()` for current delivery counters. Opening and consuming a stream can still fail independently.

### Example

See the [complete response metadata program](#inspect-protocol-and-delivery-counters), which prints `response.info().protocol` before the read loop.

## `HttpStreamResponse::flow_control()`

### Required features

`http-client-http2`.

### Function signature

```text
HttpStreamResponse::flow_control(&self) -> Option<Http2FlowControlSnapshot>
```

### Steps

After opening an HTTP/2 stream, sample counters as chunks are consumed or after the terminal read.

### Parameters

Only the borrowed streaming response; ownership of the stream is retained.

### Callback parameters

None.

### Return values

`Some` copied current SDK delivery counters/configured windows for HTTP/2; `None` for other protocols.

### Errors and behavior

Infallible. Bytes/chunks count application deliveries, not wire frames; configured windows are local initial settings, not current protocol windows or peer state. The consumer must still enforce any cumulative byte limit.

### Example

See the [complete response metadata program](#inspect-protocol-and-delivery-counters), which calls `response.flow_control()` after reading all chunks and before dropping the stream.

[HTTP guide index](README.md)

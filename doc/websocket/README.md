# WebSocket client

[Library overview](../../README.md) · [TLS providers](../tls.md) · [Engine and subscription lifetime](../lifecycle.md)

`ws-client` uses rustls for `wss://` connections, but `open-net` does not select
or install a cryptographic provider. The application must add a rustls 0.23
provider and inject it into the WebSocket `NetworkConfig` before creating the
engine, or install its chosen process provider first.

Use Rust 1.89 or later. Create a consumer with `cargo new websocket-example`,
use the dependencies below and replace the path with your checkout's `open-net`
directory. Each Rust block is a complete independent `src/main.rs` program.
`cargo check` verifies compilation; `cargo run` executes the selected example.
Only examples calling `connect` need a reachable WebSocket server. Configuration
validation and named-client creation alone do not connect to a server.

## First client with an explicit provider

The provider is application-owned. The example below uses `rustls-openssl`; a
ring or AWS-LC provider can be supplied through the same
`Arc<open_net::tls::CryptoProvider>` API.

```toml
[dependencies]
open-net = { path = "../open-net", default-features = false, features = ["ws-client"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Install the chosen provider's build prerequisites; the OpenSSL example's setup
is described in [TLS providers](../tls.md#build-requirements-for-the-openssl-example).
Replace `wss://example.com/stream` with your server's actual endpoint. A named
client owns connection workers; a session represents a logical connection that
may reconnect according to its policy. `clone()` shares handles and lifecycle
state rather than making independent connections.

```rust,no_run
use open_net::network::{NetworkConfig, TlsConfig};
use open_net::tls::CryptoProvider;
use open_net::{NetError, OpenNet};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), NetError> {
    let provider: Arc<CryptoProvider> =
        Arc::new(rustls_openssl::default_provider());
    let network = NetworkConfig::default().with_tls(
        TlsConfig::default().with_crypto_provider(provider),
    );

    let net = OpenNet::new_with_network_config(network)?;
    let client = net.create_ws_client("market-feed").await?;
    let work = async {
        let session = client.connect("wss://example.com/stream").await?;
        session.close().await
    }.await;
    let cleanup = net.shutdown().await;
    work.and(cleanup)
}
```

`OpenNet::new()` and `OpenNet::new_with_config()` compile the engine's default
WebSocket connector immediately when `ws-client` is enabled. Without an
explicit provider or a provider already installed by the host, they return
`InvalidConfig` with configuration field `tls.provider`. This failure is
reported before a worker or network connection is started.

## Per-client provider and network policy

An engine can keep one provider as its default while a particular client uses a
complete replacement policy. The replacement includes TLS, proxy, and network
status settings.

```rust,no_run
use open_net::network::{NetworkConfig, TlsConfig};
use open_net::tls::CryptoProvider;
use open_net::{NetError, OpenNet};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), NetError> {
    let provider: Arc<CryptoProvider> =
        Arc::new(rustls_openssl::default_provider());
    let defaults = NetworkConfig::default().with_tls(
        TlsConfig::default().with_crypto_provider(Arc::clone(&provider)),
    );
    let net = OpenNet::new_with_network_config(defaults)?;

    let isolated = NetworkConfig::default().with_tls(
        TlsConfig::default().with_crypto_provider(provider),
    );
    let client = net
        .create_ws_client_with_network_config(
            "isolated-feed",
            open_net::ws::WebSocketClientConfig::default(),
            isolated,
        )
        .await?;

    client.shutdown().await?;
    net.destroy_ws_client("isolated-feed").await?;
    net.shutdown().await?;
    Ok(())
}
```

`create_ws_client_with_config()` inherits the engine policy. The explicit
`create_ws_client_with_network_config()` form compiles its supplied policy once
and keeps it isolated from other clients.

## Custom roots and client identity

Applications can provide a CA bundle and a client certificate identity. PEM
parsing is bounded; private-key loading and certificate/key matching use the
provider selected for the client.

```rust,no_run
use open_net::network::{
    ClientIdentity, NetworkConfig, RootCertificateMode, TlsConfig,
};
use open_net::tls::CryptoProvider;
use open_net::{NetError, OpenNet};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), NetError> {
    let provider: Arc<CryptoProvider> =
        Arc::new(rustls_openssl::default_provider());
    let identity = ClientIdentity::from_pem(
        std::fs::read("client.pem")?,
        std::fs::read("client-key.pem")?,
    )?;
    let tls = TlsConfig::default()
        .with_crypto_provider(provider)
        .with_root_certificates(
            std::fs::read("ca.pem")?,
            RootCertificateMode::Replace,
        )?
        .with_client_identity(identity);
    let net = OpenNet::new_with_network_config(NetworkConfig::default().with_tls(tls))?;
    let client = net.create_ws_client("mtls-feed").await?;
    client.shutdown().await?;
    net.destroy_ws_client("mtls-feed").await?;
    net.shutdown().await?;
    Ok(())
}
```

`TlsConfig::crypto_provider()` reports only the explicitly configured provider;
it does not install or resolve a process-wide default. The explicit provider
takes precedence over a provider installed by the host application.

## Reusing named clients

Use `get_or_create_ws_client_with_config` when several application paths share
one logical client name. The configuration is applied only when the name is
created; later calls return a clone of the existing client. Use
`destroy_ws_client_if_exists` from repeated cleanup paths.

| Operation | Existing ready name | Missing name | Creation or destruction in progress |
| --- | --- | --- | --- |
| `create_ws_client` / `create_ws_client_with_config` / `create_ws_client_with_network_config` | Duplicate creation fails with `ClientAlreadyExists`. | Create disconnected workers and reserve the name. | Creation cannot replace a reserved name. |
| `get_ws_client` | Return a shared handle, without changing configuration. | `ClientNotFound`. | `ConnectionClosing`. |
| `get_or_create_ws_client_with_config` | Reuse the handle and its original configuration. | Create with the supplied configuration and engine network policy. | Wait for the transition and retry lookup/creation. |
| `destroy_ws_client` | Close workers and release the name after retirement. | `ClientNotFound`. | `ConnectionClosing`. |
| `destroy_ws_client_if_exists` | Close workers and release the name after retirement. | `Ok(())`. | Wait for the transition and complete cleanup. |

Calling `WebSocketClient::shutdown` closes that client but does not remove its
registry name. Destroy the registration before reusing the name. A lookup or
get-or-create call is not a way to reopen a closed handle. Once the name is
reused, old clones still refer to the old closed instance.

Closing a session ends that session; client shutdown ends its workers; engine
shutdown closes all of that engine's registered clients and waits for its owned
resources. See [engine shutdown](../lifecycle.md) for the complete boundary.

## `OpenNet::get_or_create_ws_client_with_config()`

### Required features

`ws-client`, including a provider for the engine's TLS configuration.

### Function signature

```text
pub async fn get_or_create_ws_client_with_config(&self, thread_name: &str, config: WebSocketClientConfig) -> Result<WebSocketClient, NetError>
```

### Steps

Choose one stable name per application role. Configure the engine, await this
method and use the returned client to connect when needed. Clone the handle when
sharing it. Coordinate destruction when that role is no longer needed.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `thread_name` | `&str` | Yes | Trimmed registry name; blank names and NUL characters are rejected. |
| `config` | `WebSocketClientConfig` | Yes | Used only if this call creates the client. Existing configuration is not replaced or merged. |

### Callback parameters

None. The method creates or retrieves a client; it does not connect or register
message callbacks.

### Return values

`Ok(WebSocketClient)` is a shared handle to the registered client. `Err(NetError)`
reports invalid input, creation/configuration failure or an engine lifecycle
failure such as `Closed`.

### Errors and behavior

Concurrent callers for one name wait for registry transitions instead of
receiving a duplicate-name error from this convenience method. When callers
provide different settings, whichever creation succeeds supplies the immutable
configuration; do not depend on a particular caller winning. Define the shared
configuration in one place.

New clients inherit the engine's complete network policy. This method has no
per-client `NetworkConfig` argument; use `create_ws_client_with_network_config`
when deliberately creating an override. Cancelling a wait does not undo creation
already submitted to the SDK. Retrieve or destroy the registration by name later.

### Example

This program demonstrates reuse without connecting to a server.

```rust,no_run
use open_net::network::{NetworkConfig, TlsConfig};
use open_net::{ws::WebSocketClientConfig, NetError, OpenNet};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), NetError> {
    let network = NetworkConfig::default().with_tls(
        TlsConfig::default().with_crypto_provider(Arc::new(rustls_openssl::default_provider())),
    );
    let net = OpenNet::new_with_network_config(network)?;
    let client = net
        .get_or_create_ws_client_with_config(
            "market-feed",
            WebSocketClientConfig::default(),
        )
        .await?;
    let reused = net
        .get_or_create_ws_client_with_config(
            "market-feed",
            WebSocketClientConfig::default(),
        )
        .await?;
    assert_eq!(client.id(), reused.id());

    net.destroy_ws_client_if_exists("market-feed").await?;
    net.destroy_ws_client_if_exists("market-feed").await?;
    net.shutdown().await
}
```

## `OpenNet::destroy_ws_client_if_exists()`

### Required features

`ws-client`.

### Function signature

```text
pub async fn destroy_ws_client_if_exists(&self, thread_name: &str) -> Result<(), NetError>
```

### Steps

Stop application producers, then await destruction from an external owner. Use
this method when cleanup may run repeatedly or race another creator/destroyer.

### Parameters

`thread_name: &str` is the required name to remove. Leading/trailing whitespace
is trimmed; blank names and NUL characters return `InvalidInput`.

### Callback parameters

None. Do not block an SDK callback on cleanup that must wait for that callback.

### Return values

`Ok(())` means the requested registry cleanup completed or the name was absent.
`Err(NetError)` reports validation, lifecycle or cleanup failure. In particular,
a closed engine can return `Closed`; idempotent missing-name handling does not
mean all errors are suppressed.

### Errors and behavior

Creation and destruction already in progress are awaited before completing the
cleanup. The SDK owns worker retirement, so this future needs no caller Tokio
runtime. Cancelling its wait does not cancel cleanup already accepted or free the
name before the workers exit. An error does not certify successful retirement.

This operation addresses a name, not a particular saved handle. Coordinate new
creators with cleanup: a future caller can create that name again, and another
name-based destroy could then close that replacement.

### Example

This program demonstrates repeated cleanup and an absent name without network I/O.

```rust
use open_net::network::{NetworkConfig, TlsConfig};
use open_net::OpenNet;
use std::sync::Arc;

#[tokio::main]
async fn main() -> open_net::Result<()> {
    let network = NetworkConfig::default().with_tls(
        TlsConfig::default().with_crypto_provider(Arc::new(rustls_openssl::default_provider())),
    );
    let net = OpenNet::new_with_network_config(network)?;
    let _client = net.create_ws_client("temporary").await?;
    net.destroy_ws_client_if_exists("temporary").await?;
    net.destroy_ws_client_if_exists("temporary").await?;
    net.destroy_ws_client_if_exists("never-created").await?;
    net.shutdown().await
}
```

## Control heartbeats and bounded recovery

A WebSocket control Ping asks the peer for a matching Pong. This detects a
connection that has stopped responding; it is distinct from application messages
and from `ReconnectPolicy`, which controls later connection attempts. The default
client has `heartbeat: Some(HeartbeatConfig::default())`; set `heartbeat = None`
to disable active SDK probes.

| Configuration field | Default | Meaning and bounds |
| --- | --- | --- |
| `HeartbeatConfig::interval` | 20 seconds | Interval for checking whether to send a normal Ping. Positive and representable as a monotonic deadline. No new normal probe is created while another is pending. |
| `HeartbeatConfig::pong_timeout` | 45 seconds | Time to wait for a matching Pong after the normal Ping is written. Positive and representable. |
| `HeartbeatRetryConfig::pong_recovery_attempts` | 0 | Additional fast probes after the normal Pong deadline expires; `0..=10`. Zero disables recovery. |
| `HeartbeatRetryConfig::pong_retry_interval` | 1500 milliseconds | Matching-Pong wait and spacing for recovery probes. Must be positive and representable when recovery is enabled. |
| `HeartbeatRetryConfig::ping_write_retries` | 0 | Additional safe normal-Ping write attempts, excluding the first write; `0..=1024`. Zero disables write retry. |
| `HeartbeatRetryConfig::ping_write_retry_interval` | 1500 milliseconds | Delay between safe normal-Ping write retries. Must be positive and representable when write retry is enabled. |

Recovery and write retries are disabled by default. When recovery is enabled, a
normal Pong timeout starts a bounded sequence of additional probes on the same
physical connection. A valid matching Pong ends recovery. Exhausting recovery
retires the connection with a heartbeat timeout; any reconnect follows the
session's separate reconnect policy. Old-connection or unrelated Pong payloads
do not acknowledge the current probe. Recovery yields to close/cancellation and
control work; it is not a blocking sleep in an application callback.

Ping write retry is narrower than a general network retry. Only a
`WriteBufferFull` that returns the exact attempted Ping is safe to retry. The SDK
flushes previously buffered frames, then retries that Ping. An ordinary I/O error,
a write timeout or unknown delivery does not permit a duplicate write on the
same connection; that connection is retired. Recovery probes use their bounded
recovery policy, rather than multiplying it by normal-Ping write retries.

### Configure heartbeat retries

`HeartbeatConfig::retry` controls recovery probes and Ping write retries. When
setting `interval` and `pong_timeout` in a struct literal, use
`..HeartbeatConfig::default()` to keep retries disabled, or explicitly provide
`retry: HeartbeatRetryConfig { ... }`. These are control-heartbeat settings;
they do not enable replay of application requests or messages.

## `HeartbeatConfig::validate()`

### Required features

`ws-client`. Validation alone needs no cryptographic provider or runtime.

### Function signature

```text
pub fn validate(&self) -> Result<(), NetError>
```

### Steps

Start with defaults, change the normal interval/deadline and optional retry
policy, then validate before assigning the value to a client configuration.
`WebSocketClientConfig::validate` also validates an enabled heartbeat.

### Parameters

No argument beyond `&self`; its public fields are described in the table above.

### Callback parameters

None.

### Return values

`Ok(())` means the configuration satisfies its bounds. `Err(NetError)` has kind
`InvalidConfig`; `config_error()` identifies the invalid field and reason.

### Errors and behavior

Zero/unrepresentable normal interval or Pong timeout is rejected. The nested
retry configuration is also validated. Success does not start a heartbeat,
connect to a peer or prove the chosen time budgets suit that peer.

### Example

This offline program configures the heartbeat interval and Pong timeout while
keeping retries disabled.

```rust
use open_net::ws::{HeartbeatConfig, WebSocketClientConfig};
use std::time::Duration;

fn main() -> open_net::Result<()> {
    let heartbeat = HeartbeatConfig {
        interval: Duration::from_secs(20),
        pong_timeout: Duration::from_secs(45),
        ..HeartbeatConfig::default()
    };
    heartbeat.validate()?;
    let mut config = WebSocketClientConfig::default();
    config.heartbeat = Some(heartbeat);
    config.validate()
}
```

## `HeartbeatRetryConfig::validate()`

### Required features

`ws-client`. Import `HeartbeatRetryConfig` from `open_net::ws`.

### Function signature

```text
pub fn validate(&self) -> Result<(), NetError>
```

### Steps

Choose a bounded number of additional recovery probes and safe Ping write
attempts. Validate, place the policy in `HeartbeatConfig::retry`, then validate
the complete client configuration before creation.

### Parameters

No argument beyond `&self`. All four fields, defaults and limits are listed in
the heartbeat table above; duration values use `std::time::Duration`.

### Callback parameters

None.

### Return values

`Ok(())` for a valid policy; otherwise `Err(NetError)` with `InvalidConfig` and a
field under `heartbeat.retry.*`.

### Errors and behavior

Counts above 10 recovery probes or 1024 write retries fail. An enabled retry path
requires a nonzero interval that fits a monotonic deadline. A zero count disables
that path, so its stored interval is not validated or used. Setting retry counts
does not make a write with unknown delivery safe to repeat.

### Example

This offline configuration enables three additional recovery probes and at most
two additional safe normal-Ping writes. It does not contact a server.

```rust
use open_net::ws::{HeartbeatConfig, HeartbeatRetryConfig, WebSocketClientConfig};
use std::time::Duration;

fn main() -> open_net::Result<()> {
    let retry = HeartbeatRetryConfig {
        pong_recovery_attempts: 3,
        pong_retry_interval: Duration::from_millis(1500),
        ping_write_retries: 2,
        ping_write_retry_interval: Duration::from_millis(250),
    };
    retry.validate()?;
    let mut config = WebSocketClientConfig::default();
    config.heartbeat = Some(HeartbeatConfig {
        retry,
        ..HeartbeatConfig::default()
    });
    config.validate()
}
```

## Explicit HTTP CONNECT proxies

### Required features

`ws-client`; no additional proxy feature. HTTP clients use the separate
[`HttpProxyConfig` API](../http/tls_and_proxy.md#explicit-and-environment-proxies).

### Function signature

```text
ProxyBasicAuth::new(username: &str, password: &str) -> Result<ProxyBasicAuth, NetError>
ProxyConfig::http_connect(url: &str, auth: Option<ProxyBasicAuth>) -> Result<ProxyConfig, NetError>
ProxyConfig::direct() -> ProxyConfig
NetworkConfig::with_proxy(self, proxy: ProxyConfig) -> NetworkConfig
```

### Steps

Create optional proxy credentials, parse an HTTP proxy endpoint, and install
the proxy in a complete network policy together with the application's provider.
Use that policy as an engine default or as a complete per-client replacement.

### Parameters

| Parameter | Required | Description |
| --- | --- | --- |
| `url: &str` | Yes | `http://host[:port]`, at most 2048 bytes; default port 80. No embedded credentials, non-root path, query or fragment. HTTPS and SOCKS proxy URLs are not supported by this API. |
| `auth: Option<ProxyBasicAuth>` | Yes | `None` for no proxy credentials or a separately constructed Basic identity. |
| `username`, `password` | For Basic authentication | Each at most 4096 ASCII bytes, without control characters. Username cannot contain a colon; password may contain ordinary colons. |

### Callback parameters

None.

### Return values

Credential and endpoint constructors return an owned configuration or
`Err(NetError)`. `with_proxy` replaces the policy and returns the updated
`NetworkConfig`. These operations do not connect to the proxy.

### Errors and behavior

Both `ws://` and `wss://` targets use an HTTP CONNECT tunnel when configured.
Default/direct mode never reads proxy environment variables. Proxy credentials
belong to the CONNECT request, not the origin WebSocket handshake. A plain HTTP
proxy does not encrypt those Basic credentials, even when the tunneled target
uses WSS. Configuration Debug output redacts credentials.

Invalid input produces `InvalidConfig` under `proxy.*`. Connection-time proxy
failures carry `ErrorStage::Proxy`; a rejected CONNECT response exposes its status
through `NetError::http_status()` when available. For example, a 407 rejection is
`HandshakeRejected` at `Proxy`, rather than an origin WebSocket 401. Classify the
stage and status instead of parsing formatted error strings.

Per-client `NetworkConfig` replaces TLS, proxy and network-status settings
together. To change only routing, clone an existing policy and call `with_proxy`.
Starting from `NetworkConfig::default()` would also discard its explicit provider
and other settings. HTTP clients do not inherit this WebSocket policy.

### Example

This program prepares proxy-routed and direct clients without connecting. Replace
the demonstration credentials and endpoint before connecting to a real service.

```rust
use open_net::network::{NetworkConfig, ProxyBasicAuth, ProxyConfig, TlsConfig};
use open_net::ws::WebSocketClientConfig;
use open_net::OpenNet;
use std::sync::Arc;

#[tokio::main]
async fn main() -> open_net::Result<()> {
    let auth = ProxyBasicAuth::new("demo-user", "demo-password")?;
    let proxy = ProxyConfig::http_connect("http://127.0.0.1:8080", Some(auth))?;
    let defaults = NetworkConfig::default()
        .with_proxy(proxy)
        .with_tls(TlsConfig::default().with_crypto_provider(Arc::new(
            rustls_openssl::default_provider(),
        )));
    let net = OpenNet::new_with_network_config(defaults.clone())?;
    let _proxied = net.create_ws_client("through-proxy").await?;
    let direct = defaults.with_proxy(ProxyConfig::direct());
    let _direct = net.create_ws_client_with_network_config(
        "direct", WebSocketClientConfig::default(), direct,
    ).await?;
    net.shutdown().await
}
```

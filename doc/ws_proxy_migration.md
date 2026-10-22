# WebSocket HTTPS proxies and authentication migration

The HTTPS CONNECT constructor and proxy authentication transport policy described
here are introduced in 0.3.3. Version 0.3.2 has no `https_connect` or `RequireTls`
policy and accepts Basic credentials with an HTTP proxy. These are WebSocket
proxy contracts; they do not extend the HTTP client's separate proxy API.

This guide covers `open_net::network::ProxyConfig` with the `ws-client` feature.
HTTP clients use the separate
[`HttpProxyConfig` API](http/tls_and_proxy.md#explicit-and-environment-proxies);
this change does not change their proxy policy.

## What changes

`ProxyConfig::http_connect(url, auth)` keeps its signature, but now rejects
`Some(auth)` with `InvalidConfig` before DNS lookup or TCP connection. The default
`ProxyAuthTransportPolicy::RequireTls` requires an encrypted connection to the
proxy whenever Basic credentials are configured. The destination being WSS,
or the proxy being localhost or a private address, does not bypass that rule.

The following constructors are available:

```text
ProxyConfig::direct() -> ProxyConfig
ProxyConfig::http_connect(url: &str, auth: Option<ProxyBasicAuth>) -> Result<ProxyConfig, NetError>
ProxyConfig::https_connect(url: &str, auth: Option<ProxyBasicAuth>, proxy_tls: TlsConfig) -> Result<ProxyConfig, NetError>
ProxyConfig::http_connect_with_auth_policy(url: &str, auth: Option<ProxyBasicAuth>, policy: ProxyAuthTransportPolicy) -> Result<ProxyConfig, NetError>
```

`ProxyConfig::proxy_tls()` returns `Option<&TlsConfig>` for HTTPS proxies.
`ProxyConfig::auth_transport_policy()` returns `Option<ProxyAuthTransportPolicy>`;
direct mode returns `None`, and HTTPS proxy mode always returns `RequireTls`.
The existing endpoint getter continues to return a credential-free normalized URL.

| Configuration | Result |
| --- | --- |
| HTTP proxy without Basic credentials | Allowed |
| HTTP proxy with Basic credentials and the default policy | Configuration error |
| HTTP proxy with Basic credentials and explicit `AllowPlaintext` | Allowed |
| HTTPS proxy, with or without Basic credentials | Verify proxy TLS, then send CONNECT |
| Invalid HTTPS proxy certificate or name | Fail; do not send Basic or fall back |

`http_connect` and `http_connect_with_auth_policy` accept only
`http://host[:port]`, with default port 80. `https_connect` accepts only
`https://host[:port]`, with default port 443. Endpoints are limited to 2048 bytes;
embedded credentials, non-root paths, query strings and fragments are rejected.
Supply credentials using `ProxyBasicAuth::new`, whose username and password are
each limited to 4096 ASCII bytes without control characters. Usernames cannot
contain colons. Neither constructor reads proxy environment variables.

This is a behavioral compatibility change: existing source can still compile
while authenticated `http_connect` calls now return an error in 0.3.3.

## Recommended migration: HTTPS CONNECT

The proxy must provide a TLS endpoint. Changing only `http://` to `https://` does
not make a plaintext proxy accept TLS.

Use the application's provider for both roles explicitly, while keeping their
TLS configurations separate. This complete program constructs clients without
connecting. Use these dependency settings:

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false, features = ["ws-client"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The examples use a third-party provider;
see [TLS providers](tls.md) for its build requirements and alternatives. Replace
the sample endpoint and credentials before connecting.

```rust
use open_net::network::{NetworkConfig, ProxyBasicAuth, ProxyConfig, TlsConfig};
use open_net::tls::CryptoProvider;
use open_net::OpenNet;
use std::sync::Arc;

#[tokio::main]
async fn main() -> open_net::Result<()> {
    let provider: Arc<CryptoProvider> = Arc::new(rustls_openssl::default_provider());
    let proxy_tls = TlsConfig::default().with_crypto_provider(Arc::clone(&provider));
    let target_tls = TlsConfig::default().with_crypto_provider(provider);
    let auth = ProxyBasicAuth::new("demo-user", "demo-password")?;
    let proxy = ProxyConfig::https_connect(
        "https://proxy.example.com:8443",
        Some(auth),
        proxy_tls,
    )?;
    let network = NetworkConfig::default()
        .with_proxy(proxy)
        .with_tls(target_tls);
    network.validate()?;

    let net = OpenNet::new_with_network_config(network)?;
    let _client = net.create_ws_client("through-https-proxy").await?;
    net.shutdown().await
}
```

Each TLS configuration defaults to the built-in WebPKI roots. For a private
proxy CA, supply the CA only to `proxy_tls`. The target retains its own trust
configuration. This helper replaces the proxy roots while retaining the target's
default roots. The complete example reads your CA bundle from `proxy-ca.pem`
without connecting:

```rust
use open_net::network::{NetworkConfig, ProxyConfig, RootCertificateMode, TlsConfig};
use open_net::tls::CryptoProvider;
use open_net::NetError;
use std::sync::Arc;

fn private_proxy(
    provider: Arc<CryptoProvider>,
    proxy_ca_pem: &[u8],
) -> Result<NetworkConfig, NetError> {
    let proxy_tls = TlsConfig::default()
        .with_crypto_provider(Arc::clone(&provider))
        .with_root_certificates(proxy_ca_pem, RootCertificateMode::Replace)?;
    let target_tls = TlsConfig::default().with_crypto_provider(provider);
    Ok(NetworkConfig::default()
        .with_proxy(ProxyConfig::https_connect(
            "https://proxy.example.com",
            None,
            proxy_tls,
        )?)
        .with_tls(target_tls))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proxy_ca_pem = std::fs::read("proxy-ca.pem")?;
    let network = private_proxy(
        Arc::new(rustls_openssl::default_provider()),
        &proxy_ca_pem,
    )?;
    network.validate()?;
    Ok(())
}
```

Configure proxy mTLS using `proxy_tls.with_client_identity(...)` and target mTLS
using `target_tls.with_client_identity(...)`. Neither identity is inherited by
the other session. A per-client `NetworkConfig` still replaces the complete
engine policy, including both TLS roles; clone an existing policy to change
only routing.

## Explicit exception: Basic over a plaintext HTTP proxy

For a deployment that deliberately retains plaintext proxy authentication,
replace the old call with the explicit policy constructor:

```rust
use open_net::network::{ProxyAuthTransportPolicy, ProxyBasicAuth, ProxyConfig};
use open_net::NetError;

fn plaintext_proxy() -> Result<ProxyConfig, NetError> {
    ProxyConfig::http_connect_with_auth_policy(
        "http://127.0.0.1:8080",
        Some(ProxyBasicAuth::new("demo-user", "demo-password")?),
        ProxyAuthTransportPolicy::AllowPlaintext,
    )
}

fn main() -> Result<(), NetError> {
    let _proxy = plaintext_proxy()?;
    Ok(())
}
```

This opt-in transmits reversible Basic credentials without TLS protection on
the client-to-proxy connection. A `wss://` target does not protect the earlier
CONNECT exchange. The application owns this deployment decision; the SDK does
not infer permission from the proxy address. No exception is needed for an HTTP
proxy with `auth: None`.

## TLS boundaries and connection lifecycle

For an HTTPS proxy and WSS target, connection setup is:

1. Validate configuration, resolve the proxy and connect TCP.
2. Verify the proxy TLS certificate using the proxy URL's host and proxy roots.
3. Send and flush HTTP/1.1 CONNECT, including any proxy Basic credentials.
4. Verify target TLS inside the tunnel using the target URL's host and target roots.
5. Send the target WebSocket Upgrade request without `Proxy-Authorization`.

| Proxy / target | Client to proxy | Application data after the proxy |
| --- | --- | --- |
| HTTP / WS | Plaintext CONNECT and traffic | Plaintext |
| HTTP / WSS | Plaintext CONNECT; target TLS runs inside tunnel | Target TLS |
| HTTPS / WS | Proxy TLS protects CONNECT and traffic | Plaintext |
| HTTPS / WSS | Proxy TLS protects CONNECT; target TLS runs inside tunnel | Target TLS |

The proxy knows the CONNECT destination in every proxied mode. Proxy TLS does
not authenticate the target. Target TLS does not authenticate the proxy.
Each TLS role independently uses its explicit provider or the host's global
provider. A provider configured only in `NetworkConfig::with_tls` is not
implicitly reused for proxy TLS.

HTTPS proxies use HTTP/1.1 CONNECT; HTTP/2 CONNECT and SOCKS are not provided.
All setup phases, including both TLS handshakes and flushing CONNECT, share
the existing connection deadline. Cancellation releases the connection and its
TLS layers. There is no automatic downgrade to an HTTP proxy or fallback to a
direct route.

## Errors and diagnostics

| Situation | Public error stage |
| --- | --- |
| Invalid configuration, including forbidden plaintext Basic | Configuration error (`InvalidConfig`) |
| Proxy TLS certificate or TLS protocol failure | `ErrorStage::ProxyTls` |
| CONNECT rejection or malformed CONNECT response | `ErrorStage::Proxy` |
| Target TLS certificate or TLS protocol failure | `ErrorStage::Tls` |

A CONNECT rejection continues to expose its HTTP status through
`NetError::http_status()`. For example, a 407 is `HandshakeRejected` at `Proxy`.
Proxy TLS error provenance is retained when an alert is observed during a later
CONNECT, target handshake, or established connection read/write operation.
Certificate and TLS protocol failures do not trigger automatic retries or
reconnection, including failures observed after connection setup; ordinary
transport interruption and timeout classification retain the existing retry policy.

Match `ErrorStage` with a fallback arm: it is a non-exhaustive public enum.
Configuration `Debug` output does not expose credentials or client private
keys. Continue to use structured error kind, stage and status rather than
parsing formatted messages.

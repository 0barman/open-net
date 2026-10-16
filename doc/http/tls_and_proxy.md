# TLS certificates and HTTP proxies

Required transport feature: `http-client`. Client creation also requires a rustls
cryptographic provider supplied by the application, explicitly per client or as
a host-installed process provider. HTTPS, custom
trust roots, mutual TLS and explicit HTTP proxies need no other open-net feature.
See [TLS providers](../tls.md) for platform setup and dependency choices.

```toml
[dependencies]
open-net = { path = "../open-net", default-features = false, features = ["http-client"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
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

The local path assumes your consumer project is beside this checkout's `open-net` directory; adjust it. The examples validate configuration offline. Fallible `with_*` methods that take `self` consume the input configuration on both success and error; `Err(NetError)` does not return the original object. Clone first if you need to retain a previous configuration for another attempt. A real request also needs a reachable server, a valid hostname/certificate chain and, when configured, a working proxy.

TLS configuration validates HTTPS peers; it does not by itself prohibit an HTTP redirect target. With the optional `http-client-http2` feature, [`HttpVersionPolicy::Http2Only`](http2.md) requires HTTPS for both the initial URL and followed redirect targets, preventing a connection to a downgraded target or request-body replay there. `Auto` and `Http1Only` do not impose that restriction. This is a request-target policy, so an explicitly configured HTTP proxy can still tunnel an HTTPS request through CONNECT.

## Cryptographic provider

```text
HttpTlsConfig::with_crypto_provider(self, provider: Arc<open_net::tls::CryptoProvider>) -> Self
HttpTlsConfig::crypto_provider(&self) -> Option<&Arc<open_net::tls::CryptoProvider>>
```

An explicitly configured provider takes precedence over the process-wide rustls
provider. open-net has no built-in provider or fallback and never installs a
process-wide provider. With no available provider,
client creation returns `InvalidConfig` for `tls.provider`, including for an
initial `http://` URL: HTTP clients prepare a connector that also supports HTTPS.

The setter replaces the explicitly selected provider, shares it through `Arc`,
and does not validate an identity or perform a handshake. Its position relative
to root and identity setters does not matter. The getter reports only the
explicit choice; `None` does not mean that a host-installed process provider is
unavailable. `HttpTlsConfig::default()` has no explicit choice. See the
[complete custom-provider program](../tls.md#select-a-provider-per-client).

## `HttpTlsConfig::with_crypto_provider()`

### Required features

`http-client`, plus an application dependency providing compatible rustls 0.23
cryptography. open-net has no provider feature.

### Function signature

```text
HttpTlsConfig::with_crypto_provider(self, provider: Arc<open_net::tls::CryptoProvider>) -> Self
```

### Steps

Create the application's provider, wrap it in `Arc`, and install it in the TLS
configuration before constructing the HTTP client.

### Parameters

`provider` is a required, owned `Arc`. Configuration clones share that provider.

### Callback parameters

None.

### Return values

The updated `HttpTlsConfig`, replacing any previously explicit provider.

### Errors and behavior

The setter is infallible and does not load keys or install a process-wide
provider. Its order relative to identity and root setters does not matter.
At client creation this explicit selection overrides the host's process provider;
unsupported key material or invalid provider settings can then fail validation.

### Example

The [complete per-client program](../tls.md#select-a-provider-per-client) calls
`HttpTlsConfig::default().with_crypto_provider(Arc::clone(&provider))` before
creating its HTTP client.

## `HttpTlsConfig::crypto_provider()`

### Required features

`http-client`.

### Function signature

```text
HttpTlsConfig::crypto_provider(&self) -> Option<&Arc<open_net::tls::CryptoProvider>>
```

### Steps

Borrow the TLS configuration to inspect its explicit provider before installation.

### Parameters

Only the borrowed TLS configuration.

### Callback parameters

None.

### Return values

`Some(&Arc<CryptoProvider>)` for an explicit selection, or `None` for no explicit
selection. The reference borrows the configuration.

### Errors and behavior

Infallible. It does not resolve or install the process default. `None` alone does
not imply that client creation will fail: the host may have installed a provider.

### Example

The [complete per-client program](../tls.md#select-a-provider-per-client) checks
`http_tls.crypto_provider()` with `Arc::ptr_eq` before constructing the client.

## Trust roots

### Required features

`http-client`.

### Function signature

```text
HttpTlsConfig::default() -> Self
HttpTlsConfig::with_root_certificates(self, pem: impl AsRef<[u8]>, replace_builtin_roots: bool) -> Result<Self, NetError>
HttpTlsConfig::with_root_certificates_mode(self, pem: impl AsRef<[u8]>, mode: HttpRootCertificateMode) -> Result<Self, NetError>
HttpTlsConfig::with_additional_root_certificate(self, pem: impl AsRef<[u8]>) -> Result<Self, NetError>
HttpTlsConfig::custom_root_count(&self) -> usize
HttpTlsConfig::replaces_builtin_roots(&self) -> bool
HttpClientConfig::with_tls(self, tls: HttpTlsConfig) -> Self
```

### Steps

Read a CA PEM bundle, choose whether built-in WebPKI roots should remain trusted, add any further bundles, and install the result before creating the client.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `pem` | `impl AsRef<[u8]>` | Yes | Nonempty, valid UTF-8 PEM bundle containing only `CERTIFICATE` blocks and surrounding whitespace. At most 1 MiB and 128 certificates per bundle. |
| `replace_builtin_roots` | `bool` | Yes | `false` appends custom certificates to built-ins; `true` trusts only custom certificates. |
| `mode` | `HttpRootCertificateMode` | Yes | `Append` or `Replace`, equivalent to the boolean above. |
| `tls` | `HttpTlsConfig` | Yes | Owned settings applied when the client builds its TLS connector. |
| Getters / `default` | None | — | Default uses built-ins and no custom bundles or identity. |

### Callback parameters

None.

### Return values

Root setters return `Ok(HttpTlsConfig)` or `Err(NetError)`. `with_tls` returns the updated client configuration. `replaces_builtin_roots` reports the mode. **`custom_root_count` currently counts stored PEM bundles**, not individual certificates inside those bundles; a bundle with two certificates adds one to this count.

### Errors and behavior

Invalid, oversized, empty, mixed-label or malformed material produces `InvalidConfig` for `tls.root_certificates`. Both `with_root_certificates*` methods replace the previously configured custom bundle list and mode; they do not append to that list. `with_additional_root_certificate` retains existing bundles and current mode, and rejects insertion when 128 bundles already exist. The 128-certificate check is per bundle, not an aggregate across all bundles. Trust roots are validated again during connector creation; an empty effective store is rejected. These APIs do not disable certificate or hostname validation. The built-in trust set is WebPKI, not an assertion that the operating system trust store is loaded.

### Example

Pass a PEM file path as the first command-line argument. The helper illustrates both append and replace modes without needing embedded test certificates. Running without arguments prints the default bundle count and does not read a file.

```rust
use open_net::api::http::{HttpClientConfig, HttpRootCertificateMode, HttpTlsConfig};
use open_net::NetError;

fn trust_examples(pem: &[u8]) -> Result<HttpClientConfig, NetError> {
    let append = HttpTlsConfig::default().with_root_certificates(pem, false)?;
    assert!(!append.replaces_builtin_roots());
    let replace = HttpTlsConfig::default()
        .with_root_certificates_mode(pem, HttpRootCertificateMode::Replace)?
        .with_additional_root_certificate(pem)?;
    assert_eq!(replace.custom_root_count(), 2);
    assert!(replace.replaces_builtin_roots());
    Ok(HttpClientConfig::new("https://example.com/")?.with_tls(replace))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = std::env::args().nth(1) {
        let pem = std::fs::read(path)?;
        trust_examples(&pem)?.validate()?;
    } else {
        println!("custom bundles: {}", HttpTlsConfig::default().custom_root_count());
    }
    Ok(())
}
```

## `HttpTlsConfig::with_client_identity_pem()`

### Required features

`http-client`.

### Function signature

```text
HttpTlsConfig::with_client_identity_pem(self, certificates: impl AsRef<[u8]>, private_key: impl AsRef<[u8]>) -> Result<Self, NetError>
```

### Steps

Read the client certificate chain and its PKCS#8 private key, call this setter on your TLS configuration, and install it with `with_tls`. The peer must trust the client certificate.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `certificates` | `impl AsRef<[u8]>` | Yes | Nonempty certificate chain, at most 1 MiB and 128 `CERTIFICATE` PEM blocks. |
| `private_key` | `impl AsRef<[u8]>` | Yes | Nonempty, at most 1 MiB, exactly one unencrypted PKCS#8 `PRIVATE KEY` PEM block matching the certificate. |

### Callback parameters

None.

### Return values

`Ok(HttpTlsConfig)` with the replacement identity, or `Err(NetError)`; custom roots and root mode are retained.

### Errors and behavior

This setter performs strict, bounded PEM and certificate parsing, rejects wrong
labels, multiple keys and encrypted-key blocks, and retains the decoded material.
It does not load the key through a provider or prove that the key matches the
certificate. `HttpClient::new` or the engine's HTTP factory performs those checks
when compiling the TLS connector with the final selected provider. Unsupported
key algorithms, invalid key contents and key/certificate mismatch then return
`InvalidConfig` under `tls.client_identity` or `.private_key`; malformed input can
fail earlier under `.certificates` or `.private_key`.

`HttpClientConfig::validate()` alone does not compile the TLS connector. Private
material is owned by configuration and omitted from `Debug`; cloning retains
another owned key copy. Neither parsing nor client construction proves that a
server will accept the identity.

### Example

Run with two arguments: certificate-chain PEM and private-key PEM. Without both
arguments, no files are opened. This parses and stores the identity offline;
construct a client to validate it with the selected provider.

```rust
use open_net::api::http::{HttpClientConfig, HttpTlsConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    if let (Some(cert_path), Some(key_path)) = (args.next(), args.next()) {
        let certificates = std::fs::read(cert_path)?;
        let key = std::fs::read(key_path)?;
        let tls = HttpTlsConfig::default().with_client_identity_pem(certificates, key)?;
        HttpClientConfig::new("https://example.com/")?.with_tls(tls).validate()?;
    }
    Ok(())
}
```

## Explicit and environment proxies

### Required features

`http-client`.

### Function signature

```text
HttpProxyConfig::http(url: impl AsRef<str>) -> Result<Self, NetError>
HttpProxyConfig::with_basic_auth(self, username: impl Into<String>, password: impl Into<String>) -> Result<Self, NetError>
HttpClientConfig::with_proxy(self, proxy: HttpProxyConfig) -> Self
HttpClientConfig::with_environment_proxy(self, enabled: bool) -> Self
```

### Steps

Choose an explicit proxy endpoint and optional Basic credentials, or opt into process proxy settings. Apply configuration before creating a client. A normal default client ignores environment proxies.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `url` | `impl AsRef<str>` | Yes | `http://host[:port]`, with no userinfo, non-root path, query or fragment; default port is HTTP's known default. A trailing `/` is accepted. |
| `username` | `impl Into<String>` | Yes for auth | Nonempty, at most 1024 bytes. |
| `password` | `impl Into<String>` | Yes for auth | At most 4096 bytes; empty is allowed. |
| `proxy` | `HttpProxyConfig` | Yes | Explicit proxy for all requests; HTTPS uses CONNECT. |
| `enabled` | `bool` | Yes | Allow transport proxy discovery from process environment variables; default `false`. See the platform-feature qualification below. |

### Callback parameters

None.

### Return values

Proxy construction/auth return `Ok(HttpProxyConfig)` or `Err(NetError)`. Client setters return updated configuration. There are no public URL/credential accessors on `HttpProxyConfig`.

### Errors and behavior

Invalid endpoints return `InvalidConfig` for `proxy.url`; invalid credential lengths return `InvalidConfig` for `proxy.credentials`. This explicit API accepts neither HTTPS proxy URLs nor SOCKS endpoints. Credentials are kept out of `Debug`; origin Basic/Bearer authentication is configured separately.

With this crate's minimal `http-client` dependency features, automatic discovery reads environment proxy settings; it does not include native operating-system proxy discovery. The cached transport enables native macOS/Windows discovery only when the unified dependency graph enables `hyper-util/client-proxy-system` (for example through `reqwest/system-proxy`). Therefore `with_environment_proxy(true)` is not a promise to use operating-system network settings.

`with_proxy` replaces the explicit proxy. `with_environment_proxy(false)` suppresses implicit proxies but does not disable an explicit proxy. The explicit proxy is installed after the environment setting and disables the underlying transport's automatic proxy discovery, so it takes precedence even if `environment_proxy` is true. To remove an explicit proxy before constructing a client, set the public `config.proxy = None`. Actual connection, CONNECT authentication, DNS and TLS failures occur during requests and return transport `NetError`s. The dedicated connect timeout covers proxy setup too.

### Example

```rust
use open_net::api::http::{HttpClientConfig, HttpProxyConfig};
use open_net::NetError;

fn main() -> Result<(), NetError> {
    let proxy = HttpProxyConfig::http("http://127.0.0.1:8080")?
        .with_basic_auth("demo-user", "demo-password")?;
    let config = HttpClientConfig::new("https://example.com/")?
        .with_proxy(proxy).with_environment_proxy(false);
    config.validate()?;
    let environment_config = HttpClientConfig::new("https://example.com/")?
        .with_environment_proxy(true);
    environment_config.validate()
}
```

## Individual function reference

The examples linked below are complete programs with the dependency declaration at the top of this page. Each reference identifies the call that demonstrates the function.

## `HttpTlsConfig::default()`

### Required features

`http-client`.

### Function signature

```text
HttpTlsConfig::default() -> Self
```

### Steps

Start with default TLS settings and optionally add roots or a client identity.

### Parameters

None.

### Callback parameters

None.

### Return values

An owned configuration trusting built-in WebPKI roots, with no custom bundles or client identity.

### Errors and behavior

Infallible. It does not disable hostname/certificate verification and does not promise to load the operating-system trust store.

### Example

See the [complete trust-roots program](#trust-roots), which constructs `HttpTlsConfig::default()` before every trust configuration.

## `HttpTlsConfig::with_root_certificates()`

### Required features

`http-client`.

### Function signature

```text
HttpTlsConfig::with_root_certificates(self, pem: impl AsRef<[u8]>, replace_builtin_roots: bool) -> Result<Self, NetError>
```

### Steps

Read a CA PEM bundle, choose whether to replace built-ins and retain the returned TLS configuration.

### Parameters

`pem` is required UTF-8 PEM, nonempty, at most 1 MiB, with 1..=128 `CERTIFICATE` blocks and whitespace only. Required `replace_builtin_roots`: false retains built-ins; true disables them.

### Callback parameters

None.

### Return values

`Ok(Self)` with one custom bundle and the chosen trust mode, or `Err(NetError)`.

### Errors and behavior

Replaces all prior custom bundles. Invalid, mixed-label, oversized or malformed input returns `InvalidConfig` for `tls.root_certificates`. The receiver is consumed on failure; clone first to retain it. Connector creation validates roots again.

### Example

See the [complete trust-roots program](#trust-roots), whose `trust_examples` calls `.with_root_certificates(pem, false)`.

## `HttpTlsConfig::with_root_certificates_mode()`

### Required features

`http-client`.

### Function signature

```text
HttpTlsConfig::with_root_certificates_mode(self, pem: impl AsRef<[u8]>, mode: HttpRootCertificateMode) -> Result<Self, NetError>
```

### Steps

Read a PEM bundle and select the explicit Append or Replace trust mode.

### Parameters

`pem` has the same required 1 MiB / 128 certificate and strict PEM bounds as `with_root_certificates`. Required `mode` is `Append` or `Replace`.

### Callback parameters

None.

### Return values

`Ok(Self)` with the replacement custom-bundle list and mode, or `Err(NetError)`.

### Errors and behavior

Equivalent to the boolean setter. Append retains built-in roots, not previously configured custom bundles; those are replaced. Invalid input returns `InvalidConfig` for `tls.root_certificates`, consuming the original configuration.

### Example

See the [complete trust-roots program](#trust-roots), which calls `.with_root_certificates_mode(pem, HttpRootCertificateMode::Replace)`.

## `HttpTlsConfig::with_additional_root_certificate()`

### Required features

`http-client`.

### Function signature

```text
HttpTlsConfig::with_additional_root_certificate(self, pem: impl AsRef<[u8]>) -> Result<Self, NetError>
```

### Steps

Start with your desired trust mode, then append a further bundle using the returned configuration.

### Parameters

`pem` is required strict UTF-8 PEM with 1..=128 certificates and at most 1 MiB. The previous configuration must contain fewer than 128 custom bundles.

### Callback parameters

None.

### Return values

`Ok(Self)` retaining previous bundles/mode and appending one owned bundle, or `Err(NetError)`.

### Errors and behavior

Invalid material or an exhausted bundle limit returns `InvalidConfig` for `tls.root_certificates`. The certificate bound is per bundle; the stored-bundle limit is separate. This does not deduplicate roots and consumes the original configuration on failure.

### Example

See the [complete trust-roots program](#trust-roots), which calls `.with_additional_root_certificate(pem)` after selecting Replace.

## `HttpTlsConfig::custom_root_count()`

### Required features

`http-client`.

### Function signature

```text
HttpTlsConfig::custom_root_count(&self) -> usize
```

### Steps

Borrow the configured TLS value before installation to inspect the number of stored custom bundles.

### Parameters

Only the borrowed TLS receiver.

### Callback parameters

None.

### Return values

The number of stored PEM bundles as `usize`.

### Errors and behavior

Infallible. Despite the method name, this counts bundles, not certificates; a bundle containing two certificates contributes one. Built-in WebPKI roots are excluded.

### Example

See the [complete trust-roots program](#trust-roots), which asserts `replace.custom_root_count() == 2` after storing the same bundle twice.

## `HttpTlsConfig::replaces_builtin_roots()`

### Required features

`http-client`.

### Function signature

```text
HttpTlsConfig::replaces_builtin_roots(&self) -> bool
```

### Steps

Inspect this flag before constructing a client to confirm the selected trust mode.

### Parameters

Only the borrowed TLS receiver.

### Callback parameters

None.

### Return values

`true` for Replace; `false` for Append.

### Errors and behavior

Infallible. It reports configuration intent, not the result of a remote TLS handshake.

### Example

See the [complete trust-roots program](#trust-roots), which checks both `append.replaces_builtin_roots()` and `replace.replaces_builtin_roots()`.

## `HttpClientConfig::with_tls()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_tls(self, tls: HttpTlsConfig) -> Self
```

### Steps

Prepare trust/identity settings, install the complete object, then create a new client.

### Parameters

`tls` is required and owned; it replaces all previous TLS settings on this client configuration.

### Callback parameters

None.

### Return values

The updated owned `HttpClientConfig`.

### Errors and behavior

Infallible at this step; setters already validate PEM, and client creation may still fail when compiling the connector. Does not reconfigure an existing client. Cloning TLS configurations also copies retained private-key material.

### Example

See the [complete trust-roots program](#trust-roots), which calls `HttpClientConfig::new(...).with_tls(replace)`.

## `HttpProxyConfig::http()`

### Required features

`http-client`.

### Function signature

```text
HttpProxyConfig::http(url: impl AsRef<str>) -> Result<Self, NetError>
```

### Steps

Parse an explicit HTTP proxy endpoint, optionally add credentials, then install it with `with_proxy`.

### Parameters

`url` is required: `http://host[:port]`, no credentials, query, fragment or non-root path. A trailing slash is allowed.

### Callback parameters

None.

### Return values

`Ok(HttpProxyConfig)` without authentication, or `Err(NetError)`.

### Errors and behavior

Invalid endpoints return `InvalidConfig` for `proxy.url`. This constructor accepts neither HTTPS proxy URLs nor SOCKS. HTTPS origin requests use CONNECT through the HTTP proxy. Parsing does not connect to it.

### Example

See the [complete proxy program](#explicit-and-environment-proxies), which calls `HttpProxyConfig::http("http://127.0.0.1:8080")`.

## `HttpProxyConfig::with_basic_auth()`

### Required features

`http-client`.

### Function signature

```text
HttpProxyConfig::with_basic_auth(self, username: impl Into<String>, password: impl Into<String>) -> Result<Self, NetError>
```

### Steps

Parse a proxy endpoint first, attach proxy credentials, and install the returned proxy configuration.

### Parameters

Required `username` is nonempty and at most 1024 bytes. Required `password` may be empty and is at most 4096 bytes. Both become owned strings.

### Callback parameters

None.

### Return values

`Ok(Self)` with replacement proxy credentials, or `Err(NetError)`.

### Errors and behavior

Invalid lengths return `InvalidConfig` for `proxy.credentials`, consuming the original configuration. Credentials are omitted from Debug. This configures proxy authentication independently of origin Basic/Bearer auth; success does not prove the proxy will accept them.

### Example

See the [complete proxy program](#explicit-and-environment-proxies), which calls `.with_basic_auth("demo-user", "demo-password")`.

## `HttpClientConfig::with_proxy()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_proxy(self, proxy: HttpProxyConfig) -> Self
```

### Steps

Create a proxy configuration and install it before client construction.

### Parameters

`proxy` is required and owned. It replaces the previous explicit proxy for all requests.

### Callback parameters

None.

### Return values

The updated owned configuration.

### Errors and behavior

Infallible at installation. Explicit proxy selection takes precedence over environment discovery, even with `environment_proxy = true`. To remove it before client creation, assign the public `config.proxy = None`. Connection/authentication failures occur during requests.

### Example

See the [complete proxy program](#explicit-and-environment-proxies), which calls `.with_proxy(proxy)` before setting the environment preference.

## `HttpClientConfig::with_environment_proxy()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_environment_proxy(self, enabled: bool) -> Self
```

### Steps

Opt into process environment proxy discovery before client construction, if the application needs it.

### Parameters

`enabled` is required; false is the default and suppresses implicit proxies, true permits discovery.

### Callback parameters

None.

### Return values

The updated owned configuration.

### Errors and behavior

Infallible. An explicit proxy still takes precedence. Minimal dependencies discover environment variables; native OS proxy discovery additionally depends on unified transport features, as detailed above. This is not a promise to follow operating-system network settings.

### Example

See the [complete proxy program](#explicit-and-environment-proxies), which constructs configurations with both `.with_environment_proxy(false)` and `.with_environment_proxy(true)`.

[HTTP guide index](README.md)

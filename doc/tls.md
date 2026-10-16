# TLS providers

open-net uses rustls for the TLS protocol in both HTTPS and WSS. Its
`CryptoProvider` supplies cryptographic algorithms, random generation, signature
verification and private-key loading. Replacing that provider changes the
cryptographic implementation; it does not replace rustls's TLS handshake,
certificate-validation policy or HTTP/WebSocket transport.

## Cargo features and provider setup

The default feature is `ws-client`, which also enables `net-status`.
An HTTP-only consumer can use `default-features = false, features =
["http-client"]`. Both transport features compile rustls without selecting a
cryptographic provider. open-net has no built-in provider, provider feature or
automatic fallback. Network observation alone (`net-status`) does not need one.

Applications must choose a compatible rustls **0.23** provider before
creating HTTP or WebSocket clients. Add the chosen provider as an application
dependency, then pass it in each TLS configuration or install it once in the
host process. Without a provider, default constructors return a structured
configuration error, including clients intended for plain HTTP or WS, because
their connectors are prepared for HTTPS/WSS at creation.

When `ws-client` is enabled, `OpenNet::new` and `OpenNet::new_with_config` compile
the engine's default WebSocket connector immediately. They therefore require a
provider before the engine is created, even if the application will initially
create only HTTP or network-observation clients. Use
`OpenNet::new_with_network_config` with an explicit `TlsConfig` provider, install
the host's process provider first, or disable `ws-client` if it is not needed.

The examples below use `rustls-openssl` as one third-party provider that
does not require AWS-LC or ring. This is an example application dependency,
not an open-net production dependency or a required provider. Its `tls12` feature enables
TLS 1.2; disabling its default features omits the `prefer-post-quantum`
preference. Algorithm availability still depends on the linked OpenSSL version
and configuration; omitting that preference does not prohibit post-quantum groups.

## Build requirements for the OpenSSL example

The example dependency uses installed OpenSSL libraries and development
headers. On macOS, `openssl-sys` can discover Homebrew installations; on Ubuntu,
install `pkg-config` and `libssl-dev`; on Windows, use a matching vcpkg OpenSSL
installation or supply `OPENSSL_DIR`. The library directory, headers and compiler
must match the Rust target architecture. See the upstream
[OpenSSL build instructions](https://docs.rs/openssl/latest/openssl/#building)
for discovery and cross-compilation environment variables.

Add `"vendored"` to the application's `rustls-openssl` dependency features to
compile and statically link the bundled OpenSSL source. This requires a C
compiler, Perl and make, with the corresponding build
tools on Windows. It increases native compilation work and avoids requiring a
separately installed OpenSSL for the build unless `OPENSSL_NO_VENDOR` overrides
that choice. Provider selection does not change open-net's built-in WebPKI trust
roots into the operating system or OpenSSL trust store.

## Provider precedence and lifetime

HTTP and WebSocket use the same order:

1. The provider passed to the client's `with_crypto_provider`.
2. A process-wide rustls provider already installed by the host application.

open-net never calls `install_default()` on the application's behalf. If none of
these choices is available, client creation returns `NetError` with kind
`InvalidConfig` and configuration field `tls.provider`. It does not panic or
silently enable AWS-LC or ring. A client compiles its TLS configuration at
creation, even when its first connection uses plain HTTP or WS; configure the
provider before creating that client.

`TlsConfig::crypto_provider()` and `HttpTlsConfig::crypto_provider()` return
`Option<&Arc<open_net::tls::CryptoProvider>>` for the explicitly configured
provider. They do not resolve the process default. Clones share an explicit provider
through `Arc`; a running client retains its compiled configuration. Create a new
client to change its provider.

WebSocket clients inherit the engine's `NetworkConfig` unless a complete
per-client override is supplied. HTTP uses its own `HttpTlsConfig`; the
WebSocket engine policy does not implicitly configure HTTP clients.

## Select a provider per client

This example supplies a third-party provider directly. In a consumer crate
beside the `open-net` directory, add:

```toml
[dependencies]
open-net = { path = "../open-net", default-features = false, features = ["ws-client", "http-client"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Replace the path with your checkout's library directory. Copy this complete
program to `src/main.rs` and run `cargo run`. It creates both clients and shuts
them down without contacting a server:

```rust
use open_net::api::http::{HttpClient, HttpClientConfig, HttpTlsConfig};
use open_net::network::{NetworkConfig, TlsConfig};
use open_net::tls::CryptoProvider;
use open_net::{NetError, OpenNet};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), NetError> {
    let provider: Arc<CryptoProvider> = Arc::new(rustls_openssl::default_provider());
    let ws_tls = TlsConfig::default().with_crypto_provider(Arc::clone(&provider));
    assert!(Arc::ptr_eq(ws_tls.crypto_provider().unwrap(), &provider));
    let network = NetworkConfig::default().with_tls(ws_tls);
    network.validate()?;
    let engine = OpenNet::new_with_network_config(network)?;
    let websocket = engine.create_ws_client("provider-websocket").await?;
    websocket.shutdown().await?;

    let http_tls = HttpTlsConfig::default().with_crypto_provider(Arc::clone(&provider));
    assert!(Arc::ptr_eq(http_tls.crypto_provider().unwrap(), &provider));
    let config = HttpClientConfig::new("https://example.com/")?.with_tls(http_tls);
    let http = HttpClient::new(config, "provider-http")?;
    http.shutdown().await?;
    engine.shutdown().await?;
    Ok(())
}
```

Another provider can replace `rustls_openssl::default_provider()` if it produces
a compatible rustls **0.23** `CryptoProvider`. Check its supported platforms,
algorithms, private-key formats, TLS versions and production-use guidance.
Passing a provider is an explicit choice; open-net does not silently supplement
unsupported algorithms using a different cryptographic library. Keep rustls's
default features disabled in any direct rustls dependency as well.

## Install a provider in the host application

An application that owns process-wide selection can instead call its provider's
`install_default()` before creating clients. That is a one-time process decision;
per-client configuration avoids depending on its installation order and remains
higher priority. Libraries should leave this decision to their host.

With the same consumer dependencies above, this complete alternative program
installs the application provider, creates an HTTP client and shuts it down
without contacting a server:

```rust
use open_net::api::http::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    rustls_openssl::default_provider()
        .install_default()
        .map_err(|_| std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "the application's rustls provider was already installed",
        ))?;
    let client = HttpClient::new(
        HttpClientConfig::new("https://example.com/")?,
        "application-provider",
    )?;
    client.shutdown().await?;
    Ok(())
}
```

Do not discard an installation error if the application requires a particular
provider: another component may already have chosen a different one. Explicit
per-client injection remains available without changing the process default.

## Client identities and validation timing

`ClientIdentity::from_pem` for WebSocket and
`HttpTlsConfig::with_client_identity_pem` for HTTP perform strict, bounded PEM
parsing and retain the decoded certificate chain and unencrypted PKCS#8 key.
Each input is at most 1 MiB, with 1–128 certificate blocks and exactly one
`PRIVATE KEY` block. These APIs do not load keys using a fixed provider.

Provider-specific private-key loading and certificate/key matching happen with
the final selected provider: during `NetworkConfig::validate()` and WebSocket
connector creation, or during HTTP connector creation by `HttpClient::new` or
the engine's HTTP factory. `HttpClientConfig::validate()` alone does not compile
the HTTP TLS connector. Setters can therefore succeed before an unsupported key
or mismatched identity is rejected. Selecting the provider before or after
setting the identity has the same effect. Private key material stays redacted
from configuration `Debug` output.

## Verify the final dependency graph

open-net's rustls dependency does not enable AWS-LC or ring. The OpenSSL provider
does not require either one. Cargo features are additive across the full
application: another dependency can still enable rustls's `aws_lc_rs` or `ring`
features or depend on those libraries directly. Inspect the consuming
application, with its intended features and target:

```sh
cargo tree --edges normal,build
cargo tree --edges features --invert rustls
```

An entry in `Cargo.lock` alone does not prove that a library is part of a given
production build. Conversely, disabling defaults in one dependency declaration
does not cancel a feature enabled elsewhere in the graph. Repeat the dependency
check for the actual supported targets when changing providers or features.

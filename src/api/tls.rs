//! Cryptography shared by the rustls HTTP and WebSocket transports.
//!
//! Supply an `Arc<CryptoProvider>` to a transport's TLS configuration to select
//! its algorithms and key loader independently of the process default. When no
//! provider is supplied, clients use the application's process default. The SDK
//! never installs a process default and has no built-in cryptographic provider.
//!
//! Choose a rustls 0.23 compatible provider in the application. Client creation
//! fails with `InvalidConfig` if no provider is available; the SDK never
//! implicitly enables OpenSSL, AWS-LC or ring.

pub use rustls::crypto::CryptoProvider;

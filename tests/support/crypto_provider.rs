//! Test application initialization. The SDK never selects a provider itself.

#[ctor::ctor]
fn install_test_crypto_provider() {
    if std::env::var_os("OPEN_NET_SKIP_TEST_CRYPTO_PROVIDER").is_none() {
        rustls_openssl::default_provider()
            .install_default()
            .expect("test process crypto provider must be installed exactly once");
    }
}

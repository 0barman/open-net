#![cfg(feature = "ws-client")]

use open_net::network::{
    ClientIdentity, NetworkConfig, NetworkStatusPolicy, ProxyAuthTransportPolicy, ProxyBasicAuth,
    ProxyConfig, RootCertificateMode, TlsConfig,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CA: &[u8] = include_bytes!("../src/test_fixtures/network/ca.pem");
const OTHER_CA: &[u8] = include_bytes!("../src/test_fixtures/network/other-ca.pem");
const CLIENT: &[u8] = include_bytes!("../src/test_fixtures/network/client.pem");
const CLIENT_KEY: &[u8] = include_bytes!("../src/test_fixtures/network/client-key.pem");

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[test]
fn defaults_are_observable_and_validate_without_changing_policy() -> TestResult {
    let config = NetworkConfig::default();
    config.validate()?;
    check(config.proxy().is_direct(), "default proxy must be direct")?;
    check(
        config.proxy().endpoint().is_none(),
        "direct proxy has no endpoint",
    )?;
    check(
        !config.proxy().is_authenticated(),
        "direct proxy has no authentication",
    )?;
    check(
        config.proxy().proxy_tls().is_none() && config.proxy().auth_transport_policy().is_none(),
        "direct proxy has no TLS or authentication transport policy",
    )?;
    check(
        config.tls().root_mode() == RootCertificateMode::Append,
        "default TLS must retain WebPKI roots",
    )?;
    check(
        config.tls().custom_root_count() == 0,
        "default has no custom roots",
    )?;
    check(
        config.tls().client_identity().is_none(),
        "default has no identity",
    )?;
    check(
        config.network_status_policy() == NetworkStatusPolicy::Ignore,
        "default network status policy must be Ignore",
    )?;
    let direct = ProxyConfig::direct();
    check(
        direct.is_direct() && direct.endpoint().is_none(),
        "explicit direct proxy",
    )
}

#[test]
fn proxy_endpoint_is_normalized_owned_and_separate_from_credentials() -> TestResult {
    let proxy = {
        let url = String::from("http://Proxy.Example:0080/");
        let username = String::from("network-api-user");
        let password = String::from("network-api-password");
        ProxyConfig::http_connect_with_auth_policy(
            &url,
            Some(ProxyBasicAuth::new(&username, &password)?),
            ProxyAuthTransportPolicy::AllowPlaintext,
        )?
    };
    check(!proxy.is_direct(), "HTTP CONNECT proxy must not be direct")?;
    check(
        proxy.is_authenticated(),
        "authentication must remain configured",
    )?;
    check(
        proxy.auth_transport_policy() == Some(ProxyAuthTransportPolicy::AllowPlaintext)
            && proxy.proxy_tls().is_none(),
        "explicit plaintext exception must remain observable",
    )?;
    check(
        proxy.endpoint() == Some("http://proxy.example:80"),
        "endpoint must normalize the host and port without credentials or path",
    )?;
    let ipv6 = ProxyConfig::http_connect("http://[::1]:8080/", None)?;
    check(
        ipv6.endpoint() == Some("http://[::1]:8080"),
        "IPv6 endpoint must retain brackets",
    )?;
    check(
        !ipv6.is_authenticated(),
        "unauthenticated proxy must stay unauthenticated",
    )?;
    let default_port = ProxyConfig::http_connect("http://proxy.example", None)?;
    check(
        default_port.endpoint() == Some("http://proxy.example:80"),
        "omitted HTTP proxy port must become 80",
    )?;
    let cloned = proxy.clone();
    drop(proxy);
    check(
        cloned.endpoint() == Some("http://proxy.example:80") && cloned.is_authenticated(),
        "cloned proxy must own its normalized endpoint and credentials",
    )
}

#[test]
fn getters_preserve_full_configuration_and_replacement_semantics() -> TestResult {
    let original = NetworkConfig::default();
    let identity = ClientIdentity::from_pem([CLIENT, CA].concat(), CLIENT_KEY.to_vec())?;
    check(
        identity.certificate_count() == 2,
        "identity must report its full chain",
    )?;
    let tls = TlsConfig::default()
        .with_root_certificates([CA, OTHER_CA].concat(), RootCertificateMode::Replace)?
        .with_client_identity(identity);
    let config = original
        .clone()
        .with_proxy(ProxyConfig::http_connect("http://localhost:8080", None)?)
        .with_tls(tls)
        .with_network_status_policy(NetworkStatusPolicy::PauseOnUnavailable);
    config.validate()?;
    check(
        config.tls().custom_root_count() == 2,
        "both custom roots must remain",
    )?;
    check(
        config.tls().root_mode() == RootCertificateMode::Replace,
        "exclusive trust roots must stay exclusive",
    )?;
    let observed_identity = config
        .tls()
        .client_identity()
        .ok_or_else(|| std::io::Error::other("configured identity is missing"))?;
    check(
        observed_identity.certificate_count() == 2,
        "identity getter must retain the chain",
    )?;
    check(
        config.network_status_policy() == NetworkStatusPolicy::PauseOnUnavailable,
        "policy getter must preserve explicit opt-in",
    )?;
    check(
        original.proxy().is_direct(),
        "cloning must preserve the original proxy",
    )?;
    check(
        original.tls().custom_root_count() == 0,
        "cloning must preserve original roots",
    )?;

    let replaced_roots = config
        .tls()
        .clone()
        .with_root_certificates(CA, RootCertificateMode::Append)?;
    check(
        replaced_roots.custom_root_count() == 1,
        "root setter must replace the custom bundle",
    )?;
    check(
        replaced_roots.root_mode() == RootCertificateMode::Append
            && replaced_roots.client_identity().is_some(),
        "root replacement must keep the identity and select the requested trust mode",
    )?;
    let replaced_tls = config.clone().with_tls(TlsConfig::default());
    replaced_tls.validate()?;
    check(
        replaced_tls.tls().custom_root_count() == 0
            && replaced_tls.tls().client_identity().is_none(),
        "TLS replacement must replace roots and identity together",
    )?;
    check(
        replaced_tls.proxy().endpoint() == Some("http://localhost:8080")
            && replaced_tls.network_status_policy() == NetworkStatusPolicy::PauseOnUnavailable,
        "TLS replacement must preserve independent network settings",
    )
}

#[test]
fn public_configuration_still_rejects_invalid_proxy_and_tls_material() -> TestResult {
    for url in [
        "http://user:password@localhost:8080",
        "http://localhost:0",
        "http://localhost:65536",
        "http://localhost:8080/path",
        "https://localhost:8080",
    ] {
        check(
            matches!(
                ProxyConfig::http_connect(url, None),
                Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::InvalidConfig)),
            "invalid proxy material must be rejected before construction",
        )?;
    }
    for pem in [
        Vec::new(),
        [CA, b"trailing data"].concat(),
        [CA, CLIENT_KEY].concat(),
    ] {
        check(
            matches!(
                TlsConfig::default().with_root_certificates(pem, RootCertificateMode::Replace),
                Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::InvalidConfig)),
            "invalid exclusive trust roots must remain rejected",
        )?;
    }
    let mismatched =
        ClientIdentity::from_pem(CLIENT, include_bytes!("fixtures/network/server-key.pem"))?;
    check(
        matches!(
            NetworkConfig::default().with_tls(TlsConfig::default().with_client_identity(mismatched)).validate(),
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::InvalidConfig)),
        "mismatched private key must remain rejected",
    )
}

#[test]
fn observing_configuration_does_not_change_secret_redaction() -> TestResult {
    let auth = ProxyBasicAuth::new("network-api-user", "network-api-password")?;
    let identity = ClientIdentity::from_pem(CLIENT, CLIENT_KEY)?;
    let config = NetworkConfig::default()
        .with_proxy(ProxyConfig::http_connect_with_auth_policy(
            "http://private-proxy.example:8080",
            Some(auth.clone()),
            ProxyAuthTransportPolicy::AllowPlaintext,
        )?)
        .with_tls(TlsConfig::default().with_client_identity(identity.clone()));
    config.validate()?;
    let output = format!("{config:?} {auth:?} {identity:?}");
    for sensitive in [
        "network-api-user",
        "network-api-password",
        "PRIVATE KEY",
        "private-proxy.example",
    ] {
        check(
            !output.contains(sensitive),
            "configuration Debug must remain redacted",
        )?;
    }
    check(
        config.proxy().is_authenticated(),
        "observation must not remove authentication",
    )?;
    check(
        config.tls().client_identity().is_some(),
        "observation must not remove the client identity",
    )
}

#[test]
fn invalid_network_configuration_reports_fields_without_exposing_input() -> TestResult {
    let failures = [
        (
            ProxyConfig::http_connect("http://user:secret@localhost", None).err(),
            "proxy.url",
        ),
        (
            ProxyBasicAuth::new("invalid:name", "secret").err(),
            "proxy.username",
        ),
        (
            ProxyBasicAuth::new("user", "secret\n").err(),
            "proxy.password",
        ),
        (
            TlsConfig::default()
                .with_root_certificates(b"secret-invalid-pem", RootCertificateMode::Replace)
                .err(),
            "tls.root_certificates",
        ),
        (
            ClientIdentity::from_pem(b"secret-invalid-certificate", CLIENT_KEY).err(),
            "tls.client_identity.certificates",
        ),
        (
            ClientIdentity::from_pem(CLIENT, b"secret-invalid-key").err(),
            "tls.client_identity.private_key",
        ),
    ];
    for (failure, field) in failures {
        let failure =
            failure.ok_or_else(|| std::io::Error::other("invalid configuration succeeded"))?;
        let detail = failure
            .config_error()
            .ok_or_else(|| std::io::Error::other("configuration detail missing"))?;
        check(
            detail.field() == field && !detail.reason().is_empty(),
            "invalid network field not identified",
        )?;
        check(
            !format!("{failure} {failure:?}").contains("secret"),
            "configuration failure leaked input",
        )?;
    }
    Ok(())
}

#[test]
fn plaintext_proxy_authentication_requires_explicit_exception() -> TestResult {
    check(
        ProxyAuthTransportPolicy::default() == ProxyAuthTransportPolicy::RequireTls,
        "authentication transport policy must default to TLS",
    )?;
    let auth = ProxyBasicAuth::new("network-api-user", "network-api-password")?;
    for url in [
        "http://proxy.example",
        "http://localhost",
        "http://127.0.0.1",
        "http://[::1]",
        "http://192.168.1.1",
    ] {
        for rejected in [
            ProxyConfig::http_connect(url, Some(auth.clone())),
            ProxyConfig::http_connect_with_auth_policy(
                url,
                Some(auth.clone()),
                ProxyAuthTransportPolicy::RequireTls,
            ),
        ] {
            let error = rejected
                .err()
                .ok_or_else(|| std::io::Error::other("plaintext authentication was accepted"))?;
            check(
                error.kind() == open_net::error::ErrorKind::InvalidConfig
                    && error
                        .config_error()
                        .is_some_and(|detail| detail.field() == "proxy.auth_transport_policy"),
                "plaintext credentials must fail with an actionable policy field",
            )?;
            let rendered = format!("{error} {error:?}");
            check(
                !rendered.contains("network-api-user")
                    && !rendered.contains("network-api-password"),
                "policy errors must not expose credentials",
            )?;
        }
        let proxy = ProxyConfig::http_connect_with_auth_policy(
            url,
            Some(auth.clone()),
            ProxyAuthTransportPolicy::AllowPlaintext,
        )?;
        NetworkConfig::default().with_proxy(proxy).validate()?;
        let unauthenticated = ProxyConfig::http_connect(url, None)?;
        check(
            unauthenticated.auth_transport_policy() == Some(ProxyAuthTransportPolicy::RequireTls),
            "unauthenticated HTTP must preserve the safe default policy",
        )?;
    }
    Ok(())
}

#[test]
fn https_proxy_urls_are_normalized_and_enforce_their_scheme() -> TestResult {
    let auth = ProxyBasicAuth::new("network-api-user", "network-api-password")?;
    for (url, expected) in [
        ("https://Proxy.Example", "https://proxy.example:443"),
        ("https://Proxy.Example:00443/", "https://proxy.example:443"),
        ("https://[::1]:8443/", "https://[::1]:8443"),
    ] {
        let proxy = ProxyConfig::https_connect(url, Some(auth.clone()), TlsConfig::default())?;
        check(
            proxy.endpoint() == Some(expected)
                && proxy.is_authenticated()
                && proxy.proxy_tls().is_some()
                && proxy.auth_transport_policy() == Some(ProxyAuthTransportPolicy::RequireTls),
            "HTTPS proxy must normalize its endpoint and retain its independent TLS policy",
        )?;
        NetworkConfig::default().with_proxy(proxy).validate()?;
    }
    for url in [
        "http://proxy.example",
        "https://user:secret@proxy.example",
        "https://proxy.example:0",
        "https://proxy.example:65536",
        "https://proxy.example:",
        "https://proxy.example:not-a-port",
        "https://proxy.example/path",
        "https://proxy.example?query",
        "https://proxy.example/#fragment",
        "https://[not-ipv6]",
    ] {
        let error = ProxyConfig::https_connect(url, None, TlsConfig::default())
            .err()
            .ok_or_else(|| std::io::Error::other("invalid HTTPS proxy URL succeeded"))?;
        check(
            error
                .config_error()
                .is_some_and(|detail| detail.field() == "proxy.url"),
            "HTTPS URL restrictions must identify proxy.url",
        )?;
    }
    Ok(())
}

#[test]
fn proxy_and_target_tls_getters_preserve_independent_configuration() -> TestResult {
    use std::sync::Arc;

    let proxy_provider = Arc::new(rustls_openssl::default_provider());
    let target_provider = Arc::new(rustls_openssl::default_provider());
    let proxy_tls = TlsConfig::default()
        .with_crypto_provider(Arc::clone(&proxy_provider))
        .with_root_certificates([CA, OTHER_CA].concat(), RootCertificateMode::Replace)?;
    let target_tls = TlsConfig::default()
        .with_crypto_provider(Arc::clone(&target_provider))
        .with_root_certificates(CA, RootCertificateMode::Append)?
        .with_client_identity(ClientIdentity::from_pem(CLIENT, CLIENT_KEY)?);
    let config = NetworkConfig::default()
        .with_proxy(ProxyConfig::https_connect(
            "https://private-proxy.example:8443",
            Some(ProxyBasicAuth::new(
                "network-api-user",
                "network-api-password",
            )?),
            proxy_tls,
        )?)
        .with_tls(target_tls);
    config.validate()?;
    let proxy_tls = config
        .proxy()
        .proxy_tls()
        .ok_or_else(|| std::io::Error::other("HTTPS proxy TLS configuration missing"))?;
    check(
        proxy_tls.custom_root_count() == 2
            && proxy_tls.root_mode() == RootCertificateMode::Replace
            && proxy_tls.client_identity().is_none()
            && proxy_tls
                .crypto_provider()
                .is_some_and(|provider| Arc::ptr_eq(provider, &proxy_provider)),
        "proxy roots, identity, and provider must be independent from target TLS",
    )?;
    check(
        config.tls().custom_root_count() == 1
            && config.tls().root_mode() == RootCertificateMode::Append
            && config.tls().client_identity().is_some()
            && config
                .tls()
                .crypto_provider()
                .is_some_and(|provider| Arc::ptr_eq(provider, &target_provider)),
        "target TLS must retain its separate trust, identity, and provider",
    )?;
    let replaced = config.clone().with_tls(TlsConfig::default());
    check(
        replaced
            .proxy()
            .proxy_tls()
            .is_some_and(|tls| tls.custom_root_count() == 2)
            && replaced.tls().custom_root_count() == 0,
        "replacing target TLS must preserve the proxy's TLS policy",
    )?;
    let debug = format!("{config:?}");
    for sensitive in [
        "private-proxy.example",
        "network-api-user",
        "network-api-password",
        "PRIVATE KEY",
        "BEGIN CERTIFICATE",
    ] {
        check(
            !debug.contains(sensitive),
            "HTTPS proxy Debug must redact credentials, endpoint, and certificate material",
        )?;
    }
    check(
        debug.contains("HttpsConnect"),
        "Debug must distinguish HTTPS CONNECT",
    )
}

#[test]
fn proxy_identity_validation_reports_proxy_tls_role() -> TestResult {
    let mismatch =
        ClientIdentity::from_pem(CLIENT, include_bytes!("fixtures/network/server-key.pem"))?;
    let proxy = ProxyConfig::https_connect(
        "https://proxy.example",
        None,
        TlsConfig::default().with_client_identity(mismatch),
    )?;
    let error = NetworkConfig::default()
        .with_proxy(proxy)
        .validate()
        .err()
        .ok_or_else(|| std::io::Error::other("mismatched proxy identity succeeded"))?;
    check(
        error
            .config_error()
            .is_some_and(|detail| detail.field() == "proxy.tls.client_identity.private_key"),
        "proxy identity error must identify the proxy TLS role",
    )
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

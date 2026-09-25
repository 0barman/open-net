#![cfg(feature = "ws-client")]

use open_net::network::{
    ClientIdentity, NetworkConfig, NetworkStatusPolicy, ProxyBasicAuth, ProxyConfig,
    RootCertificateMode, TlsConfig,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CA: &[u8] = include_bytes!("fixtures/network/ca.pem");
const OTHER_CA: &[u8] = include_bytes!("fixtures/network/other-ca.pem");
const CLIENT: &[u8] = include_bytes!("fixtures/network/client.pem");
const CLIENT_KEY: &[u8] = include_bytes!("fixtures/network/client-key.pem");

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
        ProxyConfig::http_connect(&url, Some(ProxyBasicAuth::new(&username, &password)?))?
    };
    check(!proxy.is_direct(), "HTTP CONNECT proxy must not be direct")?;
    check(
        proxy.is_authenticated(),
        "authentication must remain configured",
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
    check(
        matches!(
            ClientIdentity::from_pem(CLIENT, include_bytes!("fixtures/network/server-key.pem")),
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::InvalidConfig)),
        "mismatched private key must remain rejected",
    )
}

#[test]
fn observing_configuration_does_not_change_secret_redaction() -> TestResult {
    let auth = ProxyBasicAuth::new("network-api-user", "network-api-password")?;
    let identity = ClientIdentity::from_pem(CLIENT, CLIENT_KEY)?;
    let config = NetworkConfig::default()
        .with_proxy(ProxyConfig::http_connect(
            "http://private-proxy.example:8080",
            Some(auth.clone()),
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

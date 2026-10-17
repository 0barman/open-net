//! Public construction tests also run with no SDK provider feature enabled.
#![cfg(any(feature = "ws-client", feature = "http-client"))]

use open_net::tls::CryptoProvider;
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[tokio::test]
async fn injected_provider_builds_clients_without_installing_a_process_default() -> TestResult {
    let before = CryptoProvider::get_default().cloned();
    let provider = Arc::new(rustls_openssl::default_provider());
    #[cfg(feature = "ws-client")]
    {
        use open_net::network::{ClientIdentity, NetworkConfig, TlsConfig};
        let identity = ClientIdentity::from_pem(
            include_bytes!("../src/test_fixtures/network/client.pem"),
            include_bytes!("../src/test_fixtures/network/client-key.pem"),
        )?;
        let tls = TlsConfig::default()
            .with_client_identity(identity)
            .with_crypto_provider(provider.clone());
        check(
            Arc::ptr_eq(
                tls.crypto_provider()
                    .ok_or("missing explicit WS provider")?,
                &provider,
            ),
            "explicit WS provider was not retained",
        )?;
        let engine =
            open_net::OpenNet::new_with_network_config(NetworkConfig::default().with_tls(tls))?;
        drop(engine);
    }
    #[cfg(feature = "http-client")]
    {
        use open_net::api::http::{HttpClient, HttpClientConfig, HttpTlsConfig};
        let tls = HttpTlsConfig::default()
            .with_client_identity_pem(
                include_bytes!("../src/test_fixtures/network/client.pem"),
                include_bytes!("../src/test_fixtures/network/client-key.pem"),
            )?
            .with_crypto_provider(provider.clone());
        check(
            Arc::ptr_eq(
                tls.crypto_provider()
                    .ok_or("missing explicit HTTP provider")?,
                &provider,
            ),
            "explicit HTTP provider was not retained",
        )?;
        let client = HttpClient::new(
            HttpClientConfig::new("https://localhost")?.with_tls(tls),
            "injected-provider",
        )?;
        client.shutdown().await?;
    }
    match (before, CryptoProvider::get_default()) {
        (None, None) => {}
        (Some(before), Some(after)) => check(
            Arc::ptr_eq(&before, &after),
            "client creation changed the process provider",
        )?,
        _ => return Err("client creation changed the process provider".into()),
    }
    Ok(())
}

#[cfg(feature = "ws-client")]
#[tokio::test]
async fn injected_provider_reaches_engine_and_per_client_ws_factories() -> TestResult {
    use open_net::network::{NetworkConfig, TlsConfig};
    use open_net::{OpenNet, OpenNetConfig};

    let provider = Arc::new(rustls_openssl::default_provider());
    let network = NetworkConfig::default()
        .with_tls(TlsConfig::default().with_crypto_provider(Arc::clone(&provider)));
    let engine =
        OpenNet::new_with_config(OpenNetConfig::default().with_network_config(network.clone()))?;

    let inherited = engine
        .create_ws_client_with_config(
            "provider-inherited",
            open_net::ws::WebSocketClientConfig::default(),
        )
        .await?;
    let overridden = engine
        .create_ws_client_with_network_config(
            "provider-overridden",
            open_net::ws::WebSocketClientConfig::default(),
            network,
        )
        .await?;

    inherited.shutdown().await?;
    overridden.shutdown().await?;
    engine.destroy_ws_client("provider-inherited").await?;
    engine.destroy_ws_client("provider-overridden").await?;
    Ok(())
}

#[test]
fn missing_provider_returns_configuration_error_without_panicking() -> TestResult {
    check(
        CryptoProvider::get_default().is_none(),
        "test process unexpectedly has a default provider",
    )?;
    fn check_error(error: open_net::NetError) -> TestResult {
        if error.kind() != open_net::error::ErrorKind::InvalidConfig {
            return Err("missing provider did not return InvalidConfig".into());
        }
        if error.config_error().map(|error| error.field()) != Some("tls.provider") {
            return Err("missing provider error did not identify tls.provider".into());
        }
        if error.context().stage != Some(open_net::error::ErrorStage::Configuration) {
            return Err("missing provider error did not use Configuration stage".into());
        }
        Ok(())
    }
    #[cfg(feature = "ws-client")]
    match open_net::OpenNet::new() {
        Err(error) => check_error(error)?,
        Ok(_) => return Err("WS engine was built without a provider".into()),
    }
    #[cfg(feature = "http-client")]
    {
        use open_net::api::http::{HttpClient, HttpClientConfig};
        match HttpClient::new(
            HttpClientConfig::new("https://localhost")?,
            "missing-provider",
        ) {
            Err(error) => check_error(error)?,
            Ok(_) => return Err("HTTP client was built without a provider".into()),
        }
    }
    check(
        CryptoProvider::get_default().is_none(),
        "client construction installed a process provider",
    )?;
    Ok(())
}

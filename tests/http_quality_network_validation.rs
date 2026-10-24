#![cfg(all(feature = "http-client", feature = "quality-doctor"))]

//! Opt-in requests through the host's existing network configuration. This
//! harness never changes interfaces, routes, DNS or system proxy settings.

use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpProxyConfig, HttpRequest, HttpTimeouts, HttpTlsConfig,
};
use open_net::api::quality::{QualityDoctorConfig, QualityLookup, QualityQuery};
use open_net::api::retry::RetrySetting;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
#[ignore = "requires an explicit controlled/public validation URL; uses the existing host network"]
async fn existing_network_diagnostic_modes() -> TestResult {
    let target = std::env::var("OPEN_NET_QUALITY_VALIDATION_URL")?;
    let parsed = reqwest::Url::parse(&target)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(
            "use a public/controlled HTTP(S) URL without credentials, query or fragment".into(),
        );
    }
    let proxy = std::env::var("OPEN_NET_QUALITY_VALIDATION_PROXY").ok();
    let modes = ["off", "unactivated", "basic", "detailed"];
    // Rotate modes in the second round. Each new client makes a first request
    // and a subsequent request; this alone does not prove connection reuse.
    for round in 0..2 {
        for position in 0..modes.len() {
            let mode = modes[(position + round) % modes.len()];
            let quality = match mode {
                "off" => QualityDoctorConfig::default(),
                "basic" => QualityDoctorConfig::basic(),
                _ => QualityDoctorConfig::detailed(),
            };
            let tls = HttpTlsConfig::default()
                .with_crypto_provider(Arc::new(rustls_openssl::default_provider()));
            let mut config = HttpClientConfig::new(target.clone())?
                .with_tls(tls)
                .with_standalone_quality_doctor(quality);
            if let Some(proxy) = &proxy {
                // Only this test client opts into the existing proxy. The SDK
                // defaults and the host's network configuration stay intact.
                config = config.with_proxy(HttpProxyConfig::http(proxy.clone())?);
            }
            let client = HttpClient::new(config, "quality-network-validation")?;
            let doctor = if matches!(mode, "basic" | "detailed") {
                client.quality_doctor()
            } else {
                None
            };
            for request_index in 0..2 {
                let request = HttpRequest::builder()
                    .path(parsed.path())
                    .retry_setting(RetrySetting::Disabled)
                    .timeouts(HttpTimeouts::new().with_total_timeout(Duration::from_secs(12)))
                    .build()?;
                let started = Instant::now();
                let result = client.request(request).await;
                let elapsed_us = started.elapsed().as_micros();
                match result {
                    Ok(response) => {
                        let version = doctor.as_ref().and_then(|doctor| {
                            let reference = response.quality_ref()?;
                            match doctor.lookup(&reference) {
                                QualityLookup::Available(report) => report.http_version(),
                                _ => None,
                            }
                        });
                        println!(
                            "QUALITY_NETWORK {}",
                            serde_json::json!({
                                "round": round, "mode": mode, "request_index": request_index,
                                "explicit_proxy": proxy.is_some(), "elapsed_us": elapsed_us,
                                "status": response.status.as_u16(), "bytes": response.body.len(),
                                "attempts": response.attempts, "observed_http_version": version,
                            })
                        );
                        if !response.status.is_success() || response.attempts != 1 {
                            client.shutdown().await?;
                            return Err(
                                "validation target must return success without a retry".into()
                            );
                        }
                    }
                    Err(error) => {
                        // Do not log arbitrary source text or target credentials.
                        println!(
                            "QUALITY_NETWORK {}",
                            serde_json::json!({
                                "round": round, "mode": mode, "request_index": request_index,
                                "explicit_proxy": proxy.is_some(), "elapsed_us": elapsed_us,
                                "error_kind": format!("{:?}", error.kind()),
                                "timeout_phase": format!("{:?}", error.timeout_phase()),
                            })
                        );
                        client.shutdown().await?;
                        return Err("network validation failed; inspect safe result fields".into());
                    }
                }
                if let Some(doctor) = &doctor {
                    let snapshot = doctor.snapshot(QualityQuery::default())?;
                    if snapshot.counters().ingress_dropped() != 0 {
                        return Err(
                            "diagnostic ingress loss invalidates this small network sample".into(),
                        );
                    }
                }
            }
            client.shutdown().await?;
        }
    }
    Ok(())
}

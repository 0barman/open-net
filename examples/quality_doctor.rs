//! Inspect one request to a local server using lazy, passive diagnostics.
//!
//! Run a server you control, then:
//! `cargo run --example quality_doctor --no-default-features --features http-client,quality-doctor -- http://127.0.0.1:8080`

use open_net::api::http::{HttpClient, HttpClientConfig, HttpTlsConfig};
use open_net::quality::{QualityDoctorConfig, QualityQuery};
use std::sync::Arc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_owned());
    // The application chooses its crypto provider, including for plain HTTP.
    let tls =
        HttpTlsConfig::default().with_crypto_provider(Arc::new(rustls_openssl::default_provider()));
    let config = HttpClientConfig::new(base_url)?
        .with_tls(tls)
        .with_standalone_quality_doctor(QualityDoctorConfig::detailed());
    let client = HttpClient::new(config, "quality-example")?;

    // Initialize diagnostic storage explicitly before the measured request.
    // Configuring Detailed alone does not initialize it on the request path.
    let doctor = client.quality_doctor().ok_or("diagnostics are disabled")?;
    let result = client.get("/").await;
    let reference = match &result {
        Ok(response) => response.quality_ref(),
        Err(error) => error.quality_ref(),
    };
    if let Some(reference) = reference {
        println!("{:?}", doctor.lookup(&reference));
    }
    // Shutdown does not wait for subscriptions to consume diagnostics.
    client.shutdown().await?;
    println!("{}", doctor.snapshot(QualityQuery::default())?.to_json()?);
    result.map(|_| ()).map_err(Into::into)
}

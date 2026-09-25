use open_net::OpenNetConfig;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn defaults_are_observable_and_valid() -> TestResult {
    let config = OpenNetConfig::default();
    config.validate()?;
    if config.runtime_worker_threads().is_some()
        || config.async_queue_capacity() != 128
        || config.sync_queue_capacity() != 128
    {
        return Err("default runtime configuration changed".into());
    }
    Ok(())
}

#[test]
fn getters_report_each_configured_value() -> TestResult {
    let config = OpenNetConfig::default()
        .with_runtime_worker_threads(1)
        .with_async_queue_capacity(3)
        .with_sync_queue_capacity(7);
    config.validate()?;
    if config.runtime_worker_threads() != Some(1)
        || config.async_queue_capacity() != 3
        || config.sync_queue_capacity() != 7
    {
        return Err("configuration getter did not reflect builder input".into());
    }
    Ok(())
}

#[test]
fn rejects_invalid_values_before_creating_resources() -> TestResult {
    for config in [
        OpenNetConfig::default().with_runtime_worker_threads(0),
        OpenNetConfig::default().with_runtime_worker_threads(257),
        OpenNetConfig::default().with_async_queue_capacity(0),
        OpenNetConfig::default().with_sync_queue_capacity(0),
        OpenNetConfig::default().with_async_queue_capacity(usize::MAX),
        OpenNetConfig::default().with_sync_queue_capacity(usize::MAX),
    ] {
        if !matches!(config.validate(), Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::InvalidConfig))
        {
            return Err("invalid configuration was accepted".into());
        }
    }
    Ok(())
}

#[test]
fn accepts_supported_boundary_values() -> TestResult {
    OpenNetConfig::default()
        .with_runtime_worker_threads(256)
        .with_async_queue_capacity(tokio::sync::Semaphore::MAX_PERMITS)
        .with_sync_queue_capacity(tokio::sync::Semaphore::MAX_PERMITS)
        .validate()?;
    Ok(())
}

#[cfg(feature = "ws-client")]
#[test]
fn configured_network_is_observable() -> TestResult {
    use open_net::network::{NetworkConfig, NetworkStatusPolicy};

    let config = OpenNetConfig::default().with_network_config(
        NetworkConfig::default()
            .with_network_status_policy(NetworkStatusPolicy::PauseOnUnavailable),
    );
    config.validate()?;
    if config.network_config().network_status_policy() != NetworkStatusPolicy::PauseOnUnavailable {
        return Err("configured network policy is not observable".into());
    }
    Ok(())
}

#[test]
fn configuration_failures_identify_the_invalid_field() -> TestResult {
    for (config, field) in [
        (
            OpenNetConfig::default().with_runtime_worker_threads(0),
            "runtime_worker_threads",
        ),
        (
            OpenNetConfig::default().with_runtime_worker_threads(257),
            "runtime_worker_threads",
        ),
        (
            OpenNetConfig::default().with_async_queue_capacity(0),
            "async_queue_capacity",
        ),
        (
            OpenNetConfig::default().with_sync_queue_capacity(usize::MAX),
            "sync_queue_capacity",
        ),
    ] {
        let error = config
            .validate()
            .err()
            .ok_or("invalid field was accepted")?;
        let detail = error
            .config_error()
            .ok_or("configuration error has no field diagnostic")?;
        if detail.field() != field || detail.reason().is_empty() {
            return Err(format!("configuration error did not identify {field}").into());
        }
    }
    Ok(())
}

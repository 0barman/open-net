use crate::{OpenNet, OpenNetConfig};
use tokio::sync::Semaphore;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[track_caller]
fn check_capacities(engine: &OpenNet, async_capacity: usize, sync_capacity: usize) -> TestResult {
    let common = &engine.inner.common_engine;
    if common.async_tx.max_capacity() != async_capacity
        || common.sync_tx.max_capacity() != sync_capacity
    {
        let location = std::panic::Location::caller();
        return Err(format!(
            "{location}: engine task channels did not receive the configured capacities"
        )
        .into());
    }
    Ok(())
}

#[test]
fn engine_channel_defaults_apply_to_both_default_constructors() -> TestResult {
    check_capacities(&OpenNet::new()?, 128, 128)?;
    check_capacities(
        &OpenNet::new_with_config(OpenNetConfig::default())?,
        128,
        128,
    )
}

#[test]
fn engine_channel_config_reaches_each_queue_and_accepts_boundaries() -> TestResult {
    for (async_capacity, sync_capacity) in
        [(1, 7), (Semaphore::MAX_PERMITS, Semaphore::MAX_PERMITS)]
    {
        let engine = OpenNet::new_with_config(
            OpenNetConfig::default()
                .with_async_queue_capacity(async_capacity)
                .with_sync_queue_capacity(sync_capacity),
        )?;
        check_capacities(&engine, async_capacity, sync_capacity)?;
    }
    Ok(())
}

#[test]
fn engine_channel_config_rejects_invalid_capacities_without_panicking() -> TestResult {
    for capacity in [0, Semaphore::MAX_PERMITS + 1, usize::MAX] {
        for config in [
            OpenNetConfig::default().with_async_queue_capacity(capacity),
            OpenNetConfig::default().with_sync_queue_capacity(capacity),
        ] {
            if !matches!(OpenNet::new_with_config(config), Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), crate::error::ErrorKind::InvalidConfig))
            {
                return Err("invalid engine channel capacity must return ConfigError".into());
            }
        }
    }
    Ok(())
}

#[cfg(feature = "ws-client")]
#[test]
fn engine_channel_config_preserves_network_policy_and_legacy_constructor() -> TestResult {
    use crate::{NetworkConfig, NetworkStatusPolicy};

    let network = NetworkConfig::default()
        .with_network_status_policy(NetworkStatusPolicy::PauseOnUnavailable);
    let configured = OpenNet::new_with_config(
        OpenNetConfig::default()
            .with_async_queue_capacity(3)
            .with_sync_queue_capacity(5)
            .with_network_config(network.clone()),
    )?;
    let legacy = OpenNet::new_with_network_config(network)?;
    for engine in [&configured, &legacy] {
        if engine.inner.network.network_status_policy() != NetworkStatusPolicy::PauseOnUnavailable {
            return Err("engine constructor did not preserve its default network policy".into());
        }
    }
    check_capacities(&configured, 3, 5)?;
    check_capacities(&legacy, 128, 128)
}

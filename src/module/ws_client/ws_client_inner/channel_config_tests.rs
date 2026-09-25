use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};

#[test]
fn channel_config_defaults_create_128_slot_channels() -> TestResult {
    let (inner, worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())
            .map_err(|error| test_error(format!("default client config failed: {error:?}")))?;

    check_eq!(inner.command_tx.max_capacity(), 128)?;
    check_eq!(worker.io_event_tx.max_capacity(), 128)?;
    Ok(())
}

#[test]
fn channel_config_applies_independent_backpressure_limits() -> TestResult {
    let (inner, worker) = crate::module::ws_client::test_support::new_inner({
        let mut config = WebSocketClientConfig::default();
        config.queues.commands = 1;
        config.queues.io_events = 3;
        config
    })
    .map_err(|error| test_error(format!("custom client config failed: {error:?}")))?;

    let command_permit = inner.command_tx.try_reserve()?;
    check!(matches!(
        inner.command_tx.try_reserve(),
        Err(mpsc::error::TrySendError::Full(()))
    ))?;

    let event_permits: Vec<_> = (0..3)
        .map(|_| worker.io_event_tx.try_reserve())
        .collect::<Result<_, _>>()?;
    check!(matches!(
        worker.io_event_tx.try_reserve(),
        Err(mpsc::error::TrySendError::Full(()))
    ))?;

    drop(command_permit);
    check!(inner.command_tx.try_reserve().is_ok())?;
    drop(event_permits);
    check_eq!(worker.io_event_tx.capacity(), 3)?;
    Ok(())
}

#[test]
fn channel_config_rejects_zero_and_oversized_capacities_without_panicking() -> TestResult {
    for invalid in [0, Semaphore::MAX_PERMITS + 1, usize::MAX] {
        for config in [
            {
                let mut config = WebSocketClientConfig::default();
                config.queues.commands = invalid;
                config
            },
            {
                let mut config = WebSocketClientConfig::default();
                config.queues.io_events = invalid;
                config
            },
        ] {
            check!(matches!(
                crate::module::ws_client::test_support::new_inner(config),
                Err(error) if matches!(error.kind(), crate::error::ErrorKind::InvalidConfig)))?;
        }
    }
    Ok(())
}

#[test]
fn channel_config_accepts_largest_supported_capacity() -> TestResult {
    let (inner, worker) = crate::module::ws_client::test_support::new_inner({
        let mut config = WebSocketClientConfig::default();
        config.queues.commands = Semaphore::MAX_PERMITS;
        config.queues.io_events = Semaphore::MAX_PERMITS;
        config
    })
    .map_err(|error| {
        test_error(format!(
            "supported channel capacity was rejected: {error:?}"
        ))
    })?;

    check_eq!(inner.command_tx.max_capacity(), Semaphore::MAX_PERMITS)?;
    check_eq!(worker.io_event_tx.max_capacity(), Semaphore::MAX_PERMITS)?;
    Ok(())
}

use crate::{OpenNet, OpenNetConfig};
use std::process::Command;
use std::sync::{mpsc, Arc};
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(5);

fn check_workers(net: &OpenNet, expected: usize) -> TestResult {
    let actual = net
        .inner
        .common_engine
        .runtime_handle()
        .metrics()
        .num_workers();
    if actual != expected {
        return Err(format!("expected {expected} runtime workers, observed {actual}").into());
    }
    Ok(())
}

#[test]
fn runtime_worker_config_is_per_engine_and_preserves_queues() -> TestResult {
    let config = OpenNetConfig::default()
        .with_runtime_worker_threads(1)
        .with_async_queue_capacity(3)
        .with_sync_queue_capacity(5);
    let first = OpenNet::new_with_config(config.clone())?;
    let second = OpenNet::new_with_config(config.with_runtime_worker_threads(2))?;
    check_workers(&first, 1)?;
    check_workers(&second, 2)?;
    for net in [&first, &second] {
        if net.inner.common_engine.async_tx.max_capacity() != 3
            || net.inner.common_engine.sync_tx.max_capacity() != 5
        {
            return Err("runtime configuration changed queue capacities".into());
        }
    }
    Ok(())
}

#[test]
fn runtime_worker_config_rejects_invalid_values_before_creation() -> TestResult {
    for workers in [0, 257, usize::MAX] {
        let result =
            OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(workers));
        if !matches!(result, Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), crate::error::ErrorKind::InvalidConfig))
        {
            return Err(format!("invalid worker count {workers} must return ConfigError").into());
        }
    }
    // Validate the supported upper boundary without starting 256 OS threads.
    OpenNetConfig::default()
        .with_runtime_worker_threads(256)
        .validate()?;
    Ok(())
}

#[test]
fn runtime_worker_config_defaults_and_environment_override() -> TestResult {
    // Set the environment only in a child; parallel tests keep their own runtime defaults.
    let output = Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "inner::net_impl::runtime_config_tests::runtime_worker_config_child",
            "--nocapture",
        ])
        .env("OPEN_NET_RUNTIME_CONFIG_TEST_CHILD", "1")
        .env("TOKIO_WORKER_THREADS", "3")
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "runtime configuration child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )
        .into());
    }
    if !String::from_utf8_lossy(&output.stdout).contains("1 passed") {
        return Err("runtime configuration child test did not execute".into());
    }
    Ok(())
}

#[test]
fn runtime_worker_config_child() -> TestResult {
    if std::env::var_os("OPEN_NET_RUNTIME_CONFIG_TEST_CHILD").is_none() {
        return Ok(());
    }
    check_workers(&OpenNet::new()?, 3)?;
    check_workers(&OpenNet::new_with_config(OpenNetConfig::default())?, 3)?;
    check_workers(
        &OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(1))?,
        1,
    )?;
    #[cfg(feature = "ws-client")]
    check_workers(
        &OpenNet::new_with_network_config(crate::NetworkConfig::default())?,
        3,
    )?;
    Ok(())
}

#[test]
fn runtime_worker_config_single_worker_supports_reentrant_invoke_and_timers() -> TestResult {
    let net = OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(1))?;
    let engine = Arc::clone(&net.inner.common_engine);
    let engine_id = engine.runtime_handle().id();
    let posted_engine = Arc::clone(&engine);
    let (sender, receiver) = mpsc::channel();
    engine.post(async move {
        let result = posted_engine.invoke(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            tokio::runtime::Handle::try_current().map(|handle| handle.id())
        });
        if sender.send(result).is_err() {
            crate::log_e!(crate::LogType::Common; "runtime_config_test", "error", "receiver_closed");
        }
    });
    let observed = receiver
        .recv_timeout(WAIT)?
        .map_err(|error| format!("reentrant invoke failed: {error:?}"))??;
    if observed != engine_id {
        return Err("single-worker invoke escaped the engine runtime".into());
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_worker_config_single_worker_supports_foreign_runtime_and_drop() -> TestResult {
    let net = OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(1))?;
    let engine = &net.inner.common_engine;
    let engine_id = engine.runtime_handle().id();
    if engine_id == tokio::runtime::Handle::try_current()?.id() {
        return Err("test requires a distinct host runtime".into());
    }
    let result = engine
        .invoke(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            tokio::runtime::Handle::try_current().map(|handle| handle.id())
        })
        .map_err(|error| format!("foreign-runtime invoke failed: {error:?}"))??;
    if result != engine_id {
        return Err("configured engine did not execute its own timer task".into());
    }
    drop(net);
    // The caller's current-thread runtime must remain live after engine destruction.
    tokio::time::timeout(WAIT, tokio::time::sleep(Duration::from_millis(1))).await?;
    Ok(())
}

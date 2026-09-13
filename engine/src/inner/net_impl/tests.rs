use super::*;
use crate::api::network_config::{NetworkConfig, RootCertificateMode, TlsConfig};
use crate::api::open_net::OpenNet;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::future::{poll_fn, Future};
use std::task::Poll;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

const FACTORY_TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn empty_destroy_guard_reports_error_without_panicking() -> Result<(), Box<dyn std::error::Error>> {
    let guard = DestroyClientGuard {
        clients: Arc::new(Mutex::new(HashMap::new())),
        thread_name: "empty-guard".to_string(),
        entry: None,
        cleanup_handed_off: false,
    };
    match guard.client() {
        Err(NetError::InternalError) => Ok(()),
        _ => Err("an empty destroy guard must report InternalError".into()),
    }
}

/// The common engine awaits posted tasks in order. This fence also ensures that
/// completed creation tasks have released temporary configuration references.
async fn factory_queue_fence(engine: &OpenNet) -> TestResult {
    let (completed, completion) = oneshot::channel();
    engine.inner.common_engine.post(async move {
        let _ = completed.send(());
    });
    tokio::time::timeout(FACTORY_TEST_TIMEOUT, completion).await??;
    Ok(())
}

/// Run even after a fallible check fails so that successful earlier creations do
/// not leave their worker threads running. Callers release any queue gate first.
async fn finish_factory_case(engine: &OpenNet, result: TestResult) -> TestResult {
    let cleanup: TestResult = async {
        factory_queue_fence(engine).await?;
        let names = engine
            .inner
            .clients
            .lock()
            .map_err(|_| test_error("factory client registry was poisoned"))?
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut failures = Vec::new();
        for name in names {
            match tokio::time::timeout(FACTORY_TEST_TIMEOUT, engine.destroy_ws_client(&name)).await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(format!("{name}: {error}")),
                Err(error) => failures.push(format!("{name}: {error}")),
            }
        }
        check!(failures.is_empty(), "factory cleanup failed: {failures:?}")
    }
    .await;
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(test_error(format!("{error}; cleanup: {cleanup}"))),
    }
}

#[tokio::test]
async fn client_network_override_reuses_engine_arc_only_for_inherited_defaults() -> TestResult {
    let engine = OpenNet::new()?;
    let result: TestResult = async {
        check_eq!(Arc::strong_count(&engine.inner.network), 1)?;

        engine.create_ws_client("inherited-ws-defaults").await?;
        factory_queue_fence(&engine).await?;
        check_eq!(Arc::strong_count(&engine.inner.network), 2)?;

        engine
            .create_ws_client_with_config("inherited-ws-config", WebSocketClientConfig::default())
            .await?;
        factory_queue_fence(&engine).await?;
        check_eq!(Arc::strong_count(&engine.inner.network), 3)?;

        engine
            .create_ws_client_with_network_config(
                "explicit-network-defaults",
                WebSocketClientConfig::default(),
                NetworkConfig::default(),
            )
            .await?;
        factory_queue_fence(&engine).await?;
        check_eq!(
            Arc::strong_count(&engine.inner.network),
            3,
            "an explicit default network policy must have its own compiled snapshot"
        )?;

        engine.destroy_ws_client("inherited-ws-defaults").await?;
        check_eq!(Arc::strong_count(&engine.inner.network), 2)?;
        engine.destroy_ws_client("inherited-ws-config").await?;
        check_eq!(Arc::strong_count(&engine.inner.network), 1)?;
        engine
            .destroy_ws_client("explicit-network-defaults")
            .await?;
        check_eq!(Arc::strong_count(&engine.inner.network), 1)
    }
    .await;
    finish_factory_case(&engine, result).await
}

fn invalid_empty_trust_override() -> NetworkConfig {
    // Public PEM builders reject empty bundles already. Construct this state
    // internally to exercise compilation failure after reserving the client name.
    NetworkConfig::default().with_tls(TlsConfig {
        root_mode: RootCertificateMode::Only,
        ..TlsConfig::default()
    })
}

#[tokio::test]
async fn client_network_override_compile_failure_releases_name_and_preserves_duplicate_priority(
) -> TestResult {
    let engine = OpenNet::new()?;
    let result: TestResult = async {
        let invalid = engine
            .create_ws_client_with_network_config(
                "reusable-override-name",
                WebSocketClientConfig::default(),
                invalid_empty_trust_override(),
            )
            .await;
        check!(
            matches!(invalid, Err(NetError::ConfigError)),
            "empty explicit trust must fail compilation"
        )?;
        factory_queue_fence(&engine).await?;
        check!(matches!(
            engine.get_ws_client("reusable-override-name"),
            Err(NetError::ClientNotFound)
        ))?;
        check!(
            !engine
                .inner
                .clients
                .lock()
                .map_err(|_| test_error("factory client registry was poisoned"))?
                .contains_key("reusable-override-name"),
            "failed compilation must remove the Creating reservation"
        )?;
        check_eq!(Arc::strong_count(&engine.inner.network), 1)?;

        engine
            .create_ws_client_with_network_config(
                "reusable-override-name",
                WebSocketClientConfig::default(),
                NetworkConfig::default(),
            )
            .await?;
        let duplicate = engine
            .create_ws_client_with_network_config(
                "reusable-override-name",
                WebSocketClientConfig::default(),
                invalid_empty_trust_override(),
            )
            .await;
        check!(
            matches!(duplicate, Err(NetError::ClientAlreadyExists)),
            "duplicate-name admission must precede override compilation"
        )?;
        engine.get_ws_client("reusable-override-name")?;
        Ok(())
    }
    .await;
    finish_factory_case(&engine, result).await
}

#[tokio::test]
async fn client_network_override_submitted_creation_survives_waiter_drop() -> TestResult {
    let engine = OpenNet::new()?;
    let release = CancellationToken::new();
    let release_on_drop = release.clone().drop_guard();
    let gate = release.clone();
    let (entered, entry) = oneshot::channel();
    engine.inner.common_engine.post(async move {
        if entered.send(()).is_ok() {
            gate.cancelled().await;
        }
    });

    let result: TestResult = async {
        tokio::time::timeout(FACTORY_TEST_TIMEOUT, entry).await??;
        let mut creation = Box::pin(engine.create_ws_client_with_network_config(
            "dropped-override-waiter",
            WebSocketClientConfig::default(),
            NetworkConfig::default(),
        ));
        let first_poll = poll_fn(|context| Poll::Ready(creation.as_mut().poll(context))).await;
        drop(creation);
        check!(
            first_poll.is_pending(),
            "the gate must hold creation after submission and before worker readiness"
        )?;
        check!(matches!(
            engine.get_ws_client("dropped-override-waiter"),
            Err(NetError::ConnectionClosing)
        ))?;
        let duplicate = tokio::time::timeout(
            FACTORY_TEST_TIMEOUT,
            engine.create_ws_client("dropped-override-waiter"),
        )
        .await?;
        check!(
            matches!(duplicate, Err(NetError::ClientAlreadyExists)),
            "dropping the waiter must retain the submitted Creating reservation"
        )?;

        release.cancel();
        factory_queue_fence(&engine).await?;
        engine.get_ws_client("dropped-override-waiter")?;
        engine.destroy_ws_client("dropped-override-waiter").await?;
        check!(matches!(
            engine.get_ws_client("dropped-override-waiter"),
            Err(NetError::ClientNotFound)
        ))
    }
    .await;
    // Release the gate before cleanup even when an earlier check returned an error.
    drop(release_on_drop);
    finish_factory_case(&engine, result).await
}

#[tokio::test]
async fn network_status_policy_engine_default_and_client_override_are_independent() -> TestResult {
    use crate::api::network_config::NetworkStatusPolicy;

    let policy = NetworkConfig::default()
        .with_network_status_policy(NetworkStatusPolicy::PauseOnUnavailable);
    let engine = OpenNet::new_with_network_config(policy.clone())?;
    let result: TestResult = async {
        let inherited = tokio::time::timeout(
            FACTORY_TEST_TIMEOUT,
            engine.create_ws_client("status-inherited"),
        )
        .await??;
        let inherited_configured = tokio::time::timeout(
            FACTORY_TEST_TIMEOUT,
            engine.create_ws_client_with_config(
                "status-inherited-configured",
                WebSocketClientConfig::default(),
            ),
        )
        .await??;
        let explicit_default = tokio::time::timeout(
            FACTORY_TEST_TIMEOUT,
            engine.create_ws_client_with_network_config(
                "status-explicit-default",
                WebSocketClientConfig::default(),
                NetworkConfig::default(),
            ),
        )
        .await??;
        let explicit_ignore = tokio::time::timeout(
            FACTORY_TEST_TIMEOUT,
            engine.create_ws_client_with_network_config(
                "status-explicit-ignore",
                WebSocketClientConfig::default(),
                policy.with_network_status_policy(NetworkStatusPolicy::Ignore),
            ),
        )
        .await??;
        check_eq!(
            engine.inner.network.network_status_policy(),
            NetworkStatusPolicy::PauseOnUnavailable
        )?;
        for client in [&inherited, &inherited_configured] {
            check_eq!(
                client.inner.network_status_policy_for_test(),
                NetworkStatusPolicy::PauseOnUnavailable
            )?;
            check_eq!(client.connection_status(), crate::ConnectionStatus::Idle)?;
        }
        for client in [&explicit_default, &explicit_ignore] {
            check_eq!(
                client.inner.network_status_policy_for_test(),
                NetworkStatusPolicy::Ignore
            )?;
            check_eq!(client.connection_status(), crate::ConnectionStatus::Idle)?;
        }
        engine.destroy_ws_client("status-inherited").await?;
        check_eq!(
            inherited_configured.inner.network_status_policy_for_test(),
            NetworkStatusPolicy::PauseOnUnavailable
        )?;
        check_eq!(
            explicit_default.inner.network_status_policy_for_test(),
            NetworkStatusPolicy::Ignore
        )?;
        Ok(())
    }
    .await;
    finish_factory_case(&engine, result).await
}

#[tokio::test]
async fn network_status_policy_client_opt_in_does_not_change_default_siblings() -> TestResult {
    use crate::api::network_config::NetworkStatusPolicy;

    let engine = OpenNet::new()?;
    let result: TestResult = async {
        let opted_in = tokio::time::timeout(
            FACTORY_TEST_TIMEOUT,
            engine.create_ws_client_with_network_config(
                "status-opted-in",
                WebSocketClientConfig::default(),
                NetworkConfig::default()
                    .with_network_status_policy(NetworkStatusPolicy::PauseOnUnavailable),
            ),
        )
        .await??;
        let legacy = tokio::time::timeout(
            FACTORY_TEST_TIMEOUT,
            engine.create_ws_client("status-legacy"),
        )
        .await??;
        check_eq!(
            opted_in.inner.network_status_policy_for_test(),
            NetworkStatusPolicy::PauseOnUnavailable
        )?;
        check_eq!(
            legacy.inner.network_status_policy_for_test(),
            NetworkStatusPolicy::Ignore
        )?;
        check_eq!(
            engine.inner.network.network_status_policy(),
            NetworkStatusPolicy::Ignore
        )?;
        engine.destroy_ws_client("status-opted-in").await?;
        check_eq!(legacy.connection_status(), crate::ConnectionStatus::Idle)?;
        Ok(())
    }
    .await;
    finish_factory_case(&engine, result).await
}

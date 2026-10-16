use super::*;
use crate::api::network_config::{NetworkConfig, RootCertificateMode, TlsConfig};
use crate::api::open_net::OpenNet;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::future::{poll_fn, Future};
use std::task::Poll;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

const FACTORY_TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn engine() -> TestResult<OpenNet> {
    Ok(OpenNet::new_with_config(
        OpenNetConfig::default().with_network_config(
            NetworkConfig::default()
                .with_network_status_policy(crate::network::NetworkStatusPolicy::Ignore),
        ),
    )?)
}

#[test]
fn empty_destroy_guard_reports_error_without_panicking() -> Result<(), Box<dyn std::error::Error>> {
    let guard = DestroyClientGuard {
        engine: Arc::new(CommonEngine::new(4, 4)?),
        clients: Arc::new(Mutex::new(HashMap::new())),
        clients_changed: Arc::new(tokio::sync::Notify::new()),
        thread_name: "empty-guard".to_string(),
        entry: None,
    };
    match guard.client() {
        Err(error) if error.kind() == crate::error::ErrorKind::Internal => Ok(()),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_or_create_ws_client_reuses_one_named_client_for_concurrent_callers() -> TestResult {
    let engine = engine()?;
    let result: TestResult = async {
        let mut first_config = WebSocketClientConfig::default();
        first_config.queues.normal.max_items = 257;
        let second_config = WebSocketClientConfig::default();
        let (first, second) = tokio::join!(
            engine.get_or_create_ws_client_with_config("shared-get-or-create", first_config),
            engine.get_or_create_ws_client_with_config("shared-get-or-create", second_config),
        );
        let first = first?;
        let second = second?;
        check_eq!(first.id(), second.id())?;
        check_eq!(first.config().queues.normal.max_items, 257)?;
        engine
            .destroy_ws_client_if_exists("shared-get-or-create")
            .await?;
        engine
            .destroy_ws_client_if_exists("shared-get-or-create")
            .await?;
        check!(
            matches!(
                engine.get_ws_client("shared-get-or-create"),
                Err(ref error) if error.kind() == crate::error::ErrorKind::ClientNotFound
            ),
            "idempotent cleanup must release the client name"
        )?;
        Ok(())
    }
    .await;
    finish_factory_case(&engine, result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn destroy_ws_client_if_exists_waits_for_creation_and_is_repeatable() -> TestResult {
    let engine = engine()?;
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
        let mut creating = Box::pin(engine.get_or_create_ws_client_with_config(
            "destroy-during-create",
            WebSocketClientConfig::default(),
        ));
        let first_poll = poll_fn(|context| Poll::Ready(creating.as_mut().poll(context))).await;
        check!(
            first_poll.is_pending(),
            "creation must be held by the queue gate"
        )?;

        let mut destroying = Box::pin(engine.destroy_ws_client_if_exists("destroy-during-create"));
        let destroy_poll = poll_fn(|context| Poll::Ready(destroying.as_mut().poll(context))).await;
        check!(
            destroy_poll.is_pending(),
            "cleanup must wait for the in-flight creation"
        )?;

        release.cancel();
        creating.await?;
        destroying.await?;
        engine
            .destroy_ws_client_if_exists("destroy-during-create")
            .await?;
        Ok(())
    }
    .await;
    drop(release_on_drop);
    finish_factory_case(&engine, result).await
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
        root_mode: RootCertificateMode::Replace,
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
            matches!(invalid, Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), crate::error::ErrorKind::InvalidConfig)),
            "empty explicit trust must fail compilation"
        )?;
        factory_queue_fence(&engine).await?;
        check!(matches!(
            engine.get_ws_client("reusable-override-name"),
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), crate::error::ErrorKind::ClientNotFound)))?;
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
            matches!(duplicate, Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), crate::error::ErrorKind::ClientAlreadyExists)),
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
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), crate::error::ErrorKind::ConnectionClosing)))?;
        let duplicate = tokio::time::timeout(
            FACTORY_TEST_TIMEOUT,
            engine.create_ws_client("dropped-override-waiter"),
        )
        .await?;
        check!(
            matches!(duplicate, Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), crate::error::ErrorKind::ClientAlreadyExists)),
            "dropping the waiter must retain the submitted Creating reservation"
        )?;

        release.cancel();
        factory_queue_fence(&engine).await?;
        engine.get_ws_client("dropped-override-waiter")?;
        engine.destroy_ws_client("dropped-override-waiter").await?;
        check!(matches!(
            engine.get_ws_client("dropped-override-waiter"),
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), crate::error::ErrorKind::ClientNotFound)))
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
                client.network_config().network_status_policy(),
                NetworkStatusPolicy::PauseOnUnavailable
            )?;
            check!(!client.is_shutdown())?;
        }
        for client in [&explicit_default, &explicit_ignore] {
            check_eq!(
                client.network_config().network_status_policy(),
                NetworkStatusPolicy::Ignore
            )?;
            check!(!client.is_shutdown())?;
        }
        engine.destroy_ws_client("status-inherited").await?;
        check_eq!(
            inherited_configured
                .network_config()
                .network_status_policy(),
            NetworkStatusPolicy::PauseOnUnavailable
        )?;
        check_eq!(
            explicit_default.network_config().network_status_policy(),
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
            opted_in.network_config().network_status_policy(),
            NetworkStatusPolicy::PauseOnUnavailable
        )?;
        check_eq!(
            legacy.network_config().network_status_policy(),
            NetworkStatusPolicy::Ignore
        )?;
        check_eq!(
            engine.inner.network.network_status_policy(),
            NetworkStatusPolicy::Ignore
        )?;
        engine.destroy_ws_client("status-opted-in").await?;
        check!(!legacy.is_shutdown())?;
        Ok(())
    }
    .await;
    finish_factory_case(&engine, result).await
}

#[tokio::test]
async fn shared_network_demand_isolated_between_pause_ignore_and_failed_ws_creation() -> TestResult
{
    use std::sync::atomic::{AtomicUsize, Ordering};
    let policy = NetworkConfig::default()
        .with_network_status_policy(crate::NetworkStatusPolicy::PauseOnUnavailable);
    let engine = OpenNet::new_with_network_config(policy)?;
    let (attempted, mut attempts) = tokio::sync::mpsc::unbounded_channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    engine
        .inner
        .network_status
        .inner_for_test()
        .set_monitor_factory_for_test(Arc::new(move || {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            let (release, pending) = oneshot::channel::<()>();
            let _ = attempted.send(release);
            Box::pin(async move {
                let _ = pending.await;
                Err(NetError::from(crate::error::ErrorKind::Io))
            })
        }))?;
    let result: TestResult = async {
        let a = engine.create_ws_client("shared-pause-a").await?;
        let attempt = tokio::time::timeout(FACTORY_TEST_TIMEOUT, attempts.recv())
            .await?
            .ok_or_else(|| test_error("shared provider did not initialize"))?;
        let b = engine.create_ws_client("shared-pause-b").await?;
        let ignore = engine
            .create_ws_client_with_network_config(
                "shared-ignore",
                WebSocketClientConfig::default(),
                NetworkConfig::default(),
            )
            .await?;
        let _clone = b.clone();
        check_eq!(engine.inner.network_status.active_consumers_for_test()?, 2)?;
        let mut invalid = WebSocketClientConfig::default();
        invalid.queues.commands = 0;
        check!(engine
            .create_ws_client_with_config("shared-invalid", invalid)
            .await
            .is_err())?;
        check_eq!(
            engine.inner.network_status.active_consumers_for_test()?,
            2,
            "failed construction leaked lease"
        )?;
        tokio::time::timeout(FACTORY_TEST_TIMEOUT, a.shutdown()).await??;
        check_eq!(engine.inner.network_status.active_consumers_for_test()?, 1)?;
        check!(
            !attempt.is_closed(),
            "one WS shutdown cancelled another's provider"
        )?;
        check_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "WS clients duplicated provider"
        )?;
        tokio::time::timeout(FACTORY_TEST_TIMEOUT, ignore.shutdown()).await??;
        check_eq!(engine.inner.network_status.active_consumers_for_test()?, 1)?;
        tokio::time::timeout(FACTORY_TEST_TIMEOUT, b.shutdown()).await??;
        check!(
            attempt.is_closed(),
            "final WS shutdown retained provider initialization"
        )?;
        check_eq!(engine.inner.network_status.active_consumers_for_test()?, 0)?;
        Ok(())
    }
    .await;
    finish_factory_case(&engine, result).await
}

#[tokio::test]
async fn accepted_factories_cannot_register_after_the_source_engine_drops() -> TestResult {
    let engine = OpenNet::new_with_network_config(
        NetworkConfig::default()
            .with_network_status_policy(crate::NetworkStatusPolicy::PauseOnUnavailable),
    )?;
    let runtime = engine.inner.common_engine.clone();
    let service = engine.inner.network_status.clone();
    let context = service.context();
    let ws_slots = engine.inner.clients.clone();
    #[cfg(feature = "http-client")]
    let http_slots = engine.inner.http_clients.clone();
    let release = CancellationToken::new();
    let _release_guard = release.clone().drop_guard();
    let waiting = release.clone();
    let (entered, entered_wait) = oneshot::channel();
    runtime.post(async move {
        let _ = entered.send(());
        waiting.cancelled().await;
    });
    tokio::time::timeout(FACTORY_TEST_TIMEOUT, entered_wait).await??;
    // Ignore skips lease admission, so its completed worker must still be
    // rejected when the registry attempts a late Ready commit.
    let mut ws = Box::pin(engine.create_ws_client_with_network_config(
        "late-shared-ws",
        WebSocketClientConfig::default(),
        NetworkConfig::default(),
    ));
    check!(
        poll_fn(|cx| Poll::Ready(ws.as_mut().poll(cx)))
            .await
            .is_pending(),
        "WS creation escaped its queue gate"
    )?;
    drop(ws);
    let mut pause = Box::pin(engine.create_ws_client("late-shared-pause"));
    check!(
        poll_fn(|cx| Poll::Ready(pause.as_mut().poll(cx)))
            .await
            .is_pending(),
        "Pause creation escaped its queue gate"
    )?;
    drop(pause);
    #[cfg(feature = "http-client")]
    {
        let config = crate::api::http::HttpClientConfig::new("http://127.0.0.1:9")?;
        let mut http = Box::pin(engine.create_http_client_with_config("late-shared-http", config));
        check!(
            poll_fn(|cx| Poll::Ready(http.as_mut().poll(cx)))
                .await
                .is_pending(),
            "HTTP creation escaped its queue gate"
        )?;
        drop(http);
    }
    // The marker is accepted before shutdown closes management admission. It
    // confirms that the preceding accepted factories have really retired.
    let (completed, completion) = oneshot::channel();
    runtime.post(async move {
        let _ = completed.send(());
    });
    drop(engine);
    check!(matches!(
        context.snapshot()?.state,
        crate::net_status::MonitorState::Closed
    ))?;
    release.cancel();
    tokio::time::timeout(FACTORY_TEST_TIMEOUT, completion).await??;
    check_eq!(
        service.active_consumers_for_test()?,
        0,
        "late factory retained observation demand"
    )?;
    check!(
        ws_slots.lock().map_err(NetError::from_poison)?.is_empty(),
        "late WS factory revived registry"
    )?;
    #[cfg(feature = "http-client")]
    check!(
        http_slots.lock().map_err(NetError::from_poison)?.is_empty(),
        "late HTTP factory revived registry"
    )?;
    service.shutdown().await?;
    Ok(())
}

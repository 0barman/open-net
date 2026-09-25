//! Network monitoring is part of the base API, including with no default features.

use open_net::error::ErrorKind;
use open_net::net_status::{MonitorState, NetStatusClient, NetworkSnapshot};
use open_net::subscription::{CallbackContext, StateReceiver, Subscription};
use open_net::{BoxError, NetError, OpenNet};

use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

type TestResult<T = ()> = Result<T, BoxError>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

fn check_error<T>(result: Result<T, NetError>, kind: ErrorKind, operation: &str) -> TestResult {
    match result {
        Err(error) if error.kind() == kind => Ok(()),
        Err(error) => Err(std::io::Error::other(format!(
            "{operation}: required {kind:?}, received {error}"
        ))
        .into()),
        Ok(_) => Err(std::io::Error::other(format!(
            "{operation}: required {kind:?}, operation succeeded"
        ))
        .into()),
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(Duration::from_secs(20), future).await?)
}

fn check_no_observation(snapshot: &NetworkSnapshot) -> TestResult {
    check(
        snapshot.reachability.is_none(),
        "inactive reachability must be unknown",
    )?;
    check(
        snapshot.ip_stack.is_none(),
        "inactive IP stack must be unknown",
    )?;
    check(
        snapshot.observed_at.is_none(),
        "inactive snapshot must have no current observation",
    )?;
    check(
        snapshot.network_name.is_none(),
        "inactive network name must be absent",
    )
}

fn check_stopped(client: &NetStatusClient) -> TestResult {
    let snapshot = client.snapshot()?;
    check(
        matches!(snapshot.state, MonitorState::Stopped),
        "client must be stopped",
    )?;
    check_no_observation(&snapshot)
}

fn check_closed(client: &NetStatusClient) -> TestResult {
    let snapshot = client.snapshot()?;
    check(
        matches!(snapshot.state, MonitorState::Closed),
        "client must be permanently closed",
    )?;
    check_no_observation(&snapshot)
}

fn check_running(client: &NetStatusClient) -> TestResult {
    check(
        matches!(client.snapshot()?.state, MonitorState::Running),
        "client must be running",
    )
}

async fn next_snapshot(
    receiver: &mut StateReceiver<NetworkSnapshot>,
) -> TestResult<NetworkSnapshot> {
    bounded(receiver.recv()).await??.ok_or_else(|| {
        std::io::Error::other("state subscription ended before its expected snapshot").into()
    })
}

async fn check_capture_released(marker: &Weak<()>) -> TestResult {
    bounded(async {
        while marker.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
}

fn subscribe_empty(client: &NetStatusClient) -> TestResult<Subscription> {
    Ok(client.on_change(|_, _| {})?)
}

fn subscribe_client_capture(client: &NetStatusClient) -> TestResult<(Subscription, Weak<()>)> {
    let marker = Arc::new(());
    let weak = Arc::downgrade(&marker);
    let captured_client = client.clone();
    let subscription = client.on_change(move |_, _| {
        let _ = captured_client.snapshot();
        let _ = &marker;
    })?;
    Ok((subscription, weak))
}

#[test]
fn public_client_and_observation_owners_can_cross_threads() -> TestResult {
    fn requires_shared<T: Clone + Send + Sync>() {}
    fn requires_owned<T: Send + Sync>() {}
    requires_shared::<NetStatusClient>();
    requires_shared::<CallbackContext>();
    requires_owned::<Subscription>();
    requires_owned::<StateReceiver<NetworkSnapshot>>();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_validates_names_and_creates_stopped_clients() -> TestResult {
    let net = OpenNet::new()?;
    for empty_name in ["", " \t\n "] {
        check_error(
            bounded(net.create_net_status_client(empty_name)).await?,
            ErrorKind::InvalidInput,
            "create with empty name",
        )?;
        check_error(
            net.get_net_status_client(empty_name),
            ErrorKind::InvalidInput,
            "get with empty name",
        )?;
        check_error(
            bounded(net.destroy_net_status_client(empty_name)).await?,
            ErrorKind::InvalidInput,
            "destroy with empty name",
        )?;
    }
    check_error(
        net.get_net_status_client("missing"),
        ErrorKind::ClientNotFound,
        "get missing client",
    )?;
    check_error(
        bounded(net.destroy_net_status_client("missing")).await?,
        ErrorKind::ClientNotFound,
        "destroy missing client",
    )?;
    let client = bounded(net.create_net_status_client("  status \t")).await??;
    check_stopped(&client)?;
    check_stopped(&net.get_net_status_client("\tstatus ")?)?;
    check_error(
        bounded(net.create_net_status_client("status")).await?,
        ErrorKind::ClientAlreadyExists,
        "create duplicate normalized name",
    )?;
    let mut receiver = client.subscribe()?;
    check(
        matches!(
            next_snapshot(&mut receiver).await?.state,
            MonitorState::Stopped
        ),
        "pre-start subscription must deliver Stopped",
    )?;
    let _: Result<Option<std::net::Ipv4Addr>, NetError> = open_net::network::preferred_lan_ipv4();
    bounded(client.stop()).await??;
    check_stopped(&client)?;
    bounded(client.shutdown()).await??;
    check_closed(&client)?;
    check(
        matches!(
            next_snapshot(&mut receiver).await?.state,
            MonitorState::Closed
        ),
        "shutdown must publish final Closed",
    )?;
    check(
        bounded(receiver.recv()).await??.is_none(),
        "final state must end the subscription",
    )?;
    check_error(
        bounded(client.start()).await?,
        ErrorKind::Closed,
        "restart explicitly closed client",
    )?;
    bounded(net.destroy_net_status_client(" status\n")).await??;
    check_error(
        net.get_net_status_client("status"),
        ErrorKind::ClientNotFound,
        "get destroyed client",
    )?;
    check_error(
        bounded(client.start()).await?,
        ErrorKind::Closed,
        "restart destroyed client",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clones_share_idempotent_lifecycle_and_subscriptions_survive_restart() -> TestResult {
    let net = OpenNet::new()?;
    let client = bounded(net.create_net_status_client("restart")).await??;
    let cloned = client.clone();
    let retrieved = net.get_net_status_client("restart")?;
    let mut receiver = client.subscribe()?;
    let initial = next_snapshot(&mut receiver).await?;
    check(
        matches!(initial.state, MonitorState::Stopped),
        "subscription must precede start",
    )?;
    let persistent = subscribe_empty(&client)?;
    let persistent_id = persistent.id();
    let mut last_revision = initial.revision;
    let mut previous_id = None;
    for _ in 0..3 {
        let started = bounded(cloned.start()).await??;
        check(
            matches!(started.state, MonitorState::Running),
            "start must await its initial running snapshot",
        )?;
        let repeated = bounded(retrieved.start()).await??;
        check(
            matches!(repeated.state, MonitorState::Running),
            "repeated start must reuse the running monitor",
        )?;
        check_running(&client)?;
        let running = next_snapshot(&mut receiver).await?;
        check(
            matches!(running.state, MonitorState::Running) && running.revision > last_revision,
            "running snapshot must advance the persistent receiver",
        )?;
        let transient = subscribe_empty(&client)?;
        if let Some(previous_id) = previous_id {
            check(
                transient.id() != previous_id,
                "new subscriptions must have fresh identities",
            )?;
        }
        previous_id = Some(transient.id());
        check(
            transient.unsubscribe(),
            "own transient subscription must unsubscribe",
        )?;
        check(!transient.unsubscribe(), "unsubscribe must be idempotent")?;
        bounded(retrieved.stop()).await??;
        bounded(client.stop()).await??;
        check_stopped(&cloned)?;
        let stopped = next_snapshot(&mut receiver).await?;
        check(
            matches!(stopped.state, MonitorState::Stopped) && stopped.revision > running.revision,
            "stop must retain the receiver and advance its revision",
        )?;
        check_no_observation(&stopped)?;
        check(
            persistent.is_active() && persistent.id() == persistent_id,
            "stop must preserve the existing callback subscription",
        )?;
        last_revision = stopped.revision;
    }
    check_error(
        bounded(net.create_net_status_client("restart")).await?,
        ErrorKind::ClientAlreadyExists,
        "stop must preserve the named client reservation",
    )?;
    bounded(client.shutdown()).await??;
    check(
        matches!(
            next_snapshot(&mut receiver).await?.state,
            MonitorState::Closed
        ),
        "persistent receiver must observe shutdown",
    )?;
    check(
        bounded(receiver.recv()).await??.is_none(),
        "persistent receiver must end after Closed",
    )?;
    bounded(persistent.close()).await??;
    bounded(net.destroy_net_status_client("restart")).await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_owners_and_lifecycles_are_isolated_between_clients() -> TestResult {
    let net = OpenNet::new()?;
    let first = bounded(net.create_net_status_client("first")).await??;
    let second = bounded(net.create_net_status_client("second")).await??;
    bounded(first.start()).await??;
    bounded(second.start()).await??;
    let first_subscription = subscribe_empty(&first)?;
    let second_subscription = subscribe_empty(&second)?;
    check(
        first_subscription.id() != second_subscription.id(),
        "different clients must issue different subscription identities",
    )?;
    check(
        first_subscription.unsubscribe(),
        "own subscription must unsubscribe",
    )?;
    check(
        !first_subscription.unsubscribe(),
        "repeated unsubscribe must report absence",
    )?;
    check(
        second_subscription.is_active(),
        "first subscription must not affect second client",
    )?;
    let mut ids = HashSet::new();
    let mut subscriptions = Vec::new();
    for _ in 0..32 {
        let subscription = subscribe_empty(&first)?;
        ids.insert(subscription.id());
        subscriptions.push(subscription);
    }
    check(
        ids.len() == 32,
        "every registration must have a distinct identity",
    )?;
    for subscription in subscriptions {
        check(
            subscription.unsubscribe(),
            "each owned subscription must be removable",
        )?;
        check(
            !subscription.unsubscribe(),
            "removed subscription must stay inactive",
        )?;
    }
    bounded(first.stop()).await??;
    check_running(&second)?;
    check(
        second_subscription.is_active(),
        "second subscription must survive first stop",
    )?;
    let fresh = subscribe_empty(&second)?;
    bounded(net.destroy_net_status_client("first")).await??;
    check_running(&second)?;
    check(
        fresh.unsubscribe(),
        "second subscription must survive first destruction",
    )?;
    check(
        second_subscription.unsubscribe(),
        "second original subscription must remain independent",
    )?;
    bounded(net.destroy_net_status_client("second")).await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_creation_reserves_one_named_client() -> TestResult {
    const CALLERS: usize = 8;
    let net = Arc::new(OpenNet::new()?);
    let barrier = Arc::new(Barrier::new(CALLERS));
    let mut tasks = JoinSet::new();
    for _ in 0..CALLERS {
        let net = Arc::clone(&net);
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            net.create_net_status_client("shared").await
        });
    }
    let (created, duplicates) = bounded(async {
        let mut created = 0;
        let mut duplicates = 0;
        while let Some(result) = tasks.join_next().await {
            match result? {
                Ok(client) => {
                    check_stopped(&client)?;
                    created += 1;
                }
                Err(error) if error.kind() == ErrorKind::ClientAlreadyExists => duplicates += 1,
                Err(error) => return Err(error.into()),
            }
        }
        Ok::<_, BoxError>((created, duplicates))
    })
    .await??;
    check(
        (created, duplicates) == (1, CALLERS - 1),
        "concurrent creation must produce exactly one client",
    )?;
    check_stopped(&net.get_net_status_client("shared")?)?;
    bounded(net.destroy_net_status_client("shared")).await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_start_and_stop_finish_and_allow_restart() -> TestResult {
    const CALLERS: usize = 6;
    let net = OpenNet::new()?;
    let client = bounded(net.create_net_status_client("concurrent")).await??;
    for starting in [true, false] {
        let barrier = Arc::new(Barrier::new(CALLERS));
        let mut tasks = JoinSet::new();
        for _ in 0..CALLERS {
            let client = client.clone();
            let barrier = Arc::clone(&barrier);
            tasks.spawn(async move {
                barrier.wait().await;
                if starting {
                    client.start().await.map(|_| ())
                } else {
                    client.stop().await
                }
            });
        }
        bounded(async {
            while let Some(result) = tasks.join_next().await {
                result??;
            }
            Ok::<_, BoxError>(())
        })
        .await??;
        if starting {
            check_running(&client)?;
        } else {
            check_stopped(&client)?;
        }
    }
    let barrier = Arc::new(Barrier::new(CALLERS));
    let mut tasks = JoinSet::new();
    for index in 0..CALLERS {
        let client = client.clone();
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            for _ in 0..3 {
                if index % 2 == 0 {
                    client.start().await?;
                } else {
                    client.stop().await?;
                }
                tokio::task::yield_now().await;
            }
            Ok::<_, NetError>(())
        });
    }
    bounded(async {
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok::<_, BoxError>(())
    })
    .await??;
    bounded(client.stop()).await??;
    check_stopped(&client)?;
    bounded(client.start()).await??;
    check_running(&client)?;
    bounded(net.destroy_net_status_client("concurrent")).await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn destroy_invalidates_all_clones_and_releases_name_for_a_fresh_client() -> TestResult {
    let net = OpenNet::new()?;
    let old = bounded(net.create_net_status_client("reused")).await??;
    let cloned = old.clone();
    bounded(old.start()).await??;
    let old_subscription = subscribe_empty(&old)?;
    let old_id = old_subscription.id();
    bounded(net.destroy_net_status_client("reused")).await??;
    check_closed(&old)?;
    check_closed(&cloned)?;
    check_error(
        bounded(old.start()).await?,
        ErrorKind::Closed,
        "restart destroyed original",
    )?;
    check_error(
        bounded(cloned.start()).await?,
        ErrorKind::Closed,
        "restart destroyed clone",
    )?;
    check_error(
        bounded(net.destroy_net_status_client("reused")).await?,
        ErrorKind::ClientNotFound,
        "destroy already removed client",
    )?;
    let fresh = bounded(net.create_net_status_client("reused")).await??;
    check_stopped(&fresh)?;
    bounded(fresh.start()).await??;
    let fresh_subscription = subscribe_empty(&fresh)?;
    check(
        fresh_subscription.id() != old_id,
        "reused client name must receive fresh subscription identity",
    )?;
    old_subscription.unsubscribe();
    check(
        fresh_subscription.is_active(),
        "old subscription must not unregister the fresh client's observer",
    )?;
    check_error(
        bounded(old.start()).await?,
        ErrorKind::Closed,
        "name reuse must not revive destroyed client",
    )?;
    check_running(&fresh)?;
    check(
        fresh_subscription.unsubscribe(),
        "fresh subscription must remain removable",
    )?;
    bounded(old_subscription.close()).await??;
    bounded(net.destroy_net_status_client("reused")).await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_and_destroy_release_callbacks_that_capture_the_client() -> TestResult {
    let net = OpenNet::new()?;
    let client = bounded(net.create_net_status_client("capture-shutdown")).await??;
    bounded(client.start()).await??;
    let (subscription, marker) = subscribe_client_capture(&client)?;
    bounded(client.shutdown()).await??;
    check_capture_released(&marker).await?;
    check(
        !subscription.is_active(),
        "shutdown must retire the capturing callback while its guard remains held",
    )?;
    bounded(subscription.close()).await??;
    check_error(
        bounded(client.start()).await?,
        ErrorKind::Closed,
        "shutdown must remain permanent after callback release",
    )?;
    bounded(net.destroy_net_status_client("capture-shutdown")).await??;
    let client = bounded(net.create_net_status_client("capture-destroy")).await??;
    bounded(client.start()).await??;
    let (subscription, marker) = subscribe_client_capture(&client)?;
    bounded(net.destroy_net_status_client("capture-destroy")).await??;
    check_capture_released(&marker).await?;
    check(
        !subscription.is_active(),
        "destroy must retire the capturing callback while its guard remains held",
    )?;
    bounded(subscription.close()).await??;
    check_error(
        bounded(client.start()).await?,
        ErrorKind::Closed,
        "capture cleanup must not revive destroyed client",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_open_net_closes_external_clients_and_releases_callback_cycles() -> TestResult {
    for start_before_drop in [false, true] {
        let net = OpenNet::new()?;
        let client = bounded(net.create_net_status_client("drop-owner")).await??;
        let cloned = client.clone();
        if start_before_drop {
            bounded(client.start()).await??;
        }
        let (subscription, marker) = subscribe_client_capture(&client)?;
        drop(net);
        check_error(
            bounded(client.start()).await?,
            ErrorKind::Closed,
            "owner drop must invalidate original",
        )?;
        check_error(
            bounded(cloned.start()).await?,
            ErrorKind::Closed,
            "owner drop must invalidate clone",
        )?;
        check_closed(&client)?;
        check_closed(&cloned)?;
        check_capture_released(&marker).await?;
        check(
            !subscription.is_active(),
            "owner drop must retire callbacks without dropping their guards",
        )?;
        bounded(subscription.close()).await??;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_state_receive_keeps_the_initial_running_observation() -> TestResult {
    let net = OpenNet::new()?;
    let client = bounded(net.create_net_status_client("cancel-receive")).await??;
    let mut receiver = client.subscribe()?;
    check(
        matches!(
            next_snapshot(&mut receiver).await?.state,
            MonitorState::Stopped
        ),
        "initial state must be Stopped",
    )?;
    let mut waiting = Box::pin(receiver.recv());
    check(
        matches!(futures::poll!(waiting.as_mut()), std::task::Poll::Pending),
        "receive must wait without an update",
    )?;
    bounded(client.start()).await??;
    drop(waiting);
    check(
        matches!(
            next_snapshot(&mut receiver).await?.state,
            MonitorState::Running
        ),
        "cancelled receive consumed the running snapshot",
    )?;
    bounded(net.destroy_net_status_client("cancel-receive")).await??;
    Ok(())
}

#[cfg(feature = "ws-client")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_monitor_does_not_block_websocket_creation_on_the_common_engine() -> TestResult {
    let net = OpenNet::new()?;
    let status = bounded(net.create_net_status_client("monitor")).await??;
    bounded(status.start()).await??;
    let websocket = bounded(net.create_ws_client("websocket")).await??;
    check_running(&status)?;
    bounded(net.destroy_ws_client("websocket")).await??;
    drop(websocket);
    check_running(&status)?;
    bounded(net.destroy_net_status_client("monitor")).await??;
    Ok(())
}

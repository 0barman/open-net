use open_net::error::ErrorKind;
use open_net::net_status::{
    IpStack, MonitorState, NetStatusClient, NetworkSnapshot, NetworkStatus,
};
use open_net::subscription::{CallbackContext, StateReceiver, Subscription};
use open_net::{BoxError, NetError, OpenNet};

use std::future::Future;
use std::time::Duration;

type TestResult<T = ()> = Result<T, BoxError>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(Duration::from_secs(20), future).await?)
}

fn unknown(snapshot: &NetworkSnapshot) -> TestResult {
    check(
        snapshot.reachability.is_none()
            && snapshot.ip_stack.is_none()
            && snapshot.observed_at.is_none()
            && snapshot.network_name.is_none(),
        "inactive snapshots must not present stale observations as current",
    )
}

#[test]
fn public_snapshot_models_preserve_unknown_and_observed_values() -> TestResult {
    fn public_traits<T: Clone + std::fmt::Debug + Send + Sync + 'static>() {}
    public_traits::<NetStatusClient>();
    public_traits::<NetworkSnapshot>();
    public_traits::<MonitorState>();
    let snapshot = NetworkSnapshot {
        revision: 9,
        loss_epoch: 3,
        state: MonitorState::Failed(NetError::from(ErrorKind::Io)),
        reachability: None,
        ip_stack: None,
        observed_at: None,
        network_name: None,
    };
    let cloned = snapshot.clone();
    check(
        cloned.revision == 9 && cloned.loss_epoch == 3,
        "cloning must preserve snapshot version and loss history",
    )?;
    check(
        matches!(&cloned.state, MonitorState::Failed(error) if error.kind() == ErrorKind::Io),
        "failed snapshots must retain structured errors",
    )?;
    unknown(&cloned)?;
    for (stack, v4, v6, name) in [
        (IpStack::None, false, false, "None"),
        (IpStack::V4Only, true, false, "V4Only"),
        (IpStack::V6Only, false, true, "V6Only"),
        (IpStack::DualStack, true, true, "DualStack"),
    ] {
        check(
            stack.has_ipv4() == v4
                && stack.has_ipv6() == v6
                && stack.is_dual_stack() == (v4 && v6)
                && stack.name() == name
                && stack.to_string() == name,
            "IP stack queries and labels must retain their meaning",
        )?;
    }
    for (status, name) in [
        (NetworkStatus::Unavailable, "Unavailable"),
        (NetworkStatus::Available, "Available"),
    ] {
        check(
            status.name() == name && status.to_string() == name,
            "network status labels must remain stable",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn subscriptions_before_start_observe_unknown_then_final_closed() -> TestResult {
    let net = OpenNet::new()?;
    let client = net
        .create_net_status_client("api-v2-net-status-stopped")
        .await?;
    let snapshot = client.snapshot()?;
    check(
        matches!(&snapshot.state, MonitorState::Stopped),
        "new monitor must be stopped",
    )?;
    unknown(&snapshot)?;
    check(
        format!("{client:?}").starts_with("NetStatusClient"),
        "client must support a safe diagnostic representation",
    )?;
    let mut receiver: StateReceiver<NetworkSnapshot> = client.subscribe()?;
    let (sender, mut callbacks) = tokio::sync::mpsc::unbounded_channel();
    let subscription: Subscription = client.on_change(
        move |context: CallbackContext, value: Result<NetworkSnapshot, NetError>| {
            let _ = sender.send((context.id(), value));
        },
    )?;
    let initial = bounded(receiver.recv())
        .await??
        .ok_or("initial state missing")?;
    let (id, callback) = bounded(callbacks.recv())
        .await?
        .ok_or("initial callback missing")?;
    let callback = callback?;
    check(
        initial.revision == snapshot.revision
            && callback.revision == snapshot.revision
            && id == subscription.id()
            && matches!(&initial.state, MonitorState::Stopped)
            && matches!(&callback.state, MonitorState::Stopped),
        "async and callback subscriptions must receive the same initial version",
    )?;
    bounded(client.shutdown()).await??;
    let closed = bounded(receiver.recv())
        .await??
        .ok_or("final closed snapshot missing")?;
    let (_, callback) = bounded(callbacks.recv())
        .await?
        .ok_or("final closed callback missing")?;
    let callback = callback?;
    check(
        matches!(&closed.state, MonitorState::Closed)
            && matches!(&callback.state, MonitorState::Closed)
            && closed.revision == callback.revision
            && closed.revision > initial.revision,
        "shutdown must deliver one final closed version through both subscription forms",
    )?;
    unknown(&closed)?;
    check(
        bounded(receiver.recv()).await??.is_none(),
        "closed state stream must end",
    )?;
    bounded(subscription.close()).await??;
    check(
        matches!(client.snapshot()?.state, MonitorState::Closed),
        "final snapshot must remain queryable",
    )?;
    bounded(client.clone().shutdown()).await??;
    check(
        client.snapshot()?.revision == closed.revision,
        "repeated shutdown through a clone must preserve the final revision",
    )?;
    check(
        bounded(callbacks.recv()).await?.is_none(),
        "a final callback must release its capture without a duplicate delivery",
    )?;
    bounded(net.destroy_net_status_client("api-v2-net-status-stopped")).await??;
    Ok(())
}

#[tokio::test]
async fn owner_drop_delivers_final_state_to_observers_without_client_handles() -> TestResult {
    let net = OpenNet::new()?;
    let client = net
        .create_net_status_client("api-v2-net-status-owner-drop")
        .await?;
    let mut receiver = client.subscribe()?;
    let (sender, mut callbacks) = tokio::sync::mpsc::unbounded_channel();
    let subscription = client.on_change(move |_, value| {
        let _ = sender.send(value);
    })?;
    let initial = bounded(receiver.recv())
        .await??
        .ok_or("initial observer snapshot missing")?;
    let first_callback = bounded(callbacks.recv())
        .await?
        .ok_or("initial observer callback missing")??;
    check(
        matches!(&initial.state, MonitorState::Stopped)
            && first_callback.revision == initial.revision,
        "both observers must begin at the same stopped snapshot",
    )?;
    drop(client);
    drop(net);
    let closed = bounded(receiver.recv())
        .await??
        .ok_or("owner drop lost final stream snapshot")?;
    let callback = bounded(callbacks.recv())
        .await?
        .ok_or("owner drop lost final callback")??;
    check(
        matches!(&closed.state, MonitorState::Closed)
            && matches!(&callback.state, MonitorState::Closed)
            && closed.revision == callback.revision
            && closed.revision > initial.revision,
        "owner drop must deliver the same final version without retaining client handles",
    )?;
    unknown(&closed)?;
    check(
        bounded(receiver.recv()).await??.is_none(),
        "owner drop must end the stream",
    )?;
    bounded(subscription.close()).await??;
    check(
        bounded(callbacks.recv()).await?.is_none(),
        "owner drop must release the completed callback and its captures",
    )
}

#[tokio::test]
async fn clones_share_restartable_stop_and_permanent_shutdown() -> TestResult {
    let net = OpenNet::new()?;
    let client = net
        .create_net_status_client("api-v2-net-status-restart")
        .await?;
    let cloned = client.clone();
    let mut receiver = client.subscribe()?;
    let initial = bounded(receiver.recv())
        .await??
        .ok_or("initial stopped snapshot missing")?;
    check(
        matches!(&initial.state, MonitorState::Stopped),
        "subscription must begin before start",
    )?;
    let started = bounded(client.start()).await??;
    check(
        matches!(&started.state, MonitorState::Running)
            && started.revision > initial.revision
            && started.reachability.is_some()
            && started.ip_stack.is_some()
            && started.observed_at.is_some(),
        "start must await a complete initial observation",
    )?;
    let repeated = bounded(cloned.start()).await??;
    check(
        matches!(&repeated.state, MonitorState::Running) && repeated.revision >= started.revision,
        "repeated start must keep the shared monitor running",
    )?;
    bounded(cloned.stop()).await??;
    let stopped = client.snapshot()?;
    check(
        matches!(&stopped.state, MonitorState::Stopped) && stopped.revision > repeated.revision,
        "stop through any clone must stop the shared monitor",
    )?;
    unknown(&stopped)?;
    let observed_stop = bounded(receiver.recv())
        .await??
        .ok_or("subscription ended on stop")?;
    check(
        matches!(&observed_stop.state, MonitorState::Stopped)
            && observed_stop.revision == stopped.revision,
        "existing subscription must observe stop",
    )?;
    let restarted = bounded(client.start()).await??;
    check(
        matches!(&restarted.state, MonitorState::Running)
            && restarted.revision > stopped.revision
            && restarted.loss_epoch >= stopped.loss_epoch,
        "restart must keep monotonic revision and loss history",
    )?;
    let observed_restart = bounded(receiver.recv())
        .await??
        .ok_or("subscription ended on restart")?;
    check(
        matches!(&observed_restart.state, MonitorState::Running)
            && observed_restart.revision >= restarted.revision,
        "existing subscription must survive restart",
    )?;
    bounded(cloned.shutdown()).await??;
    check(
        matches!(client.snapshot()?.state, MonitorState::Closed),
        "shutdown must close every clone",
    )?;
    check(
        matches!(bounded(client.start()).await?, Err(error) if error.kind() == ErrorKind::Closed),
        "a permanently closed monitor must reject restart",
    )?;
    bounded(client.shutdown()).await??;
    bounded(net.destroy_net_status_client("api-v2-net-status-restart")).await??;
    Ok(())
}

use super::super::shared::SharedNetworkService;
use super::*;
use std::time::Duration;
type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
fn check(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}
fn service() -> Result<Arc<SharedNetworkService>, NetError> {
    SharedNetworkService::new(Arc::new(CommonEngine::new(16, 16)?))
}
#[tokio::test]
async fn closed_owner_and_clones_remain_closed_while_source_runs() -> TestResult {
    let service = service()?;
    let a = NetworkObservationOwner::new(service.context())?;
    let clone = a.clone();
    let b = NetworkObservationOwner::new(service.context())?;
    let _ = tokio::time::timeout(Duration::from_secs(5), b.start()).await??;
    a.request_close();
    check(
        matches!(clone.snapshot_latest()?.state, MonitorState::Closed),
        "closed view read source Running",
    )?;
    check(a.subscribe().is_err(), "closed view accepted subscriber")?;
    check(
        matches!(b.snapshot()?.state, MonitorState::Running),
        "closing A changed B",
    )?;
    service.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn facade_stop_restart_is_local_and_preserves_subscriptions() -> TestResult {
    let service = service()?;
    let a = NetworkObservationOwner::new(service.context())?;
    let b = NetworkObservationOwner::new(service.context())?;
    let mut subscription = a.subscribe()?;
    let _ = tokio::time::timeout(Duration::from_secs(5), a.start()).await??;
    let _ = tokio::time::timeout(Duration::from_secs(5), b.start()).await??;
    a.request_stop()?;
    a.wait_cleanup().await?;
    check(
        matches!(a.snapshot()?.state, MonitorState::Stopped),
        "stop did not commit local Stopped",
    )?;
    check(
        matches!(b.snapshot()?.state, MonitorState::Running),
        "stop affected other facade",
    )?;
    let restarted = a.start().await?;
    check(
        matches!(restarted.state, MonitorState::Running)
            && restarted.revision == a.snapshot()?.revision,
        "start returned before committing snapshot",
    )?;
    check(
        subscription.recv().await?.is_some(),
        "stop ended subscription",
    )?;
    service.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn engine_close_reaches_stopped_facade_and_passive_view() -> TestResult {
    let service = service()?;
    let a = NetworkObservationOwner::new(service.context())?;
    let mut receiver = a.subscribe()?;
    service.shutdown().await?;
    check(
        matches!(a.snapshot()?.state, MonitorState::Closed),
        "stopped facade survived source shutdown",
    )?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(snapshot) = receiver.recv().await? {
            if matches!(snapshot.state, MonitorState::Closed) {
                return Ok::<(), NetError>(());
            }
        }
        Err(NetError::from(ErrorKind::Internal))
    })
    .await??;
    Ok(())
}

fn pending_service() -> Result<
    (
        Arc<SharedNetworkService>,
        tokio::sync::mpsc::UnboundedReceiver<()>,
    ),
    NetError,
> {
    let service = service()?;
    let (entered, receiver) = tokio::sync::mpsc::unbounded_channel();
    service
        .inner_for_test()
        .set_monitor_factory_for_test(Arc::new(move || {
            let entered = entered.clone();
            Box::pin(async move {
                let _ = entered.send(());
                std::future::pending().await
            })
        }))?;
    Ok((service, receiver))
}
async fn bounded<T>(
    future: impl std::future::Future<Output = T>,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    Ok(tokio::time::timeout(Duration::from_secs(5), future).await?)
}
#[tokio::test]
async fn local_stop_wakes_all_old_starts_without_waiting_for_other_initialization() -> TestResult {
    let (service, mut entered) = pending_service()?;
    let a = NetworkObservationOwner::new(service.context())?;
    let b = NetworkObservationOwner::new(service.context())?;
    let mut a_first = Box::pin(a.start());
    let mut a_second = Box::pin(a.start());
    let mut b_start = Box::pin(b.start());
    {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        use std::future::Future;
        check(
            a_first.as_mut().poll(&mut cx).is_pending(),
            "first start did not wait",
        )?;
        check(
            a_second.as_mut().poll(&mut cx).is_pending(),
            "second start did not share pending generation",
        )?;
        check(
            b_start.as_mut().poll(&mut cx).is_pending(),
            "B did not wait",
        )?;
    }
    bounded(entered.recv())
        .await?
        .ok_or("factory not entered")?;
    a.request_stop()?;
    // Restart must not take over the previous cycle's waiters.
    let mut restarted = Box::pin(a.start());
    {
        use std::future::Future;
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        check(
            restarted.as_mut().poll(&mut cx).is_pending(),
            "restart did not use new local generation",
        )?;
    }
    check(
        matches!(bounded(a_first).await??.state, MonitorState::Stopped),
        "old start revived after restart",
    )?;
    check(
        matches!(bounded(a_second).await??.state, MonitorState::Stopped),
        "second old start missed stop",
    )?;
    {
        use std::future::Future;
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        check(
            b_start.as_mut().poll(&mut cx).is_pending(),
            "A stop cancelled B initialization",
        )?;
    }
    a.request_close();
    let closed = bounded(restarted).await?;
    check(
        matches!(closed, Err(error) if error.kind() == ErrorKind::Closed),
        "local shutdown did not wake pending restart",
    )?;
    b.request_close();
    bounded(b.wait_cleanup()).await??;
    bounded(service.shutdown()).await??;
    Ok(())
}
#[tokio::test]
async fn shutdown_terminates_pending_start_and_bridge_even_when_another_view_is_pending(
) -> TestResult {
    let (service, mut entered) = pending_service()?;
    let a = NetworkObservationOwner::new(service.context())?;
    let b = NetworkObservationOwner::new(service.context())?;
    b.activate()?;
    let a_task = {
        let a = a.clone();
        tokio::spawn(async move { a.start().await })
    };
    bounded(entered.recv())
        .await?
        .ok_or("factory not entered")?;
    // Establish A's admission deterministically even if its task has not polled.
    a.activate()?;
    a.request_close();
    bounded(a.wait_cleanup()).await??;
    check(
        matches!(bounded(a_task).await??, Err(error) if error.kind() == ErrorKind::Closed),
        "pending start survived local close",
    )?;
    check(
        *::tokio::sync::watch::Receiver::borrow(&a.bridge_finished),
        "closed bridge remained active",
    )?;
    check(
        !service.context().is_closed(),
        "local close shut down engine",
    )?;
    b.request_close();
    bounded(service.shutdown()).await??;
    Ok(())
}
#[tokio::test]
async fn each_facade_retains_its_full_subscription_quota() -> TestResult {
    let service = service()?;
    let a = NetworkObservationOwner::new(service.context())?;
    let b = NetworkObservationOwner::new(service.context())?;
    let mut a_receivers = Vec::new();
    let mut b_receivers = Vec::new();
    for _ in 0..1024 {
        a_receivers.push(a.subscribe()?);
        b_receivers.push(b.subscribe()?);
    }
    check(
        matches!(a.subscribe(), Err(error) if error.kind() == ErrorKind::SubscriptionLimitReached),
        "facade quota not enforced",
    )?;
    drop(a_receivers.pop());
    let recovered = a.subscribe()?;
    check(
        recovered.id() != b_receivers.first().ok_or("B receivers missing")?.id(),
        "independent subscriber identity collided",
    )?;
    a.request_close();
    b.request_close();
    bounded(service.shutdown()).await??;
    Ok(())
}
#[tokio::test]
async fn dropping_an_inactive_view_retires_its_bridge_without_source_updates() -> TestResult {
    let service = service()?;
    let a = NetworkObservationOwner::new(service.context())?;
    let mut finished = a.bridge_finished.clone();
    drop(a);
    bounded(async {
        while !*finished.borrow_and_update() {
            finished.changed().await?;
        }
        Ok::<(), tokio::sync::watch::error::RecvError>(())
    })
    .await??;
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn local_revision_exhaustion_still_closes_demand_and_bridge() -> TestResult {
    let (service, mut entered) = pending_service()?;
    let owner = NetworkObservationOwner::new(service.context())?;
    owner.activate()?;
    bounded(entered.recv())
        .await?
        .ok_or("factory not entered")?;
    owner
        .local
        .lock()
        .map_err(NetError::from_poison)?
        .view
        .snapshot
        .revision = u64::MAX;
    owner.request_close();
    bounded(owner.wait_cleanup()).await??;
    check(
        service.active_consumers_for_test()? == 0,
        "revision failure prevented lease release",
    )?;
    check(
        owner.snapshot().is_err(),
        "irrecoverable local revision error was hidden",
    )?;
    bounded(service.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn irrecoverable_source_failure_ends_local_stream_and_releases_demand() -> TestResult {
    let service = service()?;
    let owner = NetworkObservationOwner::new(service.context())?;
    let mut receiver = owner.subscribe()?;
    bounded(owner.start()).await??;
    service
        .inner_for_test()
        .exhaust_source_revision_for_test()?;
    let (finished, error) = service.inner_for_test().request_stop(false);
    let expected = error.ok_or("source exhaustion did not return an infrastructure error")?;
    bounded(
        super::super::inner_net_status_client::InnerNetStatusClient::wait_until_finished(finished),
    )
    .await??;
    let observed = bounded(async {
        loop {
            match receiver.recv().await {
                Err(error) => return Ok::<NetError, NetError>(error),
                Ok(Some(_)) => {}
                Ok(None) => return Err(NetError::from(ErrorKind::Internal)),
            }
        }
    })
    .await??;
    check(
        observed.kind() == expected.kind(),
        "infrastructure failure became a network state",
    )?;
    check(
        owner.snapshot().is_err(),
        "local snapshot hid source infrastructure failure",
    )?;
    check(
        service.active_consumers_for_test()? == 0,
        "source infrastructure failure leaked local demand",
    )?;
    let cleanup = bounded(owner.wait_cleanup()).await?;
    check(
        cleanup.is_err() && *::tokio::sync::watch::Receiver::borrow(&owner.bridge_finished),
        "cleanup error skipped bridge retirement",
    )?;
    let _ = service.shutdown().await;
    Ok(())
}

fn bridge_pause() -> (
    BridgePause,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
) {
    let (entered, entering) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    let (processed, processing) = oneshot::channel();
    (
        BridgePause {
            entered,
            resume: resumed,
            processed,
        },
        entering,
        resume,
        processing,
    )
}
#[tokio::test]
async fn real_bridge_delayed_initial_cannot_overwrite_start_commit() -> TestResult {
    let service = service()?;
    let (pause, entered, resume, processed) = bridge_pause();
    let owner = NetworkObservationOwner::build(service.context(), Some(pause))?;
    bounded(entered).await??;
    let started = bounded(owner.start()).await??;
    check(
        matches!(started.state, MonitorState::Running),
        "start failed to commit before bridge resumed",
    )?;
    let _ = resume.send(());
    bounded(processed).await??;
    let after = owner.snapshot()?;
    check(
        matches!(after.state, MonitorState::Running)
            && after.revision >= started.revision
            && after.loss_epoch == started.loss_epoch
            && after.observed_at == started.observed_at,
        "real delayed bridge overwrote newer start facts",
    )?;
    service.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn real_bridge_preserves_complete_facts_after_coalesced_disconnect_and_recovery() -> TestResult
{
    use crate::net_status::{IpStack, NetworkStatus};
    let (service, mut entered_factory) = pending_service()?;
    let owner = NetworkObservationOwner::new(service.context())?;
    let mut receiver = owner.subscribe()?;
    owner.activate()?;
    bounded(entered_factory.recv())
        .await?
        .ok_or("factory not entered")?;
    let inner = service.inner_for_test();
    inner.publish_observation_for_test(
        NetworkStatus::Available,
        IpStack::DualStack,
        Some("initial".to_owned()),
    )?;
    bounded(async {
        loop {
            if receiver
                .recv()
                .await?
                .is_some_and(|snapshot| snapshot.network_name.as_deref() == Some("initial"))
            {
                return Ok::<(), NetError>(());
            }
        }
    })
    .await??;
    let (pause, entered, resume, processed) = bridge_pause();
    *owner.bridge_pause.lock().map_err(NetError::from_poison)? = Some(pause);
    inner.publish_observation_for_test(
        NetworkStatus::Available,
        IpStack::V4Only,
        Some("before-loss".to_owned()),
    )?;
    bounded(entered).await??;
    inner.publish_observation_for_test(NetworkStatus::Unavailable, IpStack::None, None)?;
    inner.publish_observation_for_test(
        NetworkStatus::Available,
        IpStack::V6Only,
        Some("recovered".to_owned()),
    )?;
    let source = service.context().snapshot()?;
    let _ = resume.send(());
    bounded(processed).await??;
    let local = bounded(async {
        loop {
            if let Some(snapshot) = receiver.recv().await? {
                if snapshot.network_name.as_deref() == Some("recovered") {
                    return Ok::<NetworkSnapshot, NetError>(snapshot);
                }
            }
        }
    })
    .await??;
    check(
        local.loss_epoch == source.loss_epoch
            && local.observed_at == source.observed_at
            && local.ip_stack == source.ip_stack
            && local.network_name == source.network_name
            && local.reachability == source.reachability,
        "bridge lost source history/time/metadata during coalescing",
    )?;
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn named_terminal_observation_remains_available_without_reacquiring_monitoring() -> TestResult
{
    let service = service()?;
    let owner = NetworkObservationOwner::new(service.context())?;
    owner.request_close();
    owner.wait_cleanup().await?;
    let mut receiver = owner.subscribe_terminal()?;
    check(
        receiver
            .recv()
            .await?
            .is_some_and(|snapshot| matches!(snapshot.state, MonitorState::Closed)),
        "named closed monitor lost terminal subscription compatibility",
    )?;
    check(
        receiver.recv().await?.is_none(),
        "terminal named subscription did not finish",
    )?;
    check(
        owner.subscribe().is_err(),
        "HTTP registration was admitted after close",
    )?;
    check(
        service.active_consumers_for_test()? == 0,
        "terminal observation started monitoring",
    )?;
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn committed_stop_outcome_is_not_rewritten_by_later_close() -> TestResult {
    let (service, mut entered) = pending_service()?;
    let owner = NetworkObservationOwner::new(service.context())?;
    let mut starting = Box::pin(owner.start());
    check(
        futures::poll!(&mut starting).is_pending(),
        "start did not wait for the pending initialization",
    )?;
    bounded(entered.recv())
        .await?
        .ok_or("factory not entered")?;
    owner.request_stop()?;
    let stopped = owner.snapshot()?;
    owner.request_close();
    let outcome = bounded(starting).await?;
    bounded(owner.wait_cleanup()).await??;
    bounded(service.shutdown()).await??;
    check(
        matches!(outcome, Ok(snapshot) if matches!(snapshot.state, MonitorState::Stopped) && snapshot.revision == stopped.revision),
        "later close rewrote the outcome already committed by stop",
    )
}

#[tokio::test]
async fn activation_capture_keeps_original_cycle_after_restart() -> TestResult {
    let service = service()?;
    let owner = NetworkObservationOwner::new(service.context())?;
    let first = owner
        .activate_impl(false)?
        .ok_or("start activation did not capture its cycle")?;
    owner.request_stop()?;
    let _second = owner
        .activate_impl(false)?
        .ok_or("restart activation did not create a new cycle")?;
    let stopped = first.0.wait().await?;
    check(
        matches!(stopped.state, MonitorState::Stopped),
        "an earlier activation observed a later restart cycle",
    )?;
    owner.request_close();
    owner.wait_cleanup().await?;
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn closed_view_rejects_source_failure_already_in_flight_on_bridge() -> TestResult {
    let service = service()?;
    let owner = NetworkObservationOwner::new(service.context())?;
    let mut receiver = owner.subscribe()?;
    let _ = receiver.recv().await?;
    owner.request_close();
    let closed = owner.snapshot()?;
    // A bridge can have copied its input just before close revoked the view.
    // Apply that input through the same error entry point after close commits.
    owner.fail_source(NetError::from(ErrorKind::Internal));
    let cached = owner.snapshot()?;
    check(
        matches!(cached.state, MonitorState::Closed) && cached.revision == closed.revision,
        "late source error replaced a previously committed local Closed snapshot",
    )?;
    check(
        receiver.recv().await?.is_some_and(|value| {
            matches!(value.state, MonitorState::Closed) && value.revision == cached.revision
        }) && receiver.recv().await?.is_none(),
        "local snapshot and terminal receiver disagree after late source failure",
    )?;
    bounded(owner.wait_cleanup()).await??;
    bounded(service.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn first_source_failure_is_preserved_across_close_and_late_failures() -> TestResult {
    let service = service()?;
    let owner = NetworkObservationOwner::new(service.context())?;
    owner.fail_source(NetError::from(ErrorKind::ResourceExhausted));
    owner.request_close();
    owner.fail_source(NetError::from(ErrorKind::Internal));
    check(
        matches!(owner.snapshot(), Err(error) if error.kind() == ErrorKind::ResourceExhausted),
        "later terminal input replaced the first committed infrastructure error",
    )?;
    bounded(owner.wait_cleanup()).await??;
    bounded(service.shutdown()).await??;
    Ok(())
}

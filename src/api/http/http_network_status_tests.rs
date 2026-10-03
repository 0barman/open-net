use super::*;
use crate::module::net_status::inner::shared::SharedNetworkService;
use crate::net_status::MonitorState;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

type TestResult = std::result::Result<(), crate::BoxError>;

fn pending_service() -> Result<(Arc<SharedNetworkService>, Arc<AtomicUsize>), NetError> {
    let service = SharedNetworkService::new(Arc::new(crate::common::CommonEngine::new(8, 8)?))?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let entered = attempts.clone();
    service
        .inner_for_test()
        .set_monitor_factory_for_test(Arc::new(move || {
            entered.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::pending())
        }))?;
    Ok((service, attempts))
}

fn observed_client(
    service: &Arc<SharedNetworkService>,
    name: &str,
) -> Result<HttpClient, NetError> {
    HttpClient::new_with_network_status(
        HttpClientConfig::new("http://127.0.0.1:1")?,
        name,
        service.context(),
    )
}

async fn receive_closed(receiver: &mut StateReceiver<NetworkSnapshot>) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = receiver
                .recv()
                .await?
                .ok_or("network receiver ended before Closed")?;
            if matches!(snapshot.state, MonitorState::Closed) {
                break;
            }
        }
        if receiver.recv().await?.is_some() {
            return Err("network receiver delivered updates after Closed".into());
        }
        Ok::<(), crate::BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn snapshot_after_last_unsubscribe_reads_unchanged_shared_facts_without_reacquiring_demand(
) -> TestResult {
    let (service, _) = pending_service()?;
    let client = observed_client(&service, "network-cache-after-unsubscribe")?;
    let other = observed_client(&service, "network-cache-surviving-demand")?;
    let receiver = client.subscribe_network_status()?;
    let other_receiver = other.subscribe_network_status()?;
    let mut facts = service.context().subscribe_facts();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(facts.borrow_and_update().state, MonitorState::Starting) {
                break;
            }
            facts.changed().await?;
        }
        Ok::<(), crate::BoxError>(())
    })
    .await??;
    if !matches!(client.network_snapshot()?.state, MonitorState::Starting) {
        return Err("HTTP cache did not import the shared Starting observation".into());
    }
    receiver.unsubscribe();
    if !matches!(client.network_snapshot()?.state, MonitorState::Starting)
        || service.active_consumers_for_test()? != 1
    {
        return Err(
            "HTTP cache after unsubscribe ignored unchanged shared facts or reacquired demand"
                .into(),
        );
    }
    drop(other_receiver);
    client.shutdown().await?;
    other.shutdown().await?;
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn injected_context_projects_cache_and_client_shutdown_is_terminal() -> TestResult {
    let service = crate::module::net_status::inner::shared::SharedNetworkService::new(Arc::new(
        crate::common::CommonEngine::new(8, 8)?,
    ))?;
    let client = HttpClient::new_with_network_status(
        HttpClientConfig::new("http://127.0.0.1:1")?,
        "network-injected-cache",
        service.context(),
    )?;
    let snapshot = client.network_snapshot();
    client.shutdown().await?;
    if !matches!(snapshot?.state, crate::net_status::MonitorState::Stopped) {
        return Err("cache query started a previously stopped network monitor".into());
    }
    if !matches!(
        client.network_snapshot()?.state,
        crate::net_status::MonitorState::Closed
    ) {
        return Err("HTTP shutdown did not preserve its local Closed snapshot".into());
    }
    if !matches!(client.subscribe_network_status(), Err(error) if error.kind() == ErrorKind::Closed)
    {
        return Err("closed HTTP observation admitted another registration".into());
    }
    Ok(())
}

#[tokio::test]
async fn standalone_without_context_reports_configuration_error_for_network_observation(
) -> TestResult {
    let client = HttpClient::new(
        HttpClientConfig::new("http://127.0.0.1:1")?,
        "network-unconfigured",
    )?;
    let snapshot = client.network_snapshot();
    let receiver = client.subscribe_network_status();
    let callback = client.on_network_status_change(|_, _| {});
    client.shutdown().await?;
    if !matches!(snapshot, Err(error) if error.kind() == ErrorKind::InvalidConfig)
        || !matches!(receiver, Err(error) if error.kind() == ErrorKind::InvalidConfig)
        || !matches!(callback, Err(error) if error.kind() == ErrorKind::InvalidConfig)
    {
        return Err("standalone network observation did not report missing context".into());
    }
    if !matches!(client.network_snapshot(), Err(error) if error.kind() == ErrorKind::InvalidConfig)
    {
        return Err("shutdown implicitly created a standalone network context".into());
    }
    Ok(())
}

#[tokio::test]
async fn repeated_cache_reads_never_acquire_demand_or_start_initialization() -> TestResult {
    let (service, attempts) = pending_service()?;
    let client = observed_client(&service, "network-inert-snapshot")?;
    for _ in 0..8 {
        if !matches!(client.network_snapshot()?.state, MonitorState::Stopped) {
            return Err("pure cache reading changed the stopped network state".into());
        }
    }
    if attempts.load(Ordering::SeqCst) != 0 || service.active_consumers_for_test()? != 0 {
        return Err("cache reading started monitoring or acquired a lease".into());
    }
    client.shutdown().await?;
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn clones_and_registrations_share_one_lease_until_last_unsubscribe() -> TestResult {
    let (service, _) = pending_service()?;
    let client = observed_client(&service, "network-observer-count")?;
    let clone = client.clone();
    let first = client.subscribe_network_status()?;
    let second = clone.subscribe_network_status()?;
    if service.active_consumers_for_test()? != 1 {
        return Err("two observations on one logical HTTP client acquired duplicate leases".into());
    }
    first.unsubscribe();
    drop(first);
    if service.active_consumers_for_test()? != 1 {
        return Err("one receiver unsubscribe stopped another receiver's demand".into());
    }
    drop(second);
    if service.active_consumers_for_test()? != 0 {
        return Err("last receiver Drop leaked HTTP observation demand".into());
    }
    let receiver = clone.subscribe_network_status()?;
    let id = receiver.id();
    let subscription = receiver.into_callback(|_, _| {})?;
    if subscription.id() != id || service.active_consumers_for_test()? != 1 {
        return Err("callback conversion duplicated or dropped its observation lease".into());
    }
    subscription.close().await?;
    if service.active_consumers_for_test()? != 0 {
        return Err("callback unsubscribe did not release its observation lease".into());
    }
    client.shutdown().await?;
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn every_http_shutdown_mode_closes_local_observation_without_stopping_other_clients(
) -> TestResult {
    for mode in 0..4 {
        let (service, _) = pending_service()?;
        let client = observed_client(&service, &format!("network-closing-{mode}"))?;
        let other = observed_client(&service, &format!("network-surviving-{mode}"))?;
        let mut receiver = client.subscribe_network_status()?;
        let other_receiver = other.subscribe_network_status()?;
        let weak = Arc::downgrade(&client.inner);
        match mode {
            0 => client.request_shutdown(),
            1 => client.shutdown().await?,
            2 => client.shutdown_graceful().await?,
            _ => {}
        }
        if mode != 3 && !matches!(client.network_snapshot()?.state, MonitorState::Closed) {
            return Err("HTTP close returned before committing local Closed state".into());
        }
        if mode != 3 && service.active_consumers_for_test()? != 1 {
            return Err("HTTP shutdown retained its demand until the client was dropped".into());
        }
        drop(client);
        receive_closed(&mut receiver).await?;
        if weak.upgrade().is_some() {
            return Err("network receiver retained HTTP request workers".into());
        }
        if service.active_consumers_for_test()? != 1
            || matches!(other.network_snapshot()?.state, MonitorState::Closed)
            || matches!(other_receiver.current().state, MonitorState::Closed)
        {
            return Err(
                "closing one HTTP client affected another client's network observations".into(),
            );
        }
        other.shutdown().await?;
        service.shutdown().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_callback_can_wait_for_http_shutdown_without_waiting_for_itself() -> TestResult {
    let (service, _) = pending_service()?;
    let client = observed_client(&service, "network-callback-reentrant-shutdown")?;
    let captured = client.clone();
    let once = AtomicBool::new(false);
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let subscription = client.on_network_status_change(move |_, _| {
        if !once.swap(true, Ordering::SeqCst) {
            let result = futures::executor::block_on(captured.shutdown());
            let _ = sent.send(result);
        }
    })?;
    tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await?
        .ok_or("network callback did not complete reentrant HTTP shutdown")??;
    subscription.close().await?;
    if service.active_consumers_for_test()? != 0
        || !matches!(client.network_snapshot()?.state, MonitorState::Closed)
    {
        return Err("reentrant HTTP shutdown retained active network demand".into());
    }
    service.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_shutdown_finishes_while_network_callback_is_blocked_but_subscription_close_waits(
) -> TestResult {
    let (service, _) = pending_service()?;
    let client = observed_client(&service, "network-blocked-callback")?;
    let once = AtomicBool::new(false);
    let (entered, mut entry) = tokio::sync::mpsc::unbounded_channel();
    let (release, released) = std::sync::mpsc::channel();
    let released = std::sync::Mutex::new(released);
    let subscription = client.on_network_status_change(move |_, _| {
        if !once.swap(true, Ordering::SeqCst) {
            let _ = entered.send(());
            if let Ok(released) = released.lock() {
                let _ = released.recv();
            }
        }
    })?;
    tokio::time::timeout(Duration::from_secs(5), entry.recv())
        .await?
        .ok_or("network callback did not enter")?;
    tokio::time::timeout(Duration::from_secs(5), client.shutdown()).await??;
    let mut close = Box::pin(subscription.close());
    if !matches!(futures::poll!(close.as_mut()), std::task::Poll::Pending) {
        return Err("Subscription::close stopped waiting for its active callback".into());
    }
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(5), close).await??;
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn source_engine_shutdown_closes_observation_but_standalone_requests_continue() -> TestResult
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let serving = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request = [0_u8; 4096];
        let _ = socket.read(&mut request).await?;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await?;
        Ok::<(), std::io::Error>(())
    });
    let (service, _) = pending_service()?;
    let context = service.context();
    let client = HttpClient::new_with_network_status(
        HttpClientConfig::new(format!("http://{address}"))?,
        "network-source-closed-http-alive",
        context.clone(),
    )?;
    let mut receiver = client.subscribe_network_status()?;
    service.shutdown().await?;
    drop(service);
    receive_closed(&mut receiver).await?;
    if !matches!(context.snapshot()?.state, MonitorState::Closed)
        || !matches!(client.network_snapshot()?.state, MonitorState::Closed)
        || !matches!(client.subscribe_network_status(), Err(error) if error.kind() == ErrorKind::Closed)
    {
        return Err("closed engine context was lost or rebound to an active source".into());
    }
    let response = tokio::time::timeout(Duration::from_secs(5), client.get("/")).await??;
    if response.status != http::StatusCode::OK {
        return Err("source engine shutdown changed standalone HTTP request lifetime".into());
    }
    tokio::time::timeout(Duration::from_secs(5), serving).await???;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn new_observation_retries_failed_initialization_without_replacing_existing_lease(
) -> TestResult {
    let service = SharedNetworkService::new(Arc::new(crate::common::CommonEngine::new(8, 8)?))?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let factory_attempts = attempts.clone();
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    service
        .inner_for_test()
        .set_monitor_factory_for_test(Arc::new(move || {
            let attempt = factory_attempts.fetch_add(1, Ordering::SeqCst);
            let _ = started.send(attempt);
            Box::pin(async move {
                if attempt == 0 {
                    Err(NetError::from(ErrorKind::RuntimeUnavailable))
                } else {
                    std::future::pending().await
                }
            })
        }))?;
    let client = observed_client(&service, "network-failed-register-retry")?;
    let mut first = client.subscribe_network_status()?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = first
                .recv()
                .await?
                .ok_or("Failed terminated HTTP observation")?;
            if matches!(snapshot.state, MonitorState::Failed(_)) {
                break;
            }
        }
        Ok::<(), crate::BoxError>(())
    })
    .await??;
    let second = client.subscribe_network_status()?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(attempt) = starts.recv().await {
            if attempt == 1 {
                return Ok::<(), crate::BoxError>(());
            }
        }
        Err("new registration did not retry failed initialization".into())
    })
    .await??;
    if service.active_consumers_for_test()? != 1 || attempts.load(Ordering::SeqCst) != 2 {
        return Err("HTTP failure retry duplicated the client lease or monitor attempt".into());
    }
    drop(second);
    if service.active_consumers_for_test()? != 1 {
        return Err("old HTTP observation lost its lease during explicit retry".into());
    }
    client.shutdown().await?;
    receive_closed(&mut first).await?;
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn subscription_capacity_failure_preserves_existing_http_demand_and_is_recoverable(
) -> TestResult {
    let (service, _) = pending_service()?;
    let client = observed_client(&service, "network-observation-capacity")?;
    let mut receivers = Vec::new();
    for _ in 0..1024 {
        receivers.push(client.subscribe_network_status()?);
    }
    if !matches!(client.subscribe_network_status(), Err(error) if error.kind() == ErrorKind::SubscriptionLimitReached)
        || service.active_consumers_for_test()? != 1
    {
        return Err("observation capacity failure leaked or removed aggregate HTTP demand".into());
    }
    receivers.pop();
    let recovered = client.subscribe_network_status()?;
    drop(receivers);
    if service.active_consumers_for_test()? != 1 {
        return Err("recovered observation did not retain its own registration".into());
    }
    drop(recovered);
    if service.active_consumers_for_test()? != 0 {
        return Err("capacity exhaustion left an unreachable observation lease".into());
    }
    client.shutdown().await?;
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn callback_installation_failure_and_first_delivery_unsubscribe_release_http_registration(
) -> TestResult {
    let (service, _) = pending_service()?;
    let engine = service.context().common_engine()?;
    let client = observed_client(&service, "network-callback-install-rollback")?;
    engine.cb_pool.fail_next_start_after(1);
    if !matches!(client.on_network_status_change(|_, _| {}), Err(error) if error.kind() == ErrorKind::RuntimeUnavailable)
    {
        return Err("HTTP callback installation did not return pool startup failure".into());
    }
    if service.active_consumers_for_test()? != 0 {
        return Err("HTTP callback installation failure leaked aggregate demand".into());
    }
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let subscription = client.on_network_status_change(move |context, _| {
        let _ = sent.send(context.unsubscribe());
    })?;
    if tokio::time::timeout(Duration::from_secs(5), received.recv()).await? != Some(true) {
        return Err("first HTTP network callback could not unsubscribe itself".into());
    }
    if subscription.is_active() || service.active_consumers_for_test()? != 0 {
        return Err("HTTP callback conversion revived a self-unsubscribed registration".into());
    }
    subscription.close().await?;
    client.shutdown().await?;
    service.shutdown().await?;
    Ok(())
}

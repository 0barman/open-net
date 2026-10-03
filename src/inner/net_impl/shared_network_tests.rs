//! Exercise shared ownership through the real engine factories.
use super::*;
use crate::api::open_net::OpenNet;
use crate::error::ErrorKind;
use crate::net_status::MonitorState;
use std::future::Future;
#[cfg(all(feature = "ws-client", feature = "http-client"))]
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::oneshot;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn check(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}
async fn bounded<T>(future: impl Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(std::time::Duration::from_secs(5), future).await?)
}

#[cfg(all(feature = "ws-client", feature = "http-client"))]
#[tokio::test]
async fn public_factories_share_one_detector_and_close_consumers_independently() -> TestResult {
    let network = crate::network::NetworkConfig::default()
        .with_network_status_policy(crate::NetworkStatusPolicy::PauseOnUnavailable);
    let net = OpenNet::new_with_network_config(network)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let created = calls.clone();
    let (release, released) = oneshot::channel();
    let released = Mutex::new(Some(released));
    net.inner
        .network_status
        .inner_for_test()
        .set_monitor_factory_for_test(Arc::new(move || {
            created.fetch_add(1, Ordering::SeqCst);
            let release = released
                .lock()
                .map_err(NetError::from_poison)
                .map(|mut value| value.take());
            Box::pin(async move {
                if let Some(released) = release? {
                    released
                        .await
                        .map_err(|_| NetError::from(ErrorKind::Internal))?;
                }
                netwatch::netmon::Monitor::new()
                    .await
                    .map_err(|error| NetError::with_source(ErrorKind::RuntimeUnavailable, error))
            })
        }))?;
    let http_config = crate::api::http::HttpClientConfig::new("http://127.0.0.1")?;
    let (a, b, http) = bounded(async {
        tokio::join!(
            net.create_ws_client("shared-ws-a"),
            net.create_ws_client("shared-ws-b"),
            net.create_http_client_with_config("shared-http", http_config)
        )
    })
    .await?;
    let (a, b, http) = (a?, b?, http?);
    let facade = net.create_net_status_client("shared-facade").await?;
    let mut http_receiver = http.subscribe_network_status()?;
    let _ = release.send(());
    let fact = bounded(facade.start()).await??;
    let http_fact = http.network_snapshot()?;
    check(
        calls.load(Ordering::SeqCst) == 1,
        "mixed factories constructed multiple detectors",
    )?;
    check(
        fact.reachability == http_fact.reachability
            && fact.ip_stack == http_fact.ip_stack
            && fact.loss_epoch == http_fact.loss_epoch
            && fact.observed_at == http_fact.observed_at,
        "HTTP and facade imported different source facts",
    )?;
    check(
        net.inner.network_status.active_consumers_for_test()? == 4,
        "logical consumers were not registered once",
    )?;
    net.destroy_ws_client("shared-ws-a").await?;
    check(
        matches!(http.network_snapshot()?.state, MonitorState::Running),
        "closing one WS shut down shared observations",
    )?;
    facade.stop().await?;
    check(
        matches!(facade.snapshot()?.state, MonitorState::Stopped),
        "facade did not stop locally",
    )?;
    check(
        matches!(http.network_snapshot()?.state, MonitorState::Running),
        "facade stop interrupted HTTP",
    )?;
    let ignore = net
        .create_ws_client_with_network_config(
            "ignore",
            WebSocketClientConfig::default(),
            crate::network::NetworkConfig::default()
                .with_network_status_policy(crate::NetworkStatusPolicy::Ignore),
        )
        .await?;
    check(
        net.inner.network_status.active_consumers_for_test()? == 2,
        "Ignore WS added demand",
    )?;
    net.destroy_ws_client("shared-ws-b").await?;
    net.destroy_http_client("shared-http").await?;
    bounded(async {
        while let Some(snapshot) = http_receiver.recv().await? {
            if matches!(snapshot.state, MonitorState::Closed) {
                return Ok::<(), NetError>(());
            }
        }
        Err(NetError::from(ErrorKind::Internal))
    })
    .await??;
    check(
        net.inner.network_status.active_consumers_for_test()? == 0,
        "last close leaked service demand",
    )?;
    check(
        matches!(
            net.network_status_context().snapshot()?.state,
            MonitorState::Stopped
        ),
        "last consumer left detector running",
    )?;
    net.destroy_ws_client("ignore").await?;
    net.destroy_net_status_client("shared-facade").await?;
    drop((a, b, ignore));
    Ok(())
}

#[tokio::test]
async fn cancelled_named_destroy_retains_name_until_its_provider_exits() -> TestResult {
    let net = OpenNet::new()?;
    let (entered, entrance) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let resources = Mutex::new(Some((entered, released)));
    net.inner
        .network_status
        .inner_for_test()
        .set_monitor_factory_for_test(Arc::new(move || {
            let resources = resources
                .lock()
                .map_err(NetError::from_poison)
                .map(|mut value| value.take());
            Box::pin(async move {
                if let Some((entered, released)) = resources? {
                    tokio::task::spawn_blocking(move || {
                        let _ = entered.send(());
                        let _ = released.recv();
                    });
                }
                netwatch::netmon::Monitor::new()
                    .await
                    .map_err(|error| NetError::with_source(ErrorKind::RuntimeUnavailable, error))
            })
        }))?;
    let old = net.create_net_status_client("retiring-name").await?;
    bounded(old.start()).await??;
    bounded(entrance).await??;
    let mut destroying = Box::pin(net.destroy_net_status_client("retiring-name"));
    let pending = futures::poll!(&mut destroying).is_pending();
    drop(destroying);
    let name_reserved = matches!(net.get_net_status_client("retiring-name"), Err(error) if error.kind() == ErrorKind::ConnectionClosing);
    let other = net.create_net_status_client("ongoing-other").await?;
    let mut other_start = Box::pin(other.start());
    let other_pending = futures::poll!(&mut other_start).is_pending();
    let _ = release.send(());
    bounded(other_start).await??;
    bounded(async {
        loop {
            match net.get_net_status_client("retiring-name") {
                Err(error) if error.kind() == ErrorKind::ClientNotFound => {
                    return Ok::<(), NetError>(())
                }
                Err(error) if error.kind() == ErrorKind::ConnectionClosing => {
                    tokio::task::yield_now().await
                }
                _ => return Err(NetError::from(ErrorKind::Internal)),
            }
        }
    })
    .await??;
    check(
        pending && name_reserved && other_pending,
        "destroy or restart bypassed real provider retirement",
    )?;
    let fresh = net.create_net_status_client("retiring-name").await?;
    check(
        matches!(fresh.snapshot()?.state, MonitorState::Stopped),
        "same name reused old local lifecycle",
    )?;
    check(
        matches!(old.start().await, Err(error) if error.kind() == ErrorKind::Closed),
        "old facade revived after name reuse",
    )?;
    check(
        matches!(other.snapshot()?.state, MonitorState::Running),
        "old destroy waited for new provider",
    )?;
    net.destroy_net_status_client("retiring-name").await?;
    net.destroy_net_status_client("ongoing-other").await?;
    Ok(())
}

#[tokio::test]
async fn independent_engines_do_not_share_shutdown_or_state_lifecycles() -> TestResult {
    let a = OpenNet::new()?;
    let b = OpenNet::new()?;
    let a_client = a.create_net_status_client("same-name").await?;
    let b_client = b.create_net_status_client("same-name").await?;
    let (a_start, b_start) =
        bounded(async { tokio::join!(a_client.start(), b_client.start()) }).await?;
    a_start?;
    b_start?;
    let a_context = a.network_status_context();
    drop(a);
    check(
        matches!(a_context.snapshot()?.state, MonitorState::Closed)
            && matches!(a_client.snapshot()?.state, MonitorState::Closed),
        "engine exit did not close external observations",
    )?;
    check(
        matches!(b_client.snapshot()?.state, MonitorState::Running),
        "another engine was closed",
    )?;
    b.destroy_net_status_client("same-name").await?;
    Ok(())
}

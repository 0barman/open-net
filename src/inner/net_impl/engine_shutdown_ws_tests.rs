use crate::api::open_net::OpenNet;
use crate::error::ErrorKind;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::network::{NetworkConfig, NetworkStatusPolicy};
use crate::ws::{
    ConnectOptions, HandshakeHeaders, HandshakeProvider, TaskEventOptions, WebSocketClientConfig,
};
use crate::HeaderMap;
use std::future::Future;
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Wake, Waker};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(5);

fn engine() -> TestResult<OpenNet> {
    Ok(OpenNet::new_with_network_config(
        NetworkConfig::default().with_network_status_policy(NetworkStatusPolicy::Ignore),
    )?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_provider_after_ws_worker_exits_with_task_subscription_retained(
) -> TestResult {
    let net = engine()?;
    let mut config = WebSocketClientConfig::default();
    config.requests.manual_response_grace = Duration::ZERO;
    let client = net
        .create_ws_client_with_config("retiring-provider", config)
        .await?;
    let (entered, started) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let mut options = ConnectOptions::new("ws://127.0.0.1:1");
    options.handshake_provider = Some(HandshakeProvider::blocking(move |_| {
        entered.send(())?;
        released
            .lock()
            .map_err(|_| std::io::Error::other("test gate poisoned"))?
            .recv_timeout(WAIT)?;
        Ok(HandshakeHeaders::new(HeaderMap::new()))
    }));
    let session = client.start_session(options, None).await?;
    started.recv_timeout(WAIT)?;
    // This starts the task listener pool and retains its executor after both
    // the engine and client have closed. Retirement must not depend on Drop.
    let subscription = session.on_task(TaskEventOptions::default(), |_, _| {})?;
    net.begin_shutdown();
    tokio::time::timeout(WAIT, client.shutdown()).await??;
    check!(
        tokio::time::timeout(Duration::from_millis(30), net.shutdown())
            .await
            .is_err()
    )?;
    release.send(())?;
    tokio::time::timeout(WAIT, net.shutdown()).await??;
    tokio::time::timeout(WAIT, net.shutdown()).await??;
    check!(client
        .start_session(ConnectOptions::new("ws://127.0.0.1:1"), None)
        .await
        .is_err())?;
    subscription.close().await?;
    drop(session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_provider_cannot_wait_for_its_own_engine_retirement() -> TestResult {
    let net = Arc::new(engine()?);
    let client = net.create_ws_client("provider-self-close").await?;
    let callback_net = net.clone();
    let (observed, received) = mpsc::channel();
    let mut options = ConnectOptions::new("ws://127.0.0.1:1");
    options.handshake_provider = Some(HandshakeProvider::blocking(move |_| {
        let result = futures::executor::block_on(callback_net.shutdown());
        observed.send(result.map_err(|error| error.kind()))?;
        Ok(HandshakeHeaders::new(HeaderMap::new()))
    }));
    let _session = client.start_session(options, None).await?;
    check_eq!(received.recv_timeout(WAIT)?, Err(ErrorKind::InvalidInput))?;
    tokio::time::timeout(WAIT, net.shutdown()).await??;
    Ok(())
}

struct ReentrantCompletion {
    other: Arc<OpenNet>,
    observed: mpsc::Sender<Result<(), ErrorKind>>,
}

impl Wake for ReentrantCompletion {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let result = futures::executor::block_on(self.other.shutdown());
        let _ = self.observed.send(result.map_err(|error| error.kind()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completion_waker_cannot_block_reaper_waiting_for_another_engine() -> TestResult {
    let first = engine()?;
    let other = Arc::new(engine()?);
    let release = tokio_util::sync::CancellationToken::new();
    let release_on_drop = release.clone().drop_guard();
    let gate = release.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    first.inner.common_engine.post(async move {
        let _ = entered.send(());
        gate.cancelled().await;
    });
    tokio::time::timeout(WAIT, started).await??;
    let (observed, received) = mpsc::channel();
    let waker = Waker::from(Arc::new(ReentrantCompletion {
        other: other.clone(),
        observed,
    }));
    let mut waiting = Box::pin(first.shutdown());
    check!(waiting
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending())?;
    release.cancel();
    check_eq!(received.recv_timeout(WAIT)?, Err(ErrorKind::InvalidInput))?;
    tokio::time::timeout(WAIT, waiting).await??;
    tokio::time::timeout(WAIT, other.shutdown()).await??;
    drop(release_on_drop);
    Ok(())
}

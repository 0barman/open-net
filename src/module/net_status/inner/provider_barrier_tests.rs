//! Provider retirement must include tasks detached by the upstream monitor.

use super::*;
use std::sync::Mutex;

struct InitializerDropped(Option<oneshot::Sender<()>>);

impl Drop for InitializerDropped {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

struct DetachedWork {
    client: Arc<InnerNetStatusClient>,
    initialized: oneshot::Receiver<()>,
    cancelled: oneshot::Receiver<()>,
    release: std::sync::mpsc::Sender<()>,
    live: Arc<AtomicBool>,
}

fn client_with_detached_work() -> TestResult<DetachedWork> {
    let (client, _) = client_with_gate()?;
    let (initialized, init_wait) = oneshot::channel();
    let (cancelled, cancel_wait) = oneshot::channel();
    let (release, release_wait) = std::sync::mpsc::channel();
    let resources = Mutex::new(Some((initialized, cancelled, release_wait)));
    let live = Arc::new(AtomicBool::new(false));
    let worker_live = Arc::clone(&live);
    let factory: MonitorFactory = Arc::new(move || {
        let resources = resources
            .lock()
            .map(|mut resources| resources.take())
            .map_err(NetError::from_poison);
        let worker_live = Arc::clone(&worker_live);
        Box::pin(async move {
            if let Some((initialized, cancelled, release_wait)) = resources? {
                let _initializer = InitializerDropped(Some(cancelled));
                // This deliberately detached provider work survives the initializer's
                // cancellation. Only the provider runtime's exit may retire it.
                tokio::task::spawn_blocking(move || {
                    worker_live.store(true, Ordering::SeqCst);
                    let _ = initialized.send(());
                    let _ = release_wait.recv();
                    worker_live.store(false, Ordering::SeqCst);
                });
                return std::future::pending().await;
            }
            netwatch::netmon::Monitor::new()
                .await
                .map_err(|_| NetError::from(crate::error::ErrorKind::RuntimeUnavailable))
        })
    });
    *client
        .monitor_factory
        .lock()
        .map_err(NetError::from_poison)? = Some(factory);
    Ok(DetachedWork {
        client,
        initialized: init_wait,
        cancelled: cancel_wait,
        release,
        live,
    })
}

#[tokio::test]
async fn retirement_waits_for_detached_provider_work_after_initializer_drop() -> TestResult {
    let fixture = client_with_detached_work()?;
    let mut starting = Box::pin(fixture.client.start());
    check(
        poll_once(starting.as_mut()).await.is_pending(),
        "start did not wait",
    )?;
    bounded(fixture.initialized).await??;
    let done = completion(&fixture.client)?;
    let (tickets, error) = fixture.client.request_stop(false);
    check(error.is_none(), "stop submission failed")?;
    bounded(fixture.cancelled).await??;
    // On the old implementation the completion runs before this single-worker
    // queue barrier, although detached provider work is still running.
    bounded(fixture.client.engine.runtime_handle().spawn(async {})).await??;
    let completed_early = *::tokio::sync::watch::Receiver::borrow(&done);
    let was_live = fixture.live.load(Ordering::SeqCst);
    let _ = fixture.release.send(());
    bounded(InnerNetStatusClient::wait_until_finished(tickets)).await??;
    bounded(starting).await??;
    check(was_live, "provider resource did not remain blocked")?;
    check(
        !completed_early,
        "retirement completed before detached provider work exited",
    )?;
    check(
        !fixture.live.load(Ordering::SeqCst),
        "provider resource remained after retirement",
    )
}

#[tokio::test]
async fn restart_waits_for_previous_provider_runtime_exit() -> TestResult {
    let fixture = client_with_detached_work()?;
    let mut starting = Box::pin(fixture.client.start());
    check(
        poll_once(starting.as_mut()).await.is_pending(),
        "start did not wait",
    )?;
    bounded(fixture.initialized).await??;
    let (_, error) = fixture.client.request_stop(false);
    check(error.is_none(), "stop submission failed")?;
    bounded(fixture.cancelled).await??;
    let mut retry = Box::pin(fixture.client.start());
    let pending = poll_once(retry.as_mut()).await.is_pending();
    let installed_early = fixture.client.current_state()?.is_some();
    let _ = fixture.release.send(());
    bounded(starting).await??;
    bounded(retry).await??;
    bounded(fixture.client.stop()).await??;
    check(pending, "retry did not await the retired provider")?;
    check(
        !installed_early,
        "new provider installed while the old provider still had resources",
    )
}

#[tokio::test]
async fn real_netwatch_children_are_gone_when_retirement_completes() -> TestResult {
    let (client, _) = client_with_gate()?;
    let (handles, mut received) = mpsc::unbounded_channel();
    let factory: MonitorFactory = Arc::new(move || {
        let handles = handles.clone();
        Box::pin(async move {
            let monitor = netwatch::netmon::Monitor::new()
                .await
                .map_err(|_| NetError::from(crate::error::ErrorKind::RuntimeUnavailable))?;
            let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
                NetError::with_source(crate::error::ErrorKind::RuntimeUnavailable, error)
            })?;
            handles
                .send(runtime)
                .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
            Ok(monitor)
        })
    });
    *client
        .monitor_factory
        .lock()
        .map_err(NetError::from_poison)? = Some(factory);
    for _ in 0..3 {
        bounded(client.start()).await??;
        let provider_runtime = bounded(received.recv())
            .await?
            .ok_or("provider runtime missing")?;
        bounded(client.stop()).await??;
        check(
            provider_runtime.metrics().num_alive_tasks() == 0,
            "real provider actor or nested task remained after retirement",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn aborted_submission_still_waits_for_detached_provider_resources() -> TestResult {
    let fixture = client_with_detached_work()?;
    let installed = install_unpolled(&fixture.client)?;
    let task = fixture.client.engine.runtime_handle().spawn(installed.task);
    bounded(fixture.initialized).await??;
    task.abort();
    let joined = bounded(task).await?;
    bounded(fixture.cancelled).await??;
    let completed_early = *::tokio::sync::watch::Receiver::borrow(&installed.done);
    let _ = fixture.release.send(());
    finished(installed.done).await?;
    check(
        matches!(joined, Err(error) if error.is_cancelled()),
        "submission was not cancelled",
    )?;
    check(
        !completed_early,
        "cancelled submission signalled provider completion too early",
    )?;
    check(
        !fixture.live.load(Ordering::SeqCst),
        "cancelled provider retained its detached work",
    )?;
    check(
        matches!(
            fixture.client.snapshot()?.state,
            crate::net_status::MonitorState::Failed(_)
        ),
        "cancelled provider retained a misleading active status",
    )
}

#[tokio::test]
async fn lost_exit_sender_is_failure_instead_of_retirement_success() -> TestResult {
    let (sender, receiver) = watch::channel(false);
    drop(sender);
    let outcome = bounded(InnerNetStatusClient::wait_until_finished(vec![receiver])).await?;
    check(
        matches!(outcome, Err(error) if error.kind() == crate::error::ErrorKind::RuntimeUnavailable),
        "an unproven provider exit was accepted as successful cleanup",
    )
}

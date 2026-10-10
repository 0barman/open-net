use super::*;

#[tokio::test(start_paused = true)]
async fn worker_observes_shutdown_after_an_admission_waiter_consumes_notification(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut worker, request_rx) = tests::paused_worker()?;
    let (supervisor_tx, supervisor_rx) = mpsc::channel(1);
    worker.supervisor_tx = supervisor_tx;
    let (response_tx, _response_rx) = mpsc::channel(1);
    worker.request_shutdown();
    // A late admission waiter can consume notify_one's retained permit before
    // observing the closed flag. The worker must not depend on that permit.
    worker.shutdown_notify.notified().await;
    tokio::time::timeout(
        Duration::from_secs(1),
        send_loop(
            request_rx,
            supervisor_rx,
            response_tx,
            worker.client.clone(),
            Arc::clone(&worker.config),
            Arc::clone(&worker.shutdown_notify),
            Arc::clone(&worker.shutdown_sent),
        ),
    )
    .await
    .map_err(|_| std::io::Error::other("worker missed a consumed shutdown notification"))?;
    Ok(())
}

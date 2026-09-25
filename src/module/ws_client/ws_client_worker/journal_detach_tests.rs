use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::ws::{
    BackoffConfig, ConnectionEventKind, ConnectionState, JournalOptions, ReconnectPolicy,
};
use std::sync::atomic::AtomicUsize;

async fn next_history(
    worker: &mut WSClientWorker,
    history: &mut crate::ws::ConnectionEvents,
) -> TestResult<crate::ws::ConnectionEvent> {
    loop {
        tokio::select! {
            record = history.recv() => {
                return record?.ok_or_else(|| test_error("history ended before retry admission"));
            }
            event = worker.io_event_rx.recv() => {
                let event = event.ok_or_else(|| test_error("worker stopped before lifecycle admission"))?;
                worker.handle_io_event(event).await;
            }
        }
    }
}

#[tokio::test]
async fn native_journal_detach_releases_retry_admission_in_the_actual_worker() -> TestResult {
    for explicit_unsubscribe in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let accepts = Arc::new(AtomicUsize::new(0));
        let observed_accepts = accepts.clone();
        let release = CancellationToken::new();
        let _release_on_exit = release.clone().drop_guard();
        let released = release.clone();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await?;
            observed_accepts.fetch_add(1, Ordering::SeqCst);
            let refusal = tokio_tungstenite::tungstenite::http::Response::builder()
                .status(503)
                .body(Some("retry this handshake".to_owned()))?;
            let rejected = tokio_tungstenite::accept_hdr_async(first, move |
                _request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                _response: tokio_tungstenite::tungstenite::handshake::server::Response,
            | Err(refusal.clone())).await;
            check!(
                rejected.is_err(),
                "fixture unexpectedly accepted the first handshake"
            )?;
            let (second, _) = listener.accept().await?;
            observed_accepts.fetch_add(1, Ordering::SeqCst);
            let socket = tokio_tungstenite::accept_async(second).await?;
            released.cancelled().await;
            drop(socket);
            Ok::<(), crate::module::ws_client::test_support::TestError>(())
        });
        let (_inner, mut worker) =
            crate::module::ws_client::test_support::new_inner(WebSocketClientConfig {
                close_timeout: Duration::from_millis(40),
                ..WebSocketClientConfig::default()
            })?;
        let (runtime, journal) = crate::module::ws_client::v2_test_support::unconnected(
            worker.config.clone(),
            crate::ws::ResponseRouting::Disabled,
            10,
            20,
            Some(JournalOptions {
                max_events: 4,
                ..JournalOptions::default()
            }),
        )?;
        let session = runtime.lifecycle.clone();
        let journal = journal.ok_or_else(|| test_error("requested journal was not installed"))?;
        let mut history = session.subscribe_events()?;
        let options = {
            let mut options = crate::ws::ConnectOptions::new(format!("ws://{address}"));
            options.reconnect = ReconnectPolicy::Backoff(BackoffConfig {
                max_retries: 1,
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                max_elapsed: Some(Duration::from_secs(3)),
            });
            options
        };
        let (reply, accepted) = oneshot::channel();
        let mut terminal_deadline = None;
        check!(
            !worker
                .handle_command(
                    ClientCommand::Connect {
                        options,
                        initial_connect_deadline: None,
                        runtime: runtime.clone(),
                        session: session.clone(),
                        reply,
                    },
                    &mut terminal_deadline
                )
                .await
        )?;
        accepted.await??;
        for expected in ["started", "failed"] {
            let record = tokio::time::timeout(
                Duration::from_secs(3),
                next_history(&mut worker, &mut history),
            )
            .await
            .map_err(|error| test_error(format!("waiting for {expected}: {error}")))??;
            let actual = match record.kind {
                ConnectionEventKind::AttemptStarted { .. } => "started",
                ConnectionEventKind::AttemptFailed {
                    retry: RetryDecision::Scheduled { .. },
                    ..
                } => "failed",
                other => {
                    return Err(test_error(format!(
                        "unexpected pre-detach event: {other:?}"
                    )))
                }
            };
            check_eq!(actual, expected)?;
        }
        check!(
            tokio::time::timeout(
                Duration::from_millis(20),
                next_history(&mut worker, &mut history)
            )
            .await
            .is_err(),
            "the minimum unread journal did not hold the next admission"
        )?;
        check_eq!(accepts.load(Ordering::SeqCst), 1)?;
        if explicit_unsubscribe {
            check!(journal.unsubscribe())?;
            check!(!session.cancel_token().is_cancelled())?;
            drop(journal);
        } else {
            drop(journal);
        }
        check!(
            !session.cancel_token().is_cancelled(),
            "journal Drop cancelled its session"
        )?;
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(session.snapshot()?.state, ConnectionState::Connected(_)) {
                let event = worker
                    .io_event_rx
                    .recv()
                    .await
                    .ok_or_else(|| test_error("worker stopped before retry connected"))?;
                worker.handle_io_event(event).await;
            }
            Ok::<(), crate::module::ws_client::test_support::TestError>(())
        })
        .await??;
        check_eq!(accepts.load(Ordering::SeqCst), 2)?;
        let info = session.wait_connected().await?;
        check!(
            !tokio::time::timeout(
                Duration::from_secs(3),
                worker.handle_command(
                    ClientCommand::CloseSession {
                        session: session.clone(),
                        frame: None,
                        deadline: Instant::now() + worker.config.close_timeout
                    },
                    &mut terminal_deadline,
                )
            )
            .await?
        )?;
        check_eq!(
            session.closed().await?.reason,
            TerminationReason::LocalClose
        )?;
        let mut sequence = 2;
        let mut established = 0;
        let mut closed = 0;
        while let Some(record) =
            tokio::time::timeout(Duration::from_secs(3), history.recv()).await??
        {
            sequence += 1;
            check_eq!(record.sequence, sequence)?;
            match record.kind {
                ConnectionEventKind::Established { connection } => {
                    check_eq!(connection.connection_id, info.connection_id)?;
                    established += 1;
                }
                ConnectionEventKind::Closed { result } => {
                    check_eq!(result?.reason, TerminationReason::LocalClose)?;
                    closed += 1;
                }
                _ => {}
            }
        }
        check_eq!(established, 1)?;
        check_eq!(closed, 1)?;
        check_eq!(sequence, 6)?;
        release.cancel();
        tokio::time::timeout(Duration::from_secs(3), server).await???;
    }
    Ok(())
}

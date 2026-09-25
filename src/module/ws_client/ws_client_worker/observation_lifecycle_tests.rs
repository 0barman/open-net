use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::ws::{ConnectionEventKind, ConnectionState, JournalOptions};
use std::task::Poll;

#[tokio::test]
async fn actual_worker_commits_matching_history_and_state_then_closes_before_cleanup() -> TestResult
{
    for shutdown in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let release = CancellationToken::new();
        let _release_on_exit = release.clone().drop_guard();
        let released = release.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let socket = tokio_tungstenite::accept_async(stream).await?;
            // Hold the live socket without replying to Close. Closing must become
            // visible before the client's bounded graceful cleanup completes.
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
                max_events: 32,
                ..JournalOptions::default()
            }),
        )?;
        let session = runtime.lifecycle.clone();
        let mut journal = journal.ok_or_else(|| test_error("journal missing"))?;
        let mut states = session.watch_state()?;
        let mut history = session.subscribe_events()?;
        check!(matches!(
            states.recv().await?.map(|s| s.state),
            Some(ConnectionState::Connecting)
        ))?;
        let options = {
            let mut options = crate::ws::ConnectOptions::new(format!("ws://{address}"));
            options.reconnect = crate::ws::ReconnectPolicy::Disabled;
            options
        };
        let (reply, accepted) = oneshot::channel();
        let mut terminal_deadline = None;
        check_eq!(
            worker
                .handle_command(
                    ClientCommand::Connect {
                        options,
                        initial_connect_deadline: None,
                        runtime: Arc::clone(&runtime),
                        session: Arc::clone(&session),
                        reply,
                    },
                    &mut terminal_deadline
                )
                .await,
            false
        )?;
        accepted.await??;
        tokio::time::timeout(Duration::from_secs(3), async {
            while !matches!(session.snapshot()?.state, ConnectionState::Connected(_)) {
                let event = worker
                    .io_event_rx
                    .recv()
                    .await
                    .ok_or_else(|| test_error("worker events ended before handshake"))?;
                worker.handle_io_event(event).await;
            }
            Ok::<(), crate::module::ws_client::test_support::TestError>(())
        })
        .await??;
        let connected = session.wait_connected().await?;
        check!(matches!(states.recv().await?.map(|s| s.state),
            Some(ConnectionState::Connected(info)) if info.connection_id == connected.connection_id))?;
        let command = if shutdown {
            ClientCommand::Shutdown
        } else {
            ClientCommand::CloseSession {
                session: session.clone(),
                frame: None,
                deadline: Instant::now() + worker.config.close_timeout,
            }
        };
        let mut closing = Box::pin(worker.handle_command(command, &mut terminal_deadline));
        check!(matches!(futures::poll!(closing.as_mut()), Poll::Pending))?;
        check!(matches!(
            session.snapshot()?.state,
            ConnectionState::Closing
        ))?;
        let mut wait_connected = Box::pin(session.wait_connected());
        check!(matches!(
            futures::poll!(wait_connected.as_mut()),
            Poll::Pending
        ))?;
        check_eq!(
            tokio::time::timeout(Duration::from_secs(3), closing).await?,
            shutdown
        )?;
        let reason = if shutdown {
            TerminationReason::ClientShutdown
        } else {
            TerminationReason::LocalClose
        };
        check_eq!(session.closed().await?.reason, reason)?;
        check!(wait_connected.await.is_err())?;
        check!(matches!(states.recv().await?.map(|s| s.state),
            Some(ConnectionState::Closed(Ok(end))) if end.reason == reason))?;
        check!(states.recv().await?.is_none())?;
        let mut kinds = Vec::new();
        while let Some(record) = journal.recv().await? {
            let observed = history
                .recv()
                .await?
                .ok_or_else(|| test_error("history ended before reliable journal"))?;
            check_eq!(observed.sequence, record.sequence)?;
            check_eq!(observed.occurred_at, record.occurred_at)?;
            kinds.push(match record.kind {
                ConnectionEventKind::AttemptStarted { .. } => "started",
                ConnectionEventKind::Established { connection } => {
                    check_eq!(connection.connection_id, connected.connection_id)?;
                    "established"
                }
                ConnectionEventKind::Disconnected { .. } => "disconnected",
                ConnectionEventKind::Closed { result } => {
                    check_eq!(result?.reason, reason)?;
                    "closed"
                }
                _ => {
                    return Err(test_error(
                        "unexpected failure in successful worker lifecycle",
                    ))
                }
            });
        }
        check_eq!(
            kinds,
            vec!["started", "established", "disconnected", "closed"]
        )?;
        check!(history.recv().await?.is_none())?;
        release.cancel();
        tokio::time::timeout(Duration::from_secs(3), server).await???;
    }
    Ok(())
}

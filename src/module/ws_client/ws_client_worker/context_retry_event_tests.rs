use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::ws::{
    BackoffConfig, HandshakeHeaders, HandshakeProvider, JournalOptions, ReconnectPolicy,
};

#[tokio::test]
async fn retry_event_delay_is_the_same_duration_used_by_the_next_backoff() -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (mut task, owner) = tests::task_with_provider(HandshakeProvider::new(|_| async {
        Ok(HandshakeHeaders::new(http::HeaderMap::new()))
    }))?;
    drop(owner);
    let (runtime, session, _events) =
        crate::module::ws_client::v2_test_support::journal_runtime(JournalOptions::default())?;
    task.target.runtime = runtime;
    session.begin_cycle(task.generation, false)?;
    task.target.session = Arc::clone(&session);
    task.target.options.url = format!("ws://{address}");
    task.target.options.handshake_timeout = Duration::from_secs(1);
    task.target.options.reconnect = ReconnectPolicy::Backoff(BackoffConfig {
        max_retries: 1,
        initial_delay: Duration::from_secs(10),
        max_delay: Duration::from_secs(10),
        max_elapsed: None,
    });
    task.budget = ConnectionBudget::new(Instant::now(), None, None)?;
    let (event_tx, mut event_rx) = mpsc::channel(8);
    task.event_tx = event_tx;
    let cancel = task.cancel.clone();
    let _cancel_on_drop = cancel.clone().drop_guard();
    let samples = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sample_count = Arc::clone(&samples);
    let task_cancel = cancel.clone();
    let run = tokio::spawn(async move {
        tokio::select! {
            _ = task_cancel.cancelled() => Ok(()),
            result = task.run_attempts_with_delay(move |_, _| {
                sample_count.fetch_add(1, Ordering::SeqCst);
                Duration::from_millis(500)
            }) => result,
        }
    });
    // Admit the first attempt and its headers before holding the server handshake.
    for prepared in [false, true] {
        let event = event_rx
            .recv()
            .await
            .ok_or_else(|| test_error("missing first attempt admission"))?;
        match event {
            IoEvent::ContextAttemptStarted {
                attempt,
                reservation,
                accepted,
                ..
            } if !prepared => {
                session.begin_attempt(attempt, reservation)?;
                accepted
                    .send(Ok(()))
                    .map_err(|_| test_error("Started ACK closed"))?;
            }
            IoEvent::ContextPrepared {
                generation,
                attempt_id,
                credential_version,
                accepted,
                ..
            } if prepared => {
                session.set_attempt_credential_version(
                    generation,
                    attempt_id,
                    credential_version,
                )?;
                accepted
                    .send(Ok(()))
                    .map_err(|_| test_error("Prepared ACK closed"))?;
            }
            _ => return Err(test_error("unexpected first admission order")),
        }
    }
    let (_server, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept()).await??;
    let failed = tokio::time::timeout(Duration::from_secs(3), event_rx.recv())
        .await?
        .ok_or_else(|| test_error("missing handshake timeout"))?;
    let IoEvent::ContextAttemptFailed {
        generation,
        attempt_id,
        failure,
        retry: RetryDecision::Scheduled { after },
        accepted,
        diagnostic,
    } = failed
    else {
        return Err(test_error("timeout did not schedule retry"));
    };
    check_eq!(failure.error().kind(), crate::error::ErrorKind::TimedOut)?;
    session.attempt_failed_with_diagnostic(
        generation,
        attempt_id,
        failure,
        RetryDecision::Scheduled { after },
        diagnostic,
    )?;
    check_eq!(after, Duration::from_millis(500))?;
    tokio::time::pause();
    let started = Instant::now();
    accepted
        .send(Ok(()))
        .map_err(|_| test_error("failure ACK closed"))?;
    tokio::task::yield_now().await;
    let expected = started
        .checked_add(after)
        .ok_or_else(|| test_error("test deadline overflow"))?;
    check!(matches!(session.snapshot()?.state,
        crate::ws::ConnectionState::Reconnecting { next_attempt_at: Some(at), .. }
        if at == expected.into_std()))?;
    tokio::time::advance(Duration::from_millis(499)).await;
    check!(matches!(
        event_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ))?;
    // Tokio timers have millisecond resolution; allow its single rounding tick.
    tokio::time::advance(Duration::from_millis(2)).await;
    let retry = event_rx
        .recv()
        .await
        .ok_or_else(|| test_error("missing retry Started"))?;
    let IoEvent::ContextAttemptStarted { attempt, .. } = retry else {
        return Err(test_error("retry did not start"));
    };
    check_eq!(attempt.attempt_id.as_u64(), 1)?;
    check!(started.elapsed() >= after && started.elapsed() <= after + Duration::from_millis(1))?;
    cancel.cancel();
    check_eq!(samples.load(Ordering::SeqCst), 1)?;
    run.await??;
    Ok(())
}

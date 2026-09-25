use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::ws::{ConnectionState, JournalOptions};
use std::task::Poll;

fn session() -> TestResult<(Arc<ConnectionSession>, crate::ws::ConnectionJournal)> {
    let (session, owner) = ConnectionSession::journal_fixture(1, 2, JournalOptions::default())?;
    session.begin_cycle(7, true)?;
    Ok((session, owner))
}

#[tokio::test(start_paused = true)]
async fn offline_wait_publishes_waiting_and_recovery_clears_it_before_admission() -> TestResult {
    let (session, _owner) = session()?;
    let (sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 1,
        loss_epoch: 1,
        status: Some(NetworkStatus::Unavailable),
    });
    let mut gate = NetworkGate::new(Some(receiver), Some((Arc::clone(&session), 7)));
    let deadline =
        ConnectionBudget::new(Instant::now(), None, Some(Duration::from_secs(10)))?.deadline();
    let mut wait = Box::pin(gate.wait_available(deadline));
    check!(matches!(futures::poll!(wait.as_mut()), Poll::Pending))?;
    check!(matches!(
        session.snapshot()?.state,
        ConnectionState::WaitingForNetwork
    ))?;
    sender.send_replace(NetworkStatusSnapshot {
        revision: 2,
        loss_epoch: 1,
        status: Some(NetworkStatus::Available),
    });
    check_eq!(wait.await?, Some(1))?;
    check!(matches!(session.snapshot()?.state,
        ConnectionState::Reconnecting { cycle_id, next_attempt_at: None }
        if cycle_id.as_u64() == 7))?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn published_retry_deadline_is_the_actual_backoff_deadline_and_hint_clears_it() -> TestResult
{
    let (session, _owner) = session()?;
    let mut gate = NetworkGate::new(None, Some((Arc::clone(&session), 7)));
    let hint = Notify::new();
    let started = Instant::now();
    let delay = Duration::from_secs(5);
    let expected = started
        .checked_add(delay)
        .ok_or_else(|| test_error("test time overflow"))?;
    let mut backoff = Box::pin(gate.backoff(delay, &hint, None));
    check!(matches!(futures::poll!(backoff.as_mut()), Poll::Pending))?;
    check!(matches!(session.snapshot()?.state,
        ConnectionState::Reconnecting { cycle_id, next_attempt_at: Some(at) }
        if cycle_id.as_u64() == 7 && at == expected.into_std()))?;
    tokio::time::advance(Duration::from_secs(1)).await;
    hint.notify_one();
    backoff.await?;
    check_eq!(started.elapsed(), Duration::from_secs(1))?;
    check!(matches!(
        session.snapshot()?.state,
        ConnectionState::Reconnecting {
            next_attempt_at: None,
            ..
        }
    ))?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn unknown_and_failed_monitor_resume_transport_without_stale_waiting_state() -> TestResult {
    for failed in [false, true] {
        let (session, _owner) = session()?;
        let (sender, receiver) = watch::channel(NetworkStatusSnapshot {
            revision: 1,
            loss_epoch: 3,
            status: Some(NetworkStatus::Unavailable),
        });
        let mut gate = NetworkGate::new(Some(receiver), Some((Arc::clone(&session), 7)));
        let deadline =
            ConnectionBudget::new(Instant::now(), None, Some(Duration::from_secs(10)))?.deadline();
        let mut wait = Box::pin(gate.wait_available(deadline));
        check!(matches!(futures::poll!(wait.as_mut()), Poll::Pending))?;
        if failed {
            drop(sender);
        } else {
            sender.send_replace(NetworkStatusSnapshot {
                revision: 2,
                loss_epoch: 3,
                status: None,
            });
        }
        check_eq!(wait.await?, if failed { None } else { Some(3) })?;
        check!(matches!(
            session.snapshot()?.state,
            ConnectionState::Reconnecting {
                next_attempt_at: None,
                ..
            }
        ))?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn network_loss_interrupts_backoff_and_is_visible_before_the_offline_wait() -> TestResult {
    let (session, _owner) = session()?;
    let (sender, receiver) = watch::channel(NetworkStatusSnapshot::default());
    let mut gate = NetworkGate::new(Some(receiver), Some((Arc::clone(&session), 7)));
    let hint = Notify::new();
    let mut backoff = Box::pin(gate.backoff(Duration::from_secs(5), &hint, None));
    check!(matches!(futures::poll!(backoff.as_mut()), Poll::Pending))?;
    sender.send_replace(NetworkStatusSnapshot {
        revision: 1,
        loss_epoch: 1,
        status: Some(NetworkStatus::Unavailable),
    });
    backoff.await?;
    check!(matches!(
        session.snapshot()?.state,
        ConnectionState::WaitingForNetwork
    ))?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stale_gate_cannot_replace_a_new_cycle_or_closing_state() -> TestResult {
    let (session, _owner) = session()?;
    let mut gate = NetworkGate::new(None, Some((Arc::clone(&session), 7)));
    session.begin_cycle(8, true)?;
    check_eq!(gate.wait_available(None).await?, None)?;
    check!(matches!(session.snapshot()?.state,
        ConnectionState::Reconnecting { cycle_id, .. } if cycle_id.as_u64() == 8))?;
    session.begin_closing()?;
    check_eq!(gate.wait_available(None).await?, None)?;
    check!(matches!(
        session.snapshot()?.state,
        ConnectionState::Closing
    ))?;
    Ok(())
}

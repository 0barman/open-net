use super::*;
use crate::module::net_status::inner::network_status_snapshot::NetworkStatusSnapshot;
use crate::module::net_status::NetworkStatus;
use tokio::sync::watch;

pub(super) struct NetworkGate {
    receiver: Option<watch::Receiver<NetworkStatusSnapshot>>,
    recovered_during_attempt: bool,
    observation: Option<(Arc<ConnectionSession>, u64)>,
}

impl NetworkGate {
    pub(super) fn new(
        receiver: Option<watch::Receiver<NetworkStatusSnapshot>>,
        observation: Option<(Arc<ConnectionSession>, u64)>,
    ) -> Self {
        Self {
            receiver,
            recovered_during_attempt: false,
            observation,
        }
    }

    fn publish_progress(&self, next_attempt_at: Option<std::time::Instant>) {
        let Some((session, cycle)) = &self.observation else {
            return;
        };
        // Inspect a clone so observation cannot consume the gate's watch change.
        let unavailable = self.receiver.as_ref().is_some_and(|receiver| {
            let mut receiver = receiver.clone();
            let unavailable =
                receiver.borrow_and_update().status == Some(NetworkStatus::Unavailable);
            unavailable
        });
        let result = if unavailable {
            session.waiting_for_network(*cycle)
        } else {
            session.preparing_attempt(*cycle, next_attempt_at)
        };
        if let Err(error) = result {
            // Observation failure never turns a valid transport operation into failure.
            crate::log_e!(LogType::WSC; "connection_state_progress", "cycle|kind", cycle, format!("{:?}", error.kind()));
        }
    }

    pub(super) async fn wait_available(
        &mut self,
        deadline: Option<BudgetDeadline>,
    ) -> Result<Option<u64>, NetError> {
        loop {
            if let Some(error) = deadline.and_then(BudgetDeadline::expired_error) {
                return Err(error);
            }
            let Some(receiver) = self.receiver.as_mut() else {
                self.publish_progress(None);
                return Ok(None);
            };
            let snapshot = *receiver.borrow_and_update();
            if snapshot.status != Some(NetworkStatus::Unavailable) {
                self.publish_progress(None);
                return Ok(Some(snapshot.loss_epoch));
            }
            // Pause mode requires a finite budget before a connect command is accepted.
            let deadline =
                deadline.ok_or(NetError::from(crate::error::ErrorKind::InvalidConfig))?;
            if let Some(error) = deadline.expired_error() {
                return Err(error);
            }
            self.publish_progress(None);
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline.at()) => return Err(deadline.error()),
                changed = async {
                    match self.receiver.as_mut() {
                        Some(receiver) => receiver.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_err() {
                        // A failed monitor supplies no reliable offline evidence. The
                        // transport's own timeouts remain authoritative in this fallback.
                        self.receiver = None;
                    }
                }
            }
        }
    }

    pub(super) async fn run<F: std::future::Future>(
        &mut self,
        epoch: Option<u64>,
        operation: F,
    ) -> Result<F::Output, NetError> {
        tokio::select! {
            biased;
            _ = self.interrupted(epoch) => Err(NetError::from(crate::error::ErrorKind::Io)),
            result = operation => Ok(result),
        }
    }

    pub(super) async fn run_until<F: std::future::Future>(
        &mut self,
        epoch: Option<u64>,
        deadline: Option<BudgetDeadline>,
        operation: F,
    ) -> Result<F::Output, NetError> {
        let expiry = async {
            match deadline {
                Some(deadline) => {
                    tokio::time::sleep_until(deadline.at()).await;
                    deadline.error()
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            error = expiry => Err(error),
            result = self.run(epoch, operation) => result,
        }
    }

    async fn interrupted(&mut self, epoch: Option<u64>) {
        let Some(epoch) = epoch else {
            return std::future::pending::<()>().await;
        };
        loop {
            let Some(receiver) = self.receiver.as_mut() else {
                return std::future::pending::<()>().await;
            };
            let snapshot = *receiver.borrow_and_update();
            if snapshot.loss_epoch != epoch || snapshot.status == Some(NetworkStatus::Unavailable) {
                self.recovered_during_attempt = snapshot.status != Some(NetworkStatus::Unavailable);
                return;
            }
            if receiver.changed().await.is_err() {
                self.receiver = None;
            }
        }
    }

    pub(super) async fn backoff(
        &mut self,
        delay: Duration,
        hint: &Notify,
        cycle_deadline: Option<BudgetDeadline>,
    ) -> Result<(), NetError> {
        if std::mem::take(&mut self.recovered_during_attempt) {
            self.publish_progress(None);
            return Ok(());
        }
        let delay_deadline = Instant::now()
            .checked_add(delay)
            .ok_or(NetError::from(crate::error::ErrorKind::InvalidConfig))?;
        let deadline =
            cycle_deadline.map_or(delay_deadline, |deadline| deadline.at().min(delay_deadline));
        self.publish_progress(Some(delay_deadline.into_std()));
        tokio::select! {
            _ = hint.notified() => {}
            _ = tokio::time::sleep_until(deadline) => {}
            _ = async {
                match self.receiver.as_mut() {
                    Some(receiver) => {
                        if receiver.changed().await.is_err() {
                            std::future::pending::<()>().await;
                        }
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {}
        }
        self.publish_progress(None);
        Ok(())
    }
}

#[cfg(test)]
#[path = "network_gate_observation_tests.rs"]
mod observation_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check_eq, TestResult};

    fn cycle_deadline(
        started_at: Instant,
        max_elapsed: Option<Duration>,
    ) -> Result<Option<BudgetDeadline>, NetError> {
        Ok(ConnectionBudget::new(started_at, None, max_elapsed)?.deadline())
    }

    #[tokio::test(start_paused = true)]
    async fn offline_wait_ends_at_the_original_cycle_deadline() -> TestResult {
        let (_sender, receiver) = watch::channel(NetworkStatusSnapshot {
            revision: 1,
            loss_epoch: 1,
            status: Some(NetworkStatus::Unavailable),
        });
        let mut gate = NetworkGate::new(Some(receiver), None);
        let started = Instant::now();
        let result = gate
            .wait_available(cycle_deadline(started, Some(Duration::from_secs(5)))?)
            .await;
        check_eq!(
            result,
            Err(NetError::from(crate::error::ErrorKind::RetryExhausted))
        )?;
        check_eq!(started.elapsed(), Duration::from_secs(5))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn coalesced_loss_prevents_even_a_ready_operation_from_running() -> TestResult {
        let (sender, receiver) = watch::channel(NetworkStatusSnapshot {
            revision: 1,
            loss_epoch: 0,
            status: Some(NetworkStatus::Available),
        });
        let mut gate = NetworkGate::new(Some(receiver), None);
        let epoch = gate.wait_available(None).await?;
        sender.send_replace(NetworkStatusSnapshot {
            revision: 3,
            loss_epoch: 1,
            status: Some(NetworkStatus::Available),
        });
        let mut called = false;
        let result = gate
            .run(epoch, async {
                called = true;
            })
            .await;
        check_eq!(result, Err(NetError::from(crate::error::ErrorKind::Io)))?;
        check_eq!(called, false)?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_preserves_the_loss_epoch_and_unknown_allows_transport_probe() -> TestResult {
        let (sender, receiver) = watch::channel(NetworkStatusSnapshot {
            revision: 1,
            loss_epoch: 7,
            status: Some(NetworkStatus::Unavailable),
        });
        let mut gate = NetworkGate::new(Some(receiver), None);
        let deadline = cycle_deadline(Instant::now(), Some(Duration::from_secs(5)))?;
        let publisher = async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            sender.send_replace(NetworkStatusSnapshot {
                revision: 2,
                loss_epoch: 7,
                status: None,
            });
            sender
        };
        let (result, _sender) = tokio::join!(gate.wait_available(deadline), publisher);
        check_eq!(result, Ok(Some(7)))?;
        Ok(())
    }
    #[tokio::test(start_paused = true)]
    async fn paused_retry_backoff_cannot_extend_the_cycle_budget() -> TestResult {
        let (_sender, receiver) = watch::channel(NetworkStatusSnapshot::default());
        let mut gate = NetworkGate::new(Some(receiver), None);
        let started = Instant::now();
        gate.backoff(
            Duration::from_secs(100),
            &Notify::new(),
            cycle_deadline(started, Some(Duration::from_secs(5)))?,
        )
        .await?;
        check_eq!(started.elapsed(), Duration::from_secs(5))?;
        Ok(())
    }

    #[tokio::test]
    async fn network_loss_cancels_a_pending_attempt_without_waiting_for_its_timeout() -> TestResult
    {
        let (sender, receiver) = watch::channel(NetworkStatusSnapshot {
            revision: 1,
            loss_epoch: 0,
            status: Some(NetworkStatus::Available),
        });
        let mut gate = NetworkGate::new(Some(receiver), None);
        let epoch = gate.wait_available(None).await?;
        let mut attempt = Box::pin(gate.run(epoch, std::future::pending::<()>()));
        check_eq!(futures::poll!(attempt.as_mut()), std::task::Poll::Pending)?;
        sender.send_replace(NetworkStatusSnapshot {
            revision: 2,
            loss_epoch: 1,
            status: Some(NetworkStatus::Unavailable),
        });
        check_eq!(
            futures::poll!(attempt.as_mut()),
            std::task::Poll::Ready(Err(NetError::from(crate::error::ErrorKind::Io)))
        )?;
        Ok(())
    }
    #[tokio::test(start_paused = true)]
    async fn available_network_cannot_admit_an_attempt_after_the_cycle_deadline() -> TestResult {
        let (_sender, receiver) = watch::channel(NetworkStatusSnapshot {
            revision: 1,
            loss_epoch: 0,
            status: Some(NetworkStatus::Available),
        });
        let mut gate = NetworkGate::new(Some(receiver), None);
        check_eq!(
            gate.wait_available(cycle_deadline(Instant::now(), Some(Duration::ZERO))?)
                .await,
            Err(NetError::from(crate::error::ErrorKind::RetryExhausted))
        )?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn ignored_network_cannot_admit_an_attempt_after_the_cycle_deadline() -> TestResult {
        let mut gate = NetworkGate::new(None, None);
        check_eq!(
            gate.wait_available(cycle_deadline(Instant::now(), Some(Duration::ZERO))?)
                .await,
            Err(NetError::from(crate::error::ErrorKind::RetryExhausted))
        )?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn ignored_network_capacity_and_backoff_waits_obey_the_same_deadline() -> TestResult {
        let started = Instant::now();
        let deadline = cycle_deadline(started, Some(Duration::from_secs(5)))?;
        let mut gate = NetworkGate::new(None, None);
        gate.backoff(Duration::from_secs(100), &Notify::new(), deadline)
            .await?;
        check_eq!(started.elapsed(), Duration::from_secs(5))?;
        let mut ran = false;
        let result = gate
            .run_until(None, deadline, async {
                ran = true;
            })
            .await;
        check_eq!(
            result,
            Err(NetError::from(crate::error::ErrorKind::RetryExhausted))
        )?;
        check_eq!(ran, false)?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn interrupted_capacity_wait_cannot_restart_after_the_cycle_deadline() -> TestResult {
        let (sender, receiver) = watch::channel(NetworkStatusSnapshot {
            revision: 1,
            loss_epoch: 0,
            status: Some(NetworkStatus::Available),
        });
        let mut gate = NetworkGate::new(Some(receiver), None);
        let deadline = cycle_deadline(Instant::now(), Some(Duration::from_secs(5)))?;
        let epoch = gate.wait_available(deadline).await?;
        {
            let mut reservation = Box::pin(gate.run(epoch, std::future::pending::<()>()));
            check_eq!(
                futures::poll!(reservation.as_mut()),
                std::task::Poll::Pending
            )?;
            sender.send_replace(NetworkStatusSnapshot {
                revision: 3,
                loss_epoch: 1,
                status: Some(NetworkStatus::Available),
            });
            check_eq!(
                reservation.await,
                Err(NetError::from(crate::error::ErrorKind::Io))
            )?;
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        check_eq!(
            gate.wait_available(deadline).await,
            Err(NetError::from(crate::error::ErrorKind::RetryExhausted))
        )?;
        Ok(())
    }

    #[test]
    fn unrepresentable_cycle_budget_is_a_configuration_error() -> TestResult {
        check_eq!(
            cycle_deadline(Instant::now(), Some(Duration::MAX)).map(|_| ()),
            Err(NetError::from(crate::error::ErrorKind::InvalidConfig))
        )?;
        check_eq!(cycle_deadline(Instant::now(), None)?.is_none(), true)?;
        Ok(())
    }
}

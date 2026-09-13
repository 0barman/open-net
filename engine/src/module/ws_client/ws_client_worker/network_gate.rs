use super::*;
use crate::module::net_status::inner::network_status_snapshot::NetworkStatusSnapshot;
use crate::module::net_status::NetworkStatus;
use tokio::sync::watch;

pub(super) fn cycle_deadline(
    started_at: Instant,
    max_elapsed: Option<Duration>,
) -> Result<Option<Instant>, NetError> {
    max_elapsed
        .map(|duration| {
            started_at
                .checked_add(duration)
                .ok_or(NetError::ConfigError)
        })
        .transpose()
}

pub(super) struct NetworkGate {
    receiver: Option<watch::Receiver<NetworkStatusSnapshot>>,
    recovered_during_attempt: bool,
    enabled: bool,
}

impl NetworkGate {
    pub(super) fn new(receiver: Option<watch::Receiver<NetworkStatusSnapshot>>) -> Self {
        Self {
            enabled: receiver.is_some(),
            receiver,
            recovered_during_attempt: false,
        }
    }

    pub(super) async fn wait_available(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<Option<u64>, NetError> {
        loop {
            if self.enabled && deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(NetError::RetryExhausted);
            }
            let Some(receiver) = self.receiver.as_mut() else {
                return Ok(None);
            };
            let snapshot = *receiver.borrow_and_update();
            if snapshot.status != Some(NetworkStatus::Unavailable) {
                return Ok(Some(snapshot.loss_epoch));
            }
            // Pause mode requires a finite budget before a connect command is accepted.
            let deadline = deadline.ok_or(NetError::ConfigError)?;
            if Instant::now() >= deadline {
                return Err(NetError::RetryExhausted);
            }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => return Err(NetError::RetryExhausted),
                changed = receiver.changed() => {
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
            _ = self.interrupted(epoch) => Err(NetError::NetworkError),
            result = operation => Ok(result),
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
        cycle_deadline: Option<Instant>,
    ) -> Result<(), NetError> {
        if std::mem::take(&mut self.recovered_during_attempt) {
            return Ok(());
        }
        let delay_deadline = Instant::now()
            .checked_add(delay)
            .ok_or(NetError::ConfigError)?;
        let deadline =
            cycle_deadline.map_or(delay_deadline, |deadline| deadline.min(delay_deadline));
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check_eq, TestResult};

    #[tokio::test(start_paused = true)]
    async fn offline_wait_ends_at_the_original_cycle_deadline() -> TestResult {
        let (_sender, receiver) = watch::channel(NetworkStatusSnapshot {
            revision: 1,
            loss_epoch: 1,
            status: Some(NetworkStatus::Unavailable),
        });
        let mut gate = NetworkGate::new(Some(receiver));
        let started = Instant::now();
        let result = gate
            .wait_available(started.checked_add(Duration::from_secs(5)))
            .await;
        check_eq!(result, Err(NetError::RetryExhausted))?;
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
        let mut gate = NetworkGate::new(Some(receiver));
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
        check_eq!(result, Err(NetError::NetworkError))?;
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
        let mut gate = NetworkGate::new(Some(receiver));
        let deadline = Instant::now().checked_add(Duration::from_secs(5));
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
        let mut gate = NetworkGate::new(Some(receiver));
        let started = Instant::now();
        gate.backoff(
            Duration::from_secs(100),
            &Notify::new(),
            started.checked_add(Duration::from_secs(5)),
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
        let mut gate = NetworkGate::new(Some(receiver));
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
            std::task::Poll::Ready(Err(NetError::NetworkError))
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
        let mut gate = NetworkGate::new(Some(receiver));
        check_eq!(
            gate.wait_available(Some(Instant::now())).await,
            Err(NetError::RetryExhausted)
        )?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn interrupted_capacity_wait_cannot_restart_after_the_cycle_deadline() -> TestResult {
        let (sender, receiver) = watch::channel(NetworkStatusSnapshot {
            revision: 1,
            loss_epoch: 0,
            status: Some(NetworkStatus::Available),
        });
        let mut gate = NetworkGate::new(Some(receiver));
        let deadline = Instant::now().checked_add(Duration::from_secs(5));
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
            check_eq!(reservation.await, Err(NetError::NetworkError))?;
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        check_eq!(
            gate.wait_available(deadline).await,
            Err(NetError::RetryExhausted)
        )?;
        Ok(())
    }

    #[test]
    fn unrepresentable_cycle_budget_is_a_configuration_error() -> TestResult {
        check_eq!(
            cycle_deadline(Instant::now(), Some(Duration::MAX)),
            Err(NetError::ConfigError)
        )?;
        check_eq!(cycle_deadline(Instant::now(), None), Ok(None))?;
        Ok(())
    }
}

//! Local lifecycle and source revision are deliberately separate counters.
use crate::error::{ErrorKind, NetError};
use crate::net_status::{MonitorState, NetworkSnapshot};

#[derive(Clone)]
pub(super) struct NetworkView {
    pub(super) snapshot: NetworkSnapshot,
    pub(super) generation: u64,
    pub(super) active: bool,
    source_revision: Option<u64>,
    projected_source_revision: Option<u64>,
}

impl NetworkView {
    pub(super) fn new() -> Self {
        Self {
            snapshot: NetworkSnapshot {
                revision: 0,
                loss_epoch: 0,
                state: MonitorState::Stopped,
                reachability: None,
                ip_stack: None,
                observed_at: None,
                network_name: None,
            },
            generation: 0,
            active: false,
            source_revision: None,
            projected_source_revision: None,
        }
    }
    pub(super) fn closed(&self) -> bool {
        matches!(self.snapshot.state, MonitorState::Closed)
    }
    pub(super) fn activate(&mut self, source: NetworkSnapshot) -> Result<bool, NetError> {
        if self.closed() || matches!(source.state, MonitorState::Closed) {
            return Err(NetError::from(ErrorKind::Closed));
        }
        let mut next = self.clone();
        next.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
        next.active = true;
        let changed = next.import(source, true)?;
        *self = next;
        Ok(changed)
    }
    pub(super) fn import(
        &mut self,
        mut source: NetworkSnapshot,
        force: bool,
    ) -> Result<bool, NetError> {
        if self.closed() {
            return Ok(false);
        }
        if matches!(source.state, MonitorState::Closed) {
            return self.stop(true);
        }
        if !force
            && (!self.active
                || self
                    .source_revision
                    .is_some_and(|revision| source.revision <= revision))
        {
            return Ok(false);
        }
        // A lifecycle reactivation may import the current source twice, but it
        // never permits an older queued bridge event to roll facts backwards.
        if self
            .source_revision
            .is_some_and(|revision| source.revision < revision)
        {
            return Ok(false);
        }
        let revision = self
            .snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
        self.source_revision = Some(source.revision);
        self.projected_source_revision = Some(source.revision);
        source.revision = revision;
        self.snapshot = source;
        Ok(true)
    }
    pub(super) fn refresh(&mut self, source: NetworkSnapshot) -> Result<bool, NetError> {
        if self.projected_source_revision == Some(source.revision)
            && !matches!(source.state, MonitorState::Closed)
        {
            return Ok(false);
        }
        self.import(source, true)
    }
    pub(super) fn stop(&mut self, permanent: bool) -> Result<bool, NetError> {
        if self.closed()
            || (!permanent && !self.active && matches!(self.snapshot.state, MonitorState::Stopped))
        {
            return Ok(false);
        }
        let revision = self
            .snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
        self.active = false;
        self.projected_source_revision = None;
        self.snapshot = NetworkSnapshot {
            revision,
            loss_epoch: self.snapshot.loss_epoch,
            state: if permanent {
                MonitorState::Closed
            } else {
                MonitorState::Stopped
            },
            reachability: None,
            ip_stack: None,
            observed_at: None,
            network_name: None,
        };
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net_status::{IpStack, NetworkStatus};
    use std::time::{Duration, SystemTime};
    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn check(value: bool, message: &str) -> TestResult {
        if value {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }
    fn facts(revision: u64, epoch: u64) -> NetworkSnapshot {
        NetworkSnapshot {
            revision,
            loss_epoch: epoch,
            state: MonitorState::Running,
            reachability: Some(NetworkStatus::Available),
            ip_stack: Some(IpStack::DualStack),
            observed_at: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(revision)),
            network_name: Some(format!("source-{revision}")),
        }
    }
    #[test]
    fn delayed_initial_never_replaces_successful_start_facts() -> TestResult {
        let mut view = NetworkView::new();
        view.activate(facts(8, 2))?;
        let latest = view.snapshot.clone();
        let mut stale = facts(7, 0);
        stale.state = MonitorState::Starting;
        view.import(stale, false)?;
        check(
            matches!(view.snapshot.state, MonitorState::Running),
            "successful start did not commit Running",
        )?;
        check(
            view.snapshot.revision == latest.revision
                && view.snapshot.network_name == latest.network_name,
            "old initial overwrote current facts",
        )
    }
    #[test]
    fn merged_loss_history_and_observation_time_are_copied_exactly() -> TestResult {
        let mut view = NetworkView::new();
        view.activate(facts(1, 0))?;
        let source = facts(4, 2);
        view.import(source.clone(), false)?;
        check(
            view.snapshot.loss_epoch == 2
                && view.snapshot.observed_at == source.observed_at
                && view.snapshot.ip_stack == source.ip_stack
                && view.snapshot.network_name == source.network_name,
            "bridge reconstructed instead of importing complete source facts",
        )
    }
    #[test]
    fn restart_accepts_current_source_without_resetting_watermark() -> TestResult {
        let mut view = NetworkView::new();
        view.activate(facts(10, 3))?;
        view.stop(false)?;
        view.activate(facts(10, 3))?;
        view.import(facts(9, 1), false)?;
        check(
            matches!(view.snapshot.state, MonitorState::Running)
                && view.snapshot.loss_epoch == 3
                && view.source_revision == Some(10),
            "restart lost source watermark",
        )
    }
    #[test]
    fn local_stop_and_closed_never_manufacture_network_loss() -> TestResult {
        let mut view = NetworkView::new();
        view.activate(facts(3, 2))?;
        view.stop(false)?;
        check(
            view.snapshot.loss_epoch == 2 && view.snapshot.reachability.is_none(),
            "local stop altered network loss",
        )?;
        view.stop(true)?;
        view.import(facts(7, 3), true)?;
        check(
            matches!(view.snapshot.state, MonitorState::Closed) && view.snapshot.loss_epoch == 2,
            "closed view revived",
        )
    }
}

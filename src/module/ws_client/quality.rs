//! Passive WS observation ownership. Allocated only for an activated doctor.
use crate::api::quality::{PhaseKind, QualityOutcome};
use crate::module::quality::QualityProbe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Unwinding/dropped futures are unknown observations, never inferred cancellation.
pub(crate) struct QualityScope {
    pub(crate) probe: Option<QualityProbe>,
    pub(crate) outcome: QualityOutcome,
}
impl QualityScope {
    pub(crate) fn new(probe: Option<QualityProbe>) -> Self {
        Self {
            probe,
            outcome: QualityOutcome::Unknown,
        }
    }
}
impl Drop for QualityScope {
    fn drop(&mut self) {
        if let Some(probe) = &self.probe {
            probe.result(self.outcome);
            probe.finish();
        }
    }
}

pub(crate) struct QualityOperation {
    pub(crate) probe: QualityProbe,
    active_scopes: AtomicUsize,
    published: AtomicBool,
}

pub(crate) struct QualityPublication {
    pub(crate) observation: Arc<QualityOperation>,
    pub(crate) outcome: QualityOutcome,
    pub(crate) failure: Option<(crate::error::ErrorKind, Option<crate::error::ErrorStage>)>,
    pub(crate) deferred: bool,
}
impl QualityPublication {
    pub(crate) fn select(&self) {
        match self.failure {
            Some((kind, stage)) => self.observation.probe.failure(kind, stage),
            None => self.observation.probe.result(self.outcome),
        }
        self.observation.probe.mark(PhaseKind::ResultSelected);
    }
    pub(crate) fn publish(self) {
        if !self.deferred {
            self.observation.published();
        }
    }
}

impl QualityOperation {
    pub(crate) fn new(probe: QualityProbe) -> Arc<Self> {
        Arc::new(Self {
            probe,
            active_scopes: AtomicUsize::new(0),
            published: AtomicBool::new(false),
        })
    }
    pub(crate) fn observe_scope(self: &Arc<Self>) -> QualityActivityGuard {
        self.active_scopes.fetch_add(1, Ordering::AcqRel);
        QualityActivityGuard(self.clone())
    }
    pub(crate) fn published(&self) {
        self.probe.mark(PhaseKind::DeliveryFinished);
        self.published.store(true, Ordering::Release);
        if self.active_scopes.load(Ordering::Acquire) == 0 {
            self.probe.finish();
        }
    }
}

pub(crate) struct QualityActivityGuard(Arc<QualityOperation>);
impl Drop for QualityActivityGuard {
    fn drop(&mut self) {
        if self.0.active_scopes.fetch_sub(1, Ordering::AcqRel) == 1
            && self.0.published.load(Ordering::Acquire)
        {
            self.0.probe.finish();
        }
    }
}

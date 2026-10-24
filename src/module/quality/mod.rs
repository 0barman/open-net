//! Taskless diagnostic storage. Network adapters only enqueue bounded facts.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock, Weak,
    },
    time::Duration,
};

use futures_util::FutureExt;
use tokio::{sync::mpsc, time::Instant};

use crate::api::quality::*;

#[cfg(test)]
mod performance_tests;
#[cfg(test)]
mod tests;

static NEXT_SCOPE: AtomicU64 = AtomicU64::new(1);
const MAX_DROP_SUMMARIES: usize = 64;

fn next_id(counter: &AtomicU64) -> Option<u64> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .ok()
}
fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}
fn add(value: &mut u64, amount: u64, saturated: &mut bool) {
    let (next, overflow) = value.overflowing_add(amount);
    *value = if overflow { u64::MAX } else { next };
    *saturated |= overflow;
}

/// The only strong owner of configuration. Contexts never keep networking alive.
#[derive(Clone, Default)]
pub(crate) struct QualityOwner {
    inner: Option<Arc<OwnerInner>>,
}
struct OwnerInner {
    config: QualityDoctorConfig,
    collector: OnceLock<Arc<Collector>>,
    closed: AtomicBool,
    clients: AtomicU64,
}
impl QualityOwner {
    pub(crate) fn new(config: QualityDoctorConfig) -> Self {
        if config.mode == QualityMode::Off {
            return Self::default();
        }
        Self {
            inner: Some(Arc::new(OwnerInner {
                config,
                collector: OnceLock::new(),
                closed: AtomicBool::new(false),
                clients: AtomicU64::new(1),
            })),
        }
    }
    pub(crate) fn context(&self) -> QualityContext {
        QualityContext {
            owner: self.inner.as_ref().map_or_else(Weak::new, Arc::downgrade),
        }
    }
    pub(crate) fn doctor(&self) -> Option<QualityDoctor> {
        self.inner.as_ref().map(|owner| owner.doctor())
    }
    pub(crate) fn close(&self) {
        if let Some(owner) = &self.inner {
            owner.close();
        }
    }
}
impl OwnerInner {
    fn doctor(&self) -> QualityDoctor {
        let collector = self
            .collector
            .get_or_init(|| Arc::new(Collector::new(self.config.clone())))
            .clone();
        if self.closed.load(Ordering::Acquire) {
            collector.close();
        }
        QualityDoctor { inner: collector }
    }
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        if let Some(collector) = self.collector.get() {
            collector.close();
        }
    }
}
impl Drop for OwnerInner {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Clone, Default)]
pub(crate) struct QualityContext {
    owner: Weak<OwnerInner>,
}
impl QualityContext {
    pub(crate) fn doctor(&self) -> Option<QualityDoctor> {
        self.owner.upgrade().map(|owner| owner.doctor())
    }
    pub(crate) fn close(&self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.close();
        }
    }
    /// Configuration only; does not activate storage or read a clock.
    pub(crate) fn is_enabled_for(&self, protocol: QualityProtocol) -> bool {
        self.owner.upgrade().is_some_and(|owner| {
            !owner.closed.load(Ordering::Relaxed) && owner.config.protocols.includes(protocol)
        })
    }
    pub(crate) fn allocate_client_id(&self) -> u64 {
        self.owner
            .upgrade()
            .and_then(|owner| next_id(&owner.clients))
            .unwrap_or(0)
    }
    pub(crate) fn begin(
        &self,
        protocol: QualityProtocol,
        kind: QualityOperationKind,
        client_id: u64,
    ) -> Option<QualityProbe> {
        let owner = self.owner.upgrade()?;
        // No clock, allocation, buffer initialization or task creation before this gate.
        if owner.closed.load(Ordering::Relaxed) || !owner.config.protocols.includes(protocol) {
            return None;
        }
        let collector = owner.collector.get()?;
        if collector.state() != QualityDoctorState::Running {
            return None;
        }
        increment(&collector.begin_attempted);
        if collector
            .active_probes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < collector.config.limits.active as u64).then_some(count + 1)
            })
            .is_err()
        {
            increment(&collector.active_capacity_skipped);
            return None;
        }
        let Some(id) = next_id(&collector.next_record) else {
            increment(&collector.id_exhausted);
            collector.active_probes.fetch_sub(1, Ordering::Relaxed);
            return None;
        };
        let rate = if kind == QualityOperationKind::Connection {
            collector.config.sampling.connections
        } else {
            collector.config.sampling.operations
        };
        let detailed =
            collector.config.mode == QualityMode::Detailed && sampled(collector.scope, id, rate);
        let reference = QualityReportRef {
            scope: collector.scope,
            id,
            kind,
            detailed,
        };
        let at = collector.now();
        if !collector.emit(Event {
            reference,
            at,
            bytes: 0,
            bytes_saturated: false,
            phase_overflow: 0,
            kind: EventKind::Begin {
                protocol,
                client_id,
            },
        }) {
            collector.active_probes.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        increment(&collector.begin_enqueued);
        Some(QualityProbe {
            lease: Arc::new(ProbeLease {
                collector: collector.clone(),
                reference,
                fields: ProbeFields::default(),
                bytes: AtomicU64::new(0),
                bytes_saturated: AtomicBool::new(false),
                phase_events: AtomicU64::new(0),
                phase_overflow: AtomicU64::new(0),
                finished: AtomicBool::new(false),
            }),
        })
    }
}

fn sampled(scope: u64, id: u64, rate: f64) -> bool {
    if rate <= 0.0 {
        return false;
    }
    if rate >= 1.0 {
        return true;
    }
    let mut value = id ^ scope.wrapping_mul(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^= value >> 31;
    (value >> 11) as f64 / ((1u64 << 53) as f64) < rate
}

#[derive(Default)]
struct ProbeFields {
    attempt: AtomicU64,
    hop: AtomicU64,
    connection: AtomicU64,
    session: AtomicU64,
    cycle: AtomicU64,
    operation: AtomicU64,
}
impl ProbeFields {
    fn snapshot(&self) -> QualityAssociations {
        fn value(field: &AtomicU64) -> Option<u64> {
            field.load(Ordering::Relaxed).checked_sub(1)
        }
        QualityAssociations {
            attempt: value(&self.attempt),
            hop: value(&self.hop),
            connection: value(&self.connection),
            session: value(&self.session),
            cycle: value(&self.cycle),
            operation: value(&self.operation),
            parent: None,
        }
    }
}

/// The enabled path reserves one bounded shared lease per observed operation.
/// Its final drop, including outstanding phase guards, releases the reservation.
#[derive(Clone)]
pub(crate) struct QualityProbe {
    lease: Arc<ProbeLease>,
}
struct ProbeLease {
    collector: Arc<Collector>,
    reference: QualityReportRef,
    fields: ProbeFields,
    bytes: AtomicU64,
    bytes_saturated: AtomicBool,
    phase_events: AtomicU64,
    phase_overflow: AtomicU64,
    finished: AtomicBool,
}
impl ProbeLease {
    fn reserve_phase(&self) -> Option<u64> {
        if self.finished.load(Ordering::Relaxed) {
            return None;
        }
        match self
            .phase_events
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < self.collector.config.limits.phases as u64).then_some(count + 1)
            }) {
            Ok(previous) => previous.checked_add(1),
            Err(_) => {
                increment(&self.phase_overflow);
                None
            }
        }
    }
}
impl Drop for ProbeLease {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Relaxed) {
            self.collector.active_probes.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
impl QualityProbe {
    pub(crate) fn reference(&self) -> QualityReportRef {
        self.lease.reference
    }
    pub(crate) fn phase(&self, phase: PhaseKind) -> QualityPhaseGuard {
        let instance_id = (self.lease.reference.detailed || !detail_phase(phase))
            .then(|| self.lease.reserve_phase())
            .flatten();
        let started = if instance_id.is_some() {
            self.lease.collector.now()
        } else {
            Duration::ZERO
        };
        let fields = self.lease.fields.snapshot();
        if let Some(instance_id) = instance_id.filter(|_| self.lease.reference.detailed) {
            self.emit_at(
                EventKind::PhaseStarted {
                    instance_id,
                    phase,
                    fields: fields.clone(),
                },
                started,
            );
        }
        QualityPhaseGuard {
            lease: self.lease.clone(),
            instance_id: instance_id.unwrap_or(0),
            phase,
            started,
            fields,
            finished: instance_id.is_none(),
        }
    }
    pub(crate) fn mark(&self, phase: PhaseKind) {
        if !self.lease.reference.detailed && detail_phase(phase) {
            return;
        }
        let Some(instance_id) = self.lease.reserve_phase() else {
            return;
        };
        self.emit(EventKind::Phase {
            instance_id,
            phase,
            started: None,
            fields: self.lease.fields.snapshot(),
        });
    }
    /// Byte accounting has no clock read or ingress event per chunk/frame.
    pub(crate) fn bytes(&self, count: u64) {
        if count == 0 || self.lease.finished.load(Ordering::Relaxed) {
            return;
        }
        let previous = self
            .lease
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(count))
            })
            .unwrap_or(u64::MAX);
        if previous.checked_add(count).is_none() {
            self.lease.bytes_saturated.store(true, Ordering::Relaxed);
        }
    }
    pub(crate) fn result(&self, outcome: QualityOutcome) {
        self.emit(EventKind::Result {
            outcome,
            failure: None,
            http_status: None,
        });
    }
    #[cfg(feature = "http-client")]
    pub(crate) fn http_result(&self, status: u16) {
        self.emit(EventKind::Result {
            outcome: QualityOutcome::Success,
            failure: None,
            http_status: Some(status),
        });
    }
    #[cfg(feature = "http-client")]
    pub(crate) fn http_version(&self, version: http::Version) {
        let version = match version {
            http::Version::HTTP_09 => QualityHttpVersion::Http09,
            http::Version::HTTP_10 => QualityHttpVersion::Http10,
            http::Version::HTTP_11 => QualityHttpVersion::Http11,
            http::Version::HTTP_2 => QualityHttpVersion::Http2,
            http::Version::HTTP_3 => QualityHttpVersion::Http3,
            _ => QualityHttpVersion::Unknown,
        };
        self.emit(EventKind::HttpVersion(version));
    }
    pub(crate) fn error(&self, error: &crate::api::error::NetError) {
        self.failure(error.kind(), error.context().stage);
    }
    pub(crate) fn failure(
        &self,
        kind: crate::api::error::ErrorKind,
        stage: Option<crate::api::error::ErrorStage>,
    ) {
        use crate::api::error::ErrorKind;
        let outcome = match kind {
            ErrorKind::Cancelled => QualityOutcome::Cancelled,
            ErrorKind::TimedOut | ErrorKind::DeadlineExceeded => QualityOutcome::Timeout,
            _ => QualityOutcome::Failed,
        };
        self.emit(EventKind::Result {
            outcome,
            failure: Some(QualityFailure { kind, stage }),
            http_status: None,
        });
    }
    /// Final observation boundary. The consumer ignores duplicates and later facts.
    pub(crate) fn finish(&self) {
        if self
            .lease
            .finished
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            self.emit_open(EventKind::Finish);
            self.lease
                .collector
                .active_probes
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
    fn associate(&self, field: &AtomicU64, key: Association, value: u64) {
        if let Some(encoded) = value.checked_add(1) {
            field.store(encoded, Ordering::Relaxed);
        }
        self.emit(EventKind::Association(key, value));
    }
    pub(crate) fn attempt(&self, value: u64) {
        self.associate(&self.lease.fields.attempt, Association::Attempt, value);
    }
    pub(crate) fn hop(&self, value: u64) {
        self.associate(&self.lease.fields.hop, Association::Hop, value);
    }
    pub(crate) fn connection(&self, value: u64) {
        self.associate(
            &self.lease.fields.connection,
            Association::Connection,
            value,
        );
    }
    pub(crate) fn session(&self, value: u64) {
        self.associate(&self.lease.fields.session, Association::Session, value);
    }
    pub(crate) fn cycle(&self, value: u64) {
        self.associate(&self.lease.fields.cycle, Association::Cycle, value);
    }
    pub(crate) fn operation(&self, value: u64) {
        self.associate(&self.lease.fields.operation, Association::Operation, value);
    }
    pub(crate) fn parent(&self, value: QualityReportRef) {
        self.emit(EventKind::Parent(value));
    }
    fn emit(&self, kind: EventKind) {
        if self.lease.finished.load(Ordering::Relaxed) {
            return;
        }
        self.emit_open(kind);
    }
    fn emit_open(&self, kind: EventKind) {
        self.emit_at(kind, self.lease.collector.now());
    }
    fn emit_at(&self, kind: EventKind, at: Duration) {
        self.lease.collector.emit(Event {
            reference: self.lease.reference,
            at,
            bytes: self.lease.bytes.load(Ordering::Relaxed),
            bytes_saturated: self.lease.bytes_saturated.load(Ordering::Relaxed),
            phase_overflow: self.lease.phase_overflow.load(Ordering::Relaxed),
            kind,
        });
    }
}
fn detail_phase(phase: PhaseKind) -> bool {
    matches!(
        phase,
        PhaseKind::TcpCandidate
            | PhaseKind::RequestGetters
            | PhaseKind::ResponseMatched
            | PhaseKind::ManualDispatch
            | PhaseKind::ManualResolve
    )
}

pub(crate) struct QualityPhaseGuard {
    lease: Arc<ProbeLease>,
    instance_id: u64,
    phase: PhaseKind,
    started: Duration,
    fields: QualityAssociations,
    finished: bool,
}
impl QualityPhaseGuard {
    pub(crate) fn finish(mut self) {
        self.complete();
    }
    fn complete(&mut self) {
        if !self.finished {
            self.finished = true;
            if !self.lease.finished.load(Ordering::Relaxed) {
                self.lease.collector.emit(Event {
                    reference: self.lease.reference,
                    at: self.lease.collector.now(),
                    bytes: self.lease.bytes.load(Ordering::Relaxed),
                    bytes_saturated: self.lease.bytes_saturated.load(Ordering::Relaxed),
                    phase_overflow: self.lease.phase_overflow.load(Ordering::Relaxed),
                    kind: EventKind::Phase {
                        instance_id: self.instance_id,
                        phase: self.phase,
                        started: Some(self.started),
                        fields: self.fields.clone(),
                    },
                });
            }
        }
    }
}
impl Drop for QualityPhaseGuard {
    fn drop(&mut self) {
        self.complete();
    }
}

#[derive(Clone, Copy)]
enum Association {
    Attempt,
    Hop,
    Connection,
    Session,
    Cycle,
    Operation,
}
struct Event {
    reference: QualityReportRef,
    at: Duration,
    bytes: u64,
    bytes_saturated: bool,
    phase_overflow: u64,
    kind: EventKind,
}
impl Event {
    fn loss_summary(&self, reason: QualityDropReason) -> QualityDropSummary {
        let (event_kind, phase_kind, phase_instance_id) = match &self.kind {
            EventKind::Begin { .. } => (QualityEventKind::Begin, None, None),
            EventKind::PhaseStarted {
                instance_id, phase, ..
            } => (
                QualityEventKind::PhaseStarted,
                Some(*phase),
                Some(*instance_id),
            ),
            EventKind::Phase {
                instance_id,
                phase,
                started,
                ..
            } => (
                if started.is_some() {
                    QualityEventKind::PhaseEnded
                } else {
                    QualityEventKind::Milestone
                },
                Some(*phase),
                Some(*instance_id),
            ),
            EventKind::Result { .. } => (QualityEventKind::Result, None, None),
            EventKind::Association(..) => (QualityEventKind::Association, None, None),
            EventKind::Parent(_) => (QualityEventKind::Parent, None, None),
            EventKind::Finish => (QualityEventKind::Finish, None, None),
            #[cfg(feature = "http-client")]
            EventKind::HttpVersion(_) => (QualityEventKind::HttpVersion, None, None),
        };
        QualityDropSummary {
            reference: self.reference,
            event_kind,
            phase_kind,
            phase_instance_id,
            reason,
            event_offset: self.at,
        }
    }
}
enum EventKind {
    Begin {
        protocol: QualityProtocol,
        client_id: u64,
    },
    Phase {
        instance_id: u64,
        phase: PhaseKind,
        started: Option<Duration>,
        fields: QualityAssociations,
    },
    PhaseStarted {
        instance_id: u64,
        phase: PhaseKind,
        fields: QualityAssociations,
    },
    #[cfg(feature = "http-client")]
    HttpVersion(QualityHttpVersion),
    Result {
        outcome: QualityOutcome,
        failure: Option<QualityFailure>,
        http_status: Option<u16>,
    },
    Association(Association, u64),
    Parent(QualityReportRef),
    Finish,
}

pub(crate) struct Collector {
    config: QualityDoctorConfig,
    scope: u64,
    epoch: Instant,
    closed: AtomicBool,
    failed: AtomicBool,
    next_record: AtomicU64,
    ingress: Mutex<VecDeque<Event>>,
    drop_summaries: Mutex<VecDeque<QualityDropSummary>>,
    store: Mutex<Store>,
    begin_attempted: AtomicU64,
    begin_enqueued: AtomicU64,
    ingress_dropped: AtomicU64,
    ingress_lock_contended_dropped: AtomicU64,
    ingress_queue_full_dropped: AtomicU64,
    ingress_closed_rejected: AtomicU64,
    ingress_failed_rejected: AtomicU64,
    ingress_high_watermark: AtomicU64,
    drop_summaries_lost: AtomicU64,
    active_probes: AtomicU64,
    active_capacity_skipped: AtomicU64,
    id_exhausted: AtomicU64,
    capabilities: Arc<QualityCapabilities>,
}
struct Store {
    records: HashMap<u64, QualityReport>,
    order: VecDeque<u64>,
    tombstones: VecDeque<u64>,
    revision: u64,
    counters: QualityCounters,
    recent_drops: VecDeque<QualityDropSummary>,
    subscribers: Vec<Subscriber>,
    next_subscriber: u64,
    closed: bool,
    batch_gap: bool,
    carry_gap: bool,
}
struct Subscriber {
    id: u64,
    sender: mpsc::Sender<QualityUpdate>,
    options: QualitySubscriptionOptions,
    dropped: u64,
}

impl Collector {
    fn new(config: QualityDoctorConfig) -> Self {
        let invalid = config.validate().is_err();
        // Invalid internal callers cannot turn bad capacities into unbounded allocation.
        let config = if invalid {
            QualityDoctorConfig::basic()
        } else {
            config
        };
        let scope = next_id(&NEXT_SCOPE).unwrap_or(0);
        let records = config.limits.active + config.limits.history;
        Self {
            capabilities: Arc::new(capabilities(&config)),
            ingress: Mutex::new(VecDeque::with_capacity(config.limits.ingress)),
            drop_summaries: Mutex::new(VecDeque::with_capacity(
                MAX_DROP_SUMMARIES.min(config.limits.ingress),
            )),
            store: Mutex::new(Store {
                records: HashMap::with_capacity(records),
                order: VecDeque::with_capacity(records),
                tombstones: VecDeque::with_capacity(config.limits.history),
                revision: 0,
                counters: QualityCounters::default(),
                recent_drops: VecDeque::with_capacity(
                    MAX_DROP_SUMMARIES.min(config.limits.ingress),
                ),
                subscribers: Vec::with_capacity(config.limits.subscriptions),
                next_subscriber: 1,
                closed: false,
                batch_gap: false,
                carry_gap: false,
            }),
            config,
            scope,
            epoch: Instant::now(),
            closed: AtomicBool::new(false),
            failed: AtomicBool::new(invalid || scope == 0),
            next_record: AtomicU64::new(1),
            begin_attempted: AtomicU64::new(0),
            begin_enqueued: AtomicU64::new(0),
            ingress_dropped: AtomicU64::new(0),
            ingress_lock_contended_dropped: AtomicU64::new(0),
            ingress_queue_full_dropped: AtomicU64::new(0),
            ingress_closed_rejected: AtomicU64::new(0),
            ingress_failed_rejected: AtomicU64::new(0),
            ingress_high_watermark: AtomicU64::new(0),
            drop_summaries_lost: AtomicU64::new(0),
            id_exhausted: AtomicU64::new(0),
            active_probes: AtomicU64::new(0),
            active_capacity_skipped: AtomicU64::new(0),
        }
    }
    fn state(&self) -> QualityDoctorState {
        if self.failed.load(Ordering::Acquire) {
            QualityDoctorState::Failed
        } else if self.closed.load(Ordering::Acquire) {
            QualityDoctorState::Closed
        } else {
            QualityDoctorState::Running
        }
    }
    fn now(&self) -> Duration {
        Instant::now().saturating_duration_since(self.epoch)
    }
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
    fn fail(&self) {
        self.failed.store(true, Ordering::Release);
    }
    fn emit(&self, event: Event) -> bool {
        if self.reject_inactive() {
            return false;
        }
        match self.ingress.try_lock() {
            Ok(mut queue) => {
                if self.reject_inactive() {
                    false
                } else if queue.len() == self.config.limits.ingress {
                    increment(&self.ingress_queue_full_dropped);
                    increment(&self.ingress_dropped);
                    self.record_loss(&event, QualityDropReason::QueueFull);
                    false
                } else {
                    queue.push_back(event);
                    self.ingress_high_watermark
                        .fetch_max(queue.len() as u64, Ordering::Relaxed);
                    true
                }
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                increment(&self.ingress_lock_contended_dropped);
                increment(&self.ingress_dropped);
                self.record_loss(&event, QualityDropReason::LockContended);
                false
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                self.fail();
                increment(&self.ingress_failed_rejected);
                false
            }
        }
    }
    fn reject_inactive(&self) -> bool {
        match self.state() {
            QualityDoctorState::Running => false,
            QualityDoctorState::Closed => {
                increment(&self.ingress_closed_rejected);
                true
            }
            _ => {
                increment(&self.ingress_failed_rejected);
                true
            }
        }
    }
    fn record_loss(&self, event: &Event, reason: QualityDropReason) {
        // A single independent try_lock, no re-enqueue, retry, allocation or wait.
        if let Ok(mut summaries) = self.drop_summaries.try_lock() {
            if summaries.len() < MAX_DROP_SUMMARIES.min(self.config.limits.ingress) {
                summaries.push_back(event.loss_summary(reason));
                return;
            }
        }
        increment(&self.drop_summaries_lost);
    }
    /// All allocation, history maintenance, classification and subscriber fanout
    /// occurs on the caller's diagnostic read, never on the network path.
    fn drive(&self) {
        // Serialize drain and apply together: concurrent readers must not apply
        // a later event batch before an earlier Begin batch.
        let mut store = match self.store.lock() {
            Ok(store) => store,
            Err(_) => {
                self.fail();
                return;
            }
        };
        let events = match self.ingress.lock() {
            Ok(mut queue) => queue.drain(..).collect::<Vec<_>>(),
            Err(_) => {
                self.fail();
                // Still publish safe counters and terminate retained observations.
                // A poisoned queue cannot hide its rejection reason forever.
                Vec::new()
            }
        };
        // Loss evidence is advisory. A poisoned summary queue cannot fail the
        // collector, erase aggregate loss, or affect any network operation.
        let loss_summaries = self
            .drop_summaries
            .lock()
            .map(|mut queue| queue.drain(..).collect::<Vec<_>>())
            .unwrap_or_default();
        let now = self.now();
        let initial_revision = store.revision;
        let previous_counters = store.counters.clone();
        let previous_drops = store.counters.ingress_dropped;
        store.counters.ingress_dropped = self.ingress_dropped.load(Ordering::Relaxed);
        store.counters.ingress_lock_contended_dropped =
            self.ingress_lock_contended_dropped.load(Ordering::Relaxed);
        store.counters.ingress_queue_full_dropped =
            self.ingress_queue_full_dropped.load(Ordering::Relaxed);
        store.counters.ingress_closed_rejected =
            self.ingress_closed_rejected.load(Ordering::Relaxed);
        store.counters.ingress_failed_rejected =
            self.ingress_failed_rejected.load(Ordering::Relaxed);
        store.counters.ingress_high_watermark = self.ingress_high_watermark.load(Ordering::Relaxed);
        store.counters.drop_summaries_lost = self.drop_summaries_lost.load(Ordering::Relaxed);
        store.counters.begin_attempted = self.begin_attempted.load(Ordering::Relaxed);
        store.counters.begin_enqueued = self.begin_enqueued.load(Ordering::Relaxed);
        store.counters.id_exhausted = self.id_exhausted.load(Ordering::Relaxed);
        store.counters.active_capacity_skipped =
            self.active_capacity_skipped.load(Ordering::Relaxed);
        store.counters.saturated |= [
            store.counters.ingress_dropped,
            store.counters.ingress_lock_contended_dropped,
            store.counters.ingress_queue_full_dropped,
            store.counters.ingress_closed_rejected,
            store.counters.ingress_failed_rejected,
            store.counters.drop_summaries_lost,
            store.counters.begin_attempted,
            store.counters.begin_enqueued,
            store.counters.id_exhausted,
            store.counters.active_capacity_skipped,
        ]
        .contains(&u64::MAX);
        if previous_counters != store.counters {
            store.revision();
        }
        let new_drops = store
            .counters
            .ingress_dropped
            .saturating_sub(previous_drops);
        // Counters are sampled after draining ingress. An accepted Begin can
        // therefore remain in the next batch while a later loss is already
        // counted here, even if its best-effort summary was also lost. Carry
        // this gap across exactly one drain so that Begin cannot look complete.
        store.batch_gap = new_drops != 0 || store.carry_gap;
        store.carry_gap = new_drops != 0;
        let mut updated = Vec::with_capacity(
            events
                .len()
                .min(self.config.limits.active + self.config.limits.history),
        );
        if new_drops != 0 {
            store.revision();
            let affected = store
                .records
                .iter()
                .filter(|(_, report)| {
                    report.state == QualityObservationState::InProgress
                        && !report.possibly_incomplete
                })
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            for id in affected {
                let revision = store.revision();
                if let Some(report) = store.records.get_mut(&id) {
                    report.possibly_incomplete = true;
                    report.revision = revision;
                }
                updated.push(id);
            }
            store.publish(QualityUpdate::Gap(QualityGap {
                scope: QualityGapScope::Ingress,
                dropped: new_drops,
            }));
        }
        let had_events = !events.is_empty();
        for event in events {
            let id = event.reference.id;
            if store.apply(event, &self.config) && !updated.contains(&id) {
                updated.push(id);
            }
        }
        for summary in loss_summaries {
            let id = summary.reference.id;
            let revision = store.revision();
            if let Some(report) = store.records.get_mut(&id) {
                // This also covers a loss discovered after a preceding drain
                // completed this report. The accepted network result is immutable.
                report.possibly_incomplete = true;
                report.revision = revision;
                if !updated.contains(&id) {
                    updated.push(id);
                }
            }
            if store.recent_drops.len() == MAX_DROP_SUMMARIES.min(self.config.limits.ingress) {
                store.recent_drops.pop_front();
            }
            store.recent_drops.push_back(summary);
        }
        let previous_loss_count = store.recent_drops.len();
        store
            .recent_drops
            .retain(|summary| now.saturating_sub(summary.event_offset) < self.config.limits.ttl);
        if previous_loss_count != store.recent_drops.len() {
            store.revision();
        }
        store.expire(now, &self.config, &mut updated);
        if self.state() != QualityDoctorState::Running {
            if !store.closed {
                store.revision();
            }
            // Close can race with the ingress drain. Accepted events left in
            // the next batch must also reach a terminal observation state;
            // store.closed only gates the one-time subscriber notification.
            let closing = store
                .records
                .iter()
                .filter(|(_, report)| report.state == QualityObservationState::InProgress)
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            for id in closing {
                let revision = store.revision();
                if let Some(report) = store.records.get_mut(&id) {
                    report.state = QualityObservationState::OwnerClosed;
                    close_phase_activity(report, QualityPhaseState::OwnerClosed);
                    report.finished = Some(now);
                    report.possibly_incomplete = true;
                    report.revision = revision;
                }
                if !updated.contains(&id) {
                    updated.push(id);
                }
            }
        }
        if self.state() != QualityDoctorState::Running {
            store.enforce_history(&self.config);
        }
        let had_updates = !updated.is_empty();
        for id in updated {
            if let Some(report) = store.records.get(&id) {
                let update = if report.state == QualityObservationState::InProgress {
                    QualityUpdate::ReportUpdated(Arc::new(report.clone()))
                } else {
                    QualityUpdate::ReportCompleted(Arc::new(report.clone()))
                };
                store.publish(update);
            }
        }
        let snapshot_subscribers = store
            .subscribers
            .iter()
            .filter(|sub| {
                sub.options.snapshots
                    && (had_events || had_updates || store.revision != initial_revision)
            })
            .map(|sub| (sub.id, sub.options.query.clone()))
            .collect::<Vec<_>>();
        for (id, query) in snapshot_subscribers {
            let mut snapshot = store.snapshot(self, query, now);
            // Snapshot notifications contain aggregates only. Retaining a full
            // history copy in every subscriber slot would multiply the budget.
            snapshot.reports = Vec::new();
            snapshot.findings = Vec::new();
            snapshot.recent_drops = Vec::new();
            snapshot.results_truncated = snapshot.summary.retained != 0;
            let snapshot = Arc::new(snapshot);
            if let Some(sub) = store.subscribers.iter_mut().find(|sub| sub.id == id) {
                let dropped = sub.send(QualityUpdate::SnapshotUpdated(snapshot));
                let Store { counters, .. } = &mut *store;
                add(
                    &mut counters.subscriber_dropped,
                    dropped,
                    &mut counters.saturated,
                );
                if dropped != 0 {
                    store.revision();
                }
            }
        }
        if self.state() != QualityDoctorState::Running && !store.closed {
            store.closed = true;
            store.publish(QualityUpdate::Closed);
            store.subscribers.clear();
        }
    }
}

fn close_phase_activity(report: &mut QualityReport, state: QualityPhaseState) {
    for phase in &mut report.phase_activity {
        if phase.state == QualityPhaseState::Running {
            phase.state = state;
            report.possibly_incomplete = true;
        }
    }
}

impl Store {
    fn revision(&mut self) -> u64 {
        self.revision = self.revision.saturating_add(1);
        self.revision
    }
    fn evict(&mut self, id: u64, config: &QualityDoctorConfig) {
        if self.records.remove(&id).is_none() {
            return;
        }
        self.revision();
        self.order.retain(|value| *value != id);
        if self.tombstones.len() == config.limits.history {
            self.tombstones.pop_front();
        }
        self.tombstones.push_back(id);
        add(
            &mut self.counters.records_evicted,
            1,
            &mut self.counters.saturated,
        );
    }
    fn apply(&mut self, event: Event, config: &QualityDoctorConfig) -> bool {
        let id = event.reference.id;
        if let EventKind::Begin {
            protocol,
            client_id,
        } = event.kind
        {
            if self.records.contains_key(&id) || self.tombstones.contains(&id) {
                return false;
            }
            let active = self
                .records
                .values()
                .filter(|report| report.state == QualityObservationState::InProgress)
                .count();
            if active >= config.limits.active {
                if let Some(old) = self.order.iter().copied().find(|id| {
                    self.records
                        .get(id)
                        .is_some_and(|report| report.state == QualityObservationState::InProgress)
                }) {
                    self.evict(old, config);
                }
            }
            let revision = self.revision();
            self.records.insert(
                id,
                QualityReport {
                    schema_version: 2,
                    reference: event.reference,
                    protocol,
                    client_id,
                    revision,
                    started: event.at,
                    result_at: None,
                    finished: None,
                    outcome: None,
                    http_status: None,
                    http_version: None,
                    failure: None,
                    state: QualityObservationState::InProgress,
                    detail: if config.mode == QualityMode::Basic {
                        QualityDetailState::BasicMode
                    } else if event.reference.detailed {
                        QualityDetailState::Selected
                    } else {
                        QualityDetailState::NotSampled
                    },
                    phases: Vec::new(),
                    phase_activity: Vec::new(),
                    associations: QualityAssociations::default(),
                    bytes: 0,
                    phase_overflow: 0,
                    // Ingress and loss summaries are drained independently. A
                    // summary may arrive one drain before its accepted Begin.
                    possibly_incomplete: self.batch_gap
                        || self
                            .recent_drops
                            .iter()
                            .any(|summary| summary.reference == event.reference),
                    counters_saturated: false,
                },
            );
            self.order.push_back(id);
            add(
                &mut self.counters.records_observed,
                1,
                &mut self.counters.saturated,
            );
            return true;
        }
        let Some(report) = self.records.get(&id) else {
            add(
                &mut self.counters.orphan_events,
                1,
                &mut self.counters.saturated,
            );
            self.revision();
            return false;
        };
        if report.state != QualityObservationState::InProgress {
            return false;
        }
        let revision = self.revision();
        let Some(report) = self.records.get_mut(&id) else {
            return false;
        };
        report.revision = revision;
        report.bytes = report.bytes.max(event.bytes);
        report.counters_saturated |= event.bytes_saturated;
        report.phase_overflow = report.phase_overflow.max(event.phase_overflow);
        report.possibly_incomplete |= event.phase_overflow != 0;
        match event.kind {
            EventKind::Begin { .. } => return false,
            EventKind::PhaseStarted {
                instance_id,
                phase,
                fields,
            } => {
                if report.phase_activity.len() < config.limits.phases {
                    report.phase_activity.push(QualityPhaseActivity {
                        instance_id,
                        kind: phase,
                        start: event.at,
                        end: None,
                        state: QualityPhaseState::Running,
                        associations: fields,
                        start_observed: true,
                    });
                } else {
                    report.possibly_incomplete = true;
                }
            }
            EventKind::Phase {
                instance_id,
                phase,
                started,
                fields,
            } => {
                if let Some(start) = started.filter(|_| event.reference.detailed) {
                    if let Some(activity) = report
                        .phase_activity
                        .iter_mut()
                        .find(|activity| activity.instance_id == instance_id)
                    {
                        activity.end = Some(event.at);
                        activity.state = QualityPhaseState::Ended;
                    } else {
                        // An end carries the true start even when ingress lost the
                        // start event. Recover the interval, never its completeness.
                        report.possibly_incomplete = true;
                        if report.phase_activity.len() < config.limits.phases {
                            report.phase_activity.push(QualityPhaseActivity {
                                instance_id,
                                kind: phase,
                                start,
                                end: Some(event.at),
                                state: QualityPhaseState::Ended,
                                associations: fields.clone(),
                                start_observed: false,
                            });
                        }
                    }
                }
                if report.phases.len() < config.limits.phases {
                    report.phases.push(QualityPhase {
                        instance_id,
                        kind: phase,
                        start: started.unwrap_or(event.at),
                        end: started.map(|_| event.at),
                        associations: fields,
                    });
                } else {
                    add(
                        &mut report.phase_overflow,
                        1,
                        &mut report.counters_saturated,
                    );
                    report.possibly_incomplete = true;
                }
            }
            EventKind::Result {
                outcome,
                failure,
                http_status,
            } => {
                // Network selection is immutable; callback/cleanup events cannot
                // inflate its duration or replace it with a delivery failure.
                if report.result_at.is_none() {
                    report.outcome = Some(outcome);
                    report.http_status = http_status;
                    report.failure = failure;
                    report.result_at = Some(event.at);
                }
            }
            EventKind::Association(key, value) => match key {
                Association::Attempt => report.associations.attempt = Some(value),
                Association::Hop => report.associations.hop = Some(value),
                Association::Connection => report.associations.connection = Some(value),
                Association::Session => report.associations.session = Some(value),
                Association::Cycle => report.associations.cycle = Some(value),
                Association::Operation => report.associations.operation = Some(value),
            },
            EventKind::Parent(value) => report.associations.parent = Some(value),
            #[cfg(feature = "http-client")]
            EventKind::HttpVersion(version) => report.http_version = Some(version),
            EventKind::Finish => {
                close_phase_activity(report, QualityPhaseState::ReportCompleted);
                report.state = QualityObservationState::Completed;
                report.finished = Some(event.at);
                report.possibly_incomplete |=
                    matches!(report.outcome, None | Some(QualityOutcome::Unknown));
            }
        }
        self.enforce_history(config);
        true
    }
    fn enforce_history(&mut self, config: &QualityDoctorConfig) {
        while self
            .records
            .values()
            .filter(|report| report.state != QualityObservationState::InProgress)
            .count()
            > config.limits.history
        {
            if let Some(old) = self.order.iter().copied().find(|id| {
                self.records
                    .get(id)
                    .is_some_and(|report| report.state != QualityObservationState::InProgress)
            }) {
                self.evict(old, config);
            } else {
                break;
            }
        }
    }
    fn expire(&mut self, now: Duration, config: &QualityDoctorConfig, updated: &mut Vec<u64>) {
        let expired = self
            .records
            .iter()
            .filter(|(_, report)| {
                now.saturating_sub(report.finished.unwrap_or(report.started)) >= config.limits.ttl
            })
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in expired {
            if self
                .records
                .get(&id)
                .is_some_and(|report| report.state == QualityObservationState::InProgress)
            {
                let revision = self.revision();
                if let Some(report) = self.records.get_mut(&id) {
                    report.state = QualityObservationState::Expired;
                    close_phase_activity(report, QualityPhaseState::ReportExpired);
                    report.finished = Some(now);
                    report.possibly_incomplete = true;
                    report.revision = revision;
                    if !updated.contains(&id) {
                        updated.push(id);
                    }
                    add(
                        &mut self.counters.records_expired,
                        1,
                        &mut self.counters.saturated,
                    );
                }
            } else {
                self.evict(id, config);
            }
        }
        self.enforce_history(config);
    }
    fn publish(&mut self, update: QualityUpdate) {
        let report = match &update {
            QualityUpdate::ReportUpdated(report) | QualityUpdate::ReportCompleted(report) => {
                Some(report)
            }
            _ => None,
        };
        let mut dropped = 0u64;
        self.subscribers.retain_mut(|subscriber| {
            if subscriber.sender.is_closed() {
                return false;
            }
            if report.is_none_or(|report| subscriber.options.query.matches(report)) {
                dropped = dropped.saturating_add(subscriber.send(update.clone()));
            }
            true
        });
        add(
            &mut self.counters.subscriber_dropped,
            dropped,
            &mut self.counters.saturated,
        );
        if dropped != 0 {
            self.revision();
        }
    }
    fn snapshot(
        &self,
        collector: &Collector,
        query: QualityQuery,
        now: Duration,
    ) -> QualitySnapshot {
        let window = query.window.unwrap_or(collector.config.limits.window);
        let limit = query.limit.unwrap_or(collector.config.limits.query_results);
        let mut summary = QualitySummary::default();
        let mut latency = Vec::new();
        let mut reports = Vec::new();
        let mut findings = Vec::new();
        let mut matched = 0usize;
        for id in self.order.iter().rev() {
            let Some(report) = self.records.get(id) else {
                continue;
            };
            if !query.matches(report)
                || now.saturating_sub(report.finished.unwrap_or(report.started)) > window
            {
                continue;
            }
            matched += 1;
            if !summary.protocols.contains(&report.protocol) {
                summary.protocols.push(report.protocol);
            }
            if !summary.operation_kinds.contains(&report.reference.kind) {
                summary.operation_kinds.push(report.reference.kind);
            }
            summary.retained += 1;
            if report.state == QualityObservationState::Completed {
                summary.completed += 1;
            }
            if report.possibly_incomplete
                || matches!(
                    report.state,
                    QualityObservationState::Expired | QualityObservationState::OwnerClosed
                )
            {
                summary.incomplete += 1;
            }
            match report.outcome {
                Some(QualityOutcome::Success) => {
                    summary.successes += 1;
                    if !report.possibly_incomplete
                        && report.state == QualityObservationState::Completed
                    {
                        if let Some(duration) = report.result_duration() {
                            latency.push(duration);
                        }
                    }
                }
                Some(QualityOutcome::Failed) => summary.failures += 1,
                Some(QualityOutcome::Timeout) => summary.timeouts += 1,
                Some(QualityOutcome::Cancelled) => summary.cancelled += 1,
                _ => {}
            }
            if reports.len() < limit {
                reports.push(report.clone());
            }
            if findings.len() < limit {
                if let Some(phase) = report
                    .phases
                    .iter()
                    .filter(|phase| {
                        phase
                            .duration()
                            .is_some_and(|duration| duration >= collector.config.policy.slow_phase)
                    })
                    .max_by_key(|phase| phase.duration())
                {
                    findings.push(QualityFinding {
                        kind: QualityFindingKind::SlowObservedPhase,
                        phase: Some(phase.kind),
                        reference: Some(report.reference),
                        samples: 1,
                        duration: phase.duration(),
                        threshold: Some(collector.config.policy.slow_phase),
                    });
                }
            }
        }
        summary.latency_samples = latency.len() as u64;
        let comparable = !summary.mixed_kinds() && !summary.mixed_protocols();
        if comparable && latency.len() >= collector.config.policy.min_samples {
            latency.sort_unstable();
            let percentile =
                |p: usize| latency[(latency.len() * p).div_ceil(100).saturating_sub(1)];
            summary.p50 = Some(percentile(50));
            summary.p95 = Some(percentile(95));
            summary.p99 = Some(percentile(99));
        }
        let classified = summary.successes + summary.failures + summary.timeouts;
        if comparable
            && classified >= collector.config.policy.min_samples as u64
            && (summary.failures + summary.timeouts) as f64 / classified as f64
                >= collector.config.policy.failure_ratio
            && findings.len() < limit
        {
            findings.push(QualityFinding {
                kind: QualityFindingKind::HighObservedFailureRatio,
                phase: None,
                reference: None,
                samples: classified,
                duration: None,
                threshold: None,
            });
        }
        if (summary.incomplete > 0
            || self.counters.ingress_dropped > 0
            || self.counters.records_evicted > 0)
            && findings.len() < limit
        {
            findings.push(QualityFinding {
                kind: QualityFindingKind::IncompleteCoverage,
                phase: None,
                reference: None,
                samples: summary.retained,
                duration: None,
                threshold: None,
            });
        }
        QualitySnapshot {
            schema_version: 2,
            revision: self.revision,
            scope: collector.scope,
            captured_at: now,
            window,
            state: collector.state(),
            summary,
            counters: self.counters.clone(),
            recent_drops: self.recent_drops.iter().cloned().collect(),
            reports,
            findings,
            results_truncated: matched > limit,
        }
    }
}
impl Subscriber {
    fn send(&mut self, update: QualityUpdate) -> u64 {
        if self.dropped != 0 {
            if self
                .sender
                .try_send(QualityUpdate::Gap(QualityGap {
                    scope: QualityGapScope::Subscription,
                    dropped: self.dropped,
                }))
                .is_ok()
            {
                self.dropped = 0;
            } else {
                self.dropped = self.dropped.saturating_add(1);
                return 1;
            }
        }
        if self.sender.try_send(update).is_err() {
            self.dropped = self.dropped.saturating_add(1);
            1
        } else {
            0
        }
    }
}

impl QualityDoctor {
    /// Returns the diagnostic collector's current lifecycle state.
    ///
    /// This reads the shared lifecycle flags without draining queued observations,
    /// expiring retained reports, or polling the network. `Running` means the
    /// collector accepts observations; it does not mean any protocol is connected
    /// or that all events have been retained. `Closed` means its engine or
    /// standalone HTTP owner has closed. `Failed` takes precedence over `Closed`
    /// when an internal diagnostic failure has also been recorded.
    ///
    /// Existing handles remain usable for best-effort reads after owner closure.
    /// The state can change immediately after this call returns.
    pub fn state(&self) -> QualityDoctorState {
        self.inner.state()
    }
    /// Returns the immutable capability description built when collection activated.
    ///
    /// The description combines compiled transport features, configured protocol
    /// selection, and supported observation boundaries. It explains which phases
    /// can be measured and which remain unavailable or opaque. It is not a probe
    /// of the current connection, provider, or peer, and an available phase need
    /// not appear in every report because execution paths, sampling, and event
    /// loss differ. This call shares the existing `Arc` without draining events.
    pub fn capabilities(&self) -> Arc<QualityCapabilities> {
        self.inner.capabilities.clone()
    }
    /// Builds an owned, immutable view of retained observations matching `query`.
    ///
    /// On the calling thread, drains the currently queued diagnostic facts,
    /// applies report updates, expires old records, computes findings, and fans
    /// out subscription updates. No network operation is awaited, but collection
    /// takes internal locks and allocates result storage; use a diagnostic/control
    /// path rather than a latency-sensitive network callback when possible.
    /// Events produced concurrently may appear only in a later read.
    ///
    /// The window is measured backward from the collector's monotonic activation
    /// clock. A report is included according to its finish offset, or its start
    /// offset while unfinished. Reports are returned in reverse insertion order, up to
    /// the result limit; aggregates use all matching retained reports in the
    /// window, not just that returned prefix. Counters remain collector-wide.
    /// Omitting window or limit uses the configured defaults. The returned `Arc`
    /// is a fixed snapshot and does not update when later observations arrive.
    ///
    /// Reads remain possible after closure, subject to retention and diagnostic
    /// storage availability. A snapshot whose state is `Closed` or `Failed` can
    /// still contain useful evidence; it does not certify complete coverage.
    ///
    /// # Errors
    ///
    /// Returns [`QualityError::InvalidQuery`] for a zero limit, a limit above the
    /// configured query budget, a zero window, or a window exceeding retention
    /// TTL. Returns [`QualityError::CollectorFailed`] when the report store cannot
    /// be read. A failed collector with a readable store may instead return a
    /// snapshot carrying the `Failed` state.
    pub fn snapshot(&self, query: QualityQuery) -> Result<Arc<QualitySnapshot>, QualityError> {
        validate_query(&query, &self.inner.config)?;
        self.inner.drive();
        let store = self
            .inner
            .store
            .lock()
            .map_err(|_| QualityError::CollectorFailed)?;
        Ok(Arc::new(store.snapshot(
            &self.inner,
            query,
            self.inner.now(),
        )))
    }
    /// Looks up one report by a reference issued within this collector's scope.
    ///
    /// A foreign scope returns [`QualityLookup::WrongScope`] without driving
    /// collection. Otherwise this call first drains observations and maintains
    /// retention, as [`Self::snapshot`] does, then copies the current report into
    /// an immutable `Arc`. A reference neither pins a report nor keeps networking
    /// alive; query windows and query result limits do not restrict this lookup.
    ///
    /// [`QualityLookup::Evicted`] requires a still-retained eviction tombstone.
    /// [`QualityLookup::NotRetained`] cannot distinguish an observation that was
    /// never retained, an old eviction whose tombstone has expired from the
    /// bounded history, or unavailable diagnostic storage; inspect its owner state
    /// as well. The report may be incomplete or still in progress even after the
    /// corresponding public operation has returned.
    pub fn lookup(&self, reference: &QualityReportRef) -> QualityLookup {
        if reference.scope != self.inner.scope {
            return QualityLookup::WrongScope;
        }
        self.inner.drive();
        if let Ok(store) = self.inner.store.lock() {
            if let Some(report) = store.records.get(&reference.id) {
                return QualityLookup::Available(Arc::new(report.clone()));
            }
            if store.tombstones.contains(&reference.id) {
                return QualityLookup::Evicted;
            }
        }
        QualityLookup::NotRetained {
            owner_state: self.state(),
        }
    }
    /// Registers a bounded, pull-driven stream of diagnostic updates.
    ///
    /// Registration does not drain events, replay retained reports, or emit an
    /// initial snapshot. Call [`Self::snapshot`] when an initial view is needed.
    /// Subsequent reads through this doctor or any of its subscriptions drive
    /// collection, including facts already queued when registration occurred.
    /// No background diagnostic worker is created and network producers never
    /// wait for this subscriber.
    ///
    /// The query's protocol, client, and operation-kind filters select report
    /// updates. Its window applies to aggregate snapshot notifications. Its limit
    /// is validated but does not cap update delivery. When requested, snapshot
    /// notifications contain aggregates and counters with empty report, finding,
    /// and recent-drop lists; use [`Self::snapshot`] to retrieve those lists.
    /// Capacity counts queued notifications. Overflow drops new notifications
    /// and a later successful publication attempts to report a subscription gap;
    /// delivery of every update, including the final `Closed`, is not guaranteed.
    ///
    /// The returned subscription retains diagnostic storage, not its network
    /// owner. Dropping it or calling [`QualitySubscription::unsubscribe`] removes
    /// its registration. Polling asynchronously requires the caller's Tokio time
    /// driver; [`QualitySubscription::try_recv`] does not require one.
    ///
    /// # Errors
    ///
    /// Returns [`QualityError::OwnerClosed`] or [`QualityError::CollectorFailed`]
    /// when the owner is closed or diagnostics cannot be used;
    /// [`QualityError::Capacity`] when the configured registration budget or
    /// subscriber identifier space is exhausted;
    /// [`QualityError::InvalidConfig`] if capacity is zero or above its configured
    /// maximum, or the poll interval is outside `1 ms..=60 s`; and
    /// [`QualityError::InvalidQuery`] for an invalid query window or limit.
    pub fn subscribe(
        &self,
        options: QualitySubscriptionOptions,
    ) -> Result<QualitySubscription, QualityError> {
        if self.state() == QualityDoctorState::Closed {
            return Err(QualityError::OwnerClosed);
        }
        if self.state() == QualityDoctorState::Failed {
            return Err(QualityError::CollectorFailed);
        }
        if options.capacity == 0
            || options.capacity > self.inner.config.limits.subscription_capacity
        {
            return Err(QualityError::InvalidConfig("subscription_capacity"));
        }
        if options.poll_interval < Duration::from_millis(1)
            || options.poll_interval > Duration::from_secs(60)
        {
            return Err(QualityError::InvalidConfig("subscription_poll_interval"));
        }
        validate_query(&options.query, &self.inner.config)?;
        let mut store = self
            .inner
            .store
            .lock()
            .map_err(|_| QualityError::CollectorFailed)?;
        if store.closed || self.state() == QualityDoctorState::Closed {
            return Err(QualityError::OwnerClosed);
        }
        store
            .subscribers
            .retain(|subscriber| !subscriber.sender.is_closed());
        if store.subscribers.len() >= self.inner.config.limits.subscriptions {
            return Err(QualityError::Capacity);
        }
        let id = store.next_subscriber;
        store.next_subscriber = id.checked_add(1).ok_or(QualityError::Capacity)?;
        let (sender, receiver) = mpsc::channel(options.capacity);
        let poll_interval = options.poll_interval;
        store.subscribers.push(Subscriber {
            id,
            sender,
            options,
            dropped: 0,
        });
        Ok(QualitySubscription {
            inner: SubscriptionInner {
                collector: self.inner.clone(),
                receiver,
                id,
                unsubscribed: false,
                poll_interval,
            },
        })
    }
}
fn validate_query(query: &QualityQuery, config: &QualityDoctorConfig) -> Result<(), QualityError> {
    if query
        .limit
        .is_some_and(|limit| limit == 0 || limit > config.limits.query_results)
    {
        return Err(QualityError::InvalidQuery("limit"));
    }
    if query
        .window
        .is_some_and(|window| window.is_zero() || window > config.limits.ttl)
    {
        return Err(QualityError::InvalidQuery("window"));
    }
    Ok(())
}

pub(crate) struct SubscriptionInner {
    collector: Arc<Collector>,
    receiver: mpsc::Receiver<QualityUpdate>,
    id: u64,
    unsubscribed: bool,
    poll_interval: Duration,
}
impl QualitySubscription {
    /// Drives collection and returns the next queued update without waiting for one.
    ///
    /// Collection runs synchronously on this caller, including internal locking,
    /// retention maintenance, and subscriber publication. Consequently this is
    /// not a lock-free or bounded-execution-time operation; "without waiting"
    /// refers to an empty notification queue. No Tokio runtime or timer is needed.
    ///
    /// Returns [`QualityTryRecvError::Empty`] if the stream remains open with no
    /// queued update, or [`QualityTryRecvError::Closed`] after explicit
    /// unsubscription or after the closed stream's buffered updates are drained.
    /// Collector failure closes publication on a subsequent collection pass;
    /// this method reports stream closure, not a separate collector error. Use
    /// [`QualityDoctor::state`] or a snapshot to inspect collector health.
    pub fn try_recv(&mut self) -> Result<QualityUpdate, QualityTryRecvError> {
        if self.inner.unsubscribed {
            return Err(QualityTryRecvError::Closed);
        }
        self.inner.collector.drive();
        self.inner.receiver.try_recv().map_err(|error| match error {
            mpsc::error::TryRecvError::Empty => QualityTryRecvError::Empty,
            mpsc::error::TryRecvError::Disconnected => QualityTryRecvError::Closed,
        })
    }
    /// Drives collection until an update is available or publication has closed.
    ///
    /// Returns `Ok(Some(update))` for the next buffered notification and `Ok(None)`
    /// after explicit unsubscription, or once the closed stream's buffered updates
    /// have drained. A `Closed` notification may precede `None`, but bounded queue
    /// overflow can drop that notification. Collector failure also ends the
    /// stream; inspect [`QualityDoctor::state`] for the diagnostic lifecycle.
    ///
    /// Each collection pass runs on this caller's executor and may synchronously
    /// lock and allocate. If the queue is empty, waits for the configured polling
    /// interval using the caller's Tokio timer, then checks again. Network
    /// producers do not invoke a subscriber waker; there is no diagnostic
    /// background task and the interval is not a delivery deadline. Cancelling
    /// this waiting future keeps the subscription registered and consumes no
    /// update that has not already been returned.
    ///
    /// # Errors
    ///
    /// Returns [`QualityError::RuntimeUnavailable`] if an empty read needs to
    /// wait but no Tokio runtime/time driver is available. Buffered updates and
    /// closure can be returned without a timer. Timer panics are contained on
    /// unwind builds; `panic = "abort"` cannot be recovered. Use [`Self::try_recv`]
    /// when the caller does not provide a Tokio time driver.
    pub async fn recv(&mut self) -> Result<Option<QualityUpdate>, QualityError> {
        if self.inner.unsubscribed {
            return Ok(None);
        }
        loop {
            self.inner.collector.drive();
            match self.inner.receiver.try_recv() {
                Ok(update) => return Ok(Some(update)),
                Err(mpsc::error::TryRecvError::Disconnected) => return Ok(None),
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            if tokio::runtime::Handle::try_current().is_err() {
                return Err(QualityError::RuntimeUnavailable);
            }
            let interval = self.inner.poll_interval;
            let delay = std::panic::catch_unwind(|| tokio::time::sleep(interval))
                .map_err(|_| QualityError::RuntimeUnavailable)?;
            std::panic::AssertUnwindSafe(delay)
                .catch_unwind()
                .await
                .map_err(|_| QualityError::RuntimeUnavailable)?;
        }
    }
    /// Stops this subscription and releases its registration budget when accessible.
    ///
    /// Idempotent and also called when the subscription is dropped. Subsequent
    /// [`Self::try_recv`] calls return `Closed` and [`Self::recv`] returns `None`,
    /// even if notifications were buffered before unsubscription. It neither
    /// closes the doctor nor cancels network work or other subscriptions. Keeping
    /// this unsubscribed handle still retains its diagnostic storage until the
    /// handle itself is dropped; it does not retain the network owner.
    pub fn unsubscribe(&mut self) {
        if self.inner.unsubscribed {
            return;
        }
        self.inner.unsubscribed = true;
        self.inner.receiver.close();
        if let Ok(mut store) = self.inner.collector.store.lock() {
            store
                .subscribers
                .retain(|subscriber| subscriber.id != self.inner.id);
        }
    }
}
impl Drop for QualitySubscription {
    fn drop(&mut self) {
        self.unsubscribe();
    }
}

fn capabilities(config: &QualityDoctorConfig) -> QualityCapabilities {
    let mut phases = Vec::new();
    for protocol in [QualityProtocol::Http, QualityProtocol::WebSocket] {
        let compiled = match protocol {
            QualityProtocol::Http => cfg!(feature = "http-client"),
            QualityProtocol::WebSocket => cfg!(feature = "ws-client"),
        };
        for phase in [
            PhaseKind::Admission,
            PhaseKind::PermitWait,
            PhaseKind::QueueWait,
            PhaseKind::Published,
            PhaseKind::Dequeued,
            PhaseKind::WorkerStarted,
            PhaseKind::Preparation,
            PhaseKind::Provider,
            PhaseKind::RequestBuild,
            PhaseKind::RequestGetters,
            PhaseKind::Prepared,
            PhaseKind::ExecuteToHeaders,
            PhaseKind::ResponseHeaders,
            PhaseKind::FirstBodyChunk,
            PhaseKind::ResponseBody,
            PhaseKind::ReportFrozen,
            PhaseKind::ResponseQueue,
            PhaseKind::CallbackQueue,
            PhaseKind::Callback,
            PhaseKind::ResourcesReleased,
            PhaseKind::CompletionPublished,
            PhaseKind::Connector,
            PhaseKind::ConnectorPolled,
            PhaseKind::DeliveryFinished,
            PhaseKind::ResultSelected,
            PhaseKind::NetworkWait,
            PhaseKind::Backoff,
            PhaseKind::ConnectionAdmission,
            PhaseKind::Dns,
            PhaseKind::TcpConnect,
            PhaseKind::TcpCandidate,
            PhaseKind::ProxyTls,
            PhaseKind::ProxyConnect,
            PhaseKind::Tls,
            PhaseKind::WebSocketUpgrade,
            PhaseKind::SinkReadiness,
            PhaseKind::StartSend,
            PhaseKind::Flush,
            PhaseKind::Written,
            PhaseKind::ResponseMatched,
            PhaseKind::ManualDispatch,
            PhaseKind::ManualResolve,
            PhaseKind::Requeue,
            PhaseKind::Heartbeat,
            PhaseKind::ResponseWait,
            PhaseKind::PayloadReceived,
            PhaseKind::ResponseRouting,
        ] {
            let (level, reason) = if !compiled {
                (
                    QualityCapabilityLevel::Unsupported,
                    QualityCapabilityReason::FeatureNotCompiled,
                )
            } else if !config.protocols.includes(protocol) {
                (
                    QualityCapabilityLevel::DisabledByConfig,
                    QualityCapabilityReason::ProtocolDisabled,
                )
            } else if protocol == QualityProtocol::Http
                && matches!(
                    phase,
                    PhaseKind::Dns
                        | PhaseKind::TcpConnect
                        | PhaseKind::TcpCandidate
                        | PhaseKind::ProxyTls
                        | PhaseKind::ProxyConnect
                        | PhaseKind::Tls
                )
            {
                (
                    QualityCapabilityLevel::Unsupported,
                    QualityCapabilityReason::DependencyOpaque,
                )
            } else if !instrumented(protocol, phase) {
                (
                    QualityCapabilityLevel::Unsupported,
                    QualityCapabilityReason::NotInstrumented,
                )
            } else if detail_phase(phase)
                && (config.mode != QualityMode::Detailed
                    || (phase == PhaseKind::TcpCandidate && config.sampling.connections == 0.0)
                    || (phase != PhaseKind::TcpCandidate && config.sampling.operations == 0.0))
            {
                (
                    QualityCapabilityLevel::DisabledByConfig,
                    QualityCapabilityReason::DetailDisabled,
                )
            } else if protocol == QualityProtocol::Http && phase == PhaseKind::Connector {
                (
                    QualityCapabilityLevel::Partial,
                    QualityCapabilityReason::CompetingConnectionCandidates,
                )
            } else if phase == PhaseKind::ResponseBody && protocol == QualityProtocol::Http {
                (
                    QualityCapabilityLevel::Partial,
                    QualityCapabilityReason::ConsumerDrivenBody,
                )
            } else if phase == PhaseKind::Dns {
                (
                    QualityCapabilityLevel::Partial,
                    QualityCapabilityReason::SystemResolutionAggregate,
                )
            } else {
                (
                    QualityCapabilityLevel::Partial,
                    QualityCapabilityReason::SdkBoundary,
                )
            };
            phases.push(QualityCapability {
                protocol,
                phase,
                level,
                reason,
            });
        }
    }
    QualityCapabilities { version: 1, phases }
}

fn instrumented(protocol: QualityProtocol, phase: PhaseKind) -> bool {
    match protocol {
        QualityProtocol::Http => matches!(
            phase,
            PhaseKind::Admission
                | PhaseKind::Backoff
                | PhaseKind::Callback
                | PhaseKind::CallbackQueue
                | PhaseKind::CompletionPublished
                | PhaseKind::Connector
                | PhaseKind::ConnectorPolled
                | PhaseKind::DeliveryFinished
                | PhaseKind::Dequeued
                | PhaseKind::ExecuteToHeaders
                | PhaseKind::FirstBodyChunk
                | PhaseKind::PermitWait
                | PhaseKind::Preparation
                | PhaseKind::Published
                | PhaseKind::QueueWait
                | PhaseKind::ReportFrozen
                | PhaseKind::RequestBuild
                | PhaseKind::RequestGetters
                | PhaseKind::ResourcesReleased
                | PhaseKind::ResponseBody
                | PhaseKind::ResponseHeaders
                | PhaseKind::ResponseQueue
                | PhaseKind::WorkerStarted
        ),
        QualityProtocol::WebSocket => matches!(
            phase,
            PhaseKind::Admission
                | PhaseKind::Backoff
                | PhaseKind::ConnectionAdmission
                | PhaseKind::DeliveryFinished
                | PhaseKind::Dequeued
                | PhaseKind::Dns
                | PhaseKind::Flush
                | PhaseKind::Heartbeat
                | PhaseKind::ManualDispatch
                | PhaseKind::ManualResolve
                | PhaseKind::NetworkWait
                | PhaseKind::PayloadReceived
                | PhaseKind::Prepared
                | PhaseKind::Provider
                | PhaseKind::ProxyConnect
                | PhaseKind::ProxyTls
                | PhaseKind::Published
                | PhaseKind::RequestBuild
                | PhaseKind::Requeue
                | PhaseKind::ResourcesReleased
                | PhaseKind::ResponseMatched
                | PhaseKind::ResponseRouting
                | PhaseKind::ResultSelected
                | PhaseKind::SinkReadiness
                | PhaseKind::StartSend
                | PhaseKind::TcpCandidate
                | PhaseKind::TcpConnect
                | PhaseKind::Tls
                | PhaseKind::WebSocketUpgrade
                | PhaseKind::Written
        ),
    }
}

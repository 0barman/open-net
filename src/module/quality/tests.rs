//! Regressions for the diagnostic boundary itself. These tests never open a
//! socket or change DNS, proxy, interface, connection-pool, or routing state.

#![cfg(test)]
#![cfg(any(feature = "http-client", feature = "ws-client"))]

use super::*;

fn protocol() -> QualityProtocol {
    if cfg!(feature = "http-client") {
        QualityProtocol::Http
    } else {
        QualityProtocol::WebSocket
    }
}

fn operation() -> QualityOperationKind {
    if cfg!(feature = "http-client") {
        QualityOperationKind::HttpRequest
    } else {
        QualityOperationKind::WsRequest
    }
}

fn fixture(config: QualityDoctorConfig) -> (QualityOwner, QualityDoctor, QualityProbe) {
    config.validate().expect("valid regression configuration");
    let owner = QualityOwner::new(config);
    let doctor = owner.doctor().expect("enabled doctor");
    let probe = owner
        .context()
        .begin(protocol(), operation(), 7)
        .expect("begin operation");
    (owner, doctor, probe)
}

fn report(doctor: &QualityDoctor, reference: QualityReportRef) -> Arc<QualityReport> {
    match doctor.lookup(&reference) {
        QualityLookup::Available(report) => report,
        lookup => panic!("expected retained report, got {lookup:?}"),
    }
}

fn counters(doctor: &QualityDoctor) -> QualityCounters {
    doctor
        .snapshot(QualityQuery::default())
        .expect("diagnostic snapshot")
        .counters()
        .clone()
}

#[test]
fn off_and_dormant_observation_do_not_activate_storage() {
    let off = QualityOwner::new(QualityDoctorConfig::default());
    assert!(off.inner.is_none());
    assert!(off.doctor().is_none());
    assert!(off.context().begin(protocol(), operation(), 1).is_none());

    let dormant = QualityOwner::new(QualityDoctorConfig::detailed());
    let context = dormant.context();
    assert!(context.is_enabled_for(protocol()));
    assert!(context.begin(protocol(), operation(), 1).is_none());
    assert!(dormant.inner.as_ref().unwrap().collector.get().is_none());
    dormant.close();
    assert!(context.begin(protocol(), operation(), 1).is_none());
    assert!(dormant.inner.as_ref().unwrap().collector.get().is_none());
}

#[tokio::test(start_paused = true)]
async fn running_activity_is_separate_from_legacy_intervals_and_milestones() {
    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    tokio::time::advance(Duration::from_millis(2)).await;
    let guard = probe.phase(PhaseKind::ExecuteToHeaders);
    let running = report(&doctor, probe.reference());
    assert!(running.phases().is_empty());
    assert_eq!(running.active_phases().count(), 1);
    let activity = &running.phase_activity()[0];
    assert_eq!(activity.kind(), PhaseKind::ExecuteToHeaders);
    assert_eq!(activity.state(), QualityPhaseState::Running);
    assert_eq!(activity.end_offset(), None);
    assert!(activity.start_observed());
    assert_eq!(activity.start_offset(), Duration::from_millis(2));
    let id = activity.instance_id();

    tokio::time::advance(Duration::from_millis(5)).await;
    probe.mark(PhaseKind::ResponseHeaders);
    guard.finish();
    probe.result(QualityOutcome::Success);
    probe.finish();
    let completed = report(&doctor, probe.reference());
    assert_eq!(completed.schema_version(), 2);
    assert_eq!(completed.active_phases().count(), 0);
    assert_eq!(completed.phase_activity().len(), 1);
    let activity = &completed.phase_activity()[0];
    assert_eq!(activity.instance_id(), id);
    assert_eq!(activity.state(), QualityPhaseState::Ended);
    assert_eq!(activity.end_offset(), Some(Duration::from_millis(7)));
    assert_eq!(completed.phases().len(), 2);
    let milestone = completed
        .phases()
        .iter()
        .find(|phase| phase.kind() == PhaseKind::ResponseHeaders)
        .unwrap();
    let interval = completed
        .phases()
        .iter()
        .find(|phase| phase.kind() == PhaseKind::ExecuteToHeaders)
        .unwrap();
    assert_eq!(milestone.duration(), None);
    assert_eq!(milestone.end_offset(), None);
    assert_ne!(milestone.instance_id(), id);
    assert_eq!(interval.instance_id(), id);
    assert_eq!(interval.duration(), Some(Duration::from_millis(5)));
    assert!(!completed.possibly_incomplete());
}

#[test]
fn phase_instances_freeze_associations_and_share_one_budget_per_interval() {
    let (_owner, doctor, probe) = fixture(
        QualityDoctorConfig::detailed().with_limits(QualityLimits::default().with_max_phases(2)),
    );
    probe.attempt(1);
    probe.hop(2);
    let first = probe.phase(PhaseKind::ExecuteToHeaders);
    probe.attempt(3);
    probe.hop(4);
    let second = probe.phase(PhaseKind::ExecuteToHeaders);
    // Two starts and two ends consume two logical phases, including when
    // phases of the same kind overlap and complete in reverse order.
    second.finish();
    first.finish();
    probe.result(QualityOutcome::Success);
    probe.finish();
    let observed = report(&doctor, probe.reference());
    assert_eq!(observed.phases().len(), 2);
    assert_eq!(observed.phase_activity().len(), 2);
    assert_eq!(observed.dropped_phases(), 0);
    assert!(!observed.possibly_incomplete());
    assert_eq!(observed.associations().attempt(), Some(3));
    assert_eq!(observed.associations().hop(), Some(4));
    let first = &observed.phase_activity()[0];
    let second = &observed.phase_activity()[1];
    assert_ne!(first.instance_id(), second.instance_id());
    assert_eq!(first.associations().attempt(), Some(1));
    assert_eq!(first.associations().hop(), Some(2));
    assert_eq!(second.associations().attempt(), Some(3));
    assert_eq!(second.associations().hop(), Some(4));
    for activity in observed.phase_activity() {
        let interval = observed
            .phases()
            .iter()
            .find(|phase| phase.instance_id() == activity.instance_id())
            .expect("one legacy interval for each lifecycle");
        assert_eq!(
            interval.associations().attempt(),
            activity.associations().attempt()
        );
        assert_eq!(interval.associations().hop(), activity.associations().hop());
    }
}

#[test]
fn basic_and_unsampled_operations_keep_main_phases_without_start_events() {
    for (config, detail) in [
        (QualityDoctorConfig::basic(), QualityDetailState::BasicMode),
        (
            QualityDoctorConfig::detailed().with_sampling(QualitySampling::new(0.0, 0.0)),
            QualityDetailState::NotSampled,
        ),
    ] {
        let (_owner, doctor, probe) = fixture(config);
        report(&doctor, probe.reference());
        let guard = probe.phase(PhaseKind::ExecuteToHeaders);
        let omitted = probe.phase(PhaseKind::RequestGetters);
        assert!(doctor.inner.ingress.lock().unwrap().is_empty());
        let running = report(&doctor, probe.reference());
        assert_eq!(running.detail_state(), detail);
        assert!(running.phases().is_empty());
        assert!(running.phase_activity().is_empty());
        guard.finish();
        omitted.finish();
        probe.result(QualityOutcome::Success);
        probe.finish();
        let completed = report(&doctor, probe.reference());
        assert_eq!(completed.phases().len(), 1);
        assert_eq!(completed.phases()[0].kind(), PhaseKind::ExecuteToHeaders);
        assert!(completed.phases()[0].duration().is_some());
        assert!(completed.phase_activity().is_empty());
        assert!(!completed.possibly_incomplete());
    }
}

#[test]
fn operation_and_connection_sampling_are_independent_and_stable() {
    let owner = QualityOwner::new(
        QualityDoctorConfig::detailed().with_sampling(QualitySampling::new(0.0, 1.0)),
    );
    let doctor = owner.doctor().unwrap();
    let context = owner.context();
    let operation = context.begin(protocol(), operation(), 1).unwrap();
    let connection = context
        .begin(protocol(), QualityOperationKind::Connection, 1)
        .unwrap();
    let main = operation.phase(PhaseKind::Connector);
    let candidate = connection.phase(PhaseKind::Connector);
    assert!(!operation.reference().detail_selected());
    assert!(connection.reference().detail_selected());
    assert!(report(&doctor, operation.reference())
        .phase_activity()
        .is_empty());
    assert_eq!(
        report(&doctor, connection.reference())
            .active_phases()
            .count(),
        1
    );
    main.finish();
    candidate.finish();
    operation.finish();
    connection.finish();
}

#[test]
fn phase_capacity_is_bounded_without_overwriting_existing_instances() {
    let (_owner, doctor, probe) = fixture(
        QualityDoctorConfig::detailed().with_limits(QualityLimits::default().with_max_phases(1)),
    );
    let retained = probe.phase(PhaseKind::ExecuteToHeaders);
    let rejected = probe.phase(PhaseKind::ResponseBody);
    probe.mark(PhaseKind::ResponseHeaders);
    rejected.finish();
    retained.finish();
    probe.result(QualityOutcome::Success);
    probe.finish();
    let observed = report(&doctor, probe.reference());
    assert_eq!(observed.phases().len(), 1);
    assert_eq!(observed.phase_activity().len(), 1);
    assert_eq!(observed.phases()[0].kind(), PhaseKind::ExecuteToHeaders);
    assert_eq!(observed.dropped_phases(), 2);
    assert!(observed.possibly_incomplete());
}

#[tokio::test(start_paused = true)]
async fn lost_start_recovers_interval_from_end_but_preserves_missing_evidence() {
    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    report(&doctor, probe.reference());
    tokio::time::advance(Duration::from_millis(3)).await;
    let ingress = doctor.inner.ingress.lock().unwrap();
    let guard = probe.phase(PhaseKind::ExecuteToHeaders);
    drop(ingress);
    tokio::time::advance(Duration::from_millis(9)).await;
    guard.finish();
    probe.result(QualityOutcome::Success);
    probe.finish();
    let observed = report(&doctor, probe.reference());
    assert_eq!(observed.phase_activity().len(), 1);
    let activity = &observed.phase_activity()[0];
    assert_eq!(activity.state(), QualityPhaseState::Ended);
    assert!(!activity.start_observed());
    assert_eq!(activity.start_offset(), Duration::from_millis(3));
    assert_eq!(activity.end_offset(), Some(Duration::from_millis(12)));
    assert_eq!(
        observed.phases()[0].duration(),
        Some(Duration::from_millis(9))
    );
    assert!(observed.possibly_incomplete());
    assert_eq!(counters(&doctor).ingress_lock_contended_dropped(), 1);
}

#[test]
fn lost_end_never_becomes_a_completed_interval_or_a_milestone() {
    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    let guard = probe.phase(PhaseKind::ExecuteToHeaders);
    report(&doctor, probe.reference());
    let ingress = doctor.inner.ingress.lock().unwrap();
    guard.finish();
    drop(ingress);
    probe.result(QualityOutcome::Cancelled);
    probe.finish();
    let observed = report(&doctor, probe.reference());
    assert_eq!(observed.outcome(), Some(QualityOutcome::Cancelled));
    assert_eq!(observed.state(), QualityObservationState::Completed);
    assert!(observed.phases().is_empty());
    assert_eq!(observed.active_phases().count(), 0);
    let activity = &observed.phase_activity()[0];
    assert_eq!(activity.state(), QualityPhaseState::ReportCompleted);
    assert_eq!(activity.end_offset(), None);
    assert!(observed.possibly_incomplete());
}

#[test]
fn lost_result_does_not_invent_outcome_or_latency() {
    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    report(&doctor, probe.reference());
    let ingress = doctor.inner.ingress.lock().unwrap();
    probe.result(QualityOutcome::Success);
    drop(ingress);
    probe.finish();
    let observed = report(&doctor, probe.reference());
    assert_eq!(observed.state(), QualityObservationState::Completed);
    assert_eq!(observed.outcome(), None);
    assert_eq!(observed.result_duration(), None);
    assert!(observed.possibly_incomplete());
    let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
    assert_eq!(snapshot.summary().latency_samples(), 0);
}

#[tokio::test(start_paused = true)]
async fn lost_finish_remains_observationally_active_until_retention_expiry() {
    let (_owner, doctor, probe) = fixture(
        QualityDoctorConfig::detailed().with_limits(
            QualityLimits::default()
                .with_retention(Duration::from_secs(2))
                .with_window(Duration::from_secs(2)),
        ),
    );
    let guard = probe.phase(PhaseKind::ExecuteToHeaders);
    probe.result(QualityOutcome::Timeout);
    report(&doctor, probe.reference());
    let ingress = doctor.inner.ingress.lock().unwrap();
    probe.finish();
    drop(ingress);
    let uncertain = report(&doctor, probe.reference());
    assert_eq!(uncertain.state(), QualityObservationState::InProgress);
    assert_eq!(uncertain.active_phases().count(), 1);
    assert!(uncertain.possibly_incomplete());
    tokio::time::advance(Duration::from_secs(2)).await;
    let expired = report(&doctor, probe.reference());
    assert_eq!(expired.state(), QualityObservationState::Expired);
    assert_eq!(expired.outcome(), Some(QualityOutcome::Timeout));
    assert_eq!(expired.active_phases().count(), 0);
    assert_eq!(
        expired.phase_activity()[0].state(),
        QualityPhaseState::ReportExpired
    );
    assert_eq!(expired.phase_activity()[0].end_offset(), None);
    assert_eq!(counters(&doctor).records_expired(), 1);
    // A late local drop must not revise expired evidence or invent a duration.
    let revision = expired.revision();
    drop(guard);
    assert_eq!(report(&doctor, probe.reference()).revision(), revision);
}

#[test]
fn finish_with_a_live_guard_preserves_unknown_phase_end_and_releases_capacity() {
    let (owner, doctor, probe) = fixture(
        QualityDoctorConfig::detailed()
            .with_limits(QualityLimits::default().with_active_capacity(1)),
    );
    let guard = probe.phase(PhaseKind::ExecuteToHeaders);
    probe.result(QualityOutcome::Timeout);
    probe.finish();
    probe.finish();
    let completed = report(&doctor, probe.reference());
    assert_eq!(
        completed.phase_activity()[0].state(),
        QualityPhaseState::ReportCompleted
    );
    assert_eq!(completed.phase_activity()[0].end_offset(), None);
    assert!(completed.phases().is_empty());
    assert!(completed.possibly_incomplete());
    let next = owner.context().begin(protocol(), operation(), 7).unwrap();
    assert_ne!(probe.reference(), next.reference());
    let revision = completed.revision();
    drop(guard);
    assert_eq!(report(&doctor, probe.reference()).revision(), revision);
    next.finish();
}

#[test]
fn doctor_and_phase_guard_do_not_retain_owner_or_claim_network_cancellation() {
    let (owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    let context = owner.context();
    let weak_owner = context.owner.clone();
    let guard = probe.phase(PhaseKind::Connector);
    report(&doctor, probe.reference());
    drop(owner);
    assert!(weak_owner.upgrade().is_none());
    assert!(context.begin(protocol(), operation(), 7).is_none());
    assert_eq!(doctor.state(), QualityDoctorState::Closed);
    let closed = report(&doctor, probe.reference());
    assert_eq!(closed.state(), QualityObservationState::OwnerClosed);
    assert_eq!(closed.outcome(), None);
    assert!(closed.possibly_incomplete());
    assert_eq!(
        closed.phase_activity()[0].state(),
        QualityPhaseState::OwnerClosed
    );
    assert_eq!(closed.phase_activity()[0].end_offset(), None);
    assert!(closed.phases().is_empty());
    assert_eq!(closed.active_phases().count(), 0);
    let revision = closed.revision();
    drop(guard);
    assert_eq!(report(&doctor, probe.reference()).revision(), revision);
}

#[test]
fn held_ingress_lock_drops_observation_without_waiting() {
    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    report(&doctor, probe.reference());
    let ingress = doctor.inner.ingress.lock().unwrap();
    let producer = probe.clone();
    let (sent, received) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        producer.mark(PhaseKind::ResponseHeaders);
        sent.send(()).unwrap();
    });
    let returned_while_locked = received.recv_timeout(Duration::from_secs(2));
    drop(ingress);
    thread.join().unwrap();
    assert!(
        returned_while_locked.is_ok(),
        "diagnostic producer waited for ingress lock"
    );
    let counts = counters(&doctor);
    assert_eq!(counts.ingress_dropped(), 1);
    assert_eq!(counts.ingress_lock_contended_dropped(), 1);
    assert_eq!(counts.ingress_queue_full_dropped(), 0);
}

#[test]
fn full_ingress_reports_reason_and_monotonic_high_watermark() {
    let (_owner, doctor, probe) = fixture(
        QualityDoctorConfig::detailed()
            .with_limits(QualityLimits::default().with_ingress_capacity(2)),
    );
    report(&doctor, probe.reference());
    probe.mark(PhaseKind::ResponseHeaders);
    probe.mark(PhaseKind::FirstBodyChunk);
    probe.result(QualityOutcome::Success);
    assert_eq!(doctor.inner.ingress.lock().unwrap().len(), 2);
    let counts = counters(&doctor);
    assert_eq!(counts.ingress_dropped(), 1);
    assert_eq!(counts.ingress_queue_full_dropped(), 1);
    assert_eq!(counts.ingress_lock_contended_dropped(), 0);
    assert_eq!(counts.ingress_high_watermark(), 2);
    assert_eq!(counters(&doctor).ingress_high_watermark(), 2);
    let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
    assert_eq!(snapshot.recent_drops().len(), 1);
    assert_eq!(
        snapshot.recent_drops()[0].reason(),
        QualityDropReason::QueueFull
    );
    assert_eq!(
        snapshot.recent_drops()[0].event_kind(),
        QualityEventKind::Result
    );
    probe.finish();
    assert!(report(&doctor, probe.reference()).possibly_incomplete());
}

#[test]
fn closed_and_failed_rejections_are_not_runtime_congestion_or_gap() {
    for failed in [false, true] {
        let (owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
        report(&doctor, probe.reference());
        if failed {
            doctor.inner.fail();
        } else {
            owner.close();
        }
        probe.result(QualityOutcome::Success);
        // No probe is created after close/failure, so this is not a submission.
        assert!(owner.context().begin(protocol(), operation(), 7).is_none());
        let counts = counters(&doctor);
        assert_eq!(counts.ingress_dropped(), 0);
        assert_eq!(counts.ingress_lock_contended_dropped(), 0);
        assert_eq!(counts.ingress_queue_full_dropped(), 0);
        assert_eq!(counts.ingress_closed_rejected(), u64::from(!failed));
        assert_eq!(counts.ingress_failed_rejected(), u64::from(failed));
        assert!(doctor
            .snapshot(QualityQuery::default())
            .unwrap()
            .recent_drops()
            .is_empty());
        assert_eq!(report(&doctor, probe.reference()).outcome(), None);
    }
}

#[tokio::test(start_paused = true)]
async fn loss_summaries_preserve_phase_identity_and_exact_missing_event_kinds() {
    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    report(&doctor, probe.reference());
    tokio::time::advance(Duration::from_millis(4)).await;
    let ingress = doctor.inner.ingress.lock().unwrap();
    let guard = probe.phase(PhaseKind::ExecuteToHeaders);
    tokio::time::advance(Duration::from_millis(3)).await;
    guard.finish();
    probe.result(QualityOutcome::Timeout);
    probe.finish();
    drop(ingress);
    let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
    let drops = snapshot.recent_drops();
    assert_eq!(drops.len(), 4);
    for summary in drops {
        assert_eq!(summary.reference(), probe.reference());
        assert_eq!(summary.reason(), QualityDropReason::LockContended);
    }
    assert_eq!(drops[0].event_kind(), QualityEventKind::PhaseStarted);
    assert_eq!(drops[1].event_kind(), QualityEventKind::PhaseEnded);
    assert_eq!(drops[2].event_kind(), QualityEventKind::Result);
    assert_eq!(drops[3].event_kind(), QualityEventKind::Finish);
    assert_eq!(drops[0].phase_kind(), Some(PhaseKind::ExecuteToHeaders));
    assert_eq!(drops[0].phase_instance_id(), drops[1].phase_instance_id());
    assert!(drops[0].phase_instance_id().is_some_and(|id| id > 0));
    assert_eq!(drops[2].phase_kind(), None);
    assert_eq!(drops[3].phase_instance_id(), None);
    assert_eq!(drops[0].event_offset(), Duration::from_millis(4));
    assert_eq!(drops[1].event_offset(), Duration::from_millis(7));
    assert_eq!(snapshot.counters().drop_summaries_lost(), 0);
    assert_eq!(snapshot.counters().ingress_dropped(), 4);
    let json = snapshot.to_json().unwrap();
    assert!(json.contains("phase_started"));
    assert!(json.contains("phase_ended"));
    assert!(!json.contains("http://"));
    assert!(!json.contains("https://"));
}

#[test]
fn loss_summary_pending_capacity_is_fixed_and_cannot_hide_aggregate_gap() {
    for (ingress_capacity, loss_count, retained) in [(1, 3, 1), (128, 80, 64)] {
        let (_owner, doctor, probe) = fixture(
            QualityDoctorConfig::detailed()
                .with_limits(QualityLimits::default().with_ingress_capacity(ingress_capacity)),
        );
        report(&doctor, probe.reference());
        let mut subscription = doctor
            .subscribe(QualitySubscriptionOptions::default())
            .unwrap();
        let ingress = doctor.inner.ingress.lock().unwrap();
        for _ in 0..loss_count {
            probe.result(QualityOutcome::Success);
        }
        drop(ingress);
        let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
        assert_eq!(snapshot.recent_drops().len(), retained);
        assert_eq!(snapshot.counters().ingress_dropped(), loss_count as u64);
        assert_eq!(
            snapshot.counters().drop_summaries_lost(),
            (loss_count - retained) as u64
        );
        let mut gap_count = 0;
        while let Ok(update) = subscription.try_recv() {
            if let QualityUpdate::Gap(gap) = update {
                assert_eq!(gap.scope(), QualityGapScope::Ingress);
                gap_count += gap.dropped();
            }
        }
        assert_eq!(gap_count, loss_count as u64);
        assert!(report(&doctor, probe.reference()).possibly_incomplete());
    }
}

#[test]
fn lost_begin_releases_probe_reservation_and_does_not_create_a_phantom_report() {
    let (owner, doctor, first) = fixture(
        QualityDoctorConfig::detailed().with_limits(
            QualityLimits::default()
                .with_ingress_capacity(1)
                .with_active_capacity(2),
        ),
    );
    // The first Begin fills ingress; the second Begin loses its observation.
    assert!(owner.context().begin(protocol(), operation(), 7).is_none());
    let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
    assert_eq!(snapshot.counters().begin_attempted(), 2);
    assert_eq!(snapshot.counters().begin_enqueued(), 1);
    assert_eq!(snapshot.reports().len(), 1);
    assert_eq!(snapshot.recent_drops().len(), 1);
    let summary = &snapshot.recent_drops()[0];
    assert_eq!(summary.event_kind(), QualityEventKind::Begin);
    assert_eq!(summary.reason(), QualityDropReason::QueueFull);
    assert_ne!(summary.reference(), first.reference());
    assert_eq!(summary.phase_instance_id(), None);
    assert!(matches!(
        doctor.lookup(&summary.reference()),
        QualityLookup::NotRetained { .. }
    ));
    let next = owner
        .context()
        .begin(protocol(), operation(), 7)
        .expect("lost Begin freed reservation");
    assert_ne!(next.reference(), summary.reference());
    assert_eq!(counters(&doctor).active_capacity_skipped(), 0);
}

#[test]
fn poisoned_loss_summary_storage_does_not_fail_the_collector_or_erase_loss() {
    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    report(&doctor, probe.reference());
    let poisoned = std::panic::catch_unwind(|| {
        let _summaries = doctor.inner.drop_summaries.lock().unwrap();
        panic!("intentional loss-summary storage poisoning");
    });
    assert!(poisoned.is_err());
    probe.mark(PhaseKind::ResponseHeaders);
    let ingress = doctor.inner.ingress.lock().unwrap();
    probe.mark(PhaseKind::FirstBodyChunk);
    drop(ingress);
    let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
    assert_eq!(doctor.state(), QualityDoctorState::Running);
    assert_eq!(snapshot.counters().ingress_dropped(), 1);
    assert_eq!(snapshot.counters().drop_summaries_lost(), 1);
    assert_eq!(snapshot.counters().ingress_failed_rejected(), 0);
    assert!(snapshot.recent_drops().is_empty());
    let observed = report(&doctor, probe.reference());
    assert_eq!(observed.phases().len(), 1);
    assert!(observed.possibly_incomplete());
}

#[test]
fn poisoned_ingress_exposes_failure_rejections_and_closes_observation_honestly() {
    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    let guard = probe.phase(PhaseKind::ExecuteToHeaders);
    assert_eq!(
        report(&doctor, probe.reference()).active_phases().count(),
        1
    );
    let poisoned = std::panic::catch_unwind(|| {
        let _ingress = doctor.inner.ingress.lock().unwrap();
        panic!("intentional ingress storage poisoning");
    });
    assert!(poisoned.is_err());
    probe.result(QualityOutcome::Success);
    let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
    assert_eq!(snapshot.state(), QualityDoctorState::Failed);
    assert_eq!(snapshot.counters().ingress_failed_rejected(), 1);
    assert_eq!(snapshot.counters().ingress_dropped(), 0);
    assert_eq!(snapshot.counters().ingress_closed_rejected(), 0);
    let observed = report(&doctor, probe.reference());
    assert_eq!(observed.state(), QualityObservationState::OwnerClosed);
    assert_eq!(observed.outcome(), None);
    assert!(observed.possibly_incomplete());
    assert_eq!(
        observed.phase_activity()[0].state(),
        QualityPhaseState::OwnerClosed
    );
    assert_eq!(observed.phase_activity()[0].end_offset(), None);
    assert!(observed.phases().is_empty());
    drop(guard);
}

#[test]
fn contended_loss_summary_lock_cannot_block_accepted_or_dropped_submissions() {
    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    report(&doctor, probe.reference());
    let summaries = doctor.inner.drop_summaries.lock().unwrap();
    // Healthy ingress does not acquire the advisory summary lock.
    probe.mark(PhaseKind::ResponseHeaders);
    let ingress = doctor.inner.ingress.lock().unwrap();
    assert_eq!(ingress.len(), 1);
    let producer = probe.clone();
    let (sent, received) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        producer.mark(PhaseKind::FirstBodyChunk);
        sent.send(()).unwrap();
    });
    let returned_while_locked = received.recv_timeout(Duration::from_secs(2));
    drop(ingress);
    drop(summaries);
    thread.join().unwrap();
    assert!(
        returned_while_locked.is_ok(),
        "producer waited for loss summary lock"
    );
    let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
    assert_eq!(snapshot.counters().ingress_dropped(), 1);
    assert_eq!(snapshot.counters().drop_summaries_lost(), 1);
    assert!(snapshot.recent_drops().is_empty());
    let observed = report(&doctor, probe.reference());
    assert_eq!(observed.phases().len(), 1);
    assert_eq!(observed.phases()[0].kind(), PhaseKind::ResponseHeaders);
    assert!(observed.possibly_incomplete());
}

#[tokio::test(start_paused = true)]
async fn loss_summary_history_rolls_and_expires_without_erasing_counters() {
    let (_owner, doctor, probe) = fixture(
        QualityDoctorConfig::detailed().with_limits(
            QualityLimits::default()
                .with_ingress_capacity(1)
                .with_retention(Duration::from_secs(2))
                .with_window(Duration::from_secs(2)),
        ),
    );
    report(&doctor, probe.reference());
    let ingress = doctor.inner.ingress.lock().unwrap();
    probe.mark(PhaseKind::ResponseHeaders);
    drop(ingress);
    let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
    assert_eq!(
        snapshot.recent_drops()[0].phase_kind(),
        Some(PhaseKind::ResponseHeaders)
    );
    tokio::time::advance(Duration::from_millis(500)).await;
    let ingress = doctor.inner.ingress.lock().unwrap();
    probe.mark(PhaseKind::FirstBodyChunk);
    drop(ingress);
    let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
    assert_eq!(snapshot.recent_drops().len(), 1);
    assert_eq!(
        snapshot.recent_drops()[0].phase_kind(),
        Some(PhaseKind::FirstBodyChunk)
    );
    assert_eq!(snapshot.counters().drop_summaries_lost(), 0);
    tokio::time::advance(Duration::from_secs(2)).await;
    let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
    assert!(snapshot.recent_drops().is_empty());
    assert_eq!(snapshot.counters().ingress_dropped(), 2);
    assert_eq!(snapshot.counters().drop_summaries_lost(), 0);
}

#[test]
fn cancelled_parent_does_not_complete_or_own_its_connection_candidate() {
    let (owner, doctor, request) = fixture(QualityDoctorConfig::detailed());
    let connection = owner
        .context()
        .begin(protocol(), QualityOperationKind::Connection, 7)
        .unwrap();
    connection.parent(request.reference());
    let connector = connection.phase(PhaseKind::Connector);
    request.result(QualityOutcome::Cancelled);
    request.finish();
    let candidate = report(&doctor, connection.reference());
    assert_eq!(candidate.associations().parent(), Some(request.reference()));
    assert_eq!(candidate.state(), QualityObservationState::InProgress);
    assert_eq!(candidate.active_phases().count(), 1);
    assert_eq!(candidate.outcome(), None);
    assert_eq!(candidate.associations().connection(), None);
    connector.finish();
    connection.result(QualityOutcome::Success);
    connection.finish();
    let candidate = report(&doctor, connection.reference());
    assert_eq!(candidate.outcome(), Some(QualityOutcome::Success));
    assert_eq!(
        candidate.phase_activity()[0].state(),
        QualityPhaseState::Ended
    );
    assert_eq!(
        report(&doctor, request.reference())
            .associations()
            .connection(),
        None
    );
}

#[test]
fn slow_subscriber_stays_bounded_and_reports_its_loss_separately() {
    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    let subscription = doctor
        .subscribe(QualitySubscriptionOptions::default().with_capacity(1))
        .unwrap();
    report(&doctor, probe.reference());
    let guard = probe.phase(PhaseKind::ExecuteToHeaders);
    report(&doctor, probe.reference());
    guard.finish();
    report(&doctor, probe.reference());
    probe.result(QualityOutcome::Success);
    probe.finish();
    let completed = report(&doctor, probe.reference());
    assert_eq!(subscription.inner.receiver.len(), 1);
    assert_eq!(
        completed.phase_activity()[0].state(),
        QualityPhaseState::Ended
    );
    assert!(!completed.possibly_incomplete());
    let counts = counters(&doctor);
    assert!(counts.subscriber_dropped() > 0);
    assert_eq!(counts.ingress_dropped(), 0);
}

#[test]
fn ingress_loss_publishes_gap_and_marks_affected_active_reports() {
    let (owner, doctor, first) = fixture(QualityDoctorConfig::detailed());
    let second = owner.context().begin(protocol(), operation(), 7).unwrap();
    let mut subscription = doctor
        .subscribe(QualitySubscriptionOptions::default())
        .unwrap();
    report(&doctor, first.reference());
    // Drain initial report updates, so the next observed gap belongs to loss.
    while subscription.try_recv().is_ok() {}
    let ingress = doctor.inner.ingress.lock().unwrap();
    first.mark(PhaseKind::ResponseHeaders);
    second.mark(PhaseKind::ResponseHeaders);
    drop(ingress);
    let mut dropped = 0;
    while let Ok(update) = subscription.try_recv() {
        if let QualityUpdate::Gap(gap) = update {
            assert_eq!(gap.scope(), QualityGapScope::Ingress);
            dropped += gap.dropped();
        }
    }
    assert_eq!(dropped, 2);
    assert!(report(&doctor, first.reference()).possibly_incomplete());
    assert!(report(&doctor, second.reference()).possibly_incomplete());
}

#[test]
fn unretained_loss_before_begin_still_marks_a_later_report_incomplete() {
    let (owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    // Reproduce a reader that has drained ingress before this Begin is queued.
    // The aggregate gap is visible to this reader, but its bounded summary is
    // also lost, so retaining summaries alone cannot carry the evidence forward.
    let mut ingress = doctor.inner.ingress.lock().unwrap();
    let begin = ingress.pop_front().expect("queued Begin");
    assert!(matches!(&begin.kind, EventKind::Begin { .. }));
    let summaries = doctor.inner.drop_summaries.lock().unwrap();
    probe.attempt(9);
    drop(summaries);
    drop(ingress);

    let first_batch = doctor.snapshot(QualityQuery::default()).unwrap();
    assert!(first_batch.reports().is_empty());
    assert_eq!(first_batch.counters().ingress_dropped(), 1);
    assert_eq!(first_batch.counters().drop_summaries_lost(), 1);
    assert!(first_batch.recent_drops().is_empty());

    doctor.inner.ingress.lock().unwrap().push_back(begin);
    probe.result(QualityOutcome::Success);
    probe.finish();
    let next_batch = doctor.snapshot(QualityQuery::default()).unwrap();
    assert_eq!(next_batch.counters().ingress_dropped(), 1);
    assert_eq!(next_batch.reports().len(), 1);
    let observed = &next_batch.reports()[0];
    assert_eq!(observed.state(), QualityObservationState::Completed);
    assert_eq!(observed.outcome(), Some(QualityOutcome::Success));
    assert_eq!(observed.associations().attempt(), None);
    assert!(observed.possibly_incomplete());
    assert_eq!(next_batch.summary().latency_samples(), 0);

    // The conservative carry must end after the pending Begin batch, rather
    // than marking unrelated reports incomplete forever after one lost event.
    let fresh = owner.context().begin(protocol(), operation(), 8).unwrap();
    fresh.result(QualityOutcome::Success);
    fresh.finish();
    let third_batch = doctor.snapshot(QualityQuery::default()).unwrap();
    let fresh_report = third_batch
        .reports()
        .iter()
        .find(|report| report.reference() == fresh.reference())
        .unwrap();
    assert!(!fresh_report.possibly_incomplete());
    assert_eq!(third_batch.summary().latency_samples(), 1);
}

#[test]
fn retained_loss_before_begin_still_marks_a_later_report_incomplete() {
    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    // Reproduce the reader interleaving without scheduling or sleeps: its
    // ingress batch does not yet contain Begin, but it observes a later loss.
    let mut ingress = doctor.inner.ingress.lock().unwrap();
    let begin = ingress.pop_front().expect("queued Begin");
    assert!(matches!(&begin.kind, EventKind::Begin { .. }));
    probe.attempt(9);
    drop(ingress);

    let first_batch = doctor.snapshot(QualityQuery::default()).unwrap();
    assert!(first_batch.reports().is_empty());
    assert_eq!(first_batch.counters().ingress_dropped(), 1);
    assert_eq!(first_batch.recent_drops().len(), 1);
    assert_eq!(first_batch.recent_drops()[0].reference(), probe.reference());
    assert_eq!(
        first_batch.recent_drops()[0].event_kind(),
        QualityEventKind::Association
    );

    // Begin and a successful finish arrive in the following batch. There is no
    // new aggregate gap, but the retained evidence still belongs to this report.
    doctor.inner.ingress.lock().unwrap().push_back(begin);
    probe.result(QualityOutcome::Success);
    probe.finish();
    let next_batch = doctor.snapshot(QualityQuery::default()).unwrap();
    assert_eq!(next_batch.counters().ingress_dropped(), 1);
    assert_eq!(next_batch.reports().len(), 1);
    let observed = &next_batch.reports()[0];
    assert_eq!(observed.reference(), probe.reference());
    assert_eq!(observed.state(), QualityObservationState::Completed);
    assert_eq!(observed.outcome(), Some(QualityOutcome::Success));
    assert_eq!(observed.associations().attempt(), None);
    assert!(observed.possibly_incomplete());
    assert_eq!(next_batch.summary().latency_samples(), 0);
}

#[test]
fn first_network_result_is_immutable_during_callback_and_cleanup() {
    use crate::api::error::{ErrorKind, ErrorStage};
    for (kind, outcome) in [
        (ErrorKind::TimedOut, QualityOutcome::Timeout),
        (ErrorKind::DeadlineExceeded, QualityOutcome::Timeout),
        (ErrorKind::Cancelled, QualityOutcome::Cancelled),
    ] {
        let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
        probe.failure(kind, Some(ErrorStage::Response));
        let selected = report(&doctor, probe.reference());
        let callback = probe.phase(PhaseKind::Callback);
        probe.failure(ErrorKind::CallbackPanicked, Some(ErrorStage::Dispatch));
        callback.finish();
        probe.finish();
        let completed = report(&doctor, probe.reference());
        assert_eq!(completed.outcome(), Some(outcome));
        assert_eq!(completed.failure().unwrap().kind(), kind);
        assert_eq!(
            completed.failure().unwrap().stage(),
            Some(ErrorStage::Response)
        );
        assert_eq!(completed.result_offset(), selected.result_offset());
        assert_eq!(completed.result_duration(), selected.result_duration());
    }
}

#[test]
fn opaque_identity_is_scoped_and_does_not_depend_on_application_request_ids() {
    let (_first_owner, first_doctor, first) = fixture(QualityDoctorConfig::detailed());
    let (_second_owner, second_doctor, second) = fixture(QualityDoctorConfig::detailed());
    assert_ne!(first.reference(), second.reference());
    assert_ne!(first.reference().scope(), second.reference().scope());
    assert!(matches!(
        first_doctor.lookup(&second.reference()),
        QualityLookup::WrongScope
    ));
    assert!(matches!(
        second_doctor.lookup(&first.reference()),
        QualityLookup::WrongScope
    ));
}

#[test]
fn report_debug_and_json_never_retain_error_sources_or_sensitive_payloads() {
    #[derive(Debug)]
    struct SensitiveSource(Arc<()>);
    impl std::fmt::Display for SensitiveSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let _ = &self.0;
            f.write_str("https://user:secret@private.example/path?token=secret 192.0.2.77 Authorization: Bearer secret body-secret")
        }
    }
    impl std::error::Error for SensitiveSource {}

    let (_owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
    let owned = Arc::new(());
    let source_lifetime = Arc::downgrade(&owned);
    let error = crate::api::error::NetError::provider(SensitiveSource(owned));
    let phase = probe.phase(PhaseKind::Provider);
    probe.error(&error);
    phase.finish();
    probe.finish();
    drop(error);
    assert!(source_lifetime.upgrade().is_none());
    let observed = report(&doctor, probe.reference());
    for output in [observed.to_json().unwrap(), format!("{observed:?}")] {
        for secret in [
            "private.example",
            "192.0.2.77",
            "secret",
            "Authorization",
            "SensitiveSource",
        ] {
            assert!(
                !output.contains(secret),
                "diagnostic export retained {secret}"
            );
        }
    }
}

#[test]
fn accepted_next_batch_is_closed_after_the_first_close_notification() {
    for failed in [false, true] {
        let (owner, doctor, probe) = fixture(QualityDoctorConfig::detailed());
        let mut subscription = doctor
            .subscribe(QualitySubscriptionOptions::default())
            .unwrap();
        let guard = probe.phase(PhaseKind::ExecuteToHeaders);
        // The producer accepted these events after a reader drained ingress,
        // but before owner closure/failure. Hold that next batch aside so the
        // reader deterministically completes its empty batch and closes first.
        let delayed_batch = doctor
            .inner
            .ingress
            .lock()
            .unwrap()
            .drain(..)
            .collect::<Vec<_>>();
        assert_eq!(delayed_batch.len(), 2);
        if failed {
            doctor.inner.fail();
        } else {
            owner.close();
        }
        let expected_state = if failed {
            QualityDoctorState::Failed
        } else {
            QualityDoctorState::Closed
        };
        let first = doctor.snapshot(QualityQuery::default()).unwrap();
        assert_eq!(first.state(), expected_state);
        assert!(first.reports().is_empty());
        assert!(doctor.inner.store.lock().unwrap().closed);
        assert!(matches!(
            subscription.inner.receiver.try_recv(),
            Ok(QualityUpdate::Closed)
        ));
        assert!(matches!(
            subscription.inner.receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));

        doctor.inner.ingress.lock().unwrap().extend(delayed_batch);
        let next = doctor.snapshot(QualityQuery::default()).unwrap();
        assert_eq!(next.state(), expected_state);
        assert_eq!(next.reports().len(), 1);
        let observed = &next.reports()[0];
        assert_eq!(observed.state(), QualityObservationState::OwnerClosed);
        assert_eq!(observed.outcome(), None);
        assert!(observed.possibly_incomplete());
        assert_eq!(observed.active_phases().count(), 0);
        assert_eq!(observed.phase_activity().len(), 1);
        assert_eq!(
            observed.phase_activity()[0].state(),
            QualityPhaseState::OwnerClosed
        );
        assert_eq!(observed.phase_activity()[0].end_offset(), None);
        assert!(observed.phases().is_empty());
        assert_eq!(next.summary().latency_samples(), 0);

        let revision = next.revision();
        let report_revision = observed.revision();
        let stable = doctor.snapshot(QualityQuery::default()).unwrap();
        assert_eq!(stable.revision(), revision);
        assert_eq!(stable.reports()[0].revision(), report_revision);
        assert!(matches!(
            subscription.inner.receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));
        drop(guard);
    }
}

#[test]
fn concurrent_phase_reservations_and_finish_remain_unique_and_bounded() {
    const THREADS: usize = 4;
    const PER_THREAD: usize = 32;
    const PHASE_LIMIT: usize = 32;
    let (owner, doctor, probe) = fixture(
        QualityDoctorConfig::detailed().with_limits(
            QualityLimits::default()
                .with_active_capacity(1)
                .with_max_phases(PHASE_LIMIT),
        ),
    );
    probe.attempt(7);
    probe.hop(3);
    // Begin/associations are visible before the producer contention starts.
    report(&doctor, probe.reference());
    let barrier = Arc::new(std::sync::Barrier::new(THREADS));
    let reserved = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for _ in 0..THREADS {
            let producer = probe.clone();
            let barrier = barrier.clone();
            workers.push(scope.spawn(move || {
                barrier.wait();
                let mut ids = Vec::new();
                for _ in 0..PER_THREAD {
                    let phase = producer.phase(PhaseKind::ExecuteToHeaders);
                    if phase.instance_id != 0 {
                        ids.push(phase.instance_id);
                    }
                    phase.finish();
                }
                ids
            }));
        }
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    let mut reserved = reserved;
    reserved.sort_unstable();
    assert_eq!(reserved, (1..=PHASE_LIMIT as u64).collect::<Vec<_>>());
    assert_eq!(
        probe.lease.phase_events.load(Ordering::Relaxed),
        PHASE_LIMIT as u64
    );
    assert_eq!(
        probe.lease.phase_overflow.load(Ordering::Relaxed),
        (THREADS * PER_THREAD - PHASE_LIMIT) as u64
    );

    // Result is submitted outside contention; many simultaneous finalizers
    // must still release one active reservation and retain that same result.
    probe.result(QualityOutcome::Success);
    let barrier = Arc::new(std::sync::Barrier::new(THREADS));
    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            let producer = probe.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                barrier.wait();
                producer.finish();
            });
        }
    });
    assert_eq!(doctor.inner.active_probes.load(Ordering::Relaxed), 0);
    let observed = report(&doctor, probe.reference());
    assert_eq!(observed.state(), QualityObservationState::Completed);
    assert_eq!(observed.outcome(), Some(QualityOutcome::Success));
    assert!(observed.possibly_incomplete());
    assert!(observed.phase_activity().len() <= PHASE_LIMIT);
    assert!(observed.phases().len() <= PHASE_LIMIT);
    assert_eq!(observed.active_phases().count(), 0);
    let mut activity_ids = std::collections::HashSet::new();
    let mut legacy_ids = std::collections::HashSet::new();
    // Contention may deliberately lose observations. Every retained ID must
    // nevertheless be unique in each view, within budget and self-consistent.
    for activity in observed.phase_activity() {
        assert!(activity_ids.insert(activity.instance_id()));
        assert!((1..=PHASE_LIMIT as u64).contains(&activity.instance_id()));
        assert_eq!(activity.associations().attempt(), Some(7));
        assert_eq!(activity.associations().hop(), Some(3));
        if let Some(end) = activity.end_offset() {
            assert!(end >= activity.start_offset());
        }
    }
    for phase in observed.phases() {
        assert!(legacy_ids.insert(phase.instance_id()));
        let activity = observed
            .phase_activity()
            .iter()
            .find(|activity| activity.instance_id() == phase.instance_id())
            .expect("retained interval end has one lifecycle entry");
        assert_eq!(activity.state(), QualityPhaseState::Ended);
        assert_eq!(activity.start_offset(), phase.start_offset());
        assert_eq!(activity.end_offset(), phase.end_offset());
        assert_eq!(
            activity.associations().attempt(),
            phase.associations().attempt()
        );
        assert_eq!(activity.associations().hop(), phase.associations().hop());
    }
    let next = owner.context().begin(protocol(), operation(), 8).unwrap();
    assert_eq!(doctor.inner.active_probes.load(Ordering::Relaxed), 1);
    probe.finish();
    assert_eq!(doctor.inner.active_probes.load(Ordering::Relaxed), 1);
    next.finish();
    assert_eq!(doctor.inner.active_probes.load(Ordering::Relaxed), 0);
}

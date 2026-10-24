//! Deterministic observation regressions for the heartbeat deadline boundary.
//! Real heartbeat transitions feed a real collector; no network, sleep-based
//! scheduling assumptions, or direct mutation of private heartbeat state.

use super::heartbeat_state::{HeartbeatState, HeartbeatTick};
use super::test_support::{check, check_eq, check_ne, test_error, TestResult};
use crate::api::quality::{
    QualityDoctor, QualityDoctorConfig, QualityDoctorState, QualityLimits, QualityLookup,
    QualityObservationState, QualityOperationKind, QualityOutcome, QualityProtocols, QualityQuery,
    QualityReport, QualityReportRef, QualitySnapshot,
};
use crate::module::quality::QualityOwner;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

const TIMEOUT: Duration = Duration::from_secs(5);

fn fixture() -> TestResult<(QualityOwner, QualityDoctor, HeartbeatState)> {
    // A single active reservation also verifies that completed timeout reports
    // cannot prevent a recovery probe from being observed.
    let config = QualityDoctorConfig::detailed()
        .with_protocols(QualityProtocols::websocket())
        .with_limits(QualityLimits::default().with_active_capacity(1));
    config.validate()?;
    let owner = QualityOwner::new(config);
    let doctor = owner
        .doctor()
        .ok_or_else(|| test_error("configured doctor missing"))?;
    let heartbeat = HeartbeatState::new(42);
    heartbeat.bind_quality(owner.context(), 7, 9);
    Ok((owner, doctor, heartbeat))
}

fn begin(heartbeat: &HeartbeatState) -> TestResult<Bytes> {
    match heartbeat.on_tick(Instant::now(), TIMEOUT) {
        HeartbeatTick::SendProbe(payload) => Ok(payload),
        other => Err(test_error(format!(
            "expected a new heartbeat, got {other:?}"
        ))),
    }
}

fn snapshot(doctor: &QualityDoctor) -> TestResult<Arc<QualitySnapshot>> {
    let snapshot = doctor.snapshot(
        QualityQuery::default()
            .with_kind(QualityOperationKind::WsHeartbeat)
            .with_client_id(7),
    )?;
    check_eq!(snapshot.counters().ingress_dropped(), 0)?;
    check_eq!(snapshot.counters().ingress_closed_rejected(), 0)?;
    check_eq!(snapshot.counters().ingress_failed_rejected(), 0)?;
    check_eq!(snapshot.counters().active_capacity_skipped(), 0)?;
    check_eq!(snapshot.counters().records_expired(), 0)?;
    check_eq!(snapshot.counters().records_evicted(), 0)?;
    Ok(snapshot)
}

fn only_report(doctor: &QualityDoctor) -> TestResult<QualityReport> {
    let snapshot = snapshot(doctor)?;
    check_eq!(snapshot.reports().len(), 1)?;
    snapshot
        .reports()
        .first()
        .cloned()
        .ok_or_else(|| test_error("heartbeat report missing"))
}

fn report(doctor: &QualityDoctor, reference: QualityReportRef) -> TestResult<Arc<QualityReport>> {
    match doctor.lookup(&reference) {
        QualityLookup::Available(report) => Ok(report),
        other => Err(test_error(format!(
            "heartbeat report unavailable: {other:?}"
        ))),
    }
}

fn completed(report: &QualityReport, outcome: QualityOutcome) -> TestResult {
    check_eq!(report.state(), QualityObservationState::Completed)?;
    check_eq!(report.outcome(), Some(outcome))?;
    check!(report.result_offset().is_some())?;
    check!(report.finished_offset().is_some())?;
    check!(!report.possibly_incomplete())?;
    check_eq!(report.associations().connection(), Some(42))?;
    check_eq!(report.associations().session(), Some(9))?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn exact_deadline_completes_timeout_before_another_tick_or_owner_shutdown() -> TestResult {
    let (owner, doctor, heartbeat) = fixture()?;
    let payload = begin(&heartbeat)?;
    let sent_at = Instant::now();
    check!(heartbeat.mark_sent(&payload, sent_at))?;
    let pending = only_report(&doctor)?;
    check_eq!(pending.state(), QualityObservationState::InProgress)?;
    check_eq!(pending.outcome(), None)?;

    tokio::time::advance(TIMEOUT - Duration::from_nanos(1)).await;
    check!(!heartbeat.is_timed_out(Instant::now()))?;
    check_eq!(only_report(&doctor)?.outcome(), None)?;
    tokio::time::advance(Duration::from_nanos(1)).await;
    check!(heartbeat.is_timed_out(Instant::now()))?;

    let timeout = only_report(&doctor)?;
    completed(&timeout, QualityOutcome::Timeout)?;
    check_eq!(timeout.reference(), pending.reference())?;
    check_eq!(doctor.state(), QualityDoctorState::Running)?;
    check_eq!(snapshot(&doctor)?.summary().timeouts(), 1)?;
    // Diagnostics finish, but the writer still sees the same expired transport
    // probe and can make its existing recovery/connection-close decision.
    check!(heartbeat.has_outstanding_probe())?;
    check_eq!(heartbeat.pong_deadline(), Some(sent_at + TIMEOUT))?;

    for _ in 0..3 {
        check!(heartbeat.is_timed_out(Instant::now()))?;
    }
    check_eq!(
        heartbeat.on_tick(Instant::now(), TIMEOUT),
        HeartbeatTick::TimedOut
    )?;
    check_eq!(only_report(&doctor)?.revision(), timeout.revision())?;
    owner.close();
    check_eq!(doctor.state(), QualityDoctorState::Closed)?;
    completed(&only_report(&doctor)?, QualityOutcome::Timeout)?;
    check_eq!(
        pending.outcome(),
        None,
        "prior report copy must stay immutable"
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn unsent_ping_does_not_start_the_pong_observation_timeout() -> TestResult {
    let (_owner, doctor, heartbeat) = fixture()?;
    let payload = begin(&heartbeat)?;
    tokio::time::advance(TIMEOUT * 3).await;
    check_eq!(heartbeat.pong_deadline(), None)?;
    check!(!heartbeat.is_timed_out(Instant::now()))?;
    let sending = only_report(&doctor)?;
    check_eq!(sending.state(), QualityObservationState::InProgress)?;
    check_eq!(sending.outcome(), None)?;

    let sent_at = Instant::now();
    check!(heartbeat.mark_sent(&payload, sent_at))?;
    check!(!heartbeat.is_timed_out(sent_at))?;
    tokio::time::advance(TIMEOUT).await;
    check!(heartbeat.is_timed_out(Instant::now()))?;
    completed(&only_report(&doctor)?, QualityOutcome::Timeout)?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn matched_pong_before_deadline_preserves_success_including_early_send_completion(
) -> TestResult {
    for pong_before_mark_sent in [true, false] {
        let (_owner, doctor, heartbeat) = fixture()?;
        let payload = begin(&heartbeat)?;
        if !pong_before_mark_sent {
            check!(heartbeat.mark_sent(&payload, Instant::now()))?;
            tokio::time::advance(TIMEOUT - Duration::from_nanos(1)).await;
        }
        check!(heartbeat.acknowledge_pong(&payload))?;
        check!(!heartbeat.has_outstanding_probe())?;
        if pong_before_mark_sent {
            check!(!heartbeat.mark_sent(&payload, Instant::now()))?;
        }
        let success = only_report(&doctor)?;
        completed(&success, QualityOutcome::Success)?;
        tokio::time::advance(TIMEOUT).await;
        check!(!heartbeat.is_timed_out(Instant::now()))?;
        check_eq!(only_report(&doctor)?.revision(), success.revision())?;
        check_eq!(snapshot(&doctor)?.summary().timeouts(), 0)?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn late_pong_cannot_replace_the_timeout_before_or_after_deadline_observation() -> TestResult {
    for pong_before_timeout_check in [true, false] {
        let (_owner, doctor, heartbeat) = fixture()?;
        let payload = begin(&heartbeat)?;
        check!(heartbeat.mark_sent(&payload, Instant::now()))?;
        tokio::time::advance(TIMEOUT).await;
        if pong_before_timeout_check {
            check!(!heartbeat.acknowledge_pong(&payload))?;
        }
        check!(heartbeat.is_timed_out(Instant::now()))?;
        let timeout = only_report(&doctor)?;
        completed(&timeout, QualityOutcome::Timeout)?;
        check!(!heartbeat.acknowledge_pong(&payload))?;
        check!(heartbeat.has_outstanding_probe())?;
        check_eq!(only_report(&doctor)?.revision(), timeout.revision())?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn recovery_observes_a_new_probe_without_reopening_or_duplicating_the_expired_report(
) -> TestResult {
    // Exercise all existing timeout entry points. The exact-deadline observer,
    // interval tick and recovery replacement must agree without double-finishing.
    for observation in ["deadline", "interval", "recovery"] {
        let (_owner, doctor, heartbeat) = fixture()?;
        let first = begin(&heartbeat)?;
        check!(heartbeat.mark_sent(&first, Instant::now()))?;
        let first_ref = only_report(&doctor)?.reference();
        tokio::time::advance(TIMEOUT).await;
        match observation {
            "deadline" => check!(heartbeat.is_timed_out(Instant::now()))?,
            "interval" => check_eq!(
                heartbeat.on_tick(Instant::now(), TIMEOUT),
                HeartbeatTick::TimedOut
            )?,
            _ => {}
        }
        let next = heartbeat
            .start_recovery_probe(Instant::now(), TIMEOUT)
            .ok_or_else(|| test_error("expired heartbeat did not permit recovery"))?;
        check_ne!(first, next)?;
        completed(
            report(&doctor, first_ref)?.as_ref(),
            QualityOutcome::Timeout,
        )?;
        let first_revision = report(&doctor, first_ref)?.revision();
        let during_recovery = snapshot(&doctor)?;
        check_eq!(during_recovery.reports().len(), 2)?;
        check_eq!(during_recovery.summary().timeouts(), 1)?;
        check_eq!(during_recovery.summary().completed(), 1)?;
        let recovery_ref = during_recovery
            .reports()
            .iter()
            .find(|report| report.reference() != first_ref)
            .ok_or_else(|| test_error("recovery diagnostic missing"))?
            .reference();
        check!(!heartbeat.is_timed_out(Instant::now()))?;
        check!(!heartbeat.acknowledge_pong(&first))?;
        check!(heartbeat.mark_sent(&next, Instant::now()))?;
        check!(heartbeat.acknowledge_pong(&next))?;
        completed(
            report(&doctor, recovery_ref)?.as_ref(),
            QualityOutcome::Success,
        )?;
        check_eq!(report(&doctor, first_ref)?.revision(), first_revision)?;
        let finished = snapshot(&doctor)?;
        check_eq!(finished.summary().timeouts(), 1)?;
        check_eq!(finished.summary().successes(), 1)?;
        check_eq!(finished.summary().completed(), 2)?;
        check_eq!(finished.summary().incomplete(), 0)?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn deadline_detection_does_not_activate_dormant_collection() -> TestResult {
    let owner = QualityOwner::new(
        QualityDoctorConfig::detailed().with_protocols(QualityProtocols::websocket()),
    );
    let heartbeat = HeartbeatState::new(42);
    heartbeat.bind_quality(owner.context(), 7, 9);
    let payload = begin(&heartbeat)?;
    check!(heartbeat.mark_sent(&payload, Instant::now()))?;
    tokio::time::advance(TIMEOUT).await;
    check!(heartbeat.is_timed_out(Instant::now()))?;
    let doctor = owner
        .doctor()
        .ok_or_else(|| test_error("configured doctor missing"))?;
    let observed = snapshot(&doctor)?;
    check!(observed.reports().is_empty())?;
    check_eq!(observed.counters().begin_attempted(), 0)?;
    check!(heartbeat.is_timed_out(Instant::now()))?;
    check!(snapshot(&doctor)?.reports().is_empty())?;
    Ok(())
}

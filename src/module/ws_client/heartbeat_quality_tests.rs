//! Deterministic observation regressions for the heartbeat deadline boundary.
//! Real heartbeat transitions feed a real collector; no network, sleep-based
//! scheduling assumptions, or direct mutation of private heartbeat state.

use super::heartbeat_state::{HeartbeatState, HeartbeatTick};
use super::test_support::{check, check_eq, check_ne, test_error, TestResult};
use crate::api::quality::{
    PhaseKind, QualityDoctor, QualityDoctorConfig, QualityDoctorState, QualityLimits,
    QualityLookup, QualityObservationState, QualityOperationKind, QualityOutcome, QualityProtocols,
    QualityQuery, QualityReport, QualityReportRef, QualitySnapshot,
};
use crate::module::quality::QualityOwner;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

const TIMEOUT: Duration = Duration::from_secs(5);

fn fixture() -> TestResult<(QualityOwner, QualityDoctor, HeartbeatState)> {
    fixture_with_config(QualityDoctorConfig::detailed())
}

fn fixture_with_config(
    config: QualityDoctorConfig,
) -> TestResult<(QualityOwner, QualityDoctor, HeartbeatState)> {
    // A single active reservation also verifies that completed timeout reports
    // cannot prevent a recovery probe from being observed.
    let config = config
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
    let failure = timeout
        .failure()
        .ok_or_else(|| test_error("timeout cause missing"))?;
    check_eq!(failure.kind(), crate::error::ErrorKind::TimedOut)?;
    check_eq!(failure.stage(), Some(crate::error::ErrorStage::Heartbeat))?;
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
        heartbeat.observe_write_started(&payload);
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
        check!(success
            .phases()
            .iter()
            .any(|phase| phase.kind() == PhaseKind::HeartbeatObserved))?;
        check!(success
            .phases()
            .iter()
            .any(|phase| phase.kind() == PhaseKind::HeartbeatWriteStarted))?;
        check_eq!(
            success
                .phases()
                .iter()
                .any(|phase| phase.kind() == PhaseKind::Written),
            !pong_before_mark_sent
        )?;
        check!(success
            .phases()
            .iter()
            .any(|phase| phase.kind() == PhaseKind::Heartbeat))?;
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
        check_eq!(
            report(&doctor, recovery_ref)?.heartbeat_schedule_delay(),
            None
        )?;
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
    let payload = match heartbeat.on_scheduled_tick(Instant::now(), TIMEOUT, Some(Instant::now())) {
        HeartbeatTick::SendProbe(payload) => payload,
        other => return Err(test_error(format!("unexpected dormant tick: {other:?}"))),
    };
    heartbeat.observe_write_started(&payload);
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

#[tokio::test(start_paused = true)]
async fn scheduled_delay_and_write_pong_milestones_share_one_probe_without_backdating() -> TestResult
{
    use super::heartbeat_schedule::HeartbeatSchedule;
    for config in [
        QualityDoctorConfig::basic(),
        QualityDoctorConfig::detailed(),
    ] {
        let (_owner, doctor, heartbeat) = fixture_with_config(config)?;
        let mut schedule = HeartbeatSchedule::new(Some(crate::ws::HeartbeatConfig {
            interval: Duration::from_secs(5),
            pong_timeout: TIMEOUT,
            ..Default::default()
        }))?;
        tokio::time::advance(Duration::from_secs(12)).await;
        let due = schedule.tick().await;
        // A pre-existing ready control can run after taking the tick but before
        // creating the probe. Its time is part of scheduled-to-observed delay.
        tokio::time::advance(Duration::from_millis(3)).await;
        let payload =
            match heartbeat.on_scheduled_tick(Instant::now(), due.pong_timeout, due.scheduled_at) {
                HeartbeatTick::SendProbe(payload) => payload,
                other => return Err(test_error(format!("unexpected scheduled tick: {other:?}"))),
            };
        let created = only_report(&doctor)?;
        check_eq!(
            created.heartbeat_schedule_delay(),
            Some(Duration::from_millis(7003))
        )?;
        check!(created.started_offset() >= Duration::from_secs(12))?;
        let probe_ref = created.reference();
        // Another tick while Sending must not overwrite the first probe's lag
        // or create an observation for a Ping that was never emitted.
        check_eq!(
            heartbeat.on_scheduled_tick(
                Instant::now(),
                TIMEOUT,
                Some(Instant::now() - Duration::from_secs(10))
            ),
            HeartbeatTick::Waiting
        )?;
        check_eq!(only_report(&doctor)?.revision(), created.revision())?;
        tokio::time::advance(Duration::from_millis(2)).await;
        heartbeat.observe_write_started(&payload);
        tokio::time::advance(Duration::from_millis(4)).await;
        check!(heartbeat.mark_sent(&payload, Instant::now()))?;
        tokio::time::advance(Duration::from_millis(6)).await;
        check!(heartbeat.acknowledge_pong(&payload))?;
        let success = only_report(&doctor)?;
        completed(&success, QualityOutcome::Success)?;
        check_eq!(success.reference(), probe_ref)?;
        let phases: Vec<_> = success.phases().iter().map(|phase| phase.kind()).collect();
        check_eq!(
            phases,
            vec![
                PhaseKind::HeartbeatObserved,
                PhaseKind::HeartbeatWriteStarted,
                PhaseKind::Written,
                PhaseKind::Heartbeat
            ]
        )?;
        let stamps: Vec<_> = success
            .phases()
            .iter()
            .map(|phase| phase.start_offset())
            .collect();
        check_eq!(stamps[1] - stamps[0], Duration::from_millis(2))?;
        check_eq!(stamps[2] - stamps[1], Duration::from_millis(4))?;
        check_eq!(stamps[3] - stamps[2], Duration::from_millis(6))?;
        check!(success
            .phases()
            .iter()
            .all(|phase| phase.associations().connection() == Some(42)
                && phase.associations().session() == Some(9)))?;
        let json = serde_json::to_value(&success)?;
        check_eq!(
            json["heartbeat_schedule_delay"]["secs"],
            serde_json::json!(7)
        )?;
        check_eq!(
            json["heartbeat_schedule_delay"]["nanos"],
            serde_json::json!(3_000_000)
        )?;
        check!(!json.to_string().contains("payload"))?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn safe_write_retry_milestones_keep_probe_identity_and_failure_cause() -> TestResult {
    use crate::error::{ErrorKind, ErrorStage, NetError};
    for (kind, outcome) in [
        (ErrorKind::DeliveryUnknown, QualityOutcome::Failed),
        (ErrorKind::Cancelled, QualityOutcome::Cancelled),
    ] {
        let (_owner, doctor, heartbeat) = fixture()?;
        let payload = begin(&heartbeat)?;
        let original = only_report(&doctor)?.reference();
        heartbeat.observe_write_started(b"stale-probe");
        heartbeat.observe_write_started(&payload);
        tokio::time::advance(Duration::from_millis(5)).await;
        // A retryable WriteBufferFull retains the same probe and does not start
        // the Pong deadline; the writer emits another attempt milestone.
        heartbeat.observe_write_started(&payload);
        check_eq!(heartbeat.pong_deadline(), None)?;
        let error = NetError::from(kind).with_stage(ErrorStage::Write);
        heartbeat.fail_probe(&payload, &error);
        let failed = only_report(&doctor)?;
        completed(&failed, outcome)?;
        check_eq!(failed.reference(), original)?;
        check_eq!(failed.heartbeat_schedule_delay(), None)?;
        check!(!serde_json::to_value(&failed)?
            .as_object()
            .ok_or_else(|| test_error("report was not an object"))?
            .contains_key("heartbeat_schedule_delay"))?;
        let failure = failed
            .failure()
            .ok_or_else(|| test_error("write failure missing"))?;
        check_eq!(failure.kind(), kind)?;
        check_eq!(failure.stage(), Some(ErrorStage::Write))?;
        let phases: Vec<_> = failed.phases().iter().map(|phase| phase.kind()).collect();
        check_eq!(
            phases,
            vec![
                PhaseKind::HeartbeatObserved,
                PhaseKind::HeartbeatWriteStarted,
                PhaseKind::HeartbeatWriteStarted
            ]
        )?;
        check!(!heartbeat.has_outstanding_probe())?;
        heartbeat.observe_write_started(&payload);
        heartbeat.fail_probe(&payload, &error);
        check!(!heartbeat.mark_sent(&payload, Instant::now()))?;
        check!(!heartbeat.acknowledge_pong(&payload))?;
        check_eq!(only_report(&doctor)?.revision(), failed.revision())?;
    }
    Ok(())
}

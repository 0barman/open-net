//! Opt-in synthetic diagnostic measurements, with no sockets or background work.
//!
//! These measurements compare total enabled-diagnostic cost with the current
//! Off/dormant implementation, not the incremental cost of a historical patch.
//! They do not measure HTTP latency, contention, JSON export, allocations, RSS,
//! VPN/TUN behavior, or reader/producer concurrency. A process RSS high-water
//! mark cannot substitute for allocation counts or establish absence of leaks.

#![cfg(test)]
#![cfg(any(feature = "http-client", feature = "ws-client"))]

use super::*;
use std::{hint::black_box, time::Instant as WallInstant};

const MODES: [Mode; 5] = [
    Mode::Off,
    Mode::Dormant,
    Mode::Basic,
    Mode::DetailedZero,
    Mode::DetailedFull,
];
const ROUNDS: usize = 5;
const OPERATIONS_PER_BATCH: usize = 32;
const WARMUP_BATCHES: usize = 8;
const MEASURED_BATCHES: usize = 64;
const STEADY_READS: usize = 64;

#[derive(Clone, Copy)]
enum Mode {
    Off,
    Dormant,
    Basic,
    DetailedZero,
    DetailedFull,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Dormant => "configured_unactivated",
            Self::Basic => "basic",
            Self::DetailedZero => "detailed_sampling_0",
            Self::DetailedFull => "detailed_sampling_100",
        }
    }

    fn config(self) -> QualityDoctorConfig {
        match self {
            Self::Off => QualityDoctorConfig::default(),
            Self::Dormant | Self::DetailedFull => QualityDoctorConfig::detailed(),
            Self::Basic => QualityDoctorConfig::basic(),
            Self::DetailedZero => {
                QualityDoctorConfig::detailed().with_sampling(QualitySampling::new(0.0, 0.0))
            }
        }
    }

    fn activated(self) -> bool {
        matches!(self, Self::Basic | Self::DetailedZero | Self::DetailedFull)
    }
}

fn protocol_and_operation() -> (QualityProtocol, QualityOperationKind) {
    if cfg!(feature = "http-client") {
        (QualityProtocol::Http, QualityOperationKind::HttpRequest)
    } else {
        (QualityProtocol::WebSocket, QualityOperationKind::WsRequest)
    }
}

/// One common phase is retained in all activated modes. Detailed-full adds its
/// start event. Arc creation/drop and phase/result/finalization are timed; no
/// arbitrary simulated network work or diagnostics read is included.
#[inline(never)]
fn produce_batch(context: &QualityContext) {
    let (protocol, kind) = protocol_and_operation();
    for _ in 0..OPERATIONS_PER_BATCH {
        if let Some(probe) = black_box(context).begin(black_box(protocol), black_box(kind), 7) {
            let phase = probe.phase(PhaseKind::Admission);
            black_box(&phase);
            drop(phase);
            probe.result(QualityOutcome::Success);
            probe.finish();
            black_box(&probe);
        }
    }
}

fn assert_healthy(snapshot: &QualitySnapshot, expected: usize, mode: Mode) {
    let counters = snapshot.counters();
    assert_eq!(snapshot.state(), QualityDoctorState::Running);
    assert_eq!(counters.begin_attempted(), expected as u64);
    assert_eq!(counters.begin_enqueued(), expected as u64);
    assert_eq!(counters.ingress_dropped(), 0);
    assert_eq!(counters.ingress_closed_rejected(), 0);
    assert_eq!(counters.ingress_failed_rejected(), 0);
    assert_eq!(counters.active_capacity_skipped(), 0);
    assert_eq!(counters.drop_summaries_lost(), 0);
    assert_eq!(counters.orphan_events(), 0);
    assert_eq!(counters.records_expired(), 0);
    assert_eq!(counters.id_exhausted(), 0);
    assert!(!counters.saturated());
    let limits = QualityLimits::default();
    assert_eq!(
        counters.records_evicted(),
        expected.saturating_sub(limits.history_capacity()) as u64
    );
    let retained = expected.min(limits.history_capacity()) as u64;
    assert_eq!(snapshot.summary().retained(), retained);
    assert_eq!(snapshot.summary().completed(), retained);
    assert_eq!(snapshot.summary().successes(), retained);
    assert_eq!(snapshot.summary().incomplete(), 0);
    assert_eq!(snapshot.summary().latency_samples(), retained);
    assert_eq!(
        snapshot.reports().len(),
        (retained as usize).min(limits.max_query_results())
    );
    let full = matches!(mode, Mode::DetailedFull);
    // Four events per Basic/unsampled operation, five per Detailed operation.
    assert_eq!(
        counters.ingress_high_watermark(),
        (OPERATIONS_PER_BATCH * if full { 5 } else { 4 }) as u64
    );
    for report in snapshot.reports() {
        assert!(!report.possibly_incomplete());
        assert_eq!(report.phases().len(), 1);
        assert_eq!(report.phase_activity().len(), usize::from(full));
        assert_eq!(report.reference().detail_selected(), full);
    }
}

/// Percentiles describe batch averages, not tails of individual operations.
fn timing_summary(samples: &[u64], operations_per_sample: usize) -> serde_json::Value {
    if samples.is_empty() {
        return serde_json::Value::Null;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let total: u64 = sorted.iter().sum();
    let divisor = operations_per_sample as f64;
    let percentile = |percent: usize| {
        sorted[(sorted.len() * percent).div_ceil(100).saturating_sub(1)] as f64 / divisor
    };
    serde_json::json!({
        "sample_count": sorted.len(),
        "operations_per_sample": operations_per_sample,
        "total_ns": total,
        "mean_ns_per_operation": total as f64 / (sorted.len() as f64 * divisor),
        "min_sample_mean_ns": sorted[0] as f64 / divisor,
        "p50_sample_mean_ns": percentile(50),
        "p95_sample_mean_ns": percentile(95),
        "max_sample_mean_ns": sorted[sorted.len() - 1] as f64 / divisor,
    })
}

fn measure_round(mode: Mode, round: usize, position: usize) -> serde_json::Value {
    let config = mode.config();
    config.validate().expect("valid benchmark configuration");
    let owner = QualityOwner::new(config);
    let context = owner.context();
    // Initialization and activation costs are deliberately outside the window.
    let doctor = mode.activated().then(|| owner.doctor().unwrap());
    let mut expected = 0;
    for _ in 0..WARMUP_BATCHES {
        produce_batch(&context);
        expected += OPERATIONS_PER_BATCH;
        if let Some(doctor) = &doctor {
            let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
            assert_healthy(&snapshot, expected, mode);
        }
    }

    let mut producer_ns = Vec::with_capacity(MEASURED_BATCHES);
    let mut drain_snapshot_ns = Vec::with_capacity(MEASURED_BATCHES);
    let mut steady_snapshot_ns = Vec::with_capacity(STEADY_READS);
    for _ in 0..MEASURED_BATCHES {
        let started = WallInstant::now();
        produce_batch(&context);
        producer_ns.push(started.elapsed().as_nanos() as u64);
        expected += OPERATIONS_PER_BATCH;
        if let Some(doctor) = &doctor {
            // Draining outside producer timing is essential: a full ingress
            // would reject Begin and create misleadingly cheap measurements.
            let started = WallInstant::now();
            let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
            drain_snapshot_ns.push(started.elapsed().as_nanos() as u64);
            black_box(&snapshot);
            assert_healthy(&snapshot, expected, mode);
        }
    }

    let counts = if let Some(doctor) = &doctor {
        for _ in 0..STEADY_READS {
            let started = WallInstant::now();
            let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
            steady_snapshot_ns.push(started.elapsed().as_nanos() as u64);
            black_box(&snapshot);
            assert_healthy(&snapshot, expected, mode);
        }
        let snapshot = doctor.snapshot(QualityQuery::default()).unwrap();
        assert_healthy(&snapshot, expected, mode);
        assert_eq!(doctor.inner.active_probes.load(Ordering::Relaxed), 0);
        serde_json::json!({
            "begin_attempted_including_warmup": snapshot.counters().begin_attempted(),
            "begin_enqueued_including_warmup": snapshot.counters().begin_enqueued(),
            "ingress_dropped": snapshot.counters().ingress_dropped(),
            "ingress_high_watermark": snapshot.counters().ingress_high_watermark(),
            "active_capacity_skipped": snapshot.counters().active_capacity_skipped(),
            "records_evicted": snapshot.counters().records_evicted(),
            "retained": snapshot.summary().retained(),
            "returned_reports": snapshot.reports().len(),
            "incomplete": snapshot.summary().incomplete(),
        })
    } else {
        if let Some(inner) = &owner.inner {
            assert!(inner.collector.get().is_none());
        } else {
            assert!(matches!(mode, Mode::Off));
        }
        serde_json::Value::Null
    };

    serde_json::json!({
        "record": "round",
        "round": round,
        "position": position,
        "mode": mode.name(),
        "measured_operations": MEASURED_BATCHES * OPERATIONS_PER_BATCH,
        "warmup_operations": WARMUP_BATCHES * OPERATIONS_PER_BATCH,
        "producer": timing_summary(&producer_ns, OPERATIONS_PER_BATCH),
        "drain_and_snapshot_call": timing_summary(&drain_snapshot_ns, 1),
        "steady_snapshot_call": timing_summary(&steady_snapshot_ns, 1),
        "counts": counts,
    })
}

#[test]
#[ignore = "opt-in release-only diagnostic microbenchmark; run serially on an idle host"]
fn diagnostic_producer_and_reader_release_microbenchmark() {
    assert!(
        !cfg!(debug_assertions),
        "performance measurements require cargo test --release"
    );
    println!(
        "QUALITY_CORE_PERF {}",
        serde_json::json!({
            "record": "method",
            "arch": std::env::consts::ARCH,
            "os": std::env::consts::OS,
            "rounds": ROUNDS,
            "mode_order": MODES.map(Mode::name),
            "order": "rotate left by round; every mode occupies every position once",
            "operations_per_batch": OPERATIONS_PER_BATCH,
            "measured_batches": MEASURED_BATCHES,
            "warmup_batches": WARMUP_BATCHES,
            "steady_reads_per_round": STEADY_READS,
            "workload": "begin; one Admission interval; Success; finish; probe drop",
            "limits": "default; history 256, query results 128, ingress 1024",
            "reader_timing": "snapshot call only; returned snapshot drop and assertions excluded",
            "sampling": "operation detail 0% or 100%; one common interval",
            "interpretation": "total current diagnostic cost; batch-average percentiles, not request latency percentiles",
            "excluded": ["initialization", "HTTP", "VPN/TUN", "contention", "JSON export", "CPU profiling", "allocation counts", "RSS", "old-implementation comparison"],
        })
    );
    for round in 0..ROUNDS {
        for position in 0..MODES.len() {
            let mode = MODES[(round + position) % MODES.len()];
            // Logging, assertions and fixture destruction are never timed.
            let result = measure_round(mode, round, position);
            println!("QUALITY_CORE_PERF {result}");
        }
    }
}

#![cfg(all(feature = "http-client", feature = "quality-doctor"))]

//! Reproducible, opt-in release measurements; no performance pass/fail threshold.
//!
//! Run alone on an otherwise idle machine, after compiling the binary:
//! `cargo test --release --locked --offline --no-default-features --features
//! http-client,quality-doctor --test http_quality_performance -- --ignored --nocapture`
//!
//! `OPEN_NET_QUALITY_PERF_ROUNDS` (default 100, max 2000),
//! `OPEN_NET_QUALITY_PERF_BATCH` (default 16, max 32), and
//! `OPEN_NET_QUALITY_PERF_WARMUP` (default 256, max 4096) are bounded.
//! `OPEN_NET_QUALITY_PERF_MODE` optionally selects one printed mode name for
//! separate process CPU/RSS measurements. Otherwise every round rotates the
//! starting mode to reduce ordering drift. Repeat complete runs independently.
//! `OPEN_NET_QUALITY_PERF_ALLOW_LOCK_LOSS=1` explicitly measures continuous
//! best-effort collection, reporting missing references and contention loss.
//! The default requires lossless collection. Queue saturation is always rejected.
//!
//! Basic/Detailed drain diagnostics between timed batches, so their measurements
//! isolate request-path emission rather than permanently overflowing ingress.
//! DetailedSnapshot additionally reads a bounded snapshot after every request,
//! inside the timed region. The full snapshot also aggregates bounded history.
//! Configuration is default except that the fixture explicitly disables the
//! environment proxy for its loopback origin, equally for every mode. This does
//! not exercise a VPN, DNS, TLS, HTTP/2, saturation, or external-network latency.
//! Process-level CPU/RSS includes the test peer and SDK runtime; it is not an
//! allocation benchmark or isolated collector CPU attribution.

use open_net::api::http::{HttpClient, HttpClientConfig, HttpRequest, HttpTlsConfig};
use open_net::api::quality::{QualityCounters, QualityDoctor, QualityDoctorConfig, QualityQuery};
use open_net::api::retry::RetrySetting;
use std::hint::black_box;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WATCHDOG: Duration = Duration::from_secs(10);
const QUERY_LIMIT: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Off,
    Unactivated,
    Basic,
    Detailed,
    DetailedSnapshot,
}

impl Mode {
    const ALL: [Self; 5] = [
        Self::Off,
        Self::Unactivated,
        Self::Basic,
        Self::Detailed,
        Self::DetailedSnapshot,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Unactivated => "detailed_unactivated",
            Self::Basic => "basic",
            Self::Detailed => "detailed",
            Self::DetailedSnapshot => "detailed_snapshot",
        }
    }

    fn config(self) -> QualityDoctorConfig {
        match self {
            Self::Off => QualityDoctorConfig::default(),
            Self::Basic => QualityDoctorConfig::basic(),
            Self::Unactivated | Self::Detailed | Self::DetailedSnapshot => {
                QualityDoctorConfig::detailed()
            }
        }
    }

    fn activated(self) -> bool {
        !matches!(self, Self::Off | Self::Unactivated)
    }
}

struct Peer {
    address: std::net::SocketAddr,
    accepted: Arc<AtomicU64>,
    requests: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<TestResult>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Peer {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let accepted = Arc::new(AtomicU64::new(0));
        let requests = Arc::new(AtomicU64::new(0));
        let accepted_copy = accepted.clone();
        let requests_copy = requests.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (socket, _) = result?;
                        accepted_copy.fetch_add(1, Ordering::Relaxed);
                        connections.spawn(serve(socket, requests_copy.clone()));
                    }
                    Some(result) = connections.join_next(), if !connections.is_empty() => {
                        result??;
                    }
                }
            }
        });
        Ok(Self {
            address,
            accepted,
            requests,
            task,
        })
    }
}

async fn serve(socket: TcpStream, requests: Arc<AtomicU64>) -> TestResult {
    let mut socket = BufReader::new(socket);
    let mut line = Vec::with_capacity(256);
    loop {
        let mut total = 0;
        loop {
            line.clear();
            let length = socket.read_until(b'\n', &mut line).await?;
            if length == 0 {
                return Ok(());
            }
            if total == 0 && line.as_slice() != b"GET /quality-perf HTTP/1.1\r\n" {
                return Err("unexpected benchmark request".into());
            }
            total += length;
            if total > 16_384 {
                return Err("oversized fixture request".into());
            }
            if line.as_slice() == b"\r\n" {
                break;
            }
        }
        requests.fetch_add(1, Ordering::Relaxed);
        socket
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await?;
    }
}

struct Case {
    mode: Mode,
    peer: Peer,
    client: HttpClient,
    doctor: Option<QualityDoctor>,
    baseline_counters: QualityCounters,
    request_ns: Vec<u64>,
    round_ns: Vec<u64>,
    missing_measured_refs: usize,
}

impl Case {
    async fn new(mode: Mode, samples: usize, rounds: usize) -> TestResult<Self> {
        let peer = Peer::start().await?;
        let config = HttpClientConfig::new(format!("http://{}", peer.address))?
            .with_tls(
                HttpTlsConfig::default()
                    .with_crypto_provider(Arc::new(rustls_openssl::default_provider())),
            )
            .with_environment_proxy(false)
            .with_standalone_quality_doctor(mode.config());
        let client = HttpClient::new(config, "quality-performance")?;
        let doctor = mode.activated().then(|| client.quality_doctor()).flatten();
        assert_eq!(doctor.is_some(), mode.activated());
        Ok(Self {
            mode,
            peer,
            client,
            doctor,
            baseline_counters: QualityCounters::default(),
            request_ns: Vec::with_capacity(samples),
            round_ns: Vec::with_capacity(rounds),
            missing_measured_refs: 0,
        })
    }

    fn drain(&self) -> TestResult<QualityCounters> {
        if let Some(doctor) = &self.doctor {
            let snapshot = doctor.snapshot(QualityQuery::default().with_limit(QUERY_LIMIT))?;
            Ok(snapshot.counters().clone())
        } else {
            Ok(QualityCounters::default())
        }
    }

    async fn request(&self) -> TestResult<bool> {
        let request = HttpRequest::builder()
            .path("/quality-perf")
            .retry_setting(RetrySetting::Disabled)
            .build()?;
        let response = tokio::time::timeout(WATCHDOG, self.client.request(request)).await??;
        assert_eq!(response.status, http::StatusCode::OK);
        assert_eq!(response.body.as_ref(), b"ok");
        assert_eq!(response.attempts, 1);
        let has_reference = response.quality_ref().is_some();
        if !self.mode.activated() {
            assert!(!has_reference);
        }
        if self.mode == Mode::DetailedSnapshot {
            let doctor = self.doctor.as_ref().ok_or("active doctor missing")?;
            black_box(doctor.snapshot(QualityQuery::default().with_limit(QUERY_LIMIT))?);
        }
        Ok(has_reference)
    }
}

fn bounded_setting(name: &str, default: usize, max: usize) -> TestResult<usize> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        value => {
            let parsed = value?.parse::<usize>()?;
            if !(1..=max).contains(&parsed) {
                return Err(format!("{name} must be in 1..={max}").into());
            }
            Ok(parsed)
        }
    }
}

// Nearest-rank percentiles; round percentiles describe wall time for a full
// batch, while request percentiles include snapshot cost in snapshot mode.
fn percentile(sorted: &[u64], percentage: usize) -> u64 {
    sorted[(sorted.len() * percentage).div_ceil(100) - 1]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "opt-in release benchmark; run alone with --ignored --nocapture"]
async fn quality_loopback_release_measurements() -> TestResult {
    if cfg!(debug_assertions) {
        return Err("performance measurements require --release".into());
    }
    let rounds = bounded_setting("OPEN_NET_QUALITY_PERF_ROUNDS", 100, 2000)?;
    let batch = bounded_setting("OPEN_NET_QUALITY_PERF_BATCH", 16, 32)?;
    let warmup = bounded_setting("OPEN_NET_QUALITY_PERF_WARMUP", 256, 4096)?;
    let allow_lock_loss = match std::env::var("OPEN_NET_QUALITY_PERF_ALLOW_LOCK_LOSS") {
        Err(std::env::VarError::NotPresent) => false,
        Ok(value) if value == "1" => true,
        _ => return Err("OPEN_NET_QUALITY_PERF_ALLOW_LOCK_LOSS must be absent or 1".into()),
    };
    let modes = match std::env::var("OPEN_NET_QUALITY_PERF_MODE") {
        Err(std::env::VarError::NotPresent) => Mode::ALL.to_vec(),
        selected => {
            let selected = selected?;
            vec![Mode::ALL
                .into_iter()
                .find(|mode| mode.name() == selected)
                .ok_or("unknown OPEN_NET_QUALITY_PERF_MODE")?]
        }
    };
    let samples = rounds * batch;
    let mut cases = Vec::with_capacity(modes.len());
    for mode in modes {
        cases.push(Case::new(mode, samples, rounds).await?);
    }

    // Equal warmup fills the default retained history, establishes one live
    // pooled connection per mode, and does not contribute timing samples.
    for index in 0..warmup {
        for case in &cases {
            case.request().await?;
            if (index + 1) % batch == 0 {
                case.drain()?;
            }
        }
    }
    for case in &mut cases {
        case.baseline_counters = case.drain()?;
    }
    println!(
        "QUALITY_PERF_CONFIG {}",
        serde_json::json!({
            "rounds": rounds,
            "batch": batch,
            "warmup_requests_per_mode": warmup,
            "measured_requests_per_mode": samples,
            "modes": cases.iter().map(|case| case.mode.name()).collect::<Vec<_>>(),
            "snapshot_query_limit": QUERY_LIMIT,
            "quality_limits": "default",
            "allow_lock_loss": allow_lock_loss,
            "ordering": "rotate starting mode each round",
            "timing_scope": "request build through body validation; snapshot mode also reads snapshot",
            "outside_timing": "client setup, warmup, inter-batch diagnostic drains, shutdown",
        })
    );

    for round in 0..rounds {
        for offset in 0..cases.len() {
            let index = (round + offset) % cases.len();
            let case = &mut cases[index];
            let round_start = Instant::now();
            for _ in 0..batch {
                let start = Instant::now();
                let has_reference = case.request().await?;
                case.request_ns.push(start.elapsed().as_nanos() as u64);
                if case.mode.activated() && !has_reference {
                    case.missing_measured_refs += 1;
                }
            }
            case.round_ns.push(round_start.elapsed().as_nanos() as u64);
            // Drain outside the timer for all active modes; no hidden worker,
            // unbounded event retention, or per-mode capacity change.
            case.drain()?;
        }
    }

    for case in &mut cases {
        let counters = case.drain()?;
        let accepted = case.peer.accepted.load(Ordering::Relaxed);
        let received = case.peer.requests.load(Ordering::Relaxed);
        assert_eq!(accepted, 1, "{}: pool was not reused", case.mode.name());
        assert_eq!(received, (warmup + samples) as u64);
        assert_eq!(case.request_ns.len(), samples);
        assert!(
            !case.peer.task.is_finished(),
            "fixture stopped unexpectedly"
        );
        // Reject a saturated or incomplete measurement instead of mistaking
        // omitted diagnostic work for a cheaper successful collection path.
        assert_eq!(counters.ingress_queue_full_dropped(), 0);
        assert_eq!(counters.active_capacity_skipped(), 0);
        let rounds_in_order = case.round_ns.clone();
        case.request_ns.sort_unstable();
        case.round_ns.sort_unstable();
        println!(
            "QUALITY_PERF_RESULT {}",
            serde_json::json!({
                "mode": case.mode.name(),
                "requests": samples,
                "missing_measured_response_refs": case.missing_measured_refs,
                "lossless_including_warmup": counters.ingress_dropped() == 0,
                "interpretation": if allow_lock_loss { "continuous best-effort workload; report loss alongside latency" } else { "lossless collection required" },
                "rounds": rounds,
                "batch": batch,
                "request_p50_ns": percentile(&case.request_ns, 50),
                "request_p95_ns": percentile(&case.request_ns, 95),
                "round_p50_ns": percentile(&case.round_ns, 50),
                "round_p95_ns": percentile(&case.round_ns, 95),
                "measured_total_ns": case.round_ns.iter().sum::<u64>(),
                "round_elapsed_ns_in_order": rounds_in_order,
                "accepted_connections_including_warmup": accepted,
                "server_requests_including_warmup": received,
                "ingress_dropped_measured": counters.ingress_dropped().saturating_sub(case.baseline_counters.ingress_dropped()),
                "lock_dropped_measured": counters.ingress_lock_contended_dropped().saturating_sub(case.baseline_counters.ingress_lock_contended_dropped()),
                "queue_full_dropped_measured": counters.ingress_queue_full_dropped().saturating_sub(case.baseline_counters.ingress_queue_full_dropped()),
                "drop_summaries_lost_measured": counters.drop_summaries_lost().saturating_sub(case.baseline_counters.drop_summaries_lost()),
                "ingress_high_watermark_including_warmup": counters.ingress_high_watermark(),
                "counters_including_warmup": counters,
            })
        );
        // Publish evidence before rejecting an invalid lossless measurement.
        // In best-effort mode, callers must not describe omitted observations
        // as fully collected work or confuse this with the core microbenchmark.
        if !allow_lock_loss {
            assert_eq!(
                counters.ingress_dropped(),
                0,
                "loss invalidates lossless measurement"
            );
            assert_eq!(counters.begin_attempted(), counters.begin_enqueued());
            assert_eq!(case.missing_measured_refs, 0);
        }
        tokio::time::timeout(WATCHDOG, case.client.shutdown()).await??;
    }
    Ok(())
}

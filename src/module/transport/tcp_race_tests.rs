use super::*;
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ConnectFuture = Pin<Box<dyn Future<Output = io::Result<TestSocket>>>>;

#[derive(Clone, Default)]
struct Observations {
    starts: Vec<(u16, Duration)>,
    active: usize,
    max_active: usize,
    dropped_futures: Vec<u16>,
    dropped_sockets: Vec<u16>,
}

struct Fixture {
    started: Instant,
    observations: Arc<Mutex<Observations>>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            observations: Arc::new(Mutex::new(Observations::default())),
        }
    }

    fn snapshot(&self) -> TestResult<Observations> {
        self.observations
            .lock()
            .map(|observations| observations.clone())
            .map_err(|_| "observations mutex poisoned".into())
    }

    fn connect(
        &self,
        address: SocketAddr,
        after: Option<Duration>,
        success: bool,
    ) -> ConnectFuture {
        let id = address.port();
        let state = self.observations.lock();
        match state {
            Ok(mut observations) => {
                observations.starts.push((id, self.started.elapsed()));
                observations.active += 1;
                observations.max_active = observations.max_active.max(observations.active);
            }
            Err(_) => {
                return Box::pin(async { Err(io::Error::other("observations mutex poisoned")) });
            }
        }
        let guard = ActiveConnect {
            id,
            observations: Arc::clone(&self.observations),
        };
        // A successful connector owns its socket before its future is polled ready.
        // This exercises cleanup even when two completed candidates become ready together.
        let socket = success.then(|| TestSocket {
            id,
            observations: Arc::clone(&self.observations),
        });
        Box::pin(async move {
            let _guard = guard;
            match after {
                Some(delay) if !delay.is_zero() => tokio::time::sleep(delay).await,
                Some(_) => {}
                None => std::future::pending::<()>().await,
            }
            match socket {
                Some(socket) => Ok(socket),
                None => Err(io::Error::from(io::ErrorKind::ConnectionRefused)),
            }
        })
    }
}

struct ActiveConnect {
    id: u16,
    observations: Arc<Mutex<Observations>>,
}

impl Drop for ActiveConnect {
    fn drop(&mut self) {
        if let Ok(mut observations) = self.observations.lock() {
            observations.active = observations.active.saturating_sub(1);
            observations.dropped_futures.push(self.id);
        }
    }
}

struct TestSocket {
    id: u16,
    observations: Arc<Mutex<Observations>>,
}

impl Drop for TestSocket {
    fn drop(&mut self) {
        if let Ok(mut observations) = self.observations.lock() {
            observations.dropped_sockets.push(self.id);
        }
    }
}

fn v4(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

fn v6(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv6Addr::LOCALHOST, port))
}

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned().into())
    }
}

fn deadline(fixture: &Fixture, millis: u64) -> TestResult<Instant> {
    fixture
        .started
        .checked_add(Duration::from_millis(millis))
        .ok_or_else(|| "unrepresentable deadline".into())
}

fn check_failure<S>(
    result: Result<S, ConnectionFailure>,
    error: NetError,
    stage: ConnectStage,
) -> TestResult {
    match result {
        Ok(_) => Err("connection unexpectedly succeeded".into()),
        Err(failure) => check(
            failure.error().kind() == error.kind()
                && failure.stage() == stage
                && failure.retryable()
                && failure.http_status().is_none(),
            "connection failure lost error, stage or retryability",
        ),
    }
}

#[tokio::test(start_paused = true)]
async fn blackholed_first_address_does_not_block_healthy_second_address() -> TestResult {
    let fixture = Fixture::new();
    let result = resolve_and_connect(
        async { Ok(vec![v6(1), v4(2)]) },
        |address| {
            fixture.connect(
                address,
                (address.port() == 2).then_some(Duration::from_millis(50)),
                address.port() == 2,
            )
        },
        deadline(&fixture, 5000)?,
    )
    .await
    .map_err(|error| format!("healthy second address was not reached: {error:?}"))?;
    check(result.id == 2, "wrong address won")?;
    check(
        fixture.started.elapsed() == Duration::from_millis(300),
        "fallback was not staggered by 250ms",
    )?;
    let state = fixture.snapshot()?;
    check(
        state.starts == vec![(1, Duration::ZERO), (2, Duration::from_millis(250))],
        "unexpected connection start times",
    )?;
    check(
        state.active == 0 && state.max_active == 2,
        "winning left a connect future alive",
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn three_blackholes_make_room_for_a_healthy_fourth_without_exceeding_three() -> TestResult {
    let fixture = Fixture::new();
    let result = resolve_and_connect(
        async { Ok(vec![v4(1), v4(2), v4(3), v4(4)]) },
        |address| {
            fixture.connect(
                address,
                (address.port() == 4).then_some(Duration::from_millis(50)),
                address.port() == 4,
            )
        },
        deadline(&fixture, 6000)?,
    )
    .await
    .map_err(|error| format!("fourth address was starved: {error:?}"))?;
    check(result.id == 4, "fourth address did not win")?;
    let state = fixture.snapshot()?;
    check(
        state.starts
            == vec![
                (1, Duration::ZERO),
                (2, Duration::from_millis(250)),
                (3, Duration::from_millis(500)),
                (4, Duration::from_millis(2250)),
            ],
        "candidate replacement did not preserve stagger or retirement age",
    )?;
    check(
        state.max_active == 3 && state.active == 0,
        "connection concurrency exceeded three or leaked",
    )?;
    check(
        state.dropped_futures.len() == 4,
        "a replaced or losing future was not dropped",
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn address_families_alternate_preserve_order_and_skip_duplicates() -> TestResult {
    for (addresses, expected) in [
        (
            vec![v6(1), v6(2), v6(1), v4(3), v4(4), v6(5), v4(6), v4(3)],
            vec![1, 3, 2, 4, 5, 6],
        ),
        (vec![v4(1), v4(2), v6(3), v6(4), v6(5)], vec![1, 3, 2, 4, 5]),
    ] {
        let fixture = Fixture::new();
        let result = resolve_and_connect(
            async { Ok(addresses) },
            |address| fixture.connect(address, Some(Duration::ZERO), false),
            deadline(&fixture, 5000)?,
        )
        .await;
        check_failure(
            result,
            NetError::from(crate::error::ErrorKind::Io),
            ConnectStage::Tcp,
        )?;
        let state = fixture.snapshot()?;
        check(
            state.starts.iter().map(|entry| entry.0).collect::<Vec<_>>() == expected,
            "family interleaving, per-family order or deduplication changed",
        )?;
        check(
            state.starts.iter().all(|entry| entry.1 == Duration::ZERO),
            "immediate failures unnecessarily waited for fallback delay",
        )?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn candidate_exhaustion_preserves_last_observed_io_source_and_kind() -> TestResult {
    #[derive(Debug)]
    struct CandidateSource {
        candidate: u16,
        identity: Arc<()>,
    }
    impl std::fmt::Display for CandidateSource {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "candidate {} failed", self.candidate)
        }
    }
    impl std::error::Error for CandidateSource {}

    let fixture = Fixture::new();
    let source_identity = Arc::new(());
    let failure = resolve_and_connect(
        async { Ok(vec![v4(1), v4(2), v4(3)]) },
        |address| {
            // Starts are 0/250/500ms; completions are 800/550/600ms. The last
            // observed error therefore belongs to the first, not the last, address.
            let (delay, kind) = match address.port() {
                1 => (800, io::ErrorKind::ConnectionReset),
                2 => (300, io::ErrorKind::PermissionDenied),
                _ => (100, io::ErrorKind::AddrNotAvailable),
            };
            let source = CandidateSource {
                candidate: address.port(),
                identity: Arc::clone(&source_identity),
            };
            let attempt = fixture.connect(address, Some(Duration::from_millis(delay)), false);
            async move { attempt.await.map_err(|_| io::Error::new(kind, source)) }
        },
        deadline(&fixture, 5000)?,
    )
    .await
    .err()
    .ok_or("all failing candidates unexpectedly connected")?;
    let error = failure.error();
    check(
        error.kind() == crate::error::ErrorKind::Io
            && error.io_kind() == Some(io::ErrorKind::ConnectionReset)
            && error.context().stage == Some(crate::error::ErrorStage::Tcp),
        "TCP exhaustion lost the last observed I/O category or origin",
    )?;
    let original = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<io::Error>())
        .ok_or("TCP exhaustion lost the original I/O error")?;
    let source = original
        .get_ref()
        .and_then(|source| source.downcast_ref::<CandidateSource>())
        .ok_or("TCP exhaustion replaced the original candidate source")?;
    check(
        source.candidate == 1 && Arc::ptr_eq(&source.identity, &source_identity),
        "TCP exhaustion retained an earlier error or reconstructed its source",
    )?;
    let state = fixture.snapshot()?;
    check(
        state.starts
            == vec![
                (1, Duration::ZERO),
                (2, Duration::from_millis(250)),
                (3, Duration::from_millis(500)),
            ]
            && state.dropped_futures == vec![2, 3, 1]
            && fixture.started.elapsed() == Duration::from_millis(800)
            && state.max_active == 3
            && state.active == 0,
        "I/O error retention changed candidate timing, completion order or cleanup",
    )?;
    check_failure::<TestSocket>(
        Err(failure),
        NetError::from(crate::error::ErrorKind::Io),
        ConnectStage::Tcp,
    )
}

#[tokio::test(start_paused = true)]
async fn candidate_exhaustion_after_probe_replacement_keeps_timeout_precedence() -> TestResult {
    let fixture = Fixture::new();
    let failure = resolve_and_connect(
        async { Ok(vec![v4(1), v4(2), v4(3), v4(4)]) },
        |address| {
            fixture.connect(
                address,
                match address.port() {
                    1 => Some(Duration::from_millis(3000)),
                    // This probe is retired at 2250ms while its result is unknown.
                    2 => None,
                    3 => Some(Duration::from_millis(2750)),
                    _ => Some(Duration::from_millis(100)),
                },
                false,
            )
        },
        deadline(&fixture, 5000)?,
    )
    .await
    .err()
    .ok_or("all failing or retired candidates unexpectedly connected")?;
    let error = failure.error();
    check(
        error.kind() == crate::error::ErrorKind::TimedOut
            && error.io_kind().is_none()
            && std::error::Error::source(&error).is_none(),
        "the last I/O error replaced the unknown retired probe's timeout outcome",
    )?;
    let state = fixture.snapshot()?;
    check(
        state.starts
            == vec![
                (1, Duration::ZERO),
                (2, Duration::from_millis(250)),
                (3, Duration::from_millis(500)),
                (4, Duration::from_millis(2250)),
            ]
            && state.dropped_futures == vec![2, 4, 1, 3]
            && fixture.started.elapsed() == Duration::from_millis(3250)
            && state.max_active == 3
            && state.active == 0,
        "probe exhaustion changed replacement timing or waited for a new deadline",
    )?;
    check_failure::<TestSocket>(
        Err(failure),
        NetError::from(crate::error::ErrorKind::TimedOut),
        ConnectStage::Tcp,
    )
}

#[tokio::test(start_paused = true)]
async fn fastest_started_connection_wins_and_drops_the_slower_owned_socket() -> TestResult {
    let fixture = Fixture::new();
    let winner = resolve_and_connect(
        async { Ok(vec![v4(1), v4(2), v4(3)]) },
        |address| {
            fixture.connect(
                address,
                Some(Duration::from_millis(if address.port() == 1 {
                    700
                } else {
                    100
                })),
                true,
            )
        },
        deadline(&fixture, 5000)?,
    )
    .await
    .map_err(|error| format!("race failed: {error:?}"))?;
    check(
        winner.id == 2,
        "first enumerated address won instead of fastest ready address",
    )?;
    let state = fixture.snapshot()?;
    check(
        fixture.started.elapsed() == Duration::from_millis(350) && state.starts.len() == 2,
        "unnecessary candidate launched after success",
    )?;
    check(
        state.active == 0 && state.dropped_sockets == vec![1],
        "losing socket leaked or winner was closed",
    )?;
    drop(winner);
    check(
        fixture.snapshot()?.dropped_sockets.len() == 2,
        "winner ownership did not transfer to caller",
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn immediate_success_never_launches_another_address() -> TestResult {
    let fixture = Fixture::new();
    let winner = resolve_and_connect(
        async { Ok(vec![v4(1), v4(2), v4(3), v4(4)]) },
        |address| fixture.connect(address, Some(Duration::ZERO), true),
        deadline(&fixture, 5000)?,
    )
    .await
    .map_err(|error| format!("first connection failed: {error:?}"))?;
    check(
        winner.id == 1 && fixture.snapshot()?.starts == vec![(1, Duration::ZERO)],
        "immediate success launched additional connections",
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn single_address_and_final_candidates_keep_the_full_remaining_budget() -> TestResult {
    for count in [1u16, 2, 3, 4] {
        let fixture = Fixture::new();
        let winner = resolve_and_connect(
            async { Ok((1..=count).map(v4).collect::<Vec<_>>()) },
            |address| {
                fixture.connect(
                    address,
                    (address.port() == count).then_some(Duration::from_secs(3)),
                    address.port() == count,
                )
            },
            deadline(&fixture, 7000)?,
        )
        .await
        .map_err(|error| format!("last candidate was prematurely cut off: {error:?}"))?;
        check(
            winner.id == count,
            "last candidate did not retain its remaining budget",
        )?;
        let start_delay = match count {
            1 => Duration::ZERO,
            2 => Duration::from_millis(250),
            3 => Duration::from_millis(500),
            _ => Duration::from_millis(2250),
        };
        check(
            fixture.started.elapsed() == start_delay + Duration::from_secs(3),
            "2s replacement age became a hard per-address timeout",
        )?;
        check(
            fixture.snapshot()?.max_active <= 3,
            "too many in-flight candidates",
        )?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn all_pending_candidates_end_at_the_original_deadline_and_drop_every_future() -> TestResult {
    for count in [1u16, 3, 8] {
        let fixture = Fixture::new();
        let result = resolve_and_connect(
            async { Ok((1..=count).map(v4).collect::<Vec<_>>()) },
            |address| fixture.connect(address, None, false),
            deadline(&fixture, 8000)?,
        )
        .await;
        check_failure(
            result,
            NetError::from(crate::error::ErrorKind::TimedOut),
            ConnectStage::Tcp,
        )?;
        let state = fixture.snapshot()?;
        check(
            fixture.started.elapsed() == Duration::from_secs(8),
            "racing restarted or shortened the total budget",
        )?;
        check(
            state.active == 0
                && state.max_active <= 3
                && state.dropped_futures.len() == state.starts.len(),
            "timeout leaked a candidate or exceeded the concurrency cap",
        )?;
        check(
            state.starts.len() == usize::from(count),
            "pending first candidates starved later addresses",
        )?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn low_total_budget_prevents_any_launch_at_or_after_its_deadline() -> TestResult {
    for budget in [100, 250, 2250] {
        let fixture = Fixture::new();
        let result = resolve_and_connect(
            async { Ok(vec![v4(1), v4(2), v4(3), v4(4)]) },
            |address| fixture.connect(address, None, false),
            deadline(&fixture, budget)?,
        )
        .await;
        check_failure(
            result,
            NetError::from(crate::error::ErrorKind::TimedOut),
            ConnectStage::Tcp,
        )?;
        let state = fixture.snapshot()?;
        let expected = if budget <= 250 { 1 } else { 3 };
        check(
            state.starts.len() == expected
                && state
                    .starts
                    .iter()
                    .all(|entry| entry.1 < Duration::from_millis(budget)),
            "a fallback launched at or after the total deadline",
        )?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn resolver_elapsed_time_is_part_of_the_same_attempt_budget() -> TestResult {
    let fixture = Fixture::new();
    let result = resolve_and_connect(
        async {
            tokio::time::sleep(Duration::from_millis(900)).await;
            Ok(vec![v4(1), v4(2)])
        },
        |address| fixture.connect(address, None, false),
        deadline(&fixture, 1000)?,
    )
    .await;
    check_failure(
        result,
        NetError::from(crate::error::ErrorKind::TimedOut),
        ConnectStage::Tcp,
    )?;
    check(
        fixture.started.elapsed() == Duration::from_secs(1)
            && fixture.snapshot()?.starts == vec![(1, Duration::from_millis(900))],
        "DNS elapsed time was discarded from the TCP budget",
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dropping_the_race_releases_all_started_futures_and_owned_sockets() -> TestResult {
    let fixture = Fixture::new();
    let mut racing = Box::pin(resolve_and_connect(
        async { Ok(vec![v4(1), v4(2), v4(3), v4(4)]) },
        |address| fixture.connect(address, None, true),
        deadline(&fixture, 5000)?,
    ));
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(550)) => {}
        _ = &mut racing => return Err("pending race completed before cancellation".into()),
    }
    check(
        fixture.snapshot()?.active == 3,
        "race had not started three candidates before cancellation",
    )?;
    drop(racing);
    let state = fixture.snapshot()?;
    check(
        state.active == 0 && state.dropped_futures.len() == 3 && state.dropped_sockets.len() == 3,
        "cancelling the parent left a candidate or owned socket alive",
    )?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    check(
        fixture.snapshot()?.starts.len() == 3,
        "detached candidate launched after cancellation",
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn simultaneous_success_keeps_one_socket_and_precedes_another_launch() -> TestResult {
    let fixture = Fixture::new();
    let winner = resolve_and_connect(
        async { Ok(vec![v4(1), v4(2), v4(3)]) },
        |address| {
            fixture.connect(
                address,
                Some(Duration::from_millis(if address.port() == 1 {
                    500
                } else {
                    250
                })),
                true,
            )
        },
        deadline(&fixture, 5000)?,
    )
    .await
    .map_err(|error| format!("simultaneous success failed: {error:?}"))?;
    let state = fixture.snapshot()?;
    check(
        state.starts.len() == 2
            && state.active == 0
            && state.dropped_sockets.len() == 1
            && !state.dropped_sockets.contains(&winner.id),
        "simultaneous success launched extra work or retained multiple sockets",
    )?;
    drop(winner);
    check(
        fixture.snapshot()?.dropped_sockets.len() == 2,
        "caller did not own the sole winning socket",
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn ready_success_is_observed_before_replacing_an_oldest_candidate() -> TestResult {
    let fixture = Fixture::new();
    let winner = resolve_and_connect(
        async { Ok(vec![v4(1), v4(2), v4(3), v4(4)]) },
        |address| {
            fixture.connect(
                address,
                (address.port() == 2).then_some(Duration::from_secs(2)),
                address.port() == 2,
            )
        },
        deadline(&fixture, 5000)?,
    )
    .await
    .map_err(|error| format!("ready success was retired: {error:?}"))?;
    check(
        winner.id == 2 && fixture.snapshot()?.starts.len() == 3,
        "replacement discarded an already-ready successful connection",
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn first_pending_candidate_keeps_its_chance_while_probe_slots_rotate() -> TestResult {
    let fixture = Fixture::new();
    let winner = resolve_and_connect(
        async { Ok((1..=8).map(v4).collect::<Vec<_>>()) },
        |address| {
            fixture.connect(
                address,
                (address.port() == 1).then_some(Duration::from_secs(3)),
                address.port() == 1,
            )
        },
        deadline(&fixture, 8000)?,
    )
    .await
    .map_err(|error| format!("slow first address was prematurely retired: {error:?}"))?;
    let state = fixture.snapshot()?;
    check(
        winner.id == 1 && fixture.started.elapsed() == Duration::from_secs(3),
        "probe replacement shortened the first pending address budget",
    )?;
    check(
        state.starts.len() == 5 && state.max_active == 3 && state.active == 0,
        "probe slots did not rotate within the concurrency cap",
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn next_earliest_candidate_becomes_protected_when_the_anchor_fails() -> TestResult {
    let fixture = Fixture::new();
    let winner = resolve_and_connect(
        async { Ok((1..=5).map(v4).collect::<Vec<_>>()) },
        |address| {
            fixture.connect(
                address,
                match address.port() {
                    1 => Some(Duration::from_secs(1)),
                    2 => Some(Duration::from_secs(3)),
                    _ => None,
                },
                address.port() == 2,
            )
        },
        deadline(&fixture, 8000)?,
    )
    .await
    .map_err(|error| format!("promoted anchor was retired: {error:?}"))?;
    let state = fixture.snapshot()?;
    check(
        winner.id == 2 && fixture.started.elapsed() == Duration::from_millis(3250),
        "the next earliest candidate did not inherit anchor protection",
    )?;
    check(
        state.starts
            == vec![
                (1, Duration::ZERO),
                (2, Duration::from_millis(250)),
                (3, Duration::from_millis(500)),
                (4, Duration::from_secs(1)),
                (5, Duration::from_millis(2500)),
            ]
            && state.max_active == 3
            && state.active == 0,
        "anchor promotion did not preserve probe rotation or cleanup",
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn failed_candidate_releases_capacity_without_speeding_up_other_in_flight_candidates(
) -> TestResult {
    let fixture = Fixture::new();
    let winner = resolve_and_connect(
        async { Ok(vec![v4(1), v4(2), v4(3)]) },
        |address| {
            fixture.connect(
                address,
                match address.port() {
                    1 => None,
                    2 => Some(Duration::from_millis(50)),
                    _ => Some(Duration::ZERO),
                },
                address.port() == 3,
            )
        },
        deadline(&fixture, 5000)?,
    )
    .await
    .map_err(|error| format!("replacement after failure failed: {error:?}"))?;
    check(
        winner.id == 3
            && fixture.snapshot()?.starts
                == vec![
                    (1, Duration::ZERO),
                    (2, Duration::from_millis(250)),
                    (3, Duration::from_millis(500)),
                ],
        "active attempts lost the stagger after another attempt failed",
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn empty_resolution_and_resolver_errors_remain_dns_failures() -> TestResult {
    for addresses in [
        Ok(Vec::<SocketAddr>::new()),
        Err(io::Error::from(io::ErrorKind::NotFound)),
    ] {
        let fixture = Fixture::new();
        let result = resolve_and_connect(
            async { addresses },
            |address| fixture.connect(address, Some(Duration::ZERO), true),
            deadline(&fixture, 5000)?,
        )
        .await;
        check_failure(
            result,
            NetError::from(crate::error::ErrorKind::Io),
            ConnectStage::Dns,
        )?;
        check(
            fixture.snapshot()?.starts.is_empty(),
            "DNS failure initiated TCP work",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn losing_real_tcp_sockets_close_while_the_winner_remains_usable() -> TestResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let mut sockets = VecDeque::new();
    let mut peers = VecDeque::new();
    for _ in 0..3 {
        let (socket, (peer, _)) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::try_join!(TcpStream::connect(address), listener.accept())
        })
        .await??;
        sockets.push_back(socket);
        peers.push_back(peer);
    }
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(2))
        .ok_or("unrepresentable deadline")?;
    let mut winner = tokio::time::timeout(
        Duration::from_secs(3),
        resolve_and_connect(
            async { Ok(vec![v4(1), v4(2), v4(3)]) },
            |candidate| {
                let socket = sockets.pop_front();
                async move {
                    // Already-connected sockets model losing futures that own
                    // a TCP connection while their completion remains pending.
                    if candidate.port() != 3 {
                        std::future::pending::<()>().await;
                    }
                    socket.ok_or_else(|| io::Error::other("missing socket fixture"))
                }
            },
            deadline,
        ),
    )
    .await?
    .map_err(|error| format!("real socket race failed: {error:?}"))?;
    let mut byte = [0u8; 1];
    for _ in 0..2 {
        let mut peer = peers.pop_front().ok_or("missing losing peer")?;
        let read = tokio::time::timeout(Duration::from_secs(1), peer.read(&mut byte)).await??;
        check(read == 0, "losing TCP socket did not close")?;
    }
    let mut winner_peer = peers.pop_front().ok_or("missing winning peer")?;
    tokio::time::timeout(Duration::from_secs(1), winner.write_all(b"w")).await??;
    tokio::time::timeout(Duration::from_secs(1), winner_peer.read_exact(&mut byte)).await??;
    check(byte == *b"w", "winning TCP socket could not transfer data")?;
    drop(winner);
    let read = tokio::time::timeout(Duration::from_secs(1), winner_peer.read(&mut byte)).await??;
    check(
        read == 0,
        "caller did not own the final TCP socket lifetime",
    )?;
    Ok(())
}

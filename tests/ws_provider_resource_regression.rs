#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

// Provider resource invariants exercised through the public connection API.
// Keep the probe and resource checks unchanged when migrating the API driver.

use open_net::network::{NetworkConfig, NetworkStatusPolicy};
use open_net::ws::ConnectionEventKind;
use open_net::ws::HandshakeProvider;
use open_net::ws::{ConnectOptions, ReconnectPolicy, WebSocketClientConfig};
use open_net::{NetError, OpenNet, WebSocketClient};

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{AbortHandle, JoinHandle, JoinSet};

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

const WAIT_LIMIT: Duration = Duration::from_secs(5);
const PROVIDER_LIMIT: Duration = Duration::from_secs(20);
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);

fn error(message: impl Into<String>) -> TestError {
    std::io::Error::other(message.into()).into()
}

#[track_caller]
fn check(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(error(format!(
            "{}: {}",
            std::panic::Location::caller(),
            message.into()
        )))
    }
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(WAIT_LIMIT, future)
        .await
        .map_err(|cause| error(format!("{label}: {cause}")))
}

#[derive(Clone)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);

impl Gate {
    fn new() -> Self {
        Self(Arc::new((Mutex::new(false), Condvar::new())))
    }

    fn wait(&self) -> Result<(), NetError> {
        let deadline = Instant::now()
            .checked_add(PROVIDER_LIMIT)
            .ok_or(NetError::from(open_net::error::ErrorKind::InvalidConfig))?;
        let (lock, condition) = self.0.as_ref();
        let mut released = lock
            .lock()
            .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?;
        while !*released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(NetError::from(open_net::error::ErrorKind::TimedOut));
            }
            let (state, _) = condition
                .wait_timeout(released, remaining)
                .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?;
            released = state;
        }
        Ok(())
    }

    fn release(&self) {
        let (lock, condition) = self.0.as_ref();
        match lock.lock() {
            Ok(mut released) => *released = true,
            Err(poisoned) => *poisoned.into_inner() = true,
        }
        condition.notify_all();
    }
}

struct Release(Gate);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Default)]
struct Counts {
    started: AtomicUsize,
    active: AtomicUsize,
    peak_active: AtomicUsize,
    exited: AtomicUsize,
    gate_failures: AtomicUsize,
}

#[derive(Clone, Copy, Debug)]
enum Observation {
    Started(usize),
    Exited(usize),
}

struct ActiveCall {
    counts: Arc<Counts>,
    events: mpsc::UnboundedSender<Observation>,
    index: usize,
}

impl ActiveCall {
    fn enter(counts: &Arc<Counts>, events: &mpsc::UnboundedSender<Observation>) -> Self {
        let index = counts.started.fetch_add(1, Ordering::SeqCst);
        let active = counts.active.fetch_add(1, Ordering::SeqCst) + 1;
        counts.peak_active.fetch_max(active, Ordering::SeqCst);
        let _ = events.send(Observation::Started(index));
        Self {
            counts: Arc::clone(counts),
            events: events.clone(),
            index,
        }
    }
}

impl Drop for ActiveCall {
    fn drop(&mut self) {
        self.counts.active.fetch_sub(1, Ordering::SeqCst);
        self.counts.exited.fetch_add(1, Ordering::SeqCst);
        let _ = self.events.send(Observation::Exited(self.index));
    }
}

fn provider(
    gate: Gate,
    counts: Arc<Counts>,
) -> (HandshakeProvider, mpsc::UnboundedReceiver<Observation>) {
    let (events, observations) = mpsc::unbounded_channel();
    let provider = HandshakeProvider::blocking(move |_| {
        let _call = ActiveCall::enter(&counts, &events);
        if let Err(cause) = gate.wait() {
            counts.gate_failures.fetch_add(1, Ordering::SeqCst);
            return Err(cause.into());
        }
        Ok(open_net::ws::HandshakeHeaders {
            headers: open_net::HeaderMap::from_iter([(
                open_net::HeaderName::from_bytes(("X-Test-Provider").as_bytes())?,
                open_net::HeaderValue::from_str("released-old-result")?,
            )]),
            credential_version: Some((902).to_string()),
        })
    });
    (provider, observations)
}

#[derive(Default)]
struct NetworkCounts {
    tcp: AtomicUsize,
    upgrade: AtomicUsize,
}

struct Peer {
    url: String,
    counts: Arc<NetworkCounts>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<TestResult>>,
}

async fn observe_upgrade(mut stream: TcpStream, counts: Arc<NetworkCounts>) -> TestResult {
    let mut request = Vec::new();
    loop {
        let mut byte = [0];
        if stream.read(&mut byte).await? == 0 {
            return Ok(());
        }
        request.push(byte[0]);
        if request.ends_with(b"\r\n\r\n") {
            counts.upgrade.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
        check(request.len() <= 16 * 1024, "oversized unexpected Upgrade")?;
    }
}

impl Peer {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let counts = Arc::new(NetworkCounts::default());
        let recorded = Arc::clone(&counts);
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut sockets = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        recorded.tcp.fetch_add(1, Ordering::SeqCst);
                        let recorded = Arc::clone(&recorded);
                        sockets.spawn(async move {
                            bounded("unexpected Upgrade reader", observe_upgrade(stream, recorded))
                                .await?
                        });
                    }
                    _ = &mut stopped => break,
                }
            }
            while let Some(result) = sockets.join_next().await {
                result??;
            }
            Ok(())
        });
        Ok(Self {
            url,
            counts,
            stop: Some(stop),
            task: Some(task),
        })
    }

    async fn finish(&mut self) -> TestResult {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let task = self
            .task
            .as_mut()
            .ok_or_else(|| error("peer already finished"))?;
        bounded("stop peer and socket observers", task).await???;
        self.task.take();
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
    }
}

struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn options(url: &str, provider: HandshakeProvider, timeout: Duration) -> ConnectOptions {
    let mut connect_options = {
        let mut options = {
            let mut options = ConnectOptions::new(url);
            options.handshake_provider = Some(provider);
            options
        };
        options.reconnect = ReconnectPolicy::Disabled;
        options
    };
    connect_options.handshake_timeout = timeout;
    connect_options.connect_timeout = Some(Duration::from_secs(15));
    connect_options
}

async fn connect_terminal(
    client: &WebSocketClient,
    options: ConnectOptions,
) -> Result<open_net::ws::SessionEnd, NetError> {
    let events = session::observe(client, options).await?;
    terminal(events).await
}

async fn terminal(
    mut events: session::ObservedSession,
) -> Result<open_net::ws::SessionEnd, NetError> {
    let mut sequence = 0u64;
    let mut started = None;
    let mut failed = None;
    while let Some(event) = events
        .recv()
        .await
        .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?
    {
        if Some(event.sequence) != sequence.checked_add(1) {
            return Err(NetError::from(open_net::error::ErrorKind::Internal));
        }
        sequence = event.sequence;
        match event.kind {
            ConnectionEventKind::AttemptStarted { attempt } => {
                if started
                    .replace((
                        attempt.client_id,
                        attempt.session_id,
                        attempt.cycle_id,
                        attempt.attempt_id,
                    ))
                    .is_some()
                {
                    return Err(NetError::from(open_net::error::ErrorKind::Internal));
                }
            }
            ConnectionEventKind::AttemptFailed {
                attempt,
                error,
                retry,
                ..
            } => {
                if started.take()
                    != Some((
                        attempt.client_id,
                        attempt.session_id,
                        attempt.cycle_id,
                        attempt.attempt_id,
                    ))
                    || !matches!(retry, open_net::ws::RetryDecision::Stop)
                {
                    return Err(NetError::from(open_net::error::ErrorKind::Internal));
                }
                failed = Some(error);
            }
            ConnectionEventKind::Closed { result } => {
                if sequence != 3
                    || started.is_some()
                    || events
                        .recv()
                        .await
                        .map_err(|_| NetError::from(open_net::error::ErrorKind::Internal))?
                        .is_some()
                    || failed.as_ref().map(NetError::kind)
                        != Some(match &result {
                            Ok(_) => open_net::error::ErrorKind::Cancelled,
                            Err(error) => error.kind(),
                        })
                {
                    return Err(NetError::from(open_net::error::ErrorKind::Internal));
                }
                return result;
            }
            _ => return Err(NetError::from(open_net::error::ErrorKind::Internal)),
        }
    }
    Err(NetError::from(open_net::error::ErrorKind::Internal))
}

async fn make_client(net: &OpenNet, name: &str) -> TestResult<WebSocketClient> {
    Ok(bounded(
        "create resource-test client",
        net.create_ws_client_with_network_config(
            name,
            {
                let mut config = WebSocketClientConfig::default();
                config.close_timeout = Duration::from_millis(30);
                config.dispatch.blocking_handshake_jobs = 1;
                config
            },
            NetworkConfig::default().with_network_status_policy(NetworkStatusPolicy::Ignore),
        ),
    )
    .await??)
}

async fn resource_scenario(name: &str, cancel_first: bool) -> TestResult {
    let mut peer = Peer::start().await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, name).await?;
    let gate = Gate::new();
    let _release = Release(gate.clone());
    let counts = Arc::new(Counts::default());
    let (provider, mut observations) = provider(gate.clone(), Arc::clone(&counts));

    // Capture the scenario result so ordinary errors still run complete cleanup.
    let scenario = async {
        let first_options = options(
            &peer.url,
            provider.clone(),
            if cancel_first {
                Duration::from_secs(10)
            } else {
                ATTEMPT_TIMEOUT
            },
        );
        let first = session::observe(&client, first_options).await?;
        let observed = bounded("first provider entered", observations.recv()).await?;
        check(
            matches!(observed, Some(Observation::Started(0))),
            "first provider did not enter",
        )?;
        if cancel_first {
            bounded("disconnect blocked provider", first.session.close()).await??;
        }
        let first_result = bounded("first connection terminal", terminal(first)).await?;
        check(if cancel_first {
            matches!(&first_result, Ok(end) if end.reason == open_net::ws::TerminationReason::LocalClose && end.last_connection.is_none())
        } else { first_result.as_ref().err().map(NetError::kind) == Some(open_net::error::ErrorKind::TimedOut) },
            format!("first result: {first_result:?}"))?;
        check(
            counts.active.load(Ordering::SeqCst) == 1,
            "first closure exited before the next session",
        )?;
        eprintln!("{name}: first_result={first_result:?}; first closure remains active");

        for round in 1..3 {
            let result = bounded(
                "replacement connection terminal",
                connect_terminal(
                    &client,
                    options(&peer.url, provider.clone(), ATTEMPT_TIMEOUT),
                ),
            )
            .await?;
            check(
                result.as_ref().err().map(NetError::kind)
                    == Some(open_net::error::ErrorKind::ResourceExhausted),
                format!("round {round} result: {result:?}"),
            )?;
            check(
                result.as_ref().err().is_some_and(|error| error.context().http_status.is_none()),
                "provider timeout acquired HTTP status",
            )?;
            eprintln!(
                "{name}: round={round}; result={result:?}; active={}",
                counts.active.load(Ordering::SeqCst)
            );
        }
        Ok::<(), TestError>(())
    }
    .await;

    let before_release = (
        peer.counts.tcp.load(Ordering::SeqCst),
        peer.counts.upgrade.load(Ordering::SeqCst),
    );
    gate.release();
    let destroyed = bounded("destroy resource-test client", net.destroy_ws_client(name)).await;
    drop(client);
    drop(net);
    drop(provider);
    // The provider is the only lasting sender owner. Channel closure proves all
    // detached calls dropped their provider ownership, including late scheduling.
    let cleaned = bounded("all provider owners and closures exited", async {
        while let Some(observation) = observations.recv().await {
            match observation {
                Observation::Started(index) => eprintln!("{name}: observed start {index}"),
                Observation::Exited(index) => eprintln!("{name}: observed exit {index}"),
            }
        }
    })
    .await;
    let peer_stopped = peer.finish().await;
    let started = counts.started.load(Ordering::SeqCst);
    let active = counts.active.load(Ordering::SeqCst);
    let peak = counts.peak_active.load(Ordering::SeqCst);
    let exited = counts.exited.load(Ordering::SeqCst);
    let gate_failures = counts.gate_failures.load(Ordering::SeqCst);
    let tcp = peer.counts.tcp.load(Ordering::SeqCst);
    let upgrade = peer.counts.upgrade.load(Ordering::SeqCst);
    eprintln!("{name}: started={started}; active={active}; peak_active={peak}; exited={exited}; gate_failures={gate_failures}; tcp={tcp}; upgrade={upgrade}; cleanup_closed={}; client_destroyed={}", cleaned.is_ok(), matches!(destroyed, Ok(Ok(()))));

    destroyed??;
    cleaned?;
    peer_stopped?;
    check(
        active == 0 && exited == started,
        "cleanup left a provider closure running",
    )?;
    check(
        gate_failures == 0,
        "provider gate hit its hard limit or failed",
    )?;
    scenario?;
    check(
        before_release == (0, 0) && tcp == 0 && upgrade == 0,
        "cancelled or expired provider result reached the network",
    )?;
    check(peak <= 1 && started == 1, format!("provider resource invariant violated: started={started}, peak_active={peak}, limit=1; cleanup active={active}, exited={exited}"))
}

#[tokio::test]
async fn provider_quota_is_retained_across_timed_out_sessions() -> TestResult {
    resource_scenario("provider-timeout-resource", false).await
}

#[tokio::test]
async fn provider_quota_is_retained_after_disconnect() -> TestResult {
    resource_scenario("provider-disconnect-resource", true).await
}

fn spawn_blocked_connection(
    client: &WebSocketClient,
    url: &str,
    provider: &HandshakeProvider,
) -> JoinHandle<Result<open_net::ws::SessionEnd, NetError>> {
    let client = client.clone();
    let options = options(url, provider.clone(), Duration::from_secs(10));
    tokio::spawn(async move { connect_terminal(&client, options).await })
}

async fn first_entry(observations: &mut mpsc::UnboundedReceiver<Observation>) -> TestResult {
    let observed = bounded(
        "provider entered before lifecycle operation",
        observations.recv(),
    )
    .await?;
    check(
        matches!(observed, Some(Observation::Started(0))),
        "provider did not enter",
    )
}

async fn owners_released(observations: &mut mpsc::UnboundedReceiver<Observation>) -> TestResult {
    bounded("all provider closure owners released", async {
        while observations.recv().await.is_some() {}
    })
    .await
}

fn fully_released(counts: &Counts) -> TestResult {
    check(
        counts.started.load(Ordering::SeqCst) == 1
            && counts.peak_active.load(Ordering::SeqCst) == 1
            && counts.active.load(Ordering::SeqCst) == 0
            && counts.exited.load(Ordering::SeqCst) == 1
            && counts.gate_failures.load(Ordering::SeqCst) == 0,
        "provider exceeded its per-client slot or did not exit cleanly",
    )
}

#[tokio::test]
async fn separate_clients_have_independent_provider_slots() -> TestResult {
    let mut peer = Peer::start().await?;
    let net = OpenNet::new()?;
    let first_client = make_client(&net, "independent-provider-first").await?;
    let second_client = make_client(&net, "independent-provider-second").await?;
    let gate = Gate::new();
    let _release = Release(gate.clone());
    let first_counts = Arc::new(Counts::default());
    let second_counts = Arc::new(Counts::default());
    let (first_provider, mut first_observations) =
        provider(gate.clone(), Arc::clone(&first_counts));
    let (second_provider, mut second_observations) =
        provider(gate.clone(), Arc::clone(&second_counts));
    let first = session::observe(
        &first_client,
        options(&peer.url, first_provider.clone(), Duration::from_secs(10)),
    )
    .await?;
    let second = session::observe(
        &second_client,
        options(&peer.url, second_provider.clone(), Duration::from_secs(10)),
    )
    .await?;
    let scenario = async {
        first_entry(&mut first_observations).await?;
        first_entry(&mut second_observations).await?;
        check(
            first_counts.active.load(Ordering::SeqCst) == 1
                && second_counts.active.load(Ordering::SeqCst) == 1,
            "two independent clients could not run one provider each",
        )?;
        bounded("disconnect first client", first.session.close()).await??;
        bounded("disconnect second client", second.session.close()).await??;
        let end = bounded("first terminal", terminal(first)).await??;
        check(
            end.reason == open_net::ws::TerminationReason::LocalClose
                && end.last_connection.is_none(),
            "first normal disconnect result changed",
        )?;
        let end = bounded("second terminal", terminal(second)).await??;
        check(
            end.reason == open_net::ws::TerminationReason::LocalClose
                && end.last_connection.is_none(),
            "second normal disconnect result changed",
        )?;
        Ok::<(), TestError>(())
    }
    .await;
    gate.release();
    let first_destroyed = bounded(
        "destroy first independent client",
        net.destroy_ws_client("independent-provider-first"),
    )
    .await;
    let second_destroyed = bounded(
        "destroy second independent client",
        net.destroy_ws_client("independent-provider-second"),
    )
    .await;
    drop(first_client);
    drop(second_client);
    drop(net);
    drop(first_provider);
    drop(second_provider);
    let first_cleaned = owners_released(&mut first_observations).await;
    let second_cleaned = owners_released(&mut second_observations).await;
    let peer_stopped = peer.finish().await;
    first_destroyed??;
    second_destroyed??;
    first_cleaned?;
    second_cleaned?;
    peer_stopped?;
    fully_released(&first_counts)?;
    fully_released(&second_counts)?;
    scenario?;
    check(
        peer.counts.tcp.load(Ordering::SeqCst) == 0
            && peer.counts.upgrade.load(Ordering::SeqCst) == 0,
        "cancelled independent providers opened a socket",
    )
}

async fn shutdown_scenario(destroy: bool) -> TestResult {
    let name = if destroy {
        "blocked-provider-destroy"
    } else {
        "blocked-provider-shutdown"
    };
    let mut peer = Peer::start().await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, name).await?;
    let gate = Gate::new();
    let _release = Release(gate.clone());
    let counts = Arc::new(Counts::default());
    let (provider, mut observations) = provider(gate.clone(), Arc::clone(&counts));
    let connecting = spawn_blocked_connection(&client, &peer.url, &provider);
    let _abort_connecting = AbortOnDrop(connecting.abort_handle());
    let mut removed = false;
    let scenario = async {
        first_entry(&mut observations).await?;
        if destroy {
            bounded(
                "destroy while provider remains blocked",
                net.destroy_ws_client(name),
            )
            .await??;
            removed = true;
        } else {
            bounded("shutdown while provider remains blocked", client.shutdown()).await??;
        }
        check(
            counts.active.load(Ordering::SeqCst) == 1,
            "lifecycle operation waited for user closure to exit",
        )?;
        let result = bounded("blocked connection terminal after shutdown", connecting).await??;
        check(
            matches!(&result, Ok(end) if end.reason == open_net::ws::TerminationReason::ClientShutdown && end.last_connection.is_none()),
            format!("shutdown result: {result:?}"),
        )?;
        Ok::<(), TestError>(())
    }
    .await;
    gate.release();
    let destroyed = if removed {
        Ok(Ok(()))
    } else {
        bounded("remove shutdown client", net.destroy_ws_client(name)).await
    };
    drop(client);
    drop(net);
    drop(provider);
    let cleaned = owners_released(&mut observations).await;
    let peer_stopped = peer.finish().await;
    destroyed??;
    cleaned?;
    peer_stopped?;
    fully_released(&counts)?;
    scenario?;
    check(
        peer.counts.tcp.load(Ordering::SeqCst) == 0
            && peer.counts.upgrade.load(Ordering::SeqCst) == 0,
        "late provider result opened a socket after shutdown",
    )
}

#[tokio::test]
async fn shutdown_is_bounded_while_provider_is_blocked() -> TestResult {
    shutdown_scenario(false).await
}

#[tokio::test]
async fn destroy_is_bounded_while_provider_is_blocked() -> TestResult {
    shutdown_scenario(true).await
}

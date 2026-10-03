//! Real Linux route/link notifications, isolated from the host network.
//!
//! Run explicitly with:
//! `cargo test --test net_status_linux -- --ignored --nocapture`
//! Requires `unshare`, `ip`, and permission to create user/network namespaces.
//! Missing tools or permissions fail the test instead of silently skipping it.

#![cfg(target_os = "linux")]

use open_net::error::ErrorKind;
use open_net::net_status::{
    IpStack, MonitorState, NetStatusClient, NetworkSnapshot, NetworkStatus,
};
use open_net::subscription::StateReceiver;
use open_net::{BoxError, OpenNet};
use std::future::Future;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

type TestResult<T = ()> = Result<T, BoxError>;

const PARENT_NAMESPACE: &str = "OPEN_NET_TEST_PARENT_NET_NAMESPACE";
const TEST_NAME: &str = "linux_network_namespace_lifecycle";
const DEADLINE: Duration = Duration::from_secs(10);

fn check(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message.into()).into())
    }
}

fn current_namespace() -> std::io::Result<PathBuf> {
    std::fs::read_link("/proc/self/ns/net")
}

/// Constructed only after the child proves that unshare changed its namespace.
/// Every ip invocation checks again, so no network mutation can run in the parent.
struct IsolatedNetwork {
    parent: PathBuf,
    child: PathBuf,
}

impl IsolatedNetwork {
    fn verify(parent: PathBuf) -> TestResult<Self> {
        // The parent opens its namespace before unshare and passes that real
        // descriptor as stdin. Reading another user namespace's /proc/PID/ns
        // can be denied even for the same host UID. This also rejects a stale
        // child-marker environment variable without trusting it on its own.
        let actual_parent = std::fs::read_link("/proc/self/fd/0")?;
        check(
            parent == actual_parent,
            format!(
                "refusing network changes: supplied parent {parent:?} differs from actual parent {actual_parent:?}"
            ),
        )?;
        let child = current_namespace()?;
        check(
            parent != child,
            format!("refusing network changes: child {child:?} equals parent {parent:?}"),
        )?;
        Ok(Self { parent, child })
    }

    fn ip(&self, arguments: &[&str]) -> TestResult {
        let current = current_namespace()?;
        check(
            current == self.child && current != self.parent,
            "refusing ip command outside the verified isolated network namespace",
        )?;
        let output = Command::new("ip").args(arguments).output()?;
        check(
            output.status.success(),
            format!(
                "ip {} failed ({}): stdout={} stderr={}",
                arguments.join(" "),
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            ),
        )
    }
}

#[test]
#[ignore = "requires unshare/ip and user/network namespace permissions; never modifies host networking"]
fn linux_network_namespace_lifecycle() -> TestResult {
    if let Some(parent) = std::env::var_os(PARENT_NAMESPACE) {
        let network = IsolatedNetwork::verify(PathBuf::from(parent))?;
        return tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?
            .block_on(exercise_network(network));
    }

    // Even a privileged caller goes through unshare: the parent never runs ip.
    let parent_namespace = std::fs::File::open("/proc/self/ns/net")?;
    let mut child = Command::new("unshare")
        .args(["-Urn", "--"])
        .arg(std::env::current_exe()?)
        .args([
            "--ignored",
            "--exact",
            TEST_NAME,
            "--nocapture",
            "--test-threads=1",
        ])
        .env(PARENT_NAMESPACE, current_namespace()?)
        .stdin(Stdio::from(parent_namespace))
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        if let Some(status) = child.try_wait()? {
            return check(
                status.success(),
                format!("isolated network test failed: {status}"),
            );
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return check(
                false,
                "isolated network test exceeded its 180-second process deadline",
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

async fn bounded<T>(operation: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(DEADLINE, future)
        .await
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("{operation} exceeded {DEADLINE:?}: {error}"),
            )
            .into()
        })
}

async fn observation(
    client: &NetStatusClient,
    receiver: &mut StateReceiver<NetworkSnapshot>,
    previous: &NetworkSnapshot,
    operation: &str,
    reachability: NetworkStatus,
    ip_stack: IpStack,
) -> TestResult<NetworkSnapshot> {
    let result = bounded(operation, async {
        loop {
            let snapshot = receiver.recv().await?.ok_or_else(|| {
                std::io::Error::other(format!("{operation}: observation stream ended"))
            })?;
            check(
                !matches!(
                    &snapshot.state,
                    MonitorState::Failed(_) | MonitorState::Closed
                ),
                format!("{operation}: unexpected terminal snapshot {snapshot:?}"),
            )?;
            if matches!(snapshot.state, MonitorState::Running)
                && snapshot.reachability == Some(reachability)
                && snapshot.ip_stack == Some(ip_stack)
                && snapshot.revision > previous.revision
            {
                return Ok::<_, BoxError>(snapshot);
            }
        }
    })
    .await;
    let snapshot = match result {
        Ok(result) => result?,
        Err(error) => {
            return Err(std::io::Error::other(format!(
                "{error}; expected {reachability:?}/{ip_stack:?}, latest={:?}",
                client.snapshot()?,
            ))
            .into());
        }
    };
    let expected_loss = previous.loss_epoch
        + u64::from(
            reachability == NetworkStatus::Unavailable
                && previous.reachability != Some(NetworkStatus::Unavailable),
        );
    check(
        snapshot.loss_epoch == expected_loss && snapshot.observed_at.is_some(),
        format!(
            "{operation}: expected loss_epoch={expected_loss} and an observation time, got {snapshot:?}"
        ),
    )?;
    eprintln!("{operation}: {snapshot:?}");
    Ok(snapshot)
}

fn inactive(snapshot: &NetworkSnapshot, previous: &NetworkSnapshot) -> TestResult {
    check(
        snapshot.revision > previous.revision
            && snapshot.loss_epoch == previous.loss_epoch
            && snapshot.reachability.is_none()
            && snapshot.ip_stack.is_none()
            && snapshot.observed_at.is_none()
            && snapshot.network_name.is_none(),
        format!("inactive state must advance revision and clear observation: {snapshot:?}"),
    )
}

async fn stays_available(
    client: &NetStatusClient,
    previous: &NetworkSnapshot,
) -> TestResult<NetworkSnapshot> {
    // Wait past the Linux refresh period as well as the link notification. An
    // unchanged observation emits no event, so inspect snapshots throughout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let snapshot = client.snapshot()?;
        check(
            matches!(snapshot.state, MonitorState::Running)
                && snapshot.reachability == Some(NetworkStatus::Available)
                && snapshot.ip_stack == Some(IpStack::V4Only)
                && snapshot.loss_epoch == previous.loss_epoch
                && snapshot.revision >= previous.revision,
            format!("usable alternate IPv4 default must remain available: {snapshot:?}"),
        )?;
        if tokio::time::Instant::now() >= deadline {
            eprintln!("IPv4 alternate default after primary carrier loss: {snapshot:?}");
            return Ok(snapshot);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn exercise_network(network: IsolatedNetwork) -> TestResult {
    network.ip(&["link", "set", "lo", "up"])?;
    let net = OpenNet::new()?;
    let client = bounded(
        "create client",
        net.create_net_status_client("linux-namespace"),
    )
    .await??;
    let mut receiver = client.subscribe()?;
    let initial = bounded("initial stopped snapshot", receiver.recv())
        .await??
        .ok_or("missing initial snapshot")?;
    check(
        matches!(initial.state, MonitorState::Stopped),
        "client must begin stopped",
    )?;
    bounded("initial start", client.start()).await??;
    let mut previous = observation(
        &client,
        &mut receiver,
        &initial,
        "loopback only",
        NetworkStatus::Unavailable,
        IpStack::None,
    )
    .await?;

    network.ip(&[
        "link", "add", "net0", "type", "veth", "peer", "name", "peer0",
    ])?;
    network.ip(&["link", "set", "net0", "up"])?;
    network.ip(&["link", "set", "peer0", "up"])?;
    network.ip(&["address", "add", "192.0.2.2/24", "dev", "net0"])?;
    network.ip(&["route", "add", "default", "via", "192.0.2.1", "dev", "net0"])?;
    previous = observation(
        &client,
        &mut receiver,
        &previous,
        "IPv4 default route",
        NetworkStatus::Available,
        IpStack::V4Only,
    )
    .await?;

    network.ip(&[
        "-6",
        "address",
        "add",
        "2001:db8:1::2/64",
        "dev",
        "net0",
        "nodad",
    ])?;
    previous = observation(
        &client,
        &mut receiver,
        &previous,
        "IPv4 route with dual-stack addresses",
        NetworkStatus::Available,
        IpStack::DualStack,
    )
    .await?;
    network.ip(&["-6", "address", "del", "2001:db8:1::2/64", "dev", "net0"])?;
    previous = observation(
        &client,
        &mut receiver,
        &previous,
        "IPv4 route after IPv6 address removal",
        NetworkStatus::Available,
        IpStack::V4Only,
    )
    .await?;

    network.ip(&["route", "del", "default"])?;
    previous = observation(
        &client,
        &mut receiver,
        &previous,
        "IPv4 route removed",
        NetworkStatus::Unavailable,
        IpStack::V4Only,
    )
    .await?;
    network.ip(&["route", "add", "default", "via", "192.0.2.1", "dev", "net0"])?;
    previous = observation(
        &client,
        &mut receiver,
        &previous,
        "IPv4 route restored",
        NetworkStatus::Available,
        IpStack::V4Only,
    )
    .await?;

    // Only the unaddressed peer changes. net0 keeps its address, admin-up flag,
    // and default route, while its carrier (IFF_RUNNING/LOWER_UP) changes.
    network.ip(&["link", "set", "peer0", "down"])?;
    previous = observation(
        &client,
        &mut receiver,
        &previous,
        "IPv4 carrier lost",
        NetworkStatus::Unavailable,
        IpStack::V4Only,
    )
    .await?;
    network.ip(&["link", "set", "peer0", "up"])?;
    previous = observation(
        &client,
        &mut receiver,
        &previous,
        "IPv4 carrier restored",
        NetworkStatus::Available,
        IpStack::V4Only,
    )
    .await?;

    network.ip(&[
        "link", "add", "alt0", "type", "veth", "peer", "name", "altpeer0",
    ])?;
    network.ip(&["link", "set", "alt0", "up"])?;
    network.ip(&["link", "set", "altpeer0", "up"])?;
    network.ip(&["address", "add", "198.51.100.2/24", "dev", "alt0"])?;
    network.ip(&[
        "route",
        "add",
        "default",
        "via",
        "198.51.100.1",
        "dev",
        "alt0",
        "metric",
        "100",
    ])?;
    network.ip(&["link", "set", "peer0", "down"])?;
    previous = stays_available(&client, &previous).await?;
    network.ip(&["link", "set", "peer0", "up"])?;
    network.ip(&["link", "del", "alt0"])?;

    network.ip(&["route", "del", "default"])?;
    network.ip(&["address", "del", "192.0.2.2/24", "dev", "net0"])?;
    previous = observation(
        &client,
        &mut receiver,
        &previous,
        "IPv4 removed before IPv6-only",
        NetworkStatus::Unavailable,
        IpStack::None,
    )
    .await?;

    for (address, gateway, label) in [
        ("2001:db8:1::2/64", "2001:db8:1::1", "IPv6 global"),
        ("fd12:3456:789a::2/64", "fd12:3456:789a::1", "IPv6 ULA"),
    ] {
        network.ip(&["-6", "address", "add", address, "dev", "net0", "nodad"])?;
        network.ip(&[
            "-6", "route", "add", "default", "via", gateway, "dev", "net0",
        ])?;
        previous = observation(
            &client,
            &mut receiver,
            &previous,
            &format!("{label} default via gateway"),
            NetworkStatus::Available,
            IpStack::V6Only,
        )
        .await?;
        network.ip(&["-6", "route", "del", "default"])?;
        previous = observation(
            &client,
            &mut receiver,
            &previous,
            &format!("{label} default removed"),
            NetworkStatus::Unavailable,
            IpStack::V6Only,
        )
        .await?;
        network.ip(&["-6", "route", "add", "default", "dev", "net0"])?;
        previous = observation(
            &client,
            &mut receiver,
            &previous,
            &format!("{label} default dev without gateway"),
            NetworkStatus::Available,
            IpStack::V6Only,
        )
        .await?;
        network.ip(&["-6", "route", "del", "default"])?;
        network.ip(&["-6", "address", "del", address, "dev", "net0"])?;
        previous = observation(
            &client,
            &mut receiver,
            &previous,
            &format!("{label} removed"),
            NetworkStatus::Unavailable,
            IpStack::None,
        )
        .await?;
    }

    // Stop retains the stream; restart must discover changes made while stopped.
    bounded("stop", client.stop()).await??;
    let stopped = bounded("stopped stream snapshot", receiver.recv())
        .await??
        .ok_or("stop ended the stream")?;
    check(
        matches!(stopped.state, MonitorState::Stopped),
        "stop must publish Stopped",
    )?;
    inactive(&stopped, &previous)?;
    network.ip(&[
        "-6",
        "address",
        "add",
        "fd12:3456:789a::2/64",
        "dev",
        "net0",
        "nodad",
    ])?;
    network.ip(&["-6", "route", "add", "default", "dev", "net0"])?;
    bounded("restart", client.start()).await??;
    previous = observation(
        &client,
        &mut receiver,
        &stopped,
        "restart observes changed IPv6 route",
        NetworkStatus::Available,
        IpStack::V6Only,
    )
    .await?;
    network.ip(&["-6", "route", "del", "default"])?;
    previous = observation(
        &client,
        &mut receiver,
        &previous,
        "restarted monitor receives route loss",
        NetworkStatus::Unavailable,
        IpStack::V6Only,
    )
    .await?;

    bounded("shutdown", client.shutdown()).await??;
    let closed = bounded("closed stream snapshot", receiver.recv())
        .await??
        .ok_or("shutdown lost final snapshot")?;
    check(
        matches!(closed.state, MonitorState::Closed),
        "shutdown must publish Closed",
    )?;
    inactive(&closed, &previous)?;
    check(
        bounded("stream end", receiver.recv()).await??.is_none(),
        "closed stream must end",
    )?;
    check(
        matches!(bounded("restart after shutdown", client.start()).await?,
            Err(error) if error.kind() == ErrorKind::Closed),
        "shutdown must permanently reject restart",
    )?;
    bounded("repeated shutdown", client.shutdown()).await??;
    check(
        client.snapshot()?.revision == closed.revision,
        "repeated shutdown must be idempotent",
    )?;
    bounded(
        "destroy client",
        net.destroy_net_status_client("linux-namespace"),
    )
    .await??;
    Ok(())
}

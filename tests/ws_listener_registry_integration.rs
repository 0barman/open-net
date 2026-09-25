#![cfg(feature = "ws-client")]
use open_net::ws::{
    ConnectOptions, ConnectionState, InitialMessages, ReceiveOptions, ReconnectPolicy, Session,
    TaskEventOptions, WebSocketClientConfig,
};
use open_net::{NetError, OpenNet};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::net::TcpListener;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn check(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(message.into())
    }
}
async fn start(
    net: &OpenNet,
    name: &str,
    config: WebSocketClientConfig,
) -> TestResult<(open_net::WebSocketClient, Session, TcpListener)> {
    let peer = TcpListener::bind("127.0.0.1:0").await?;
    let client = net.create_ws_client_with_config(name, config).await?;
    let mut options = ConnectOptions::new(format!("ws://{}", peer.local_addr()?));
    options.reconnect = ReconnectPolicy::Disabled;
    options.initial_messages = InitialMessages::DiscardUnmatched;
    let session = client.start_session(options, None).await?;
    Ok((client, session, peer))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registrations_are_distinct_and_unregistration_is_scoped_and_idempotent() -> TestResult {
    let net = OpenNet::new()?;
    let (_, session, _peer) = start(&net, "registry-identities", Default::default()).await?;
    let (_, other, _other_peer) = start(&net, "registry-other", Default::default()).await?;
    let a = session.subscribe_messages(ReceiveOptions::default())?;
    let b = session.subscribe_messages(ReceiveOptions::default())?;
    let state = session.watch_state()?;
    let task = session.subscribe_tasks(TaskEventOptions::default())?;
    let foreign = other.subscribe_messages(ReceiveOptions::default())?;
    check(
        HashSet::from([a.id(), b.id(), state.id(), task.id(), foreign.id()]).len() == 5,
        "subscription IDs reused",
    )?;
    check(
        a.unsubscribe() && !a.unsubscribe(),
        "unsubscribe not idempotent",
    )?;
    check(
        b.unsubscribe() && state.unsubscribe() && task.unsubscribe() && foreign.unsubscribe(),
        "one subscription affected another",
    )?;
    net.destroy_ws_client("registry-identities").await?;
    net.destroy_ws_client("registry-other").await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_status_receives_its_id_and_can_unregister_itself() -> TestResult {
    let net = OpenNet::new()?;
    let (_, session, _peer) = start(&net, "registry-self", Default::default()).await?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let subscription = session.on_state(move |context, state| {
        let result = (context.id(), state, context.unsubscribe());
        let _ = tx.send(result);
    })?;
    let (id, state, removed) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await?
        .ok_or("initial state missing")?;
    check(
        id == subscription.id() && removed && !subscription.unsubscribe(),
        "self-unsubscribe identity/state changed",
    )?;
    check(
        !matches!(state?.state, ConnectionState::Closed(_)),
        "initial session already closed",
    )?;
    net.destroy_ws_client("registry-self").await?;
    Ok(())
}
#[tokio::test]
async fn closed_session_rejects_new_work_and_retains_terminal_observation() -> TestResult {
    let net = OpenNet::new()?;
    let (client, session, _peer) = start(&net, "registry-closed", Default::default()).await?;
    client.shutdown().await?;
    check(client.is_shutdown(), "shutdown state missing")?;
    check(
        matches!(session.state()?.state, ConnectionState::Closed(_)),
        "terminal state unavailable",
    )?;
    check(
        session.sender().send("late").await.is_err(),
        "closed session accepted work",
    )?;
    check(
        matches!(client.start_session("ws://127.0.0.1:1", None).await, Err(e) if e.kind() == open_net::error::ErrorKind::Closed),
        "closed client admitted session",
    )?;
    net.destroy_ws_client("registry-closed").await?;
    Ok(())
}
#[tokio::test]
async fn listener_limits_are_fallible_and_removing_one_data_subscription_restores_capacity(
) -> TestResult {
    let net = OpenNet::new()?;
    let mut config = WebSocketClientConfig::default();
    config.dispatch.message_subscriptions = 1;
    config.dispatch.task_subscriptions = 1;
    config.dispatch.state_subscriptions = 1;
    let (_, session, _peer) = start(&net, "registry-capacity", config).await?;
    let a = session.subscribe_messages(Default::default())?;
    check(
        matches!(session.subscribe_messages(Default::default()), Err(e) if e.kind() == open_net::error::ErrorKind::SubscriptionLimitReached),
        "message limit bypassed",
    )?;
    check(a.unsubscribe(), "unsubscribe lost")?;
    let b = session.subscribe_messages(Default::default())?;
    check(a.id() != b.id(), "retired ID reused")?;
    let task = session.subscribe_tasks(Default::default())?;
    check(
        matches!(session.subscribe_tasks(Default::default()), Err(e) if e.kind() == open_net::error::ErrorKind::SubscriptionLimitReached),
        "task limit bypassed",
    )?;
    check(task.unsubscribe(), "task unsubscribe lost")?;
    let state = session.watch_state()?;
    check(
        matches!(session.watch_state(), Err(e) if e.kind() == open_net::error::ErrorKind::SubscriptionLimitReached),
        "state limit bypassed",
    )?;
    check(state.unsubscribe(), "state unsubscribe lost")?;
    net.destroy_ws_client("registry-capacity").await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_registrations_preserve_every_id_across_shared_session_handles() -> TestResult {
    let net = OpenNet::new()?;
    let (_, session, _peer) = start(&net, "registry-concurrent", Default::default()).await?;
    let session = Arc::new(session);
    let mut threads = Vec::new();
    for _ in 0..8 {
        let session = session.clone();
        threads.push(
            std::thread::Builder::new().spawn(move || -> Result<_, NetError> {
                (0..4)
                    .map(|_| session.subscribe_messages(Default::default()))
                    .collect::<Result<Vec<_>, _>>()
            })?,
        );
    }
    let mut ids = HashSet::new();
    let mut subscriptions = Vec::new();
    for thread in threads {
        for subscription in thread.join().map_err(|_| "registration thread failed")?? {
            check(ids.insert(subscription.id()), "concurrent ID reused")?;
            subscriptions.push(subscription);
        }
    }
    check(ids.len() == 32, "registrations lost")?;
    for subscription in subscriptions {
        check(subscription.unsubscribe(), "subscription not removable")?;
    }
    net.destroy_ws_client("registry-concurrent").await?;
    Ok(())
}

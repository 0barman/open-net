#![cfg(feature = "ws-client")]
#[path = "support/v2_peer.rs"]
mod v2;
use open_net::ws::*;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as Wire;
use v2::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_initial_status_callback_allows_network_traffic_heartbeats_and_bounded_shutdown(
) -> TestResult {
    let (_net, client, mut session, mut peer) =
        connected("blocked-status", WebSocketClientConfig::default()).await?;
    let mut incoming = session.take_messages().ok_or("inbox")?;
    let gate = Gate::default();
    let _release = ReleaseOnDrop(gate.clone());
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _status = session.on_state(move |_, state| {
        if state.is_ok() {
            let _ = tx.send(());
            gate.wait();
        }
    })?;
    bounded(rx.recv()).await?;
    session
        .sender()
        .message("outgoing")
        .enqueue()
        .await?
        .written()
        .await?;
    check(
        matches!(peer.next().await?,Wire::Text(t) if t=="outgoing"),
        "state callback blocked writer",
    )?;
    peer.text("incoming")?;
    check(
        bounded(incoming.recv())
            .await??
            .and_then(|v| v.message().cloned())
            .and_then(|v| v.as_text().map(str::to_owned))
            .as_deref()
            == Some("incoming"),
        "state callback blocked reader",
    )?;
    peer.outbound.send(Wire::Ping(vec![1, 2, 3].into()))?;
    check(
        matches!(peer.next().await?,Wire::Pong(v) if v.as_ref()==[1,2,3]),
        "state callback blocked Pong",
    )?;
    bounded(client.shutdown()).await??;
    _release.0.release();
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_subscriptions_have_distinct_ids_and_can_unregister_themselves() -> TestResult {
    let (_net, client, session, _peer) =
        connected("status-id", WebSocketClientConfig::default()).await?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let one = tx.clone();
    let a = session.on_state(move |context, value| {
        let _ = one.send((context.id(), value));
        context.unsubscribe();
    })?;
    let b = session.on_state(move |context, value| {
        let _ = tx.send((context.id(), value));
        context.unsubscribe();
    })?;
    check(a.id() != b.id(), "subscription IDs reused")?;
    let first = bounded(rx.recv()).await?.ok_or("initial state")?;
    let second = bounded(rx.recv()).await?.ok_or("second state")?;
    check(
        first.0 != second.0 && first.1.is_ok() && second.1.is_ok(),
        "initial state missing or wrong identity",
    )?;
    client.shutdown().await?;
    check(
        rx.try_recv().is_err(),
        "unsubscribed state listener received terminal state",
    )?;
    Ok(())
}

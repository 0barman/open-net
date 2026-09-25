#![cfg(feature = "ws-client")]
#[path = "support/v2_peer.rs"]
mod v2;
use open_net::ws::*;
use tokio::sync::mpsc;
use v2::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_data_callback_keeps_original_owner_after_same_name_client_recreation() -> TestResult
{
    let (net, client, mut session, peer) =
        connected("same-owner", WebSocketClientConfig::default()).await?;
    let old_client = client.id();
    let old_session = session.id();
    let gate = Gate::default();
    let _release = ReleaseOnDrop(gate.clone());
    let (started_tx, mut started) = mpsc::unbounded_channel();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _callback = session.on_message(move |_, value| {
        let _ = started_tx.send(());
        gate.wait();
        let _ = tx.send(value);
    })?;
    peer.text("old-owner")?;
    bounded(started.recv()).await?;
    bounded(net.destroy_ws_client("same-owner")).await??;
    let replacement = net.create_ws_client("same-owner").await?;
    check(
        replacement.id() != old_client,
        "recreated client reused identity",
    )?;
    _release.0.release();
    let incoming = bounded(rx.recv()).await?.ok_or("callback dropped")??;
    check(
        incoming.client_id() == old_client && incoming.session_id() == old_session,
        "queued callback changed owner",
    )?;
    check(
        incoming.message().and_then(Message::as_text) == Some("old-owner"),
        "queued payload changed",
    )?;
    net.destroy_ws_client("same-owner").await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_callback_can_wait_for_its_own_client_shutdown() -> TestResult {
    let (_net, client, mut session, peer) =
        connected("self-shutdown", WebSocketClientConfig::default()).await?;
    let handle = tokio::runtime::Handle::current();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _callback = session.on_message(move |_, value| {
        if value.is_ok() {
            let _ = tx.send(handle.block_on(client.shutdown()));
        }
    })?;
    peer.text("shutdown")?;
    bounded(rx.recv())
        .await?
        .ok_or("callback did not finish shutdown")??;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_shutdown_finishes_with_blocked_callback_and_releases_terminal_registration(
) -> TestResult {
    let (_net, client, mut session, peer) =
        connected("blocked-shutdown", WebSocketClientConfig::default()).await?;
    let gate = Gate::default();
    let _release = ReleaseOnDrop(gate.clone());
    let (tx, mut rx) = mpsc::unbounded_channel();
    let callback = session.on_message(move |_, value| {
        if value.is_ok() {
            let _ = tx.send(());
            gate.wait();
        }
    })?;
    peer.text("block")?;
    bounded(rx.recv()).await?;
    let (a, b) = bounded(async { tokio::join!(client.shutdown(), client.shutdown()) }).await?;
    a?;
    b?;
    check(client.is_shutdown(), "concurrent shutdown did not finish")?;
    check(
        matches!(session.state()?.state, ConnectionState::Closed(_)),
        "session did not reach terminal state",
    )?;
    callback.unsubscribe();
    _release.0.release();
    check(
        session
            .subscribe_messages(ReceiveOptions::default())
            .is_err(),
        "closed message source admitted work",
    )?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_broadcast_shares_response_and_only_one_subscriber_claims_the_request() -> TestResult {
    let (_net, client, mut session, mut peer) =
        connected("broadcast", WebSocketClientConfig::default()).await?;
    let mut first = session.take_messages().ok_or("initial inbox")?;
    let mut second = session.subscribe_messages(ReceiveOptions::default())?;
    let prepared = session
        .requests()?
        .request(request("broadcast")?)
        .prepare()
        .await?;
    let registration = prepared.handle().registration().clone();
    let receipt = prepared.commit()?;
    peer.next().await?;
    peer.text("answer")?;
    let one = bounded(first.recv()).await??.ok_or("first subscriber")?;
    let two = bounded(second.recv()).await??.ok_or("second subscriber")?;
    check(one.message() == two.message(), "broadcast payload differs")?;
    let resolver = session.response_resolver()?;
    check(
        resolver.resolve(&registration, &one)? == ResolveOutcome::Resolved,
        "first claim failed",
    )?;
    check(
        resolver.resolve(&registration, &two)? == ResolveOutcome::StaleOrFinished,
        "second claim won twice",
    )?;
    check(
        receipt.response().await?.message().as_text() == Some("answer"),
        "response body",
    )?;
    client.shutdown().await?;
    Ok(())
}

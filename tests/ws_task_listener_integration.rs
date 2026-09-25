#![cfg(feature = "ws-client")]
#[path = "support/v2_peer.rs"]
mod v2;
use open_net::ws::*;
use open_net::{error::ErrorKind, OpenNet};
use std::{collections::HashSet, time::Duration};
use tokio::sync::mpsc;
use v2::*;
async fn next(events: &mut TaskEvents) -> TestResult<TaskEvent> {
    bounded(events.recv())
        .await??
        .ok_or_else(|| std::io::Error::other("task event ended").into())
}
fn source(event: &TaskEvent) -> String {
    match &event.source {
        TaskSource::Message(v) => v.as_text().unwrap_or("").to_owned(),
        TaskSource::Request(v) => v.id().as_str().to_owned(),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_listener_registration_is_fallible_and_rejected_after_shutdown() -> TestResult {
    let (_net, client, session, _peer) =
        connected("registration", WebSocketClientConfig::default()).await?;
    check(
        session
            .subscribe_tasks(TaskEventOptions {
                max_tasks: 0,
                ..Default::default()
            })
            .is_err(),
        "invalid subscription accepted",
    )?;
    let subscription = session.subscribe_tasks(TaskEventOptions::default())?;
    subscription.unsubscribe();
    client.shutdown().await?;
    check(
        matches!(session.subscribe_tasks(TaskEventOptions::default()),Err(e) if e.kind()==ErrorKind::QueueClosed),
        "closed task source accepted subscription",
    )?;
    check(
        session.sender().message("late").try_enqueue().is_err(),
        "terminal observation revived session",
    )?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_during_connect_reports_every_queued_request_and_body_once() -> TestResult {
    for close in 0..3 {
        let net = OpenNet::new()?;
        let client = net.create_ws_client("queued").await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let session = client
            .start_session(options(format!("ws://{}", listener.local_addr()?)), None)
            .await?;
        let mut events = session.subscribe_tasks(TaskEventOptions::default())?;
        let wait = SendOptions {
            disconnected: DisconnectedPolicy::WaitForReconnect,
            ..Default::default()
        };
        let tracked = session
            .requests()?
            .request(request("tracked")?)
            .options(RequestOptions {
                send: wait.clone(),
                ..Default::default()
            })
            .try_enqueue()?;
        let a = session
            .sender()
            .message("normal")
            .options(wait.clone())
            .try_enqueue()?;
        let b = session
            .sender()
            .message("urgent")
            .options(SendOptions {
                lane: MessageLane::Urgent,
                ..wait
            })
            .try_enqueue()?;
        match close {
            0 => {
                session.close().await?;
            }
            1 => {
                client.shutdown().await?;
            }
            _ => {
                net.destroy_ws_client("queued").await?;
            }
        }
        let direct = [
            tracked.response().await.map(|_| ()).map_err(|e| e.kind()),
            a.written().await.map_err(|e| e.kind()),
            b.written().await.map_err(|e| e.kind()),
        ];
        let mut seen = HashSet::new();
        for _ in 0..3 {
            let e = next(&mut events).await?;
            check(
                e.session_id == session.id() && e.delivery == DeliveryEvidence::NotStarted,
                "queued task owner/delivery changed",
            )?;
            check(e.result.is_err(), "closed queue task succeeded")?;
            seen.insert(source(&e));
        }
        check(
            seen == HashSet::from(["tracked".into(), "normal".into(), "urgent".into()]),
            "missing or duplicate terminal event",
        )?;
        check(
            direct.iter().all(Result::is_err),
            "receipt disagreed with close",
        )?;
        check(
            bounded(events.recv()).await??.is_none(),
            "extra terminal event",
        )?;
    }
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn written_messages_succeed_once_and_do_not_become_shutdown_failures() -> TestResult {
    let (_net, client, session, mut peer) =
        connected("written", WebSocketClientConfig::default()).await?;
    let mut events = session.subscribe_tasks(TaskEventOptions::default())?;
    let receipt = session.sender().message("written").enqueue().await?;
    receipt.written().await?;
    peer.next().await?;
    let event = next(&mut events).await?;
    check(
        event.operation_id == receipt.id()
            && matches!(event.result, Ok(TaskSuccess::Written))
            && event.delivery == DeliveryEvidence::Written,
        "written terminal differs",
    )?;
    client.shutdown().await?;
    check(
        bounded(events.recv()).await??.is_none(),
        "shutdown emitted second terminal",
    )?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_claim_and_existing_completion_share_one_terminal_success() -> TestResult {
    let (_net, client, mut session, mut peer) =
        connected("claim", WebSocketClientConfig::default()).await?;
    let mut events = session.subscribe_tasks(TaskEventOptions::default())?;
    let mut inbox = session.take_messages().ok_or("inbox")?;
    let receipt = session
        .requests()?
        .request(request("claim")?)
        .enqueue()
        .await?;
    let handle = receipt.handle().clone();
    peer.next().await?;
    peer.text("answer")?;
    let incoming = bounded(inbox.recv()).await??.ok_or("reply")?;
    check(
        session
            .response_resolver()?
            .resolve(handle.registration(), &incoming)?
            == ResolveOutcome::Resolved,
        "claim failed",
    )?;
    check(
        receipt.response().await?.message().as_text() == Some("answer"),
        "wrong response",
    )?;
    let event = next(&mut events).await?;
    check(
        event.operation_id == handle.id()
            && matches!(event.result, Ok(TaskSuccess::ResponseReceived))
            && event.delivery == DeliveryEvidence::ResponseConfirmed,
        "observer differs from receipt",
    )?;
    check(
        handle.cancel()? == TerminationOutcome::AlreadyFinished,
        "cancel rewrote success",
    )?;
    client.shutdown().await?;
    check(
        bounded(events.recv()).await??.is_none(),
        "duplicate terminal",
    )?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn written_pending_requests_report_close_and_match_handle_and_receipt() -> TestResult {
    for close in 0..3 {
        let (net, client, session, mut peer) =
            connected("pending-close", WebSocketClientConfig::default()).await?;
        let mut events = session.subscribe_tasks(TaskEventOptions::default())?;
        let receipt = session
            .requests()?
            .request(request("pending")?)
            .enqueue()
            .await?;
        let handle = receipt.handle().clone();
        handle.written().await?;
        peer.next().await?;
        match close {
            0 => {
                session.close().await?;
            }
            1 => {
                client.shutdown().await?;
            }
            _ => {
                net.destroy_ws_client("pending-close").await?;
            }
        }
        let failure = receipt
            .response()
            .await
            .err()
            .ok_or("closed pending succeeded")?;
        let event = next(&mut events).await?;
        check(
            event.result.as_ref().err().map(|e| e.kind()) == Some(failure.kind()),
            "event and receipt disagree",
        )?;
        check(
            handle
                .state()?
                .result
                .and_then(Result::err)
                .map(|e| e.kind())
                == Some(failure.kind()),
            "handle terminal differs",
        )?;
        check(
            event.delivery == DeliveryEvidence::Written,
            "lost written evidence",
        )?;
        check(bounded(events.recv()).await??.is_none(), "extra terminal")?;
    }
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_task_listener_does_not_block_destroy_and_queued_callbacks_survive() -> TestResult {
    let (net, _client, session, _peer) =
        connected("blocked-task", WebSocketClientConfig::default()).await?;
    let gate = Gate::default();
    let release = ReleaseOnDrop(gate.clone());
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (start, mut started) = mpsc::unbounded_channel();
    let _callback = session.on_task(TaskEventOptions::default(), move |_, event| {
        let _ = start.send(());
        gate.wait();
        let _ = tx.send((tokio::runtime::Handle::try_current().is_err(), event));
    })?;
    for text in ["a", "b"] {
        session
            .sender()
            .message(text)
            .enqueue()
            .await?
            .written()
            .await?;
    }
    bounded(started.recv()).await?;
    bounded(net.destroy_ws_client("blocked-task")).await??;
    release.0.release();
    let mut sources = HashSet::new();
    for _ in 0..2 {
        let (outside, event) = bounded(rx.recv()).await?.ok_or("callback lost")?;
        check(outside, "callback ran in network runtime")?;
        sources.insert(source(&event?));
    }
    check(sources.len() == 2, "queued callbacks lost/duplicated")?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_send_waiting_for_write_capacity_returns_the_original_request() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.queues.normal.max_items = 1;
    let (_net, client, session, mut peer) = connected("waiting", config).await?;
    let mut events = session.subscribe_tasks(TaskEventOptions::default())?;
    let holder = session.sender().message("holder").prepare().await?;
    let group = CancellationGroup::new();
    let request_client = session.requests()?;
    let builder = request_client
        .request(request("waiting-original")?)
        .options(RequestOptions {
            send: SendOptions {
                cancellation: Some(group.clone()),
                ..Default::default()
            },
            ..Default::default()
        });
    let mut waiting = Box::pin(builder.enqueue());
    check(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err(),
        "capacity waiter did not wait",
    )?;
    group.cancel();
    let rejected = bounded(waiting)
        .await?
        .err()
        .ok_or("cancelled admission succeeded")?;
    check(
        rejected.error().kind() == ErrorKind::Cancelled,
        "wrong cancellation cause",
    )?;
    let retained = rejected.into_parts().0;
    let rejected_event = next(&mut events).await?;
    check(
        source(&rejected_event) == "waiting-original"
            && matches!(rejected_event.result,Err(e) if e.kind()==ErrorKind::Cancelled),
        "capacity waiter lost original terminal source",
    )?;
    drop(holder);
    let e = next(&mut events).await?;
    check(source(&e) == "holder", "holder cancellation missing")?;
    let retried = retained
        .options(RequestOptions::default())
        .enqueue()
        .await?;
    retried.handle().written().await?;
    check(
        matches!(peer.next().await?,tokio_tungstenite::tungstenite::Message::Text(v) if v=="waiting-original"),
        "failed admission lost request",
    )?;
    retried.handle().cancel()?;
    let e = next(&mut events).await?;
    check(source(&e) == "waiting-original", "retried source changed")?;
    client.shutdown().await?;
    check(
        bounded(events.recv()).await??.is_none(),
        "unadmitted operation emitted terminal",
    )?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn additive_subscriptions_freeze_recipients_and_unregister_cancels_old_notifications(
) -> TestResult {
    let (_net, client, session, _peer) =
        connected("recipients", WebSocketClientConfig::default()).await?;
    let mut original = session.subscribe_tasks(TaskEventOptions::default())?;
    let removed = session.subscribe_tasks(TaskEventOptions::default())?;
    let prepared = session.sender().message("frozen").prepare().await?;
    let mut late = session.subscribe_tasks(TaskEventOptions::default())?;
    removed.unsubscribe();
    prepared.commit()?.written().await?;
    check(
        source(&next(&mut original).await?) == "frozen",
        "initial recipient missing",
    )?;
    client.shutdown().await?;
    check(
        bounded(late.recv()).await??.is_none(),
        "late subscriber observed historical admission",
    )?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_terminal_capacity_backpressures_normal_tasks_without_consuming_urgent_reserve(
) -> TestResult {
    let (_net, client, session, _peer) =
        connected("reserve", WebSocketClientConfig::default()).await?;
    let mut events = session.subscribe_tasks(TaskEventOptions {
        max_tasks: 2,
        max_payload_bytes: 1024,
        urgent_reserve: Some(QueueLimit {
            max_items: 1,
            max_bytes: 128,
        }),
    })?;
    session
        .sender()
        .message("normal")
        .enqueue()
        .await?
        .written()
        .await?;

    check(
        matches!(session.sender().message("blocked").try_enqueue(),Err(e) if e.error().kind()==ErrorKind::QueueFull),
        "normal exceeded observer reserve",
    )?;
    session
        .sender()
        .message("urgent")
        .options(SendOptions {
            lane: MessageLane::Urgent,
            ..Default::default()
        })
        .enqueue()
        .await?
        .written()
        .await?;
    let normal = next(&mut events).await?;
    check(source(&normal) == "normal", "normal event missing")?;
    // Delivery releases queue capacity; a blocked callback permits only a bounded queued tail.
    session
        .sender()
        .message("released")
        .try_enqueue()?
        .written()
        .await?;
    let a = next(&mut events).await?;
    let b = next(&mut events).await?;
    check(
        HashSet::from([source(&a), source(&b)])
            == HashSet::from(["urgent".into(), "released".into()]),
        "reserve admission lost tasks",
    )?;
    client.shutdown().await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_rejections_timeouts_and_duplicate_ids_match_direct_errors_once() -> TestResult {
    let (_net, client, session, mut peer) =
        connected("errors", WebSocketClientConfig::default()).await?;
    let mut events = session.subscribe_tasks(TaskEventOptions::default())?;
    let prepared = session
        .requests()?
        .request(request("same")?)
        .options(RequestOptions {
            response_timeout: Duration::from_millis(50),
            ..Default::default()
        })
        .prepare()
        .await?;
    check(
        matches!(session.requests()?.request(request("same")?).try_enqueue(),Err(e) if e.error().kind()==ErrorKind::DuplicateRequestId),
        "duplicate accepted",
    )?;
    let receipt = prepared.commit()?;
    peer.next().await?;
    let direct = receipt.response().await.err().ok_or("timeout succeeded")?;
    check(direct.kind() == ErrorKind::TimedOut, "timeout changed")?;
    let duplicate = next(&mut events).await?;
    check(
        matches!(duplicate.result,Err(e) if e.kind()==ErrorKind::DuplicateRequestId),
        "duplicate direct error and event differ",
    )?;
    let event = next(&mut events).await?;
    check(
        event.result.as_ref().err().map(|e| e.kind()) == Some(direct.kind()),
        "timeout event differs",
    )?;
    client.shutdown().await?;
    check(
        bounded(events.recv()).await??.is_none(),
        "extra rejection/timeout terminal",
    )?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_listener_reentrancy_cancels_on_unregister_and_drains_on_shutdown() -> TestResult {
    let (_net, client, session, mut peer) =
        connected("reentry", WebSocketClientConfig::default()).await?;
    let sender = session.sender();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _callback = session.on_task(TaskEventOptions::default(), move |context, event| {
        context.unsubscribe();
        let result = sender.message("callback-reentry").try_enqueue();
        let _ = tx.send((event, result));
    })?;
    session
        .sender()
        .message("trigger")
        .enqueue()
        .await?
        .written()
        .await?;
    peer.next().await?;
    let (event, reentry) = bounded(rx.recv()).await?.ok_or("callback missing")?;
    check(
        matches!(event?.result, Ok(TaskSuccess::Written)),
        "trigger terminal",
    )?;
    reentry?.written().await?;
    check(
        matches!(peer.next().await?,tokio_tungstenite::tungstenite::Message::Text(v) if v=="callback-reentry"),
        "callback reentry blocked",
    )?;
    client.shutdown().await?;
    check(rx.try_recv().is_err(), "unsubscribed callback ran twice")?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_task_callback_bounds_queued_notifications_and_recovers_on_return() -> TestResult {
    let (_net, client, session, _peer) =
        connected("callback-capacity", WebSocketClientConfig::default()).await?;
    let gate = Gate::default();
    let release = ReleaseOnDrop(gate.clone());
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _callback = session.on_task(
        TaskEventOptions {
            max_tasks: 1,
            max_payload_bytes: 1024,
            urgent_reserve: None,
        },
        move |_, event| {
            let _ = tx.send(event);
            gate.wait();
        },
    )?;
    session
        .sender()
        .message("held")
        .enqueue()
        .await?
        .written()
        .await?;
    bounded(rx.recv()).await?.ok_or("callback missing")??;
    session
        .sender()
        .message("queued-behind-callback")
        .try_enqueue()?
        .written()
        .await?;
    check(
        matches!(session.sender().message("blocked").try_enqueue(),Err(e) if e.error().kind()==ErrorKind::QueueFull),
        "blocked callback allowed unbounded queued notifications",
    )?;
    release.0.release();
    session
        .sender()
        .message("after-return")
        .options(SendOptions {
            enqueue_timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        })
        .enqueue()
        .await?
        .written()
        .await?;
    check(
        matches!(
            bounded(rx.recv()).await?.ok_or("second callback")??.result,
            Ok(TaskSuccess::Written)
        ),
        "callback did not recover capacity",
    )?;
    client.shutdown().await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_callback_capture_drop_can_reenter_session_subscription() -> TestResult {
    struct Capture {
        session: std::sync::Arc<Session>,
        done: Option<tokio::sync::oneshot::Sender<bool>>,
    }
    impl Drop for Capture {
        fn drop(&mut self) {
            let result = self.session.watch_state().is_ok();
            if let Some(done) = self.done.take() {
                let _ = done.send(result);
            }
        }
    }
    let (_net, client, session, _peer) =
        connected("capture-drop", WebSocketClientConfig::default()).await?;
    let session = std::sync::Arc::new(session);
    let (done, rx) = tokio::sync::oneshot::channel();
    let capture = Capture {
        session: session.clone(),
        done: Some(done),
    };
    let callback = session.on_task(TaskEventOptions::default(), move |_, _| {
        let _ = capture.session.id();
    })?;
    callback.unsubscribe();
    drop(callback);
    check(
        bounded(rx).await??,
        "capture destructor could not reenter state subscription",
    )?;
    client.shutdown().await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_task_event_retains_source_payload_after_shutdown_until_application_drop(
) -> TestResult {
    struct Payload {
        bytes: Vec<u8>,
        released: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl AsRef<[u8]> for Payload {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            if let Some(tx) = self.released.take() {
                let _ = tx.send(());
            }
        }
    }
    let (_net, client, session, mut peer) =
        connected("source-owner", WebSocketClientConfig::default()).await?;
    let (tx, mut released) = tokio::sync::oneshot::channel();
    let bytes = bytes::Bytes::from_owner(Payload {
        bytes: b"owned-source".to_vec(),
        released: Some(tx),
    });
    let mut events = session.subscribe_tasks(TaskEventOptions::default())?;
    let receipt = session
        .sender()
        .message(Message::binary(bytes))
        .enqueue()
        .await?;
    receipt.written().await?;
    peer.next().await?;
    let event = next(&mut events).await?;
    check(
        matches!(&event.source,TaskSource::Message(m) if m.as_bytes()==b"owned-source"),
        "event lost original payload",
    )?;
    client.shutdown().await?;
    drop(receipt);
    drop(events);
    drop(session);
    drop(client);
    check(
        matches!(
            released.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "application-owned event lost payload early",
    )?;
    drop(event);
    bounded(released).await??;
    Ok(())
}

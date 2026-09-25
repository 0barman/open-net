use super::{IncomingMessage, IncomingOrigin, IncomingPayload};
use crate::ws::{ClientId, ConnectionId, Message, PeerClose, SessionId};
use bytes::Bytes;
use std::time::{Duration, SystemTime};

type TestResult = std::result::Result<(), crate::BoxError>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

fn origin(client: u64, session: u64, connection: u64) -> IncomingOrigin {
    IncomingOrigin::new(
        ClientId::from_allocated(client),
        SessionId::from_allocated(session),
        ConnectionId::from_allocated(connection),
    )
}

#[test]
fn incoming_origin_distinguishes_every_scope_and_preserves_the_assigned_arrival_time() -> TestResult
{
    let original = origin(11, 21, 31);
    let copied = original;
    check(copied == original, "incoming origin was not a stable copy")?;
    let arrived = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(42))
        .ok_or("test timestamp unavailable")?;
    for (client, session, connection) in [(11, 21, 32), (11, 22, 31), (12, 21, 31)] {
        let changed = origin(client, session, connection);
        check(changed != original, "origin ignored one identity dimension")?;
        let incoming = IncomingMessage::new(
            changed,
            IncomingPayload::Message(Message::text("origin-bound data")),
            arrived,
        );
        let cloned = incoming.clone();
        drop(incoming);
        check(
            cloned.client_id().as_u64() == client
                && cloned.session_id().as_u64() == session
                && cloned.connection_id().as_u64() == connection
                && cloned.received_at() == arrived,
            "cloning or retaining the envelope changed its physical origin or time",
        )?;
    }
    Ok(())
}

#[test]
fn incoming_clones_share_text_and_binary_payloads_until_the_remaining_owner_drops() -> TestResult {
    for message in [
        Message::text("完整 UTF-8 正文"),
        Message::binary(vec![0, 255, 128, 3]),
    ] {
        let expected = message.clone();
        let incoming = IncomingMessage::new(
            origin(1, 2, 3),
            IncomingPayload::Message(message),
            SystemTime::UNIX_EPOCH,
        );
        let clone = incoming.clone();
        check(
            std::ptr::eq(incoming.payload(), clone.payload()),
            "IncomingMessage clone duplicated its shared payload allocation",
        )?;
        let bytes = incoming
            .message()
            .ok_or("business message missing")?
            .as_bytes();
        let pointer = bytes.as_ptr();
        check(
            clone
                .message()
                .ok_or("cloned business message missing")?
                .as_bytes()
                .as_ptr()
                == pointer,
            "envelope clone copied the body",
        )?;
        drop(incoming);
        check(
            clone.message() == Some(&expected) && clone.received_at() == SystemTime::UNIX_EPOCH,
            "remaining envelope lost its body or original arrival time",
        )?;
    }
    Ok(())
}

#[test]
fn ping_pong_and_close_remain_distinct_control_payloads_without_business_messages() -> TestResult {
    let reason = "界".repeat(41);
    for (index, payload) in [
        IncomingPayload::Ping(Bytes::from_static(&[0, 255, 1])),
        IncomingPayload::Pong(Bytes::from_static(&[128, 0, 2])),
        IncomingPayload::Close(PeerClose {
            code: Some(1000),
            reason: reason.clone(),
        }),
    ]
    .into_iter()
    .enumerate()
    {
        let incoming = IncomingMessage::new(origin(1, 2, 3), payload, SystemTime::UNIX_EPOCH);
        let cloned = incoming.clone();
        check(
            std::ptr::eq(incoming.payload(), cloned.payload()),
            "control envelope clone duplicated its payload",
        )?;
        drop(incoming);
        check(
            cloned.message().is_none(),
            "control payload was exposed as application data",
        )?;
        let correct_payload = match (index, cloned.payload()) {
            (0, IncomingPayload::Ping(value)) => value.as_ref() == [0, 255, 1],
            (1, IncomingPayload::Pong(value)) => value.as_ref() == [128, 0, 2],
            (2, IncomingPayload::Close(value)) => {
                value.code == Some(1000) && value.reason == reason
            }
            _ => false,
        };
        check(correct_payload, "control kind or complete payload changed")?;
        check(
            cloned.received_at() == SystemTime::UNIX_EPOCH,
            "control envelope generated a new arrival time",
        )?;
    }
    Ok(())
}

#[test]
fn response_retains_the_business_message_without_retaining_the_incoming_allocation() -> TestResult {
    let incoming = IncomingMessage::new(
        origin(1, 2, 3),
        IncomingPayload::Message(Message::text("independent response owner")),
        SystemTime::UNIX_EPOCH,
    );
    let envelope_payload = std::sync::Arc::downgrade(&incoming.payload);
    let response = crate::ws::Response::from_incoming(
        crate::ws::RequestId::new("response-owner")?,
        &incoming,
    )?;
    drop(incoming);
    check(
        envelope_payload.upgrade().is_none(),
        "response retained the incoming envelope's shared payload allocation",
    )?;
    check(
        response.message().as_text() == Some("independent response owner")
            && response.connection_id().as_u64() == 3,
        "response depended on the retired incoming allocation",
    )
}

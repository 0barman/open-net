use super::Response;
use crate::error::ErrorKind;
use crate::ws::{
    ClientId, ConnectionId, IncomingMessage, IncomingOrigin, IncomingPayload, Message, PeerClose,
    RequestId, SessionId,
};
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

type TestResult = std::result::Result<(), crate::BoxError>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

fn incoming(payload: IncomingPayload) -> IncomingMessage {
    IncomingMessage::new(
        IncomingOrigin::new(
            ClientId::from_allocated(11),
            SessionId::from_allocated(21),
            ConnectionId::from_allocated(31),
        ),
        payload,
        SystemTime::UNIX_EPOCH,
    )
}

#[test]
fn text_response_survives_its_incoming_envelope_and_moves_owned_text_without_copying() -> TestResult
{
    let request = RequestId::new(" 业务-request-7 ")?;
    let response = {
        let incoming = incoming(IncomingPayload::Message(Message::text("完整响应正文")));
        Response::from_incoming(request.clone(), &incoming)?
    };
    check(
        response.request_id() == &request
            && response.received_at() == SystemTime::UNIX_EPOCH
            && response.connection_id().as_u64() == 31,
        "response lost request identity, connection identity or original time",
    )?;
    let clone = response.clone();
    drop(response);
    check(
        clone.message().as_text() == Some("完整响应正文") && clone.request_id() == &request,
        "response clone depended on a discarded envelope or response",
    )?;
    let pointer = clone.message().as_bytes().as_ptr();
    let message = clone.into_message();
    check(
        message.as_text() == Some("完整响应正文") && message.as_bytes().as_ptr() == pointer,
        "into_message copied or changed the owned text",
    )
}

#[test]
fn binary_response_clones_share_bytes_and_release_the_owner_after_the_last_message() -> TestResult {
    /// Test owner used to verify shared response-body storage and its final
    /// release timing.
    struct Owner(
        /// Counts releases of the response body owner.
        Arc<AtomicUsize>,
    );
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &[0, 255, 128, 7]
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let incoming = incoming(IncomingPayload::Message(Message::binary(
        Bytes::from_owner(Owner(drops.clone())),
    )));
    let pointer = incoming
        .message()
        .ok_or("binary message missing")?
        .as_bytes()
        .as_ptr();
    let response = Response::from_incoming(RequestId::new("binary-request")?, &incoming)?;
    let clone = response.clone();
    drop(incoming);
    drop(response);
    check(
        drops.load(Ordering::SeqCst) == 0
            && clone.message().as_bytes().as_ptr() == pointer
            && clone.message().as_text().is_none(),
        "response copied, decoded or prematurely retired its binary owner",
    )?;
    let message = clone.into_message();
    check(
        message.as_bytes() == [0, 255, 128, 7] && message.as_bytes().as_ptr() == pointer,
        "moving binary response changed its original byte allocation",
    )?;
    drop(message);
    check(
        drops.load(Ordering::SeqCst) == 1,
        "response ownership retained or duplicated the final byte owner",
    )
}

#[test]
fn control_frames_cannot_be_claimed_as_responses_and_are_unchanged_after_rejection() -> TestResult {
    for payload in [
        IncomingPayload::Ping(Bytes::from_static(b"ping")),
        IncomingPayload::Pong(Bytes::from_static(b"pong")),
        IncomingPayload::Close(PeerClose {
            code: None,
            reason: String::new(),
        }),
    ] {
        let incoming = incoming(payload);
        let before = incoming.payload() as *const IncomingPayload;
        let error = Response::from_incoming(RequestId::new("request")?, &incoming)
            .err()
            .ok_or("control frame was accepted as a business response")?;
        check(
            error.kind() == ErrorKind::InvalidInput,
            "invalid response classification changed",
        )?;
        check(
            std::ptr::eq(before, incoming.payload())
                && incoming.message().is_none()
                && incoming.connection_id().as_u64() == 31
                && incoming.received_at() == SystemTime::UNIX_EPOCH,
            "rejected response claim mutated its borrowed input",
        )?;
    }
    Ok(())
}

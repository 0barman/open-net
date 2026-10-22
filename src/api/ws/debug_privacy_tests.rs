#![cfg(test)]

//! Regressions for Debug composition across the WS public API.
//!
//! These fixtures never connect or submit a message. Check privacy and useful
//! diagnostics rather than treating Debug field order or whitespace as an API.
use super::*;
use crate::module::ws_client::v2_test_support;
use crate::{Bytes, Metadata};
use std::fmt::Debug;
use std::sync::{Arc, Weak};
use std::time::{Instant, SystemTime};

type TestResult = Result<(), crate::BoxError>;

const TEXT: &str = "ws-private-text-正文";
const BINARY: &str = "ws-private-binary";
const ID: &str = " ws-private-id-用户 ";
const META_KEY: &str = "ws-private-metadata-key";
const META_VALUE: &str = "ws-private-metadata-value";
const PING: &str = "ws-private-ping";
const PONG: &str = "ws-private-pong";
const CLOSE: &str = "ws-private-close-原因";
const PRIVATE_VALUES: &[&str] = &[TEXT, BINARY, ID, META_KEY, META_VALUE, PING, PONG, CLOSE];

fn private_debug(value: &impl Debug, diagnostics: &[&str]) {
    for rendered in [
        format!("{value:?}"),
        format!("{value:#?}"),
        format!("{:?}", Some(value)),
        format!("{:#?}", Some(value)),
    ] {
        for secret in PRIVATE_VALUES {
            assert!(!rendered.contains(secret), "Debug exposed a private string");
            // A derived Debug on a byte slice may expose decimal values instead
            // of the original string, including in alternate formatting.
            for byte_dump in [
                format!("{:?}", secret.as_bytes()),
                format!("{:#?}", secret.as_bytes()),
            ] {
                let compact_dump: String =
                    byte_dump.chars().filter(|ch| !ch.is_whitespace()).collect();
                let compact_rendered: String =
                    rendered.chars().filter(|ch| !ch.is_whitespace()).collect();
                assert!(
                    !compact_rendered.contains(&compact_dump),
                    "Debug exposed private bytes"
                );
            }
        }
        for diagnostic in diagnostics {
            assert!(
                rendered.contains(diagnostic),
                "Debug lost useful diagnostic {diagnostic}"
            );
        }
    }
}

fn metadata() -> Metadata {
    Metadata::from([(META_KEY.to_owned(), META_VALUE.to_owned())])
}

fn request() -> crate::Result<Request> {
    Ok(Request::new(RequestId::new(ID)?, Message::text(TEXT)).with_metadata(metadata()))
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
fn incoming_and_response_debug_hide_bodies_and_business_identifiers() -> TestResult {
    for message in [
        Message::text(TEXT),
        Message::binary(Bytes::from_static(BINARY.as_bytes())),
    ] {
        let received = incoming(IncomingPayload::Message(message.clone()));
        private_debug(&received, &["IncomingMessage", "received_at", "bytes"]);
        assert_eq!(received.message(), Some(&message));
        assert_eq!(received.received_at(), SystemTime::UNIX_EPOCH);
        assert_eq!(received.connection_id().as_u64(), 31);

        let response = Response::from_incoming(RequestId::new(ID)?, &received)?;
        private_debug(&response, &["Response", "RequestId", "bytes"]);
        assert_eq!(response.request_id().as_str(), ID);
        assert_eq!(response.message().as_bytes(), message.as_bytes());
        assert_eq!(response.received_at(), received.received_at());
        assert_eq!(response.connection_id(), received.connection_id());
        assert_eq!(response.into_message(), message);
    }
    for route in [
        ResponseRoute::Intermediate {
            request_id: RequestId::new(ID)?,
        },
        ResponseRoute::Final {
            request_id: RequestId::new(ID)?,
        },
    ] {
        private_debug(&route, &["RequestId", "bytes"]);
    }
    Ok(())
}

#[test]
fn control_frame_debug_hides_payloads_and_preserves_their_wire_values() -> TestResult {
    for (payload, marker, variant) in [
        (
            IncomingPayload::Ping(Bytes::copy_from_slice(PING.as_bytes())),
            PING,
            "Ping",
        ),
        (
            IncomingPayload::Pong(Bytes::copy_from_slice(PONG.as_bytes())),
            PONG,
            "Pong",
        ),
    ] {
        private_debug(&payload, &[variant, "bytes"]);
        let received = incoming(payload);
        private_debug(&received, &["IncomingMessage", variant, "bytes"]);
        assert!(received.message().is_none());
        match received.payload() {
            IncomingPayload::Ping(bytes) | IncomingPayload::Pong(bytes) => {
                assert_eq!(bytes.as_ref(), marker.as_bytes());
            }
            _ => return Err("control-frame variant changed".into()),
        }
    }

    let close = CloseFrame::new(1000, CLOSE)?;
    private_debug(&close, &["CloseFrame", "1000", "reason_len"]);
    let wire = close.into_wire();
    assert_eq!(u16::from(wire.code), 1000);
    assert_eq!(wire.reason.as_str(), CLOSE);
    let peer = PeerClose::from_frame(Some(&wire))?;
    private_debug(&peer, &["PeerClose", "1000", "reason_len"]);
    let received = incoming(IncomingPayload::Close(peer));
    private_debug(
        &received,
        &["IncomingMessage", "Close", "1000", "reason_len"],
    );
    match received.payload() {
        IncomingPayload::Close(peer) => {
            assert_eq!(peer.code, Some(1000));
            assert_eq!(peer.reason, CLOSE);
        }
        _ => return Err("close-frame variant changed".into()),
    }
    assert!(received.message().is_none());
    Ok(())
}

#[test]
fn task_debug_keeps_diagnostics_without_exposing_original_inputs() -> TestResult {
    for (source, success) in [
        (
            TaskSource::Message(Arc::new(Message::binary(Bytes::from_static(
                BINARY.as_bytes(),
            )))),
            TaskSuccess::Written,
        ),
        (
            TaskSource::Request(Arc::new(request()?)),
            TaskSuccess::ResponseReceived,
        ),
    ] {
        private_debug(&source, &["bytes"]);
        let event = TaskEvent {
            client_id: ClientId::from_allocated(11),
            session_id: SessionId::from_allocated(21),
            operation_id: OperationId::from_allocated(41),
            connection_id: Some(ConnectionId::from_allocated(31)),
            source,
            lane: MessageLane::Normal,
            phase: OperationPhase::Finished,
            delivery: DeliveryEvidence::Written,
            cause: TaskEndCause::Completed,
            result: Ok(success),
        };
        private_debug(
            &event,
            &[
                "TaskEvent",
                "operation_id",
                "Finished",
                "Written",
                "Completed",
            ],
        );
        match &event.source {
            TaskSource::RequestOperation(_) => return Err("unexpected logical request source".into()),
            TaskSource::Message(message) => assert_eq!(message.as_bytes(), BINARY.as_bytes()),
            TaskSource::Request(request) => {
                assert_eq!(request.id().as_str(), ID);
                assert_eq!(request.message().as_text(), Some(TEXT));
                assert_eq!(request.metadata(), &metadata());
            }
        }
        assert_eq!(event.result.as_ref().ok(), Some(&success));
    }
    Ok(())
}

#[test]
fn registration_handle_and_snapshot_debug_hide_id_and_metadata() -> TestResult {
    let registered_at = Instant::now();
    let registration = RequestRegistration::new(
        RequestId::new(ID)?,
        SessionId::from_allocated(1),
        registered_at,
        7,
        Weak::new(),
        Arc::new(request_control::RegistrationTiming::new(None)),
    );
    private_debug(
        &registration,
        &["RequestRegistration", "RequestId", "registered_at"],
    );
    assert_eq!(registration.request_id().as_str(), ID);
    assert_eq!(registration.registered_at(), registered_at);
    assert_eq!(registration.response_deadline()?, None);

    let options = SendOptions {
        metadata: metadata(),
        ..Default::default()
    };
    let core = v2_test_support::operation(&options, true, 41)?;
    let handle = RequestHandle::new(core, registration);
    private_debug(
        &handle,
        &[
            "RequestHandle",
            "RequestRegistration",
            "WaitingForCapacity",
            "NotStarted",
        ],
    );
    assert_eq!(handle.request_id().as_str(), ID);
    assert_eq!(handle.id().as_u64(), 41);
    let snapshot = RequestSnapshot {
        operation_id: handle.id(),
        request_id: handle.request_id().clone(),
        session_id: handle.registration().session_id(),
        connection_id: None,
        registered_at,
        response_deadline: None,
        metadata: metadata(),
        operation: handle.state()?,
    };
    private_debug(
        &snapshot,
        &[
            "RequestSnapshot",
            "metadata_entries",
            "WaitingForCapacity",
            "NotStarted",
        ],
    );
    assert_eq!(snapshot.request_id.as_str(), ID);
    assert_eq!(snapshot.metadata, metadata());
    Ok(())
}

#[test]
fn unsubmitted_builders_keep_nested_debug_private_without_admission() -> TestResult {
    let (runtime, _) = v2_test_support::unconnected(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        11,
        21,
        None,
    )?;
    let options = SendOptions {
        metadata: metadata(),
        ..Default::default()
    };
    let message = v2_test_support::sender(&runtime)
        .message(Message::text(TEXT))
        .options(options.clone());
    private_debug(
        &message,
        &["MessageBuilder", "SendOptions", "metadata_entries", "bytes"],
    );
    let requests = v2_test_support::requests(&runtime);
    let request = requests.request(request()?).options(RequestOptions {
        send: options,
        ..Default::default()
    });
    private_debug(
        &request,
        &[
            "RequestBuilder",
            "RequestOptions",
            "RequestId",
            "metadata_entries",
            "bytes",
        ],
    );
    assert!(requests.pending_snapshot()?.is_empty());
    assert_eq!(runtime.current_connection_id()?, None);
    Ok(())
}

#![cfg(feature = "ws-client")]

use open_net::ws::{IncomingMessage, IncomingPayload, Message, Request, RequestId, Response};
use open_net::{Bytes, Metadata};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn check(value: bool, reason: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(reason).into())
    }
}

#[test]
fn owned_request_preserves_identity_body_and_metadata_when_moved() -> TestResult {
    let id = RequestId::new("operation-甲")?;
    let text = "owned request data".to_owned();
    let address = text.as_ptr();
    let mut metadata = Metadata::new();
    metadata.insert("trace".to_owned(), "first".to_owned());
    let request = Request::new(id.clone(), Message::text(text)).with_metadata(metadata.clone());
    check(request.id() == &id, "request identity changed")?;
    check(
        request.message().as_text() == Some("owned request data"),
        "request body changed",
    )?;
    check(
        request.metadata() == &metadata,
        "metadata changed during construction",
    )?;
    let (moved_id, message, moved_metadata) = request.into_parts();
    check(
        moved_id == id && moved_metadata == metadata,
        "moving lost request fields",
    )?;
    check(
        message.as_bytes().as_ptr() == address,
        "moving copied the owned text allocation",
    )
}

#[test]
fn cloned_request_can_replace_metadata_without_changing_the_original() -> TestResult {
    let body = Bytes::from_static(b"shared binary body");
    let mut original_metadata = Metadata::new();
    original_metadata.insert("a".to_owned(), "initial".to_owned());
    let original = Request::new(
        RequestId::new("binary-operation")?,
        Message::binary(body.clone()),
    )
    .with_metadata(original_metadata.clone());
    let mut replacement = Metadata::new();
    replacement.insert("b".to_owned(), "replacement".to_owned());
    let cloned = original.clone().with_metadata(replacement.clone());
    check(
        original.metadata() == &original_metadata,
        "cloning changed original metadata",
    )?;
    check(
        cloned.metadata() == &replacement,
        "with_metadata merged instead of replacing",
    )?;
    check(
        cloned.message().as_bytes().as_ptr() == body.as_ptr(),
        "binary clone copied payload storage",
    )?;
    let empty = Request::new(RequestId::new("empty")?, Message::text(""));
    check(
        empty.message().is_empty() && empty.metadata().is_empty(),
        "empty request defaults changed",
    )
}

#[test]
fn public_message_values_support_the_declared_owned_interfaces() {
    fn transferable<T: Clone + std::fmt::Debug + Send + Sync + 'static>() {}
    transferable::<Request>();
    transferable::<IncomingPayload>();
    transferable::<IncomingMessage>();
    transferable::<Response>();
    let _payload: fn(&IncomingMessage) -> &IncomingPayload = IncomingMessage::payload;
    let _message: fn(&IncomingMessage) -> Option<&Message> = IncomingMessage::message;
    let _response_message: fn(&Response) -> &Message = Response::message;
    let _into_message: fn(Response) -> Message = Response::into_message;
    let _parts: fn(Request) -> (RequestId, Message, Metadata) = Request::into_parts;
}

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

fn check_private_debug<T: std::fmt::Debug + ?Sized>(
    value: &T,
    summaries: &[&str],
    private_values: &[&str],
) -> TestResult {
    let nested = Some(value);
    for output in [
        format!("{value:?}"),
        format!("{value:#?}"),
        format!("{nested:?}"),
        format!("{nested:#?}"),
    ] {
        for summary in summaries {
            check(
                output.contains(*summary),
                "Debug lost useful payload or scheduling summary",
            )?;
        }
        let compact = output.split_whitespace().collect::<String>();
        for private in private_values {
            let compact_private = private.split_whitespace().collect::<String>();
            check(
                !output.contains(*private) && !compact.contains(compact_private.as_str()),
                "Debug exposed a private value, including inside an Option",
            )?;
        }
    }
    Ok(())
}

#[test]
fn public_debug_summarizes_payloads_and_preserves_explicit_request_id_access() -> TestResult {
    const TEXT: &str = "private-text-payload-用户";
    const BINARY: &[u8] = b"\x00\xffprivate-binary-payload\x80";
    const ID: &str = "  private-request-id-用户\t ";

    let text = Message::text(TEXT);
    let text_size = format!("bytes: {}", TEXT.len());
    check_private_debug(
        &text,
        &["Text", &text_size],
        &["private-text-payload", "用户"],
    )?;
    check(
        text.as_text() == Some(TEXT) && text.as_bytes() == TEXT.as_bytes(),
        "Debug changed the text payload",
    )?;
    check(
        text.into_bytes().as_ref() == TEXT.as_bytes(),
        "moving a formatted text message changed its bytes",
    )?;

    let binary = Message::binary(Bytes::from_static(BINARY));
    let binary_size = format!("bytes: {}", BINARY.len());
    let numeric_dump = format!("{BINARY:?}");
    check_private_debug(
        &binary,
        &["Binary", &binary_size],
        &["private-binary-payload", "\\xff", &numeric_dump],
    )?;
    check(
        binary.as_text().is_none() && binary.as_bytes() == BINARY,
        "Debug decoded or changed the binary payload",
    )?;
    check(
        binary.into_bytes().as_ref() == BINARY,
        "moving a formatted binary message changed its bytes",
    )?;

    let id = RequestId::new(ID)?;
    let id_size = format!("bytes: {}", ID.len());
    check_private_debug(
        &id,
        &["RequestId", &id_size],
        &["private-request-id", "用户"],
    )?;
    check(
        id.as_str() == ID
            && id.as_ref() == ID
            && id.to_string() == ID
            && format!("{id:#}") == ID
            && id.into_string() == ID,
        "Debug redaction changed explicit request identifier access or Display",
    )
}

#[test]
fn public_debug_hides_request_and_send_metadata_without_changing_owned_values() -> TestResult {
    use open_net::ws::{Priority, RequestOptions, SendOptions};

    const ID: &str = "private-owned-request-id";
    const BODY: &str = "private-owned-request-body";
    const KEY: &str = "private-metadata-key";
    const VALUE: &str = "private-metadata-value";
    let metadata = Metadata::from([(KEY.to_owned(), VALUE.to_owned())]);
    let request =
        Request::new(RequestId::new(ID)?, Message::text(BODY)).with_metadata(metadata.clone());
    check_private_debug(
        &request,
        &["Request", "RequestId", "Text", "metadata_entries: 1"],
        &[ID, BODY, KEY, VALUE],
    )?;
    check(
        request.id().as_str() == ID
            && request.message().as_text() == Some(BODY)
            && request.metadata() == &metadata,
        "Debug changed the original request fields",
    )?;
    let (id, body, moved_metadata) = request.into_parts();
    check(
        id.as_str() == ID && body.as_text() == Some(BODY) && moved_metadata == metadata,
        "formatting a request changed its recovered components",
    )?;

    let send = SendOptions {
        priority: Priority::High,
        metadata: metadata.clone(),
        ..Default::default()
    };
    check_private_debug(
        &send,
        &["SendOptions", "priority: High", "metadata_entries: 1"],
        &[KEY, VALUE],
    )?;
    let options = RequestOptions {
        send,
        ..Default::default()
    };
    check_private_debug(
        &options,
        &[
            "RequestOptions",
            "SendOptions",
            "priority: High",
            "metadata_entries: 1",
        ],
        &[KEY, VALUE],
    )?;
    check(
        options.send.metadata == metadata && options.send.priority == Priority::High,
        "nested Debug redaction changed request options",
    )
}

#[test]
fn public_debug_hides_close_reasons_but_retains_close_code_and_byte_count() -> TestResult {
    use open_net::ws::{CloseFrame, PeerClose};

    const REASON: &str = "private-close-reason-用户";
    let reason_size = format!("reason_len: {}", REASON.len());
    let close = CloseFrame::new(4001, REASON)?;
    check_private_debug(
        &close,
        &["CloseFrame", "code: 4001", &reason_size],
        &["private-close-reason", "用户"],
    )?;
    check(
        close.code() == 4001 && close.reason() == REASON,
        "Debug changed the outbound close code or reason",
    )?;
    let peer = PeerClose {
        code: Some(close.code()),
        reason: close.reason().to_owned(),
    };
    check_private_debug(
        &peer,
        &["PeerClose", "code", "4001", &reason_size],
        &["private-close-reason", "用户"],
    )?;
    check(
        peer.code == Some(4001) && peer.reason == REASON,
        "Debug changed the observed peer close",
    )
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

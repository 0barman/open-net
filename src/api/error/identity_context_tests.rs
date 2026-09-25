use super::{ErrorContext, ErrorKind, ErrorStage, NetError};
use crate::ws::{AttemptId, ClientId, ConnectionId, RequestId, SessionId};
use std::sync::atomic::AtomicU64;

type TestResult = Result<(), crate::BoxError>;
fn check(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[test]
fn default_context_has_no_fabricated_identity() -> TestResult {
    let context = ErrorContext::default();
    check(
        context.client_id.is_none()
            && context.session_id.is_none()
            && context.connection_id.is_none()
            && context.attempt_id.is_none()
            && context.request_id.is_none(),
        "default context fabricated an identity",
    )
}

#[test]
fn cloned_errors_retain_typed_identity_without_dumping_application_request_text() -> TestResult {
    let counter = AtomicU64::new(0);
    let client = ClientId::allocate(&counter)?;
    let session = SessionId::allocate(&counter)?;
    let connection = ConnectionId::allocate(&counter)?;
    let attempt = AttemptId::allocate(&counter)?;
    let request = RequestId::new("private application request identifier")?;
    let context = ErrorContext {
        client_id: Some(client),
        session_id: Some(session),
        connection_id: Some(connection),
        attempt_id: Some(attempt),
        request_id: Some(request.clone()),
        ..Default::default()
    };
    let error = NetError::from(ErrorKind::Io).with_context(context);
    let cloned = error.clone();
    check(
        cloned.context().client_id == Some(client)
            && cloned.context().session_id == Some(session)
            && cloned.context().connection_id == Some(connection)
            && cloned.context().attempt_id == Some(attempt)
            && cloned.context().request_id.as_ref() == Some(&request),
        "cloning the error lost typed identity",
    )?;
    check(
        !format!("{error:?}").contains(request.as_str())
            && !format!("{:?}", error.context()).contains(request.as_str()),
        "default error formatting exposed application request text",
    )
}

#[test]
fn invalid_input_retains_field_detail_and_supports_question_mark() -> TestResult {
    fn build() -> crate::Result<()> {
        Err(NetError::input("request.id", "must not be blank"))?;
        Ok(())
    }
    let error = match build() {
        Ok(()) => return Err("invalid input unexpectedly succeeded".into()),
        Err(error) => error,
    };
    check(
        error.kind() == ErrorKind::InvalidInput
            && error.context().stage == Some(ErrorStage::RequestBuild),
        "invalid input had the wrong category or stage",
    )?;
    let detail = error.config_error().ok_or("input field detail missing")?;
    check(
        detail.field() == "request.id" && detail.reason() == "must not be blank",
        "input validation detail was discarded",
    )
}

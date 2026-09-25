#![cfg(feature = "ws-client")]

use open_net::error::{ErrorContext, ErrorKind};
use open_net::ws::{
    HandshakeBodyCaptureState, HandshakeDiagnostic, HandshakeDiagnosticKind,
    HandshakeDiagnosticOptions, IoEndKind, PeerClose,
};
use open_net::HeaderMap;

type TestResult = std::result::Result<(), open_net::BoxError>;

const _: fn(&HandshakeDiagnostic) -> &HeaderMap = HandshakeDiagnostic::headers;
const _: fn(&HandshakeDiagnostic) -> HandshakeDiagnosticKind = HandshakeDiagnostic::kind;
const _: fn(&HandshakeDiagnostic) -> Option<&str> = HandshakeDiagnostic::body_summary;
const _: fn(&HandshakeDiagnostic) -> HandshakeBodyCaptureState =
    HandshakeDiagnostic::body_capture_state;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[test]
fn diagnostic_options_accept_empty_policy_and_exact_public_code_limits() -> TestResult {
    let default = HandshakeDiagnosticOptions::default();
    default.validate()?;
    check(
        default.public_json_codes.is_empty(),
        "default diagnostics must not opt into response body codes",
    )?;
    let code = "A0_".repeat(21) + "Z";
    let options = HandshakeDiagnosticOptions {
        public_json_codes: vec![code; 32],
    };
    options.validate()?;
    check(
        options.public_json_codes.len() == 32
            && options
                .public_json_codes
                .iter()
                .all(|value| value.len() == 64),
        "validation modified allowed codes or rejected duplicate entries",
    )
}

#[test]
fn invalid_public_codes_report_the_field_without_echoing_input() -> TestResult {
    let invalid = [
        vec![String::new()],
        vec!["lowercase_secret".to_owned()],
        vec!["DASH-SECRET".to_owned()],
        vec!["SPACE SECRET".to_owned()],
        vec!["\u{00c9}SECRET".to_owned()],
        vec!["X".repeat(65)],
        vec!["PUBLIC".to_owned(); 33],
    ];
    for public_json_codes in invalid {
        let options = HandshakeDiagnosticOptions { public_json_codes };
        let failure = match options.validate() {
            Err(failure) => failure,
            Ok(()) => return Err("invalid public diagnostic policy was accepted".into()),
        };
        let detail = failure
            .config_error()
            .ok_or("diagnostic validation omitted its field detail")?;
        check(
            failure.kind() == ErrorKind::InvalidConfig
                && detail.field() == "diagnostics.public_json_codes"
                && !detail.reason().is_empty(),
            "diagnostic validation returned an unexpected category or field",
        )?;
        let rendered = format!("{failure} {failure:?}");
        check(
            !rendered.contains("SECRET") && !rendered.contains("secret"),
            "diagnostic validation rendered rejected input",
        )?;
    }
    Ok(())
}

#[test]
fn mutable_public_options_are_revalidated_and_debug_omits_codes() -> TestResult {
    let mut options = HandshakeDiagnosticOptions {
        public_json_codes: vec!["PRIVATE_FIXTURE_CODE".to_owned()],
    };
    options.validate()?;
    let cloned = options.clone();
    check(
        !format!("{options:?}").contains("PRIVATE_FIXTURE_CODE"),
        "diagnostic options Debug retained configured code text",
    )?;
    options.public_json_codes.push("invalid code".to_owned());
    check(
        matches!(options.validate(), Err(error) if error.kind() == ErrorKind::InvalidConfig),
        "validation trusted a stale result after public field mutation",
    )?;
    cloned.validate()?;
    check(
        cloned.public_json_codes == ["PRIVATE_FIXTURE_CODE"],
        "mutating owned options changed an earlier clone",
    )
}

#[test]
fn peer_close_fields_own_the_reason_and_preserve_an_absent_code() -> TestResult {
    let mut observed = PeerClose {
        code: Some(4001),
        reason: "untrusted peer secret".to_owned(),
    };
    let cloned = observed.clone();
    observed.code = None;
    observed.reason.clear();
    check(
        cloned.code == Some(4001) && cloned.reason == "untrusted peer secret",
        "PeerClose clone did not own complete decoded details",
    )?;
    let empty = PeerClose {
        code: None,
        reason: String::new(),
    };
    check(
        empty.code.is_none() && empty.reason.is_empty(),
        "empty Close was assigned a synthetic wire status",
    )?;
    check(
        !format!("{cloned:?}").contains("untrusted peer secret"),
        "PeerClose Debug exposed untrusted reason text",
    )?;
    let mut context = ErrorContext::default();
    context.peer_close = Some(cloned);
    context.io_end = Some(IoEndKind::PeerClose);
    let retained = context.clone();
    drop(context);
    check(
        retained.peer_close.as_ref().is_some_and(|value| {
            value.code == Some(4001) && value.reason == "untrusted peer secret"
        }) && retained.io_end == Some(IoEndKind::PeerClose),
        "error context did not retain the new owned close observation",
    )
}

// Compile the complete public classification surface without constructing a
// diagnostic through any crate-private implementation helper.
const _: [IoEndKind; 5] = [
    IoEndKind::PeerClose,
    IoEndKind::UnexpectedEof,
    IoEndKind::ConnectionReset,
    IoEndKind::ProtocolError,
    IoEndKind::Other,
];
const _: [HandshakeDiagnosticKind; 9] = [
    HandshakeDiagnosticKind::HttpRejected,
    HandshakeDiagnosticKind::Io(std::io::ErrorKind::ConnectionRefused),
    HandshakeDiagnosticKind::TlsCertificate,
    HandshakeDiagnosticKind::TlsProtocol,
    HandshakeDiagnosticKind::Protocol,
    HandshakeDiagnosticKind::Timeout,
    HandshakeDiagnosticKind::Dns,
    HandshakeDiagnosticKind::Local,
    HandshakeDiagnosticKind::Other,
];
const _: [HandshakeBodyCaptureState; 7] = [
    HandshakeBodyCaptureState::Omitted,
    HandshakeBodyCaptureState::Unavailable,
    HandshakeBodyCaptureState::Incomplete,
    HandshakeBodyCaptureState::Unsupported,
    HandshakeBodyCaptureState::TooLarge,
    HandshakeBodyCaptureState::Unrecognized,
    HandshakeBodyCaptureState::Captured,
];

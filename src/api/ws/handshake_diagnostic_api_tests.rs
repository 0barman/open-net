use super::{HandshakeBodyCaptureState, HandshakeDiagnostic, HandshakeDiagnosticOptions};
use crate::error::ErrorKind;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};

#[test]
fn public_code_configuration_is_borrowed_and_revalidated_after_mutation() -> TestResult {
    let mut options = HandshakeDiagnosticOptions {
        public_json_codes: vec!["CODE_1".to_owned(), "CODE_1".to_owned(), "Z".repeat(64)],
    };
    let pointer = options.public_json_codes.as_ptr();
    let expected = options.public_json_codes.clone();
    options.validate()?;
    check_eq!(options.public_json_codes.as_ptr(), pointer)?;
    check_eq!(options.public_json_codes, expected)?;
    options
        .public_json_codes
        .push("sensitive-invalid-code".to_owned());
    let error = options
        .validate()
        .err()
        .ok_or("invalid public field was accepted")?;
    check_eq!(error.kind(), ErrorKind::InvalidConfig)?;
    let config = error
        .config_error()
        .ok_or("missing diagnostic ConfigError")?;
    check_eq!(config.field(), "diagnostics.public_json_codes")?;
    check!(!config.reason().contains("sensitive-invalid-code"))?;
    check!(!format!("{options:?} {error:?} {error}").contains("sensitive-invalid-code"))?;
    Ok(())
}

#[test]
fn diagnostic_headers_are_an_owned_normalized_header_map() -> TestResult {
    let options = HandshakeDiagnosticOptions {
        public_json_codes: vec!["PUBLIC_CODE".to_owned()],
    };
    options.validate()?;
    let body = br#"{"code":"PUBLIC_CODE","message":"BODY_SECRET"}"#;
    let length = body.len().to_string();
    let diagnostic = HandshakeDiagnostic::http(
        &options,
        [
            (
                "Content-Type",
                b"Application/Problem+JSON; secret=HEADER_SECRET".as_slice(),
            ),
            ("Content-Length", length.as_bytes()),
            ("Retry-After", b" 0007\t".as_slice()),
            ("Set-Cookie", b"COOKIE_SECRET".as_slice()),
        ]
        .into_iter(),
        Some(body),
    );
    let headers: &http::HeaderMap = diagnostic.headers();
    check_eq!(headers.len(), 2)?;
    check_eq!(
        headers
            .get(http::header::CONTENT_TYPE)
            .ok_or("missing safe content-type")?
            .to_str()?,
        "application/problem+json"
    )?;
    check_eq!(
        headers
            .get(http::header::RETRY_AFTER)
            .ok_or("missing safe retry-after")?
            .to_str()?,
        "7"
    )?;
    check_eq!(
        diagnostic.body_capture_state(),
        HandshakeBodyCaptureState::Captured
    )?;
    check_eq!(diagnostic.body_summary(), Some("code=PUBLIC_CODE"))?;
    let owned = diagnostic.clone();
    drop(diagnostic);
    check_eq!(owned.headers().len(), 2)?;
    let debug = format!("{owned:?} {options:?}");
    for hidden in [
        "PUBLIC_CODE",
        "BODY_SECRET",
        "HEADER_SECRET",
        "COOKIE_SECRET",
        "application/problem+json",
    ] {
        check!(!debug.contains(hidden), "Debug leaked {hidden}")?;
    }
    Ok(())
}

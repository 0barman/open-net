use super::*;
use crate::module::transport::failure::ConnectStage;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};

fn options() -> Result<HandshakeDiagnosticOptions, NetError> {
    let options = HandshakeDiagnosticOptions {
        public_json_codes: vec!["TOKEN_EXPIRED".to_owned(), "RATE_LIMITED".to_owned()],
    };
    options.validate()?;
    Ok(options)
}

fn capture(body: &[u8]) -> TestResult<HandshakeDiagnostic> {
    let length = body.len().to_string();
    Ok(HandshakeDiagnostic::http(
        &options()?,
        [
            ("content-type", b"application/json".as_slice()),
            ("content-length", length.as_bytes()),
        ]
        .into_iter(),
        Some(body),
    ))
}

#[test]
fn validates_allowlist_size_and_safe_code_alphabet() -> TestResult {
    for invalid in [
        "",
        "token",
        "WITH SPACE",
        "WITH-DASH",
        "TOKEN=SECRET",
        "界",
        "BAD\n",
    ] {
        let options = HandshakeDiagnosticOptions {
            public_json_codes: vec![invalid.to_owned()],
        };
        let error = options
            .validate()
            .err()
            .ok_or("invalid diagnostic code was accepted")?;
        check_eq!(error.kind(), crate::error::ErrorKind::InvalidConfig)?;
        check_eq!(
            error.config_error().ok_or("missing ConfigError")?.field(),
            "diagnostics.public_json_codes"
        )?;
    }
    HandshakeDiagnosticOptions {
        public_json_codes: vec!["A".repeat(64)],
    }
    .validate()?;
    check_eq!(
        HandshakeDiagnosticOptions {
            public_json_codes: vec!["A".repeat(65)]
        }
        .validate()
        .err()
        .map(|error| error.kind()),
        Some(crate::error::ErrorKind::InvalidConfig)
    )?;
    HandshakeDiagnosticOptions {
        public_json_codes: (0..32).map(|index| format!("CODE_{index}")).collect(),
    }
    .validate()?;
    check_eq!(
        HandshakeDiagnosticOptions {
            public_json_codes: (0..33).map(|index| format!("CODE_{index}")).collect()
        }
        .validate()
        .err()
        .map(|error| error.kind()),
        Some(crate::error::ErrorKind::InvalidConfig)
    )?;
    Ok(())
}

#[test]
fn default_and_empty_allowlist_omit_body() -> TestResult {
    let body = br#"{"code":"TOKEN_EXPIRED","token":"BODY_SECRET"}"#;
    for options in [
        HandshakeDiagnosticOptions::default(),
        HandshakeDiagnosticOptions {
            public_json_codes: Vec::new(),
        },
    ] {
        let diagnostic = HandshakeDiagnostic::http(
            &options,
            [("content-type", b"application/json".as_slice())].into_iter(),
            Some(body),
        );
        check_eq!(
            diagnostic.body_capture_state(),
            HandshakeBodyCaptureState::Omitted
        )?;
        check_eq!(diagnostic.body_summary(), None)?;
    }
    Ok(())
}

#[test]
fn emits_only_configured_code_and_normalized_allowlisted_headers() -> TestResult {
    let body = br#"{"code":"TOKEN_EXPIRED","message":"BODY_SECRET","token":"TOKEN_SECRET"}"#;
    let length = body.len().to_string();
    let diagnostic = HandshakeDiagnostic::http(
        &options()?,
        [
            (
                "Content-Type",
                b"Application/JSON; charset=utf-8; token=HEADER_SECRET".as_slice(),
            ),
            ("Content-Length", length.as_bytes()),
            ("Retry-After", b"00030".as_slice()),
            ("Authorization", b"Bearer AUTH_SECRET".as_slice()),
            ("Set-Cookie", b"session=COOKIE_SECRET".as_slice()),
            ("Proxy-Authenticate", b"PROXY_SECRET".as_slice()),
        ]
        .into_iter(),
        Some(body),
    );
    check_eq!(diagnostic.kind(), HandshakeDiagnosticKind::HttpRejected)?;
    check_eq!(
        diagnostic.body_capture_state(),
        HandshakeBodyCaptureState::Captured
    )?;
    check_eq!(diagnostic.body_summary(), Some("code=TOKEN_EXPIRED"))?;
    check_eq!(diagnostic.headers().len(), 2)?;
    check_eq!(
        diagnostic
            .headers()
            .get(CONTENT_TYPE)
            .ok_or("missing content-type")?
            .to_str()?,
        "application/json"
    )?;
    check_eq!(
        diagnostic
            .headers()
            .get(RETRY_AFTER)
            .ok_or("missing retry-after")?
            .to_str()?,
        "30"
    )?;
    let debug = format!("{diagnostic:?} {:?}", options()?);
    for hidden in [
        "BODY_SECRET",
        "TOKEN_SECRET",
        "HEADER_SECRET",
        "AUTH_SECRET",
        "COOKIE_SECRET",
        "PROXY_SECRET",
        "TOKEN_EXPIRED",
        "application/json",
    ] {
        check!(!debug.contains(hidden), "Debug leaked {hidden}")?;
    }
    Ok(())
}

#[test]
fn accepts_problem_json_and_escaped_json_code_without_retaining_other_fields() -> TestResult {
    let body = br#"{"\u0063ode":"TOKEN_\u0045XPIRED","nested":{"token":"SECRET"}}"#;
    let length = body.len().to_string();
    let diagnostic = HandshakeDiagnostic::http(
        &options()?,
        [
            ("content-type", b"application/problem+json".as_slice()),
            ("content-length", length.as_bytes()),
            ("content-encoding", b"identity".as_slice()),
        ]
        .into_iter(),
        Some(body),
    );
    check_eq!(
        diagnostic.body_capture_state(),
        HandshakeBodyCaptureState::Captured
    )?;
    check_eq!(diagnostic.body_summary(), Some("code=TOKEN_EXPIRED"))?;
    Ok(())
}

#[test]
fn absent_and_empty_body_are_unavailable() -> TestResult {
    for body in [None, Some(b"".as_slice())] {
        let diagnostic = HandshakeDiagnostic::http(
            &options()?,
            [("content-type", b"application/json".as_slice())].into_iter(),
            body,
        );
        check_eq!(
            diagnostic.body_capture_state(),
            HandshakeBodyCaptureState::Unavailable
        )?;
        check_eq!(diagnostic.body_summary(), None)?;
    }
    Ok(())
}

#[test]
fn missing_length_and_length_mismatch_cannot_capture_body() -> TestResult {
    let body = br#"{"code":"TOKEN_EXPIRED"}"#;
    for length in [None, Some("1"), Some("1024")] {
        let mut headers = vec![("content-type", b"application/json".as_slice())];
        if let Some(length) = length {
            headers.push(("content-length", length.as_bytes()));
        }
        let diagnostic = HandshakeDiagnostic::http(&options()?, headers.into_iter(), Some(body));
        check_eq!(
            diagnostic.body_capture_state(),
            HandshakeBodyCaptureState::Incomplete
        )?;
        check_eq!(diagnostic.body_summary(), None)?;
    }
    Ok(())
}

#[test]
fn rejects_ambiguous_or_unsupported_http_body_framing() -> TestResult {
    let body = br#"{"code":"TOKEN_EXPIRED"}"#;
    let length = body.len().to_string();
    for extra in [
        vec![("content-length", length.as_bytes())],
        vec![("transfer-encoding", b"chunked".as_slice())],
        vec![("transfer-encoding", b"identity".as_slice())],
        vec![("content-encoding", b"gzip".as_slice())],
        vec![("content-encoding", b"identity, identity".as_slice())],
        vec![
            ("content-encoding", b"identity".as_slice()),
            ("content-encoding", b"identity".as_slice()),
        ],
        vec![("content-type", b"application/json".as_slice())],
    ] {
        let headers = [
            ("content-type", b"application/json".as_slice()),
            ("content-length", length.as_bytes()),
        ]
        .into_iter()
        .chain(extra);
        let diagnostic = HandshakeDiagnostic::http(&options()?, headers, Some(body));
        check_eq!(
            diagnostic.body_capture_state(),
            HandshakeBodyCaptureState::Unsupported
        )?;
        check_eq!(diagnostic.body_summary(), None)?;
    }
    for invalid in ["+24", "24, 24", "-1", "garbage", "18446744073709551616", ""] {
        let diagnostic = HandshakeDiagnostic::http(
            &options()?,
            [
                ("content-type", b"application/json".as_slice()),
                ("content-length", invalid.as_bytes()),
            ]
            .into_iter(),
            Some(body),
        );
        check_eq!(
            diagnostic.body_capture_state(),
            HandshakeBodyCaptureState::Unsupported
        )?;
    }
    Ok(())
}

#[test]
fn rejects_unsupported_content_types_and_preserves_no_arbitrary_header_values() -> TestResult {
    let body = br#"{"code":"TOKEN_EXPIRED"}"#;
    let length = body.len().to_string();
    for content_type in [
        "text/html",
        "application/SECRET+json",
        "application/json, text/plain",
        "SECRET",
        "",
    ] {
        let diagnostic = HandshakeDiagnostic::http(
            &options()?,
            [
                ("content-type", content_type.as_bytes()),
                ("content-length", length.as_bytes()),
                (
                    "retry-after",
                    b"Wed, 21 Oct 2015 07:28:00 GMT SECRET".as_slice(),
                ),
            ]
            .into_iter(),
            Some(body),
        );
        check_eq!(
            diagnostic.body_capture_state(),
            HandshakeBodyCaptureState::Unsupported
        )?;
        check_eq!(diagnostic.body_summary(), None)?;
        check!(diagnostic.headers().is_empty())?;
    }
    Ok(())
}

#[test]
fn header_size_duplicates_and_numeric_overflow_are_omitted() -> TestResult {
    let large = format!("application/json; {}", "X".repeat(128));
    for headers in [
        vec![("content-type", large.as_bytes())],
        vec![("retry-after", b"18446744073709551616".as_slice())],
        vec![("retry-after", b"+20".as_slice())],
        vec![
            ("retry-after", b"20".as_slice()),
            ("retry-after", b"20".as_slice()),
        ],
        vec![
            ("content-type", b"application/json".as_slice()),
            ("content-type", b"application/json".as_slice()),
        ],
    ] {
        let diagnostic = HandshakeDiagnostic::http(
            &HandshakeDiagnosticOptions::default(),
            headers.into_iter(),
            None,
        );
        check!(diagnostic.headers().is_empty())?;
    }
    Ok(())
}

#[test]
fn full_input_budget_is_enforced_before_json_parsing() -> TestResult {
    let prefix = br#"{"code":"TOKEN_EXPIRED","message":""#;
    let mut body = prefix.to_vec();
    body.extend(std::iter::repeat_n(b'X', 16 * 1024 - prefix.len() - 2));
    body.extend_from_slice(b"\"}");
    let diagnostic = capture(&body)?;
    check_eq!(body.len(), 16 * 1024)?;
    check_eq!(
        diagnostic.body_capture_state(),
        HandshakeBodyCaptureState::Captured
    )?;
    body.push(b' ');
    let diagnostic = capture(&body)?;
    check_eq!(
        diagnostic.body_capture_state(),
        HandshakeBodyCaptureState::TooLarge
    )?;
    check_eq!(diagnostic.body_summary(), None)?;
    Ok(())
}

#[test]
fn declared_body_over_budget_is_not_parsed_even_when_buffer_is_short() -> TestResult {
    let diagnostic = HandshakeDiagnostic::http(
        &options()?,
        [
            ("content-type", b"application/json".as_slice()),
            ("content-length", b"16385".as_slice()),
        ]
        .into_iter(),
        Some(br#"{"code":"TOKEN_EXPIRED"}"#),
    );
    check_eq!(
        diagnostic.body_capture_state(),
        HandshakeBodyCaptureState::TooLarge
    )?;
    Ok(())
}

#[test]
fn rejects_duplicate_code_invalid_json_non_object_and_invalid_utf8() -> TestResult {
    for body in [
        br#"{"code":"TOKEN_EXPIRED","code":"RATE_LIMITED"}"#.as_slice(),
        br#"{"code":"TOKEN_EXPIRED","\u0063ode":"TOKEN_EXPIRED"}"#.as_slice(),
        br#"{"code":"TOKEN_EXPIRED"} {"token":"SECRET"}"#.as_slice(),
        br#"{"code":"TOKEN_EXPIRED""#.as_slice(),
        br#"["TOKEN_EXPIRED"]"#.as_slice(),
        b"{\"code\":\"TOKEN_EXPIRED\",\"secret\":\"\xff\"}".as_slice(),
    ] {
        let diagnostic = capture(body)?;
        check_eq!(
            diagnostic.body_capture_state(),
            HandshakeBodyCaptureState::Unsupported
        )?;
        check_eq!(diagnostic.body_summary(), None)?;
    }
    Ok(())
}

#[test]
fn absent_unknown_nested_or_non_string_codes_are_unrecognized() -> TestResult {
    for body in [
        br#"{"message":"TOKEN_EXPIRED"}"#.as_slice(),
        br#"{"code":"UNKNOWN_SECRET"}"#.as_slice(),
        br#"{"nested":{"code":"TOKEN_EXPIRED"}}"#.as_slice(),
        br#"{"code":42}"#.as_slice(),
        br#"{"code":null}"#.as_slice(),
        br#"{"code":["TOKEN_EXPIRED"]}"#.as_slice(),
        br#"{"code":{"code":"TOKEN_EXPIRED"}}"#.as_slice(),
    ] {
        let diagnostic = capture(body)?;
        check_eq!(
            diagnostic.body_capture_state(),
            HandshakeBodyCaptureState::Unrecognized
        )?;
        check_eq!(diagnostic.body_summary(), None)?;
    }
    Ok(())
}

#[test]
fn summary_uses_configured_code_budget_and_owned_details_stay_small() -> TestResult {
    let code = "X".repeat(64);
    let options = HandshakeDiagnosticOptions {
        public_json_codes: vec![code.clone()],
    };
    options.validate()?;
    let body = format!("{{\"code\":\"{code}\"}}");
    let length = body.len().to_string();
    let diagnostic = HandshakeDiagnostic::http(
        &options,
        [
            ("content-type", b"application/json".as_slice()),
            ("content-length", length.as_bytes()),
            ("retry-after", b"18446744073709551615".as_slice()),
        ]
        .into_iter(),
        Some(body.as_bytes()),
    );
    check_eq!(
        diagnostic.body_capture_state(),
        HandshakeBodyCaptureState::Captured
    )?;
    let summary_bytes = diagnostic
        .body_summary()
        .map(str::len)
        .ok_or(NetError::from(crate::error::ErrorKind::Internal))?;
    let header_bytes: usize = diagnostic
        .headers()
        .iter()
        .map(|(key, value)| key.as_str().len() + value.len())
        .sum();
    check!(summary_bytes <= 512 && header_bytes + summary_bytes <= 2048)?;
    check!(diagnostic.headers().len() <= 8)?;
    for (_, value) in diagnostic.headers() {
        check!(value.len() <= 128)?;
    }
    Ok(())
}

#[test]
fn fallback_classification_retains_only_stable_failure_categories() -> TestResult {
    use HandshakeDiagnosticKind as Kind;
    for (error, stage, status, kind) in [
        (
            NetError::from(crate::error::ErrorKind::HandshakeRejected),
            ConnectStage::WebSocketUpgrade,
            Some(401),
            Kind::HttpRejected,
        ),
        (
            NetError::from(crate::error::ErrorKind::TimedOut),
            ConnectStage::Tcp,
            None,
            Kind::Timeout,
        ),
        (
            NetError::from(crate::error::ErrorKind::Io),
            ConnectStage::Dns,
            None,
            Kind::Dns,
        ),
        (
            NetError::from(crate::error::ErrorKind::Tls),
            ConnectStage::Tls,
            None,
            Kind::TlsProtocol,
        ),
        (
            NetError::from(crate::error::ErrorKind::InvalidConfig),
            ConnectStage::Provider,
            None,
            Kind::Local,
        ),
        (
            NetError::from(crate::error::ErrorKind::InvalidInput),
            ConnectStage::RequestBuild,
            None,
            Kind::Local,
        ),
        (
            NetError::from(crate::error::ErrorKind::Protocol),
            ConnectStage::WebSocketUpgrade,
            None,
            Kind::Protocol,
        ),
        (
            NetError::from(crate::error::ErrorKind::Internal),
            ConnectStage::Tcp,
            None,
            Kind::Other,
        ),
    ] {
        let failure = ConnectionFailure::new(error, stage, status, false);
        let diagnostic = HandshakeDiagnostic::from_failure(failure);
        check_eq!(diagnostic.kind(), kind)?;
        check!(diagnostic.headers().is_empty())?;
        check_eq!(diagnostic.body_summary(), None)?;
    }
    let diagnostic = HandshakeDiagnostic::new(Kind::Io(std::io::ErrorKind::ConnectionRefused));
    check_eq!(
        diagnostic.kind(),
        Kind::Io(std::io::ErrorKind::ConnectionRefused)
    )?;
    Ok(())
}

#[test]
fn only_http_optional_whitespace_is_accepted_around_header_values() -> TestResult {
    let body = br#"{"code":"TOKEN_EXPIRED"}"#;
    for invalid in ['\r', '\n', '\u{000b}', '\u{000c}'] {
        let length = format!("{invalid}{}", body.len());
        let diagnostic = HandshakeDiagnostic::http(
            &options()?,
            [
                ("content-type", b"application/json".as_slice()),
                ("content-length", length.as_bytes()),
            ]
            .into_iter(),
            Some(body),
        );
        check_eq!(
            diagnostic.body_capture_state(),
            HandshakeBodyCaptureState::Unsupported
        )?;

        let retry = format!("7{invalid}");
        let content_type = format!("{invalid}application/json");
        let diagnostic = HandshakeDiagnostic::http(
            &HandshakeDiagnosticOptions::default(),
            [
                ("content-type", content_type.as_bytes()),
                ("retry-after", retry.as_bytes()),
            ]
            .into_iter(),
            None,
        );
        check!(diagnostic.headers().is_empty())?;
    }
    let length = format!(" \t{}\t ", body.len());
    let diagnostic = HandshakeDiagnostic::http(
        &options()?,
        [
            ("content-type", b" \tapplication/json\t ".as_slice()),
            ("content-length", length.as_bytes()),
            ("retry-after", b" \t007\t ".as_slice()),
        ]
        .into_iter(),
        Some(body),
    );
    check_eq!(
        diagnostic.body_capture_state(),
        HandshakeBodyCaptureState::Captured
    )?;
    check!(diagnostic
        .headers()
        .iter()
        .any(|(name, value)| name == "retry-after" && value == "7"))?;
    Ok(())
}

#[test]
fn deeply_nested_ignored_values_are_processed_with_bounded_input() -> TestResult {
    let nested = format!("{}0{}", "[".repeat(6000), "]".repeat(6000));
    let body = format!("{{\"code\":\"TOKEN_EXPIRED\",\"ignored\":{nested}}}");
    let diagnostic = capture(body.as_bytes())?;
    check_eq!(
        diagnostic.body_capture_state(),
        HandshakeBodyCaptureState::Captured
    )?;
    let body = format!("{{\"code\":{nested}}}");
    let diagnostic = capture(body.as_bytes())?;
    check_eq!(
        diagnostic.body_capture_state(),
        HandshakeBodyCaptureState::Unrecognized
    )?;
    Ok(())
}

#[test]
fn public_code_count_includes_duplicates_and_preserves_rejected_input() -> TestResult {
    let mut options = HandshakeDiagnosticOptions {
        public_json_codes: vec!["TOKEN_EXPIRED".to_owned(); 32],
    };
    options.validate()?;
    options.public_json_codes.push("TOKEN_EXPIRED".to_owned());
    let before = options.public_json_codes.clone();
    let error = options
        .validate()
        .err()
        .ok_or("33 duplicate entries were accepted")?;
    check_eq!(error.kind(), crate::error::ErrorKind::InvalidConfig)?;
    check_eq!(
        error.config_error().ok_or("missing ConfigError")?.field(),
        "diagnostics.public_json_codes"
    )?;
    check_eq!(options.public_json_codes, before)?;
    Ok(())
}

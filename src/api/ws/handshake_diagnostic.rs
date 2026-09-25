use crate::error::NetError;
use crate::module::transport::failure::{ConnectStage, ConnectionFailure};
use http::header::{HeaderName, HeaderValue, CONTENT_TYPE, RETRY_AFTER};
use http::HeaderMap;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::fmt;

const MAX_ALLOWLIST_CODES: usize = 32;
const MAX_CODE_BYTES: usize = 64;
const MAX_BODY_INPUT_BYTES: usize = 16 * 1024;
const MAX_HEADER_VALUE_BYTES: usize = 128;

/// Optional handshake-diagnostic configuration. By default, only safe response
/// headers and a stable error classification are retained; the response body is
/// not retained.
///
/// A body summary contains only a code that exactly matches a public error code
/// configured by the caller; no error message or original text fragment is
/// retained. `Debug` displays only the number of configured error codes.
#[derive(Clone, Default)]
pub struct HandshakeDiagnosticOptions {
    /// Top-level JSON `code` values that may be exposed; an empty list disables body-summary capture.
    /// Up to 32 entries are accepted (duplicates count), and each entry contains 1–64 ASCII uppercase letters, digits, or underscores.
    pub public_json_codes: Vec<String>,
}

impl HandshakeDiagnosticOptions {
    /// Checks the public code list without copying or modifying its entries.
    ///
    /// At most 32 entries are accepted, including duplicates. Each must contain
    /// 1–64 ASCII uppercase letters, digits or underscores.
    ///
    /// Capture requires a complete JSON object within 16 KiB, a unique
    /// `Content-Length` matching the bytes already available, a recognized JSON
    /// content type and no transfer encoding or compression. Only the unique
    /// top-level `code` string is considered; comparison is case sensitive.
    /// This does not request more body bytes or extend the handshake deadline.
    pub fn validate(&self) -> crate::Result<()> {
        if self.public_json_codes.len() > MAX_ALLOWLIST_CODES {
            return Err(NetError::config(
                "diagnostics.public_json_codes",
                "must contain at most 32 public codes, including duplicates",
            ));
        }
        for code in &self.public_json_codes {
            if code.is_empty()
                || code.len() > MAX_CODE_BYTES
                || !code
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            {
                return Err(NetError::config(
                    "diagnostics.public_json_codes",
                    "each public code must contain 1–64 ASCII uppercase letters, digits or underscores",
                ));
            }
        }
        Ok(())
    }
}

impl fmt::Debug for HandshakeDiagnosticOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HandshakeDiagnosticOptions")
            .field("public_json_code_count", &self.public_json_codes.len())
            .finish()
    }
}

/// Stable handshake-diagnostic classification that supplements the original
/// failure without changing its retryability, stage, timeout, or HTTP status.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandshakeDiagnosticKind {
    /// The HTTP response rejected a WebSocket handshake or proxy handshake.
    HttpRejected,
    /// An operating-system I/O error; only its category is retained, not the
    /// original text, which may contain sensitive information.
    Io(
        /// The I/O error category returned by the operating system.
        std::io::ErrorKind,
    ),
    /// TLS certificate verification failed.
    TlsCertificate,
    /// TLS negotiation or protocol failed except certificate verification.
    TlsProtocol,
    /// Invalid HTTP or WebSocket protocol data.
    Protocol,
    /// The original deadline for the current handshake attempt has been reached.
    Timeout,
    /// Domain name resolution failed and there is no more precise classification that can be safely preserved.
    Dns,
    /// Request construction, handshake provider, configuration, or local event delivery failed.
    Local,
    /// A more precise and safe classification of the error cannot be given.
    Other,
}

/// Result of safe response-body summary capture, or the specific reason no summary was collected.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandshakeBodyCaptureState {
    /// The configuration requires that the body be omitted, for example, the allowed list of public error codes is empty.
    Omitted,
    /// No body data is available, or allocating memory for the safe summary failed.
    Unavailable,
    /// Unable to verify body length and integrity based on currently available bytes.
    Incomplete,
    /// HTTP body delimitation, content type, or JSON representation is not supported or is ambiguous.
    Unsupported,
    /// The available body or declared length exceeds the 16 KiB input limit.
    TooLarge,
    /// The JSON is valid, but a unique top-level string error code that matches the allowed list was not found.
    Unrecognized,
    /// A summary was collected containing only a code allowed by configuration.
    Captured,
}

/// Size-bounded diagnostic information attached to one failed handshake attempt.
///
/// Only normalized `content-type` and numeric `retry-after` values are retained.
/// The body summary is generated from configured public codes and never copies
/// arbitrary response messages. Total retained string data is below 2 KiB, each
/// response header is below 128 bytes, and the summary is below 512 bytes;
/// `Debug` never prints string contents.
///
/// This value owns no timer and is not stored in a global cache. Its lifecycle is
/// determined by the holder.
#[derive(Clone)]
pub struct HandshakeDiagnostic {
    /// Safe diagnostic classification that supplements the original failure.
    kind: HandshakeDiagnosticKind,
    /// Security response headers retained and normalized from the fixed allowed list.
    headers: HeaderMap,
    /// `code=PUBLIC_CODE` when capture succeeds; `None` otherwise.
    body_summary: Option<String>,
    /// Body-summary capture result or omission reason.
    body_capture_state: HandshakeBodyCaptureState,
}

impl HandshakeDiagnostic {
    pub(crate) fn new(kind: HandshakeDiagnosticKind) -> Self {
        Self {
            kind,
            headers: HeaderMap::new(),
            body_summary: None,
            body_capture_state: HandshakeBodyCaptureState::Unavailable,
        }
    }

    pub(crate) fn http<'a>(
        options: &HandshakeDiagnosticOptions,
        headers: impl Iterator<Item = (&'a str, &'a [u8])>,
        body: Option<&[u8]>,
    ) -> Self {
        let metadata = HttpMetadata::read(headers);
        let mut diagnostic = Self::new(HandshakeDiagnosticKind::HttpRejected);
        if let Some(content_type) = metadata.json_content_type() {
            diagnostic.retain_header(CONTENT_TYPE, content_type);
        }
        if let Some(seconds) = metadata.retry_after.unique().and_then(parse_decimal) {
            // u64 has at most 20 decimal digits; write without copying any
            // untrusted input or requesting an unbounded temporary allocation.
            let mut normalized = String::new();
            if normalized.try_reserve_exact(20).is_ok() {
                use std::fmt::Write;
                if write!(&mut normalized, "{seconds}").is_ok() {
                    diagnostic.retain_header(RETRY_AFTER, &normalized);
                } else {
                    log_capture_allocation_failure();
                }
            } else {
                log_capture_allocation_failure();
            }
        }
        let (state, summary) = capture_body(options, &metadata, body);
        diagnostic.body_capture_state = state;
        diagnostic.body_summary = summary;
        diagnostic
    }

    pub(crate) fn from_failure(failure: ConnectionFailure) -> Self {
        use HandshakeDiagnosticKind as Kind;
        let kind = if failure.http_status().is_some() {
            Kind::HttpRejected
        } else if failure.error().kind() == crate::error::ErrorKind::TimedOut {
            Kind::Timeout
        } else {
            match failure.stage() {
                ConnectStage::Provider
                | ConnectStage::RequestBuild
                | ConnectStage::EventDelivery => Kind::Local,
                ConnectStage::Dns => Kind::Dns,
                ConnectStage::Tls => Kind::TlsProtocol,
                _ if failure.error().kind() == crate::error::ErrorKind::Tls => Kind::TlsProtocol,
                _ if failure.error().kind() == crate::error::ErrorKind::Protocol => Kind::Protocol,
                _ => Kind::Other,
            }
        };
        Self::new(kind)
    }

    fn retain_header(&mut self, name: HeaderName, value: &str) {
        let Some(value) = try_owned(value) else {
            return;
        };
        let Ok(value) = HeaderValue::from_maybe_shared(bytes::Bytes::from(value)) else {
            return;
        };
        if self.headers.try_insert(name, value).is_err() {
            log_capture_allocation_failure();
        }
    }

    /// A safe supplement to the existing failure classification.
    pub fn kind(&self) -> HandshakeDiagnosticKind {
        self.kind
    }
    /// Normalized fixed-allowlist values; never credentials or arbitrary headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
    /// `code=PUBLIC_CODE` when a unique configured code was captured.
    pub fn body_summary(&self) -> Option<&str> {
        self.body_summary.as_deref()
    }
    /// The result of the body capture policy, including reasons for omission.
    pub fn body_capture_state(&self) -> HandshakeBodyCaptureState {
        self.body_capture_state
    }
}

fn try_owned(value: &str) -> Option<String> {
    let mut owned = String::new();
    if owned.try_reserve_exact(value.len()).is_err() {
        log_capture_allocation_failure();
        return None;
    }
    owned.push_str(value);
    Some(owned)
}

fn log_capture_allocation_failure() {
    crate::log_e!(crate::common::log::log_def::LogType::WSC; "handshake_diagnostic", "error", "safe_capture_allocation_failed");
}

/// Borrowed value and duplicate-detection state for one response header; used
/// to reject ambiguous diagnostic input.
#[derive(Default)]
struct HeaderField<'a> {
    /// The raw response header value when first encountered; `None` if not present.
    value: Option<&'a [u8]>,
    /// Whether the response header with the same name appears again; when it is `true`, it cannot be used as a unique value.
    duplicate: bool,
}

impl<'a> HeaderField<'a> {
    fn insert(&mut self, value: &'a [u8]) {
        if self.value.is_some() {
            self.duplicate = true;
        } else {
            self.value = Some(value);
        }
    }

    fn unique(&self) -> Option<&'a [u8]> {
        if self.duplicate {
            None
        } else {
            self.value
        }
    }
}

/// Diagnostic metadata extracted from a handshake HTTP response. It borrows
/// header values and never retains an arbitrary response body.
#[derive(Default)]
struct HttpMetadata<'a> {
    /// Content type used to determine whether JSON body capture is supported.
    content_type: HeaderField<'a>,
    /// The declared body length, used to verify that the current bytes constitute the complete response body.
    content_length: HeaderField<'a>,
    /// Content encoding; only a missing value or a unique `identity` value
    /// permits body-summary capture.
    content_encoding: HeaderField<'a>,
    /// Retry-after value; only a unique value that parses as decimal seconds is
    /// retained.
    retry_after: HeaderField<'a>,
    /// Whether `Transfer-Encoding` appears; when present, body-summary capture
    /// is disabled.
    transfer_encoding: bool,
}

impl<'a> HttpMetadata<'a> {
    fn read(headers: impl Iterator<Item = (&'a str, &'a [u8])>) -> Self {
        let mut metadata = Self::default();
        for (name, value) in headers {
            if name.eq_ignore_ascii_case("content-type") {
                metadata.content_type.insert(value);
            } else if name.eq_ignore_ascii_case("content-length") {
                metadata.content_length.insert(value);
            } else if name.eq_ignore_ascii_case("content-encoding") {
                metadata.content_encoding.insert(value);
            } else if name.eq_ignore_ascii_case("retry-after") {
                metadata.retry_after.insert(value);
            } else if name.eq_ignore_ascii_case("transfer-encoding") {
                metadata.transfer_encoding = true;
            }
        }
        metadata
    }

    fn json_content_type(&self) -> Option<&'static str> {
        let value = self.content_type.unique()?;
        if value.len() > MAX_HEADER_VALUE_BYTES {
            return None;
        }
        let media_type = trim_ows(value.split(|byte| *byte == b';').next()?);
        if media_type.eq_ignore_ascii_case(b"application/json") {
            Some("application/json")
        } else if media_type.eq_ignore_ascii_case(b"application/problem+json") {
            Some("application/problem+json")
        } else {
            None
        }
    }

    fn has_supported_encoding(&self) -> bool {
        if self.transfer_encoding || self.content_encoding.duplicate {
            return false;
        }
        match self.content_encoding.value {
            None => true,
            Some(value) => {
                value.len() <= MAX_HEADER_VALUE_BYTES
                    && trim_ows(value).eq_ignore_ascii_case(b"identity")
            }
        }
    }
}

fn trim_ows(mut value: &[u8]) -> &[u8] {
    while let Some((byte, rest)) = value.split_first() {
        if matches!(byte, b' ' | b'\t') {
            value = rest;
        } else {
            break;
        }
    }
    while let Some((byte, rest)) = value.split_last() {
        if matches!(byte, b' ' | b'\t') {
            value = rest;
        } else {
            break;
        }
    }
    value
}

fn parse_decimal(value: &[u8]) -> Option<u64> {
    if value.len() > MAX_HEADER_VALUE_BYTES {
        return None;
    }
    let value = trim_ows(value);
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(value).ok()?.parse().ok()
}

fn capture_body(
    options: &HandshakeDiagnosticOptions,
    metadata: &HttpMetadata<'_>,
    body: Option<&[u8]>,
) -> (HandshakeBodyCaptureState, Option<String>) {
    use HandshakeBodyCaptureState as State;
    if options.public_json_codes.is_empty() {
        return (State::Omitted, None);
    }
    let Some(body) = body.filter(|body| !body.is_empty()) else {
        return (State::Unavailable, None);
    };
    if body.len() > MAX_BODY_INPUT_BYTES {
        return (State::TooLarge, None);
    }
    if metadata.json_content_type().is_none()
        || !metadata.has_supported_encoding()
        || metadata.content_length.duplicate
    {
        return (State::Unsupported, None);
    }
    let Some(length) = metadata.content_length.unique() else {
        return (State::Incomplete, None);
    };
    let Some(length) = parse_decimal(length) else {
        return (State::Unsupported, None);
    };
    if length > MAX_BODY_INPUT_BYTES as u64 {
        return (State::TooLarge, None);
    }
    if length != body.len() as u64 {
        return (State::Incomplete, None);
    }
    if std::str::from_utf8(body).is_err() {
        return (State::Unsupported, None);
    }
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let selected = CodeObjectSeed {
        codes: &options.public_json_codes,
    }
    .deserialize(&mut deserializer);
    let Ok(selected) = selected else {
        return (State::Unsupported, None);
    };
    if deserializer.end().is_err() {
        return (State::Unsupported, None);
    }
    let Some(code) = selected.and_then(|index| options.public_json_codes.get(index)) else {
        return (State::Unrecognized, None);
    };
    let mut summary = String::new();
    if summary
        .try_reserve_exact("code=".len() + code.len())
        .is_err()
    {
        log_capture_allocation_failure();
        return (State::Unavailable, None);
    }
    summary.push_str("code=");
    summary.push_str(code);
    (State::Captured, Some(summary))
}

/// Parses the top-level JSON object and looks for unique public error codes, without saving arbitrary keys or strings.
/// Unknown values are consumed by serde's ignored-value parser without
/// constructing objects; the total input size is bounded before parsing begins.
struct CodeObjectSeed<'a> {
    /// An allowed list of public error codes for exact matching of the top-level `code` value.
    codes: &'a [String],
}

impl<'de> DeserializeSeed<'de> for CodeObjectSeed<'_> {
    type Value = Option<usize>;

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for CodeObjectSeed<'_> {
    type Value = Option<usize>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object with at most one top-level code")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut seen_code = false;
        let mut selected = None;
        while let Some(is_code) = map.next_key_seed(CodeKeySeed)? {
            if is_code {
                if seen_code {
                    return Err(serde::de::Error::custom("duplicate diagnostic code"));
                }
                seen_code = true;
                selected = map.next_value_seed(CodeValueSeed { codes: self.codes })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(selected)
    }
}

/// Recognizes whether a JSON object key is exactly `code`, without allocating or retaining the key string.
struct CodeKeySeed;

impl<'de> DeserializeSeed<'de> for CodeKeySeed {
    type Value = bool;

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<bool, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl Visitor<'_> for CodeKeySeed {
    type Value = bool;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object key")
    }

    fn visit_str<E: serde::de::Error>(self, key: &str) -> Result<bool, E> {
        Ok(key == "code")
    }
}

/// Maps JSON string error codes to allowed-list indexes; consumed non-string
/// values are treated as mismatches.
struct CodeValueSeed<'a> {
    /// Allowed list of public error codes for exact case comparison.
    codes: &'a [String],
}

impl<'de> DeserializeSeed<'de> for CodeValueSeed<'_> {
    type Value = Option<usize>;

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for CodeValueSeed<'_> {
    type Value = Option<usize>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a public diagnostic code")
    }

    fn visit_str<E: serde::de::Error>(self, code: &str) -> Result<Self::Value, E> {
        Ok(self.codes.iter().position(|allowed| allowed == code))
    }

    fn visit_bool<E: serde::de::Error>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_i64<E: serde::de::Error>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_u64<E: serde::de::Error>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_f64<E: serde::de::Error>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<Self::Value, S::Error> {
        while sequence.next_element::<IgnoredAny>()?.is_some() {}
        Ok(None)
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(None)
    }
}

impl fmt::Debug for HandshakeDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HandshakeDiagnostic")
            .field("kind", &self.kind)
            .field("header_count", &self.headers.len())
            .field(
                "body_summary_bytes",
                &self.body_summary.as_ref().map(String::len),
            )
            .field("body_capture_state", &self.body_capture_state)
            .finish()
    }
}

#[cfg(test)]
#[path = "handshake_diagnostic_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "handshake_diagnostic_api_tests.rs"]
mod api_tests;

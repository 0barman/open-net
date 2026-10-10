//! Optional HTTP/2 policy, tuning, and response metadata.

use crate::api::error::{ErrorStage, NetError};
use http::Version;
use std::fmt;
use std::time::Duration;

const MAX_HTTP2_WINDOW: u32 = i32::MAX as u32;
const MIN_HTTP2_FRAME: u32 = 16_384;
const MAX_HTTP2_FRAME: u32 = 16_777_215;

/// Selects the HTTP protocol versions an HTTP client may use.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum HttpVersionPolicy {
    /// Negotiate h2 with HTTPS ALPN and fall back to HTTP/1.1.
    #[default]
    Auto,
    /// Use HTTP/1.1 only.
    Http1Only,
    /// Require HTTPS ALPN to negotiate h2, including every followed redirect.
    /// Cleartext redirect targets are rejected before connecting or replaying a body.
    Http2Only,
}

/// HTTP/2 transport tuning applied when the `http-client-http2` feature is enabled.
///
/// The fields are private so invalid protocol values cannot be assembled without passing
/// [`HttpClientConfig::with_http2`](crate::api::http::HttpClientConfig::with_http2).
#[derive(Clone, Default)]
pub struct Http2Config {
    adaptive_window: bool,
    initial_stream_window: Option<u32>,
    initial_connection_window: Option<u32>,
    max_frame_size: Option<u32>,
    max_header_list_size: Option<u32>,
    keep_alive: Option<Http2KeepAlive>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Http2KeepAlive {
    interval: Duration,
    timeout: Duration,
    while_idle: bool,
}

impl fmt::Debug for Http2Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Http2Config")
            .field("adaptive_window", &self.adaptive_window)
            .field("initial_stream_window", &self.initial_stream_window)
            .field("initial_connection_window", &self.initial_connection_window)
            .field("max_frame_size", &self.max_frame_size)
            .field("max_header_list_size", &self.max_header_list_size)
            .field("keep_alive", &self.keep_alive)
            .finish()
    }
}

impl Http2Config {
    /// Construct the default HTTP/2 configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable adaptive HTTP/2 flow-control windows.
    pub fn adaptive_window(mut self, enabled: bool) -> Self {
        self.adaptive_window = enabled;
        self
    }

    /// Set the initial per-stream flow-control window.
    pub fn initial_stream_window(mut self, bytes: u32) -> Self {
        self.initial_stream_window = Some(bytes);
        self
    }

    /// Set the initial connection-level flow-control window.
    pub fn initial_connection_window(mut self, bytes: u32) -> Self {
        self.initial_connection_window = Some(bytes);
        self
    }

    /// Set the maximum HTTP/2 frame payload size.
    pub fn max_frame_size(mut self, bytes: u32) -> Self {
        self.max_frame_size = Some(bytes);
        self
    }

    /// Set the maximum received HTTP/2 header-list size.
    pub fn max_header_list_size(mut self, bytes: u32) -> Self {
        self.max_header_list_size = Some(bytes);
        self
    }

    /// Configure HTTP/2 PING keep-alive behavior.
    pub fn keep_alive(mut self, interval: Duration, timeout: Duration, while_idle: bool) -> Self {
        self.keep_alive = Some(Http2KeepAlive {
            interval,
            timeout,
            while_idle,
        });
        self
    }

    pub(crate) fn validate(&self) -> Result<(), NetError> {
        if self
            .initial_stream_window
            .is_some_and(|value| value == 0 || value > MAX_HTTP2_WINDOW)
        {
            return Err(invalid_http2_config("http2.initial_stream_window"));
        }
        if self
            .initial_connection_window
            .is_some_and(|value| value == 0 || value > MAX_HTTP2_WINDOW)
        {
            return Err(invalid_http2_config("http2.initial_connection_window"));
        }
        if self
            .max_frame_size
            .is_some_and(|value| !(MIN_HTTP2_FRAME..=MAX_HTTP2_FRAME).contains(&value))
        {
            return Err(invalid_http2_config("http2.max_frame_size"));
        }
        if self.max_header_list_size.is_some_and(|value| value == 0) {
            return Err(invalid_http2_config("http2.max_header_list_size"));
        }
        if self
            .keep_alive
            .is_some_and(|value| value.interval.is_zero() || value.timeout.is_zero())
        {
            return Err(invalid_http2_config("http2.keep_alive"));
        }
        Ok(())
    }

    pub(crate) fn configure_builder(
        &self,
        mut builder: reqwest::ClientBuilder,
    ) -> reqwest::ClientBuilder {
        if let Some(value) = self.initial_stream_window {
            builder = builder.http2_initial_stream_window_size(value);
        }
        if let Some(value) = self.initial_connection_window {
            builder = builder.http2_initial_connection_window_size(value);
        }
        if self.adaptive_window {
            builder = builder.http2_adaptive_window(true);
        }
        if let Some(value) = self.max_frame_size {
            builder = builder.http2_max_frame_size(value);
        }
        if let Some(value) = self.max_header_list_size {
            builder = builder.http2_max_header_list_size(value);
        }
        if let Some(keep_alive) = self.keep_alive {
            builder = builder
                .http2_keep_alive_interval(keep_alive.interval)
                .http2_keep_alive_timeout(keep_alive.timeout)
                .http2_keep_alive_while_idle(keep_alive.while_idle);
        }
        builder
    }

    pub(crate) fn configured_stream_window(&self) -> Option<u32> {
        self.initial_stream_window
    }

    pub(crate) fn configured_connection_window(&self) -> Option<u32> {
        self.initial_connection_window
    }
}

/// Protocol version observed on a response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HttpProtocol {
    /// HTTP/1.0 response.
    Http10,
    /// HTTP/1.1 response.
    Http11,
    /// HTTP/2 response.
    Http2,
    /// A synthetic response or a version added by a future transport.
    Unknown,
}

impl HttpProtocol {
    pub(crate) fn from_http_version(version: Version) -> Self {
        match version {
            Version::HTTP_10 => Self::Http10,
            Version::HTTP_11 => Self::Http11,
            Version::HTTP_2 => Self::Http2,
            _ => Self::Unknown,
        }
    }
}

/// SDK-observable HTTP/2 configuration and response-delivery counters.
///
/// Window values are the local configured initial values. They are not a claim about the
/// peer's current window or SETTINGS state, which reqwest does not expose.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Http2FlowControlSnapshot {
    /// Initial stream window configured locally, if explicitly set.
    pub configured_stream_window: Option<u32>,
    /// Initial connection window configured locally, if explicitly set.
    pub configured_connection_window: Option<u32>,
    /// Whether the transport adaptively manages its flow-control windows.
    pub adaptive_window: bool,
    /// Number of response bytes delivered to the SDK caller so far.
    pub body_bytes_delivered: u64,
    /// Number of response chunks delivered to the SDK caller so far.
    pub body_chunks_delivered: u64,
}

/// Metadata retained for both buffered and streaming responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpResponseInfo {
    /// Protocol version observed on the actual response.
    pub protocol: HttpProtocol,
    /// Local HTTP/2 configuration and delivery counters, when the response used HTTP/2.
    pub flow_control: Option<Http2FlowControlSnapshot>,
}

impl Default for HttpResponseInfo {
    fn default() -> Self {
        Self {
            protocol: HttpProtocol::Unknown,
            flow_control: None,
        }
    }
}

impl HttpResponseInfo {
    pub(crate) fn from_response(version: Version, config: &Http2Config) -> Self {
        let protocol = HttpProtocol::from_http_version(version);
        let flow_control = (protocol == HttpProtocol::Http2).then_some(Http2FlowControlSnapshot {
            configured_stream_window: config.configured_stream_window(),
            configured_connection_window: config.configured_connection_window(),
            adaptive_window: config.adaptive_window,
            body_bytes_delivered: 0,
            body_chunks_delivered: 0,
        });
        Self {
            protocol,
            flow_control,
        }
    }
}

fn invalid_http2_config(field: &'static str) -> NetError {
    NetError::config(field, "invalid HTTP/2 configuration").with_stage(ErrorStage::Configuration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::error::{ErrorKind, ErrorStage};

    fn assert_invalid(config: Http2Config, field: &str) {
        let error = config
            .validate()
            .expect_err("invalid HTTP/2 config was accepted");
        assert_eq!(error.kind(), ErrorKind::InvalidConfig);
        assert_eq!(error.context().stage, Some(ErrorStage::Configuration));
        assert_eq!(
            error.config_error().map(|detail| detail.field()),
            Some(field)
        );
    }

    #[test]
    fn rejects_values_outside_protocol_bounds() {
        assert_invalid(
            Http2Config::new().initial_stream_window(0),
            "http2.initial_stream_window",
        );
        assert_invalid(
            Http2Config::new().initial_connection_window(MAX_HTTP2_WINDOW.saturating_add(1)),
            "http2.initial_connection_window",
        );
        assert_invalid(
            Http2Config::new().max_frame_size(MIN_HTTP2_FRAME - 1),
            "http2.max_frame_size",
        );
        assert_invalid(
            Http2Config::new().max_frame_size(MAX_HTTP2_FRAME.saturating_add(1)),
            "http2.max_frame_size",
        );
        assert_invalid(
            Http2Config::new().max_header_list_size(0),
            "http2.max_header_list_size",
        );
        assert_invalid(
            Http2Config::new().keep_alive(Duration::ZERO, Duration::from_secs(1), false),
            "http2.keep_alive",
        );
    }

    #[test]
    fn maps_http_versions_without_transport_types() {
        assert_eq!(
            HttpProtocol::from_http_version(Version::HTTP_10),
            HttpProtocol::Http10
        );
        assert_eq!(
            HttpProtocol::from_http_version(Version::HTTP_11),
            HttpProtocol::Http11
        );
        assert_eq!(
            HttpProtocol::from_http_version(Version::HTTP_2),
            HttpProtocol::Http2
        );
        assert_eq!(
            HttpProtocol::from_http_version(Version::HTTP_09),
            HttpProtocol::Unknown
        );
    }
}

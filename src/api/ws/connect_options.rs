use super::{
    EventOptions, HandshakeDiagnosticOptions, HandshakeProvider, ReceiveOptions, ReceiveOverflow,
    ReconnectPolicy, ResponseRouting,
};
use crate::{HeaderMap, Metadata, NetError, Result};
use std::time::Duration;

/// URL, handshake, reconnect, and receive settings for a WebSocket session.
#[derive(Clone)]
pub struct ConnectOptions {
    /// Target WebSocket URL with a host and `ws` or `wss` scheme.
    pub url: String,
    /// Static HTTP headers added to each handshake; provider values may override duplicates.
    pub headers: HeaderMap,
    /// Provider for dynamic handshake headers and context; `None` uses static settings only.
    pub handshake_provider: Option<HandshakeProvider>,
    /// Application metadata for local observation and correlation; never sent automatically.
    pub metadata: Metadata,
    /// Retry policy after connection failure or termination; bounded exponential backoff by default.
    pub reconnect: ReconnectPolicy,
    /// Per-attempt handshake deadline, including context preparation and network setup (10 s default).
    pub handshake_timeout: Duration,
    /// Overall deadline from session start to the first successful connection (30 s default).
    /// Retries and waits count toward it; `None` removes this additional limit.
    pub connect_timeout: Option<Duration>,
    /// Whether to create a buffered initial message inbox when the session starts.
    pub initial_messages: InitialMessages,
    /// Count and byte capacity of connection-event history for later replay.
    pub event_history: EventOptions,
    /// Strategy for recognizing and completing pending requests from business messages.
    pub routing: ResponseRouting,
    /// Handshake diagnostic collection and redaction; `None` disables optional diagnostics.
    pub diagnostics: Option<HandshakeDiagnosticOptions>,
}

impl ConnectOptions {
    /// Creates options for a URL with safe defaults.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: HeaderMap::new(),
            handshake_provider: None,
            metadata: Metadata::new(),
            reconnect: ReconnectPolicy::default(),
            handshake_timeout: Duration::from_secs(10),
            connect_timeout: Some(Duration::from_secs(30)),
            initial_messages: InitialMessages::default(),
            event_history: EventOptions::default(),
            routing: ResponseRouting::default(),
            diagnostics: None,
        }
    }
    /// Validates URL, limits, routing, and nested option values.
    pub fn validate(&self) -> Result<()> {
        let uri = self
            .url
            .parse::<http::Uri>()
            .map_err(|_| NetError::config("url", "must be a valid WebSocket URL"))?;
        if !matches!(uri.scheme_str(), Some("ws" | "wss")) || uri.host().is_none() {
            return Err(NetError::config("url", "must use ws or wss with a host"));
        }
        super::validate_headers(&self.headers)?;
        super::validate_metadata(&self.metadata)?;
        self.reconnect.validate()?;
        super::config::deadline(self.handshake_timeout, "handshake_timeout", false)?;
        if let Some(timeout) = self.connect_timeout {
            super::config::deadline(timeout, "connect_timeout", false)?;
        }
        self.event_history.validate()?;
        if let InitialMessages::Buffer(options) = &self.initial_messages {
            options.validate()?;
            if matches!(self.routing, ResponseRouting::Manual)
                && options.overflow == ReceiveOverflow::DropOldest
            {
                return Err(NetError::config(
                    "initial_messages.overflow",
                    "Manual routing requires a lossless initial inbox",
                ));
            }
        }
        if let Some(options) = &self.diagnostics {
            options.validate()?;
        }
        Ok(())
    }
}
impl From<String> for ConnectOptions {
    fn from(url: String) -> Self {
        Self::new(url)
    }
}
impl From<&str> for ConnectOptions {
    fn from(url: &str) -> Self {
        Self::new(url)
    }
}
impl std::fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("url", &"[redacted]")
            .field("header_count", &self.headers.len())
            .field("has_handshake_provider", &self.handshake_provider.is_some())
            .field("metadata_entries", &self.metadata.len())
            .field("reconnect", &self.reconnect)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("initial_messages", &self.initial_messages)
            .field("event_history", &self.event_history)
            .field("routing", &self.routing)
            .field("diagnostics", &self.diagnostics)
            .finish()
    }
}
/// Initial inbox policy between session startup and application takeover.
#[derive(Clone, Debug)]
pub enum InitialMessages {
    /// Create a bounded inbox that the application can take or convert to a callback subscription.
    Buffer(
        /// Inbox limits, overflow policy, and control-frame settings.
        ReceiveOptions,
    ),
    /// Do not create an inbox; unmatched messages without subscribers are discarded.
    DiscardUnmatched,
}
impl Default for InitialMessages {
    fn default() -> Self {
        Self::Buffer(ReceiveOptions::default())
    }
}

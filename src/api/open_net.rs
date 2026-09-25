#[cfg(feature = "http-client")]
use crate::api::http::http_client::HttpClient;
#[cfg(feature = "http-client")]
use crate::api::http::http_config::HttpClientConfig;
use crate::api::open_net_config::OpenNetConfig;
use crate::common::log::log_def::LogType;
use crate::error::NetError;
use crate::inner::net_impl::OpenNetInner;
use crate::net_status::NetStatusClient;
#[cfg(feature = "ws-client")]
use crate::ws::WebSocketClient;
#[cfg(feature = "ws-client")]
use crate::ws::WebSocketClientConfig;

/// Top-level networking engine that owns runtime workers and named client instances.
///
/// Cloneable client handles remain valid until their named client is destroyed or the engine is dropped.
pub struct OpenNet {
    #[allow(dead_code)]
    pub(crate) inner: OpenNetInner,
}

impl OpenNet {
    /// Creates an engine with an immutable default network policy for WebSocket clients.
    ///
    /// Proxy and TLS settings are validated before any work starts. This
    /// constructor currently applies only to WebSocket clients; use the
    /// per-client network-config constructor to override defaults.
    #[cfg(feature = "ws-client")]
    pub fn new_with_network_config(
        config: crate::network::NetworkConfig,
    ) -> Result<Self, NetError> {
        Self::new_with_config(OpenNetConfig::default().with_network_config(config))
    }

    /// Creates an engine with default settings and queue capacities of 128.
    pub fn new() -> Result<Self, NetError> {
        crate::log_t!(LogType::Engine; "new");
        Self::new_with_config(OpenNetConfig::default())
    }

    /// Creates an engine with the supplied runtime, queue capacities, and network policy.
    ///
    /// Validates configuration before creating channels or workers. Invalid
    /// capacities or worker counts return `InvalidConfig`; values remain fixed thereafter.
    ///
    /// ```no_run
    /// use open_net::{config::OpenNetConfig, NetError, OpenNet};
    /// # fn example() -> Result<(), NetError> {
    /// let net = OpenNet::new_with_config(
    ///     OpenNetConfig::default()
    ///         .with_async_queue_capacity(256)
    ///         .with_sync_queue_capacity(128),
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_with_config(config: OpenNetConfig) -> Result<Self, NetError> {
        crate::log_t!(LogType::Engine; "new_with_config");
        let result: Result<Self, NetError> = (|| {
            Ok(Self {
                inner: OpenNetInner::new_with_config(config)?,
            })
        })();
        result.inspect_err(|error| {
            crate::log_e!(LogType::Engine; "new_with_config", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }

    /// Creates a WebSocket client with default parameters and waits for its worker to become ready.
    ///
    /// The returned client is disconnected; call `connect` or `start_session` to establish a session.
    #[cfg(feature = "ws-client")]
    pub async fn create_ws_client(&self, thread_name: &str) -> Result<WebSocketClient, NetError> {
        crate::log_t!(LogType::WSC; "create_ws_client", "thread_name", thread_name);
        let result: Result<WebSocketClient, NetError> = async {
            self.create_ws_client_with_config(thread_name, WebSocketClientConfig::default())
                .await
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "create_ws_client", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }

    /// Creates a WebSocket client with the supplied parameters and engine network policy.
    ///
    /// Cancelling the wait does not cancel submitted creation; retrieve the
    /// client later with `get_ws_client` and release it with `destroy_ws_client`.
    #[cfg(feature = "ws-client")]
    pub async fn create_ws_client_with_config(
        &self,
        thread_name: &str,
        config: WebSocketClientConfig,
    ) -> Result<WebSocketClient, NetError> {
        crate::log_t!(LogType::WSC; "create_ws_client_with_config", "thread_name|config", thread_name, format!("{:?}", config));
        let result: Result<WebSocketClient, NetError> = async {
            let thread_name = thread_name.trim();
            if thread_name.is_empty() {
                return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
            }
            self.inner
                .create_ws_client(thread_name.to_string(), config, None)
                .await
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "create_ws_client_with_config", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }

    /// Creates a WebSocket client with an immutable per-client proxy and TLS policy.
    ///
    /// The supplied policy completely replaces engine defaults. `default()` selects
    /// direct mode and WebPKI roots; connect, retry, and reconnect use this snapshot.
    ///
    /// Waits for the worker to become ready but does not connect. Dropping the
    /// future does not cancel creation; retrieve and later destroy the client by name.
    ///
    /// ```no_run
    /// use open_net::{network::{NetworkConfig, ProxyConfig}, ws::WebSocketClientConfig, NetError, OpenNet};
    /// # async fn example() -> Result<(), NetError> {
    /// let defaults = NetworkConfig::default()
    ///     .with_proxy(ProxyConfig::http_connect("http://127.0.0.1:8080", None)?);
    /// let net = OpenNet::new_with_network_config(defaults.clone())?;
    /// let inherited = net.create_ws_client("inherited").await?;
    /// let direct = net.create_ws_client_with_network_config(
    ///     "direct", WebSocketClientConfig::default(), NetworkConfig::default(),
    /// ).await?;
    /// let other = net.create_ws_client_with_network_config(
    ///     "other", WebSocketClientConfig::default(),
    ///     defaults.with_proxy(ProxyConfig::http_connect("http://127.0.0.1:8081", None)?),
    /// ).await?;
    /// net.destroy_ws_client("inherited").await?;
    /// net.destroy_ws_client("direct").await?;
    /// net.destroy_ws_client("other").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "ws-client")]
    pub async fn create_ws_client_with_network_config(
        &self,
        thread_name: &str,
        config: WebSocketClientConfig,
        network_config: crate::network::NetworkConfig,
    ) -> Result<WebSocketClient, NetError> {
        crate::log_t!(LogType::WSC; "create_ws_client_with_network_config", "thread_name|config", thread_name, format!("{config:?}"));
        let result: Result<WebSocketClient, NetError> = async {
            let thread_name = thread_name.trim();
            if thread_name.is_empty() {
                return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
            }
            self.inner
                .create_ws_client(thread_name.to_string(), config, Some(network_config))
                .await
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "create_ws_client_with_network_config", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }

    #[cfg(feature = "ws-client")]
    /// Returns a shared handle to an existing WebSocket client.
    pub fn get_ws_client(&self, thread_name: &str) -> Result<WebSocketClient, NetError> {
        crate::log_t!(LogType::WSC; "get_ws_client", "thread_name", thread_name);
        let result: Result<WebSocketClient, NetError> = (|| {
            if thread_name.trim().is_empty() {
                return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
            }
            self.inner.get_ws_client(thread_name.trim())
        })();
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "get_ws_client", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }

    #[cfg(feature = "ws-client")]
    /// Permanently destroys a named WebSocket client after its worker exits.
    pub async fn destroy_ws_client(&self, thread_name: &str) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "destroy_ws_client", "thread_name", thread_name);
        let result: Result<(), NetError> = async {
            if thread_name.trim().is_empty() {
                return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
            }
            self.inner.destroy_ws_client(thread_name.trim()).await
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "destroy_ws_client", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }
}

impl OpenNet {
    /// Creates a network-status client; call `start().await` to begin monitoring.
    ///
    /// Names are trimmed and must be unique within an engine. This feature is always available.
    ///
    /// ```no_run
    /// use open_net::{NetError, OpenNet};
    /// # async fn example() -> Result<(), NetError> {
    /// let net = OpenNet::new()?;
    /// let client = net.create_net_status_client("network-status").await?;
    /// client.start().await?;
    /// let snapshot = client.snapshot()?;
    /// let status = snapshot.reachability;
    /// let ip_stack = snapshot.ip_stack;
    /// net.destroy_net_status_client("network-status").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_net_status_client(
        &self,
        thread_name: &str,
    ) -> Result<NetStatusClient, NetError> {
        let thread_name = thread_name.trim();
        if thread_name.is_empty() {
            return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
        }
        self.inner.create_net_status_client(thread_name)
    }

    /// Returns a shared handle to a named network-status client.
    pub fn get_net_status_client(&self, thread_name: &str) -> Result<NetStatusClient, NetError> {
        let thread_name = thread_name.trim();
        if thread_name.is_empty() {
            return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
        }
        self.inner.get_net_status_client(thread_name)
    }

    /// Permanently stops a client, waits for monitoring to exit, and releases its name.
    ///
    /// Cancelling the wait does not cancel submitted destruction; the name remains
    /// reserved until cleanup completes. Use `NetStatusClient::shutdown` to pause instead.
    pub async fn destroy_net_status_client(&self, thread_name: &str) -> Result<(), NetError> {
        let thread_name = thread_name.trim();
        if thread_name.is_empty() {
            return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
        }
        self.inner.destroy_net_status_client(thread_name).await
    }
}

impl OpenNet {
    #[cfg(feature = "http-client")]
    /// Creates an HTTP client with the supplied configuration.
    pub async fn create_http_client_with_config(
        &self,
        thread_name: &str,
        config: HttpClientConfig,
    ) -> Result<HttpClient, NetError> {
        crate::log_t!(LogType::HTTP; "create_http_client_with_config", "thread_name|config", thread_name, format!("{:?}", config));
        let result: Result<HttpClient, NetError> = async {
            let thread_name = thread_name.trim();
            if thread_name.is_empty() || thread_name.contains('\0') {
                return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
            }
            self.inner
                .create_http_client(thread_name.to_string(), config)
                .await
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::HTTP; "create_http_client_with_config", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }

    #[cfg(feature = "http-client")]
    /// Returns a shared handle to an existing HTTP client.
    pub fn get_http_client(&self, thread_name: &str) -> Result<HttpClient, NetError> {
        crate::log_t!(LogType::HTTP; "get_http_client", "thread_name", thread_name);
        let name = thread_name.trim();
        if name.is_empty() {
            return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
        }
        self.inner.get_http_client(name)
    }

    #[cfg(feature = "http-client")]
    /// Permanently destroys a named HTTP client after its worker exits.
    pub async fn destroy_http_client(&self, thread_name: &str) -> Result<(), NetError> {
        crate::log_t!(LogType::HTTP; "destroy_http_client", "thread_name", thread_name);
        let name = thread_name.trim();
        if name.is_empty() {
            return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
        }
        self.inner.destroy_http_client(name).await
    }
}

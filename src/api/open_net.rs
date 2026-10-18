#[cfg(feature = "http-client")]
use crate::api::http::http_client::HttpClient;
#[cfg(feature = "http-client")]
use crate::api::http::http_config::HttpClientConfig;
use crate::api::open_net_config::OpenNetConfig;
use crate::common::log::log_def::LogType;
use crate::error::NetError;
use crate::inner::net_impl::OpenNetInner;
#[cfg(feature = "net-status")]
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
    /// Permanently close this engine's admission and request cancellation.
    ///
    /// Idempotent and safe inside SDK callbacks. It does not wait for callback,
    /// provider, worker or runtime retirement. Use [`Self::shutdown`] from an
    /// external control flow to confirm completion. The engine cannot reopen.
    pub fn begin_shutdown(&self) {
        self.inner.begin_shutdown(false);
    }

    /// Close this engine and wait for its running resources to exit.
    ///
    /// The first poll also calls [`Self::begin_shutdown`]. Concurrent/repeated
    /// waiters share the result; cancelling a waiter never cancels cleanup.
    /// No caller Tokio runtime is required. Success confirms retirement of
    /// registered clients, their SDK callbacks/providers, network monitoring,
    /// engine callback workers and runtime, even while public handles survive.
    ///
    /// Standalone HTTP workers, other engines, process-wide logging and the
    /// process-wide retirement reaper are outside this boundary. A standalone
    /// HTTP client using this engine's network context keeps its HTTP workers;
    /// only its engine-owned observations retire. User-retained data is not freed.
    /// Application tasks on an application-owned runtime remain its owner's duty.
    ///
    /// SDK callback/worker contexts are conservatively rejected on every poll
    /// with `InvalidInput` (field `open_net.shutdown`, stage `Close`), including
    /// cross-engine waits. Closing still starts; use `begin_shutdown` then return.
    /// Indirect cross-thread waits cannot be detected. A callback/provider that
    /// never returns can prevent completion; a caller timeout only ends its wait.
    /// Errors do not certify that all resources have retired. Dropping the engine
    /// still initiates automatic cleanup, but Drop itself is not a completion receipt.
    pub async fn shutdown(&self) -> Result<(), NetError> {
        self.inner.shutdown().await
    }

    /// Creates an engine with an immutable default network policy for WebSocket clients.
    ///
    /// Proxy and TLS settings are validated before any work starts. This
    /// constructor currently applies only to WebSocket clients; use the
    /// per-client network-config constructor to override defaults.
    #[cfg(feature = "ws-client")]
    pub fn new_with_network_config(
        config: crate::network::NetworkConfig,
    ) -> Result<Self, NetError> {
        crate::log_s!(LogType::Engine; "OpenNet-new_with_network_config", "config", format!("{:?}", config));
        Self::new_with_config(OpenNetConfig::default().with_network_config(config))
    }

    /// Creates an engine with default settings and queue capacities of 128.
    ///
    /// When the `ws-client` feature is enabled, the default WebSocket TLS
    /// connector is compiled during this call. Supply a provider with
    /// [`Self::new_with_network_config`] or install the application's process
    /// provider before calling this constructor. The SDK never installs one.
    pub fn new() -> Result<Self, NetError> {
        crate::log_s!(LogType::Engine; "OpenNet-new");
        Self::new_with_config(OpenNetConfig::default())
    }

    /// Creates an engine with the supplied runtime, queue capacities, and network policy.
    ///
    /// Validates configuration before creating channels or workers. Invalid
    /// capacities, worker counts, or missing WebSocket TLS providers return
    /// `InvalidConfig`; values remain fixed thereafter.
    ///
    /// ```no_run
    /// use open_net::{config::OpenNetConfig, NetError, OpenNet};
    /// #[cfg(feature = "ws-client")]
    /// use open_net::network::{NetworkConfig, TlsConfig};
    /// #[cfg(feature = "ws-client")]
    /// use std::sync::Arc;
    /// # fn example() -> Result<(), NetError> {
    /// let config = OpenNetConfig::default()
    ///     .with_async_queue_capacity(256)
    ///     .with_sync_queue_capacity(128);
    /// #[cfg(feature = "ws-client")]
    /// let config = config.with_network_config(
    ///     NetworkConfig::default().with_tls(
    ///         TlsConfig::default().with_crypto_provider(Arc::new(
    ///             rustls_openssl::default_provider(),
    ///         )),
    ///     ),
    /// );
    /// let net = OpenNet::new_with_config(config)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_with_config(config: OpenNetConfig) -> Result<Self, NetError> {
        crate::log_s!(LogType::Engine; "OpenNet-new_with_config","config", format!("{:?}", config));
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

    /// Returns the named WebSocket client, creating it when the name is not
    /// registered yet.
    ///
    /// The supplied configuration is used only when this call creates the
    /// client. If another caller has already registered the name, this method
    /// returns a clone of that client and keeps its original immutable
    /// configuration. Concurrent callers wait for the same lifecycle
    /// transition and do not receive `ClientAlreadyExists` for the same name.
    #[cfg(feature = "ws-client")]
    pub async fn get_or_create_ws_client_with_config(
        &self,
        thread_name: &str,
        config: WebSocketClientConfig,
    ) -> Result<WebSocketClient, NetError> {
        crate::log_t!(LogType::WSC; "get_or_create_ws_client_with_config", "thread_name|config", thread_name, format!("{:?}", config));
        let result: Result<WebSocketClient, NetError> = async {
            let thread_name = thread_name.trim();
            if thread_name.is_empty() || thread_name.contains('\0') {
                return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
            }
            self.inner
                .get_or_create_ws_client(thread_name.to_string(), config)
                .await
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "get_or_create_ws_client_with_config", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
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
    /// use open_net::{
    ///     network::{NetworkConfig, ProxyConfig, TlsConfig},
    ///     tls::CryptoProvider,
    ///     ws::WebSocketClientConfig,
    ///     NetError, OpenNet,
    /// };
    /// use std::sync::Arc;
    /// # async fn example() -> Result<(), NetError> {
    /// let provider: Arc<CryptoProvider> =
    ///     Arc::new(rustls_openssl::default_provider());
    /// let defaults = NetworkConfig::default()
    ///     .with_proxy(ProxyConfig::http_connect("http://127.0.0.1:8080", None)?)
    ///     .with_tls(TlsConfig::default().with_crypto_provider(Arc::clone(&provider)));
    /// let net = OpenNet::new_with_network_config(defaults.clone())?;
    /// let inherited = net.create_ws_client("inherited").await?;
    /// let direct = net.create_ws_client_with_network_config(
    ///     "direct", WebSocketClientConfig::default(),
    ///     NetworkConfig::default().with_tls(
    ///         TlsConfig::default().with_crypto_provider(Arc::clone(&provider)),
    ///     ),
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
        crate::log_t!(LogType::WSC; "ws.create_ws_client_with_network_config",
            "thread_name|config_close_timeout|config_command_capacity|config_heartbeat_enabled|network_config_proxy_direct|network_config_proxy_authenticated|network_config_status_policy|network_config_tls_root_mode|network_config_tls_custom_root_count|network_config_tls_identity_present|network_config_tls_provider_present",
            thread_name, config.close_timeout, config.queues.commands, config.heartbeat.is_some(),
            network_config.proxy().is_direct(), network_config.proxy().is_authenticated(),
            format!("{:?}", network_config.network_status_policy()), format!("{:?}", network_config.tls().root_mode()),
            network_config.tls().custom_root_count(), network_config.tls().client_identity().is_some(),
            network_config.tls().crypto_provider().is_some());
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
            crate::log_e!(LogType::WSC; "ws.create_ws_client_with_network_config", "error_kind|stage", format!("{:?}", error.kind()), format!("{:?}", error.context().stage));
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
    ///
    /// This future can be awaited without a caller Tokio runtime, including via
    /// `futures::executor::block_on`. The SDK owns the worker join. Cancelling
    /// the wait does not cancel cleanup or release the name before worker exit.
    /// A concurrent destroy returns [`crate::error::ErrorKind::ConnectionClosing`];
    /// an absent name returns [`crate::error::ErrorKind::ClientNotFound`]. Use
    /// [`Self::destroy_ws_client_if_exists`] for repeated, idempotent cleanup.
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

    /// Permanently destroys a named WebSocket client when it exists.
    ///
    /// Missing names are treated as success, so this method can be used from
    /// repeated or best-effort cleanup paths. If creation or destruction is
    /// already in progress, it waits for that lifecycle operation and then
    /// completes the requested cleanup.
    /// Like [`Self::destroy_ws_client`], this does not require a caller Tokio
    /// runtime and accepted cleanup continues if its waiter is cancelled.
    #[cfg(feature = "ws-client")]
    pub async fn destroy_ws_client_if_exists(&self, thread_name: &str) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "destroy_ws_client_if_exists", "thread_name", thread_name);
        let result: Result<(), NetError> = async {
            let thread_name = thread_name.trim();
            if thread_name.is_empty() || thread_name.contains('\0') {
                return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
            }
            self.inner.destroy_ws_client_if_exists(thread_name).await
        }
        .await;
        result.inspect_err(|error| {
            crate::log_e!(LogType::WSC; "destroy_ws_client_if_exists", "error|error_kind", format!("{error:?}"), format!("{:?}", error.kind()));
        })
    }
}

#[cfg(feature = "net-status")]
impl OpenNet {
    /// Export this engine's cached network state for an explicitly configured
    /// standalone HTTP client. Reading the context never starts monitoring.
    /// The context remains bound to this engine and becomes Closed on engine drop.
    pub fn network_status_context(&self) -> crate::net_status::NetworkStatusContext {
        self.inner.network_status.context()
    }

    /// Creates a network-status client; call `start().await` to begin monitoring.
    ///
    /// Names are trimmed and must be unique within an engine. Requires the
    /// `net-status` feature, also enabled by `http-client` and `ws-client`.
    ///
    /// ```no_run
    /// use open_net::{NetError, OpenNet};
    /// #[cfg(feature = "ws-client")]
    /// use open_net::network::{NetworkConfig, TlsConfig};
    /// #[cfg(feature = "ws-client")]
    /// use std::sync::Arc;
    /// # async fn example() -> Result<(), NetError> {
    /// #[cfg(feature = "ws-client")]
    /// let net = OpenNet::new_with_network_config(
    ///     NetworkConfig::default().with_tls(
    ///         TlsConfig::default().with_crypto_provider(Arc::new(
    ///             rustls_openssl::default_provider(),
    ///         )),
    ///     ),
    /// )?;
    /// #[cfg(not(feature = "ws-client"))]
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
        crate::log_t!(LogType::Engine; "net_status.create_net_status_client", "thread_name", thread_name);
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
    /// reserved until cleanup completes. Use `NetStatusClient::stop` to pause instead.
    pub async fn destroy_net_status_client(&self, thread_name: &str) -> Result<(), NetError> {
        let thread_name = thread_name.trim();
        crate::log_t!(LogType::Engine; "net_status.destroy_net_status_client", "thread_name", thread_name);
        if thread_name.is_empty() {
            return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
        }
        self.inner.destroy_net_status_client(thread_name).await
    }
}

impl OpenNet {
    /// Register a lazy HTTP client with fixed configuration and its own shutdown boundary.
    ///
    /// Names share the existing HTTP registry and must be nonblank and contain
    /// no NUL. Registration validates configuration without constructing HTTP
    /// workers. Clone the returned handle to share initialization and shutdown;
    /// declaring an already reserved name returns `ClientAlreadyExists`.
    #[cfg(feature = "http-client")]
    pub fn managed_http(
        &self,
        name: &str,
        config: HttpClientConfig,
    ) -> Result<crate::api::http::ManagedHttpClient, NetError> {
        let name = name.trim();
        if name.is_empty() || name.contains('\0') {
            return Err(NetError::from(crate::error::ErrorKind::InvalidInput));
        }
        self.inner.managed_http(name.to_owned(), config)
    }

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

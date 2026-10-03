use super::{ClientId, ConnectOptions, JournalOptions, Session, WebSocketClientConfig};
use crate::module::ws_client::ws_client_inner::WSClientInner;
use crate::{network::NetworkConfig, LogType, Result};
use std::{fmt, sync::Arc};

/// Shared entry point for WebSocket clients, used to create sessions, query configuration, and close the client.
///
/// # Example
///
/// ```no_run
/// use open_net::{ws::WebSocketClient, OpenNet, Result};
///
/// # async fn example() -> Result<()> {
/// let net = OpenNet::new()?;
/// let client: WebSocketClient = net.create_ws_client("market-feed").await?;
/// println!("Client id: {}", client.id());
/// client.shutdown().await?;
/// net.destroy_ws_client("market-feed").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct WebSocketClient {
    /// Configuration, session management, and shutdown state common to all client clones.
    pub(crate) inner: Arc<WSClientInner>,
}
impl WebSocketClient {
    pub(crate) fn from_inner(inner: Arc<WSClientInner>) -> Self {
        Self { inner }
    }

    /// Returns the identity of the current WebSocket client instance.
    ///
    /// # Use cases
    ///
    /// When an application maintains multiple server connections, it can use this
    /// identifier in logs, metrics, and session indexes alongside
    /// [`Session::client_id`] and the `client_id` field in connection events.
    /// The identifier is allocated within the current process. All clones of a
    /// client share it, and creating a session or reconnecting does not change it.
    /// It is not a server-assigned user ID and must not be used as a persistent
    /// business key across process restarts.
    ///
    /// # Example
    ///
    /// ```
    /// use open_net::ws::WebSocketClient;
    ///
    /// fn log_client(client: &WebSocketClient) {
    ///     let worker_client = client.clone();
    ///     assert_eq!(client.id(), worker_client.id());
    ///     println!(
    ///         "Preparing to process business messages from client {}",
    ///         client.id()
    ///     );
    /// }
    /// ```
    pub fn id(&self) -> ClientId {
        self.inner.id()
    }

    /// Borrows the WebSocket configuration determined when the client was created.
    ///
    /// # Use cases
    ///
    /// Use this snapshot when diagnosing queue backpressure, message-size
    /// limits, heartbeat behavior, or shutdown timing. It can also be cloned as
    /// the starting point for another client configuration.
    /// The returned reference is read-only, and the configuration remains fixed
    /// for the client's lifetime, including reconnects. Changing a clone does
    /// not affect this client. The target address, handshake headers, and
    /// reconnect policy are supplied by [`ConnectOptions`] for each session.
    ///
    /// # Example
    ///
    /// ```
    /// use open_net::ws::{WebSocketClient, WebSocketClientConfig};
    ///
    /// fn config_for_another_client(client: &WebSocketClient) -> WebSocketClientConfig {
    ///     let current = client.config();
    ///     println!(
    ///         "Normal sending queue can hold up to {} messages",
    ///         current.queues.normal.max_items
    ///     );
    ///
    ///     // Pass the adjusted configuration to OpenNet::create_ws_client_with_config.
    ///     let mut next = current.clone();
    ///     next.queues.normal.max_items = 2048;
    ///     next
    /// }
    /// ```
    pub fn config(&self) -> &WebSocketClientConfig {
        self.inner.config()
    }

    /// Borrows the actual proxy, TLS, and network state policies adopted by the current client.
    ///
    /// # Use cases
    ///
    /// Use this snapshot to determine which policy a client uses when the engine
    /// has a default configuration and the client may have an override. It can
    /// be cloned and adjusted for another client. The returned value is either
    /// the inherited engine configuration or the complete per-client replacement.
    /// All sessions and reconnects use this immutable policy; it describes the
    /// configured behavior, not whether the live network is currently reachable.
    ///
    /// # Example
    ///
    /// ```
    /// use open_net::network::{NetworkConfig, ProxyConfig};
    /// use open_net::ws::WebSocketClient;
    ///
    /// fn direct_config_for_another_client(client: &WebSocketClient) -> NetworkConfig {
    ///     let current = client.network_config();
    ///     println!(
    ///         "Network status policy: {:?}",
    ///         current.network_status_policy()
    ///     );
    ///
    ///     // Keep current TLS and network state policies, only change to direct for new clients.
    ///     // Pass the return value to OpenNet::create_ws_client_with_network_config.
    ///     current.clone().with_proxy(ProxyConfig::direct())
    /// }
    /// ```
    pub fn network_config(&self) -> &NetworkConfig {
        self.inner.network_config()
    }

    /// Create a session and wait for the session to establish a usable WebSocket connection before returning.
    ///
    /// # Use cases
    ///
    /// Use this method when startup must establish a connection before the
    /// application sends subscriptions or other business messages. `options`
    /// may be a `ws://` or `wss://` address or a [`ConnectOptions`] value that
    /// sets authentication headers, timeouts, receive options, and reconnect policy.
    /// Use [`Self::start_session`] when the session must be available before the
    /// connection completes, when the attempt may be cancelled, or when a full
    /// connection journal is required.
    ///
    /// This is equivalent to `start_session(options, None)` followed by
    /// [`Session::wait_connected`], so it does not create an independent
    /// connection journal. The default policy may retry an initial failure, and
    /// `connect_timeout` bounds the total wait for the first connection; one
    /// failed handshake therefore need not fail immediately. Later
    /// disconnections continue to follow the session's reconnect policy, and a
    /// successful return does not guarantee that the connection remains usable.
    ///
    /// Hold the returned [`Session`] for as long as the session is needed:
    /// dropping it requests session cancellation even if a client or sender
    /// clone remains. Cancelling the future returned by this method also drops
    /// its internally created session and requests cancellation; cleanup may
    /// continue in the background.
    ///
    /// # Errors
    ///
    /// Errors during session creation are the same as [`Self::start_session`].
    /// If the session times out while connecting, encounters a non-retryable
    /// handshake error, exhausts its retry budget, or is shut down while this
    /// method waits, the corresponding error is returned.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{ws::ConnectOptions, OpenNet, Result};
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<()> {
    /// let net = OpenNet::new()?;
    /// let client = net.create_ws_client("market-feed").await?;
    /// let mut options = ConnectOptions::new("wss://example.com/stream");
    /// options.connect_timeout = Some(Duration::from_secs(15));
    ///
    /// let session = client.connect(options).await?;
    /// // The connection has succeeded; keep holding the session while the
    /// // subscription is active.
    /// session
    ///     .sender()
    ///     .send(r#"{"op":"subscribe","topic":"prices"}"#)
    ///     .await?;
    ///
    /// // Close the session when the work is complete; the client can create
    /// // another session afterward.
    /// session.close().await?;
    /// net.destroy_ws_client("market-feed").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(&self, options: impl Into<ConnectOptions>) -> Result<Session> {
        let options = options.into();
        crate::log_s!(LogType::WSC; "connect", "options", format!("{:?}", options));
        let session = self.start_session(options, None).await?;
        session.wait_connected().await?;
        Ok(session)
    }

    /// Submit the connection task and return after the background work task receives the session without waiting for the handshake to succeed.
    ///
    /// # Use cases
    ///
    /// Use this method to display a connecting state immediately, provide a
    /// cancel action, install state monitoring, or record the first handshake
    /// and later reconnect attempts. After it returns, use
    /// [`Session::watch_state`] to observe state, [`Session::cancel`] to request
    /// cancellation, or [`Session::wait_connected`] to await a usable connection.
    /// A successful return means only that the session was admitted; subsequent
    /// connection attempts may still fail.
    ///
    /// # Parameters and lifecycle
    ///
    /// - `options`: Target address or complete connection configuration. The
    ///   connection process follows its timeout and reconnect policies. A
    ///   message inbox is created by default; use [`Session::take_messages`]
    ///   to receive messages that arrive early.
    /// - `journal`: With `Some`, creates a bounded connection journal before the
    ///   first attempt. Retrieve it with [`Session::take_journal`] and consume
    ///   it continuously; a full journal can block later connection attempts.
    ///   With `None`, no journal is created, but state observation and the
    ///   history-limited [`Session::subscribe_events`] stream remain available.
    ///
    /// Each client (including all clones) can only have one outstanding session at a time.
    /// Each client, including all clones, can have only one outstanding session.
    /// To change the target, first call [`Session::close`] on the old session and
    /// wait for completion. If using `cancel`, also wait for [`Session::closed`].
    /// The returned session owns the lifecycle and must remain held; retaining
    /// only a sender or receiver does not prevent cancellation when the owner is dropped.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::ErrorKind::InvalidConfig`] for invalid options,
    /// [`crate::error::ErrorKind::SessionAlreadyExists`] when another session
    /// is still active, and [`crate::error::ErrorKind::Closed`] after client
    /// shutdown has been requested. Resource exhaustion, a command wait that
    /// exceeds the initial connection budget, or engine shutdown may also fail
    /// admission. Connection errors after this method returns are reported by
    /// the session's wait and observation interfaces.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{ws::{JournalOptions, WebSocketClient}, Result};
    ///
    /// async fn connect_with_journal(client: &WebSocketClient) -> Result<()> {
    ///     let mut session = client.start_session(
    ///         "wss://example.com/stream",
    ///         Some(JournalOptions::default()),
    ///     ).await?;
    ///
    ///     let journal = session.take_journal().expect("Connection log is enabled when created");
    ///     // The callback continues to consume logs; keep the subscription handle alive to avoid log accumulation while waiting for a connection.
    ///     let _journal_subscription = journal.into_callback(|_, event| {
    ///         match event {
    ///             Ok(event) => println!("Session {}: {:?}", event.session_id, event.kind),
    ///             Err(error) => eprintln!("Failed to receive connection log: {error:?}"),
    ///         }
    ///     })?;
    ///
    ///     let connection = session.wait_connected().await?;
    ///     println!("Physical connection {} is ready", connection.connection_id);
    ///     session.sender().send("hello").await?;
    ///     session.close().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn start_session(
        &self,
        options: impl Into<ConnectOptions>,
        journal: Option<JournalOptions>,
    ) -> Result<Session> {
        let options = options.into();
        crate::log_s!(LogType::WSC; "start_session", "options|journal", format!("{:?}", options), format!("{:?}", journal));
        self.inner.start_session(options, journal).await
    }

    /// Determine whether the client has received a shutdown request.
    ///
    /// # Use cases
    ///
    /// Use this check before scheduling work during application shutdown or
    /// while reusing a client handle. The state is shared by all clones and is
    /// permanent once `true`; an ordinary disconnect, automatic reconnect, or
    /// end of one session does not set it.
    ///
    /// Returning `true` does not mean that the background cleanup has been completed; call [`Self::shutdown`] when you need to wait for completion.
    /// `true` does not mean that cleanup has finished; call [`Self::shutdown`]
    /// when completion must be awaited. `false` does not mean that a usable
    /// connection exists, and another task may request shutdown immediately,
    /// so connection and send errors must still be handled after this check.
    ///
    /// # Example
    ///
    /// ```
    /// use open_net::ws::WebSocketClient;
    ///
    /// fn show_client_lifecycle(client: &WebSocketClient) {
    ///     if client.is_shutdown() {
    ///         println!("Client {} is shutting down or has been shut down, stopping scheduling new tasks", client.id());
    ///     } else {
    ///         println!("Client {} has not been closed; the connection status needs to be queried from Session", client.id());
    ///     }
    /// }
    /// ```
    pub fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown()
    }

    /// Close the client permanently and wait for the background work tasks to finish cleaning up.
    ///
    /// # Use cases
    ///
    /// Use this method when the application exits, logs an account out, or will
    /// no longer use the client. It stops connection and reconnect attempts,
    /// ends the current session, and cleans up queues, pending requests, and
    /// client-level subscriptions. All clones observe the shutdown, and no new
    /// session can be created after completion. To close only the current
    /// session while retaining the client, use [`Session::close`].
    ///
    /// This operation is idempotent and may be called concurrently; calls made
    /// after cleanup completes return success immediately.
    /// Closing does not guarantee that all queued messages have been written out, nor does it guarantee that the server has processed previously sent data.
    /// If the application needs delivery or business confirmation, wait for the
    /// relevant receipt or response before calling this method.
    /// After the shutdown request is issued, the client will not resume operation even if the wait is canceled.
    ///
    /// This method does not remove the client name registration in the engine; if you still need to release the registration and reuse the name,
    /// Calling [`crate::OpenNet::destroy_ws_client`] also closes the client.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::ErrorKind::EngineDropped`] if the background
    /// command channel exits before cleanup is reported complete.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{ws::WebSocketClient, Result};
    ///
    /// async fn stop_background_client(client: &WebSocketClient) -> Result<()> {
    ///     let worker_client = client.clone();
    ///     // Complete the business sending and response waiting that need to be reserved before calling.
    ///     client.shutdown().await?;
    ///     assert!(worker_client.is_shutdown());
    ///
    ///     // Multiple components can also be cleaned separately; here they will be returned successfully.
    ///     worker_client.shutdown().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn shutdown(&self) -> Result<()> {
        crate::log_s!(LogType::WSC; "shutdown");
        self.inner.shutdown().await
    }
}
impl fmt::Debug for WebSocketClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketClient")
            .field("id", &self.id())
            .field("shutdown", &self.is_shutdown())
            .finish()
    }
}

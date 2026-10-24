#[cfg(feature = "ws-client")]
use crate::api::network_config::NetworkConfig;
use crate::error::NetError;
use tokio::sync::Semaphore;

/// Internal runtime, queue capacities, and default network policy used when
/// constructing [`crate::OpenNet`]. Queue capacities count pending tasks, not
/// bytes or concurrency, default to 128, must be in
/// `1..=tokio::sync::Semaphore::MAX_PERMITS`, and are fixed after engine
/// creation. [`crate::OpenNet::new_with_config`] validates them before creating
/// channels or workers and reports invalid values as
/// [`crate::error::ErrorKind::InvalidConfig`].
#[cfg_attr(
    feature = "ws-client",
    doc = "
When `ws-client` is enabled, configure a provider in the nested
[`NetworkConfig`](crate::network::NetworkConfig) before constructing [`crate::OpenNet`],
unless the host application has already installed a process-wide rustls provider."
)]
#[derive(Clone, Debug)]
pub struct OpenNetConfig {
    #[cfg(feature = "quality-doctor")]
    pub(crate) quality_doctor: crate::quality::QualityDoctorConfig,
    pub(crate) runtime_worker_threads: Option<usize>,
    pub(crate) async_queue_capacity: usize,
    pub(crate) sync_queue_capacity: usize,
    #[cfg(feature = "ws-client")]
    pub(crate) network_config: NetworkConfig,
}

impl Default for OpenNetConfig {
    fn default() -> Self {
        Self {
            #[cfg(feature = "quality-doctor")]
            quality_doctor: crate::quality::QualityDoctorConfig::default(),
            runtime_worker_threads: None,
            async_queue_capacity: 128,
            sync_queue_capacity: 128,
            #[cfg(feature = "ws-client")]
            network_config: NetworkConfig::default(),
        }
    }
}

impl OpenNetConfig {
    /// Sets the passive diagnostic policy shared by this engine's managed clients.
    ///
    /// `config` supplies the mode, protocol selection, memory/retention limits,
    /// Detailed sampling rates, and finding thresholds. The default is
    /// [`crate::quality::QualityMode::Off`]. This builder stores the supplied value
    /// without validation; [`Self::validate`] and engine construction validate it
    /// and report invalid values as [`crate::error::ErrorKind::InvalidConfig`].
    /// Call `config.validate()` directly when the detailed diagnostic validation
    /// error is needed.
    ///
    /// Basic and Detailed remain dormant until an explicit
    /// [`crate::OpenNet::quality_doctor`] call, or a `quality_doctor()` call on a
    /// managed HTTP client, activates the shared collector. Configure and activate
    /// before submitting operations: requests never initialize the diagnostic
    /// store, and earlier observations are not reconstructed. Calling this builder
    /// does not start diagnostic tasks, perform network probes, or alter request
    /// deadlines and retry policies. The policy is fixed for the resulting engine.
    #[cfg(feature = "quality-doctor")]
    pub fn with_quality_doctor(mut self, config: crate::quality::QualityDoctorConfig) -> Self {
        self.quality_doctor = config;
        self
    }

    /// Sets the management runtime worker count (`1..=256`). If omitted, Tokio
    /// defaults apply, including the `TOKIO_WORKER_THREADS` environment
    /// variable. The explicit value is validated before creation and does not
    /// cap per-client I/O, callback, management, or blocking threads.
    ///
    /// ```no_run
    /// use open_net::{NetError, OpenNet, OpenNetConfig};
    ///
    /// #[cfg(feature = "ws-client")]
    /// use open_net::network::{NetworkConfig, TlsConfig};
    /// #[cfg(feature = "ws-client")]
    /// use std::sync::Arc;
    /// # fn example() -> Result<(), NetError> {
    /// let config = OpenNetConfig::default().with_runtime_worker_threads(2);
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
    pub fn with_runtime_worker_threads(mut self, threads: usize) -> Self {
        self.runtime_worker_threads = Some(threads);
        self
    }

    /// Sets the asynchronous submission queue capacity (default: 128).
    /// The value is validated during engine creation and must be within the
    /// semaphore-supported range.
    pub fn with_async_queue_capacity(mut self, capacity: usize) -> Self {
        self.async_queue_capacity = capacity;
        self
    }

    /// Sets the synchronous-call queue capacity (default: 128). Synchronous
    /// means the caller waits for a result; the underlying channel is still
    /// MPSC. The value is validated during engine creation and must be within
    /// the semaphore-supported range.
    pub fn with_sync_queue_capacity(mut self, capacity: usize) -> Self {
        self.sync_queue_capacity = capacity;
        self
    }

    /// Sets the default WebSocket network policy; clients may override it at creation.
    #[cfg(feature = "ws-client")]
    pub fn with_network_config(mut self, config: NetworkConfig) -> Self {
        self.network_config = config;
        self
    }

    /// Returns the configured management-runtime worker count, if explicitly set.
    pub fn runtime_worker_threads(&self) -> Option<usize> {
        self.runtime_worker_threads
    }

    /// Returns the bounded asynchronous submission queue capacity.
    pub fn async_queue_capacity(&self) -> usize {
        self.async_queue_capacity
    }

    /// Returns the bounded synchronous submission queue capacity.
    pub fn sync_queue_capacity(&self) -> usize {
        self.sync_queue_capacity
    }

    #[cfg(feature = "ws-client")]
    /// Returns the default WebSocket network policy used for new clients.
    pub fn network_config(&self) -> &NetworkConfig {
        &self.network_config
    }

    /// Validates runtime worker, queue, and nested network configuration limits.
    pub fn validate(&self) -> Result<(), NetError> {
        #[cfg(feature = "quality-doctor")]
        self.quality_doctor
            .validate()
            .map_err(|_| NetError::config("quality_doctor", "invalid diagnostic configuration"))?;
        if self.runtime_worker_threads.is_some_and(|threads| {
            threads == 0 || threads > crate::common::common_engine::MAX_RUNTIME_WORKER_THREADS
        }) {
            return Err(NetError::config(
                "runtime_worker_threads",
                "must be between 1 and 256",
            ));
        }
        for (field, capacity) in [
            ("async_queue_capacity", self.async_queue_capacity),
            ("sync_queue_capacity", self.sync_queue_capacity),
        ] {
            if capacity == 0 || capacity > Semaphore::MAX_PERMITS {
                return Err(NetError::config(
                    field,
                    "must be between 1 and Semaphore::MAX_PERMITS",
                ));
            }
        }
        #[cfg(feature = "ws-client")]
        self.network_config.validate()?;
        Ok(())
    }
}

#[cfg(all(test, feature = "ws-client"))]
mod validation_tests {
    use super::*;
    use crate::network::RootCertificateMode;

    #[test]
    fn engine_validation_includes_nested_network_configuration() -> Result<(), NetError> {
        let mut network = NetworkConfig::default();
        network.tls.root_mode = RootCertificateMode::Replace;
        let config = OpenNetConfig::default().with_network_config(network);
        if !matches!(config.validate(), Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), crate::error::ErrorKind::InvalidConfig))
        {
            return Err(NetError::from(crate::error::ErrorKind::Internal));
        }
        Ok(())
    }
}

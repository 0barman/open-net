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
#[derive(Clone, Debug)]
pub struct OpenNetConfig {
    pub(crate) runtime_worker_threads: Option<usize>,
    pub(crate) async_queue_capacity: usize,
    pub(crate) sync_queue_capacity: usize,
    #[cfg(feature = "ws-client")]
    pub(crate) network_config: NetworkConfig,
}

impl Default for OpenNetConfig {
    fn default() -> Self {
        Self {
            runtime_worker_threads: None,
            async_queue_capacity: 128,
            sync_queue_capacity: 128,
            #[cfg(feature = "ws-client")]
            network_config: NetworkConfig::default(),
        }
    }
}

impl OpenNetConfig {
    /// Sets the management runtime worker count (`1..=256`). If omitted, Tokio
    /// defaults apply, including the `TOKIO_WORKER_THREADS` environment
    /// variable. The explicit value is validated before creation and does not
    /// cap per-client I/O, callback, management, or blocking threads.
    ///
    /// ```no_run
    /// use open_net::{NetError, OpenNet, OpenNetConfig};
    /// # fn example() -> Result<(), NetError> {
    /// let net = OpenNet::new_with_config(
    ///     OpenNetConfig::default().with_runtime_worker_threads(2),
    /// )?;
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

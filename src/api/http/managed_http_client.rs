use super::HttpClient;
use crate::inner::net_impl::managed_http_client::ManagedHttpClientInner;
use crate::Result;
use std::sync::Arc;

/// A lazy, fixed-configuration HTTP client owned by one `OpenNet` registry.
///
/// Clones share initialization and permanent shutdown. The handle does not keep
/// the engine alive. Dropping handles alone does not remove the named client;
/// explicitly shut it down, destroy its name, or drop the engine.
#[derive(Clone)]
pub struct ManagedHttpClient {
    pub(crate) inner: Arc<ManagedHttpClientInner>,
}

impl ManagedHttpClient {
    /// Initialize once and return a handle to the shared HTTP workers.
    ///
    /// Concurrent callers share one initialization result. A failed attempt can
    /// be retried by a later call; cancelling a waiter does not cancel creation.
    /// Initialization runs on the SDK runtime, independently of the caller.
    /// Closing the returned raw client also makes future `get` calls fail with
    /// `Closed`; it never causes automatic replacement or reopening.
    pub async fn get(&self) -> Result<HttpClient> {
        self.inner.get().await
    }

    /// Permanently close admission and request cancellation without awaiting cleanup.
    ///
    /// On return, previously returned `HttpClient` clones also reject new work.
    /// This method does not wait for queue capacity or worker exit. Creation
    /// already in progress may finish, but its client is retired, never published.
    /// Repeated calls are harmless. Call this method from an HTTP request callback
    /// and let an external task await [`Self::shutdown`].
    pub fn begin_shutdown(&self) {
        self.inner.begin_shutdown();
    }

    /// Close the client and await the shared shutdown result.
    ///
    /// Cleanup continues on the SDK runtime if a waiter is cancelled or its
    /// runtime stops. Completion covers HTTP workers and request callbacks, not
    /// separately registered network callbacks or application tasks they spawn.
    /// Await network subscriptions' `close` separately when required.
    /// The registry name is released only after worker retirement is confirmed;
    /// old handles remain permanently closed if that name is reused.
    /// Never await this method from one of this client's HTTP request callbacks.
    pub async fn shutdown(&self) -> Result<()> {
        self.inner.shutdown().await
    }
}

use super::{AttemptId, ClientId, CycleId, SessionId};
use crate::error::{ErrorKind, ErrorStage};
use crate::{BoxError, HeaderMap, Metadata, NetError, Result};
use std::fmt;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{oneshot, Semaphore};

type ProviderOutput = std::result::Result<HandshakeHeaders, BoxError>;
type ProviderFuture = Pin<Box<dyn Future<Output = ProviderOutput> + Send + 'static>>;
type AsyncProvider = dyn Fn(HandshakeAttempt) -> Result<ProviderFuture> + Send + Sync;
type BlockingProvider = dyn Fn(HandshakeAttempt) -> Result<HandshakeHeaders> + Send + Sync;

/// Provider that generates dynamic request headers for each WebSocket handshake.
/// It supports asynchronous and blocking callbacks; clones share the callback,
/// and the SDK converts callback and future panics into classified errors.
#[derive(Clone)]
pub struct HandshakeProvider {
    /// Shared callbacks and how they are executed.
    inner: ProviderKind,
}

/// The internal implementation of the handshake request header provider.
#[derive(Clone)]
enum ProviderKind {
    /// Future returned by the asynchronous callback.
    Async(
        /// Shared asynchronous callback that returns a future for computing
        /// handshake request headers.
        Arc<AsyncProvider>,
    ),
    /// Execute blocking callbacks on separate threads subject to concurrency quotas.
    Blocking(
        /// A shared blocking callback that directly returns handshake request headers or errors.
        Arc<BlockingProvider>,
    ),
}

impl HandshakeProvider {
    /// Creates an asynchronous provider invoked for each handshake attempt.
    ///
    /// Callback panics and future polling failures are converted to classified errors.
    pub fn new<F, Fut>(provider: F) -> Self
    where
        F: Fn(HandshakeAttempt) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ProviderOutput> + Send + 'static,
    {
        let callback = Owned::new(provider);
        Self {
            inner: ProviderKind::Async(Arc::new(move |attempt| {
                let provider = callback.value.as_ref().ok_or_else(internal_failure)?;
                caught(catch_unwind(AssertUnwindSafe(|| provider(attempt))))
                    .map(|future| Box::pin(future) as ProviderFuture)
            })),
        }
    }

    /// Creates a provider whose callback runs on a dedicated blocking worker.
    ///
    /// The configured blocking-handshake quota limits concurrent callback execution.
    pub fn blocking<F>(provider: F) -> Self
    where
        F: Fn(HandshakeAttempt) -> ProviderOutput + Send + Sync + 'static,
    {
        let callback = Owned::new(provider);
        Self {
            inner: ProviderKind::Blocking(Arc::new(move |attempt| {
                let provider = callback.value.as_ref().ok_or_else(internal_failure)?;
                resolve_provider_outcome(catch_unwind(AssertUnwindSafe(|| provider(attempt))))
            })),
        }
    }

    pub(crate) async fn provide(
        &self,
        attempt: HandshakeAttempt,
        slots: Arc<Semaphore>,
    ) -> Result<HandshakeHeaders> {
        let identity = (attempt.client_id, attempt.session_id, attempt.attempt_id);
        let result = match &self.inner {
            ProviderKind::Async(provider) => match provider(attempt) {
                Ok(future) => GuardedFuture::new(future).await,
                Err(error) => Err(error),
            },
            ProviderKind::Blocking(provider) => {
                run_blocking_with_spawn(provider.clone(), attempt, slots, |job| {
                    std::thread::Builder::new()
                        .name("open-net-ws-context-provider".to_owned())
                        .spawn(job)
                })
                .await
            }
        };
        result.map_err(|error| {
            let mut context = error.context().clone();
            context.client_id = Some(identity.0);
            context.session_id = Some(identity.1);
            context.attempt_id = Some(identity.2);
            error.with_context(context)
        })
    }
}

impl fmt::Debug for HandshakeProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode = match self.inner {
            ProviderKind::Async(_) => "async",
            ProviderKind::Blocking(_) => "blocking",
        };
        formatter
            .debug_struct("HandshakeProvider")
            .field("mode", &mode)
            .finish_non_exhaustive()
    }
}

/// Identity and caller metadata for one handshake attempt, exposed to dynamic
/// header callbacks and connection events. `Debug` reports only the metadata
/// entry count and never its contents.
#[derive(Clone)]
pub struct HandshakeAttempt {
    /// The ID of the client instance that initiated the handshake.
    pub client_id: ClientId,
    /// The connection session ID to which this attempt belongs.
    pub session_id: SessionId,
    /// The connection or reconnection cycle identifier to which this attempt belongs.
    pub cycle_id: CycleId,
    /// Identifies this specific handshake attempt.
    pub attempt_id: AttemptId,
    /// Shared metadata inherited from connection options; the SDK does not automatically send this as a request header.
    pub metadata: Arc<Metadata>,
}

impl fmt::Debug for HandshakeAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HandshakeAttempt")
            .field("client_id", &self.client_id)
            .field("session_id", &self.session_id)
            .field("cycle_id", &self.cycle_id)
            .field("attempt_id", &self.attempt_id)
            .field("metadata_count", &self.metadata.len())
            .finish()
    }
}

/// Additional request headers and an optional credential-version marker returned
/// by the dynamic handshake provider. `Debug` reports only the header count and
/// version length, never their contents.
#[derive(Clone)]
pub struct HandshakeHeaders {
    /// Additional request headers for this handshake. They must not override
    /// SDK-managed `Host`, `Connection`, `Upgrade`, `Sec-WebSocket-Key`, or
    /// `Sec-WebSocket-Version` headers.
    pub headers: HeaderMap,
    /// Credential version used to correlate connection and failure events, up to 256 UTF-8 bytes;
    /// `None` means no version is provided, and the SDK does not automatically add this field to the request header.
    pub credential_version: Option<String>,
}

impl HandshakeHeaders {
    /// Creates handshake headers with no credential-version marker.
    pub fn new(headers: HeaderMap) -> Self {
        Self {
            headers,
            credential_version: None,
        }
    }
}

impl fmt::Debug for HandshakeHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HandshakeHeaders")
            .field("header_count", &self.headers.len())
            .field(
                "credential_version_bytes",
                &self.credential_version.as_ref().map(String::len),
            )
            .finish()
    }
}

pub(crate) fn validate_headers(headers: &HeaderMap) -> Result<()> {
    use http::header::{CONNECTION, HOST, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE};
    if [
        HOST,
        CONNECTION,
        UPGRADE,
        SEC_WEBSOCKET_KEY,
        SEC_WEBSOCKET_VERSION,
    ]
    .iter()
    .any(|name| headers.contains_key(name))
    {
        return Err(NetError::input(
            "headers",
            "must not override SDK-managed WebSocket handshake headers",
        ));
    }
    Ok(())
}

/// Wraps user values held by the SDK so they can be released without holding
/// internal locks, while preventing destructor panics from escaping. Values
/// handed to the caller are removed from this wrapper and then follow normal
/// ownership rules.
struct Owned<T> {
    /// Value not yet handed over or released; `None` means this wrapper no
    /// longer owns a value.
    value: Option<T>,
}

impl<T> Owned<T> {
    fn new(value: T) -> Self {
        Self { value: Some(value) }
    }
    fn take(&mut self) -> Result<T> {
        self.value.take().ok_or_else(internal_failure)
    }
}

impl<T> Drop for Owned<T> {
    fn drop(&mut self) {
        retire(self.value.take());
    }
}

fn retire<T>(value: T) -> bool {
    match catch_unwind(AssertUnwindSafe(|| drop(value))) {
        Ok(()) => false,
        Err(payload) => {
            crate::log_e!(crate::common::log::log_def::LogType::WSC;
                "handshake_provider", "stage|error", "provider", "user_value_cleanup_panicked");
            dispose_payload(payload);
            true
        }
    }
}

fn dispose_payload(payload: Box<dyn std::any::Any + Send>) {
    // Only a second unwind while destroying a caught panic payload is abandoned.
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        crate::log_e!(crate::common::log::log_def::LogType::WSC;
            "handshake_provider", "stage|error", "provider", "panic_payload_cleanup_panicked");
        std::mem::forget(secondary);
    }
}

fn internal_failure() -> NetError {
    NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Provider)
}

fn callback_failure() -> NetError {
    NetError::from(ErrorKind::CallbackPanicked).with_stage(ErrorStage::Provider)
}

fn caught<T>(outcome: std::thread::Result<T>) -> Result<T> {
    outcome.map_err(|payload| {
        dispose_payload(payload);
        callback_failure()
    })
}

fn resolve_provider_outcome(
    outcome: std::thread::Result<ProviderOutput>,
) -> Result<HandshakeHeaders> {
    resolve_provider_output(caught(outcome)?)
}

fn resolve_provider_output(output: ProviderOutput) -> Result<HandshakeHeaders> {
    let mut headers = Owned::new(output.map_err(NetError::provider)?);
    if let Some(version) = headers
        .value
        .as_ref()
        .and_then(|headers| headers.credential_version.as_deref())
    {
        super::value_limits::validate_credential_version(version)
            .map_err(|error| error.with_stage(ErrorStage::Provider))?;
    }
    headers.take()
}

/// Future wrapper that isolates panics while an asynchronous handshake callback
/// is polled or dropped.
struct GuardedFuture {
    /// User future protected during completion, failure, or cancellation cleanup.
    future: Owned<ProviderFuture>,
}

impl GuardedFuture {
    fn new(future: ProviderFuture) -> Self {
        Self {
            future: Owned::new(future),
        }
    }

    fn complete_poll(
        &mut self,
        outcome: std::thread::Result<Poll<ProviderOutput>>,
    ) -> Poll<Result<HandshakeHeaders>> {
        match caught(outcome) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => {
                let mut output = Owned::new(output);
                if retire(self.future.value.take()) {
                    return Poll::Ready(Err(callback_failure()));
                }
                Poll::Ready(output.take().and_then(resolve_provider_output))
            }
            Err(error) => {
                retire(self.future.value.take());
                Poll::Ready(Err(error))
            }
        }
    }
}

impl Future for GuardedFuture {
    type Output = Result<HandshakeHeaders>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(future) = self.future.value.as_mut() else {
            return Poll::Ready(Err(internal_failure()));
        };
        let outcome = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx)));
        self.complete_poll(outcome)
    }
}

async fn run_blocking_with_spawn(
    provider: Arc<BlockingProvider>,
    attempt: HandshakeAttempt,
    slots: Arc<Semaphore>,
    spawn: impl FnOnce(
        Box<dyn FnOnce() + Send + 'static>,
    ) -> std::io::Result<std::thread::JoinHandle<()>>,
) -> Result<HandshakeHeaders> {
    let permit = slots.try_acquire_owned().map_err(|error| {
        NetError::with_source(ErrorKind::ResourceExhausted, error).with_stage(ErrorStage::Provider)
    })?;
    let (sent, received) = oneshot::channel();
    let job = Box::new(move || {
        let _permit = permit;
        let result = provider(attempt);
        // The real closure retains its quota through provider-capture and rejected-result cleanup.
        retire(provider);
        if let Err(result) = sent.send(Owned::new(result)) {
            drop(result);
        }
    });
    spawn(job).map(drop).map_err(|error| {
        NetError::with_source(ErrorKind::RuntimeUnavailable, error).with_stage(ErrorStage::Provider)
    })?;
    let mut output = received.await.map_err(|error| {
        NetError::with_source(ErrorKind::RuntimeUnavailable, error).with_stage(ErrorStage::Provider)
    })?;
    output.take()?
}

#[cfg(test)]
#[path = "handshake_tests.rs"]
mod tests;

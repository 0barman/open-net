use super::{
    dispatch, Request, RequestHandle, RequestOptions, RequestSnapshot, Response, SessionId,
};
use crate::error::ErrorKind;
use crate::module::ws_client::session_runtime::SessionRuntime;
use crate::{EnqueueError, NetError, Result};
use std::fmt;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot;

/// Request entry point bound to one session. It constructs requests and
/// exposes snapshots of requests awaiting responses.
#[derive(Clone)]
pub struct RequestClient {
    /// Shared session runtime and pending-request table.
    pub(crate) runtime: Arc<SessionRuntime>,
}
impl RequestClient {
    /// Returns the logical session identifier for this request entry point.
    pub fn session_id(&self) -> SessionId {
        self.runtime.id
    }
    /// Creates a builder for a request owned by this session.
    pub fn request(&self, request: Request) -> RequestBuilder {
        RequestBuilder {
            client: self.clone(),
            request,
            options: RequestOptions::default(),
            admission_started: None,
        }
    }
    /// Enqueues a request and waits for its final response.
    ///
    /// The returned error covers admission, local writing, cancellation, and
    /// response timeout; a successful response means the response was routed to
    /// this request, not that any separate application side effect is committed.
    pub async fn execute(&self, request: Request) -> Result<Response> {
        self.request(request).send().await
    }
    /// Returns snapshots of requests currently waiting for responses.
    pub fn pending_snapshot(&self) -> Result<Vec<RequestSnapshot>> {
        self.runtime.pending.snapshot()
    }
}
impl fmt::Debug for RequestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestClient")
            .field("session_id", &self.session_id())
            .finish()
    }
}
/// Holds request content and execution options. If registration or enqueueing
/// fails, the builder is returned with the error for inspection or retry.
#[must_use]
#[derive(Debug)]
pub struct RequestBuilder {
    /// Session and pending-response table used by this request.
    client: RequestClient,
    /// The request ID, message, and metadata to be sent.
    request: Request,
    /// The sending, registration and response waiting strategies for this request.
    options: RequestOptions,
    /// Time of the first registration or enqueue attempt; `None` before an
    /// attempt or after options have been reset.
    admission_started: Option<Instant>,
}
impl RequestBuilder {
    /// Replaces all request options and resets the admission-time origin.
    pub fn options(mut self, options: RequestOptions) -> Self {
        self.options = options;
        self.admission_started = None;
        self
    }
    /// Admits the request and waits for its final response.
    pub async fn send(self) -> Result<Response> {
        self.enqueue()
            .await
            .map_err(EnqueueError::into_error)?
            .response()
            .await
    }
    /// Waits for admission, commits the request, and returns a response receipt.
    ///
    /// On failure, the error retains this builder so the request can be inspected
    /// or retried without reconstructing its message.
    pub async fn enqueue(mut self) -> std::result::Result<RequestReceipt, EnqueueError<Self>> {
        match self.admit().await.and_then(PreparedRequest::commit) {
            Ok(receipt) => Ok(receipt),
            Err(error) => Err(EnqueueError::new(self, error)),
        }
    }
    /// Attempts admission without waiting for queue capacity.
    pub fn try_enqueue(mut self) -> std::result::Result<RequestReceipt, EnqueueError<Self>> {
        match self.try_admit().and_then(PreparedRequest::commit) {
            Ok(receipt) => Ok(receipt),
            Err(error) => Err(EnqueueError::new(self, error)),
        }
    }
    /// Waits for admission and returns a reserved request that is not committed.
    ///
    /// Dropping the prepared value cancels the reservation and releases its
    /// capacity; call [`PreparedRequest::commit`] to make it eligible for send.
    pub async fn prepare(mut self) -> std::result::Result<PreparedRequest, EnqueueError<Self>> {
        match self.admit().await {
            Ok(prepared) => Ok(prepared),
            Err(error) => Err(EnqueueError::new(self, error)),
        }
    }
    /// Attempts to reserve request resources without waiting for capacity.
    pub fn try_prepare(mut self) -> std::result::Result<PreparedRequest, EnqueueError<Self>> {
        match self.try_admit() {
            Ok(prepared) => Ok(prepared),
            Err(error) => Err(EnqueueError::new(self, error)),
        }
    }
    async fn admit(&mut self) -> Result<PreparedRequest> {
        let started = *self.admission_started.get_or_insert_with(Instant::now);
        let admission = dispatch::prepare(
            &self.client.runtime,
            self.request.message(),
            &self.options.send,
            started,
            Some((&self.request, &self.options)),
            true,
        )
        .await?;
        PreparedRequest::new(admission)
    }
    fn try_admit(&mut self) -> Result<PreparedRequest> {
        let started = *self.admission_started.get_or_insert_with(Instant::now);
        let admission = dispatch::try_prepare(
            &self.client.runtime,
            self.request.message(),
            &self.options.send,
            started,
            Some((&self.request, &self.options)),
        )?;
        PreparedRequest::new(admission)
    }
}
/// A receipt for a submitted request, holding a control handle and a response receiver that can only be consumed once.
#[must_use]
#[derive(Debug)]
pub struct RequestReceipt {
    /// Independently cloneable request observation and termination handles.
    handle: RequestHandle,
    /// A one-time channel to receive the final response or the reason why the request failed.
    response: oneshot::Receiver<Result<Response>>,
}
impl RequestReceipt {
    /// Borrows the handle used to observe or terminate this request.
    pub fn handle(&self) -> &RequestHandle {
        &self.handle
    }
    /// Waits for the response or the request's terminal failure.
    ///
    /// Dropping this future stops waiting but does not cancel the request.
    pub async fn response(self) -> Result<Response> {
        self.response
            .await
            .map_err(|_| NetError::from(ErrorKind::EngineDropped))?
    }
}
/// Request that has registered and reserved send resources but has not yet been
/// submitted. Dropping it cancels the reservation and operation.
#[must_use]
pub struct PreparedRequest {
    /// Send-queue reservation and cancellation cleanup for an unsubmitted request.
    admission: dispatch::Admission,
    /// Observation and termination handle registered for this request.
    handle: RequestHandle,
    /// Response receiver handed to the submission receipt; `None` after commit
    /// takes ownership of it.
    response: Option<oneshot::Receiver<Result<Response>>>,
}
impl PreparedRequest {
    fn new(mut admission: dispatch::Admission) -> Result<Self> {
        let (handle, response) = admission
            .request
            .take()
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
        Ok(Self {
            admission,
            handle,
            response: Some(response),
        })
    }
    /// Borrows the request handle before commit.
    pub fn handle(&self) -> &RequestHandle {
        &self.handle
    }
    /// Commits the reservation and returns a receipt for the same operation.
    ///
    /// This is a local synchronous transition: it does not wait for a network
    /// write or a peer response. Cancellation, close, or an expired deadline can
    /// still make the commit fail.
    pub fn commit(mut self) -> Result<RequestReceipt> {
        let response = self
            .response
            .take()
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
        self.admission.commit()?;
        Ok(RequestReceipt {
            handle: self.handle.clone(),
            response,
        })
    }
}
impl fmt::Debug for PreparedRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedRequest")
            .field("handle", &self.handle)
            .finish()
    }
}

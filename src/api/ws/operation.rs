use crate::Result;

/// Point-in-time snapshot of a send or request operation.
///
/// Processing phase and delivery evidence describe different facts. An operation with an
/// uncertain write may be requeued, and a final failure does not prove that the peer saw no data.
///
/// SDK snapshots carry a result only in [`OperationPhase::Finished`]. Reading a snapshot does not
/// win termination races; completion, cancellation, and expiry still compete for one terminal state.
#[derive(Clone, Debug)]
pub struct OperationSnapshot {
    /// Current processing phase, independent of peer receipt.
    pub phase: OperationPhase,
    /// Strongest confirmed delivery evidence across all write attempts.
    pub delivery: DeliveryEvidence,
    /// Final success or error; `None` for an unfinished SDK snapshot.
    pub result: Option<Result<TaskSuccess>>,
}

/// Current operation phase; does not itself prove peer delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationPhase {
    /// Waiting for bounded send capacity or event capacity.
    WaitingForCapacity,
    /// Capacity is reserved; submission is required before writing can begin.
    Prepared,
    /// Submitted and queued for writing, including requeued retries.
    Queued,
    /// Writer is processing; this phase alone proves no bytes were sent.
    Writing,
    /// A tracked request is waiting for its final protocol response.
    AwaitingResponse,
    /// A success or failure has become the operation's sole terminal result.
    Finished,
}

/// Strongest delivery evidence confirmed across write attempts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryEvidence {
    /// No attempt has started writing business data to the transport.
    NotStarted,
    /// Data may have reached the peer, but delivery is unconfirmed.
    ///
    /// Retries do not reset this to `NotStarted`; retrying a non-idempotent operation may be unsafe.
    Unknown,
    /// Local WebSocket write completed, without peer-response confirmation.
    Written,
    /// A matching final response confirms delivery, possibly before local flush completion.
    ResponseConfirmed,
}

/// Successful terminal result category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskSuccess {
    /// An untracked message completed local WebSocket writing.
    Written,
    /// A tracked request received its matching final protocol response.
    ///
    /// Correlation succeeded; this does not mean the peer accepted the business request.
    ResponseReceived,
}

/// Result of racing cancellation or expiry against operation completion.
///
/// Describes the effective termination decision rather than an earlier snapshot. Transport and
/// callback tasks may still be shutting down when the terminating call returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminationOutcome {
    /// Termination won before any business data could be written.
    TerminatedBeforeWrite,
    /// Writing was confirmed and termination won while awaiting a response.
    TerminatedAfterWrite,
    /// Termination won, but operation data may have reached the peer.
    DeliveryUnknown,
    /// Another terminal result already won; this call changes nothing.
    AlreadyFinished,
}

/// Delivery evidence that ends a request's write wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteOutcome {
    /// Local WebSocket writing completed.
    Written,
    /// A matching final response confirmed delivery before local writing completed.
    ///
    /// A later flush failure cannot overturn the successful response, but the failed physical
    /// connection must still terminate.
    ResponseConfirmed,
}

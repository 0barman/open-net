#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingRequestStatus {
    Queued,
    Writing,
    AwaitingResponse,
}

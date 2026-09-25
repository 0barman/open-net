use http::StatusCode;

/// Defines which HTTP statuses a caller considers successful.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpStatusPolicy {
    /// Accept every HTTP status.
    Any,
    /// Accept every status in the 2xx range.
    Success2xx,
    /// Accept only one exact status.
    Exact(StatusCode),
}

impl HttpStatusPolicy {
    /// Return whether `status` satisfies this policy.
    ///
    /// This method performs only status classification; it does not inspect a
    /// response body or convert a rejected status into an error.
    pub fn accepts(self, status: StatusCode) -> bool {
        match self {
            Self::Any => true,
            Self::Success2xx => status.is_success(),
            Self::Exact(expected) => status == expected,
        }
    }
}

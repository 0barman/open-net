//! Admission behavior for HTTP APIs that return response futures.

/// Select how a response-returning HTTP Future handles exhausted client capacity.
///
/// [`crate::api::http::HttpClientConfig`] defaults to [`Self::Wait`], and an
/// individual [`crate::api::http::HttpRequest`] may override that choice. The
/// policy does not change callback submission: `send` always attempts immediate
/// admission and `send_wait` always waits for it.
///
/// Wait uses the resolved Admission and Total budgets. Admission defaults to
/// 30 seconds independently of the network attempt timeout. Explicitly disabling
/// both budgets permits an unbounded wait until capacity, cancellation, or shutdown.
/// Waiting Futures retain their request bodies, so applications should still bound
/// the number of concurrent callers. A nested Wait request on the same client can
/// wait for capacity held by its own outer middleware or callback; cloning the
/// client shares that capacity. Use an independent client or handle FailFast for
/// such nested work.
///
/// ```
/// use open_net::api::error::NetError;
/// use open_net::api::http::{HttpAdmissionPolicy, HttpClientConfig, HttpRequest};
///
/// fn configure() -> Result<(), NetError> {
///     let config = HttpClientConfig::new("https://example.com")?
///         .with_admission_policy(HttpAdmissionPolicy::FailFast);
///     let request = HttpRequest::builder()
///         .path("/status")
///         .admission_policy(HttpAdmissionPolicy::Wait)
///         .build()?;
///     let _ = (config, request);
///     Ok(())
/// }
/// # configure()?;
/// # Ok::<(), NetError>(())
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HttpAdmissionPolicy {
    /// Wait asynchronously for all admission resources within the resolved budgets.
    #[default]
    Wait,
    /// Reject immediately if any required admission resource is unavailable.
    FailFast,
}

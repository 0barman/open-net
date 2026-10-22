//! HTTP redirect authorization and transport security, independent of HTTP version.

use crate::api::error::{ErrorStage, NetError};
use std::fmt;

const DEFAULT_MAX_REDIRECTS: usize = 10;
const MAX_REDIRECTS: usize = 128;
const MAX_ALLOWED_ORIGINS: usize = 128;
const MAX_ORIGIN_BYTES: usize = 2048;

/// Transport requirements for initial requests and followed redirects.
///
/// These requirements apply independently of negotiated HTTP versions, installed
/// authentication providers, and Cookie stores. HTTP/2-only clients additionally
/// require HTTPS even when [`Self::PreventDowngrade`] is selected.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum HttpTransportSecurity {
    /// Permit an explicitly configured HTTP base URL, but reject every
    /// HTTPS-to-HTTP redirect before connecting to its target.
    ///
    /// An HTTP-to-HTTPS upgrade still needs authorization from [`RedirectPolicy`],
    /// because changing the scheme changes the origin.
    #[default]
    PreventDowngrade,
    /// Require HTTPS for the base URL and every followed redirect.
    HttpsOnly,
}

/// Controls which redirects the SDK may follow.
///
/// The default follows at most 10 redirects within the **original request's**
/// origin. An origin consists of the scheme, normalized host, and effective
/// port. An allowed target never acquires authority to redirect to other origins.
///
/// Cross-origin redirects require [`Self::allow_origins`]. That authorization
/// includes any method and replayable body preserved by the redirect status;
/// use it only for destinations trusted to receive request content. Credentials
/// are still stripped on cross-origin hops, and transport security cannot be
/// relaxed by an origin allowlist.
///
/// With [`Self::none`], a redirect response is returned without following it.
/// Otherwise a forbidden target or exhausted hop limit produces a
/// [`HttpRedirectError`] as the source of [`NetError`]. A response without
/// `Location`, or an otherwise permitted redirect that requires replaying a
/// non-replayable body, is returned without following it.
///
/// ```
/// use open_net::api::http::{HttpClientConfig, HttpTransportSecurity, RedirectPolicy};
///
/// let policy = RedirectPolicy::allow_origins(["https://downloads.example.com"])?
///     .with_max_redirects(5)?;
/// let config = HttpClientConfig::new("https://api.example.com")?
///     .with_transport_security(HttpTransportSecurity::HttpsOnly)?
///     .with_redirect_policy(policy);
/// assert_eq!(config.redirect_policy().max_redirects(), 5);
/// # Ok::<(), open_net::api::error::NetError>(())
/// ```
#[derive(Clone)]
pub struct RedirectPolicy {
    enabled: bool,
    allowed_origins: Vec<reqwest::Url>,
    max_redirects: usize,
}

impl Default for RedirectPolicy {
    fn default() -> Self {
        Self::same_origin()
    }
}

impl RedirectPolicy {
    /// Return redirect responses without following their `Location` headers.
    pub fn none() -> Self {
        Self {
            enabled: false,
            allowed_origins: Vec::new(),
            max_redirects: DEFAULT_MAX_REDIRECTS,
        }
    }

    /// Follow at most 10 redirects within the original request's origin.
    pub fn same_origin() -> Self {
        Self {
            enabled: true,
            allowed_origins: Vec::new(),
            max_redirects: DEFAULT_MAX_REDIRECTS,
        }
    }

    /// Permit the original request's origin and the explicitly listed origins.
    ///
    /// Each value must be an absolute HTTP or HTTPS origin, with an optional
    /// trailing `/`. Credentials, other paths, queries, and fragments are
    /// rejected. Hosts and default ports are normalized before matching; no
    /// wildcard, suffix, or subdomain matching is performed.
    ///
    /// At most 128 distinct origins are retained, each input is limited to 2048
    /// bytes, and duplicates are ignored. An empty list behaves like
    /// [`Self::same_origin`]. The default maximum is 10 followed redirects.
    pub fn allow_origins<I, S>(origins: I) -> Result<Self, NetError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut policy = Self::same_origin();
        for value in origins {
            let parsed = parse_origin(value.as_ref())?;
            if policy
                .allowed_origins
                .iter()
                .any(|allowed| allowed.origin() == parsed.origin())
            {
                continue;
            }
            if policy.allowed_origins.len() >= MAX_ALLOWED_ORIGINS {
                return Err(invalid_policy("http.redirect.allowed_origins"));
            }
            policy.allowed_origins.push(parsed);
        }
        Ok(policy)
    }

    /// Set the maximum number of followed redirects within `1..=128`.
    ///
    /// This does not enable a disabled policy. Use [`Self::none`] to follow zero
    /// redirects; reaching this limit on an enabled policy returns
    /// [`HttpRedirectError::TooManyRedirects`].
    pub fn with_max_redirects(mut self, max_redirects: usize) -> Result<Self, NetError> {
        if !(1..=MAX_REDIRECTS).contains(&max_redirects) {
            return Err(invalid_policy("http.redirect.max_redirects"));
        }
        self.max_redirects = max_redirects;
        Ok(self)
    }

    /// Whether redirect following is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Maximum number of redirects followed when this policy is enabled.
    pub fn max_redirects(&self) -> usize {
        self.max_redirects
    }

    /// Authorize a candidate before preparing or connecting its next request.
    ///
    /// The caller skips this method entirely when redirect following is disabled
    /// and promotes `security` to `HttpsOnly` for HTTP/2-only clients.
    pub(crate) fn check_redirect(
        &self,
        original_url: &reqwest::Url,
        current_url: &reqwest::Url,
        next_url: &reqwest::Url,
        followed: usize,
        security: HttpTransportSecurity,
    ) -> Result<(), HttpRedirectError> {
        if !matches!(next_url.scheme(), "http" | "https")
            || next_url.host_str().is_none()
            || !next_url.username().is_empty()
            || next_url.password().is_some()
        {
            return Err(HttpRedirectError::InvalidLocation);
        }
        if next_url.scheme() != "https"
            && (security == HttpTransportSecurity::HttpsOnly || current_url.scheme() == "https")
        {
            return Err(HttpRedirectError::InsecureTarget);
        }
        if next_url.origin() != original_url.origin()
            && !self
                .allowed_origins
                .iter()
                .any(|allowed| allowed.origin() == next_url.origin())
        {
            return Err(HttpRedirectError::OriginNotAllowed);
        }
        if followed >= self.max_redirects {
            return Err(HttpRedirectError::TooManyRedirects);
        }
        Ok(())
    }
}

impl fmt::Debug for RedirectPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedirectPolicy")
            .field("enabled", &self.enabled)
            .field("allowed_origin_count", &self.allowed_origins.len())
            .field("max_redirects", &self.max_redirects)
            .finish()
    }
}

/// Reason a redirect was rejected before sending the next request.
///
/// SDK redirect failures use [`crate::api::error::ErrorKind::Protocol`] at
/// [`ErrorStage::Receive`] and retain this value in [`std::error::Error::source`].
/// Neither formatting nor the retained reason contains the target URL. These
/// policy failures do not trigger automatic retries.
///
/// ```
/// use open_net::api::error::NetError;
/// use open_net::api::http::HttpRedirectError;
/// use std::error::Error;
///
/// fn redirect_reason(error: &NetError) -> Option<&HttpRedirectError> {
///     error.source()?.downcast_ref::<HttpRedirectError>()
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HttpRedirectError {
    /// The next URL violates the client's transport security requirements.
    InsecureTarget,
    /// The target is neither the original origin nor an explicitly allowed one.
    OriginNotAllowed,
    /// The maximum number of followed redirects has already been reached.
    TooManyRedirects,
    /// The `Location` cannot be decoded or parsed, has credentials, or uses an
    /// unsupported scheme.
    InvalidLocation,
}

impl fmt::Display for HttpRedirectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InsecureTarget => "HTTP redirect violates transport security",
            Self::OriginNotAllowed => "HTTP redirect origin is not allowed",
            Self::TooManyRedirects => "HTTP redirect limit exceeded",
            Self::InvalidLocation => "invalid HTTP redirect location",
        })
    }
}

impl std::error::Error for HttpRedirectError {}

fn parse_origin(value: &str) -> Result<reqwest::Url, NetError> {
    let invalid = || invalid_policy("http.redirect.allowed_origins");
    if value.len() > MAX_ORIGIN_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
        || value.contains(['\\', '@', '?', '#'])
    {
        return Err(invalid());
    }
    // Check the input path before URL normalization can collapse `/path/..`.
    let (_, authority_and_path) = value.split_once("://").ok_or_else(invalid)?;
    if authority_and_path
        .find('/')
        .is_some_and(|index| &authority_and_path[index..] != "/")
    {
        return Err(invalid());
    }
    let parsed = reqwest::Url::parse(value).map_err(|_| invalid())?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(invalid());
    }
    Ok(parsed)
}

fn invalid_policy(field: &'static str) -> NetError {
    NetError::config(field, "invalid HTTP redirect policy").with_stage(ErrorStage::Configuration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::error::ErrorKind;
    use std::error::Error;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    #[test]
    fn normalized_origins_match_exactly_without_disclosing_hosts() -> TestResult {
        let policy = RedirectPolicy::allow_origins([
            "https://PRIVATE.example:443/",
            "https://private.example",
            "https://[::1]:8443",
        ])?;
        let original = reqwest::Url::parse("https://api.example/start")?;
        for target in [
            "https://private.example/file?token=secret",
            "https://[0:0:0:0:0:0:0:1]:8443/file",
            "https://api.example/return",
        ] {
            policy.check_redirect(
                &original,
                &original,
                &reqwest::Url::parse(target)?,
                0,
                HttpTransportSecurity::PreventDowngrade,
            )?;
        }
        for target in [
            "https://sub.private.example",
            "https://private.example:8443",
            "https://private.example.attacker.example",
        ] {
            assert_eq!(
                policy.check_redirect(
                    &original,
                    &original,
                    &reqwest::Url::parse(target)?,
                    0,
                    HttpTransportSecurity::PreventDowngrade,
                ),
                Err(HttpRedirectError::OriginNotAllowed)
            );
        }
        let debug = format!("{policy:?}");
        assert!(debug.contains("allowed_origin_count: 2"));
        assert!(!debug.contains("private.example"));
        assert!(!debug.contains("::1"));
        Ok(())
    }

    #[test]
    fn rejects_non_origin_inputs_without_echoing_them() {
        for origin in [
            "ftp://private.example",
            "https://user:secret@private.example",
            "https://@private.example",
            "https://private.example/path",
            "https://private.example/path/..",
            "https://private.example?secret",
            "https://private.example#secret",
            "https:\\\\private.example",
            "https:///private.example",
            " https://private.example",
            "https://private.example\n",
        ] {
            let error = RedirectPolicy::allow_origins([origin])
                .expect_err("an allowlist entry must be an origin");
            assert_eq!(error.kind(), ErrorKind::InvalidConfig);
            assert_eq!(error.context().stage, Some(ErrorStage::Configuration));
            assert!(!format!("{error:?} {error}").contains("private.example"));
        }
    }

    #[test]
    fn limits_are_bounded_and_do_not_enable_a_disabled_policy() -> TestResult {
        assert_eq!(RedirectPolicy::default().max_redirects(), 10);
        assert!(RedirectPolicy::default().is_enabled());
        assert!(!RedirectPolicy::none().with_max_redirects(1)?.is_enabled());
        for limit in [0, 129, usize::MAX] {
            assert!(RedirectPolicy::same_origin()
                .with_max_redirects(limit)
                .is_err());
        }
        let origins = (0..129).map(|index| format!("https://host{index}.example"));
        assert!(RedirectPolicy::allow_origins(origins).is_err());
        assert!(RedirectPolicy::allow_origins(["x".repeat(MAX_ORIGIN_BYTES + 1)]).is_err());
        let policy = RedirectPolicy::same_origin().with_max_redirects(1)?;
        let original = reqwest::Url::parse("https://api.example")?;
        assert_eq!(
            policy.check_redirect(
                &original,
                &original,
                &original,
                1,
                HttpTransportSecurity::PreventDowngrade,
            ),
            Err(HttpRedirectError::TooManyRedirects)
        );
        Ok(())
    }

    #[test]
    fn allowlisted_hops_never_expand_the_original_authorization() -> TestResult {
        let original = reqwest::Url::parse("https://api.example/start")?;
        let current = reqwest::Url::parse("https://cdn.example/first")?;
        let next = reqwest::Url::parse("https://cdn.example.evil/next")?;
        let policy = RedirectPolicy::allow_origins(["https://cdn.example"])?;
        assert_eq!(
            policy.check_redirect(
                &original,
                &current,
                &next,
                1,
                HttpTransportSecurity::PreventDowngrade,
            ),
            Err(HttpRedirectError::OriginNotAllowed)
        );
        policy.check_redirect(
            &original,
            &current,
            &original,
            1,
            HttpTransportSecurity::PreventDowngrade,
        )?;
        Ok(())
    }

    #[test]
    fn transport_checks_each_hop_even_for_allowlisted_targets() -> TestResult {
        let original = reqwest::Url::parse("http://api.example/start")?;
        let secure = reqwest::Url::parse("https://api.example/secure")?;
        let policy = RedirectPolicy::allow_origins(["https://api.example"])?;
        policy.check_redirect(
            &original,
            &original,
            &secure,
            0,
            HttpTransportSecurity::PreventDowngrade,
        )?;
        assert_eq!(
            policy.check_redirect(
                &original,
                &secure,
                &original,
                1,
                HttpTransportSecurity::PreventDowngrade,
            ),
            Err(HttpRedirectError::InsecureTarget)
        );
        assert_eq!(
            policy.check_redirect(
                &original,
                &original,
                &original,
                0,
                HttpTransportSecurity::HttpsOnly,
            ),
            Err(HttpRedirectError::InsecureTarget)
        );
        let failure =
            NetError::protocol(HttpRedirectError::InsecureTarget).with_stage(ErrorStage::Receive);
        assert_eq!(
            failure
                .source()
                .and_then(|source| source.downcast_ref::<HttpRedirectError>()),
            Some(&HttpRedirectError::InsecureTarget)
        );
        Ok(())
    }
}

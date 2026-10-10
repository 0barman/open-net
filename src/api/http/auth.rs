//! Authentication providers for the optional HTTP client.
//!
//! Authentication values are kept in private state and are only exposed to the
//! transport as validated headers.  The public context types intentionally do
//! not expose request bodies or mutable request metadata.

use crate::api::error::{ErrorKind, ErrorStage, NetError};
use crate::api::http::HttpRequestId;
use async_trait::async_trait;
use base64::Engine;
use futures_util::FutureExt;
use http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, Notify};

const REFRESH_TIMEOUT: Duration = Duration::from_secs(5);

/// Result returned by an authentication provider.
pub type AuthResult<T> = Result<T, NetError>;

/// Action selected after an unauthorized response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthAction {
    /// The provider updated credentials and the SDK may reconstruct the request.
    RetryWithFreshCredentials,
    /// Do not refresh or retry this request lineage.
    DoNotRetry,
}

/// Read-only request metadata plus a restricted header writer for one attempt.
pub struct AuthRequestContext<'a> {
    method: &'a Method,
    origin: &'a str,
    path: &'a str,
    request_id: HttpRequestId,
    attempt: u32,
    generation: u64,
    headers: &'a mut HeaderMap,
}

impl<'a> AuthRequestContext<'a> {
    pub(crate) fn new(
        method: &'a Method,
        origin: &'a str,
        path: &'a str,
        request_id: HttpRequestId,
        attempt: u32,
        headers: &'a mut HeaderMap,
    ) -> Self {
        Self {
            method,
            origin,
            path,
            request_id,
            attempt,
            generation: 0,
            headers,
        }
    }

    /// HTTP method for this attempt.
    pub fn method(&self) -> &Method {
        self.method
    }

    /// Origin to which the request is being sent.
    pub fn origin(&self) -> &str {
        self.origin
    }

    /// Validated relative request path.
    pub fn path(&self) -> &str {
        self.path
    }

    /// SDK request identifier.
    pub fn request_id(&self) -> HttpRequestId {
        self.request_id
    }

    /// One-based attempt number for this request lineage.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Credential generation used by the provider for this attempt.
    pub fn credential_generation(&self) -> u64 {
        self.generation
    }

    /// Insert an authentication header for this attempt.
    ///
    /// SDK-managed transport headers are rejected.  The SDK records the
    /// resulting header map afresh for every attempt, so a provider cannot
    /// mutate a persisted request object.
    pub fn set_header(&mut self, name: HeaderName, value: HeaderValue) -> AuthResult<()> {
        if is_forbidden_header(&name) {
            return Err(NetError::input(
                "auth.header",
                "cannot override an SDK-managed HTTP header",
            )
            .with_stage(ErrorStage::Provider));
        }
        let mut value = value;
        value.set_sensitive(true);
        self.headers.insert(name, value);
        Ok(())
    }

    pub(crate) fn set_credential_generation(&mut self, generation: u64) {
        self.generation = generation;
    }
}

/// Response metadata supplied to a provider after a 401 response.
pub struct AuthResponseContext<'a> {
    status: StatusCode,
    www_authenticate: Option<&'a HeaderValue>,
    request_id: HttpRequestId,
    attempt: u32,
    generation: u64,
}

impl<'a> AuthResponseContext<'a> {
    pub(crate) fn new(
        status: StatusCode,
        www_authenticate: Option<&'a HeaderValue>,
        request_id: HttpRequestId,
        attempt: u32,
        generation: u64,
    ) -> Self {
        Self {
            status,
            www_authenticate,
            request_id,
            attempt,
            generation,
        }
    }

    /// Response status.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Optional `WWW-Authenticate` challenge value.
    pub fn www_authenticate(&self) -> Option<&HeaderValue> {
        self.www_authenticate
    }

    /// SDK request identifier.
    pub fn request_id(&self) -> HttpRequestId {
        self.request_id
    }

    /// Attempt that received the response.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Credential generation used for the request that received the response.
    pub fn credential_generation(&self) -> u64 {
        self.generation
    }
}

/// Asynchronous client-level authentication provider.
#[async_trait]
pub trait HttpAuth: Send + Sync {
    /// Apply current credentials to one freshly built attempt.
    async fn apply(&self, request: &mut AuthRequestContext<'_>) -> AuthResult<()>;

    /// Decide whether a 401 response should refresh and retry.
    async fn on_unauthorized(&self, response: &AuthResponseContext<'_>) -> AuthResult<AuthAction>;
}

/// An access token held in private memory.
#[derive(Clone)]
pub struct AccessToken {
    value: Arc<str>,
    expires_at: Option<SystemTime>,
}

impl AccessToken {
    /// Create a validated access token. Values are bounded and cannot contain
    /// control characters because they will be used in an HTTP header.
    pub fn new(value: impl Into<String>, expires_at: Option<SystemTime>) -> AuthResult<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 16 * 1024
            || !value.is_ascii()
            || value.chars().any(char::is_control)
            || value.chars().any(char::is_whitespace)
        {
            return Err(NetError::input("auth.token", "token is empty or invalid")
                .with_stage(ErrorStage::Provider));
        }
        Ok(Self {
            value: Arc::from(value),
            expires_at,
        })
    }

    /// Return whether the token has passed its optional expiration time.
    pub fn is_expired(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|expires_at| now >= expires_at)
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccessToken")
            .field("value", &"<redacted>")
            .field("expires_at_configured", &self.expires_at.is_some())
            .finish()
    }
}

/// Source used by [`BearerAuth`] to load and refresh credentials.
#[async_trait]
pub trait BearerTokenSource: Send + Sync {
    /// Load the current token for the first attempt.
    async fn load(&self) -> AuthResult<AccessToken>;

    /// Refresh after a 401 response.
    async fn refresh(&self, reason: RefreshReason) -> AuthResult<AccessToken>;
}

/// Reason passed to a token source when selecting a credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefreshReason {
    /// No cached credential was available.
    Initial,
    /// The server rejected the current credential.
    Unauthorized,
}

struct RefreshGate {
    notify: Notify,
    result: Mutex<Option<Result<(), ErrorKind>>>,
}

impl RefreshGate {
    fn new() -> Self {
        Self {
            notify: Notify::new(),
            result: Mutex::new(None),
        }
    }
}

struct BearerState {
    token: Option<AccessToken>,
    generation: u64,
    in_flight: Option<Arc<RefreshGate>>,
    last_refresh_failure_generation: Option<u64>,
}

struct BearerInner {
    source: Arc<dyn BearerTokenSource>,
    allow_refresh: bool,
    state: Mutex<BearerState>,
}

/// Bearer authentication with cached credentials and single-flight refresh.
#[derive(Clone)]
pub struct BearerAuth {
    inner: Arc<BearerInner>,
}

impl fmt::Debug for BearerAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BearerAuth")
            .field("configured", &true)
            .finish()
    }
}

impl BearerAuth {
    /// Create a dynamic bearer provider backed by a token source.
    pub fn new(source: Arc<dyn BearerTokenSource>) -> Self {
        Self {
            inner: Arc::new(BearerInner {
                source,
                allow_refresh: true,
                state: Mutex::new(BearerState {
                    token: None,
                    generation: 0,
                    in_flight: None,
                    last_refresh_failure_generation: None,
                }),
            }),
        }
    }

    /// Create a static bearer provider. A 401 is returned to the caller and is
    /// never followed by an automatic refresh.
    pub fn from_static(value: impl Into<String>) -> AuthResult<Self> {
        let token = AccessToken::new(value, None)?;
        Ok(Self {
            inner: Arc::new(BearerInner {
                source: Arc::new(StaticTokenSource { token }),
                allow_refresh: false,
                state: Mutex::new(BearerState {
                    token: None,
                    generation: 0,
                    in_flight: None,
                    last_refresh_failure_generation: None,
                }),
            }),
        })
    }

    async fn current_token(&self) -> AuthResult<(AccessToken, u64)> {
        loop {
            let (gate, source) = {
                let mut state = self.inner.state.lock().await;
                if let Some(token) = state.token.clone() {
                    if !token.is_expired(SystemTime::now()) {
                        return Ok((token, state.generation));
                    }
                }
                if let Some(gate) = state.in_flight.as_ref() {
                    (Arc::clone(gate), None)
                } else {
                    let gate = Arc::new(RefreshGate::new());
                    state.in_flight = Some(Arc::clone(&gate));
                    (gate, Some(Arc::clone(&self.inner.source)))
                }
            };
            if let Some(source) = source {
                let inner = Arc::clone(&self.inner);
                let task_gate = Arc::clone(&gate);
                let spawn_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    tokio::spawn(async move {
                        let loaded = guarded_source_load(source).await;
                        complete_token_fetch(inner, task_gate, loaded, false, 0).await;
                    })
                }));
                if spawn_result.is_err() {
                    complete_token_fetch(
                        Arc::clone(&self.inner),
                        Arc::clone(&gate),
                        Err(provider_failure()),
                        false,
                        0,
                    )
                    .await;
                }
            }
            if let Err(error) = wait_for_gate(&gate).await {
                return Err(error);
            }
            let state = self.inner.state.lock().await;
            if let Some(token) = state.token.clone() {
                return Ok((token, state.generation));
            }
            return Err(provider_failure());
        }
    }

    async fn refresh_if_needed(&self, generation: u64) -> AuthResult<AuthAction> {
        if !self.inner.allow_refresh {
            return Ok(AuthAction::DoNotRetry);
        }
        loop {
            let (gate, source) = {
                let mut state = self.inner.state.lock().await;
                if state.generation > generation && state.token.is_some() {
                    return Ok(AuthAction::RetryWithFreshCredentials);
                }
                if state.last_refresh_failure_generation == Some(generation) {
                    return Err(provider_failure());
                }
                if let Some(gate) = state.in_flight.as_ref() {
                    (Arc::clone(gate), None)
                } else {
                    let gate = Arc::new(RefreshGate::new());
                    state.in_flight = Some(Arc::clone(&gate));
                    (gate, Some(Arc::clone(&self.inner.source)))
                }
            };
            if let Some(source) = source {
                let inner = Arc::clone(&self.inner);
                let task_gate = Arc::clone(&gate);
                let spawn_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    tokio::spawn(async move {
                        let refreshed = guarded_source_refresh(source).await;
                        complete_token_fetch(inner, task_gate, refreshed, true, generation).await;
                    })
                }));
                if spawn_result.is_err() {
                    complete_token_fetch(
                        Arc::clone(&self.inner),
                        Arc::clone(&gate),
                        Err(provider_failure()),
                        true,
                        generation,
                    )
                    .await;
                }
            }
            if let Err(error) = wait_for_gate(&gate).await {
                return Err(error);
            }
            let state = self.inner.state.lock().await;
            if state.generation > generation && state.token.is_some() {
                return Ok(AuthAction::RetryWithFreshCredentials);
            }
        }
    }
}

async fn wait_for_gate(gate: &RefreshGate) -> AuthResult<()> {
    let notified = gate.notify.notified();
    let result = gate.result.lock().await;
    if let Some(result) = result.as_ref() {
        return result.map_or_else(
            |kind| Err(NetError::from(kind).with_stage(ErrorStage::Provider)),
            |_| Ok(()),
        );
    }
    drop(result);
    notified.await;
    gate.result.lock().await.as_ref().map_or_else(
        || Err(provider_failure()),
        |result| {
            result.map_or_else(
                |kind| Err(NetError::from(kind).with_stage(ErrorStage::Provider)),
                |_| Ok(()),
            )
        },
    )
}

async fn complete_token_fetch(
    inner: Arc<BearerInner>,
    gate: Arc<RefreshGate>,
    loaded: AuthResult<AccessToken>,
    is_refresh: bool,
    refresh_generation: u64,
) {
    let mut outcome = loaded;
    if let Ok(token) = outcome.as_ref() {
        if token.is_expired(SystemTime::now()) {
            outcome = Err(provider_failure());
        }
    }
    let ok = outcome.is_ok();
    let mut state = inner.state.lock().await;
    if ok {
        if let Ok(token) = outcome.as_ref() {
            if let Some(generation) = state.generation.checked_add(1) {
                state.token = Some(token.clone());
                state.generation = generation;
                state.last_refresh_failure_generation = None;
            } else {
                outcome = Err(provider_failure());
            }
        }
    }
    let gate_result = match &outcome {
        Ok(_) => Ok(()),
        Err(error) => Err(error.kind()),
    };
    let final_ok = gate_result.is_ok();
    if is_refresh && !final_ok {
        state.last_refresh_failure_generation = Some(refresh_generation);
    }
    if state
        .in_flight
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, &gate))
    {
        state.in_flight = None;
    }
    drop(state);
    *gate.result.lock().await = Some(gate_result);
    gate.notify.notify_waiters();
}

#[async_trait]
impl HttpAuth for BearerAuth {
    async fn apply(&self, request: &mut AuthRequestContext<'_>) -> AuthResult<()> {
        let (token, generation) = self.current_token().await?;
        let value = HeaderValue::from_str(&format!("Bearer {}", token.value))
            .map_err(|_| provider_failure())?;
        request.set_credential_generation(generation);
        request.set_header(header::AUTHORIZATION, value)
    }

    async fn on_unauthorized(&self, response: &AuthResponseContext<'_>) -> AuthResult<AuthAction> {
        if response.status != StatusCode::UNAUTHORIZED {
            return Ok(AuthAction::DoNotRetry);
        }
        self.refresh_if_needed(response.generation).await
    }
}

struct StaticTokenSource {
    token: AccessToken,
}

#[async_trait]
impl BearerTokenSource for StaticTokenSource {
    async fn load(&self) -> AuthResult<AccessToken> {
        Ok(self.token.clone())
    }

    async fn refresh(&self, _reason: RefreshReason) -> AuthResult<AccessToken> {
        Err(provider_failure())
    }
}

/// Static HTTP Basic authentication. It never refreshes after a 401.
#[derive(Clone)]
pub struct BasicAuth {
    username: Arc<str>,
    password: Arc<str>,
}

impl BasicAuth {
    /// Construct a bounded username/password pair.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> AuthResult<Self> {
        let username = username.into();
        let password = password.into();
        if username.is_empty()
            || username.len() > 4096
            || password.len() > 4096
            || username.contains(':')
            || !username.is_ascii()
            || !password.is_ascii()
            || username.chars().any(char::is_control)
            || password.chars().any(char::is_control)
        {
            return Err(NetError::input("auth.basic", "credentials are invalid")
                .with_stage(ErrorStage::Provider));
        }
        Ok(Self {
            username: Arc::from(username),
            password: Arc::from(password),
        })
    }
}

impl fmt::Debug for BasicAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BasicAuth")
            .field("configured", &true)
            .finish()
    }
}

#[async_trait]
impl HttpAuth for BasicAuth {
    async fn apply(&self, request: &mut AuthRequestContext<'_>) -> AuthResult<()> {
        let raw = format!("{}:{}", self.username, self.password);
        let encoded = base64::engine::general_purpose::STANDARD.encode(raw.as_bytes());
        let value =
            HeaderValue::from_str(&format!("Basic {encoded}")).map_err(|_| provider_failure())?;
        request.set_header(header::AUTHORIZATION, value)
    }

    async fn on_unauthorized(&self, _response: &AuthResponseContext<'_>) -> AuthResult<AuthAction> {
        Ok(AuthAction::DoNotRetry)
    }
}

async fn guarded_source_load(source: Arc<dyn BearerTokenSource>) -> AuthResult<AccessToken> {
    let future = std::panic::AssertUnwindSafe(source.load()).catch_unwind();
    match tokio::time::timeout(REFRESH_TIMEOUT, future).await {
        Err(_) => Err(NetError::from(ErrorKind::TimedOut).with_stage(ErrorStage::Provider)),
        Ok(Err(_)) => Err(provider_failure()),
        Ok(Ok(result)) => result.map_err(sanitize_provider_error),
    }
}

async fn guarded_source_refresh(source: Arc<dyn BearerTokenSource>) -> AuthResult<AccessToken> {
    let future =
        std::panic::AssertUnwindSafe(source.refresh(RefreshReason::Unauthorized)).catch_unwind();
    match tokio::time::timeout(REFRESH_TIMEOUT, future).await {
        Err(_) => Err(NetError::from(ErrorKind::TimedOut).with_stage(ErrorStage::Provider)),
        Ok(Err(_)) => Err(provider_failure()),
        Ok(Ok(result)) => result.map_err(sanitize_provider_error),
    }
}

fn provider_failure() -> NetError {
    NetError::from(ErrorKind::ProviderFailed).with_stage(ErrorStage::Provider)
}

fn sanitize_provider_error(error: NetError) -> NetError {
    NetError::from(error.kind()).with_stage(ErrorStage::Provider)
}

fn is_forbidden_header(name: &HeaderName) -> bool {
    let name = name.as_str();
    name.starts_with("proxy-")
        || matches!(
            name,
            "host"
                | "connection"
                | "content-length"
                | "transfer-encoding"
                | "upgrade"
                | "te"
                | "trailer"
                | "keep-alive"
                | "proxy-connection"
                | "proxy-authenticate"
                | "proxy-authorization"
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use std::error::Error as StdError;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    #[test]
    fn token_validation_and_debug_are_redacted() -> TestResult {
        check(AccessToken::new("", None).is_err(), "empty token accepted")?;
        let token = AccessToken::new("secret-token", None)?;
        let debug = format!("{token:?}");
        check(!debug.contains("secret-token"), "token leaked in debug")?;
        let auth = BearerAuth::from_static("secret-token")?;
        check(
            !format!("{auth:?}").contains("secret-token"),
            "bearer token leaked in provider debug",
        )
    }

    #[tokio::test]
    async fn basic_provider_only_writes_authorization() -> TestResult {
        let auth = BasicAuth::new("user", "password")?;
        let method = Method::GET;
        let mut headers = HeaderMap::new();
        let mut request = AuthRequestContext::new(
            &method,
            "https://example.com",
            "/",
            HttpRequestId(1),
            1,
            &mut headers,
        );
        auth.apply(&mut request).await?;
        let value = headers
            .get(header::AUTHORIZATION)
            .ok_or_else(|| std::io::Error::other("authorization missing"))?;
        check(
            value.to_str()?.starts_with("Basic "),
            "invalid basic header",
        )
    }

    #[test]
    fn request_context_rejects_transport_headers() -> TestResult {
        let method = Method::GET;
        let mut headers = HeaderMap::new();
        let mut request = AuthRequestContext::new(
            &method,
            "https://example.com",
            "/",
            HttpRequestId(1),
            1,
            &mut headers,
        );
        let blocked = [
            "host",
            "connection",
            "content-length",
            "transfer-encoding",
            "upgrade",
            "te",
            "trailer",
            "keep-alive",
            "proxy-connection",
            "proxy-authenticate",
            "proxy-authorization",
        ];
        for name in blocked {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| std::io::Error::other("header name failed"))?;
            check(
                request
                    .set_header(name, HeaderValue::from_static("evil"))
                    .is_err(),
                "transport header override accepted",
            )?;
        }
        Ok(())
    }

    #[test]
    fn token_limits_ascii_expiry_and_clone_are_safe() -> TestResult {
        check(
            AccessToken::new("токен", None).is_err(),
            "non-ascii token accepted",
        )?;
        let oversized = "a".repeat(16 * 1024 + 1);
        check(
            AccessToken::new(oversized, None).is_err(),
            "oversized token accepted",
        )?;
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let token = AccessToken::new("short", Some(at))?;
        check(
            token.is_expired(SystemTime::UNIX_EPOCH + Duration::from_secs(11)),
            "expired token not recognized",
        )?;
        let cloned = token.clone();
        check(
            !cloned.is_expired(SystemTime::UNIX_EPOCH + Duration::from_secs(9)),
            "valid token reported expired",
        )
    }

    #[test]
    fn auth_headers_are_marked_sensitive_and_basic_colon_is_rejected() -> TestResult {
        check(
            BasicAuth::new("user:name", "secret").is_err(),
            "basic username containing colon accepted",
        )?;
        let method = Method::GET;
        let mut headers = HeaderMap::new();
        let mut request = AuthRequestContext::new(
            &method,
            "https://example.com",
            "/",
            HttpRequestId(1),
            1,
            &mut headers,
        );
        request.set_header(
            HeaderName::from_static("x-auth-token"),
            HeaderValue::from_static("secret"),
        )?;
        let value = headers
            .get("x-auth-token")
            .ok_or_else(|| std::io::Error::other("sensitive header missing"))?;
        check(
            value.is_sensitive(),
            "provider header was not marked sensitive",
        )
    }

    #[tokio::test]
    async fn bearer_cache_and_refresh_are_single_flight() -> TestResult {
        let source = Arc::new(CountingSource::new(false));
        let auth = BearerAuth::new(source.clone());
        let first_generation = apply_generation(&auth, 1).await?;
        let second_generation = apply_generation(&auth, 2).await?;
        check(
            first_generation == second_generation,
            "cached token changed",
        )?;
        let responses = (0..8).map(|request_id| {
            let auth = auth.clone();
            async move {
                let response = AuthResponseContext::new(
                    StatusCode::UNAUTHORIZED,
                    None,
                    HttpRequestId(request_id),
                    1,
                    first_generation,
                );
                auth.on_unauthorized(&response).await
            }
        });
        let outcomes = futures_util::future::join_all(responses).await;
        for outcome in outcomes {
            let is_retry = match outcome {
                Ok(action) => action == AuthAction::RetryWithFreshCredentials,
                Err(_) => false,
            };
            check(is_retry, "single-flight refresh did not retry")?;
        }
        check(
            source.refreshes.load(std::sync::atomic::Ordering::SeqCst) == 1,
            "concurrent unauthorized requests refreshed more than once",
        )
    }

    #[tokio::test]
    async fn failed_refresh_is_shared_for_old_generation() -> TestResult {
        let source = Arc::new(CountingSource::new(true));
        let auth = BearerAuth::new(source.clone());
        let generation = apply_generation(&auth, 1).await?;
        let first = AuthResponseContext::new(
            StatusCode::UNAUTHORIZED,
            None,
            HttpRequestId(1),
            1,
            generation,
        );
        let second = AuthResponseContext::new(
            StatusCode::UNAUTHORIZED,
            None,
            HttpRequestId(2),
            1,
            generation,
        );
        let outcomes = futures_util::future::join_all([
            auth.on_unauthorized(&first),
            auth.on_unauthorized(&second),
        ])
        .await;
        for outcome in outcomes {
            check(outcome.is_err(), "failed refresh unexpectedly succeeded")?;
        }
        check(
            source.refreshes.load(std::sync::atomic::Ordering::SeqCst) == 1,
            "failed refresh was duplicated",
        )
    }

    #[tokio::test]
    async fn source_errors_are_redacted_from_refresh_failures() -> TestResult {
        let auth = BearerAuth::new(Arc::new(SecretFailureSource));
        let generation = apply_generation(&auth, 1).await?;
        let response = AuthResponseContext::new(
            StatusCode::UNAUTHORIZED,
            None,
            HttpRequestId(1),
            1,
            generation,
        );
        let error = match auth.on_unauthorized(&response).await {
            Ok(_) => {
                return Err(std::io::Error::other("secret source unexpectedly succeeded").into())
            }
            Err(error) => error,
        };
        check(error.source().is_none(), "provider source was retained")?;
        check(
            !format!("{error:?}").contains("secret-refresh-token"),
            "provider secret leaked in error debug",
        )
    }

    #[tokio::test]
    async fn expired_loaded_token_fails_without_an_infinite_load_loop() -> TestResult {
        let source = Arc::new(ExpiredSource::new());
        let auth = BearerAuth::new(source.clone());
        let method = Method::GET;
        let mut headers = HeaderMap::new();
        let mut request = AuthRequestContext::new(
            &method,
            "https://example.com",
            "/",
            HttpRequestId(1),
            1,
            &mut headers,
        );
        let outcome = tokio::time::timeout(Duration::from_millis(250), auth.apply(&mut request))
            .await
            .map_err(|_| std::io::Error::other("expired token load hung"))?;
        check(outcome.is_err(), "expired token was applied")?;
        check(
            source.loads.load(std::sync::atomic::Ordering::SeqCst) == 1,
            "expired token caused repeated loads",
        )
    }

    #[tokio::test]
    async fn static_bearer_and_basic_do_not_retry_401() -> TestResult {
        let bearer = BearerAuth::from_static("static-token")?;
        let basic = BasicAuth::new("user", "secret")?;
        let response =
            AuthResponseContext::new(StatusCode::UNAUTHORIZED, None, HttpRequestId(1), 1, 1);
        check(
            bearer.on_unauthorized(&response).await? == AuthAction::DoNotRetry,
            "static bearer requested retry",
        )?;
        check(
            basic.on_unauthorized(&response).await? == AuthAction::DoNotRetry,
            "basic requested retry",
        )?;
        let forbidden =
            AuthResponseContext::new(StatusCode::FORBIDDEN, None, HttpRequestId(2), 1, 1);
        check(
            bearer.on_unauthorized(&forbidden).await? == AuthAction::DoNotRetry,
            "bearer refreshed for a non-401 response",
        )
    }

    async fn apply_generation(auth: &BearerAuth, request_id: u64) -> AuthResult<u64> {
        let method = Method::GET;
        let mut headers = HeaderMap::new();
        let mut request = AuthRequestContext::new(
            &method,
            "https://example.com",
            "/",
            HttpRequestId(request_id),
            1,
            &mut headers,
        );
        auth.apply(&mut request).await?;
        Ok(request.credential_generation())
    }

    struct CountingSource {
        loads: std::sync::atomic::AtomicUsize,
        refreshes: std::sync::atomic::AtomicUsize,
        fail_refresh: bool,
    }

    impl CountingSource {
        fn new(fail_refresh: bool) -> Self {
            Self {
                loads: std::sync::atomic::AtomicUsize::new(0),
                refreshes: std::sync::atomic::AtomicUsize::new(0),
                fail_refresh,
            }
        }
    }

    #[async_trait]
    impl BearerTokenSource for CountingSource {
        async fn load(&self) -> AuthResult<AccessToken> {
            self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            AccessToken::new("initial-token", None)
        }

        async fn refresh(&self, _reason: RefreshReason) -> AuthResult<AccessToken> {
            self.refreshes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            if self.fail_refresh {
                Err(provider_failure())
            } else {
                AccessToken::new("fresh-token", None)
            }
        }
    }

    struct ExpiredSource {
        loads: std::sync::atomic::AtomicUsize,
    }

    impl ExpiredSource {
        fn new() -> Self {
            Self {
                loads: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    struct SecretFailureSource;

    #[async_trait]
    impl BearerTokenSource for SecretFailureSource {
        async fn load(&self) -> AuthResult<AccessToken> {
            AccessToken::new("initial-token", None)
        }

        async fn refresh(&self, _reason: RefreshReason) -> AuthResult<AccessToken> {
            Err(NetError::provider(std::io::Error::other(
                "secret-refresh-token",
            )))
        }
    }

    #[async_trait]
    impl BearerTokenSource for ExpiredSource {
        async fn load(&self) -> AuthResult<AccessToken> {
            self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            AccessToken::new("expired-token", Some(SystemTime::UNIX_EPOCH))
        }

        async fn refresh(&self, _reason: RefreshReason) -> AuthResult<AccessToken> {
            AccessToken::new("expired-token", Some(SystemTime::UNIX_EPOCH))
        }
    }
}

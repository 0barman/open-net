# HTTP authentication and token refresh

Required feature: `http-client-auth` (includes `http-client`). Manually setting an `Authorization` header needs only basic `http-client`; providers and automatic credential refresh require this feature.

```toml
[dependencies]
open-net = { path = "../open-net", default-features = false, features = ["http-client-auth"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Application TLS provider

open-net does not choose a cryptographic provider. Each complete program that
creates an HTTP client below initializes the example application's provider in
`main`; copy the program as written. Configuration-only examples need no provider
until a client is built. Client creation requires a provider even for plain HTTP
and otherwise returns `InvalidConfig` for `tls.provider`.

The examples use OpenSSL through `rustls-openssl`, an application dependency.
Another compatible rustls 0.23 provider can replace it. Install a process provider
once in the host application, or pass an explicit per-client provider. A library
using open-net should accept the host's choice rather than install its own global
provider. See [TLS providers](../tls.md) for build prerequisites, provider
precedence and complete examples of both approaches.

Adjust the local path from your consumer project to this checkout. Authentication types are re-exported through `open_net::api::http`. Import the re-exported `open_net::async_trait` when implementing asynchronous provider traits; it supplies the object-safe asynchronous trait plumbing. Provider methods borrow `&self`, so shared mutable application state needs synchronization.

## Static Basic and Bearer authentication

### Required features

`http-client-auth`.

### Function signature

```text
BasicAuth::new(username: impl Into<String>, password: impl Into<String>) -> AuthResult<Self>
BearerAuth::from_static(value: impl Into<String>) -> AuthResult<Self>
HttpClientConfig::with_auth<A>(self, auth: A) -> Self where A: HttpAuth + 'static
HttpClientConfig::with_auth_arc(self, auth: Arc<dyn HttpAuth>) -> Self
pub type AuthResult<T> = Result<T, NetError>;
```

### Steps

Obtain credentials from your application's secret storage, construct one provider, install it before client creation and send requests. Use HTTPS to protect the credentials in transit.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `username` | `impl Into<String>` | Basic only | Nonempty ASCII, at most 4096 bytes, no control characters or colon. |
| `password` | `impl Into<String>` | Basic only | ASCII, at most 4096 bytes, no control characters; empty allowed. |
| `value` | `impl Into<String>` | Bearer only | Nonempty ASCII, at most 16,384 bytes; no whitespace or control characters. |
| `auth` | `A: HttpAuth + 'static` or `Arc<dyn HttpAuth>` | Yes | Client-level provider; replacing it replaces the previous provider. `Arc` makes sharing explicit. |

### Callback parameters

The worker invokes `apply` and, when eligible, `on_unauthorized` as specified below. There are no callback arguments to these constructors.

### Return values

Constructors return `Ok(provider)` or `Err(NetError)`; installation returns the configuration. `AuthResult<T>` is a convenience alias, not a distinct error type.

### Errors and behavior

Invalid credentials return `InvalidInput` at `Provider`. Providers inject a sensitive Authorization header into each newly built attempt, replacing an existing value for that name after common/request header merging. Both static providers return `DoNotRetry` for 401; they never refresh. A final 401 is normally a successful HTTP response value until you apply an explicit status policy. Secret fields are private and Debug is redacted.

### Example

This program constructs alternative configurations without network requests. Replace demonstration credentials with application secrets before real use.

```rust
use open_net::api::http::{BasicAuth, BearerAuth, HttpAuth, HttpClientConfig};
use open_net::NetError;
use std::sync::Arc;

fn main() -> Result<(), NetError> {
    let basic = BasicAuth::new("demo-user", "demo-password")?;
    HttpClientConfig::new("https://example.com/")?.with_auth(basic).validate()?;
    let bearer: Arc<dyn HttpAuth> = Arc::new(BearerAuth::from_static("demo-token")?);
    HttpClientConfig::new("https://example.com/")?.with_auth_arc(bearer).validate()
}
```

## `AccessToken`, `BearerTokenSource` and dynamic `BearerAuth`

### Required features

`http-client-auth`.

### Function signature

```text
AccessToken::new(value: impl Into<String>, expires_at: Option<SystemTime>) -> AuthResult<Self>
AccessToken::is_expired(&self, now: SystemTime) -> bool
BearerAuth::new(source: Arc<dyn BearerTokenSource>) -> Self
#[async_trait]
pub trait BearerTokenSource: Send + Sync {
    async fn load(&self) -> AuthResult<AccessToken>;
    async fn refresh(&self, reason: RefreshReason) -> AuthResult<AccessToken>;
}
```

`SystemTime` is `std::time::SystemTime`. `AccessToken` is cloneable, has no public secret accessor, and hides its value from Debug. `BearerAuth::clone` shares the credential cache and refresh coordination.

### Steps

1. Implement `load` to retrieve the current token and `refresh` to obtain a new token.
2. Wrap the source in `Arc` and pass it to `BearerAuth::new`.
3. Install the provider and issue requests; a current valid cached token is reused.
4. Shut down the HTTP client; do not put token refresh I/O on the same saturated client's request quota.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `value` | `impl Into<String>` | Yes | Same token validation as `from_static`. |
| `expires_at` | `Option<SystemTime>` | Yes | `None` means no known expiry; `Some` is an absolute instant. |
| `now` | `SystemTime` | Yes for expiration query | Expired when `now >= expires_at`; no expiration means false. |
| `source` | `Arc<dyn BearerTokenSource>` | Yes | Shared asynchronous token loader/refresher. |

### Callback parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `load` arguments | None besides `&self` | Used when the cache is empty or its token expired. |
| `reason` | `RefreshReason` | Variants are `Initial` and `Unauthorized`. The built-in provider currently calls `load` for initial/expired state and passes `Unauthorized` to `refresh`. |

### Return values

Source methods and `AccessToken::new` return `Ok(AccessToken)` or `Err(NetError)`. `BearerAuth::new` returns a provider without performing I/O. `is_expired` returns `bool`.

### Errors and behavior

The provider rejects already expired fetched tokens. Concurrent loads/refreshes share one in-flight operation; a newer credential generation avoids refreshing an already replaced token. A failed refresh is remembered for that rejected generation. Built-in source load/refresh jobs have a five-second guard, catch unwinding panics and sanitize errors. An outer HTTP attempt/total deadline can end the waiting request earlier. Direct provider calls may see `TimedOut` from the guard; the HTTP worker sanitizes errors returned by providers to `ProviderFailed/Provider`. When the worker's own budget expires while awaiting a provider, it preserves the timeout classification: Total produces `DeadlineExceeded`, while other elapsed phases produce `TimedOut`, with the corresponding timeout phase. See [timeout error classification](timeouts_and_admission.md). Cancellation of one waiter does not necessarily cancel a shared source job needed by other requests.

Recovery after a refresh failure needs an explicit policy. Later 401 responses for the same rejected credential generation fail without calling `refresh` again, even after the identity service recovers. A cached token with `expires_at: None` never expires locally, so that failure can remain latched indefinitely. Cloning `BearerAuth`, wrapping the same provider in another `Arc`, or installing that same provider in another client shares the latch and does not reset it. To recover, the source can perform bounded retries within its original five-second source call; or the application can replace the affected client with one configured with a **new** `BearerAuth::new(source)` after restoring the source. For expiring tokens, a later successful `load` after local expiry advances the generation and clears the failure; expiry alone does not guarantee recovery if `load` keeps failing or returns an expired token. Shut down the replaced client separately. There is no public cache-reset or force-refresh method. The example below deliberately uses `None`, so changing its environment variable alone cannot clear a refresh failure already recorded by an existing provider.

An eligible 401 allows **one** credential-refresh replay per logical operation, independently of the transient retry count. A second 401 or `DoNotRetry` blocks further 401/application replay in that operation. Replay still requires the request's safety policy and replayable body. `Automatic` safety permits HTTP idempotent methods; an ordinary unsafe POST is not automatically made replayable by installing auth. See [retry](retry.md) before declaring an endpoint safe. `RequestSafety::Never` blocks refresh replay. Streaming authentication happens before the stream is returned; a delivered stream is never replayed.

### Example

This complete program uses two tokens supplied by your application through environment variables. Set `AUTH_BASE_URL`, `ACCESS_TOKEN` and `REFRESHED_ACCESS_TOKEN`; the server must expose `/account`, reject the old token with 401 when testing refresh, and accept the refreshed token. The example's source reads a pre-obtained replacement; a production source would call your identity service in `refresh` using an independent client. No OAuth protocol is provided by this trait itself.

```rust
use open_net::api::http::{
    AccessToken, AuthResult, BearerAuth, BearerTokenSource,
    HttpClient, HttpClientConfig, RefreshReason,
};
use open_net::{async_trait, NetError};
use open_net::error::ErrorKind;
use std::sync::Arc;
use std::time::SystemTime;

struct EnvironmentTokens;
fn read_token(name: &str) -> AuthResult<AccessToken> {
    let value = std::env::var(name)
        .map_err(|_| NetError::from(ErrorKind::ProviderFailed))?;
    AccessToken::new(value, None)
}
#[async_trait]
impl BearerTokenSource for EnvironmentTokens {
    async fn load(&self) -> AuthResult<AccessToken> { read_token("ACCESS_TOKEN") }
    async fn refresh(&self, _reason: RefreshReason) -> AuthResult<AccessToken> {
        read_token("REFRESHED_ACCESS_TOKEN")
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let token = AccessToken::new("example-validation-only", None)?;
    assert!(!token.is_expired(SystemTime::now()));
    let base = std::env::var("AUTH_BASE_URL")?;
    let auth = BearerAuth::new(Arc::new(EnvironmentTokens));
    let config = HttpClientConfig::new(base)?.with_auth(auth);
    let client = HttpClient::new(config, "auth-example")?;
    let result = client.get("/account").await;
    client.shutdown().await?;
    println!("status: {}", result?.status);
    Ok(())
}
```

## Custom `HttpAuth` and context accessors

### Required features

`http-client-auth`.

### Function signature

```text
#[async_trait]
pub trait HttpAuth: Send + Sync {
    async fn apply(&self, request: &mut AuthRequestContext<'_>) -> AuthResult<()>;
    async fn on_unauthorized(&self, response: &AuthResponseContext<'_>) -> AuthResult<AuthAction>;
}
AuthRequestContext::method(&self) -> &Method
AuthRequestContext::origin(&self) -> &str
AuthRequestContext::path(&self) -> &str
AuthRequestContext::request_id(&self) -> HttpRequestId
AuthRequestContext::attempt(&self) -> u32
AuthRequestContext::credential_generation(&self) -> u64
AuthRequestContext::set_header(&mut self, name: HeaderName, value: HeaderValue) -> AuthResult<()>
AuthResponseContext::status(&self) -> StatusCode
AuthResponseContext::www_authenticate(&self) -> Option<&HeaderValue>
AuthResponseContext::request_id(&self) -> HttpRequestId
AuthResponseContext::attempt(&self) -> u32
AuthResponseContext::credential_generation(&self) -> u64
```

### Steps

Implement both methods, inspect the read-only context, set only your authentication headers and install with `with_auth` or `with_auth_arc`. The SDK creates contexts; integrations cannot construct them directly or mutate a body through them. `async` methods may suspend with `.await`; avoid blocking calls inside them.

### Parameters

`HttpAuth` methods receive `&self`; context getters take no arguments beyond the receiver. `set_header` requires a typed `HeaderName` and `HeaderValue` (both available from open-net's public exports).

### Callback parameters

| Parameter / accessor | Type | Description |
| --- | --- | --- |
| `request` | `&mut AuthRequestContext<'_>` | Current attempt; method, origin and validated relative path. Path may contain a query, so do not log it without sanitizing. |
| `response` | `&AuthResponseContext<'_>` | Eligible 401 response metadata, no body. |
| `request_id()` | `HttpRequestId` | SDK operation identifier on either context. |
| `attempt()` | `u32` | One-based actual attempt count. |
| `credential_generation()` | `u64` | Credential generation recorded by built-in Bearer auth; custom providers have no public setter and normally observe zero. |
| `status()` | `StatusCode` | Unauthorized response status. |
| `www_authenticate()` | `Option<&HeaderValue>` | Challenge header if present; `None` if absent. |
| `set_header(name, value)` | `HeaderName`, `HeaderValue` | Inserts/replaces a header and marks it sensitive. |

### Return values

`apply`: `Ok(())` to continue or `Err(NetError)` to abort. `on_unauthorized`: `Ok(AuthAction::RetryWithFreshCredentials)` after updating credentials, `Ok(AuthAction::DoNotRetry)` to deliver the response, or `Err(NetError)` to fail. Getters return borrowed/copied metadata; `set_header` returns `Ok(())` or `Err(NetError)`.

### Errors and behavior

`set_header` rejects every `proxy-*` name plus `host`, `connection`, `content-length`, `transfer-encoding`, `upgrade`, `te`, `trailer`, and `keep-alive`, with `InvalidInput/Provider`. The worker catches unwinding provider panics and strips arbitrary provider errors. Async provider execution consumes the operation's request budgets; custom `HttpAuth` does not automatically acquire the built-in token source's five-second guard.

Same-origin redirects reapply credentials for the destination path. A cross-origin hop strips sensitive headers and disables auth for the remainder of that redirect chain, including if it later returns to the original origin. Redirects remain bounded to ten followed hops. Authentication providers operate on both concrete and callback requests. Middleware, if enabled, surrounds the logical request, while auth is reevaluated inside attempts.

### Example

This complete offline example configures a custom API-key provider and demonstrates all context accessors in its hooks. It never prints secrets.

```rust
use open_net::api::http::{
    AuthAction, AuthRequestContext, AuthResponseContext, AuthResult,
    HttpAuth, HttpClientConfig,
};
use open_net::{async_trait, HeaderName, HeaderValue, NetError};

struct ApiKey(HeaderValue);
#[async_trait]
impl HttpAuth for ApiKey {
    async fn apply(&self, request: &mut AuthRequestContext<'_>) -> AuthResult<()> {
        let _metadata = (request.method(), request.origin(), request.path(),
            request.request_id(), request.attempt(), request.credential_generation());
        request.set_header(HeaderName::from_static("x-api-key"), self.0.clone())
    }
    async fn on_unauthorized(&self, response: &AuthResponseContext<'_>) -> AuthResult<AuthAction> {
        let _metadata = (response.status(), response.www_authenticate(),
            response.request_id(), response.attempt(), response.credential_generation());
        Ok(AuthAction::DoNotRetry)
    }
}
fn main() -> Result<(), NetError> {
    HttpClientConfig::new("https://example.com/")?
        .with_auth(ApiKey(HeaderValue::from_static("example-key"))).validate()
}
```


## `BasicAuth::new()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn new(username: impl Into<String>, password: impl Into<String>) -> AuthResult<Self>
```

### Steps

Read credentials from your application's secret storage, construct the provider, then install it on a configuration.

### Parameters

`username` must be nonempty ASCII, at most 4096 bytes, without control characters or a colon. `password` has the same size/ASCII/control limits but may be empty and contain a colon.

### Callback parameters

None.

### Return values

`Ok(BasicAuth)` or `Err(NetError)`.

### Errors and behavior

Invalid credentials return `InvalidInput/Provider`. A static Basic provider never refreshes a 401; send its credentials over HTTPS.

### Example

See the complete [static authentication example](#static-basic-and-bearer-authentication), which constructs and validates both alternative configurations.


## `BearerAuth::from_static()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn from_static(value: impl Into<String>) -> AuthResult<Self>
```

### Steps

Construct a provider from a pre-obtained token, then install it before client creation.

### Parameters

`value` is nonempty ASCII, at most 16,384 bytes, without whitespace or control characters.

### Callback parameters

None.

### Return values

`Ok(BearerAuth)` or `Err(NetError)`.

### Errors and behavior

Invalid tokens return `InvalidInput/Provider`. This constructor disables automatic refresh; a 401 does not call a token source.

### Example

See the complete [static authentication example](#static-basic-and-bearer-authentication), which constructs and validates both alternative configurations.


## `HttpClientConfig::with_auth()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn with_auth<A>(self, auth: A) -> Self
where A: HttpAuth + 'static
```

### Steps

Create a provider and move it into the configuration before constructing the client.

### Parameters

`auth` is an owned thread-safe provider; `self` is the consumed configuration.

### Callback parameters

The worker later invokes the provider's `apply` and, when eligible, `on_unauthorized` hooks.

### Return values

The updated configuration.

### Errors and behavior

Replaces any previously installed provider. Installing a clone of a `BearerAuth` shares its token cache, in-flight job and failed-refresh state.

### Example

See the complete [static authentication example](#static-basic-and-bearer-authentication), which constructs and validates both alternative configurations.


## `HttpClientConfig::with_auth_arc()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn with_auth_arc(self, auth: Arc<dyn HttpAuth>) -> Self
```

### Steps

Wrap a provider in `Arc` when its sharing is intentional, then install it before client construction.

### Parameters

`auth` is the shared provider; `self` is the consumed configuration.

### Callback parameters

The worker later invokes `apply` and eligible `on_unauthorized` hooks.

### Return values

The updated configuration.

### Errors and behavior

Replaces the previous provider. A new client using the same `Arc` does not reset the provider's state. Shared providers may be called concurrently.

### Example

See the complete [static authentication example](#static-basic-and-bearer-authentication), which constructs and validates both alternative configurations.


## `AccessToken::new()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn new(value: impl Into<String>, expires_at: Option<SystemTime>) -> AuthResult<Self>
```

### Steps

Validate the token returned by your secret store or identity service and attach its absolute expiry when known.

### Parameters

`value` is nonempty ASCII, at most 16,384 bytes, without whitespace/control characters. `expires_at` is `Some(SystemTime)` or `None` when no expiry is known.

### Callback parameters

None.

### Return values

`Ok(AccessToken)` or `Err(NetError)`.

### Errors and behavior

Invalid token text returns `InvalidInput/Provider`. This constructor validates the text; the built-in provider separately rejects an already expired fetched token. `None` prevents local expiration, including recovery by loading after expiry.

### Example

See the complete [dynamic token-source example](#accesstoken-bearertokensource-and-dynamic-bearerauth), including the source implementation, provider installation, request and shutdown.


## `AccessToken::is_expired()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn is_expired(&self, now: SystemTime) -> bool
```

### Steps

Supply the current time when checking a token before returning it from your source.

### Parameters

`now` is an absolute system-clock instant; `&self` borrows the token.

### Callback parameters

None.

### Return values

`true` when the configured expiry is at or before `now`; `false` for no expiry or a future expiry.

### Errors and behavior

No token value is exposed and no I/O occurs. This is a local clock comparison, not proof that the server accepts the token.

### Example

See the complete [dynamic token-source example](#accesstoken-bearertokensource-and-dynamic-bearerauth), including the source implementation, provider installation, request and shutdown.


## `BearerAuth::new()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn new(source: Arc<dyn BearerTokenSource>) -> Self
```

### Steps

Implement the asynchronous token source, wrap it in `Arc`, and construct a fresh provider before client creation.

### Parameters

`source` is the shared token loader/refresher. It must implement `Send + Sync`.

### Callback parameters

The provider later calls `load` for missing/expired cached tokens and `refresh` after an eligible 401.

### Return values

A provider with a new empty credential cache; construction performs no I/O.

### Errors and behavior

Each call creates independent coordination state. Cloning the returned provider shares that state. To recover from a latched refresh failure, construct a fresh provider and replacement client; reusing or cloning the old provider does not reset it.

### Example

See the complete [dynamic token-source example](#accesstoken-bearertokensource-and-dynamic-bearerauth), including the source implementation, provider installation, request and shutdown.


## `BearerTokenSource::load()`

### Required features

`http-client-auth`.

### Function signature

```text
#[async_trait]
async fn load(&self) -> AuthResult<AccessToken>
```

### Steps

Implement retrieval of the current usable token. The built-in provider calls it for an empty or locally expired cache.

### Parameters

Only `&self`, borrowing the source.

### Callback parameters

No additional arguments. An expired token triggers `load`, not `refresh(RefreshReason::Initial)`.

### Return values

`Ok(AccessToken)` or `Err(NetError)`.

### Errors and behavior

The built-in provider shares concurrent loads, rejects expired returned tokens and imposes a five-second guard. A successful load advances the credential generation and clears a remembered refresh failure. Avoid I/O through the same saturated HTTP client.

### Example

See the complete [dynamic token-source example](#accesstoken-bearertokensource-and-dynamic-bearerauth), including the source implementation, provider installation, request and shutdown.


## `BearerTokenSource::refresh()`

### Required features

`http-client-auth`.

### Function signature

```text
#[async_trait]
async fn refresh(&self, reason: RefreshReason) -> AuthResult<AccessToken>
```

### Steps

Implement acquisition of a replacement credential after rejection; return only after the replacement is available.

### Parameters

`&self` borrows the source.

### Callback parameters

`reason: RefreshReason`; the built-in provider currently passes `Unauthorized`. The public enum also defines `Initial`.

### Return values

`Ok(AccessToken)` or `Err(NetError)`.

### Errors and behavior

Shares one in-flight fetch and has a five-second guard. Failure is latched for the rejected credential generation: later 401 responses do not automatically retry this method. Any source-internal retry must fit within that guard; see the recovery policy above.

### Example

See the complete [dynamic token-source example](#accesstoken-bearertokensource-and-dynamic-bearerauth), including the source implementation, provider installation, request and shutdown.


## `HttpAuth::apply()`

### Required features

`http-client-auth`.

### Function signature

```text
#[async_trait]
async fn apply(&self, request: &mut AuthRequestContext<'_>) -> AuthResult<()>
```

### Steps

Implement the hook to read metadata and set only the authentication headers needed by this attempt.

### Parameters

`&self` borrows the provider. Context construction is internal to the SDK.

### Callback parameters

`request: &mut AuthRequestContext` describes the current attempt and supplies the restricted header writer.

### Return values

`Ok(())` continues the attempt; `Err(NetError)` aborts it.

### Errors and behavior

Runs when a new authenticated attempt is built, after common/request headers merge. The worker sanitizes returned provider failures to `ProviderFailed/Provider`; its own Total expiry remains `DeadlineExceeded` and other timeout phases remain `TimedOut`. Custom providers do not inherit the token-source five-second guard.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `HttpAuth::on_unauthorized()`

### Required features

`http-client-auth`.

### Function signature

```text
#[async_trait]
async fn on_unauthorized(&self, response: &AuthResponseContext<'_>) -> AuthResult<AuthAction>
```

### Steps

Implement the hook to decide whether credentials have been refreshed for an eligible 401 replay.

### Parameters

`&self` borrows the provider. The SDK constructs the read-only response context.

### Callback parameters

`response: &AuthResponseContext` includes status, optional challenge, request ID, attempt and credential generation.

### Return values

`Ok(RetryWithFreshCredentials)` requests replay after updating credentials; `Ok(DoNotRetry)` delivers the response; `Err(NetError)` fails the operation.

### Errors and behavior

Only one credential-refresh replay is allowed per logical operation and request safety still applies. Ordinary unsafe POST and `RequestSafety::Never` are not made replayable by installing auth. The hook never replays a stream that has already been delivered.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthRequestContext::method()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn method(&self) -> &Method
```

### Steps

Read the HTTP method when choosing credentials or signing an attempt.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpAuth::apply` hook receives the context from the SDK.

### Return values

The current request method.

### Errors and behavior

Read-only; a provider cannot change the method through this context.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthRequestContext::origin()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn origin(&self) -> &str
```

### Steps

Read the destination origin before choosing origin-specific credentials.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpAuth::apply` hook receives the context from the SDK.

### Return values

The current attempt's origin string.

### Errors and behavior

Read-only; cross-origin redirects disable auth for the rest of that redirect chain.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthRequestContext::path()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn path(&self) -> &str
```

### Steps

Read the validated relative path when selecting credentials or constructing a signature.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpAuth::apply` hook receives the context from the SDK.

### Return values

The current request path, potentially including a query.

### Errors and behavior

May contain sensitive query data. This accessor does not sanitize it for logging or permit mutation.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthRequestContext::request_id()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn request_id(&self) -> HttpRequestId
```

### Steps

Correlate provider work with the logical HTTP operation.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpAuth::apply` hook receives the context from the SDK.

### Return values

The copied SDK request identifier.

### Errors and behavior

Does not allocate a new ID; repeated attempts for one operation retain the operation identity.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthRequestContext::attempt()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn attempt(&self) -> u32
```

### Steps

Read the attempt number for diagnostics or provider decisions.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpAuth::apply` hook receives the context from the SDK.

### Return values

The one-based current attempt count.

### Errors and behavior

Counts actual attempts; do not interpret it as a refresh count or a zero-based index.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthRequestContext::credential_generation()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn credential_generation(&self) -> u64
```

### Steps

Read the generation currently recorded on the context.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpAuth::apply` hook receives the context from the SDK.

### Return values

The recorded credential generation.

### Errors and behavior

Built-in Bearer authentication records its generation internally. Custom providers have no public setter and normally observe zero; this value is not an application-owned retry counter.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthRequestContext::set_header()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn set_header(&mut self, name: HeaderName, value: HeaderValue) -> AuthResult<()>
```

### Steps

Build typed header values, then set the authentication header in `apply`.

### Parameters

`name` is a typed header name; `value` is its owned value; `&mut self` borrows the request context.

### Callback parameters

The enclosing `HttpAuth::apply` hook supplies the context.

### Return values

`Ok(())` after insertion/replacement, or `Err(NetError)`.

### Errors and behavior

Marks the value sensitive. Rejects every `proxy-*` name plus `host`, `connection`, `content-length`, `transfer-encoding`, `upgrade`, `te`, `trailer` and `keep-alive` with `InvalidInput/Provider`. Invalid typed header construction can fail before this method is called.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthResponseContext::status()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn status(&self) -> StatusCode
```

### Steps

Inspect the response status before selecting an authentication action.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpAuth::on_unauthorized` hook receives the context from the SDK.

### Return values

The copied HTTP status.

### Errors and behavior

The worker supplies an eligible 401 to this hook; reading status does not turn that response into an HTTP-status error.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthResponseContext::www_authenticate()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn www_authenticate(&self) -> Option<&HeaderValue>
```

### Steps

Inspect an optional challenge when your authentication scheme uses it.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpAuth::on_unauthorized` hook receives the context from the SDK.

### Return values

`Some(&HeaderValue)` for a present challenge, otherwise `None`.

### Errors and behavior

The header remains a raw typed value; no challenge parser or OAuth protocol is supplied. Do not assume it exists or is UTF-8.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthResponseContext::request_id()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn request_id(&self) -> HttpRequestId
```

### Steps

Correlate the rejected response with the logical request.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpAuth::on_unauthorized` hook receives the context from the SDK.

### Return values

The copied SDK request identifier.

### Errors and behavior

This is metadata only; it does not cancel, replay or create a request.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthResponseContext::attempt()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn attempt(&self) -> u32
```

### Steps

Read which actual attempt received this response.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpAuth::on_unauthorized` hook receives the context from the SDK.

### Return values

The one-based attempt number.

### Errors and behavior

The number does not remove the one-refresh-replay limit or override request safety.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.


## `AuthResponseContext::credential_generation()`

### Required features

`http-client-auth`.

### Function signature

```text
pub fn credential_generation(&self) -> u64
```

### Steps

Read which credential generation produced the rejected response.

### Parameters

Only `&self`.

### Callback parameters

The enclosing `HttpAuth::on_unauthorized` hook receives the context from the SDK.

### Return values

The generation recorded on that request attempt.

### Errors and behavior

Built-in Bearer auth uses this to avoid refreshing an already superseded credential. Custom providers normally observe zero and cannot set it through a public API.

### Example

See the complete [custom authentication example](#custom-httpauth-and-context-accessors). Its hook implementations use every context accessor; the SDK supplies the contexts during actual requests.

[HTTP guide index](README.md)

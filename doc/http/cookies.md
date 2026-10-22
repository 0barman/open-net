# Cookies and application-managed persistence

Required feature: `http-client-cookies` (includes `http-client`). Cookie processing also requires an installed store; enabling the feature alone changes no request into a Cookie session.

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false, features = ["http-client-cookies"] }
rustls-openssl = { version = "=0.4.2", default-features = false, features = ["tls12"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
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

Public Cookie types, `Url`, `Method`, `HeaderMap` and `HeaderValue` are re-exported by `open_net::api::http`. `Arc` is `std::sync::Arc`.

## Create, limit and install a `CookieJar`

### Required features

`http-client-cookies`.

### Function signature

```text
CookieJar::new() -> Self
CookieJar::with_limits(limits: CookieLimits) -> Result<Self, NetError>
CookieJar::limits(&self) -> CookieLimits
CookieJar::clear(&self) -> Result<(), NetError>
CookieLimits::validate(&self) -> Result<(), NetError>
HttpClientConfig::with_cookie_store<S>(self, store: S) -> Self where S: CookieStore + 'static
HttpClientConfig::with_cookie_store_arc(self, store: Arc<dyn CookieStore>) -> Self
HttpClientConfig::with_cookie_limits(self, limits: CookieLimits) -> Result<Self, NetError>
```

### Steps

1. Choose positive limits and create a jar.
2. Clone the jar when you intentionally want to share its state.
3. Install the jar and worker limits before client creation.
4. Send requests; matching cookies are selected for each attempt and response cookies are processed at headers.
5. Shut down the client when finished. Call `clear` separately when you want to erase the shared in-memory cookies.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `limits` | `CookieLimits` | For custom limits | Jar storage limits and/or worker processing limits; configure both when required. |
| `store` | `S: CookieStore + 'static` or `Arc<dyn CookieStore>` | Yes | Provider owned/shared by the client. Installation replaces the previous store. |
| `new`, getters, `clear`, `validate` | None beyond receiver | — | `new` creates independent empty state; `clear` affects every clone. |

Every field below is public, measured in bytes except the count, and must be greater than zero. No smaller SDK maximum is imposed by `validate`.

| `CookieLimits` field | Type | Default | Use |
| --- | --- | --- | --- |
| `max_cookies` | `usize` | 1024 | Maximum unexpired entries in the jar. |
| `max_cookie_bytes` | `usize` | 16,384 | One Set-Cookie item and derived stored field bounds; stored domains are additionally capped at 253 bytes. |
| `max_set_cookie_bytes` | `usize` | 65,536 | Prefix budget across Set-Cookie values in one response. |
| `max_cookie_header_bytes` | `usize` | 65,536 | Generated/explicit outgoing Cookie header budget. |

### Callback parameters

The installed provider receives the two calls described under `CookieStore` below; no callback is passed directly to these constructors.

### Return values

`new`/`Default` return an empty jar; `with_limits` returns a validated jar; `limits` returns a copy. `clear` and `validate` return `Ok(())`. Fallible methods return `Err(NetError)`. Client store setters return configuration; `with_cookie_limits` validates and returns `Result`.

### Errors and behavior

Zero limits yield `InvalidConfig` with a `cookie_limits.*` field. Lock failure produces `ProviderFailed`; allocation/resource failure may fail jar operations. New jars and restored jars are independent; `CookieJar::clone`, shared `Arc` installation and cloned configurations intentionally share state. A client shutdown does not erase a retained jar.

`with_cookie_limits` bounds worker header processing; it does **not** change an installed jar's storage limits. Use `CookieJar::with_limits` too. At capacity, new entries are ignored while replacements/deletions still work. Invalid or individually oversized Set-Cookie values are ignored. The response-prefix budget includes invalid and oversized values and stops further processing when exhausted. These omissions do not discard the HTTP response. An oversized outgoing Cookie header fails with `ItemTooLarge/RequestBuild` before sending.

The jar enforces domain/path, Secure, expiry, prefix and SameSite rules. Secure cookies require HTTPS; `SameSite=None` requires Secure. Public-suffix domain cookies are rejected. The jar sorts outgoing cookies by descending path length, preserving creation order on ties; replacement keeps the original position. Explicit request Cookie overrides a common Cookie header; either explicit source suppresses jar injection for that hop, but response cookies still update the store. Retry reconstructs its Cookie header after the previous response updates the jar.

### Example

This program creates and closes a client without sending network traffic. Replace the comment with calls to your server to exercise session cookies.

```rust
use open_net::api::http::{CookieJar, CookieLimits, HttpClient, HttpClientConfig};
use open_net::NetError;

#[tokio::main]
async fn main() -> Result<(), NetError> {
    rustls_openssl::default_provider()
        .install_default()
        .expect("install the application TLS provider once");
    let limits = CookieLimits { max_cookies: 100, ..CookieLimits::default() };
    limits.validate()?;
    let jar = CookieJar::with_limits(limits)?;
    assert_eq!(jar.limits().max_cookies, 100);
    let config = HttpClientConfig::new("https://example.com/")?
        .with_cookie_store(jar.clone()).with_cookie_limits(limits)?;
    let client = HttpClient::new(config, "cookie-example")?;
    // Send your login and authenticated requests here using the same client.
    client.shutdown().await?;
    jar.clear()?;
    Ok(())
}
```

## Request site context

### Required features

`http-client-cookies`.

### Function signature

```text
CookieRequestContext::same_site() -> Self
CookieRequestContext::initiated_by(site: Url, top_level_navigation: bool) -> Self
HttpRequestBuilder::cookie_context(self, context: CookieRequestContext) -> Self
HttpRequestTrait::cookie_context(&self) -> CookieRequestContext
```

### Steps

Use the default context for a native API request. If you are representing an initiating web site, parse its URL and attach an explicit context to the request. For trait requests, override `cookie_context` in your implementation; see [callback requests](callback_requests.md).

### Parameters

| Parameter / public field | Type | Required | Description |
| --- | --- | --- | --- |
| `site` constructor argument | `Url` | For `initiated_by` | Initiating site, compared using scheme and registrable domain/IP. |
| `site` field | `Option<Url>` | No | `None` means the normal same-site API context. |
| `top_level_navigation` | `bool` | For `initiated_by` | Default `false`; use `true` only for a represented top-level navigation. |
| `context` | `CookieRequestContext` | For builder override | Owned replacement context. |

### Callback parameters

The worker passes the chosen context to `CookieStore::cookies_for_request`; `HttpRequestTrait::cookie_context` is queried by the SDK.

### Return values

Constructors and trait method return `CookieRequestContext`; builder setter returns the builder. No Result is returned by these methods. URL parsing has its own parse error before construction.

### Errors and behavior

`same_site` and `Default` mean `site: None, top_level_navigation: false`. Strict cookies require same-site. Lax cookies also allow a known cross-site top-level navigation using GET, HEAD, OPTIONS or TRACE. Absent SameSite is permitted according to native RFC6265 semantics; it is not implicitly converted to browser Lax. An unknown site relation does not qualify for Strict/Lax. In particular, an explicit `initiated_by` URL using `localhost` or an intranet domain with an unknown public suffix can produce Unknown even when it matches the target URL; equal URL strings do not guarantee a known registrable site. IP literals are compared as IP sites with their schemes. Use `same_site` only when the application actually intends the native same-site context. This context is an application declaration, not a browser environment detector.

### Example

```rust
use open_net::api::http::{CookieRequestContext, HttpRequest, Url};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = CookieRequestContext::initiated_by(
        Url::parse("https://www.example.org/")?, false,
    );
    assert!(!context.top_level_navigation);
    let _request = HttpRequest::builder().path("/account")
        .cookie_context(context).build()?;
    let _native = CookieRequestContext::same_site();
    Ok(())
}
```

## Snapshot, serialize and restore

### Required features

`http-client-cookies`; the example also declares `serde_json`.

### Function signature

```text
CookieJar::snapshot(&self) -> Result<CookieJarSnapshot, NetError>
CookieJar::from_snapshot(snapshot: CookieJarSnapshot) -> Result<Self, NetError>
```

`CookieJarSnapshot` implements `Clone`, `Serialize`, `Deserialize` and redacted `Debug`. Its fields and validation are private; create it through `snapshot` or a deserializer.

### Steps

Take a snapshot, serialize it on an application worker, and save the bytes using your storage policy. On startup, bound the input bytes before deserializing, restore a fresh jar, and install that jar. You choose the file location, access permissions, encryption, atomic replacement and retention policy; the HTTP worker performs no durable I/O.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `snapshot` | `CookieJarSnapshot` | For restore | Owned, versioned payload; all records are validated before state is installed. |
| Snapshot receiver | `&CookieJar` | For export | Jar whose unexpired persistent cookies are exported in creation order. |

### Callback parameters

None.

### Return values

`Ok(CookieJarSnapshot)` on export; `Ok(CookieJar)` with fresh independent state on restore; `Err(NetError)` for jar/resource or validation failure. Serialization/deserialization return their serializer's error type, such as `serde_json::Error`.

### Errors and behavior

Session cookies are excluded. Expiry is an absolute UTC timestamp and is not restarted on restore; expired records are dropped. Schema version must be 1. Unknown fields, duplicate cookie keys, invalid values, scopes, prefixes or dates are rejected. Decoding and restoration use **default limits**, including when the exporting jar used larger limits. Thus a snapshot from an oversized custom jar may not round-trip under default bounds. The format contains actual cookie secrets despite redacted Debug. Keep serialized data out of logs. Bound raw input because a deserializer may allocate before field visitors run.

### Example

This complete offline program demonstrates the persistence payload and restore boundary in memory. Save the `bytes` using your own file/database worker after serialization; load a bounded byte buffer before the decode step. No real session secrets or files are involved here.

```rust
use open_net::api::http::{
    CookieJar, CookieJarSnapshot, CookieRequestContext, CookieStore,
    HeaderMap, HeaderValue, Method, Url,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let jar = CookieJar::new();
    let url = Url::parse("https://example.com/")?;
    let mut headers = HeaderMap::new();
    headers.append("set-cookie", HeaderValue::from_static(
        "demo=one; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age=3600"));
    jar.set_response_cookies(&url, &headers)?;
    let bytes = serde_json::to_vec(&jar.snapshot()?)?;
    let maximum_file_bytes = 1024 * 1024;
    if bytes.len() > maximum_file_bytes {
        return Err(std::io::Error::other("snapshot exceeds application limit").into());
    }
    let snapshot: CookieJarSnapshot = serde_json::from_slice(&bytes)?;
    let restored = CookieJar::from_snapshot(snapshot)?;
    assert!(restored.cookies_for_request(
        &url, &Method::GET, &CookieRequestContext::same_site())?.is_some());
    jar.clear()?;
    // Restoration creates independent state; clearing the original leaves it intact.
    assert!(restored.cookies_for_request(
        &url, &Method::GET, &CookieRequestContext::same_site())?.is_some());
    restored.clear()?;
    Ok(())
}
```

## Custom `CookieStore`

### Required features

`http-client-cookies`.

### Function signature

```text
pub type CookieStoreResult<T> = Result<T, NetError>;
pub trait CookieStore: Send + Sync + 'static {
    fn cookies_for_request(&self, url: &Url, method: &Method,
        context: &CookieRequestContext) -> CookieStoreResult<Option<HeaderValue>>;
    fn set_response_cookies(&self, url: &Url, headers: &HeaderMap) -> CookieStoreResult<()>;
}
```

### Steps

Implement both bounded, non-blocking synchronous methods, then install by value or `Arc`. Prefer delegating standards compliance to `CookieJar`. Keep filesystem/network persistence outside these methods.

### Parameters

No closure arguments at installation; the trait methods borrow the provider with `&self`. `Send + Sync + 'static` allows shared worker access without temporary borrowed state.

### Callback parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `url` | `&Url` | Actual request/response hop URL, which may differ from the original base URL. |
| `method` | `&Method` | Current request method after redirects. |
| `context` | `&CookieRequestContext` | Application-supplied site/navigation context. |
| `headers` | `&HeaderMap` | Response headers with bounded Set-Cookie input. |

### Return values

`cookies_for_request`: `Ok(Some(HeaderValue))` sends a Cookie header; `Ok(None)` sends none. `set_response_cookies`: `Ok(())` means processing completed. Both may return `Err(NetError)`.

### Errors and behavior

Custom stores must enforce domain, path, Secure, expiry and SameSite themselves. Worker calls catch unwinding panics and sanitize arbitrary provider errors to `ProviderFailed/Provider`. Only outgoing header-size `ItemTooLarge/RequestBuild` retains that classification. Synchronous provider calls cannot be preempted; cancellation is checked around them, so blocking them delays cleanup.

Each response hop updates the store once before redirect/auth/retry decisions and before exposing streaming headers. The [redirect policy](redirects.md) defaults to at most ten followed hops on the original origin and blocks HTTPS-to-HTTP redirects in every HTTP version mode. An explicitly allowed cross-origin redirect removes explicit sensitive headers, while the jar is reevaluated for the destination's allowed cookies. The store still processes the received response when redirects are disabled or the next target is rejected, but no cookies are requested for a rejected destination. Redirect policy rejections are not automatically retried. Cancellation remains a normal HTTP operation error. Installing a Cookie store does not change the redirect implementation, and request paths still cannot be absolute URLs.

### Example

```rust
use open_net::api::http::{
    CookieJar, CookieRequestContext, CookieStore, CookieStoreResult,
    HeaderMap, HeaderValue, HttpClientConfig, Method, Url,
};
use open_net::NetError;
use std::sync::Arc;

struct Store { jar: CookieJar }
impl CookieStore for Store {
    fn cookies_for_request(&self, url: &Url, method: &Method,
        context: &CookieRequestContext) -> CookieStoreResult<Option<HeaderValue>> {
        self.jar.cookies_for_request(url, method, context)
    }
    fn set_response_cookies(&self, url: &Url, headers: &HeaderMap) -> CookieStoreResult<()> {
        self.jar.set_response_cookies(url, headers)
    }
}
fn main() -> Result<(), NetError> {
    let store: Arc<dyn CookieStore> = Arc::new(Store { jar: CookieJar::new() });
    HttpClientConfig::new("https://example.com/")?
        .with_cookie_store_arc(store).validate()
}
```


## Callback request site context example

This complete offline program overrides the trait provider and calls it directly to inspect the result. When submitting the same implementation through `send*`, the worker queries this context for Cookie selection. The remaining trait methods provide a complete compilable implementation; its terminal callback receives the result after a submitted request completes.

```rust
use open_net::api::http::{
    CookieRequestContext, HttpRequestMethod, HttpRequestTrait, HttpResponseResult, Url,
};
use open_net::async_trait;

struct SiteRequest { site: Url }
#[async_trait]
impl HttpRequestTrait for SiteRequest {
    fn get_path(&self) -> String { "/account".to_owned() }
    fn get_method(&self) -> HttpRequestMethod { HttpRequestMethod::GET }
    fn cookie_context(&self) -> CookieRequestContext {
        CookieRequestContext::initiated_by(self.site.clone(), true)
    }
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        println!("callback completed: {}", result.is_ok());
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let request = SiteRequest { site: Url::parse("https://www.example.org/")? };
    let context = request.cookie_context();
    assert!(context.top_level_navigation);
    assert_eq!(context.site.as_ref().and_then(Url::host_str), Some("www.example.org"));
    Ok(())
}
```


## `CookieJar::new()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn new() -> Self
```

### Steps

Create an independent empty jar, then install it or use its `CookieStore` methods directly.

### Parameters

None.

### Callback parameters

None during construction.

### Return values

An empty jar using `CookieLimits::default()`.

### Errors and behavior

Construction returns directly and performs no durable I/O. `clone` shares this jar's state; another `new` creates independent state.

### Example

See the complete [snapshot and restore example](#snapshot-serialize-and-restore), which inserts a cookie, selects it, serializes a snapshot and checks independent restored state.


## `CookieJar::with_limits()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn with_limits(limits: CookieLimits) -> Result<Self, NetError>
```

### Steps

Choose positive storage limits, validate them by constructing the jar, then install that jar.

### Parameters

`limits` is an owned `CookieLimits` value controlling this jar's storage and Cookie processing.

### Callback parameters

None during construction.

### Return values

`Ok(CookieJar)` with independent empty state or `Err(NetError)`.

### Errors and behavior

Zero values fail with `InvalidConfig`. These jar limits do not change the client worker's limits; also configure `with_cookie_limits` when necessary.

### Example

See the complete [jar setup example](#create-limit-and-install-a-cookiejar), including custom limits, installation, shutdown and clearing shared state.


## `CookieJar::limits()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn limits(&self) -> CookieLimits
```

### Steps

Read the configured limits when reporting or checking application policy.

### Parameters

Only `&self`.

### Callback parameters

None.

### Return values

A copy of this jar's `CookieLimits`.

### Errors and behavior

Does not expose mutable settings or the client worker's limits. Editing the returned copy leaves the jar unchanged.

### Example

See the complete [jar setup example](#create-limit-and-install-a-cookiejar), including custom limits, installation, shutdown and clearing shared state.


## `CookieJar::clear()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn clear(&self) -> Result<(), NetError>
```

### Steps

Retain the jar as long as needed, then explicitly clear it when application policy requires discarding its cookies.

### Parameters

Only `&self`.

### Callback parameters

None.

### Return values

`Ok(())` after clearing or `Err(NetError)`.

### Errors and behavior

Clears shared in-memory state for every clone. Client shutdown does not call this automatically. An independently restored jar is unaffected; lock failure produces `ProviderFailed`.

### Example

See the complete [jar setup example](#create-limit-and-install-a-cookiejar), including custom limits, installation, shutdown and clearing shared state.


## `CookieLimits::validate()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn validate(&self) -> Result<(), NetError>
```

### Steps

Validate custom limits before using them in a jar or client configuration.

### Parameters

Only `&self`; all four public fields must be greater than zero.

### Callback parameters

None.

### Return values

`Ok(())` for positive limits or `Err(NetError)`.

### Errors and behavior

Zero yields `InvalidConfig` identifying the `cookie_limits.*` field. Validation imposes no smaller SDK maximum and does not reserve capacity or mutate a jar.

### Example

See the complete [jar setup example](#create-limit-and-install-a-cookiejar), including custom limits, installation, shutdown and clearing shared state.


## `HttpClientConfig::with_cookie_store()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn with_cookie_store<S>(self, store: S) -> Self
where S: CookieStore + 'static
```

### Steps

Move a jar or custom provider into the configuration before client construction.

### Parameters

`store` is an owned `Send + Sync + 'static` provider; `self` is the consumed configuration.

### Callback parameters

The worker later calls `cookies_for_request` and `set_response_cookies`.

### Return values

The updated configuration.

### Errors and behavior

Replaces the previous provider. Installing a cloned `CookieJar` intentionally shares its cookies. Enabling the Cargo feature alone does not install a provider.

### Example

See the complete [jar setup example](#create-limit-and-install-a-cookiejar), including custom limits, installation, shutdown and clearing shared state.


## `HttpClientConfig::with_cookie_store_arc()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn with_cookie_store_arc(self, store: Arc<dyn CookieStore>) -> Self
```

### Steps

Wrap a store in `Arc` when sharing is intentional, then install it before client construction.

### Parameters

`store` is the shared Cookie provider; `self` is the consumed configuration.

### Callback parameters

The worker later calls the two synchronous `CookieStore` hooks.

### Return values

The updated configuration.

### Errors and behavior

Replaces the previous provider. The store may be used concurrently and its hooks must remain bounded and non-blocking.

### Example

See the complete [custom store example](#custom-cookiestore), which delegates both hooks to a jar and installs a shared provider.


## `HttpClientConfig::with_cookie_limits()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn with_cookie_limits(self, limits: CookieLimits) -> Result<Self, NetError>
```

### Steps

Choose positive worker limits and apply them while constructing the client configuration.

### Parameters

`limits` contains the worker's Cookie processing bounds; `self` is the consumed configuration.

### Callback parameters

None.

### Return values

`Ok(HttpClientConfig)` with the limits or `Err(NetError)`.

### Errors and behavior

Zero values fail with `InvalidConfig`. This setter does not reconfigure storage limits on an installed jar. An oversized outgoing Cookie header later fails with `ItemTooLarge/RequestBuild`.

### Example

See the complete [jar setup example](#create-limit-and-install-a-cookiejar), including custom limits, installation, shutdown and clearing shared state.


## `CookieRequestContext::same_site()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn same_site() -> Self
```

### Steps

Use this context for an application-declared native same-site API request.

### Parameters

None.

### Callback parameters

None.

### Return values

`CookieRequestContext { site: None, top_level_navigation: false }`.

### Errors and behavior

No browser environment is detected. Declaring this context permits same-site selection; choose an explicit initiating site when representing a web navigation.

### Example

See the complete [site-context example](#request-site-context), which constructs both contexts and installs an explicit context with the request builder.


## `CookieRequestContext::initiated_by()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn initiated_by(site: Url, top_level_navigation: bool) -> Self
```

### Steps

Parse the actual initiating URL, declare whether the operation represents top-level navigation, then attach the context.

### Parameters

`site` is the initiating URL; `top_level_navigation` indicates the navigation type.

### Callback parameters

None.

### Return values

An owned context with `site: Some(site)` and the supplied navigation flag.

### Errors and behavior

Constructor returns directly; URL parsing may fail beforehand. Same-site comparison uses scheme and a known registrable domain or IP. Explicit `localhost`/unknown suffix sites can be Unknown even when URLs match, which excludes Strict/Lax cookies.

### Example

See the complete [site-context example](#request-site-context), which constructs both contexts and installs an explicit context with the request builder.


## `HttpRequestBuilder::cookie_context()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn cookie_context(self, context: CookieRequestContext) -> Self
```

### Steps

Construct the context, attach it to the builder, then call `build` and submit the request.

### Parameters

`context` is the owned replacement context; `self` is the consumed builder.

### Callback parameters

The installed store later receives this context in `cookies_for_request`.

### Return values

The updated builder.

### Errors and behavior

Replaces the request's default context. Does not install a store or create a Cookie header immediately; normal request validation remains in `build` and dispatch.

### Example

See the complete [site-context example](#request-site-context), which constructs both contexts and installs an explicit context with the request builder.


## `HttpRequestTrait::cookie_context()`

### Required features

`http-client-cookies`.

### Function signature

```text
fn cookie_context(&self) -> CookieRequestContext
```

### Steps

Override the provider on a callback request when it represents an explicit initiating site.

### Parameters

`&self` borrows the callback request implementation.

### Callback parameters

The SDK queries this no-argument provider while processing the request; it is not a response callback.

### Return values

The context used for Cookie selection. The default is the native same-site context.

### Errors and behavior

Must return the intended context consistently across attempts. It does not bypass domain, Secure, expiry or SameSite rules and has an effect only when a store is installed.

### Example

See the complete [callback request context example](#callback-request-site-context-example), which implements the provider and calls `request.cookie_context()` directly.


## `CookieJar::snapshot()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn snapshot(&self) -> Result<CookieJarSnapshot, NetError>
```

### Steps

Export a snapshot, then serialize and persist it on an application worker under your storage policy.

### Parameters

Only `&self`.

### Callback parameters

None.

### Return values

`Ok(CookieJarSnapshot)` containing unexpired persistent entries in creation order, or `Err(NetError)`.

### Errors and behavior

Session cookies are omitted. The payload contains secrets despite redacted Debug; the SDK performs no disk I/O. Lock or resource failures may fail export.

### Example

See the complete [snapshot and restore example](#snapshot-serialize-and-restore), which inserts a cookie, selects it, serializes a snapshot and checks independent restored state.


## `CookieJar::from_snapshot()`

### Required features

`http-client-cookies`.

### Function signature

```text
pub fn from_snapshot(snapshot: CookieJarSnapshot) -> Result<Self, NetError>
```

### Steps

Bound raw input bytes before deserializing, then validate and restore a new jar from the decoded snapshot.

### Parameters

`snapshot` is the owned, versioned payload produced by export or deserialization.

### Callback parameters

None.

### Return values

`Ok(CookieJar)` with fresh independent state, or `Err(NetError)`.

### Errors and behavior

Restores under default limits, even when export used larger limits. Invalid versions, records, duplicates, scopes or prefixes are rejected; expired records are discarded. Expiry remains absolute and session cookies are not restored.

### Example

See the complete [snapshot and restore example](#snapshot-serialize-and-restore), which inserts a cookie, selects it, serializes a snapshot and checks independent restored state.


## `CookieStore::cookies_for_request()`

### Required features

`http-client-cookies`.

### Function signature

```text
fn cookies_for_request(&self, url: &Url, method: &Method,
    context: &CookieRequestContext) -> CookieStoreResult<Option<HeaderValue>>
```

### Steps

Implement bounded synchronous selection for the actual hop; prefer delegating to `CookieJar` for standard matching.

### Parameters

`&self` borrows the thread-safe store.

### Callback parameters

`url` is the current hop URL; `method` is its method after redirects; `context` is the request's declared site/navigation context.

### Return values

`Ok(Some(HeaderValue))` supplies a Cookie header; `Ok(None)` supplies none; `Err(NetError)` fails the operation.

### Errors and behavior

Custom providers must enforce domain, path, Secure, expiry and SameSite. Worker invocation sanitizes provider failures/panics to `ProviderFailed/Provider`; outgoing size failures retain `ItemTooLarge/RequestBuild`. Explicit request/common Cookie headers suppress injection for that hop.

### Example

See the complete [snapshot and restore example](#snapshot-serialize-and-restore), which inserts a cookie, selects it, serializes a snapshot and checks independent restored state.


## `CookieStore::set_response_cookies()`

### Required features

`http-client-cookies`.

### Function signature

```text
fn set_response_cookies(&self, url: &Url, headers: &HeaderMap) -> CookieStoreResult<()>
```

### Steps

Implement bounded synchronous processing of response cookies for this hop; keep persistence outside the hook.

### Parameters

`&self` borrows the thread-safe store.

### Callback parameters

`url` is the actual response hop URL; `headers` supplies bounded Set-Cookie input.

### Return values

`Ok(())` after processing or `Err(NetError)`.

### Errors and behavior

Called once per response hop before redirect/auth/retry decisions and before streaming headers are exposed. The built-in jar ignores invalid or oversized items subject to the response-prefix budget. Synchronous provider work cannot be preempted; blocking delays cancellation and cleanup.

### Example

See the complete [snapshot and restore example](#snapshot-serialize-and-restore), which inserts a cookie, selects it, serializes a snapshot and checks independent restored state.

[HTTP guide index](README.md)

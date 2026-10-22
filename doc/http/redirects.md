# Transport security and HTTP redirects

`HttpTransportSecurity` and `RedirectPolicy` are introduced in 0.3.3. The
0.3.2 release does not provide these configurable policies or all of the
guarantees below; its HTTP/2-only mode already has separate HTTPS restrictions.

[HTTP guide](README.md) · [Configuration](configuration.md)

Required feature: `http-client`. These policies do not require HTTP/2, auth, or
Cookie features. All public types below are re-exported by
`open_net::api::http`. Configuration examples run offline and need no TLS provider
until a client is constructed; see [TLS setup](tls_and_proxy.md) for client setup.

Every HTTP client now prevents HTTPS-to-HTTP redirects by default, follows only
the original request's origin, and follows at most ten redirects per attempt.
The rules apply to buffered requests, callback requests, stream opening, and SSE
opening. SSE opens a GET request without an application request body.

Every complete program on this page uses these dependencies:

```toml
[dependencies]
open-net = { version = "0.3.3", default-features = false, features = ["http-client"] }
```

## Independent policies

`HttpTransportSecurity` sets transport requirements:

| Variant | Initial request | Redirect targets |
| --- | --- | --- |
| `PreventDowngrade` (default) | HTTP or HTTPS | Never allow HTTPS-to-HTTP, including after an earlier HTTP-to-HTTPS upgrade |
| `HttpsOnly` | HTTPS required | HTTPS required |

`RedirectPolicy` authorizes destinations and limits the chain:

| API | Behavior |
| --- | --- |
| `RedirectPolicy::default()` / `same_origin() -> Self` | Allow only the original request's origin; maximum ten followed redirects |
| `RedirectPolicy::none() -> Self` | Return the redirect response without inspecting or following its `Location` |
| `RedirectPolicy::allow_origins(origins) -> Result<Self, NetError>` | Allow the original origin and the listed origins; maximum ten followed redirects |
| `with_max_redirects(self, count: usize) -> Result<Self, NetError>` | Replace the limit with a value in `1..=128`; does not enable a disabled policy |
| `is_enabled(&self) -> bool` | Inspect whether following is enabled |
| `max_redirects(&self) -> usize` | Inspect the configured limit |

`allow_origins` accepts an `IntoIterator` of values implementing `AsRef<str>`,
including a string array. Each value must be a bare absolute HTTP or HTTPS origin,
optionally ending in `/`. Credentials, non-root paths, queries, fragments,
backslashes, surrounding whitespace, and control characters are rejected.
Inputs are limited to 2048 bytes each and 128 distinct origins in total.
Duplicates are ignored; an empty list behaves like `same_origin()`.

An origin is the scheme, normalized host, and effective port.
`https://example.com` and `https://example.com:443/` identify the same origin;
another port, a subdomain, or HTTP is a different origin. No wildcard or suffix
matching is performed. Each hop is checked against the original origin and the
same explicit allowlist: allowing B does not let B redirect to an unlisted C.
An allowlist never bypasses transport security. HTTP-to-HTTPS upgrades also
change origin and need an explicit allowed destination.

Install the policies on client configuration:

```text
HttpClientConfig::with_redirect_policy(self, policy: RedirectPolicy) -> Self
HttpClientConfig::redirect_policy(&self) -> &RedirectPolicy
HttpClientConfig::with_transport_security(self, security: HttpTransportSecurity) -> Result<Self, NetError>
HttpClientConfig::transport_security(&self) -> HttpTransportSecurity
```

The transport setter rejects `HttpsOnly` with an HTTP base URL. Client creation
and `validate()` check the combination again, including a directly mutated
public `base_url`. Configuration failures return `InvalidConfig/Configuration`;
inspect `config_error().field()` for `http.transport_security`,
`http.redirect.allowed_origins`, or `http.redirect.max_redirects`.
Fallible consuming setters do not return the input configuration on failure.

HTTP version selection remains independent. `Auto` and `Http1Only` use these
same policies; [`Http2Only`](http2.md) additionally requires HTTPS and HTTP/2.
An explicit HTTP proxy can carry an HTTPS request through CONNECT because the
policy checks the request target, not the proxy connection.

## Methods and bodies

Once the next target is authorized, the redirect status controls the next
request's method and body:

| Status | Next method | Next body |
| --- | --- | --- |
| 301 / 302 with POST | GET | Removed |
| 301 / 302 with another method | Unchanged | Preserved |
| 303 | GET, except HEAD remains HEAD | Removed |
| 307 / 308 | Unchanged | Preserved |

Removing a body also removes its content length, type, encoding, and transfer
encoding headers. A later 307/308 cannot restore a body or method dropped by
an earlier hop.

Allowlisting another origin authorizes any method and body preserved by these
rules. Only list destinations trusted to receive that content. Removing
Authorization and Cookie headers does not remove private information from a
request body.

Concrete requests own buffered `Bytes`, which can be replayed. Callback requests
can set `HttpRequestOptions::with_replayable_body(false)`. If following an
authorized redirect would preserve such a body, the SDK returns the received
3xx without another send. This also applies to non-POST bodies preserved by
301/302. A redirect that drops the body can still be followed.

## One redirect lifecycle

The SDK disables reqwest's automatic redirects and processes each received
response in one shared handler, regardless of installed providers. Within the
existing attempt, that handler:

1. Checks cancellation and deadlines and updates the Cookie store once for the
   received response, when a store is installed.
2. Resolves `Location`, checks transport security, original-origin/allowlist
   authorization, and the hop limit before preparing the next request.
3. Applies the method/body rules and checks body replayability.
4. Removes sensitive headers when the origin changes, reapplies permitted auth,
   and obtains cookies for the authorized destination.
5. Executes the next request under the same cancellation signal and remaining
   header, attempt, and total budgets.

Common headers are merged by the SDK, with no second copy installed as reqwest
defaults. A credential removed on a cross-origin hop cannot be reinserted by
transport defaults. The auth provider stays disabled for the rest of that chain
after a cross-origin hop, even when a later hop returns to the original origin.
Cookie selection still follows the destination's domain/path/Secure/SameSite
rules. See [authentication](authentication.md) and [cookies](cookies.md).

Redirects are part of an attempt, not retries. Disabling retries does not disable
redirects. Middleware surrounds the logical request; it is not invoked again for
each hop.

## Outcomes and errors

Disabling redirects returns the original response, including an HTTP `Location`
on an HTTPS response. No connection to that target is attempted. Missing
`Location` also leaves the 3xx unchanged. An undecodable header value, invalid
URL, unsupported scheme, or embedded credentials returns an error when following
is enabled.

Policy failures use `ErrorKind::Protocol` at `ErrorStage::Receive`, retaining the
request ID and attempt. Their source is the public, non-exhaustive
`HttpRedirectError`:

| Reason | Meaning |
| --- | --- |
| `InsecureTarget` | Target violates transport security |
| `OriginNotAllowed` | Target is neither the original origin nor explicitly allowed |
| `TooManyRedirects` | Following another redirect would exceed the configured limit |
| `InvalidLocation` | Header or target cannot be parsed, or target has an unsupported scheme or credentials |

These reasons contain no URL, headers, or body, including when formatted or
inspected through `source()`. `RedirectPolicy` Debug reports configuration
metadata rather than listed origins. No redirect rejection triggers automatic
retry, even if a retry policy explicitly enables protocol transport errors.

```rust
use open_net::api::http::HttpRedirectError;
use open_net::NetError;
use std::error::Error;

fn redirect_reason(error: &NetError) -> Option<&HttpRedirectError> {
    error.source()?.downcast_ref::<HttpRedirectError>()
}

fn main() {}
```

An unfollowed 3xx is still an HTTP response. Inspect its status or use
`HttpStatusPolicy::Success2xx`; `Ok(response)` alone does not imply that the
redirect was followed or the application operation succeeded.

## Migration examples

Applications that previously relied on automatic cross-origin redirects must
enumerate trusted destinations. Include the effective port used by the service:

```rust
use open_net::api::http::{HttpClientConfig, HttpTransportSecurity, RedirectPolicy};
use open_net::NetError;

fn main() -> Result<(), NetError> {
    let redirects = RedirectPolicy::allow_origins([
        "https://downloads.example.com",
        "https://archive.example.com:8443",
    ])?
    .with_max_redirects(5)?;
    let config = HttpClientConfig::new("https://api.example.com/")?
        .with_transport_security(HttpTransportSecurity::HttpsOnly)?
        .with_redirect_policy(redirects);
    config.validate()?;
    if config.redirect_policy().max_redirects() != 5
        || config.transport_security() != HttpTransportSecurity::HttpsOnly
    {
        return Err(NetError::protocol("redirect configuration changed"));
    }
    Ok(())
}
```

To inspect redirect responses and decide what to do in application code:

```rust
use open_net::api::http::{HttpClientConfig, RedirectPolicy};
use open_net::NetError;

fn main() -> Result<(), NetError> {
    let config = HttpClientConfig::new("https://api.example.com/")?
        .with_redirect_policy(RedirectPolicy::none());
    if config.redirect_policy().is_enabled() {
        return Err(NetError::protocol("redirects should be disabled"));
    }
    let same_origin = RedirectPolicy::same_origin();
    if same_origin.max_redirects() != 10 {
        return Err(NetError::protocol("unexpected default redirect limit"));
    }
    Ok(())
}
```

Existing initial HTTP configurations, such as a local development server,
continue to work under `PreventDowngrade`. There is no option to re-enable
HTTPS-to-HTTP redirect following. This change does not introduce a plaintext
Basic/Bearer credential policy or change WebSocket transport policy.

Provider-backed 301/302 redirects now preserve non-POST methods and bodies,
matching the unified behavior above. Review endpoints that previously depended
on an auth/Cookie provider causing PUT or PATCH to become GET. The separate
replayability declaration still applies.

The shared handler does not synthesize a `Referer` from a previous request URL.
Applications that previously received this header from reqwest's automatic
redirects should account for its absence. Explicit caller-supplied headers
continue to follow the normal header and sensitive-header rules.

## `RedirectPolicy::none()`

### Required features

`http-client`.

### Function signature

```text
RedirectPolicy::none() -> Self
```

### Steps

Disable automatic redirect following before constructing a client.

### Parameters

None.

### Callback parameters

None.

### Return values

A disabled redirect policy.

### Errors and behavior

Returns the received 3xx without following or validating Location. Does not disable request retries.

### Example

See the complete [configuration examples](#migration-examples).

## `RedirectPolicy::same_origin()`

### Required features

`http-client`.

### Function signature

```text
RedirectPolicy::same_origin() -> Self
```

### Steps

Use the default origin-restricted redirect policy.

### Parameters

None.

### Callback parameters

None.

### Return values

An enabled policy with at most ten followed hops.

### Errors and behavior

Only the original request origin is authorized. HTTPS-to-HTTP downgrades remain forbidden.

### Example

See the complete [configuration examples](#migration-examples).

## `RedirectPolicy::allow_origins()`

### Required features

`http-client`.

### Function signature

```text
RedirectPolicy::allow_origins<I, S>(origins: I) -> Result<Self, NetError>
where I: IntoIterator<Item = S>, S: AsRef<str>
```

### Steps

List explicitly trusted destination origins and install the policy.

### Parameters

`origins`: absolute HTTP/HTTPS origins, at most 128 distinct values, each at most 2048 bytes.

### Callback parameters

None.

### Return values

An enabled policy or InvalidConfig.

### Errors and behavior

Rejects credentials, non-root paths, queries, fragments, backslashes, whitespace and control characters. The original origin remains permitted; transport security still applies.

### Example

See the complete [configuration examples](#migration-examples).

## `RedirectPolicy::with_max_redirects()`

### Required features

`http-client`.

### Function signature

```text
RedirectPolicy::with_max_redirects(self, count: usize) -> Result<Self, NetError>
```

### Steps

Replace the hop budget independently of the retry attempt limit.

### Parameters

`count`: in 1..=128.

### Callback parameters

None.

### Return values

The updated policy or InvalidConfig.

### Errors and behavior

Does not enable a disabled policy. The limit applies within each attempt.

### Example

See the complete [configuration examples](#migration-examples).

## `RedirectPolicy::is_enabled()`

### Required features

`http-client`.

### Function signature

```text
RedirectPolicy::is_enabled(&self) -> bool
```

### Steps

Inspect whether redirect following is enabled.

### Parameters

None.

### Callback parameters

None.

### Return values

The enabled flag.

### Errors and behavior

Read-only; an enabled policy still validates each destination and hop budget.

### Example

See the complete [configuration examples](#migration-examples).

## `RedirectPolicy::max_redirects()`

### Required features

`http-client`.

### Function signature

```text
RedirectPolicy::max_redirects(&self) -> usize
```

### Steps

Inspect the configured hop budget.

### Parameters

None.

### Callback parameters

None.

### Return values

The maximum followed redirects per attempt.

### Errors and behavior

The value does not indicate whether following is enabled.

### Example

See the complete [configuration examples](#migration-examples).

## `HttpClientConfig::with_redirect_policy()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_redirect_policy(self, policy: RedirectPolicy) -> Self
```

### Steps

Install destination authorization and hop limits on client configuration.

### Parameters

`policy`: validated redirect configuration.

### Callback parameters

None.

### Return values

The updated client configuration.

### Errors and behavior

Applies to buffered, callback, streaming and SSE opening, independently of providers and HTTP version.

### Example

See the complete [configuration examples](#migration-examples).

## `HttpClientConfig::redirect_policy()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::redirect_policy(&self) -> &RedirectPolicy
```

### Steps

Read client redirect configuration.

### Parameters

None.

### Callback parameters

None.

### Return values

A borrowed redirect policy.

### Errors and behavior

Read-only; does not expose an in-progress redirect chain.

### Example

See the complete [configuration examples](#migration-examples).

## `HttpClientConfig::with_transport_security()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::with_transport_security(self, security: HttpTransportSecurity) -> Result<Self, NetError>
```

### Steps

Select transport requirements before constructing a client.

### Parameters

`security`: PreventDowngrade or HttpsOnly.

### Callback parameters

None.

### Return values

The updated configuration or InvalidConfig.

### Errors and behavior

HttpsOnly rejects an HTTP base URL. Client validation rechecks the combination; HTTP/2-only independently requires HTTPS. Consuming failures do not return the input configuration.

### Example

See the complete [configuration examples](#migration-examples).

## `HttpClientConfig::transport_security()`

### Required features

`http-client`.

### Function signature

```text
HttpClientConfig::transport_security(&self) -> HttpTransportSecurity
```

### Steps

Read the selected transport requirement.

### Parameters

None.

### Callback parameters

None.

### Return values

The configured transport security value.

### Errors and behavior

The default PreventDowngrade permits explicitly configured initial HTTP while forbidding HTTPS-to-HTTP redirects.

### Example

See the complete [configuration examples](#migration-examples).

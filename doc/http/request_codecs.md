# Query, JSON, form and multipart requests

Required feature: `http-client-codecs` (includes `http-client`). In a consumer project beside this checkout's `open-net` directory:

```toml
[dependencies]
open-net = { path = "../open-net", default-features = false, features = ["http-client-codecs"] }
serde = { version = "1", features = ["derive"] }
```

Adjust the local path. Examples construct requests offline. To send them, use the client from [requests and responses](requests_and_responses.md), with a server that accepts the specified endpoint and media type. Fallible builder and Multipart methods that take `self` consume that builder/form on both success and error; `Err(NetError)` does not return the original object. `HttpRequestBuilder` is not cloneable: retain the original input values to reconstruct it after an error. `Multipart` is cloneable, so clone a form before a fallible call if a retained copy is required. Borrowed serialization input is not consumed. These codecs encode outgoing requests. `HttpResponse::json` already works with basic `http-client`, and sending manually constructed JSON bytes also needs no codecs feature.

## `HttpRequestBuilder::query()`

### Required features

`http-client-codecs`.

### Function signature

```text
pub fn query<T: serde::Serialize>(self, value: &T) -> Result<Self, NetError>
```

### Steps

Define a serializable structure, set a relative path and append one or more query values. Call `build` to obtain the owned request.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `value` | `&T` where `T: serde::Serialize` | Yes | Borrowed value serialized immediately; the request does not retain this reference. |

### Callback parameters

None.

### Return values

`Ok(HttpRequestBuilder)` containing the encoded query or `Err(NetError)` on serialization failure.

### Errors and behavior

Query uses `serde_qs`, unindexed array encoding and maximum nesting depth 5. Existing query parameters remain; successive calls append, without deduplicating keys. Empty output adds no separator. Changing `path` later reapplies query fragments created by `query`. Invalid input yields `InvalidInput` at `RequestBuild`; the client separately checks URL resolution at submission. Do not percent-encode input strings yourself.

### Example

```rust
use open_net::api::http::HttpRequest;
use open_net::NetError;
use serde::Serialize;

#[derive(Serialize)]
struct Search<'a> { term: &'a str, page: u32 }

fn main() -> Result<(), NetError> {
    let request = HttpRequest::builder()
        .path("/search?language=en")
        .query(&Search { term: "red fox", page: 2 })?
        .build()?;
    assert!(request.path.starts_with("/search?language=en&"));
    Ok(())
}
```

## `HttpRequestBuilder::json()`

### Required features

`http-client-codecs`.

### Function signature

```text
pub fn json<T: serde::Serialize>(self, value: &T) -> Result<Self, NetError>
```

### Steps

Create a serializable value, select the HTTP method and endpoint, call `json`, then `build`.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `value` | `&T` where `T: serde::Serialize` | Yes | Serialized synchronously into an owned buffered body. |

### Callback parameters

None.

### Return values

`Ok(HttpRequestBuilder)` or `Err(NetError)`. The builder is consumed even on failure.

### Errors and behavior

Sets `Content-Type: application/json` only if the request map has no Content-Type. An explicitly supplied request Content-Type remains, so use it only when the server accepts that media type. A common client Content-Type is overridden by the generated request header. Serialization errors and a previously selected body produce `InvalidInput/RequestBuild`. Calling `body` after a typed codec defers the conflict error until `build`. Codecs do not select POST automatically.

### Example

```rust
use open_net::api::http::{HttpRequest, Method};
use open_net::NetError;
use serde::Serialize;

#[derive(Serialize)]
struct Item<'a> { name: &'a str, quantity: u32 }

fn main() -> Result<(), NetError> {
    let request = HttpRequest::builder().method(Method::POST).path("/items")
        .json(&Item { name: "pencil", quantity: 3 })?.build()?;
    assert_eq!(request.headers["content-type"], "application/json");
    assert!(!request.body.is_empty());
    Ok(())
}
```

## `HttpRequestBuilder::form()`

### Required features

`http-client-codecs`.

### Function signature

```text
pub fn form<T: serde::Serialize>(self, value: &T) -> Result<Self, NetError>
```

### Steps

Create a serializable value, select the endpoint and method, call `form`, then `build`.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `value` | `&T` where `T: serde::Serialize` | Yes | Form fields, serialized immediately; unindexed arrays, maximum nesting depth 5. |

### Callback parameters

None.

### Return values

`Ok(HttpRequestBuilder)` with a buffered form body or `Err(NetError)`.

### Errors and behavior

Uses form URL encoding, including `+` for spaces, and defaults the request Content-Type to `application/x-www-form-urlencoded`. Explicit request Content-Type is preserved. Serialization/depth/body conflicts return `InvalidInput/RequestBuild`. Query may coexist with any body codec; two body codecs may not coexist, even when the first body is empty.

### Example

```rust
use open_net::api::http::{HttpRequest, Method};
use open_net::NetError;
use serde::Serialize;

#[derive(Serialize)]
struct Form<'a> { message: &'a str }

fn main() -> Result<(), NetError> {
    let request = HttpRequest::builder().method(Method::POST).path("/messages")
        .form(&Form { message: "hello world" })?.build()?;
    assert_eq!(&request.body[..], b"message=hello+world");
    Ok(())
}
```

## Multipart construction example

### Required features

`http-client-codecs`.

### Function signature

```text
Multipart::new() -> Self
Multipart::text(self, name: impl Into<String>, value: impl Into<String>) -> Result<Self, NetError>
Multipart::bytes(self, name: impl Into<String>, value: impl Into<Bytes>) -> Result<Self, NetError>
Multipart::file_bytes(self, name: impl Into<String>, filename: impl Into<String>, value: impl Into<Bytes>) -> Result<Self, NetError>
Multipart::file(self, name: impl Into<String>, path: impl AsRef<Path>) -> Result<Self, NetError>
HttpRequestBuilder::multipart(self, value: Multipart) -> Result<Self, NetError>
```

`Bytes` is re-exported as `open_net::Bytes`; `Path` is `std::path::Path`.

### Steps

Create an empty form, append text/binary/file parts, and pass the completed form to `multipart`. Let the SDK set the multipart boundary and Content-Type. `Multipart::default()` is equivalent to `new`; cloning copies the form and shares any `Bytes` storage.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `new` | None | — | Creates an empty form with a generated boundary. |
| `name` | `impl Into<String>` | Yes for each part | Nonempty field name with no control characters. Repeated names are allowed. |
| Text `value` | `impl Into<String>` | Yes | UTF-8 text, may be empty; part Content-Type is `text/plain; charset=utf-8`. |
| Binary `value` | `impl Into<Bytes>` | Yes | Owned bytes, may be empty; part Content-Type is `application/octet-stream`. |
| `filename` | `impl Into<String>` | Yes for `file_bytes` | Nonempty single path component; no control characters, `/` or `\`. |
| `path` | `impl AsRef<Path>` | Yes for `file` | Existing readable file with a UTF-8 filename; filename is derived from the final path component. |
| Builder `value` | `Multipart` | Yes | Form transferred into a fully encoded request body. |

### Callback parameters

None.

### Return values

`new` returns a form. Part methods return `Ok(Multipart)`; `multipart` returns `Ok(HttpRequestBuilder)`. Fallible methods return `Err(NetError)`.

### Errors and behavior

`file` performs a synchronous full-file read before submission; the encoded multipart body is also buffered. There is no streaming upload interface here and no upload-size bound supplied by these methods. Read large files on an application blocking worker or prefer already-loaded `file_bytes`, and enforce application size limits.

Invalid names, filenames, body conflicts or conflicting Content-Type yield `InvalidInput/RequestBuild`; file read failures yield `Io/RequestBuild` and can expose `io_kind()`. `multipart` rejects an already present request Content-Type even if it looks like multipart. Replacing its generated Content-Type with a different value through `header` fails immediately with `InvalidInput`. Replacing the entire map through `headers` and thereby changing or removing that value is detected later by `build`. The boundary is checked against part data; repeated collisions or random-source failure can fail encoding. An empty form is legal. The SDK escapes quotes and backslashes in part disposition values.

### Example

This uses in-memory parts and a helper for an optional file upload. `from_file` compiles without opening a file until called; it requires the chosen file to exist.

```rust
use open_net::api::http::{HttpRequest, Method, Multipart};
use open_net::{Bytes, NetError};
use std::path::Path;

fn from_file(path: &Path) -> Result<HttpRequest, NetError> {
    let form = Multipart::new().file("attachment", path)?;
    HttpRequest::builder().method(Method::POST).path("/upload")
        .multipart(form)?.build()
}

fn main() -> Result<(), NetError> {
    let form = Multipart::new()
        .text("description", "sample upload")?
        .bytes("raw", Bytes::from_static(&[0, 1, 255]))?
        .file_bytes("attachment", "note.txt", Bytes::from_static(b"hello"))?;
    let request = HttpRequest::builder().method(Method::POST).path("/upload")
        .multipart(form)?.build()?;
    assert!(request.headers["content-type"].to_str().unwrap()
        .starts_with("multipart/form-data; boundary="));
    let _file_builder: fn(&Path) -> Result<HttpRequest, NetError> = from_file;
    Ok(())
}
```

## Individual function reference

The examples linked below are complete programs with the dependency declaration at the top of this page. Each reference identifies the call that demonstrates the function.

## `Multipart::new()`

### Required features

`http-client-codecs`.

### Function signature

```text
Multipart::new() -> Self
```

### Steps

Create an empty form, append parts and transfer it to the request builder.

### Parameters

None.

### Callback parameters

None.

### Return values

An empty owned `Multipart` form with a generated boundary.

### Errors and behavior

Infallible; a random-source failure during construction uses a fallback boundary. Encoding later checks data collisions and may fail. `Default` is equivalent; cloning preserves parts and shares `Bytes` storage.

### Example

See the [complete multipart program](#multipart-construction-example), which starts with `Multipart::new()` in both `main` and `from_file`.

## `Multipart::text()`

### Required features

`http-client-codecs`.

### Function signature

```text
Multipart::text(self, name: impl Into<String>, value: impl Into<String>) -> Result<Self, NetError>
```

### Steps

Append a named text value and retain the returned form before calling `multipart`.

### Parameters

`name` is required, nonempty and must contain no control characters. Required `value` is owned UTF-8 text; empty is allowed. Repeated names are allowed.

### Callback parameters

None.

### Return values

`Ok(Self)` with an appended text part, or `Err(NetError)`.

### Errors and behavior

An invalid name returns `InvalidInput/RequestBuild`. The part uses `text/plain; charset=utf-8`. The original form is consumed on error; clone beforehand if it must be retained.

### Example

See the [complete multipart program](#multipart-construction-example), which calls `.text("description", "sample upload")`.

## `Multipart::bytes()`

### Required features

`http-client-codecs`.

### Function signature

```text
Multipart::bytes(self, name: impl Into<String>, value: impl Into<Bytes>) -> Result<Self, NetError>
```

### Steps

Supply an in-memory binary value, retain the returned form, then encode it with the request builder.

### Parameters

`name` is required, nonempty and contains no controls. `value` is required owned or convertible `Bytes`; empty is allowed. No filename is sent.

### Callback parameters

None.

### Return values

`Ok(Self)` with an appended binary part, or `Err(NetError)`.

### Errors and behavior

An invalid name returns `InvalidInput/RequestBuild`; the form is consumed on failure. The part uses `application/octet-stream`. This does not stream or cap the uploaded bytes.

### Example

See the [complete multipart program](#multipart-construction-example), which calls `.bytes("raw", Bytes::from_static(&[0, 1, 255]))`.

## `Multipart::file_bytes()`

### Required features

`http-client-codecs`.

### Function signature

```text
Multipart::file_bytes(self, name: impl Into<String>, filename: impl Into<String>, value: impl Into<Bytes>) -> Result<Self, NetError>
```

### Steps

Load or construct file data, give it a display filename, and append the returned part to your form.

### Parameters

`name` is required, nonempty and control-free. `filename` is required, nonempty and a single path component without controls, `/` or `\`. `value` is required binary data; empty is allowed.

### Callback parameters

None.

### Return values

`Ok(Self)` with an appended file part, or `Err(NetError)`.

### Errors and behavior

Invalid name or filename returns `InvalidInput/RequestBuild`. Filename quotes are escaped in the disposition header. No file is opened; the bytes are already in memory, and the media type is `application/octet-stream`. The form is consumed on failure.

### Example

See the [complete multipart program](#multipart-construction-example), which calls `.file_bytes("attachment", "note.txt", Bytes::from_static(b"hello"))`.

## `Multipart::file()`

### Required features

`http-client-codecs`.

### Function signature

```text
Multipart::file(self, name: impl Into<String>, path: impl AsRef<Path>) -> Result<Self, NetError>
```

### Steps

Choose an existing readable file and call this method on an application blocking worker if file I/O could block an async executor.

### Parameters

`name` is required, nonempty and control-free. `path` is a required borrowed path to a file with a valid UTF-8 final filename; path contents are read immediately.

### Callback parameters

None.

### Return values

`Ok(Self)` with the fully buffered file part, or `Err(NetError)`.

### Errors and behavior

A missing/invalid filename or invalid name returns `InvalidInput/RequestBuild`; read errors return `Io/RequestBuild` with `io_kind()` when available. The synchronous read occurs before the name is validated by `file_bytes`. There is no upload-size bound: check application limits before reading. The form is consumed on failure.

### Example

See the [complete multipart program](#multipart-construction-example), whose `from_file(path)` helper calls `Multipart::new().file("attachment", path)`. Running `main` alone does not open a file.

## `HttpRequestBuilder::multipart()`

### Required features

`http-client-codecs`.

### Function signature

```text
HttpRequestBuilder::multipart(self, value: Multipart) -> Result<Self, NetError>
```

### Steps

Construct the complete form, set the desired HTTP method/path, call `multipart`, then `build`.

### Parameters

`value` is a required owned form. Empty forms are legal. The builder receiver is also consumed.

### Callback parameters

None.

### Return values

`Ok(HttpRequestBuilder)` with the fully encoded body and generated Content-Type, or `Err(NetError)`.

### Errors and behavior

Another selected body or an already present request Content-Type returns `InvalidInput/RequestBuild`. Boundary collision retries may fail with an input or I/O error. Replacing the generated Content-Type later is rejected immediately by `header` or on `build` after `headers`. No POST method is selected automatically; encoding is buffered.

### Example

See the [complete multipart program](#multipart-construction-example), which calls `.multipart(form)?.build()` after selecting `Method::POST`.

[HTTP guide index](README.md)

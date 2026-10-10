#![cfg(feature = "http-client")]

//! Public consumer contract for merging request headers over common headers.

use http::HeaderName;
use open_net::api::error::{ErrorKind, ErrorStage};
use open_net::api::http::{HeaderMap, HeaderValue, HttpClientConfig};
use std::collections::BTreeMap;

type HeaderSnapshot = BTreeMap<String, Vec<(Vec<u8>, bool)>>;

fn headers(entries: &[(&str, &str)]) -> HeaderMap {
    let mut result = HeaderMap::new();
    for (name, value) in entries {
        result.append(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    result
}

fn config(common: HeaderMap) -> HttpClientConfig {
    HttpClientConfig::new("https://example.com")
        .unwrap()
        .with_common_headers(common)
        .unwrap()
}

/// HeaderMap equality does not compare sensitivity; retain it explicitly.
fn snapshot(headers: &HeaderMap) -> HeaderSnapshot {
    headers
        .keys()
        .map(|name| {
            let values = headers
                .get_all(name)
                .iter()
                .map(|value| (value.as_bytes().to_vec(), value.is_sensitive()))
                .collect();
            (name.as_str().to_owned(), values)
        })
        .collect()
}

fn values<'a>(headers: &'a HeaderMap, name: &str) -> Vec<&'a str> {
    headers
        .get_all(name)
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect()
}

#[test]
fn request_values_replace_the_entire_common_group_and_keep_unrelated_groups() {
    let config = config(headers(&[
        ("x-shared", "common-a"),
        ("x-keep", "keep-a"),
        ("x-shared", "common-b"),
        ("x-keep", "keep-b"),
    ]));
    let request = headers(&[
        ("x-shared", "request-a"),
        ("x-new", "new-a"),
        ("x-shared", "request-b"),
        ("x-new", "new-b"),
    ]);

    let merged = config.merge_headers(&request).unwrap();

    assert_eq!(values(&merged, "x-shared"), ["request-a", "request-b"]);
    assert_eq!(values(&merged, "x-keep"), ["keep-a", "keep-b"]);
    assert_eq!(values(&merged, "x-new"), ["new-a", "new-b"]);
    assert_eq!(merged.len(), 6);
}

#[test]
fn one_request_value_replaces_every_common_value_for_that_name() {
    let config = config(headers(&[
        ("x-shared", "common-a"),
        ("x-shared", "common-b"),
        ("x-shared", "common-c"),
    ]));

    let merged = config
        .merge_headers(&headers(&[("x-shared", "only-request")]))
        .unwrap();

    assert_eq!(values(&merged, "x-shared"), ["only-request"]);
    assert_eq!(merged.len(), 1);
}

#[test]
fn new_request_header_preserves_empty_and_repeated_values_in_order() {
    let config = config(HeaderMap::new());
    let request = headers(&[
        ("x-new", "same"),
        ("x-new", ""),
        ("x-new", "same"),
        ("x-new", "last"),
    ]);

    let merged = config.merge_headers(&request).unwrap();

    assert_eq!(values(&merged, "x-new"), ["same", "", "same", "last"]);
    assert_eq!(snapshot(&merged), snapshot(&request));
}

#[test]
fn empty_request_value_overrides_a_nonempty_common_value() {
    let config = config(headers(&[("x-empty", "common-a"), ("x-empty", "common-b")]));

    let merged = config.merge_headers(&headers(&[("x-empty", "")])).unwrap();

    assert_eq!(values(&merged, "x-empty"), [""]);
    assert_eq!(merged.len(), 1);
}

#[test]
fn mixed_case_names_override_one_group_without_reordering_its_values() {
    let config = config(headers(&[("X-Mixed", "common-a"), ("x-MIXED", "common-b")]));
    let request = headers(&[
        ("X-MIXED", "request-a"),
        ("x-mixed", "request-b"),
        ("X-Mixed", "request-c"),
    ]);

    let merged = config.merge_headers(&request).unwrap();

    assert_eq!(merged.keys().count(), 1);
    assert_eq!(
        values(&merged, "x-mixed"),
        ["request-a", "request-b", "request-c"]
    );
}

#[test]
fn empty_request_preserves_common_groups_and_both_empty_maps_stay_empty() {
    let config = config(headers(&[
        ("x-repeat", "first"),
        ("x-repeat", ""),
        ("x-repeat", "first"),
        ("x-other", "other"),
    ]));

    let merged = config.merge_headers(&HeaderMap::new()).unwrap();

    assert_eq!(snapshot(&merged), snapshot(&config.common_headers));
    assert!(HttpClientConfig::new("https://example.com")
        .unwrap()
        .merge_headers(&HeaderMap::new())
        .unwrap()
        .is_empty());
}

#[test]
fn merge_preserves_each_value_sensitivity_and_leaves_both_inputs_unchanged() {
    let mut common = headers(&[
        ("x-overridden", "old-secret"),
        ("x-common-only", "common-secret"),
        ("x-common-only", "common-public"),
    ]);
    common.get_mut("x-overridden").unwrap().set_sensitive(true);
    common.get_mut("x-common-only").unwrap().set_sensitive(true);
    let config = config(common);
    let mut request = HeaderMap::new();
    for (text, sensitive) in [("secret-a", true), ("public", false), ("secret-b", true)] {
        let mut value = HeaderValue::from_static(text);
        value.set_sensitive(sensitive);
        request.append("x-overridden", value);
    }
    let common_before = snapshot(&config.common_headers);
    let request_before = snapshot(&request);
    let mut expected = common_before.clone();
    expected.extend(request_before.clone());

    let mut merged = config.merge_headers(&request).unwrap();

    assert_eq!(snapshot(&merged), expected);
    assert_eq!(snapshot(&config.common_headers), common_before);
    assert_eq!(snapshot(&request), request_before);
    // The returned owned map must not let a consumer mutate either input.
    merged.insert("x-overridden", HeaderValue::from_static("changed"));
    merged
        .get_mut("x-common-only")
        .unwrap()
        .set_sensitive(false);
    merged.append("x-new", HeaderValue::from_static("new"));
    assert_eq!(snapshot(&config.common_headers), common_before);
    assert_eq!(snapshot(&request), request_before);
    assert_eq!(snapshot(&config.merge_headers(&request).unwrap()), expected);
}

#[test]
fn forbidden_common_or_request_headers_are_still_rejected_before_merging() {
    // This is the existing transport-owned/hop-by-hop deny list, not a new policy.
    for name in [
        "Connection",
        "Keep-Alive",
        "Proxy-Connection",
        "Proxy-Authenticate",
        "Proxy-Authorization",
        "TE",
        "Trailer",
        "Transfer-Encoding",
        "Upgrade",
        "Host",
        "Content-Length",
    ] {
        let forbidden = headers(&[(name, "first"), (name, "second")]);
        let forbidden_before = snapshot(&forbidden);
        let valid = config(headers(&[("x-valid", "keep")]));
        let error = valid.merge_headers(&forbidden).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidConfig, "request {name}");
        assert_eq!(error.context().stage, Some(ErrorStage::Configuration));
        assert_eq!(snapshot(&forbidden), forbidden_before);
        assert_eq!(values(&valid.common_headers, "x-valid"), ["keep"]);

        let error = HttpClientConfig::new("https://example.com")
            .unwrap()
            .with_common_headers(forbidden.clone())
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidConfig, "common {name}");
        assert_eq!(error.context().stage, Some(ErrorStage::Configuration));

        // Public field assignment can bypass the setter, so merge must revalidate.
        let mut invalid_common = valid;
        invalid_common.common_headers = forbidden;
        let request = headers(&[("x-valid", "request-a"), ("x-valid", "request-b")]);
        let request_before = snapshot(&request);
        let error = invalid_common.merge_headers(&request).unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::InvalidConfig,
            "public common {name}"
        );
        assert_eq!(error.context().stage, Some(ErrorStage::Configuration));
        assert_eq!(snapshot(&invalid_common.common_headers), forbidden_before);
        assert_eq!(snapshot(&request), request_before);
    }
}

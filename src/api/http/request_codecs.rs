//! Shared request-codec helpers kept behind the optional HTTP codec feature.

use crate::api::error::{ErrorKind, ErrorStage, NetError};
use http::{HeaderMap, HeaderValue};
use serde::Serialize;

/// The body representation selected by the fluent request builder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BodyEncoding {
    Raw,
    Json,
    Form,
    Multipart,
}

pub(crate) fn invalid_codec<T>(_field: &'static str, error: T) -> NetError
where
    T: Into<crate::BoxError>,
{
    NetError::with_source(ErrorKind::InvalidInput, error).with_stage(ErrorStage::RequestBuild)
}

pub(crate) fn invalid_codec_message(field: &'static str, message: &'static str) -> NetError {
    // The message is fixed so payloads, credentials, and local paths do not
    // get copied into diagnostics.
    NetError::input(field, message)
}

pub(crate) fn encode_query<T: Serialize>(value: &T) -> Result<String, NetError> {
    serde_qs::Config::new()
        .array_format(serde_qs::ArrayFormat::Unindexed)
        .max_depth(5)
        .serialize_string(value)
        .map_err(|error| invalid_codec("query", error))
}

pub(crate) fn encode_form<T: Serialize>(value: &T) -> Result<Vec<u8>, NetError> {
    serde_qs::Config::new()
        .array_format(serde_qs::ArrayFormat::Unindexed)
        .use_form_encoding(true)
        .max_depth(5)
        .serialize_string(value)
        .map(String::into_bytes)
        .map_err(|error| invalid_codec("form", error))
}

pub(crate) fn encode_json<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, NetError> {
    serde_json::to_vec(value).map_err(|error| invalid_codec("json", error))
}

pub(crate) fn set_default_content_type(
    headers: &mut HeaderMap,
    value: &'static str,
) -> Result<(), NetError> {
    if !headers.contains_key(http::header::CONTENT_TYPE) {
        let header = HeaderValue::from_static(value);
        headers.insert(http::header::CONTENT_TYPE, header);
    }
    Ok(())
}

pub(crate) fn append_query(path: &mut String, encoded: &str) {
    if encoded.is_empty() {
        return;
    }
    let separator = if path.contains('?') {
        if path.ends_with('?') || path.ends_with('&') {
            ""
        } else {
            "&"
        }
    } else {
        "?"
    };
    path.push_str(separator);
    path.push_str(encoded);
}

pub(crate) fn ensure_body_slot(
    current: Option<BodyEncoding>,
    next: BodyEncoding,
) -> Result<(), NetError> {
    if current.is_some() {
        return Err(invalid_codec_message(
            "body",
            match next {
                BodyEncoding::Json => "a request body codec is already selected",
                BodyEncoding::Form => "a request body codec is already selected",
                BodyEncoding::Multipart => "a request body codec is already selected",
                BodyEncoding::Raw => "a request body codec is already selected",
            },
        ));
    }
    Ok(())
}

pub(crate) fn validate_multipart_header(
    headers: &HeaderMap,
    expected: &HeaderValue,
) -> Result<(), NetError> {
    if let Some(value) = headers.get(http::header::CONTENT_TYPE) {
        if value != expected {
            return Err(invalid_codec_message(
                "content-type",
                "multipart content-type is managed by the SDK",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::ser::Error as _;
    use std::error::Error as _;

    struct FailingSerialize;

    impl Serialize for FailingSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(S::Error::custom("intentional codec failure"))
        }
    }

    #[derive(Serialize)]
    struct Nested {
        child: Option<Box<Nested>>,
        value: Option<String>,
    }

    fn nested(depth: usize) -> Nested {
        if depth == 0 {
            Nested {
                child: None,
                value: Some("too-deep".to_owned()),
            }
        } else {
            Nested {
                child: Some(Box::new(nested(depth - 1))),
                value: None,
            }
        }
    }

    #[test]
    fn query_and_form_serialization_failures_keep_request_build_context() {
        let query =
            encode_query(&FailingSerialize).expect_err("query must reject serializer errors");
        assert_eq!(query.kind(), crate::api::error::ErrorKind::InvalidInput);
        assert_eq!(
            query.context().stage,
            Some(crate::api::error::ErrorStage::RequestBuild)
        );
        assert!(query.source().is_some());

        let form = encode_form(&FailingSerialize).expect_err("form must reject serializer errors");
        assert_eq!(form.kind(), crate::api::error::ErrorKind::InvalidInput);
        assert_eq!(
            form.context().stage,
            Some(crate::api::error::ErrorStage::RequestBuild)
        );
        assert!(form.source().is_some());
    }

    #[test]
    fn deeply_nested_query_and_form_values_hit_the_documented_depth_guard() {
        let value = nested(12);

        assert!(encode_query(&value).is_err());
        assert!(encode_form(&value).is_err());
    }
}

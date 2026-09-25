use super::RequestId;
use crate::error::{ErrorKind, ErrorStage};
use std::collections::{BTreeSet, HashSet};
use std::error::Error;

type TestResult = Result<(), crate::BoxError>;
fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[test]
fn identity_parsing_preserves_nonempty_input_exactly() -> TestResult {
    let text = "  外部/request id\t ";
    let id = RequestId::new(text)?;
    check(
        id.as_str() == text && id.as_ref() == text && id.to_string() == text,
        "identity input was normalized",
    )?;
    check(
        text.parse::<RequestId>()? == id,
        "FromStr differs from construction",
    )?;
    check(
        RequestId::try_from(text.to_owned())? == id,
        "TryFrom<String> differs from construction",
    )?;
    check(
        id.clone().into_string() == text,
        "identity ownership conversion lost input",
    )?;
    let sorted = BTreeSet::from([id.clone(), RequestId::new("a")?, id.clone()]);
    let hashed = HashSet::from([id.clone(), id]);
    check(
        sorted.len() == 2 && hashed.len() == 1,
        "identity ordering or hashing lost equality",
    )
}

#[test]
fn request_id_rejects_blank_and_excess_utf8_bytes_with_safe_field_details() -> TestResult {
    for input in [
        String::new(),
        " \t\r\n".to_owned(),
        "\u{2003}\u{3000}".to_owned(),
        "x".repeat(1025),
        "é".repeat(513),
    ] {
        let error = RequestId::new(input)
            .err()
            .ok_or("invalid identity was accepted")?;
        check(
            error.kind() == ErrorKind::InvalidInput
                && error.context().stage == Some(ErrorStage::RequestBuild),
            "invalid identity classification changed",
        )?;
        check(
            error.config_error().is_some_and(|detail| {
                detail.field() == "request_id" && !detail.reason().is_empty()
            }),
            "invalid identity lacks field detail",
        )?;
    }
    check(
        RequestId::new("é".repeat(512))?.as_str().len() == 1024,
        "UTF-8 boundary must be measured in bytes",
    )?;
    check(
        RequestId::new("x".repeat(1024))?.as_str().len() == 1024,
        "maximum identity length must be accepted",
    )?;
    let secret = format!("private-token-{}", "x".repeat(1024));
    let error = RequestId::new(secret)
        .err()
        .ok_or("oversized secret was accepted")?;
    check(
        !format!("{error} {error:?}").contains("private-token"),
        "validation output exposed input",
    )
}

#[test]
fn generated_ids_use_fallible_entropy_and_valid_uuid_shape() -> TestResult {
    let mut seen = HashSet::new();
    for _ in 0..32 {
        let id = RequestId::random()?;
        let parsed = uuid::Uuid::parse_str(id.as_str())?;
        check(
            parsed.get_version() == Some(uuid::Version::Random)
                && parsed.get_variant() == uuid::Variant::RFC4122,
            "random identity is not a v4 UUID",
        )?;
        check(seen.insert(id), "random identity was repeated")?;
    }
    Ok(())
}

#[test]
fn injected_entropy_failure_retains_the_actual_random_error() -> TestResult {
    let error = RequestId::random_with(|bytes| {
        bytes.fill(7);
        Err(getrandom::Error::UNEXPECTED)
    })
    .err()
    .ok_or("failed entropy source issued an identity")?;
    check(
        error.kind() == ErrorKind::Io,
        "entropy failure must remain an I/O error",
    )?;
    check(
        error
            .source()
            .and_then(|source| source.downcast_ref::<getrandom::Error>())
            == Some(&getrandom::Error::UNEXPECTED),
        "entropy failure discarded the original getrandom source",
    )?;
    check(
        !format!("{error} {error:?}").contains(&getrandom::Error::UNEXPECTED.to_string()),
        "default error output included arbitrary source text",
    )
}

#[test]
fn controlled_entropy_uses_the_same_generation_path() -> TestResult {
    let first = RequestId::random_with(|bytes| {
        bytes.fill(0);
        Ok(())
    })?;
    let second = RequestId::random_with(|bytes| {
        bytes.fill(255);
        Ok(())
    })?;
    check(
        first != second && first.as_str() == "00000000-0000-4000-8000-000000000000",
        "random byte conversion changed identity or version bits",
    )
}

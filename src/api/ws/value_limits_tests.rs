use super::{validate_credential_version, validate_metadata, validate_metadata_lengths};
use crate::error::{ErrorKind, ErrorStage};
use crate::{Metadata, NetError};

type TestResult = std::result::Result<(), crate::BoxError>;

fn check_config_error(error: NetError, field: &str, limit: &str) -> TestResult {
    if error.kind() != ErrorKind::InvalidConfig
        || error.context().stage != Some(ErrorStage::Configuration)
    {
        return Err("value limit failure lost its configuration classification".into());
    }
    let detail = error
        .config_error()
        .ok_or("configuration details missing")?;
    if detail.field() != field
        || !detail.reason().contains(limit)
        || !detail.reason().contains("UTF-8")
        || !detail.reason().contains("bytes")
    {
        return Err("value limit failure did not identify its field and byte limit".into());
    }
    Ok(())
}

#[test]
fn empty_metadata_is_valid() -> TestResult {
    validate_metadata(&Metadata::new())?;
    validate_metadata(&Metadata::from([(String::new(), String::new())]))?;
    Ok(())
}

#[test]
fn metadata_counts_utf8_bytes_across_keys_and_values() -> TestResult {
    let metadata = Metadata::from([
        ("é".repeat(2_048), "中".repeat(2_048)),
        ("😀".to_owned(), "v".repeat(6_140)),
    ]);
    validate_metadata(&metadata)?;
    let mut over_limit = metadata.clone();
    over_limit.insert(String::new(), "x".to_owned());
    let error = validate_metadata(&over_limit)
        .err()
        .ok_or("metadata above 16384 UTF-8 bytes was accepted")?;
    check_config_error(error, "metadata", "16384")
}

#[test]
fn metadata_key_bytes_alone_can_reach_or_exceed_the_limit() -> TestResult {
    validate_metadata(&Metadata::from([("x".repeat(16_384), String::new())]))?;
    let error = validate_metadata(&Metadata::from([("x".repeat(16_385), String::new())]))
        .err()
        .ok_or("oversized metadata key was accepted")?;
    check_config_error(error, "metadata", "16384")
}

#[test]
fn metadata_validation_preserves_all_input_on_success_and_failure() -> TestResult {
    for (metadata, valid) in [
        (
            Metadata::from([
                ("  label\n".to_owned(), " value 😀 \0".to_owned()),
                (String::new(), String::new()),
            ]),
            true,
        ),
        (
            Metadata::from([("key".to_owned(), "é".repeat(8_193))]),
            false,
        ),
    ] {
        let before = metadata.clone();
        if validate_metadata(&metadata).is_ok() != valid {
            return Err("metadata validation imposed a constraint beyond its byte limit".into());
        }
        if metadata != before {
            return Err("metadata validation changed its borrowed input".into());
        }
    }
    Ok(())
}

#[test]
fn metadata_length_arithmetic_rejects_overflow_without_large_allocations() -> TestResult {
    for lengths in [vec![(usize::MAX, 1)], vec![(16_384, 0), (usize::MAX, 0)]] {
        let error = validate_metadata_lengths(lengths)
            .err()
            .ok_or("metadata byte count overflow was accepted")?;
        check_config_error(error, "metadata", "16384")?;
    }
    Ok(())
}

#[test]
fn credential_version_uses_utf8_byte_boundaries() -> TestResult {
    validate_credential_version("")?;
    validate_credential_version(&"v".repeat(256))?;
    validate_credential_version(&"😀".repeat(64))?;
    for version in ["v".repeat(257), format!("{}a", "😀".repeat(64))] {
        let error = validate_credential_version(&version)
            .err()
            .ok_or("credential version above 256 UTF-8 bytes was accepted")?;
        check_config_error(error, "credential_version", "256")?;
    }
    Ok(())
}

#[test]
fn credential_version_validation_preserves_input_on_success_and_failure() -> TestResult {
    for (version, valid) in [
        ("  release 中\n\0".to_owned(), true),
        ("😀".repeat(65), false),
    ] {
        let before = version.clone();
        if validate_credential_version(&version).is_ok() != valid {
            return Err(
                "credential version validation imposed a constraint beyond its byte limit".into(),
            );
        }
        if version != before {
            return Err("credential version validation changed its borrowed input".into());
        }
    }
    Ok(())
}

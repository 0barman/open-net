use crate::{Metadata, NetError, Result};

const MAX_METADATA_BYTES: usize = 16 * 1024;
const MAX_CREDENTIAL_VERSION_BYTES: usize = 256;

pub(crate) fn validate_metadata(metadata: &Metadata) -> Result<()> {
    validate_metadata_lengths(metadata.iter().map(|(key, value)| (key.len(), value.len())))
}

fn validate_metadata_lengths(lengths: impl IntoIterator<Item = (usize, usize)>) -> Result<()> {
    let mut total = 0usize;
    for (key_bytes, value_bytes) in lengths {
        total = key_bytes
            .checked_add(value_bytes)
            .and_then(|entry_bytes| total.checked_add(entry_bytes))
            .filter(|total| *total <= MAX_METADATA_BYTES)
            .ok_or_else(|| {
                NetError::config(
                    "metadata",
                    "keys and values must total at most 16384 UTF-8 bytes",
                )
            })?;
    }
    Ok(())
}

pub(crate) fn validate_credential_version(version: &str) -> Result<()> {
    if version.len() > MAX_CREDENTIAL_VERSION_BYTES {
        return Err(NetError::config(
            "credential_version",
            "must contain at most 256 UTF-8 bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "value_limits_tests.rs"]
mod tests;

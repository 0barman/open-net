use crate::error::{ErrorKind, ErrorStage};
use crate::{NetError, Result};
use std::fmt;
use std::str::FromStr;

/// Business request identifier preserved exactly as supplied by the caller.
/// It must contain a non-whitespace character and be at most 1024 UTF-8 bytes.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RequestId(
    /// Original identifier after length and non-whitespace validation.
    String,
);

impl RequestId {
    /// Validates and constructs an identifier without trimming it.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.len() > 1024 {
            return Err(NetError::input(
                "request_id",
                "must be at most 1024 UTF-8 bytes",
            ));
        }
        if value.trim().is_empty() {
            return Err(NetError::input(
                "request_id",
                "must contain a non-whitespace character",
            ));
        }
        Ok(Self(value))
    }

    /// Generates a random UUID-based request identifier.
    pub fn random() -> Result<Self> {
        Self::random_with(getrandom::fill)
    }

    fn random_with(
        fill: impl FnOnce(&mut [u8]) -> std::result::Result<(), getrandom::Error>,
    ) -> Result<Self> {
        let mut bytes = [0_u8; 16];
        fill(&mut bytes).map_err(|error| {
            NetError::with_source(ErrorKind::Io, error).with_stage(ErrorStage::RequestBuild)
        })?;
        Ok(Self(
            uuid::Builder::from_random_bytes(bytes)
                .into_uuid()
                .to_string(),
        ))
    }

    /// Borrows the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the identifier and returns its string representation.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AsRef<str> for RequestId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for RequestId {
    type Err = NetError;

    fn from_str(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl TryFrom<String> for RequestId {
    type Error = NetError;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

#[cfg(test)]
#[path = "request_id_tests.rs"]
mod tests;

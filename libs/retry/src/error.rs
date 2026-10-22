use std::fmt;

/// A rejected retry configuration, with a stable field name and an explanation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryConfigError {
    field: String,
    reason: String,
}

impl RetryConfigError {
    /// Construct an error for a common or protocol-specific retry field.
    pub fn new(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// Return the configuration field that failed validation.
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Return the explanation of the rejected value.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Display for RetryConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.field, self.reason)
    }
}

impl std::error::Error for RetryConfigError {}

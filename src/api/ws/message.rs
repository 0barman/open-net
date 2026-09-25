use bytes::Bytes;

/// Owned business payload; protocol control frames use dedicated APIs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    /// UTF-8 text payload.
    Text(
        /// Owned text body.
        String,
    ),
    /// Binary payload with no UTF-8 requirement.
    Binary(
        /// Binary body whose backing storage can be shared by clones.
        Bytes,
    ),
}

impl Message {
    /// Creates a text message from any string-like value.
    pub fn text(value: impl Into<String>) -> Self {
        Self::Text(value.into())
    }

    /// Creates a binary message from bytes.
    pub fn binary(value: impl Into<Bytes>) -> Self {
        Self::Binary(value.into())
    }

    /// Returns the text body, or `None` for binary messages.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(value) => Some(value),
            Self::Binary(_) => None,
        }
    }

    /// Returns the payload bytes for either message variant.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Text(value) => value.as_bytes(),
            Self::Binary(value) => value.as_ref(),
        }
    }

    /// Converts the payload into shared bytes, consuming the message.
    pub fn into_bytes(self) -> Bytes {
        match self {
            Self::Text(value) => Bytes::from(value),
            Self::Binary(value) => value,
        }
    }

    /// Returns the payload length in bytes.
    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    /// Reports whether the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }
}

impl From<String> for Message {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for Message {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<Bytes> for Message {
    fn from(value: Bytes) -> Self {
        Self::Binary(value)
    }
}

impl From<Vec<u8>> for Message {
    fn from(value: Vec<u8>) -> Self {
        Self::Binary(Bytes::from(value))
    }
}

impl From<&[u8]> for Message {
    fn from(value: &[u8]) -> Self {
        Self::Binary(Bytes::copy_from_slice(value))
    }
}

#[cfg(test)]
#[path = "message_tests.rs"]
mod tests;

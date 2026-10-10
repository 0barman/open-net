//! A small, transport-independent buffered `multipart/form-data` builder.
//!
//! The request worker still consumes `Bytes`, so this API intentionally reads
//! file parts before request admission.  Streaming and one-shot upload bodies
//! belong to the HTTP body work planned separately from these codecs.

use crate::api::error::{ErrorKind, ErrorStage, NetError};
use bytes::Bytes;
use http::HeaderValue;
use std::fmt;
use std::path::Path;

const BOUNDARY_BYTES: usize = 18;

#[derive(Clone)]
enum Part {
    Text {
        name: String,
        value: String,
    },
    Bytes {
        name: String,
        filename: Option<String>,
        value: Bytes,
    },
}

/// A buffered multipart form whose wire representation is independent of
/// reqwest.  The boundary and all part values are owned by this value.
#[derive(Clone)]
pub struct Multipart {
    boundary: String,
    parts: Vec<Part>,
}

impl Default for Multipart {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Multipart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts = self
            .parts
            .iter()
            .map(|part| match part {
                Part::Text { .. } => "text",
                Part::Bytes { .. } => "bytes",
            })
            .collect::<Vec<_>>();
        formatter
            .debug_struct("Multipart")
            .field("boundary_len", &self.boundary.len())
            .field("part_count", &self.parts.len())
            .field("part_kinds", &parts)
            .finish()
    }
}

impl Multipart {
    /// Create an empty multipart form with an SDK-generated boundary.
    pub fn new() -> Self {
        Self {
            boundary: make_boundary().unwrap_or_else(|_| "open-net-boundary-fallback".to_owned()),
            parts: Vec::new(),
        }
    }

    /// Add a UTF-8 text field.
    pub fn text(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, NetError> {
        let name = validate_token("multipart.name", name.into())?;
        self.parts.push(Part::Text {
            name,
            value: value.into(),
        });
        Ok(self)
    }

    /// Add a binary field without a filename.
    pub fn bytes(
        mut self,
        name: impl Into<String>,
        value: impl Into<Bytes>,
    ) -> Result<Self, NetError> {
        let name = validate_token("multipart.name", name.into())?;
        self.parts.push(Part::Bytes {
            name,
            filename: None,
            value: value.into(),
        });
        Ok(self)
    }

    /// Add an in-memory file field with a caller-provided filename.
    pub fn file_bytes(
        mut self,
        name: impl Into<String>,
        filename: impl Into<String>,
        value: impl Into<Bytes>,
    ) -> Result<Self, NetError> {
        let name = validate_token("multipart.name", name.into())?;
        let filename = validate_filename(filename.into())?;
        self.parts.push(Part::Bytes {
            name,
            filename: Some(filename),
            value: value.into(),
        });
        Ok(self)
    }

    /// Read a file and add it as a replayable buffered file field.
    pub fn file(self, name: impl Into<String>, path: impl AsRef<Path>) -> Result<Self, NetError> {
        let path = path.as_ref();
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| NetError::input("multipart.filename", "file path has no filename"))?;
        let filename = validate_filename(filename.to_owned())?;
        let value = std::fs::read(path).map_err(|error| {
            NetError::with_source(ErrorKind::Io, error).with_stage(ErrorStage::RequestBuild)
        })?;
        self.file_bytes(name, filename, Bytes::from(value))
    }

    /// Encode this form into a replayable body and its SDK-owned content type.
    pub(crate) fn encode(self) -> Result<(Bytes, HeaderValue), NetError> {
        let mut boundary = self.boundary;
        let mut attempts = 0;
        while self_contains_boundary(&self.parts, &boundary) && attempts < 8 {
            boundary = make_boundary().map_err(|error| {
                NetError::with_source(ErrorKind::Io, error).with_stage(ErrorStage::RequestBuild)
            })?;
            attempts += 1;
        }
        if self_contains_boundary(&self.parts, &boundary) {
            return Err(NetError::input(
                "multipart.boundary",
                "multipart boundary collides with a part",
            ));
        }

        let mut body = Vec::new();
        for part in self.parts {
            body.extend_from_slice(b"--");
            body.extend_from_slice(boundary.as_bytes());
            body.extend_from_slice(b"\r\n");
            match part {
                Part::Text { name, value } => {
                    write_disposition(&mut body, &name, None)?;
                    body.extend_from_slice(b"Content-Type: text/plain; charset=utf-8\r\n\r\n");
                    body.extend_from_slice(value.as_bytes());
                }
                Part::Bytes {
                    name,
                    filename,
                    value,
                } => {
                    write_disposition(&mut body, &name, filename.as_deref())?;
                    body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
                    body.extend_from_slice(value.as_ref());
                }
            }
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(b"--");
        body.extend_from_slice(boundary.as_bytes());
        body.extend_from_slice(b"--\r\n");

        let content_type = HeaderValue::from_str(&format!(
            "multipart/form-data; boundary={boundary}"
        ))
        .map_err(|_| NetError::input("multipart.content-type", "invalid generated boundary"))?;
        Ok((Bytes::from(body), content_type))
    }
}

fn validate_token(field: &'static str, value: String) -> Result<String, NetError> {
    if value.is_empty() || value.chars().any(|character| character.is_control()) {
        return Err(NetError::input(
            field,
            "multipart field name must not be empty or contain control characters",
        ));
    }
    Ok(value)
}

fn validate_filename(value: String) -> Result<String, NetError> {
    if value.is_empty()
        || value.chars().any(|character| character.is_control())
        || value.contains('/')
        || value.contains('\\')
    {
        return Err(NetError::input(
            "multipart.filename",
            "multipart filename must be a single non-empty path component",
        ));
    }
    Ok(value)
}

fn write_disposition(
    body: &mut Vec<u8>,
    name: &str,
    filename: Option<&str>,
) -> Result<(), NetError> {
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"");
    write_quoted(body, name)?;
    body.extend_from_slice(b"\"");
    if let Some(filename) = filename {
        body.extend_from_slice(b"; filename=\"");
        write_quoted(body, filename)?;
        body.extend_from_slice(b"\"");
    }
    body.extend_from_slice(b"\r\n");
    Ok(())
}

fn write_quoted(body: &mut Vec<u8>, value: &str) -> Result<(), NetError> {
    for character in value.chars() {
        if character == '\r' || character == '\n' || character == '\0' || character.is_control() {
            return Err(NetError::input(
                "multipart.header",
                "multipart header value contains a control character",
            ));
        }
        match character {
            '\\' | '"' => body.push(b'\\'),
            _ => {}
        }
        let mut encoded = [0_u8; 4];
        body.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
    }
    Ok(())
}

fn self_contains_boundary(parts: &[Part], boundary: &str) -> bool {
    let marker = boundary.as_bytes();
    parts.iter().any(|part| match part {
        Part::Text { name, value } => {
            name.as_bytes()
                .windows(marker.len())
                .any(|window| window == marker)
                || value
                    .as_bytes()
                    .windows(marker.len())
                    .any(|window| window == marker)
        }
        Part::Bytes {
            name,
            filename,
            value,
        } => {
            name.as_bytes()
                .windows(marker.len())
                .any(|window| window == marker)
                || filename.as_ref().is_some_and(|filename| {
                    filename
                        .as_bytes()
                        .windows(marker.len())
                        .any(|window| window == marker)
                })
                || value
                    .as_ref()
                    .windows(marker.len())
                    .any(|window| window == marker)
        }
    })
}

fn make_boundary() -> Result<String, getrandom::Error> {
    let mut bytes = [0_u8; BOUNDARY_BYTES];
    getrandom::fill(&mut bytes)?;
    let mut boundary = String::with_capacity(2 + BOUNDARY_BYTES * 2);
    boundary.push_str("open-net-");
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut boundary, "{byte:02x}");
    }
    Ok(boundary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_text_binary_and_file_parts_without_leaking_debug_values() {
        let form = Multipart::new()
            .text("message", "中文")
            .unwrap()
            .file_bytes("asset", "demo.bin", Bytes::from_static(&[0, 1, 255]))
            .unwrap();
        let debug = format!("{form:?}");
        assert!(!debug.contains("中文") && !debug.contains("demo.bin"));
        let (body, content_type) = form.encode().unwrap();
        assert!(content_type
            .to_str()
            .unwrap()
            .starts_with("multipart/form-data; boundary="));
        assert!(body
            .windows("Content-Disposition".len())
            .any(|window| window == b"Content-Disposition"));
        assert!(body.windows(3).any(|window| window == [0, 1, 255]));
        assert!(body
            .windows("中文".len())
            .any(|window| window == "中文".as_bytes()));
    }

    #[test]
    fn rejects_header_injection() {
        let result = Multipart::new().text("bad\r\nname", "value");
        assert!(result.is_err());
    }

    #[test]
    fn accepts_empty_parts_and_rejects_invalid_names_and_filenames() {
        let form = Multipart::new()
            .text("empty-text", "")
            .unwrap()
            .bytes("empty-bytes", Bytes::new())
            .unwrap();
        let (body, _) = form.encode().unwrap();
        assert!(body
            .windows(b"name=\"empty-text\"".len())
            .any(|window| { window == b"name=\"empty-text\"" }));
        assert!(body
            .windows(b"name=\"empty-bytes\"".len())
            .any(|window| { window == b"name=\"empty-bytes\"" }));

        assert!(Multipart::new().text("", "value").is_err());
        assert!(Multipart::new().bytes("bad\0name", Bytes::new()).is_err());
        assert!(Multipart::new()
            .file_bytes("asset", "nested/path.bin", Bytes::new())
            .is_err());
        assert!(Multipart::new()
            .file_bytes("asset", "nested\\path.bin", Bytes::new())
            .is_err());
        assert!(Multipart::new()
            .file_bytes("asset", "bad\nname.bin", Bytes::new())
            .is_err());
    }

    #[test]
    fn missing_files_return_io_errors_and_large_buffered_parts_are_replayable() {
        let missing_path = std::env::temp_dir().join(format!(
            "open-net-missing-{}-{}-asset.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let missing = Multipart::new().file("asset", &missing_path);
        let error = missing.expect_err("missing file must fail during construction");
        assert_eq!(error.kind(), crate::api::error::ErrorKind::Io);
        assert_eq!(error.io_kind(), Some(std::io::ErrorKind::NotFound));

        let value = Bytes::from(vec![0xA5; 1024 * 1024]);
        let (body, _) = Multipart::new()
            .bytes("large", value.clone())
            .unwrap()
            .encode()
            .unwrap();
        assert!(body.len() >= value.len());
        assert!(body
            .windows(value.len())
            .any(|window| window == value.as_ref()));
    }

    #[test]
    fn regenerates_a_boundary_when_the_initial_boundary_is_in_a_part() {
        let form = Multipart {
            boundary: "fixed-boundary".to_owned(),
            parts: vec![Part::Text {
                name: "field".to_owned(),
                value: "contains fixed-boundary".to_owned(),
            }],
        };
        let (body, content_type) = form.encode().unwrap();
        let content_type = content_type.to_str().unwrap();
        assert!(!content_type.ends_with("fixed-boundary"));
        assert!(body
            .windows(b"contains fixed-boundary".len())
            .any(|window| { window == b"contains fixed-boundary" }));
    }
}

//! Bounded, conservative summaries for diagnostic fields. Never use full request
//! bodies or headers as log arguments; these helpers are an additional safeguard.

use std::fmt::{self, Display, Write};

const MAX_SUMMARY_BYTES: usize = 512;

fn bounded(value: &str) -> String {
    let mut end = value.len().min(MAX_SUMMARY_BYTES);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end]
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect()
}

/// Keeps an HTTP/WebSocket URL's scheme, host and bounded path, omitting
/// userinfo, query and fragment. Malformed or unsupported URLs are redacted.
pub fn url(value: &str) -> String {
    let value = value.trim();
    let Some((scheme, rest)) = value.split_once("://") else {
        return "<redacted-url>".to_string();
    };
    if !["http", "https", "ws", "wss"].contains(&scheme.to_ascii_lowercase().as_str()) {
        return "<redacted-url>".to_string();
    }
    let rest = rest.split(['?', '#']).next().unwrap_or_default();
    let (authority, path) = rest
        .split_once('/')
        .map_or((rest, ""), |(host, path)| (host, path));
    let host = authority.rsplit('@').next().unwrap_or_default();
    if !valid_authority(host) {
        return "<redacted-url>".to_string();
    }
    let slash = if rest.contains('/') { "/" } else { "" };
    bounded(&format!("{scheme}://{host}{slash}{path}"))
}

fn valid_authority(authority: &str) -> bool {
    if let Some(ipv6) = authority.strip_prefix('[') {
        let Some((address, rest)) = ipv6.split_once(']') else {
            return false;
        };
        return address.parse::<std::net::Ipv6Addr>().is_ok()
            && (rest.is_empty()
                || rest
                    .strip_prefix(':')
                    .is_some_and(|port| port.parse::<u16>().is_ok()));
    }
    let (host, port) = authority
        .split_once(':')
        .map_or((authority, None), |(host, port)| (host, Some(port)));
    !host.is_empty()
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_'))
        && port.is_none_or(|port| port.parse::<u16>().is_ok())
}

struct BoundedWriter(String, bool);

impl Write for BoundedWriter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let remaining = MAX_SUMMARY_BYTES.saturating_sub(self.0.len());
        let mut end = remaining.min(value.len());
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        self.0.push_str(&value[..end]);
        if end < value.len() {
            self.1 = true;
            Err(fmt::Error)
        } else {
            Ok(())
        }
    }
}

/// Formats an error into at most 512 bytes, strips URL credentials/query/fragment,
/// and conservatively redacts everything following a common credential/header
/// key. This retains error context preceding the sensitive detail, rather than
/// trying to guess where an arbitrary error's secret value ends.
pub fn error(value: &(impl Display + ?Sized)) -> String {
    let mut writer = BoundedWriter(String::new(), false);
    let _ = write!(&mut writer, "{value}");
    let input = writer.0;
    let truncated = writer.1;
    let mut result = String::new();
    let mut remaining = input.as_str();
    while let Some(start) = remaining.find("://") {
        let scheme_start = remaining[..start]
            .char_indices()
            .rev()
            .find(|(_, character)| !character.is_ascii_alphabetic())
            .map_or(0, |(index, character)| index + character.len_utf8());
        result.push_str(&remaining[..scheme_start]);
        let candidate = &remaining[scheme_start..];
        let end = candidate
            .find(|character: char| {
                character.is_whitespace() || ['"', '\'', '<', '>'].contains(&character)
            })
            .unwrap_or(candidate.len());
        if truncated && end == candidate.len() {
            result.push_str("<redacted-url>");
        } else {
            result.push_str(&url(&candidate[..end]));
        }
        remaining = &candidate[end..];
    }
    result.push_str(remaining);
    let lowercase = result.to_ascii_lowercase();
    let sensitive = [
        "authorization",
        "bearer",
        "cookie",
        "token",
        "password",
        "passwd",
        "secret",
        "credential",
        "api_key",
        "api-key",
        "apikey",
    ]
    .iter()
    .filter_map(|key| lowercase.find(key).map(|position| (position, key.len())))
    .min_by_key(|(position, _)| *position);
    if let Some((position, key_len)) = sensitive {
        result.truncate(position + key_len);
        result.push_str("=<redacted>");
    }
    bounded(&result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_drop_credentials_and_queries() {
        assert_eq!(
            url("wss://user:pass@example.com/a?token=secret#fragment"),
            "wss://example.com/a"
        );
        assert_eq!(url("not a url?token=secret"), "<redacted-url>");
        assert_eq!(url("wss://user:password/secret@host"), "<redacted-url>");
        assert_eq!(
            url("wss://[::1]:443/path?token=secret"),
            "wss://[::1]:443/path"
        );
        assert!(url(&format!("wss://host/{}", "é".repeat(1000))).len() <= 512);
    }

    #[test]
    fn errors_keep_diagnostics_and_redact_sensitive_details() {
        assert_eq!(
            error(&"connection reset by peer"),
            "connection reset by peer"
        );
        let summary = error(&"connect failed at wss://user:pass@host/path?key=secret: timed out");
        assert!(!summary.contains("secret") && !summary.contains("pass@"));
        assert!(summary.contains("connect failed") && summary.contains("timed out"));
        for secret in [
            "Authorization: Bearer SECRET",
            "cookie: session=SECRET",
            "access_token=SECRET",
            "password=SECRET",
            "api_key: SECRET",
        ] {
            assert!(!error(&format!("handshake failed: {secret}")).contains("SECRET"));
        }
        let summary = error(&"é".repeat(1000));
        assert!(summary.len() <= 512);
        let summary = error(&format!(
            "connection failed: wss://user:{}@host/",
            "x".repeat(600)
        ));
        assert_eq!(summary, "connection failed: <redacted-url>");
        assert_eq!(
            error(&"错误wss://user:pass@host/path?token=secret"),
            "错误wss://host/path"
        );
    }
}

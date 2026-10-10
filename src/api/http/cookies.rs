//! Optional, isolated HTTP cookie storage.
//!
//! Cookie handling is deliberately opt-in.  [`CookieJar`] keeps only bounded,
//! in-memory state; applications that need persistence can serialize the
//! redacted-by-debug [`CookieJarSnapshot`] on their own worker.

use crate::api::error::{ErrorKind, ErrorStage, NetError};
use cookie::{Cookie as RawCookie, SameSite};
use http::{HeaderMap, HeaderValue, Method};
use serde::de::{Error as DeserializeError, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::sync::{Arc, Mutex};
use url::{Host, Url};

const DEFAULT_MAX_COOKIE_BYTES: usize = 16 * 1024;

/// Context supplied by an API caller for SameSite cookie selection.
#[derive(Clone, Eq, PartialEq)]
pub struct CookieRequestContext {
    /// The site which initiated this request.  `None` is the normal SDK API
    /// context and is treated as same-site with the target URL.
    pub site: Option<Url>,
    /// Whether this request is a top-level navigation.  Native API clients
    /// should normally leave this `false`.
    pub top_level_navigation: bool,
}

impl fmt::Debug for CookieRequestContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CookieRequestContext")
            .field("site_configured", &self.site.is_some())
            .field("top_level_navigation", &self.top_level_navigation)
            .finish()
    }
}

impl CookieRequestContext {
    /// Construct the default same-site API request context.
    pub fn same_site() -> Self {
        Self {
            site: None,
            top_level_navigation: false,
        }
    }

    /// Declare the site which initiated a request and whether it is a
    /// top-level navigation.
    pub fn initiated_by(site: Url, top_level_navigation: bool) -> Self {
        Self {
            site: Some(site),
            top_level_navigation,
        }
    }
}

impl Default for CookieRequestContext {
    fn default() -> Self {
        Self::same_site()
    }
}

/// Errors returned by a [`CookieStore`] implementation.
pub type CookieStoreResult<T> = Result<T, NetError>;

/// A thread-safe cookie provider used by an HTTP client.
///
/// Implementations must keep these methods bounded and non-blocking.  Durable
/// persistence belongs outside the HTTP worker, using [`CookieJar::snapshot`].
/// Custom stores must enforce domain, path, Secure, expiry, and SameSite rules.
/// The worker bounds headers and catches unwinding panics, discarding arbitrary
/// provider error sources. A header-size error with `ItemTooLarge/RequestBuild`
/// retains that classification; other store failures become `ProviderFailed`.
pub trait CookieStore: Send + Sync + 'static {
    /// Build a `Cookie` header for one transport attempt.
    fn cookies_for_request(
        &self,
        url: &Url,
        method: &Method,
        context: &CookieRequestContext,
    ) -> CookieStoreResult<Option<HeaderValue>>;

    /// Consume `Set-Cookie` headers from one response hop.
    fn set_response_cookies(&self, url: &Url, headers: &HeaderMap) -> CookieStoreResult<()>;
}

/// Resource limits applied by [`CookieJar`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CookieLimits {
    /// Maximum number of unexpired cookies retained by a jar.
    pub max_cookies: usize,
    /// Maximum bytes accepted for one `Set-Cookie` item. Derived paths and
    /// name/value pairs also use this limit; stored host names are capped at 253 bytes.
    pub max_cookie_bytes: usize,
    /// Maximum bytes processed from one response's `Set-Cookie` values.
    pub max_set_cookie_bytes: usize,
    /// Maximum bytes emitted in a generated `Cookie` header.
    pub max_cookie_header_bytes: usize,
}

impl Default for CookieLimits {
    fn default() -> Self {
        Self {
            max_cookies: 1024,
            max_cookie_bytes: DEFAULT_MAX_COOKIE_BYTES,
            max_set_cookie_bytes: 64 * 1024,
            max_cookie_header_bytes: 64 * 1024,
        }
    }
}

impl CookieLimits {
    /// Validate all limits before constructing or installing a jar.
    pub fn validate(&self) -> Result<(), NetError> {
        if self.max_cookies == 0 {
            return Err(invalid_config("cookie_limits.max_cookies"));
        }
        if self.max_cookie_bytes == 0 {
            return Err(invalid_config("cookie_limits.max_cookie_bytes"));
        }
        if self.max_set_cookie_bytes == 0 {
            return Err(invalid_config("cookie_limits.max_set_cookie_bytes"));
        }
        if self.max_cookie_header_bytes == 0 {
            return Err(invalid_config("cookie_limits.max_cookie_header_bytes"));
        }
        Ok(())
    }
}

/// Versioned persistence payload for [`CookieJar`].
///
/// Serialization contains cookie secrets and must be handled like credentials.
/// Debug and deserialization errors never include names, values, or origins.
/// Only persistent cookies are exported; expiry is an absolute UTC timestamp.
/// Decoding and restoration use default limits, including for snapshots from
/// jars with larger custom limits. Applications must also bound serialized
/// input bytes: a deserializer may buffer input before calling our visitors.
#[derive(Clone, Serialize)]
pub struct CookieJarSnapshot {
    schema_version: u32,
    cookies: Vec<SnapshotRecord>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotRecord {
    #[serde(deserialize_with = "deserialize_cookie_field")]
    name: String,
    #[serde(deserialize_with = "deserialize_cookie_field")]
    value: String,
    #[serde(deserialize_with = "deserialize_domain")]
    domain: String,
    host_only: bool,
    #[serde(deserialize_with = "deserialize_cookie_field")]
    path: String,
    secure: bool,
    http_only: bool,
    same_site: Option<SnapshotSameSite>,
    expires_unix: i64,
}

fn deserialize_cookie_field<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_string::<D, DEFAULT_MAX_COOKIE_BYTES>(deserializer)
}

fn deserialize_domain<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_string::<D, 253>(deserializer)
}

fn deserialize_bounded_string<'de, D, const LIMIT: usize>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoundedStringVisitor<const LIMIT: usize>;
    impl<'de, const LIMIT: usize> Visitor<'de> for BoundedStringVisitor<LIMIT> {
        type Value = String;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded cookie field")
        }

        fn visit_str<E: DeserializeError>(self, value: &str) -> Result<String, E> {
            if value.len() > LIMIT {
                return Err(E::custom("invalid cookie snapshot"));
            }
            let mut owned = String::new();
            owned
                .try_reserve(value.len())
                .map_err(|_| E::custom("invalid cookie snapshot"))?;
            owned.push_str(value);
            Ok(owned)
        }

        fn visit_string<E: DeserializeError>(self, value: String) -> Result<String, E> {
            if value.len() > LIMIT {
                return Err(E::custom("invalid cookie snapshot"));
            }
            Ok(value)
        }
    }
    deserializer.deserialize_str(BoundedStringVisitor::<LIMIT>)
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SnapshotSameSite {
    Strict,
    Lax,
    None,
}

impl<'de> Deserialize<'de> for CookieJarSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireSnapshot {
            schema_version: u32,
            #[serde(deserialize_with = "deserialize_records")]
            cookies: Vec<SnapshotRecord>,
        }
        // Strip every nested decoder error: third-party errors may quote the
        // value that failed to parse, including a cookie or entire record.
        let wire = WireSnapshot::deserialize(deserializer)
            .map_err(|_| D::Error::custom("invalid cookie snapshot"))?;
        let snapshot = Self {
            schema_version: wire.schema_version,
            cookies: wire.cookies,
        };
        snapshot
            .validate()
            .map_err(|_| D::Error::custom("invalid cookie snapshot"))?;
        Ok(snapshot)
    }
}

fn deserialize_records<'de, D>(deserializer: D) -> Result<Vec<SnapshotRecord>, D::Error>
where
    D: Deserializer<'de>,
{
    // Once full, reject the next element without asking its deserializer to
    // parse or allocate any of its fields. SeqAccess still allows end-of-list.
    struct ExcessRecord;
    impl<'de> Deserialize<'de> for ExcessRecord {
        fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
            Err(D::Error::custom("invalid cookie snapshot"))
        }
    }
    struct RecordsVisitor;
    impl<'de> Visitor<'de> for RecordsVisitor {
        type Value = Vec<SnapshotRecord>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded cookie snapshot")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let limit = CookieLimits::default().max_cookies;
            let mut records = Vec::new();
            while records.len() < limit {
                let Some(record) = sequence.next_element::<SnapshotRecord>()? else {
                    return Ok(records);
                };
                records
                    .try_reserve(1)
                    .map_err(|_| A::Error::custom("invalid cookie snapshot"))?;
                records.push(record);
            }
            sequence.next_element::<ExcessRecord>()?;
            Ok(records)
        }
    }
    deserializer.deserialize_seq(RecordsVisitor)
}

impl CookieJarSnapshot {
    fn validate(&self) -> Result<(), NetError> {
        let limits = CookieLimits::default();
        if self.schema_version != 1 || self.cookies.len() > limits.max_cookies {
            return Err(snapshot_error());
        }
        for (index, record) in self.cookies.iter().enumerate() {
            if !record.valid(limits.max_cookie_bytes)
                || self.cookies.iter().take(index).any(|other| {
                    other.name == record.name
                        && other.domain == record.domain
                        && other.path == record.path
                })
            {
                return Err(snapshot_error());
            }
        }
        Ok(())
    }
}

impl fmt::Debug for CookieJarSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CookieJarSnapshot")
            .field("schema_version", &self.schema_version)
            .field("cookie_count", &self.cookies.len())
            .finish()
    }
}

struct StoredCookie {
    name: String,
    value: String,
    domain: String,
    host_only: bool,
    path: String,
    secure: bool,
    http_only: bool,
    same_site: Option<SameSite>,
    expires_unix: Option<i64>,
}

/// An isolated, thread-safe in-memory cookie jar.
///
/// `new` creates independent state. Cloning a jar explicitly shares its state;
/// restoring a snapshot creates a fresh independent jar. Replacement preserves
/// original creation order, and expired entries are pruned on every operation.
#[derive(Clone)]
pub struct CookieJar {
    // A flat, bounded table keeps one global creation order, including across
    // domains and paths. Each operation releases the lock before returning.
    state: Arc<Mutex<Vec<StoredCookie>>>,
    limits: CookieLimits,
}

impl fmt::Debug for CookieJar {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CookieJar")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl Default for CookieJar {
    fn default() -> Self {
        Self::new()
    }
}

impl CookieJar {
    /// Create an empty independent jar with [`CookieLimits::default`].
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(Vec::new())),
            limits: CookieLimits::default(),
        }
    }

    /// Create an empty independent jar with validated resource limits.
    pub fn with_limits(limits: CookieLimits) -> Result<Self, NetError> {
        limits.validate()?;
        Ok(Self {
            state: Arc::new(Mutex::new(Vec::new())),
            limits,
        })
    }

    /// Return the resource limits used by this jar.
    pub fn limits(&self) -> CookieLimits {
        self.limits
    }

    /// Export unexpired persistent cookies in stable creation order.
    ///
    /// This does not perform durable storage. Serialize and save the result
    /// outside the HTTP worker, using the application's own I/O policy.
    pub fn snapshot(&self) -> Result<CookieJarSnapshot, NetError> {
        let mut state = self.state.lock().map_err(|_| provider_error())?;
        prune_expired(&mut state);
        let mut cookies = Vec::new();
        for entry in state.iter() {
            if let Some(expires_unix) = entry.expires_unix {
                cookies.try_reserve(1).map_err(|_| resource_error())?;
                cookies.push(SnapshotRecord {
                    name: entry.name.clone(),
                    value: entry.value.clone(),
                    domain: entry.domain.clone(),
                    host_only: entry.host_only,
                    path: entry.path.clone(),
                    secure: entry.secure,
                    http_only: entry.http_only,
                    same_site: entry.same_site.map(SnapshotSameSite::from),
                    expires_unix,
                });
            }
        }
        Ok(CookieJarSnapshot {
            schema_version: 1,
            cookies,
        })
    }

    /// Restore persistent cookies with default resource limits.
    ///
    /// Every record is validated before any state is installed. Expired
    /// records are omitted; absolute expiration never restarts on restore.
    pub fn from_snapshot(snapshot: CookieJarSnapshot) -> Result<Self, NetError> {
        snapshot.validate()?;
        let mut entries = Vec::new();
        entries
            .try_reserve(snapshot.cookies.len())
            .map_err(|_| resource_error())?;
        for record in snapshot.cookies {
            entries.push(StoredCookie {
                name: record.name,
                value: record.value,
                domain: record.domain,
                host_only: record.host_only,
                path: record.path,
                secure: record.secure,
                http_only: record.http_only,
                same_site: record.same_site.map(SameSite::from),
                expires_unix: Some(record.expires_unix),
            });
        }
        prune_expired(&mut entries);
        Ok(Self {
            state: Arc::new(Mutex::new(entries)),
            limits: CookieLimits::default(),
        })
    }

    /// Remove all cookies, also affecting explicit clones of this jar.
    pub fn clear(&self) -> Result<(), NetError> {
        self.state.lock().map_err(|_| provider_error())?.clear();
        Ok(())
    }
}

impl CookieStore for CookieJar {
    fn cookies_for_request(
        &self,
        url: &Url,
        method: &Method,
        context: &CookieRequestContext,
    ) -> CookieStoreResult<Option<HeaderValue>> {
        let mut state = self.state.lock().map_err(|_| provider_error())?;
        prune_expired(&mut state);
        if !http_url(url) {
            return Ok(None);
        }
        let relation = site_relation(url, context);
        let mut matching = Vec::new();
        matching
            .try_reserve(state.len())
            .map_err(|_| resource_error())?;
        for entry in state.iter() {
            if entry.matches(url) && same_site_allowed(entry, relation, method, context) {
                matching.push(entry);
            }
        }
        // Stable sort retains the table's creation order for equal lengths.
        matching.sort_by_key(|entry| std::cmp::Reverse(entry.path.len()));
        let mut header = String::new();
        for entry in matching {
            let separator = if header.is_empty() { 0_usize } else { 2_usize };
            let added = separator
                .checked_add(entry.name.len())
                .and_then(|size| size.checked_add(1))
                .and_then(|size| size.checked_add(entry.value.len()))
                .ok_or_else(header_size_error)?;
            let size = header
                .len()
                .checked_add(added)
                .ok_or_else(header_size_error)?;
            if size > self.limits.max_cookie_header_bytes {
                return Err(header_size_error());
            }
            header.try_reserve(added).map_err(|_| resource_error())?;
            if separator != 0 {
                header.push_str("; ");
            }
            header.push_str(&entry.name);
            header.push('=');
            header.push_str(&entry.value);
        }
        if header.is_empty() {
            return Ok(None);
        }
        let mut value = HeaderValue::from_str(&header).map_err(|_| provider_error())?;
        value.set_sensitive(true);
        Ok(Some(value))
    }

    fn set_response_cookies(&self, url: &Url, headers: &HeaderMap) -> CookieStoreResult<()> {
        let mut state = self.state.lock().map_err(|_| provider_error())?;
        prune_expired(&mut state);
        if !http_url(url) {
            return Ok(());
        }
        let mut total_bytes = 0_usize;
        for header in headers.get_all(http::header::SET_COOKIE) {
            // Prefix budget includes invalid and oversized values, so an
            // attacker cannot make the parser inspect unbounded later items.
            total_bytes = match total_bytes.checked_add(header.as_bytes().len()) {
                Some(total) if total <= self.limits.max_set_cookie_bytes => total,
                _ => break,
            };
            if header.as_bytes().is_empty()
                || header.as_bytes().len() > self.limits.max_cookie_bytes
            {
                continue;
            }
            let entry = match header
                .to_str()
                .ok()
                .and_then(|value| parse_cookie(value, url))
            {
                Some(entry) => entry,
                None => continue,
            };
            // Default domain/path metadata comes from the response URL and
            // need not appear in the bounded Set-Cookie field itself.
            if !bounded_fields(
                &entry.name,
                &entry.value,
                &entry.domain,
                &entry.path,
                self.limits.max_cookie_bytes,
            ) {
                continue;
            }
            // An insecure response cannot replace or delete a secure cookie
            // with overlapping name/domain/path scope.
            if url.scheme() != "https"
                && state.iter().any(|old| {
                    old.secure
                        && old.name == entry.name
                        && (domain_matches(&old.domain, &entry.domain)
                            || domain_matches(&entry.domain, &old.domain))
                        && path_matches(&entry.path, &old.path)
                })
            {
                continue;
            }
            let existing = state.iter().position(|old| {
                old.name == entry.name && old.domain == entry.domain && old.path == entry.path
            });
            if entry.expired(now_unix()) {
                if let Some(index) = existing {
                    state.remove(index);
                }
            } else if let Some(index) = existing {
                if let Some(slot) = state.get_mut(index) {
                    *slot = entry;
                } else {
                    return Err(provider_error());
                }
            } else if state.len() < self.limits.max_cookies {
                state.try_reserve(1).map_err(|_| resource_error())?;
                state.push(entry);
            }
        }
        Ok(())
    }
}

fn parse_cookie(value: &str, url: &Url) -> Option<StoredCookie> {
    let raw = RawCookie::parse(value).ok()?;
    if !valid_pair(raw.name(), raw.value())
        || (raw.secure() == Some(true) && url.scheme() != "https")
        || (raw.same_site() == Some(SameSite::None) && raw.secure() != Some(true))
        || !valid_prefix(
            raw.name(),
            raw.secure() == Some(true),
            raw.domain().is_none(),
            raw.path() == Some("/"),
        )
    {
        return None;
    }
    let parsed = cookie_store::Cookie::try_from_raw_cookie(&raw, url).ok()?;
    let (domain, host_only) = match parsed.domain {
        cookie_store::CookieDomain::HostOnly(domain) => (domain, true),
        cookie_store::CookieDomain::Suffix(domain) => {
            if !valid_domain_scope(&domain) {
                return None;
            }
            (domain, false)
        }
        _ => return None,
    };
    let path = parsed.path.as_ref().to_owned();
    if !valid_path(&path) {
        return None;
    }
    let expires_unix = match parsed.expires {
        cookie_store::CookieExpiration::AtUtc(time) => Some(time.unix_timestamp()),
        cookie_store::CookieExpiration::SessionEnd => None,
    };
    Some(StoredCookie {
        name: raw.name().to_owned(),
        value: raw.value().to_owned(),
        domain,
        host_only,
        path,
        secure: raw.secure() == Some(true),
        http_only: raw.http_only() == Some(true),
        same_site: raw.same_site(),
        expires_unix,
    })
}

impl StoredCookie {
    fn expired(&self, now: i64) -> bool {
        self.expires_unix.is_some_and(|expiry| expiry <= now)
    }

    fn matches(&self, url: &Url) -> bool {
        let Some(host) = url.host_str() else {
            return false;
        };
        (self.domain == host || (!self.host_only && domain_matches(host, &self.domain)))
            && path_matches(url.path(), &self.path)
            && (!self.secure || url.scheme() == "https")
    }
}

impl SnapshotRecord {
    fn valid(&self, maximum_bytes: usize) -> bool {
        let valid_host = Host::parse(&self.domain)
            .ok()
            .is_some_and(|host| host.to_string() == self.domain);
        bounded_fields(
            &self.name,
            &self.value,
            &self.domain,
            &self.path,
            maximum_bytes,
        ) && valid_pair(&self.name, &self.value)
            && valid_host
            && !self.domain.is_empty()
            && (self.host_only || valid_domain_scope(&self.domain))
            && valid_path(&self.path)
            && (!matches!(self.same_site, Some(SnapshotSameSite::None)) || self.secure)
            && valid_prefix(&self.name, self.secure, self.host_only, self.path == "/")
            && cookie::time::OffsetDateTime::from_unix_timestamp(self.expires_unix).is_ok()
    }
}

impl From<SameSite> for SnapshotSameSite {
    fn from(value: SameSite) -> Self {
        match value {
            SameSite::Strict => Self::Strict,
            SameSite::Lax => Self::Lax,
            SameSite::None => Self::None,
        }
    }
}
impl From<SnapshotSameSite> for SameSite {
    fn from(value: SnapshotSameSite) -> Self {
        match value {
            SnapshotSameSite::Strict => Self::Strict,
            SnapshotSameSite::Lax => Self::Lax,
            SnapshotSameSite::None => Self::None,
        }
    }
}

#[derive(Clone, Copy)]
enum SiteRelation {
    Same,
    Cross,
    Unknown,
}

fn same_site_allowed(
    entry: &StoredCookie,
    relation: SiteRelation,
    method: &Method,
    context: &CookieRequestContext,
) -> bool {
    match entry.same_site {
        Some(SameSite::Strict) => matches!(relation, SiteRelation::Same),
        Some(SameSite::Lax) => {
            matches!(relation, SiteRelation::Same)
                || (matches!(relation, SiteRelation::Cross)
                    && context.top_level_navigation
                    && matches!(
                        *method,
                        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
                    ))
        }
        Some(SameSite::None) => entry.secure,
        // Native API requests preserve RFC6265 semantics for absent SameSite;
        // browser callers must declare their own navigation context explicitly.
        None => true,
    }
}

fn site_relation(target: &Url, context: &CookieRequestContext) -> SiteRelation {
    let Some(site) = context.site.as_ref() else {
        return SiteRelation::Same;
    };
    match (schemeful_site(site), schemeful_site(target)) {
        (Some(left), Some(right)) if left == right => SiteRelation::Same,
        (Some(_), Some(_)) => SiteRelation::Cross,
        _ => SiteRelation::Unknown,
    }
}

fn schemeful_site(url: &Url) -> Option<(&str, String)> {
    if !http_url(url) {
        return None;
    }
    let domain = match url.host()? {
        Host::Domain(host) => known_registrable_domain(host)?.to_owned(),
        Host::Ipv4(ip) => ip.to_string(),
        Host::Ipv6(ip) => ip.to_string(),
    };
    Some((url.scheme(), domain))
}

fn known_registrable_domain(host: &str) -> Option<&str> {
    let suffix = psl::suffix(host.as_bytes())?;
    if !suffix.is_known() {
        return None;
    }
    psl::domain_str(host)
}

fn valid_domain_scope(domain: &str) -> bool {
    matches!(Host::parse(domain), Ok(Host::Domain(_)))
        && !domain.ends_with('.')
        // The PSL implicit wildcard still rejects a bare unknown suffix.
        // SameSite site inference is stricter and requires a known PSL rule.
        && psl::domain_str(domain).is_some()
}

fn bounded_fields(name: &str, value: &str, domain: &str, path: &str, limit: usize) -> bool {
    name.len().saturating_add(value.len()).saturating_add(1) <= limit
        && domain.len() <= 253
        && path.len() <= limit
}

fn domain_matches(host: &str, domain: &str) -> bool {
    host == domain
        || host
            .strip_suffix(domain)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn path_matches(request: &str, cookie: &str) -> bool {
    request == cookie
        || request
            .strip_prefix(cookie)
            .is_some_and(|remaining| cookie.ends_with('/') || remaining.starts_with('/'))
}

fn valid_pair(name: &str, value: &str) -> bool {
    let valid_name = !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte));
    let value = value
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .map_or(value, |inner| inner);
    valid_name
        && value.bytes().all(
            |byte| matches!(byte, 0x21 | 0x23..=0x2b | 0x2d..=0x3a | 0x3c..=0x5b | 0x5d..=0x7e),
        )
}

fn valid_path(path: &str) -> bool {
    path.starts_with('/')
        && path
            .bytes()
            .all(|byte| matches!(byte, 0x20..=0x3a | 0x3c..=0x7e))
}

fn valid_prefix(name: &str, secure: bool, host_only: bool, root_path: bool) -> bool {
    (!name.starts_with("__Secure-") || secure)
        && (!name.starts_with("__Host-") || (secure && host_only && root_path))
}

fn http_url(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https") && url.host().is_some()
}
fn now_unix() -> i64 {
    cookie::time::OffsetDateTime::now_utc().unix_timestamp()
}
fn prune_expired(entries: &mut Vec<StoredCookie>) {
    let now = now_unix();
    entries.retain(|entry| !entry.expired(now));
}
fn invalid_config(field: &'static str) -> NetError {
    NetError::config(field, "must be greater than zero")
}
fn snapshot_error() -> NetError {
    NetError::from(ErrorKind::InvalidInput).with_stage(ErrorStage::Provider)
}
fn provider_error() -> NetError {
    NetError::from(ErrorKind::ProviderFailed).with_stage(ErrorStage::Provider)
}
fn resource_error() -> NetError {
    NetError::from(ErrorKind::ResourceExhausted).with_stage(ErrorStage::Provider)
}
fn header_size_error() -> NetError {
    NetError::from(ErrorKind::ItemTooLarge).with_stage(ErrorStage::RequestBuild)
}

#[cfg(test)]
#[path = "cookies_tests.rs"]
mod tests;

use super::*;
use std::error::Error;
use std::io;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

fn check(condition: bool, message: &'static str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message).into())
    }
}

fn put(jar: &CookieJar, origin: &str, values: &[&str]) -> TestResult {
    let mut headers = HeaderMap::new();
    for value in values {
        headers.append(http::header::SET_COOKIE, HeaderValue::from_str(value)?);
    }
    jar.set_response_cookies(&Url::parse(origin)?, &headers)?;
    Ok(())
}

fn header(
    jar: &CookieJar,
    target: &str,
    method: Method,
    context: CookieRequestContext,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    jar.cookies_for_request(&Url::parse(target)?, &method, &context)?
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()
        .map_err(Into::into)
}

fn same(jar: &CookieJar, target: &str) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    header(jar, target, Method::GET, CookieRequestContext::same_site())
}

#[test]
fn host_only_domain_default_path_and_path_boundary_are_respected() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://api.example.com/account/login",
        &["host=1", "domain=2; Domain=.example.com; Path=/"],
    )?;
    check(
        same(&jar, "https://api.example.com/account/profile")?.as_deref()
            == Some("host=1; domain=2"),
        "default path or ordering mismatch",
    )?;
    check(
        same(&jar, "https://child.example.com/account/profile")?.as_deref() == Some("domain=2"),
        "host-only cookie leaked",
    )?;
    check(
        same(&jar, "https://api.example.com/accounting")?.as_deref() == Some("domain=2"),
        "path prefix crossed segment boundary",
    )?;
    check(
        same(&jar, "https://badexample.com/")?.is_none(),
        "domain match omitted label boundary",
    )
}

#[test]
fn unrelated_and_public_suffix_domains_are_rejected() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://account.example.co.uk/",
        &[
            "a=1; Domain=co.uk",
            "b=2; Domain=com",
            "c=3; Domain=other.co.uk",
            "d=4; Domain=example.co.uk",
        ],
    )?;
    check(
        same(&jar, "https://account.example.co.uk/")?.as_deref() == Some("d=4"),
        "public suffix or unrelated domain accepted",
    )?;
    let private = CookieJar::new();
    put(
        &private,
        "https://one.github.io/",
        &["a=1; Domain=github.io"],
    )?;
    check(
        same(&private, "https://two.github.io/")?.is_none(),
        "private public suffix accepted",
    )
}

#[test]
fn secure_cookies_require_secure_receipt_and_transport() -> TestResult {
    let jar = CookieJar::new();
    put(&jar, "http://example.com/", &["insecure=bad; Secure"])?;
    put(
        &jar,
        "https://example.com/",
        &["secure=good; Secure", "plain=yes"],
    )?;
    check(
        same(&jar, "http://example.com/")?.as_deref() == Some("plain=yes"),
        "secure cookie sent over HTTP",
    )?;
    check(
        same(&jar, "https://example.com/")?.as_deref() == Some("secure=good; plain=yes"),
        "secure cookie accepted over HTTP",
    )
}

#[test]
fn max_age_overrides_expires_and_deletion_reuses_capacity() -> TestResult {
    let jar = CookieJar::with_limits(CookieLimits {
        max_cookies: 1,
        ..CookieLimits::default()
    })?;
    put(
        &jar,
        "https://example.com/",
        &["a=1; Max-Age=3600; Expires=Thu, 01 Jan 1970 00:00:00 GMT"],
    )?;
    check(
        same(&jar, "https://example.com/")?.as_deref() == Some("a=1"),
        "Max-Age did not take precedence",
    )?;
    put(
        &jar,
        "https://example.com/",
        &[
            "a=x; Max-Age=0; Expires=Thu, 01 Jan 2099 00:00:00 GMT",
            "b=2",
        ],
    )?;
    check(
        same(&jar, "https://example.com/")?.as_deref() == Some("b=2"),
        "deletion retained capacity",
    )?;
    put(&jar, "https://example.com/", &["b=x; Max-Age=-1"])?;
    check(
        same(&jar, "https://example.com/")?.is_none(),
        "negative Max-Age did not delete",
    )
}

#[test]
fn duplicate_cookie_replacement_preserves_global_creation_order() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://api.example.com/",
        &[
            "first=1; Domain=example.com; Path=/",
            "second=2; Path=/",
            "third=3; Domain=example.com; Path=/",
            "first=new; Domain=example.com; Path=/",
        ],
    )?;
    check(
        same(&jar, "https://api.example.com/")?.as_deref() == Some("first=new; second=2; third=3"),
        "creation order was grouped by domain",
    )
}

#[test]
fn longer_paths_precede_shorter_paths_with_stable_ties() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://example.com/a/b",
        &[
            "root=1; Path=/",
            "first=2; Path=/a",
            "second=3; Path=/a",
            "deep=4; Path=/a/b",
        ],
    )?;
    check(
        same(&jar, "https://example.com/a/b")?.as_deref()
            == Some("deep=4; first=2; second=3; root=1"),
        "cookie path order was unstable",
    )
}

#[test]
fn samesite_uses_registrable_domain_and_scheme() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://account.example.co.uk/",
        &[
            "strict=1; SameSite=Strict",
            "lax=2; SameSite=Lax",
            "none=3; SameSite=None; Secure",
        ],
    )?;
    let request = |site: &str, top| -> Result<CookieRequestContext, url::ParseError> {
        Ok(CookieRequestContext::initiated_by(Url::parse(site)?, top))
    };
    let related = header(
        &jar,
        "https://account.example.co.uk/",
        Method::GET,
        request("https://login.example.co.uk/", false)?,
    )?;
    check(
        related.as_deref() == Some("strict=1; lax=2; none=3"),
        "same registrable site rejected",
    )?;
    let cross = header(
        &jar,
        "https://account.example.co.uk/",
        Method::GET,
        request("https://other.co.uk/", false)?,
    )?;
    check(
        cross.as_deref() == Some("none=3"),
        "co.uk treated as registrable site",
    )?;
    let insecure = header(
        &jar,
        "https://account.example.co.uk/",
        Method::GET,
        request("http://account.example.co.uk/", false)?,
    )?;
    check(
        insecure.as_deref() == Some("none=3"),
        "schemeful site comparison ignored scheme",
    )
}

#[test]
fn lax_cross_site_top_level_navigation_requires_safe_method() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://example.com/",
        &[
            "strict=1; SameSite=Strict",
            "lax=2; SameSite=Lax",
            "none=3; SameSite=None; Secure",
            "bad=4; SameSite=None",
        ],
    )?;
    let context = CookieRequestContext::initiated_by(Url::parse("https://other.com/")?, true);
    for method in [Method::GET, Method::HEAD, Method::OPTIONS, Method::TRACE] {
        check(
            header(&jar, "https://example.com/", method, context.clone())?.as_deref()
                == Some("lax=2; none=3"),
            "safe navigation lost Lax or leaked Strict",
        )?;
    }
    for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
        check(
            header(&jar, "https://example.com/", method, context.clone())?.as_deref()
                == Some("none=3"),
            "unsafe navigation sent Lax",
        )?;
    }
    Ok(())
}

#[test]
fn unknown_or_opaque_sites_are_conservative_for_strict_and_lax() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://one.unknownsuffix/",
        &[
            "strict=1; SameSite=Strict",
            "lax=2; SameSite=Lax",
            "none=3; SameSite=None; Secure",
        ],
    )?;
    for site in [
        "https://one.unknownsuffix/",
        "https://two.unknownsuffix/",
        "data:text/plain,test",
    ] {
        let context = CookieRequestContext::initiated_by(Url::parse(site)?, true);
        check(
            header(&jar, "https://one.unknownsuffix/", Method::GET, context)?.as_deref()
                == Some("none=3"),
            "unknown site allowed Strict or Lax",
        )?;
    }
    Ok(())
}

#[test]
fn private_suffix_and_ip_sites_are_distinguished() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://one.github.io/",
        &["strict=1; SameSite=Strict"],
    )?;
    let cross = CookieRequestContext::initiated_by(Url::parse("https://two.github.io/")?, false);
    check(
        header(&jar, "https://one.github.io/", Method::GET, cross)?.is_none(),
        "private suffix sites were conflated",
    )?;
    let ip = CookieJar::new();
    put(&ip, "https://127.0.0.1/", &["strict=1; SameSite=Strict"])?;
    let same_ip = CookieRequestContext::initiated_by(Url::parse("https://127.0.0.1:444/")?, false);
    check(
        header(&ip, "https://127.0.0.1/", Method::GET, same_ip)?.as_deref() == Some("strict=1"),
        "same IP site rejected",
    )?;
    let other_ip = CookieRequestContext::initiated_by(Url::parse("https://127.0.0.2/")?, false);
    check(
        header(&ip, "https://127.0.0.1/", Method::GET, other_ip)?.is_none(),
        "different IP site accepted",
    )
}

#[test]
fn malformed_cookie_items_are_ignored_without_failing_response() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://example.com/",
        &["invalid", "bad name=1", "a=bad,value", "valid=yes"],
    )?;
    check(
        same(&jar, "https://example.com/")?.as_deref() == Some("valid=yes"),
        "invalid cookie pair was retained",
    )
}

#[test]
fn non_http_urls_do_not_receive_or_store_cookies() -> TestResult {
    let jar = CookieJar::new();
    put(&jar, "ftp://example.com/", &["a=1"])?;
    check(
        same(&jar, "https://example.com/")?.is_none(),
        "non-HTTP source set cookie",
    )?;
    put(&jar, "https://example.com/", &["b=2"])?;
    check(
        same(&jar, "ftp://example.com/")?.is_none(),
        "cookie sent over non-HTTP scheme",
    )
}

#[test]
fn limits_reject_zero_and_preserve_previous_cookies_at_capacity() -> TestResult {
    for limits in [
        CookieLimits {
            max_cookies: 0,
            ..CookieLimits::default()
        },
        CookieLimits {
            max_cookie_bytes: 0,
            ..CookieLimits::default()
        },
        CookieLimits {
            max_set_cookie_bytes: 0,
            ..CookieLimits::default()
        },
        CookieLimits {
            max_cookie_header_bytes: 0,
            ..CookieLimits::default()
        },
    ] {
        check(
            matches!(CookieJar::with_limits(limits), Err(ref e) if e.kind() == ErrorKind::InvalidConfig),
            "zero limit accepted",
        )?;
    }
    let jar = CookieJar::with_limits(CookieLimits {
        max_cookies: 1,
        ..CookieLimits::default()
    })?;
    put(&jar, "https://example.com/", &["a=1", "b=2", "a=3"])?;
    check(
        same(&jar, "https://example.com/")?.as_deref() == Some("a=3"),
        "capacity rejected replacement or evicted old cookie",
    )
}

#[test]
fn single_response_and_generated_header_byte_limits_are_enforced() -> TestResult {
    let jar = CookieJar::with_limits(CookieLimits {
        max_cookie_bytes: 5,
        max_set_cookie_bytes: 12,
        ..CookieLimits::default()
    })?;
    put(&jar, "https://example.com/", &["too=long", "a=1", "b=2"])?;
    check(
        same(&jar, "https://example.com/")?.as_deref() == Some("a=1"),
        "Set-Cookie prefix budget mismatch",
    )?;
    let short = CookieJar::with_limits(CookieLimits {
        max_cookie_header_bytes: 3,
        ..CookieLimits::default()
    })?;
    put(&short, "https://example.com/", &["a=1"])?;
    check(
        same(&short, "https://example.com/")?.as_deref() == Some("a=1"),
        "exact header size limit rejected",
    )?;
    put(&short, "https://example.com/", &["b=2"])?;
    let result = short.cookies_for_request(
        &Url::parse("https://example.com/")?,
        &Method::GET,
        &Default::default(),
    );
    check(
        matches!(result, Err(ref e) if e.kind() == ErrorKind::ItemTooLarge && e.context().stage == Some(ErrorStage::RequestBuild)),
        "header overflow lacks structured build error",
    )
}

#[test]
fn clone_shares_but_independent_jars_and_restores_are_isolated() -> TestResult {
    let jar = CookieJar::new();
    let clone = jar.clone();
    put(&jar, "https://example.com/", &["a=1; Max-Age=3600"])?;
    let restore = CookieJar::from_snapshot(jar.snapshot()?)?;
    check(
        same(&clone, "https://example.com/")?.as_deref() == Some("a=1"),
        "clone did not share state",
    )?;
    check(
        same(&CookieJar::new(), "https://example.com/")?.is_none(),
        "independent jar shared state",
    )?;
    clone.clear()?;
    check(
        same(&jar, "https://example.com/")?.is_none(),
        "clear did not affect clone",
    )?;
    check(
        same(&restore, "https://example.com/")?.as_deref() == Some("a=1"),
        "restore shared source state",
    )
}

#[test]
fn snapshot_round_trip_retains_attributes_and_only_persistent_cookies() -> TestResult {
    let jar = CookieJar::new();
    put(&jar, "https://api.example.com/area/login", &["session=omit", "persist=secret; Domain=example.com; Path=/area; Secure; HttpOnly; SameSite=Strict; Max-Age=3600", "gone=omit; Max-Age=0"])?;
    let snapshot = jar.snapshot()?;
    let encoded = serde_json::to_string(&snapshot)?;
    check(
        !encoded.contains("session") && !encoded.contains("gone"),
        "nonpersistent or expired cookie exported",
    )?;
    check(
        !format!("{snapshot:?}").contains("secret") && !format!("{jar:?}").contains("persist"),
        "debug leaked cookie",
    )?;
    let restored = CookieJar::from_snapshot(serde_json::from_str(&encoded)?)?;
    check(
        same(&restored, "https://other.example.com/area/item")?.as_deref()
            == Some("persist=secret"),
        "snapshot lost domain/path metadata",
    )?;
    check(
        same(&restored, "http://other.example.com/area/item")?.is_none(),
        "snapshot lost Secure",
    )?;
    let context = CookieRequestContext::initiated_by(Url::parse("https://elsewhere.com/")?, false);
    check(
        header(
            &restored,
            "https://other.example.com/area/item",
            Method::GET,
            context,
        )?
        .is_none(),
        "snapshot lost SameSite",
    )
}

#[test]
fn snapshot_has_owned_schema_and_absolute_expiry() -> TestResult {
    let jar = CookieJar::new();
    put(&jar, "https://example.com/", &["a=1; Max-Age=3600"])?;
    let first = serde_json::to_value(jar.snapshot()?)?;
    check(
        first
            .pointer("/schema_version")
            .and_then(serde_json::Value::as_u64)
            == Some(1),
        "snapshot version missing",
    )?;
    check(
        first
            .pointer("/cookies/0/expires_unix")
            .and_then(serde_json::Value::as_i64)
            .is_some(),
        "snapshot does not store absolute expiry",
    )?;
    let restored = CookieJar::from_snapshot(serde_json::from_value(first.clone())?)?;
    let second = serde_json::to_value(restored.snapshot()?)?;
    check(
        first == second,
        "restoring snapshot reset expiry or creation order",
    )
}

#[test]
fn corrupt_snapshot_errors_are_redacted_and_expired_records_are_skipped() -> TestResult {
    for json in [
        r#"{"schema_version":"SECRET_MARKER","cookies":[]}"#,
        r#"{"schema_version":99,"cookies":[]}"#,
        r#"{"schema_version":1,"cookies":[{"secret":"SECRET_MARKER"}]}"#,
    ] {
        let result = serde_json::from_str::<CookieJarSnapshot>(json);
        check(result.is_err(), "malformed snapshot accepted")?;
        if let Err(error) = result {
            check(
                !error.to_string().contains("SECRET_MARKER"),
                "snapshot parse error leaked content",
            )?;
        }
    }
    let jar = CookieJar::new();
    put(&jar, "https://example.com/", &["a=1; Max-Age=3600"])?;
    let mut expired = serde_json::to_value(jar.snapshot()?)?;
    let field = expired
        .pointer_mut("/cookies/0/expires_unix")
        .ok_or_else(|| io::Error::other("expiry field missing"))?;
    *field = serde_json::json!(1);
    let restored = CookieJar::from_snapshot(serde_json::from_value(expired)?)?;
    check(
        same(&restored, "https://example.com/")?.is_none(),
        "expired record restored",
    )
}

#[test]
fn snapshot_rejects_invalid_metadata_and_oversized_records() -> TestResult {
    let jar = CookieJar::new();
    put(&jar, "https://example.com/", &["a=1; Max-Age=3600"])?;
    let original = serde_json::to_value(jar.snapshot()?)?;
    for (field, replacement) in [
        (
            "/cookies/0/domain",
            serde_json::json!("https://evil.com/path"),
        ),
        ("/cookies/0/path", serde_json::json!("relative")),
        (
            "/cookies/0/value",
            serde_json::json!("a\r\nCookie: injected"),
        ),
        ("/cookies/0/name", serde_json::json!("bad name")),
        ("/cookies/0/expires_unix", serde_json::json!(i64::MAX)),
        (
            "/cookies/0/value",
            serde_json::json!("x".repeat(CookieLimits::default().max_cookie_bytes + 1)),
        ),
        (
            "/cookies/0/name",
            serde_json::json!("n".repeat(CookieLimits::default().max_cookie_bytes + 1)),
        ),
        ("/cookies/0/domain", serde_json::json!("d".repeat(254))),
        (
            "/cookies/0/path",
            serde_json::json!(format!(
                "/{}",
                "p".repeat(CookieLimits::default().max_cookie_bytes)
            )),
        ),
    ] {
        let mut bad = original.clone();
        let slot = bad
            .pointer_mut(field)
            .ok_or_else(|| io::Error::other("snapshot field missing"))?;
        *slot = replacement;
        let parsed = serde_json::from_value::<CookieJarSnapshot>(bad);
        let rejected = match parsed {
            Err(_) => true,
            Ok(snapshot) => CookieJar::from_snapshot(snapshot).is_err(),
        };
        check(rejected, "invalid snapshot metadata accepted")?;
    }
    Ok(())
}

#[test]
fn snapshot_record_capacity_accepts_exact_limit_and_rejects_duplicates() -> TestResult {
    let jar = CookieJar::new();
    for number in 0..CookieLimits::default().max_cookies {
        put(
            &jar,
            "https://example.com/",
            &[&format!("c{number}=1; Max-Age=3600")],
        )?;
    }
    let snapshot = jar.snapshot()?;
    let encoded = serde_json::to_vec(&snapshot)?;
    let restored = CookieJar::from_snapshot(serde_json::from_slice(&encoded)?)?;
    check(
        restored.snapshot()?.cookies.len() == CookieLimits::default().max_cookies,
        "snapshot rejected exact record capacity",
    )?;
    let mut duplicate = snapshot;
    duplicate.cookies.truncate(1);
    let first = duplicate
        .cookies
        .first()
        .ok_or_else(|| io::Error::other("missing snapshot record"))?
        .clone();
    duplicate.cookies.push(first);
    let decoded = serde_json::from_slice::<CookieJarSnapshot>(&serde_json::to_vec(&duplicate)?);
    check(
        decoded.is_err(),
        "snapshot accepted ambiguous duplicate identity",
    )?;
    check(
        CookieJar::from_snapshot(duplicate).is_err(),
        "snapshot restore accepted duplicate identity",
    )
}

#[test]
fn concurrent_snapshot_update_and_clear_remain_consistent() -> TestResult {
    let jar = CookieJar::new();
    let mut threads = Vec::new();
    for number in 0..4 {
        let shared = jar.clone();
        threads.push(std::thread::spawn(move || -> TestResult {
            for attempt in 0..40 {
                put(
                    &shared,
                    "https://example.com/",
                    &[&format!("c{number}={attempt}; Max-Age=3600")],
                )?;
                let restored = CookieJar::from_snapshot(shared.snapshot()?)?;
                let _ = same(&restored, "https://example.com/")?;
                if attempt % 5 == 0 {
                    shared.clear()?;
                }
            }
            Ok(())
        }));
    }
    for thread in threads {
        thread
            .join()
            .map_err(|_| io::Error::other("snapshot test worker unwound"))??;
    }
    Ok(())
}

#[test]
fn context_debug_omits_site_credentials_path_and_query() -> TestResult {
    let context = CookieRequestContext::initiated_by(
        Url::parse("https://user:CONTEXT_SECRET@example.com/CONTEXT_SECRET?q=CONTEXT_SECRET")?,
        true,
    );
    check(
        !format!("{context:?}").contains("CONTEXT_SECRET"),
        "Cookie context Debug leaked site secrets",
    )
}

#[test]
fn origin_derived_cookie_metadata_cannot_exceed_item_memory_budget() -> TestResult {
    let jar = CookieJar::with_limits(CookieLimits {
        max_cookie_bytes: 32,
        ..CookieLimits::default()
    })?;
    let target = format!("https://example.com/{}/login", "a".repeat(100));
    put(&jar, &target, &["a=1; Max-Age=3600"])?;
    check(
        same(&jar, &target)?.is_none(),
        "default path bypassed stored-cookie memory limit",
    )
}

#[test]
fn unknown_suffix_does_not_prevent_ordinary_domain_matching() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://a.example.test/",
        &["a=1; Domain=example.test", "bad=2; Domain=test"],
    )?;
    check(
        same(&jar, "https://b.example.test/")?.as_deref() == Some("a=1"),
        "valid private domain cookie was rejected",
    )
}

#[cfg(test)]
#[test]
fn poisoned_cookie_lock_is_reported_without_exposing_state() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://example.com/",
        &["secret=LOCK_SECRET; Max-Age=3600"],
    )?;
    let shared = jar.clone();
    let fault =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<(), NetError> {
            let _guard = shared.state.lock().map_err(|_| provider_error())?;
            std::panic::resume_unwind(Box::new(()));
        }));
    check(fault.is_err(), "test did not poison the lock")?;
    let target = Url::parse("https://example.com/")?;
    let operations = [
        jar.cookies_for_request(&target, &Method::GET, &Default::default())
            .map(|_| ()),
        jar.set_response_cookies(&target, &HeaderMap::new()),
        jar.snapshot().map(|_| ()),
        jar.clear(),
    ];
    for outcome in operations {
        let error = match outcome {
            Err(error) => error,
            Ok(()) => return Err(io::Error::other("poisoned state was used").into()),
        };
        check(
            error.kind() == ErrorKind::ProviderFailed
                && error.context().stage == Some(ErrorStage::Provider),
            "poisoned lock lacks provider classification",
        )?;
        check(
            error.source().is_none() && !format!("{error:?}").contains("LOCK_SECRET"),
            "poison error retained protected state",
        )?;
    }
    Ok(())
}

#[test]
fn snapshot_decoder_rejects_excess_records_before_parsing_their_contents() -> TestResult {
    use serde::de::{value::SeqDeserializer, IntoDeserializer};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordProbe<'a> {
        value: &'a serde_json::Value,
        decoded: &'a AtomicUsize,
    }

    impl<'de> IntoDeserializer<'de, serde_json::Error> for RecordProbe<'de> {
        type Deserializer = Self;
        fn into_deserializer(self) -> Self {
            self
        }
    }

    impl<'de> Deserializer<'de> for RecordProbe<'de> {
        type Error = serde_json::Error;
        fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
            self.decoded.fetch_add(1, Ordering::SeqCst);
            self.value.into_deserializer().deserialize_any(visitor)
        }
        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
            byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
            map struct enum identifier ignored_any
        }
    }

    let value = serde_json::json!({
        "name": "session", "value": "secret", "domain": "example.com",
        "host_only": true, "path": "/", "secure": true, "http_only": true,
        "same_site": "strict", "expires_unix": 2_000_000_000,
    });
    let limit = CookieLimits::default().max_cookies;
    let decoded = AtomicUsize::new(0);
    let records = (0..=limit).map(|_| RecordProbe {
        value: &value,
        decoded: &decoded,
    });
    let outcome = deserialize_records(SeqDeserializer::new(records));
    check(outcome.is_err(), "snapshot decoder accepted excess records")?;
    check(
        decoded.load(Ordering::SeqCst) == limit,
        "snapshot decoder parsed a record beyond its memory budget",
    )
}

#[test]
fn ipv6_host_only_snapshot_and_samesite_keep_the_exact_host() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://[::1]/",
        &["v6=1; SameSite=Strict; Max-Age=3600"],
    )?;
    let restored = CookieJar::from_snapshot(serde_json::from_str(&serde_json::to_string(
        &jar.snapshot()?,
    )?)?)?;
    let same_host = CookieRequestContext::initiated_by(Url::parse("https://[::1]:444/")?, false);
    check(
        header(&restored, "https://[::1]/", Method::GET, same_host)?.as_deref() == Some("v6=1"),
        "IPv6 snapshot lost host-only cookie",
    )?;
    let other_host = CookieRequestContext::initiated_by(Url::parse("https://[::2]/")?, false);
    check(
        header(&restored, "https://[::1]/", Method::GET, other_host)?.is_none(),
        "different IPv6 sites were conflated",
    )
}

#[test]
fn extreme_max_age_values_remain_fallible_and_do_not_overflow() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://example.com/",
        &["long=1; Max-Age=9223372036854775807"],
    )?;
    check(
        same(&jar, "https://example.com/")?.as_deref() == Some("long=1"),
        "large positive expiry was not bounded",
    )?;
    let restored = CookieJar::from_snapshot(jar.snapshot()?)?;
    check(
        same(&restored, "https://example.com/")?.as_deref() == Some("long=1"),
        "large expiry cannot round-trip",
    )?;
    put(
        &jar,
        "https://example.com/",
        &["long=gone; Max-Age=-9223372036854775808"],
    )?;
    check(
        same(&jar, "https://example.com/")?.is_none(),
        "large negative expiry did not delete cookie",
    )
}

#[test]
fn secure_cookie_overwrites_and_cookie_prefixes_reject_insecure_input() -> TestResult {
    let jar = CookieJar::new();
    put(
        &jar,
        "https://example.com/",
        &[
            "session=secure; Secure; Path=/",
            "__Host-good=1; Secure; Path=/",
            "__Host-bad=1; Secure; Domain=example.com; Path=/",
            "__Secure-bad=1",
        ],
    )?;
    put(
        &jar,
        "http://example.com/",
        &[
            "session=attacker; Path=/",
            "session=deleted; Path=/; Max-Age=0",
        ],
    )?;
    check(
        same(&jar, "https://example.com/")?.as_deref() == Some("session=secure; __Host-good=1"),
        "insecure response replaced or deleted a secure cookie",
    )
}

#[test]
fn expired_entries_are_retired_before_admitting_new_names() -> TestResult {
    let jar = CookieJar::with_limits(CookieLimits {
        max_cookies: 4,
        ..CookieLimits::default()
    })?;
    for batch in 0..32 {
        for index in 0..4 {
            put(
                &jar,
                "https://example.com/",
                &[&format!("c{batch}_{index}=1; Max-Age=3600")],
            )?;
        }
        let mut state = jar
            .state
            .lock()
            .map_err(|_| io::Error::other("jar lock failed"))?;
        check(state.len() == 4, "expired entries retained capacity")?;
        // Advance stored expirations deterministically without relying on sleeps.
        for entry in state.iter_mut() {
            entry.expires_unix = Some(1);
        }
    }
    put(&jar, "https://example.com/", &["fresh=1"])?;
    check(
        same(&jar, "https://example.com/")?.as_deref() == Some("fresh=1"),
        "expiry cleanup lost new cookie",
    )?;
    let retained = jar
        .state
        .lock()
        .map_err(|_| io::Error::other("jar lock failed"))?
        .len();
    check(retained == 1, "expired records accumulated invisibly")
}

//! Fixed-clock compatibility and boundary checks; no sleeps or wall-clock tolerances.

use super::{parse_retry_after, parse_retry_after_at};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

fn instant(value: &str) -> TestResult<SystemTime> {
    Ok(httpdate::parse_http_date(value)?)
}

fn now() -> TestResult<SystemTime> {
    instant("Fri, 16 Oct 2026 12:00:00 GMT")?
        .checked_add(Duration::from_millis(250))
        .ok_or_else(|| std::io::Error::other("fixed clock exceeds SystemTime range").into())
}

fn delay(value: &str, now: SystemTime) -> TestResult<Option<Duration>> {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::RETRY_AFTER,
        http::HeaderValue::from_str(value)?,
    );
    Ok(parse_retry_after_at(&headers, now))
}

fn expect_delay(value: &str, now: SystemTime, expected: Option<Duration>) -> TestResult {
    let actual = delay(value, now)?;
    check(
        actual == expected,
        &format!("{value:?}: expected {expected:?}, got {actual:?}"),
    )
}

#[test]
fn numeric_seconds_retain_existing_integer_semantics() -> TestResult {
    for (value, expected) in [
        ("0", Some(0)),
        ("3", Some(3)),
        (" \t3\t ", Some(3)),
        ("0003", Some(3)),
        ("+3", Some(3)),
        ("18446744073709551615", Some(u64::MAX)),
        ("18446744073709551616", None),
        ("-1", None),
        ("3.5", None),
        ("3 0", None),
        ("", None),
        ("not-a-date", None),
    ] {
        expect_delay(value, now()?, expected.map(Duration::from_secs))?;
    }
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::RETRY_AFTER,
        http::HeaderValue::from_static("3"),
    );
    check(
        parse_retry_after(&headers) == Some(Duration::from_secs(3)),
        "production entry point changed numeric seconds",
    )
}

#[test]
fn all_three_http_date_forms_keep_subsecond_precision() -> TestResult {
    for value in [
        "Fri, 16 Oct 2026 12:00:02 GMT",
        " \tFri, 16 Oct 2026 12:00:02 GMT\t ",
        "Friday, 16-Oct-26 12:00:02 GMT",
        "Fri Oct 16 12:00:02 2026",
    ] {
        expect_delay(value, now()?, Some(Duration::from_millis(1750)))?;
    }
    Ok(())
}

#[test]
fn rfc2822_numeric_timezones_preserve_zero_offsets_and_rollover() -> TestResult {
    for value in [
        "Fri, 16 Oct 2026 12:00:02 +0000",
        "Fri, 16 Oct 2026 12:00:02 -0000",
        "Fri, 16 Oct 2026 14:00:02 +0200",
        "Fri, 16 Oct 2026 08:00:02 -0400",
        "Fri, 16 Oct 2026 17:45:02 +0545",
        "Fri, 16 Oct 2026 08:30:02 -0330",
        "Sat, 17 Oct 2026 11:59:02 +2359",
        "Thu, 15 Oct 2026 12:01:02 -2359",
    ] {
        expect_delay(value, now()?, Some(Duration::from_millis(1750)))?;
    }
    expect_delay(
        "Fri, 01 Jan 2027 00:30:00 +0100",
        instant("Thu, 31 Dec 2026 23:00:00 GMT")?,
        Some(Duration::from_secs(1800)),
    )?;
    expect_delay(
        "Thu, 31 Dec 2026 23:30:00 -0100",
        instant("Fri, 01 Jan 2027 00:00:00 GMT")?,
        Some(Duration::from_secs(1800)),
    )?;
    expect_delay(
        "Thu, 01 Jan 1970 00:00:00 +0001",
        UNIX_EPOCH,
        Some(Duration::ZERO),
    )?;
    expect_delay(
        "Wed, 31 Dec 1969 23:59:59 -0001",
        UNIX_EPOCH,
        Some(Duration::from_secs(59)),
    )
}

#[test]
fn named_timezones_remain_compatible() -> TestResult {
    for value in [
        "Fri, 16 Oct 2026 12:00:02 GMT",
        "Fri, 16 Oct 2026 12:00:02 UT",
        "Fri, 16 Oct 2026 12:00:02 UTC",
        "Fri, 16 Oct 2026 07:00:02 EST",
        "Fri, 16 Oct 2026 08:00:02 EDT",
        "Fri, 16 Oct 2026 06:00:02 CST",
        "Fri, 16 Oct 2026 07:00:02 CDT",
        "Fri, 16 Oct 2026 05:00:02 MST",
        "Fri, 16 Oct 2026 06:00:02 MDT",
        "Fri, 16 Oct 2026 04:00:02 PST",
        "Fri, 16 Oct 2026 05:00:02 PDT",
    ] {
        expect_delay(value, now()?, Some(Duration::from_millis(1750)))?;
    }
    Ok(())
}

#[test]
fn common_rfc2822_spelling_variants_remain_compatible() -> TestResult {
    for value in [
        "fri, 16 oct 2026 12:00:02 gmt",
        "FRI, 16 OCT 2026 12:00:02 utc",
        "Fri,\t16  Oct\t2026 12:00:02 GMT",
        "16 Oct 2026 12:00:02 +0000",
        "Fri, 16 Oct 2026 12:00:02 GMT (UTC)",
        "Fri, 16 Oct 2026 12:00:02 +0000(comment)",
        "Fri, 16 Oct 26 12:00:02 GMT",
        "Fri, 16 Oct 126 12:00:02 GMT",
    ] {
        expect_delay(value, now()?, Some(Duration::from_millis(1750)))?;
    }
    expect_delay(
        "Fri, 6 Nov 2026 12:00:02 GMT",
        instant("Fri, 06 Nov 2026 12:00:00 GMT")?,
        Some(Duration::from_secs(2)),
    )?;
    expect_delay(
        "Fri, 16 Oct 2026 12:01 GMT",
        now()?,
        Some(Duration::from_millis(59_750)),
    )
}

#[test]
fn past_equal_and_pre_epoch_dates_produce_zero_delay() -> TestResult {
    for value in [
        "Fri, 16 Oct 2026 12:00:00 GMT",
        "Sun, 06 Nov 1994 08:49:37 GMT",
        "Sunday, 06-Nov-94 08:49:37 GMT",
        "Sun Nov  6 08:49:37 1994",
        "Thu, 01 Jan 1970 00:00:00 GMT",
        "Wed, 31 Dec 1969 23:59:59 GMT",
        "Mon, 01 Jan 1900 12:00:00 GMT",
        "Mon, 01 Jan 1601 12:00:00 GMT",
        "Tue, 29 Feb 1600 12:00:00 GMT",
        "Sat, 01 Jan 0000 12:00:00 GMT",
    ] {
        expect_delay(value, now()?, Some(Duration::ZERO))?;
    }
    expect_delay(
        "Fri, 16 Oct 2026 12:00:02 +0000",
        instant("Fri, 16 Oct 2026 12:00:02 GMT")?,
        Some(Duration::ZERO),
    )?;
    let pre_epoch = UNIX_EPOCH
        .checked_sub(Duration::from_secs(2))
        .ok_or_else(|| std::io::Error::other("platform has no pre-epoch SystemTime"))?;
    expect_delay(
        "Wed, 31 Dec 1969 23:59:59 GMT",
        pre_epoch,
        Some(Duration::from_secs(1)),
    )
}

#[test]
fn leap_days_and_last_supported_year_are_validated() -> TestResult {
    expect_delay(
        "Thu, 29 Feb 2024 12:00:00 +0000",
        instant("Thu, 29 Feb 2024 11:59:59 GMT")?,
        Some(Duration::from_secs(1)),
    )?;
    expect_delay(
        "Tue, 29 Feb 2000 12:00:00 +0000",
        instant("Tue, 29 Feb 2000 11:59:59 GMT")?,
        Some(Duration::from_secs(1)),
    )?;
    expect_delay(
        "Fri, 31 Dec 9999 23:59:59 +0000",
        instant("Fri, 31 Dec 9999 23:59:58 GMT")?,
        Some(Duration::from_secs(1)),
    )
}

#[test]
fn leap_seconds_normalize_to_the_following_second() -> TestResult {
    expect_delay(
        "Sat, 31 Dec 2016 23:59:60 GMT",
        now()?,
        Some(Duration::ZERO),
    )?;
    expect_delay(
        "Thu, 31 Dec 2026 23:59:60 GMT",
        now()?,
        Some(Duration::from_millis(6_609_599_750)),
    )?;
    expect_delay(
        "Fri, 16 Oct 2026 12:00:60 +0000",
        now()?,
        Some(Duration::from_millis(59_750)),
    )?;
    expect_delay(
        "Fri, 01 Jan 2027 00:59:60 +0100",
        instant("Thu, 31 Dec 2026 23:59:59 GMT")?,
        Some(Duration::from_secs(1)),
    )
}

#[test]
fn two_digit_year_pivots_are_explicit() -> TestResult {
    // RFC2822's historical pivot is 1950..2049; RFC850 inherits httpdate's
    // fixed 1970..2069 pivot, not a clock-relative 50-year rule.
    expect_delay(
        "Fri, 01 Jan 49 12:00:00 +0000",
        instant("Fri, 01 Jan 2049 11:59:59 GMT")?,
        Some(Duration::from_secs(1)),
    )?;
    expect_delay(
        "Sun, 01 Jan 50 12:00:00 +0000",
        now()?,
        Some(Duration::ZERO),
    )?;
    expect_delay(
        "Tuesday, 01-Jan-69 12:00:00 GMT",
        instant("Tue, 01 Jan 2069 11:59:59 GMT")?,
        Some(Duration::from_secs(1)),
    )?;
    expect_delay(
        "Thursday, 01-Jan-70 12:00:00 GMT",
        now()?,
        Some(Duration::ZERO),
    )?;
    expect_delay("Wednesday, 01-Jan-70 12:00:00 GMT", now()?, None)
}

#[test]
fn invalid_calendar_timezone_and_unsupported_grammar_are_rejected() -> TestResult {
    for value in [
        "Thu, 16 Oct 2026 12:00:02 GMT", // Explicit wrong weekday.
        "Fri, 00 Oct 2026 12:00:02 GMT",
        "Fri, 32 Oct 2026 12:00:02 GMT",
        "Tue, 31 Nov 2026 12:00:02 GMT",
        "Sat, 29 Feb 2025 12:00:02 GMT",
        "Thu, 29 Feb 1900 12:00:02 GMT",
        "Fri, 16 Oct 2026 24:00:02 GMT",
        "Fri, 16 Oct 2026 12:60:02 GMT",
        "Fri, 16 Oct 2026 12:00:61 GMT",
        "Fri, 16 Oct 2026 12:00:60 +2400",
        "Fri, 16 Oct 2026 12:00:02 -2400",
        "Fri, 16 Oct 2026 12:00:02 +0060",
        "Fri, 16 Oct 2026 12:00:02 -0060",
        "Fri, 16 Oct 2026 12:00:02 +00:00",
        "Fri, 16 Oct 2026 12:00:02 +000",
        "Fri, 16 Oct 2026 12:00:02 +00000",
        "Fri, 16 Oct 2026 12:00:02 +00x0",
        "Fri, 16 Oct 2026 12:00:02 Europe/London",
        "Fri, 16 Oct 2026 12:00:02 XYZ",
        "Fri, 16 Oct 2026 12:00:02 GMT garbage",
        "Fri, 16 Oct 2026 12:00:02 GMT (unclosed",
        "Fri, 16 Oct 2026 12:00:02 GMT (nested (comment))",
        "Fri, 16 Oct 2026 12:00:02 GMT (escaped\\))",
        "Fri, 16 Oct 2026 12:00:02 GMT (one) (two)",
        "Fri, 16 Oct 2026 12:00:02 GMT (comment) trailing",
        "Sat, 01 Jan 10000 12:00:00 GMT",
        "Fri, 16 Oct +2026 12:00:02 GMT",
        "Fri, 16 Oct 2026 1:00:02 GMT",
        "Fri, 16 Oct 2026 12:0:02 GMT",
        "Fri, 16 Oct 2026 12:00:2 GMT",
        "Fri, 16 Oct 2026 12:00:02:03 GMT",
        "Thursday, 16-Oct-26 12:00:02 GMT",
        "Thu Oct 16 12:00:02 2026",
        "Fri,\u{a0}16 Oct 2026 12:00:02 GMT",
    ] {
        expect_delay(value, now()?, None)?;
    }
    Ok(())
}

#[test]
fn header_selection_and_non_ascii_rejection_are_unchanged() -> TestResult {
    let mut headers = http::HeaderMap::new();
    check(
        parse_retry_after_at(&headers, now()?).is_none(),
        "missing header was accepted",
    )?;
    headers.append(
        http::header::RETRY_AFTER,
        http::HeaderValue::from_static("3"),
    );
    headers.append(
        http::header::RETRY_AFTER,
        http::HeaderValue::from_static("9"),
    );
    check(
        parse_retry_after_at(&headers, now()?) == Some(Duration::from_secs(3)),
        "first header no longer wins",
    )?;
    for value in [b"\xff".as_slice(), b"\x80", b"3\x80", b"\xc2\xa03\xc2\xa0"] {
        headers.insert(
            http::header::RETRY_AFTER,
            http::HeaderValue::from_bytes(value)?,
        );
        check(
            parse_retry_after_at(&headers, now()?).is_none(),
            "non-ASCII header was accepted",
        )?;
    }
    headers.insert(
        http::header::RETRY_AFTER,
        http::HeaderValue::from_static("invalid"),
    );
    headers.append(
        http::header::RETRY_AFTER,
        http::HeaderValue::from_static("3"),
    );
    check(
        parse_retry_after_at(&headers, now()?).is_none(),
        "invalid first header unexpectedly used later value",
    )
}

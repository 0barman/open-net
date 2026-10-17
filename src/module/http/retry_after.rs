//! Retry-After parsing with a narrow adapter for existing RFC 2822 inputs.

use std::time::{Duration, SystemTime};

const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

pub(super) fn parse_retry_after(headers: &http::HeaderMap) -> Option<Duration> {
    parse_retry_after_at(headers, SystemTime::now())
}

fn parse_retry_after_at(headers: &http::HeaderMap, now: SystemTime) -> Option<Duration> {
    let value = headers
        .get(http::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    // Keep the existing integer semantics, including leading '+' and u64::MAX.
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = httpdate::parse_http_date(value)
        .ok()
        .or_else(|| parse_compatible_date(value))?;
    Some(
        date.duration_since(now)
            .map_or(Duration::ZERO, |delay| delay),
    )
}

fn parse_compatible_date(value: &str) -> Option<SystemTime> {
    let value = without_simple_comment(value)?;
    let (weekday, value) = match value.split_once(',') {
        Some((weekday, rest)) => (Some(canonical(weekday.trim(), &WEEKDAYS)?), rest),
        None => (None, value),
    };
    let mut fields = value.split_ascii_whitespace();
    let day = fields.next()?;
    if !(1..=2).contains(&day.len()) {
        return None;
    }
    let day = decimal(day)?;
    let month = canonical(fields.next()?, &MONTHS)?;
    let year = compatible_year(fields.next()?)?;
    let (hour, minute, second) = clock_time(fields.next()?)?;
    let leap_second = second == 60;
    let second = if leap_second { 59 } else { second };
    let offset = timezone_offset(fields.next()?)?;
    if fields.next().is_some() {
        return None;
    }

    // Gregorian weekdays and leap days repeat every 400 years. Move older
    // dates into httpdate's supported range for validation, then restore the
    // actual instant without introducing a second calendar implementation.
    let cycles = if year < 1970 {
        (1970 - year).div_ceil(400)
    } else {
        0
    };
    let validation_year = year + cycles * 400;
    let validate = |weekday| {
        httpdate::parse_http_date(&format!(
            "{weekday}, {day:02} {month} {validation_year:04} {hour:02}:{minute:02}:{second:02} GMT"
        ))
        .ok()
    };
    let local = match weekday {
        Some(weekday) => validate(weekday)?,
        // A missing weekday was accepted by the previous parser. Let
        // httpdate find and validate it; never ignore an explicitly wrong one.
        None => WEEKDAYS.into_iter().find_map(validate)?,
    };
    let local = local.checked_sub(Duration::from_secs(u64::from(cycles) * 146_097 * 86_400))?;
    // SystemTime has no leap-second representation. Validate the preceding
    // second, then normalize :60 to the following second for the retry delay.
    let local = local.checked_add(Duration::from_secs(u64::from(leap_second)))?;
    let offset_duration = Duration::from_secs(u64::from(offset.unsigned_abs()));
    if offset >= 0 {
        local.checked_sub(offset_duration)
    } else {
        local.checked_add(offset_duration)
    }
}

fn without_simple_comment(value: &str) -> Option<&str> {
    match value.split_once('(') {
        Some((date, comment)) => {
            let comment = comment.strip_suffix(')')?;
            if comment.contains(['(', ')', '\\']) {
                return None;
            }
            Some(date.trim_end())
        }
        None => Some(value),
    }
}

fn canonical<'a>(value: &str, names: &[&'a str]) -> Option<&'a str> {
    names
        .iter()
        .copied()
        .find(|name| value.eq_ignore_ascii_case(name))
}

fn decimal(value: &str) -> Option<u16> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn compatible_year(value: &str) -> Option<u16> {
    let year = decimal(value)?;
    match value.len() {
        2 if year < 50 => Some(year + 2000),
        2 | 3 => Some(year + 1900),
        4 => Some(year),
        _ => None,
    }
}

fn clock_time(value: &str) -> Option<(u16, u16, u16)> {
    let parse = |value: &str| (value.len() == 2).then(|| decimal(value)).flatten();
    let mut parts = value.split(':');
    let hour = parse(parts.next()?)?;
    let minute = parse(parts.next()?)?;
    let second = match parts.next() {
        Some(value) => parse(value)?,
        None => 0,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((hour, minute, second))
}

fn timezone_offset(value: &str) -> Option<i32> {
    const ZONES: [(&str, i32); 11] = [
        ("GMT", 0),
        ("UT", 0),
        ("UTC", 0),
        ("EST", -5),
        ("EDT", -4),
        ("CST", -6),
        ("CDT", -5),
        ("MST", -7),
        ("MDT", -6),
        ("PST", -8),
        ("PDT", -7),
    ];
    if let Some((_, hours)) = ZONES
        .iter()
        .find(|(name, _)| value.eq_ignore_ascii_case(name))
    {
        return Some(hours * 3600);
    }
    let (positive, value) = match value.as_bytes().first()? {
        b'+' => (true, value.strip_prefix('+')?),
        b'-' => (false, value.strip_prefix('-')?),
        _ => return None,
    };
    if value.len() != 4 {
        return None;
    }
    let digits = decimal(value)?;
    let hours = digits / 100;
    let minutes = digits % 100;
    if hours >= 24 || minutes >= 60 {
        return None;
    }
    let seconds = i32::from(hours) * 3600 + i32::from(minutes) * 60;
    Some(if positive { seconds } else { -seconds })
}

#[cfg(test)]
#[path = "retry_after_tests.rs"]
mod tests;

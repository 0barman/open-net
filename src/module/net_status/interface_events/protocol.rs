//! The helpers only send bounded ASCII records. Never retain malformed provider text.
use crate::net_status::{
    DefaultRouteState, InterfaceAddressFamily, InterfaceEventKind, InterfaceEventSource,
};
use crate::NetError;
use std::collections::BTreeMap;
use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) enum Record {
    Ready,
    Gap(Option<i64>),
    Failed(Option<i64>),
    Wlan {
        code: u32,
        interface: String,
        at: Option<SystemTime>,
        reason: Option<u32>,
    },
    Route {
        family: InterfaceAddressFamily,
        state: DefaultRouteState,
    },
    RouteFailed {
        family: InterfaceAddressFamily,
        native_code: Option<i64>,
    },
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid Windows interface diagnostic record",
    )
}

fn optional<T: std::str::FromStr>(field: &str) -> io::Result<Option<T>> {
    if field == "-" {
        Ok(None)
    } else {
        field.parse().map(Some).map_err(|_| invalid())
    }
}

fn guid(field: &str) -> io::Result<String> {
    if field == "00000000-0000-0000-0000-000000000000"
        || field.len() != 36
        || !field.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
    {
        return Err(invalid());
    }
    Ok(field.to_ascii_lowercase())
}

pub(super) fn decode(source: InterfaceEventSource, line: &str) -> io::Result<Record> {
    let mut fields = line.split('\t');
    let result = match fields.next() {
        Some("READY") => Record::Ready,
        Some("ERROR") => Record::Failed(optional(fields.next().ok_or_else(invalid)?)?),
        Some("GAP") => Record::Gap(optional(fields.next().ok_or_else(invalid)?)?),
        Some("WLAN") if source == InterfaceEventSource::Wlan => {
            let code = match fields.next() {
                Some("8001") => 8001,
                Some("8003") => 8003,
                _ => return Err(invalid()),
            };
            let interface = guid(fields.next().ok_or_else(invalid)?)?;
            let at = optional::<i64>(fields.next().ok_or_else(invalid)?)?
                .map(|millis| {
                    let offset = Duration::from_millis(millis.unsigned_abs());
                    if millis < 0 {
                        UNIX_EPOCH.checked_sub(offset)
                    } else {
                        UNIX_EPOCH.checked_add(offset)
                    }
                    .ok_or_else(invalid)
                })
                .transpose()?;
            let reason = optional(fields.next().ok_or_else(invalid)?)?;
            Record::Wlan {
                code,
                interface,
                at,
                reason,
            }
        }
        Some("ROUTE") if source == InterfaceEventSource::DefaultRoute => {
            let family = match fields.next() {
                Some("4") => InterfaceAddressFamily::Ipv4,
                Some("6") => InterfaceAddressFamily::Ipv6,
                _ => return Err(invalid()),
            };
            match fields.next().ok_or_else(invalid)? {
                "UNKNOWN" => Record::RouteFailed {
                    family,
                    native_code: optional(fields.next().ok_or_else(invalid)?)?,
                },
                "TIE" => Record::Route {
                    family,
                    state: DefaultRouteState::Ambiguous,
                },
                "NONE" => Record::Route {
                    family,
                    state: DefaultRouteState::NoRoute,
                },
                value => Record::Route {
                    family,
                    state: DefaultRouteState::Unique {
                        interface_id: guid(value)?,
                    },
                },
            }
        }
        _ => return Err(invalid()),
    };
    if fields.next().is_some() {
        return Err(invalid());
    }
    Ok(result)
}

pub(super) struct RouteBaseline {
    pub(super) observed_at: SystemTime,
    pub(super) kind: InterfaceEventKind,
}

#[derive(Default)]
pub(super) struct RouteHistory {
    // A failed native hint source stays degraded even when both queries succeed.
    gap: Option<RouteBaseline>,
    latest: BTreeMap<InterfaceAddressFamily, RouteBaseline>,
}

impl RouteHistory {
    pub(super) fn gap(
        &mut self,
        error: NetError,
        native_code: Option<i64>,
        observed_at: SystemTime,
    ) -> InterfaceEventKind {
        self.latest.clear();
        let kind = InterfaceEventKind::SourceGap { error, native_code };
        self.gap = Some(RouteBaseline {
            observed_at,
            kind: kind.clone(),
        });
        kind
    }

    pub(super) fn baselines(&self) -> impl Iterator<Item = &RouteBaseline> {
        self.gap.iter().chain(self.latest.values())
    }

    pub(super) fn failed(
        &mut self,
        family: InterfaceAddressFamily,
        error: NetError,
        native_code: Option<i64>,
        observed_at: SystemTime,
    ) -> Option<InterfaceEventKind> {
        let error_kind = error.kind();
        let kind = InterfaceEventKind::DefaultRouteQueryFailed {
            family,
            error,
            native_code,
        };
        let previous = self.latest.insert(
            family,
            RouteBaseline {
                observed_at,
                kind: kind.clone(),
            },
        );
        match previous.map(|sample| sample.kind) {
            Some(InterfaceEventKind::DefaultRouteQueryFailed {
                error,
                native_code: previous_code,
                ..
            }) if error.kind() == error_kind && previous_code == native_code => None,
            _ => Some(kind),
        }
    }

    pub(super) fn observe(
        &mut self,
        family: InterfaceAddressFamily,
        state: DefaultRouteState,
        observed_at: SystemTime,
    ) -> Option<InterfaceEventKind> {
        let baseline = RouteBaseline {
            observed_at,
            kind: InterfaceEventKind::DefaultRouteObserved {
                family,
                state: state.clone(),
            },
        };
        let previous =
            self.latest
                .insert(family, baseline)
                .and_then(|previous| match previous.kind {
                    InterfaceEventKind::DefaultRouteObserved { state, .. } => Some(state),
                    _ => None,
                });
        match (previous, state) {
            (Some(previous), state) if previous == state => None,
            (
                Some(DefaultRouteState::Unique {
                    interface_id: previous,
                }),
                DefaultRouteState::Unique {
                    interface_id: current,
                },
            ) => Some(InterfaceEventKind::DefaultRouteChanged {
                family,
                previous,
                current,
            }),
            (_, state) => Some(InterfaceEventKind::DefaultRouteObserved { family, state }),
        }
    }
}

//! Fallible checks shared by the client tests. Failures must reach the test runner.

pub(crate) type TestError = Box<dyn std::error::Error + Send + Sync>;
pub(crate) type TestResult<T = ()> = Result<T, TestError>;

#[track_caller]
pub(crate) fn test_error(message: impl Into<String>) -> TestError {
    let location = std::panic::Location::caller();
    let message = message.into();
    std::io::Error::other(format!("{location}: {message}")).into()
}

macro_rules! check {
    ($condition:expr $(,)?) => {
        $crate::module::ws_client::test_support::check!(
            $condition, "check failed: {}", stringify!($condition)
        )
    };
    ($condition:expr, $($message:tt)+) => {{
        if $condition {
            Ok::<(), $crate::module::ws_client::test_support::TestError>(())
        } else {
            Err($crate::module::ws_client::test_support::test_error(format!($($message)+)))
        }
    }};
}

macro_rules! check_eq {
    ($left:expr, RequestAction::Continue $(,)?) => {{
        match &$left {
            RequestAction::Continue => Ok(()),
            actual => Err($crate::module::ws_client::test_support::test_error(format!("expected RequestAction::Continue, got {:?}", actual))),
        }
    }};
    ($left:expr, RequestAction::Stop $(,)?) => {{
        match &$left {
            RequestAction::Stop => Ok(()),
            actual => Err($crate::module::ws_client::test_support::test_error(format!("expected RequestAction::Stop, got {:?}", actual))),
        }
    }};
    ($left:expr, ControlAction::Continue $(,)?) => {{
        match &$left {
            ControlAction::Continue => Ok(()),
            actual => Err($crate::module::ws_client::test_support::test_error(format!("expected ControlAction::Continue, got {:?}", actual))),
        }
    }};
    ($left:expr, RequestAction::StopWithError($error:expr) $(,)?) => {{
        match &$left {
            RequestAction::StopWithError(error) => $crate::module::ws_client::test_support::check_eq!(error.kind(), ($error).kind()),
            actual => Err($crate::module::ws_client::test_support::test_error(format!("expected RequestAction::StopWithError, got {:?}", actual))),
        }
    }};
    ($left:expr, ControlAction::Stop($stop:expr) $(,)?) => {{
        match &$left {
            ControlAction::Stop(stop) => $crate::module::ws_client::test_support::check_eq!(*stop, $stop),
            actual => Err($crate::module::ws_client::test_support::test_error(format!("expected ControlAction::Stop, got {:?}", actual))),
        }
    }};

    // Inspect variants explicitly: errors compare their category, successes their value.
    ($left:expr, NetError::from($kind:path) $(,)?) => {
        $crate::module::ws_client::test_support::check_eq!(($left).kind(), $kind)
    };
    ($left:expr, Err(NetError::from($kind:path)) $(,)?) => {{
        match &$left {
            Err(error) => $crate::module::ws_client::test_support::check_eq!(error.kind(), $kind),
            actual => Err($crate::module::ws_client::test_support::test_error(format!("expected Err({:?}), got {:?}", $kind, actual))),
        }
    }};
    ($left:expr, Ok($($right:tt)+) $(,)?) => {{
        match &$left {
            Ok(value) => $crate::module::ws_client::test_support::check_eq!(*value, $($right)+),
            actual => Err($crate::module::ws_client::test_support::test_error(format!("expected Ok, got {:?}", actual))),
        }
    }};
    ($left:expr, Some($($right:tt)+) $(,)?) => {{
        match &$left {
            Some(value) => $crate::module::ws_client::test_support::check_eq!(*value, $($right)+),
            actual => Err($crate::module::ws_client::test_support::test_error(format!("expected Some, got {:?}", actual))),
        }
    }};
    ($left:expr, None $(,)?) => {{
        match &$left {
            None => Ok(()),
            actual => Err($crate::module::ws_client::test_support::test_error(format!("expected None, got {:?}", actual))),
        }
    }};
    ($left:expr, std::task::Poll::Pending $(,)?) => {
        $crate::module::ws_client::test_support::check!(matches!($left, std::task::Poll::Pending))
    };
    ($left:expr, std::task::Poll::Ready($($right:tt)+) $(,)?) => {{
        match &$left {
            std::task::Poll::Ready(value) => $crate::module::ws_client::test_support::check_eq!(*value, $($right)+),
            actual => Err($crate::module::ws_client::test_support::test_error(format!("expected Ready, got {:?}", actual))),
        }
    }};
    ($left:expr, Ok($($right:tt)+), $($message:tt)+) => {
        $crate::module::ws_client::test_support::check_eq!($left, Ok($($right)+))
            .map_err(|error| $crate::module::ws_client::test_support::test_error(format!("{}; {}", format_args!($($message)+), error)))
    };
    ($left:expr, Err(NetError::from($kind:path)), $($message:tt)+) => {
        $crate::module::ws_client::test_support::check_eq!($left, Err(NetError::from($kind)))
            .map_err(|error| $crate::module::ws_client::test_support::test_error(format!("{}; {}", format_args!($($message)+), error)))
    };
    ($left:expr, $right:expr $(,)?) => {
        $crate::module::ws_client::test_support::check_eq!(
            $left, $right, "{} == {}", stringify!($left), stringify!($right)
        )
    };
    ($left:expr, $right:expr, $($message:tt)+) => {{
        match (&$left, &$right) {
            (left, right) => $crate::module::ws_client::test_support::check!(
                *left == *right,
                "{}; left: {:?}, right: {:?}", format_args!($($message)+), left, right
            ),
        }
    }};
}

macro_rules! check_ne {
    ($left:expr, $right:expr $(,)?) => {
        $crate::module::ws_client::test_support::check_ne!(
            $left, $right, "{} != {}", stringify!($left), stringify!($right)
        )
    };
    ($left:expr, $right:expr, $($message:tt)+) => {{
        match (&$left, &$right) {
            (left, right) => $crate::module::ws_client::test_support::check!(
                *left != *right,
                "{}; both: {:?}", format_args!($($message)+), left
            ),
        }
    }};
}

pub(crate) use {check, check_eq, check_ne};

#[test]
fn fallible_checks_report_the_actual_call_site() -> TestResult {
    let direct_line = line!() + 1;
    let direct = test_error("direct diagnostic");
    check!(direct
        .to_string()
        .starts_with(&format!("{}:{direct_line}:", file!())))?;

    let macro_line = line!() + 1;
    let result = check_eq!(1, 2, "macro diagnostic");
    let error = result
        .err()
        .ok_or_else(|| test_error("failed check returned success"))?;
    check!(error
        .to_string()
        .starts_with(&format!("{}:{macro_line}:", file!())))?;
    check!(error
        .to_string()
        .contains("macro diagnostic; left: 1, right: 2"))?;
    Ok(())
}

/// Build an actual V2 session runtime for focused transport/handshake tests.
pub(super) fn connection_target(
    url: &str,
) -> TestResult<(
    super::connect_target::ConnectTarget,
    crate::ws::ConnectionJournal,
)> {
    let options = crate::ws::ConnectOptions::new(url);
    let (runtime, journal) = super::v2_test_support::unconnected(
        crate::ws::WebSocketClientConfig::default(),
        options.routing.clone(),
        101,
        102,
        Some(crate::ws::JournalOptions {
            max_events: 32,
            ..crate::ws::JournalOptions::default()
        }),
    )?;
    let initial_connect_deadline = options
        .connect_timeout
        .and_then(|timeout| tokio::time::Instant::now().checked_add(timeout));
    Ok((
        super::connect_target::ConnectTarget {
            options,
            initial_connect_deadline,
            session: runtime.lifecycle.clone(),
            runtime,
        },
        journal.ok_or_else(|| test_error("fixture journal missing"))?,
    ))
}

#[test]
fn error_checks_preserve_categories_and_container_variants() -> TestResult {
    use crate::error::{ErrorKind, NetError};

    let cancelled: Result<(), NetError> = Err(ErrorKind::Cancelled.into());
    check!(check_eq!(cancelled, Err(NetError::from(ErrorKind::TimedOut))).is_err())?;
    check_eq!(cancelled, Err(NetError::from(ErrorKind::Cancelled)))?;
    let success: Result<(), NetError> = Ok(());
    check!(check_eq!(success, Err(NetError::from(ErrorKind::Cancelled))).is_err())?;

    let missing: Option<NetError> = None;
    check!(check_eq!(missing, Some(NetError::from(ErrorKind::Cancelled))).is_err())?;
    let different: Option<NetError> = Some(ErrorKind::TimedOut.into());
    check!(check_eq!(different, Some(NetError::from(ErrorKind::Cancelled))).is_err())?;

    let nested_value: Option<Result<u8, NetError>> = Some(Ok(1));
    check!(check_eq!(nested_value, Some(Ok(2))).is_err())?;
    check_eq!(nested_value, Some(Ok(1)))?;
    let outer: Result<Result<(), NetError>, std::io::Error> = Ok(Err(ErrorKind::TimedOut.into()));
    check!(check_eq!(outer, Ok(Err(NetError::from(ErrorKind::Cancelled)))).is_err())?;

    let missing_result: Option<Result<(), NetError>> = None;
    check!(check_eq!(
        missing_result,
        Some(Err(NetError::from(ErrorKind::Cancelled)))
    )
    .is_err())?;
    let nested_success: Option<Result<(), NetError>> = Some(Ok(()));
    check!(check_eq!(
        nested_success,
        Some(Err(NetError::from(ErrorKind::Cancelled)))
    )
    .is_err())?;
    Ok(())
}

/// Application error used to observe destructor re-entry without blocking a test thread.
pub(crate) fn error_with_drop_probe(
    probe: impl FnOnce() + Send + Sync + 'static,
) -> crate::NetError {
    struct DropProbe(Option<Box<dyn FnOnce() + Send + Sync>>);
    impl std::fmt::Debug for DropProbe {
        fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            out.write_str("DropProbe")
        }
    }
    impl std::fmt::Display for DropProbe {
        fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            out.write_str("application source with destructor")
        }
    }
    impl std::error::Error for DropProbe {}
    impl Drop for DropProbe {
        fn drop(&mut self) {
            if let Some(probe) = self.0.take() {
                probe();
            }
        }
    }
    crate::NetError::with_source(
        crate::error::ErrorKind::Io,
        DropProbe(Some(Box::new(probe))),
    )
}

/// Constructs the current client worker with an explicitly compiled network policy.
pub(crate) fn new_inner(
    config: crate::ws::WebSocketClientConfig,
) -> crate::Result<(
    std::sync::Arc<crate::module::ws_client::ws_client_inner::WSClientInner>,
    crate::module::ws_client::ws_client_worker::WSClientWorker,
)> {
    crate::module::ws_client::ws_client_inner::WSClientInner::new_with_network(
        config,
        std::sync::Arc::new(
            crate::module::transport::compiled_network_config::CompiledNetworkConfig::new(
                crate::network::NetworkConfig::default(),
            )?,
        ),
        None,
    )
}

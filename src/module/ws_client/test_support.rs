//! Fallible checks shared by the client tests. Failures must reach the test runner.

pub(crate) type TestError = Box<dyn std::error::Error + Send + Sync>;
pub(crate) type TestResult<T = ()> = Result<T, TestError>;

pub(crate) fn test_error(message: impl Into<String>) -> TestError {
    std::io::Error::other(message.into()).into()
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

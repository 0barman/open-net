use super::{AttemptId, ClientId, ConnectionId, CycleId, OperationId, SessionId};
use crate::{error::ErrorKind, Result};
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

type TestResult = std::result::Result<(), crate::BoxError>;

fn value_contract<T>(allocate: fn(&AtomicU64) -> Result<T>, raw: fn(T) -> u64) -> TestResult
where
    T: Copy + Clone + Debug + Display + Eq + Ord + Hash,
{
    let counter = AtomicU64::new(0);
    let first = allocate(&counter)?;
    let second = allocate(&counter)?;
    let copied = first;
    let cloned = Clone::clone(&first);
    if raw(first) != 1 || raw(second) != 2 || counter.load(Ordering::Relaxed) != 2 {
        return Err("identity allocation did not advance its supplied counter".into());
    }
    if copied != first || cloned != first || first >= second {
        return Err("identity value traits do not preserve identity and ordering".into());
    }
    if first.to_string() != "1" || format!("{first:?}").is_empty() {
        return Err("identity formatting is missing its numeric value".into());
    }
    let values = HashSet::from([first, cloned, second]);
    if values.len() != 2 || !values.contains(&first) || !values.contains(&second) {
        return Err("identity hashing disagrees with equality".into());
    }
    Ok(())
}

fn exhaustion_contract<T>(allocate: fn(&AtomicU64) -> Result<T>, raw: fn(T) -> u64) -> TestResult {
    let counter = AtomicU64::new(u64::MAX - 1);
    if raw(allocate(&counter)?) != u64::MAX {
        return Err("last available identity was rejected or altered".into());
    }
    for _ in 0..3 {
        let error = allocate(&counter)
            .err()
            .ok_or("exhausted counter allocated another identity")?;
        if error.kind() != ErrorKind::ResourceExhausted
            || counter.load(Ordering::Relaxed) != u64::MAX
        {
            return Err("exhausted counter returned the wrong error or wrapped".into());
        }
    }
    Ok(())
}

fn concurrent_contract<T>(allocate: fn(&AtomicU64) -> Result<T>, raw: fn(T) -> u64) -> TestResult
where
    T: Copy + Send + 'static,
{
    let counter = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for index in 0..8 {
        let counter = counter.clone();
        workers.push(
            std::thread::Builder::new()
                .name(format!("identity-allocation-{index}"))
                .spawn(move || {
                    (0..64)
                        .map(|_| allocate(&counter))
                        .collect::<Result<Vec<_>>>()
                })?,
        );
    }
    let mut values = HashSet::new();
    for worker in workers {
        for value in worker
            .join()
            .map_err(|_| "identity allocator thread failed")??
        {
            if !values.insert(raw(value)) {
                return Err("concurrent identity allocation reused a value".into());
            }
        }
    }
    if values.len() != 512
        || counter.load(Ordering::Relaxed) != 512
        || values.iter().min() != Some(&1)
        || values.iter().max() != Some(&512)
    {
        return Err("concurrent identity allocation lost or invented values".into());
    }
    Ok(())
}

macro_rules! identity_tests {
    ($module:ident, $id:ty) => {
        mod $module {
            use super::*;
            #[test]
            fn supports_opaque_value_traits() -> TestResult {
                value_contract(<$id>::allocate, <$id>::as_u64)
            }
            #[test]
            fn exhaustion_never_wraps_or_reuses_values() -> TestResult {
                exhaustion_contract(<$id>::allocate, <$id>::as_u64)
            }
            #[test]
            fn concurrent_allocations_are_unique() -> TestResult {
                concurrent_contract(<$id>::allocate, <$id>::as_u64)
            }
        }
    };
}
identity_tests!(client, ClientId);
identity_tests!(session, SessionId);
identity_tests!(connection, ConnectionId);
identity_tests!(attempt, AttemptId);
identity_tests!(cycle, CycleId);
identity_tests!(operation, OperationId);

//! Acceptance tests for the Windows reachability diagnostics (RUST-1C).
//!
//! The STA test exercises a real failing CoInitializeEx through the sampler.
//! It does not reproduce the application's worker setup or establish the cause
//! of the reported events. Per-call fakes fail each production call boundary;
//! they do not induce native CoCreateInstance or GetConnectivity failures.

#![cfg(test)]

use super::{sample_reachability, sample_reachability_using, WindowsApi};
use crate::error::{ErrorKind, ErrorStage, NetError, WindowsReachabilityOperation as Operation};
use std::cell::RefCell;
use std::error::Error as StdError;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};
use windows::core::{Error as WindowsError, HRESULT};
use windows::Win32::System::Com::{CoInitializeEx, CoUninitialize, COINIT_APARTMENTTHREADED};

const RPC_E_CHANGED_MODE: u32 = 0x8001_0106;

fn on_sta_thread(test: impl FnOnce() + Send + 'static) {
    std::thread::spawn(move || {
        struct StaApartment;

        impl Drop for StaApartment {
            fn drop(&mut self) {
                // SAFETY: The guard is created only after successful COM init,
                // and is dropped on that same dedicated thread, also on panic.
                unsafe { CoUninitialize() };
            }
        }

        // SAFETY: This fresh test-owned thread creates no COM objects and never
        // shares its apartment with the test runner or another test case.
        unsafe { CoInitializeEx(None, COINIT_APARTMENTTHREADED) }
            .ok()
            .expect("initialize a test-owned STA apartment");
        let _apartment = StaApartment;
        test();
    })
    .join()
    .expect("STA diagnostic test thread panicked");
}

fn assert_monitor_io(error: &NetError, operation: Operation, raw_hresult: u32) {
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.context().stage, Some(ErrorStage::NetworkMonitor));
    assert_eq!(error.to_string(), "Io at NetworkMonitor");

    let source = error
        .source()
        .and_then(|source| source.downcast_ref::<WindowsError>())
        .expect("the original Windows error must remain downcastable");
    assert_eq!(source.code().0 as u32, raw_hresult);

    let diagnostic = error
        .context()
        .windows_reachability
        .expect("diagnostic missing");
    assert_eq!(diagnostic.operation, operation);
    assert_eq!(diagnostic.hresult, raw_hresult);
    assert_eq!(
        error
            .context()
            .network_monitor
            .as_deref()
            .unwrap()
            .phase
            .as_str(),
        operation.as_str()
    );
    let fields = error.network_monitor_diagnostics().unwrap();
    assert_eq!(fields["phase"], operation.as_str());
    assert_eq!(fields["operation"], operation.as_str());
    assert_eq!(fields["hresult"], format!("0x{raw_hresult:08X}"));
    assert_eq!(
        format!("0x{:08X}", diagnostic.hresult),
        format!("0x{raw_hresult:08X}")
    );

    let source_text = source.to_string();
    assert!(!source_text.is_empty());
    let default_output = format!("{error} {error:?}");
    assert!(!default_output.contains(&source_text));

    let cloned = error.clone();
    let cloned_source = cloned
        .source()
        .and_then(|source| source.downcast_ref::<WindowsError>())
        .expect("cloning must retain the original Windows source");
    assert!(std::ptr::eq(source, cloned_source));
    assert_eq!(cloned_source.code().0 as u32, raw_hresult);
    assert_eq!(cloned.to_string(), error.to_string());
    assert_eq!(cloned.context().windows_reachability, Some(diagnostic));
    assert_eq!(cloned.network_monitor_diagnostics(), Some(fields));
}

#[test]
fn real_com_apartment_conflict_exposes_operation_and_original_hresult() {
    on_sta_thread(|| {
        // sample_reachability requests MTA on our STA thread. Windows rejects
        // this before any NLM creation or network query, irrespective of the
        // machine's Internet connectivity. No network configuration is changed.
        let error = sample_reachability(
            Instant::now() + Duration::from_secs(10),
            &AtomicBool::new(false),
        )
        .err()
        .expect("a conflicting COM apartment must fail, not report offline");

        assert_monitor_io(&error, Operation::ComInit, RPC_E_CHANGED_MODE);
        let cloned = error.clone();
        drop(error);
        assert_monitor_io(&cloned, Operation::ComInit, RPC_E_CHANGED_MODE);
    });
}

#[test]
fn cancelled_and_expired_reachability_requests_precede_native_com_failure() {
    on_sta_thread(|| {
        for (cancelled, expired, expected) in [
            (true, false, ErrorKind::Cancelled),
            (false, true, ErrorKind::TimedOut),
            (true, true, ErrorKind::Cancelled),
        ] {
            let deadline = if expired {
                Instant::now()
            } else {
                Instant::now() + Duration::from_secs(10)
            };
            let error = sample_reachability(deadline, &AtomicBool::new(cancelled))
                .err()
                .expect("a revoked request must fail before COM initialization");

            assert_eq!(error.kind(), expected);
            assert_eq!(error.context().stage, Some(ErrorStage::NetworkMonitor));
            assert!(error.source().is_none());
            assert!(error.context().windows_reachability.is_none());
        }
    });
}

#[derive(Default)]
struct FakeWindowsApi {
    failure: Option<(Operation, u32)>,
    bits: u32,
    events: Rc<RefCell<Vec<&'static str>>>,
}

struct TrackedResource(&'static str, Rc<RefCell<Vec<&'static str>>>);

impl Drop for TrackedResource {
    fn drop(&mut self) {
        self.1.borrow_mut().push(self.0);
    }
}

impl FakeWindowsApi {
    fn step(&self, operation: Operation) -> windows::core::Result<()> {
        self.events.borrow_mut().push(operation.as_str());
        if let Some((failed, raw)) = self.failure {
            if failed == operation {
                return Err(WindowsError::from_hresult(HRESULT(raw as i32)));
            }
        }
        Ok(())
    }
}

impl WindowsApi for FakeWindowsApi {
    type Apartment = TrackedResource;
    type Manager = TrackedResource;

    fn initialize(&self) -> windows::core::Result<Self::Apartment> {
        self.step(Operation::ComInit)?;
        Ok(TrackedResource("uninitialize", self.events.clone()))
    }

    fn create_manager(&self) -> windows::core::Result<Self::Manager> {
        self.step(Operation::NlmCreate)?;
        Ok(TrackedResource("release_manager", self.events.clone()))
    }

    fn get_connectivity(&self, _: &Self::Manager) -> windows::core::Result<u32> {
        self.step(Operation::NlmGetConnectivity)?;
        Ok(self.bits)
    }
}

#[test]
fn each_native_failure_boundary_preserves_operation_hresult_and_cleanup() {
    // Run the actual sampling control flow against per-instance fakes. Reusing
    // each HRESULT across all operations proves the stage is not inferred from it.
    for (operation, expected) in [
        (Operation::ComInit, vec!["com_init"]),
        (
            Operation::NlmCreate,
            vec!["com_init", "nlm_create", "uninitialize"],
        ),
        (
            Operation::NlmGetConnectivity,
            vec![
                "com_init",
                "nlm_create",
                "nlm_get_connectivity",
                "release_manager",
                "uninitialize",
            ],
        ),
    ] {
        for raw in [RPC_E_CHANGED_MODE, 0x8004_0154, 0x8000_4005, 0x8007_0005] {
            let api = FakeWindowsApi {
                failure: Some((operation, raw)),
                ..Default::default()
            };
            let error = sample_reachability_using(
                &api,
                Instant::now() + Duration::from_secs(10),
                &AtomicBool::new(false),
            )
            .err()
            .expect("injected Windows failure must remain an error");
            assert_monitor_io(&error, operation, raw);
            assert_eq!(*api.events.borrow(), expected);
        }
    }
}

#[test]
fn successful_samples_preserve_offline_and_release_manager_before_apartment() {
    use crate::net_status::NetworkStatus;
    for (bits, status) in [
        (0, NetworkStatus::Unavailable),
        (0x333, NetworkStatus::Unavailable),
        (0x40, NetworkStatus::Available),
        (0x400, NetworkStatus::Available),
    ] {
        let api = FakeWindowsApi {
            bits,
            ..Default::default()
        };
        let sample = sample_reachability_using(
            &api,
            Instant::now() + Duration::from_secs(10),
            &AtomicBool::new(false),
        )
        .unwrap();
        assert_eq!(sample.reachability, status);
        assert_eq!(
            *api.events.borrow(),
            [
                "com_init",
                "nlm_create",
                "nlm_get_connectivity",
                "release_manager",
                "uninitialize"
            ]
        );
    }
}

#[test]
fn name_queries_do_not_receive_reachability_diagnostics() {
    on_sta_thread(|| {
        let error = super::query_name(
            Instant::now() + Duration::from_secs(10),
            &AtomicBool::new(false),
        )
        .err()
        .expect("STA name query must fail before running netsh");
        assert_eq!(error.kind(), ErrorKind::Io);
        assert_eq!(error.to_string(), "Io at NetworkMonitor");
        assert!(error.context().windows_reachability.is_none());
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<WindowsError>()
                .unwrap()
                .code()
                .0 as u32,
            RPC_E_CHANGED_MODE
        );
    });
    let api = FakeWindowsApi {
        failure: Some((Operation::NlmCreate, 0x8004_0154)),
        ..Default::default()
    };
    let error = super::with_manager_using(
        &api,
        Instant::now() + Duration::from_secs(10),
        &AtomicBool::new(false),
        super::QueryKind::Name,
        |_| Ok(()),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert!(error.context().windows_reachability.is_none());
    assert_eq!(
        *api.events.borrow(),
        ["com_init", "nlm_create", "uninitialize"]
    );
}

#[test]
fn repeated_native_initialization_accepts_s_false_and_balances_each_guard() {
    std::thread::spawn(|| {
        let first = super::NativeWindowsApi.initialize().unwrap();
        // COM is already MTA: the native CoInitializeEx now returns S_FALSE.
        let nested = super::NativeWindowsApi
            .initialize()
            .expect("S_FALSE is success");
        drop(nested);
        // SAFETY: deliberate incompatible init on this test-owned MTA thread;
        // failure adds no reference and must not be uninitialized.
        assert_eq!(
            unsafe { CoInitializeEx(None, COINIT_APARTMENTTHREADED) }.0 as u32,
            RPC_E_CHANGED_MODE
        );
        drop(first);
        // SAFETY: after both guards release, this thread may initialize STA.
        let initialized = unsafe { CoInitializeEx(None, COINIT_APARTMENTTHREADED) };
        initialized
            .ok()
            .expect("all MTA references must have been released");
        let _sta = super::ComApartment;
        assert_eq!(initialized.0, 0);
    })
    .join()
    .unwrap();
}

use crate::common::log::log_def::LogType;
use crate::NetError;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use tokio_util::sync::CancellationToken;

/// A cancellation control handle shared by a group of operations.
///
/// Cancelling a parent group prevents all descendants from admitting further
/// operations; cancelling a child does not affect its parent or sibling groups.
/// Dropping this handle only releases a reference. Use
/// [`Self::cancel_on_drop`] to cancel when a guard is dropped.
#[derive(Clone)]
pub struct CancellationGroup {
    /// Cancellation status and parent-child relationship shared by multiple handles.
    pub(crate) inner: Arc<CancellationState>,
}

impl CancellationGroup {
    /// Creates an active cancellation group with no parent.
    pub fn new() -> Self {
        Self {
            inner: CancellationState::new(),
        }
    }

    /// Creates a child group whose cancellation is independent unless the parent is cancelled.
    pub fn child(&self) -> Self {
        Self {
            inner: self.inner.child(),
        }
    }

    /// Irreversibly close admission, then wake waiters outside internal locks.
    /// This needs no runtime and does not wait for network or callback cleanup.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// Returns whether cancellation has been requested for this group or an ancestor.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// Wait for cancellation without creating a worker or requiring a runtime.
    pub async fn cancelled(&self) {
        self.inner.notification.cancelled().await;
    }

    /// Returns a guard that cancels this group when dropped unless disarmed.
    pub fn cancel_on_drop(&self) -> CancellationGuard {
        CancellationGuard {
            group: self.clone(),
            armed: true,
        }
    }
}

impl Default for CancellationGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CancellationGroup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CancellationGroup")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// Cancels the associated group when dropped; call [`Self::disarm`] to disable
/// automatic cancellation.
#[must_use]
#[derive(Debug)]
pub struct CancellationGuard {
    /// Group cancelled by this guard when it remains armed.
    group: CancellationGroup,
    /// Whether cancellation still needs to be performed on destruction; `false` after removing the guard.
    armed: bool,
}

impl CancellationGuard {
    /// Disarms automatic cancellation and returns the associated group.
    pub fn disarm(mut self) -> CancellationGroup {
        self.armed = false;
        self.group.clone()
    }
}

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.group.cancel();
        }
    }
}

/// Internal state for one cancellation-tree node, coordinating operation
/// admission, cancellation propagation, and cleanup callbacks.
pub(crate) struct CancellationState {
    /// Mutex gate shared by the cancellation tree to order admission and
    /// cancellation deterministically.
    gate: Arc<Mutex<()>>,
    /// Whether this node has been canceled; once set to `true` it will not be restored.
    cancelled: AtomicBool,
    /// Used to wake up asynchronous tasks waiting for cancellation by this node.
    notification: CancellationToken,
    /// Strong reference to the parent. It keeps an intermediate node alive while
    /// descendants remain; the root has `None`.
    parent: Option<Arc<CancellationState>>,
    /// Weak references to child nodes indexed by allocation address, used to propagate cancellation downward without forming a reference cycle.
    pub(crate) children: Mutex<HashMap<usize, Weak<CancellationState>>>,
    /// Weak references to cancellation hooks. Hooks are removed on cancellation
    /// and invoked after releasing the gate lock.
    pub(crate) hooks: Mutex<HashMap<usize, Weak<CancelHook>>>,
}

/// Internal cleanup action performed when its cancellation domain is cancelled.
pub(crate) struct CancelHook {
    /// Shared cleanup callback invoked after the tree gate is released.
    action: Arc<dyn Fn() + Send + Sync>,
}

/// Holds an internal cleanup-callback registration. Dropping it unregisters the
/// callback without cancelling the domain. If the callback owner also holds
/// this guard, its back-reference must be weak.
pub(crate) struct CancelHookGuard {
    /// Cancellation domain for the registration; keeps it accessible while the
    /// guard is alive.
    owner: Arc<CancellationState>,
    /// Cleanup callback removed from the domain when this guard is dropped.
    hook: Arc<CancelHook>,
}

impl CancellationState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            gate: Arc::new(Mutex::new(())),
            cancelled: AtomicBool::new(false),
            notification: CancellationToken::new(),
            parent: None,
            children: Mutex::new(HashMap::new()),
            hooks: Mutex::new(HashMap::new()),
        })
    }

    /// Create a separately cancellable child. A cancelled parent creates a cancelled child.
    pub(crate) fn child(self: &Arc<Self>) -> Arc<Self> {
        let gate = lock_recover(&self.gate);
        let cancelled = self.is_cancelled();
        let child = Arc::new(CancellationState {
            gate: self.gate.clone(),
            cancelled: AtomicBool::new(cancelled),
            notification: CancellationToken::new(),
            parent: Some(self.clone()),
            children: Mutex::new(HashMap::new()),
            hooks: Mutex::new(HashMap::new()),
        });
        lock_recover(&self.children).insert(Arc::as_ptr(&child) as usize, Arc::downgrade(&child));
        drop(gate);
        if cancelled {
            child.notification.cancel();
        }
        child
    }

    /// Close this domain and all descendants, then notify waiters and internal cleanup.
    ///
    /// This is synchronous and needs no runtime. Cleanup callbacks run after the shared
    /// admission lock is released; cancellation does not wait for user response handlers.
    pub(crate) fn cancel(self: &Arc<Self>) {
        let gate = lock_recover(&self.gate);
        let mut pending = vec![self.clone()];
        let mut cancelled = Vec::new();
        let mut hooks = Vec::new();
        while let Some(domain) = pending.pop() {
            domain.cancelled.store(true, Ordering::Release);
            pending.extend(
                lock_recover(&domain.children)
                    .values()
                    .filter_map(Weak::upgrade),
            );
            hooks.extend(
                lock_recover(&domain.hooks)
                    .drain()
                    .filter_map(|(_, hook)| hook.upgrade()),
            );
            cancelled.push(domain);
        }
        drop(gate);
        // Even custom wake implementations must not execute while the gate is held.
        for domain in cancelled {
            domain.notification.cancel();
        }
        for hook in hooks {
            (hook.action)();
        }
    }

    /// Read the irreversible local cancellation state without taking the admission lock.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Lock a single synchronous admission step. Do not hold the guard across an await,
    /// invoke cancellation/binding, or reacquire this tree's gate from the guarded code.
    pub(crate) fn lock_if_active(&self) -> Result<MutexGuard<'_, ()>, NetError> {
        let gate = self.gate.lock().map_err(|_| {
            crate::log_e!(LogType::WSC; "cancel_domain_admission", "error", "gate_lock_poisoned");
            NetError::from(crate::error::ErrorKind::Internal)
        })?;
        if self.is_cancelled() {
            return Err(NetError::from(crate::error::ErrorKind::Cancelled));
        }
        Ok(gate)
    }

    /// Register before publishing the operation through `lock_if_active`/`run_if_active`.
    /// A cancellation winning before publication runs the hook and rejects publication.
    pub(crate) fn bind_cancel_hook(
        self: &Arc<Self>,
        action: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<CancelHookGuard, NetError> {
        // Declare the capture before every guard so failed reservation releases
        // user-owned captures only after the admission gate has been unlocked.
        let hook = Arc::new(CancelHook { action });
        let _gate = self.lock_if_active()?;
        let mut hooks = lock_recover(&self.hooks);
        hooks
            .try_reserve(1)
            .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
        hooks.insert(Arc::as_ptr(&hook) as usize, Arc::downgrade(&hook));
        Ok(CancelHookGuard {
            owner: self.clone(),
            hook,
        })
    }
}

impl Drop for CancelHookGuard {
    fn drop(&mut self) {
        // No tree gate here: completion may drop this guard while holding that gate.
        lock_recover(&self.owner.hooks).remove(&(Arc::as_ptr(&self.hook) as usize));
    }
}

impl Drop for CancellationState {
    fn drop(&mut self) {
        let Some(mut ancestor) = self.parent.take() else {
            return;
        };
        lock_recover(&ancestor.children).remove(&(self as *const Self as usize));
        loop {
            // Weak child keys use the original Arc allocation, not the address of
            // the CancellationState value after try_unwrap moves it onto this stack.
            let identity = Arc::as_ptr(&ancestor) as usize;
            match Arc::try_unwrap(ancestor) {
                Ok(mut state) => {
                    let Some(parent) = state.parent.take() else {
                        return;
                    };
                    lock_recover(&parent.children).remove(&identity);
                    ancestor = parent;
                    // state now has no parent: its own Drop cannot recurse.
                }
                Err(shared) => {
                    drop(shared);
                    return;
                }
            }
        }
    }
}

pub(crate) fn lock_recover<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    match lock.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            crate::log_e!(LogType::WSC; "cancel_domain", "error", "lock_poisoned_recovered");
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
#[path = "cancellation_tests.rs"]
mod tests;

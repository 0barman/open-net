use super::data_callback_worker::DataCallbackWorker;
use super::data_callback_worker_lease::DataCallbackWorkerLease;
use super::try_start_user_callback_with;
use crate::common::log::log_def::LogType;
use std::future::Future;
use std::io;
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

type Callback = Box<dyn FnOnce() + Send>;

/// One client-wide pool. Registration prepares one real worker; dispatch grows
/// it lazily up to the original limit and reuses busy workers if growth fails.
pub(in crate::module::ws_client) struct DataCallbackPool {
    state: Mutex<PoolState>,
    max_concurrency: usize,
    available: Arc<Notify>,
}

struct PoolState {
    workers: Vec<DataCallbackWorker>,
    closed: bool,
}

struct ReadyWorker {
    sender: SyncSender<Callback>,
    lease: DataCallbackWorkerLease,
}

struct Preparation {
    worker: Option<ReadyWorker>,
    growth_error: Option<io::Error>,
    retired: Option<DataCallbackWorker>,
}

impl DataCallbackPool {
    /// Construction does not start threads. All sessions share this same pool.
    pub(in crate::module::ws_client) fn new(max_concurrency: usize) -> Self {
        Self {
            state: Mutex::new(PoolState {
                workers: Vec::new(),
                closed: false,
            }),
            max_concurrency,
            available: Arc::new(Notify::new()),
        }
    }

    /// Complete first-worker initialization before transferring an inbox. A
    /// busy existing worker is sufficient: callback registration never waits
    /// for application code and later dispatch may wait in its bounded lane.
    pub(in crate::module::ws_client) fn ensure_ready(&self) -> io::Result<()> {
        let mut state = self.lock()?;
        if state.closed {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        if state.workers.iter().any(DataCallbackWorker::is_alive) {
            return Ok(());
        }
        let (_, retired) = self.start_worker(&mut state)?;
        drop(state);
        drop(retired);
        Ok(())
    }

    /// The returned future occupies the existing dispatcher's in-flight slot.
    /// A failed optional pool expansion retains its job and waits for an
    /// initialized worker, so accepted callback buffers cannot be discarded by
    /// a later thread-allocation failure.
    pub(in crate::module::ws_client) fn dispatch(
        self: &Arc<Self>,
        callback: Callback,
    ) -> io::Result<impl Future<Output = io::Result<()>> + Send + 'static> {
        let pool = Arc::clone(self);
        Ok(async move {
            loop {
                let notified = pool.available.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let preparation = match pool.prepare_worker() {
                    Ok(preparation) => preparation,
                    Err(error) => {
                        // Dropping the tracked callback reports a structured
                        // dispatch failure to its subscription outside the lock.
                        return Err(error);
                    }
                };
                let Preparation {
                    worker,
                    growth_error,
                    retired,
                } = preparation;
                drop(retired);
                if let Some(error) = growth_error {
                    crate::log_e!(LogType::WSC; "data_callback_pool", "growth_error", crate::common::log::summary::error(&error));
                }
                if let Some(ReadyWorker { sender, lease }) = worker {
                    let submitted = try_start_user_callback_with(
                        "open-net-ws-data-callback",
                        move || {
                            let _lease = lease;
                            callback();
                        },
                        move |task| {
                            sender.try_send(task).map_err(|error| match error {
                                TrySendError::Full(_) => io::Error::from(io::ErrorKind::WouldBlock),
                                TrySendError::Disconnected(_) => {
                                    io::Error::from(io::ErrorKind::BrokenPipe)
                                }
                            })
                        },
                    );
                    submitted?.await;
                    return Ok(());
                }
                notified.await;
            }
        })
    }

    fn prepare_worker(&self) -> io::Result<Preparation> {
        let mut state = self.lock()?;
        if state.closed {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        if let Some(worker) = state.workers.iter().find(|worker| worker.is_available()) {
            if let Some(lease) = worker.claim() {
                return Ok(Preparation {
                    worker: Some(ReadyWorker {
                        sender: worker.sender(),
                        lease,
                    }),
                    growth_error: None,
                    retired: None,
                });
            }
        }
        let alive = state
            .workers
            .iter()
            .filter(|worker| worker.is_alive())
            .count();
        if alive >= self.max_concurrency {
            return Ok(Preparation {
                worker: None,
                growth_error: None,
                retired: None,
            });
        }
        let (index, retired) = match self.start_worker(&mut state) {
            Ok(started) => started,
            Err(error) if alive > 0 => {
                return Ok(Preparation {
                    worker: None,
                    growth_error: Some(error),
                    retired: None,
                });
            }
            Err(error) => return Err(error),
        };
        let worker = state.workers.get(index).and_then(|worker| {
            worker.claim().map(|lease| ReadyWorker {
                sender: worker.sender(),
                lease,
            })
        });
        Ok(Preparation {
            worker,
            growth_error: None,
            retired,
        })
    }

    /// Return replaced workers for retirement after the pool mutex is released.
    fn start_worker(
        &self,
        state: &mut PoolState,
    ) -> io::Result<(usize, Option<DataCallbackWorker>)> {
        if self.max_concurrency == 0 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        let vacant = state.workers.iter().position(|worker| !worker.is_alive());
        if vacant.is_none() {
            if state.workers.len() >= self.max_concurrency {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            state.workers.try_reserve(1).map_err(io::Error::other)?;
        }
        let worker = DataCallbackWorker::start(Arc::clone(&self.available))?;
        match vacant.and_then(|index| state.workers.get_mut(index).map(|slot| (index, slot))) {
            Some((index, slot)) => Ok((index, Some(std::mem::replace(slot, worker)))),
            None => {
                let index = state.workers.len();
                state.workers.push(worker);
                Ok((index, None))
            }
        }
    }

    fn lock(&self) -> io::Result<std::sync::MutexGuard<'_, PoolState>> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("data callback pool lock poisoned"))
    }

    /// Stops new dispatch without waiting for arbitrary application callbacks.
    pub(in crate::module::ws_client) fn close(&self) {
        let workers = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => {
                    crate::log_e!(LogType::WSC; "data_callback_pool", "error", "lock_poisoned_recovered");
                    poisoned.into_inner()
                }
            };
            state.closed = true;
            std::mem::take(&mut state.workers)
        };
        self.available.notify_waiters();
        drop(workers);
    }
}

impl Drop for DataCallbackPool {
    fn drop(&mut self) {
        self.close();
    }
}

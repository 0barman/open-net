use crate::common::common_error::CommonError;
use crate::common::inner::common_engine_impl::run_engine_queue_future;
use crate::common::log::log_def::LogType;
use crate::common::log::log_def::DESC;
use crate::common::retirement::{ExecutionScope, ThreadRegistry, WorkTracker};
use crate::log_e;
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender};

/// Library policy for explicitly configured management runtime workers.
pub(crate) const MAX_RUNTIME_WORKER_THREADS: usize = 256;

thread_local! {
    // Runtime ownership, not an entered Tokio context: callback-pool threads
    // enter this runtime too, but do not drive its timers or management tasks.
    static IS_ENGINE_RUNTIME_THREAD: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn is_engine_runtime_thread() -> bool {
    IS_ENGINE_RUNTIME_THREAD.with(Cell::get)
}

#[path = "lazy_callback_pool.rs"]
mod lazy_callback_pool;
pub(crate) use lazy_callback_pool::LazyCallbackPool;

#[path = "owned_runtime.rs"]
mod owned_runtime;
use owned_runtime::OwnedRuntime;

#[cfg(test)]
#[path = "common_engine_execution_tests.rs"]
mod execution_tests;
#[cfg(test)]
#[path = "common_engine_runtime_tests.rs"]
mod runtime_tests;

pub struct CommonEngine {
    pub(crate) cb_pool: LazyCallbackPool,
    pub(crate) async_tx: Sender<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    pub(crate) sync_tx: Sender<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    pub(crate) rt: Arc<OwnedRuntime>,
    management_close: tokio::sync::watch::Sender<bool>,
    management_threads: Arc<ThreadRegistry>,
    resource_threads: Arc<ThreadRegistry>,
    retirement_work: Arc<WorkTracker>,
}

impl CommonEngine {
    /// Returns the engine runtime for independently scheduled, long-lived tasks.
    ///
    /// Unlike `post`, spawned tasks do not occupy the sequential engine queue.
    /// The caller must retain the engine and stop its tasks before releasing it.
    pub fn runtime_handle(&self) -> tokio::runtime::Handle {
        self.rt.handle().clone()
    }

    pub fn new(
        channel_buffer_size: usize,
        sync_channel_buffer_size: usize,
    ) -> Result<CommonEngine, CommonError> {
        Self::new_with_runtime_worker_threads(channel_buffer_size, sync_channel_buffer_size, None)
    }

    pub(crate) fn new_with_runtime_worker_threads(
        channel_buffer_size: usize,
        sync_channel_buffer_size: usize,
        worker_threads: Option<usize>,
    ) -> Result<CommonEngine, CommonError> {
        crate::log_t!(LogType::Common; "new", "channel_buffer_size|sync_channel_buffer_size", channel_buffer_size, sync_channel_buffer_size);
        if [channel_buffer_size, sync_channel_buffer_size]
            .into_iter()
            .any(|capacity| capacity == 0 || capacity > tokio::sync::Semaphore::MAX_PERMITS)
            || worker_threads
                .is_some_and(|threads| threads == 0 || threads > MAX_RUNTIME_WORKER_THREADS)
        {
            return Err(CommonError::RuntimeError);
        }
        let (async_tx, mut async_rx) = channel(channel_buffer_size);
        let (sync_tx, mut sync_rx) = channel(sync_channel_buffer_size);
        let (management_close, mut sync_close) = tokio::sync::watch::channel(false);
        let mut async_close = sync_close.clone();
        let management_threads = Arc::new(ThreadRegistry::new());
        let resource_threads = Arc::new(ThreadRegistry::new());

        #[cfg(not(target_arch = "wasm32"))]
        let rt = {
            // 非 WASM 环境使用多线程运行时
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.enable_all();
            // Includes runtime blocking-pool threads. Rejecting synchronous
            // refresh there also avoids waiting on the pool's own work.
            builder.on_thread_start(|| {
                IS_ENGINE_RUNTIME_THREAD.with(|role| role.set(true));
                ExecutionScope::mark_current_thread();
            });
            if let Some(threads) = worker_threads {
                builder.worker_threads(threads);
            }
            let rt = Arc::new(OwnedRuntime::new(
                builder
                    .build()
                    .map_err(|error| {
                        log_e!(LogType::Common; "new", "stage|error", "create_runtime", crate::common::log::summary::error(&error));
                        CommonError::RuntimeError
                    })?,
            ));

            let rt_clone = Arc::clone(&rt);
            management_threads.spawn("open-net-engine", move || {
                rt_clone.block_on(async move {
                    // 分别处理同步和异步任务
                    let sync_handle = tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                biased;
                                _ = sync_close.changed() => {
                                    sync_rx.close();
                                    break;
                                }
                                future = sync_rx.recv() => match future {
                                    Some(future) => run_engine_queue_future("sync", future).await,
                                    None => return,
                                }
                            }
                        }
                        while let Some(future) = sync_rx.recv().await {
                            run_engine_queue_future("sync", future).await;
                        }
                    });

                    let async_handle = tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                biased;
                                _ = async_close.changed() => {
                                    async_rx.close();
                                    break;
                                }
                                future = async_rx.recv() => match future {
                                    Some(future) => run_engine_queue_future("async", future).await,
                                    None => return,
                                }
                            }
                        }
                        while let Some(future) = async_rx.recv().await {
                            run_engine_queue_future("async", future).await;
                        }
                    });

                    // Joining both queues retires their futures before the
                    // management OS thread can report its own completion.
                    let (sync, asynchronous) = tokio::join!(sync_handle, async_handle);
                    for result in [sync, asynchronous] {
                        if let Err(error) = result {
                            log_e!(LogType::Common; "new", DESC, format!("management queue worker failed: {error}"));
                        }
                    }
                });
            }).map_err(|error| {
                log_e!(LogType::Common; "new", "stage|error", "spawn_engine", crate::common::log::summary::error(&error));
                CommonError::RuntimeError
            })?;
            rt
        };
        crate::log_s!(LogType::Common; "new", "stage", "runtime_ready");
        Ok(CommonEngine {
            cb_pool: LazyCallbackPool::new(4),
            async_tx,
            sync_tx,
            rt,
            management_close,
            management_threads,
            resource_threads,
            retirement_work: Arc::new(WorkTracker::default()),
        })
    }

    pub(crate) fn resource_threads(&self) -> Arc<ThreadRegistry> {
        Arc::clone(&self.resource_threads)
    }

    pub(crate) fn retirement_work(&self) -> Arc<WorkTracker> {
        Arc::clone(&self.retirement_work)
    }

    pub(crate) fn management_is_closed(&self) -> bool {
        *self.management_close.borrow()
    }

    /// Stop management admission and finish every already accepted factory.
    /// Network resources and callbacks can still use the runtime after this.
    pub(crate) fn drain_management(&self) -> Result<(), CommonError> {
        self.management_close.send_replace(true);
        self.management_threads.seal_and_join()
    }

    /// Complete shutdown from an independent ordinary OS coordinator thread.
    pub(crate) fn shutdown_blocking(&self) -> Result<(), CommonError> {
        let mut failed = self.drain_management().is_err();
        failed |= self.resource_threads.seal_and_join().is_err();
        self.retirement_work.wait();
        failed |= self.cb_pool.shutdown_blocking().is_err();
        failed |= self.rt.shutdown_blocking().is_err();
        if failed {
            Err(CommonError::RuntimeError)
        } else {
            Ok(())
        }
    }

    pub fn task1<CB>(&self, ticket_id: &str, cb: CB)
    where
        CB: FnOnce(Result<(), CommonError>) + Send + 'static,
    {
        crate::log_t!(LogType::Common; "task1", "ticket_id|callback_type", ticket_id, std::any::type_name_of_val(&cb));
        let cb = self.cb_pool_once(cb);
        self.post(async move {
            cb(Ok(()));
        });
    }
}

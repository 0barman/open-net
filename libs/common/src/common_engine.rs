use crate::common_error::CommonError;
use crate::inner::common_engine_impl::run_engine_queue_future;
use crate::log::log_def::LogType;
use crate::log::log_def::DESC;
use crate::log_e;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use threadpool::ThreadPool;
use tokio::sync::mpsc::{channel, Sender};

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
    pub(crate) cb_pool: ThreadPool,
    pub(crate) async_tx: Sender<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    pub(crate) sync_tx: Sender<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    pub(crate) rt: Arc<OwnedRuntime>,
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
        crate::log_t!(LogType::Common; "new", "channel_buffer_size|sync_channel_buffer_size", channel_buffer_size, sync_channel_buffer_size);
        let (async_tx, mut async_rx) = channel(channel_buffer_size);
        let (sync_tx, mut sync_rx) = channel(sync_channel_buffer_size);

        #[cfg(not(target_arch = "wasm32"))]
        let rt = {
            // 非 WASM 环境使用多线程运行时
            let rt = Arc::new(OwnedRuntime::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| {
                        log_e!(LogType::Common; "new", "stage|error", "create_runtime", crate::log::summary::error(&error));
                        CommonError::RuntimeError
                    })?,
            ));

            let rt_clone = Arc::clone(&rt);
            std::thread::spawn(move || {
                rt_clone.block_on(async move {
                    // 分别处理同步和异步任务
                    let mut sync_handle = tokio::spawn(async move {
                        while let Some(future) = sync_rx.recv().await {
                            run_engine_queue_future("sync", future).await;
                        }
                    });

                    let mut async_handle = tokio::spawn(async move {
                        while let Some(future) = async_rx.recv().await {
                            run_engine_queue_future("async", future).await;
                        }
                    });

                    // 监控两个任务的完成状态
                    tokio::select! {
                        r = &mut sync_handle => {
                            if let Err(e) = r {
                                log_e!(LogType::Common; "new", DESC, format!("sync queue worker failed: {}", e));
                            }
                            async_handle.abort();
                        }
                        r = &mut async_handle => {
                            if let Err(e) = r {
                                log_e!(LogType::Common; "new", DESC, format!("async queue worker failed: {}", e));
                            }
                            sync_handle.abort();
                        }
                    }
                });
            });
            rt
        };
        crate::log_s!(LogType::Common; "new", "stage", "runtime_ready");
        Ok(CommonEngine {
            cb_pool: ThreadPool::new(4),
            async_tx,
            sync_tx,
            rt,
        })
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

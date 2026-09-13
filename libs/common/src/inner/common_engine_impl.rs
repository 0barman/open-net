use crate::common::common_engine::CommonEngine;
use crate::common::common_error::CommonError;
use crate::common::log::log_def::LogType;
use crate::common::log::log_def::DESC;
use crate::log_e;
use futures_util::FutureExt;
use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::oneshot;

// Used while constructing log fields; do not log this formatting dependency.
fn panic_payload_message(payload: &Box<dyn Any + Send + 'static>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

pub async fn run_engine_queue_future(
    queue_name: &'static str,
    future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
) {
    crate::log_t!(LogType::Common; "run_engine_queue_future", "queue_name|future_type", queue_name, std::any::type_name_of_val(&future));
    if let Err(payload) = AssertUnwindSafe(future).catch_unwind().await {
        log_e!(LogType::Common; "run_engine_queue_future",
            DESC,
            format!(
                "{} queue task panicked: {}",
                queue_name,
                crate::common::log::summary::error(&panic_payload_message(&payload))
            )
        );
    }
}

#[allow(clippy::type_complexity)]
impl CommonEngine {
    /// 在引擎专用 Runtime 上执行会调用 `self.rt.block_on(...)` 的闭包。
    ///
    /// - 无当前 Tokio 上下文（如 `spawn_blocking` 线程）：直接执行即可。
    /// - 已在**本引擎**的 worker 上：须 `block_in_place`，否则会死锁/嵌套阻塞。
    /// - 已在**其它** Runtime 上（如 Tauri 命令的 `tokio-rt-worker`）：不能在本线程上对引擎
    ///   `block_on`，否则触发 *Cannot start a runtime from within a runtime*；改到独立 OS 线程执行。
    pub fn run_blocking_on_engine_rt<R>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, CommonError>
    where
        R: Send + 'static,
    {
        crate::log_t!(LogType::Common; "run_blocking_on_engine_rt", "callback_type", std::any::type_name_of_val(&f));
        let engine_rt_id = self.rt.handle().id();
        match Handle::try_current() {
            Ok(current) if current.id() == engine_rt_id => tokio::task::block_in_place(|| {
                std::panic::catch_unwind(AssertUnwindSafe(f)).map_err(|payload| {
                    log_e!(LogType::Common; "run_blocking_on_engine_rt",
                        DESC,
                        format!(
                            "engine runtime task panicked: {}",
                            crate::common::log::summary::error(&panic_payload_message(&payload))
                        )
                    );
                    CommonError::RuntimeError
                })
            }),
            Ok(_) => {
                let handle =
                    std::thread::spawn(move || std::panic::catch_unwind(AssertUnwindSafe(f)));
                match handle.join() {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(payload)) => {
                        log_e!(LogType::Common; "run_blocking_on_engine_rt",
                            DESC,
                            format!(
                                "helper thread task panicked: {}",
                                crate::common::log::summary::error(&panic_payload_message(&payload))
                            )
                        );
                        Err(CommonError::RuntimeError)
                    }
                    Err(payload) => {
                        log_e!(LogType::Common; "run_blocking_on_engine_rt",
                            DESC,
                            format!(
                                "helper thread panicked: {}",
                                crate::common::log::summary::error(&panic_payload_message(&payload))
                            )
                        );
                        Err(CommonError::RuntimeError)
                    }
                }
            }
            Err(_) => std::panic::catch_unwind(AssertUnwindSafe(f)).map_err(|payload| {
                log_e!(LogType::Common; "run_blocking_on_engine_rt",
                    DESC,
                    format!(
                        "non-runtime thread task panicked: {}",
                        crate::common::log::summary::error(&panic_payload_message(&payload))
                    )
                );
                CommonError::RuntimeError
            }),
        }
    }

    /// async 转同步请求
    pub fn invoke<T, F>(&self, future: T) -> Result<F, CommonError>
    where
        T: Future<Output = F> + Send + 'static,
        F: Send + 'static,
    {
        crate::log_t!(LogType::Common; "invoke", "future_type", std::any::type_name_of_val(&future));
        let (result_tx, result_rx) = oneshot::channel();
        crate::log_s!(LogType::Common; "invoke", "stage", "submit_sync_task");
        let invoke_future = async move {
            let result = future.await;
            let _ = result_tx.send(result);
        };

        let rt = Arc::clone(&self.rt);
        let sync_tx = self.sync_tx.clone();
        self.run_blocking_on_engine_rt(move || {
            if rt.block_on(sync_tx.send(Box::pin(invoke_future))).is_err() {
                log_e!(LogType::Common; "invoke", DESC, "rt.block_on error");
                return Err(CommonError::PostError);
            }
            result_rx.blocking_recv().map_err(|e| {
                log_e!(LogType::Common; "invoke", DESC, format!("blocking_recv error {}", e));
                CommonError::RuntimeError
            })
        })?
    }

    /// async 转异步请求
    pub fn post<T>(&self, future: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        crate::log_t!(LogType::Common; "post", "future_type", std::any::type_name_of_val(&future));
        let rt = Arc::clone(&self.rt);
        let async_tx = self.async_tx.clone();
        crate::log_s!(LogType::Common; "post", "stage", "submit_async_task");
        if let Err(error) = self.run_blocking_on_engine_rt(move || {
            let r = rt.block_on(async_tx.send(Box::pin(future)));
            if let Err(error) = r {
                log_e!(LogType::Common; "post",
                    DESC,
                    format!("{}{}", "send async task error: {:?}", error.to_string())
                );
            }
        }) {
            log_e!(LogType::Common; "post",
                DESC,
                format!("run_blocking_on_engine_rt error: {:?}", error)
            );
        }
    }

    /// 为避免回调中的耗时操作阻塞，以及回调中调用其他接口造成死锁，所有回调必须放入线程池执行
    pub fn cb_pool_once<F, R>(&self, cb: F) -> impl FnOnce(R)
    where
        F: FnOnce(R) + Send + 'static,
        R: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_once", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        move |r1| cb_pool.execute(move || cb(r1))
    }

    /// cb_pool 重载接口，接受两个回调参数
    pub fn cb_pool_once2<F, R1, R2>(&self, cb: F) -> impl FnOnce(R1, R2)
    where
        F: FnOnce(R1, R2) + Send + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_once2", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        move |r1, r2| cb_pool.execute(move || cb(r1, r2))
    }

    /// cb_pool 重载接口，接受三个回调参数
    pub fn cb_pool_once3<F, R1, R2, R3>(&self, cb: F) -> impl FnOnce(R1, R2, R3)
    where
        F: FnOnce(R1, R2, R3) + Send + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_once3", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        move |r1, r2, r3| cb_pool.execute(move || cb(r1, r2, r3))
    }

    /// cb_pool 重载接口，接受三个回调参数
    pub fn cb_pool_once3_boxed<F, R1, R2, R3>(
        &self,
        cb: F,
    ) -> Box<dyn FnOnce(R1, R2, R3) + Send + Sync + 'static>
    where
        F: FnOnce(R1, R2, R3) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_once3_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        Box::new(move |r1, r2, r3| cb_pool.execute(move || cb(r1, r2, r3)))
    }

    /// cb_pool 重载接口，接受零个回调参数，返回 Fn()
    /// 用于包装 Box<dyn Fn()> 类型的 listener
    pub fn cb_pool_fn0_boxed<F>(&self, cb: F) -> Box<dyn Fn() + Send + Sync + 'static>
    where
        F: Fn() + Send + Sync + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn0_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move || {
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone())
        })
    }

    /// cb_pool 重载接口，接受单个回调参数，返回 Fn(R)
    /// 用于包装 Box<dyn Fn(R)> 类型的 listener
    pub fn cb_pool_fn1_boxed<F, R>(&self, cb: F) -> Box<dyn Fn(R) + Send + Sync + 'static>
    where
        F: Fn(R) + Send + Sync + 'static,
        R: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn1_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move |r| {
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(r))
        })
    }

    /// cb_pool 重载接口，接受两个回调参数，返回 Fn(R1, R2)
    /// 用于包装 Box<dyn Fn(R1, R2)> 类型的 listener
    pub fn cb_pool_fn2_boxed<F, R1, R2>(&self, cb: F) -> Box<dyn Fn(R1, R2) + Send + Sync + 'static>
    where
        F: Fn(R1, R2) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn2_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move |r1, r2| {
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(r1, r2))
        })
    }

    pub fn cb_pool_process_cloneable<F, R1, R2>(
        &self,
        cb: F,
    ) -> impl Fn(R1, R2) + Clone + Send + Sync + 'static
    where
        F: Fn(R1, R2) + Clone + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_process_cloneable", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        move |r1, r2| {
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(r1, r2))
        }
    }

    pub fn cb_pool_process<F, R1, R2>(&self, cb: F) -> impl Fn(R1, R2) + Send + Sync + 'static
    where
        F: Fn(R1, R2) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_process", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        move |r1, r2| {
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(r1, r2))
        }
    }

    /// cb_pool 重载接口，接受三个回调参数，返回 Fn(R1, R2, R3)
    /// 用于包装 Box<dyn Fn(R1, R2, R3)> 类型的 listener
    pub fn cb_pool_fn3_boxed<F, R1, R2, R3>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(R1, R2, R3) + Send + Sync + 'static>
    where
        F: Fn(R1, R2, R3) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn3_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move |r1, r2, r3| {
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(r1, r2, r3))
        })
    }

    /// cb_pool 重载接口，接受四个回调参数，返回 Fn(R1, R2, R3, R4)
    /// 用于包装 Box<dyn Fn(R1, R2, R3, R4)> 类型的 listener
    pub fn cb_pool_fn4_boxed<F, R1, R2, R3, R4>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(R1, R2, R3, R4) + Send + Sync + 'static>
    where
        F: Fn(R1, R2, R3, R4) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
        R4: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn4_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move |r1, r2, r3, r4| {
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(r1, r2, r3, r4))
        })
    }

    /// cb_pool 重载接口，接受五个回调参数，返回 Fn(R1, R2, R3, R4, R5)
    /// 用于包装 Box<dyn Fn(R1, R2, R3, R4, R5)> 类型的 listener
    pub fn cb_pool_fn5_boxed<F, R1, R2, R3, R4, R5>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(R1, R2, R3, R4, R5) + Send + Sync + 'static>
    where
        F: Fn(R1, R2, R3, R4, R5) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
        R4: Send + 'static,
        R5: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn5_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move |r1, r2, r3, r4, r5| {
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(r1, r2, r3, r4, r5))
        })
    }

    /// cb_pool 重载接口，接受单个 &R1 回调参数，返回 Fn(&R1,R2)
    /// 用于包装 Box<dyn Fn(&R1,R2)> 类型的 listener
    pub fn cb_pool_fn2_ref_boxed<F, R1, R2>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(&R1, R2) + Send + Sync + 'static>
    where
        F: Fn(&R1, R2) + Send + Sync + 'static,
        R1: Clone + Send + 'static,
        R2: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn2_ref_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move |r1, r2| {
            let r1 = r1.clone();
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(&r1, r2))
        })
    }

    /// cb_pool 重载接口，接受单个 &str 回调参数，返回 Fn(&str)
    /// 用于包装 Box<dyn Fn(&str)> 类型的 listener
    pub fn cb_pool_fn_str_boxed<F>(&self, cb: F) -> Box<dyn Fn(&str) + Send + Sync + 'static>
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn_str_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move |s: &str| {
            let s_clone = s.to_string();
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(&s_clone))
        })
    }

    /// cb_pool 重载接口，接受两个回调参数（第一个是 &str），返回 Fn(&str, R2)
    /// 用于包装 Box<dyn Fn(&str, R2)> 类型的 listener
    pub fn cb_pool_fn2_str_boxed<F, R2>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(&str, R2) + Send + Sync + 'static>
    where
        F: Fn(&str, R2) + Send + Sync + 'static,
        R2: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn2_str_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move |s: &str, r2: R2| {
            let s_clone = s.to_string();
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(&s_clone, r2))
        })
    }

    /// cb_pool 重载接口，接受三个回调参数（第一个是 &str），返回 Fn(&str, R2, R3)
    /// 用于包装 Box<dyn Fn(&str, R2, R3)> 类型的 listener
    pub fn cb_pool_fn3_str_boxed<F, R2, R3>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(&str, R2, R3) + Send + Sync + 'static>
    where
        F: Fn(&str, R2, R3) + Send + Sync + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn3_str_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move |s: &str, r2: R2, r3: R3| {
            let s_clone = s.to_string();
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(&s_clone, r2, r3))
        })
    }

    /// cb_pool 重载接口，接受四个回调参数（第一个是 &str），返回 Fn(&str, R2, R3, R4)
    /// 用于包装 Box<dyn Fn(&str, R2, R3, R4)> 类型的 listener
    pub fn cb_pool_fn4_str_boxed<F, R2, R3, R4>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(&str, R2, R3, R4) + Send + Sync + 'static>
    where
        F: Fn(&str, R2, R3, R4) + Send + Sync + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
        R4: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn4_str_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move |s: &str, r2: R2, r3: R3, r4: R4| {
            let s_clone = s.to_string();
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(&s_clone, r2, r3, r4))
        })
    }

    /// cb_pool 重载接口，接受四个回调参数（第一个和第四个是 &str），返回 Fn(&str, R2, R3, &str)
    /// 用于包装 Box<dyn Fn(&str, R2, R3, &str)> 类型的 listener
    pub fn cb_pool_fn4_str_str4_boxed<F, R2, R3>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(&str, R2, R3, &str) + Send + Sync + 'static>
    where
        F: Fn(&str, R2, R3, &str) + Send + Sync + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
    {
        crate::log_t!(LogType::Common; "cb_pool_fn4_str_str4_boxed", "callback_type", std::any::type_name_of_val(&cb));
        let cb_pool = self.cb_pool.clone();
        let cb = Arc::new(cb);
        Box::new(move |s1: &str, r2: R2, r3: R3, s2: &str| {
            let s1_clone = s1.to_string();
            let s2_clone = s2.to_string();
            let cb_clone = cb.clone();
            cb_pool.execute(move || cb_clone(&s1_clone, r2, r3, &s2_clone))
        })
    }
}

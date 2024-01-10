use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{backend::Backend, prelude::*};

use super::TaskReturns;

pub struct AsyncResultGetBuilder<T> {
    task_id: String,
    backend: Option<Arc<dyn Backend>>,
    timeout: Option<Duration>,
    interval: Duration,
    cleanup: bool,
    _marker: std::marker::PhantomData<T>,
}

impl<T> AsyncResultGetBuilder<T>
where
    T: TaskReturns,
{
    pub fn new(task_id: String, backend: Option<Arc<dyn Backend>>) -> Self {
        Self {
            task_id,
            backend,
            timeout: None,
            interval: Duration::from_millis(500),
            cleanup: false,
            _marker: std::marker::PhantomData,
        }
    }

    /// How long to wait, before the operation times out. This is the setting
    /// for the publisher (celery client) and is different from `timeout`
    /// parameter of `@app.task`, which is the setting for the worker. The task
    /// isn't terminated even if timeout occurs.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Time to wait before retrying to retrieve the result.
    #[must_use]
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Whether to remove the result from the backend after retrieving it.
    #[must_use]
    pub fn cleanup(mut self, cleanup: bool) -> Self {
        self.cleanup = cleanup;
        self
    }

    pub async fn fetch(self) -> Result<T, crate::error::CeleryError> {
        use super::TaskState::*;

        let backend = self.backend.unwrap();
        let start = Instant::now();
        let meta = loop {
            if let Some(timeout) = self.timeout {
                if start.elapsed() > timeout {
                    return Err(crate::error::TaskError::timeout().into());
                }
            }

            // Note: The task meta gets set by the client before the request is
            // send, so this call should never error with "response was nil".
            let meta = backend.get_task_meta(&self.task_id).await?;

            match meta.status {
                Pending | Started | Received => {
                    tokio::time::sleep(self.interval).await;
                }

                _ => break meta,
            }
        };

        let result = match (meta.status, meta.result) {
            (_, None) => {
                log::error!("task {} has no result", self.task_id);
                Err(TaskError::unexpected(
                    "Task succeeded but did not provide a result value.".to_string(),
                )
                .into())
            }

            (Failure, Some(val)) => {
                match (serde_json::from_value::<TaskError>(val), meta.traceback) {
                    (Ok(mut err), Some(traceback)) => {
                        err.exc_traceback = err
                            .exc_traceback
                            .map(|mut tb| {
                                tb += "\n\n";
                                tb += &traceback;
                                tb
                            })
                            .or(Some(traceback));
                        Err(err.into())
                    }

                    (Ok(err), _) => {
                        log::trace!("task {} failed", self.task_id);
                        Err(err.into())
                    }

                    (Err(err), _) => {
                        log::error!("unable to deserialize task failure value: {:?}", err);
                        Err(BackendError::Json(err).into())
                    }
                }
            }

            (_, Some(val)) => {
                log::trace!("task {} succeeded", self.task_id);
                T::from_json(val).map_err(|e| BackendError::Json(e).into())
            }
        };

        if self.cleanup {
            log::trace!("deleting task meta");
            backend.delete_task_meta(&self.task_id).await?;
        }

        result
    }
}

/// An [`AsyncResult`] is a handle for the result of a task.
#[derive(Clone)]
pub struct AsyncResult<T> {
    pub task_id: String,
    pub backend: Option<Arc<dyn Backend>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for AsyncResult<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncResult")
            .field("task_id", &self.task_id)
            .finish()
    }
}

impl<T> AsyncResult<T>
where
    T: TaskReturns,
{
    pub fn new(task_id: &str, backend: Option<Arc<dyn Backend>>) -> Self {
        Self {
            task_id: task_id.into(),
            backend,
            _marker: std::marker::PhantomData,
        }
    }

    /// Wait until task is ready, and return its result.
    pub fn get(&self) -> AsyncResultGetBuilder<T> {
        AsyncResultGetBuilder::new(self.task_id.clone(), self.backend.clone())
    }
}

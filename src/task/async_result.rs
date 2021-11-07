use serde::{Deserialize, Serialize};

use crate::{backend::Backend, prelude::BackendError};

use std::sync::Arc;

use super::TaskState;

/// An [`AsyncResult`] is a handle for the result of a task.
pub struct AsyncResult<B: Backend> {
    task_id: String,
    backend: Option<Arc<B>>,
}

impl<B: Backend> AsyncResult<B> {
    pub(crate) fn new(task_id: &str, backend: Option<Arc<B>>) -> Self {
        Self {
            task_id: task_id.into(),
            backend
        }
    }

    /// Returns true if task is failed
    pub async fn failed<T: Send + Sync + Unpin + for<'de>Deserialize<'de>>(&self) -> Result<bool, BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        Ok(backend.get_state::<T>(&self.task_id).await? == TaskState::Failure)
    }

    /// Forget result of task
    pub async fn forget<T: Send + Sync + Unpin + Serialize>(&self) -> Result<(), BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        Ok(backend.forget::<T>(&self.task_id).await?)
    }

    /// Returns true if task is finished
    pub async fn ready<T: Send + Sync + Unpin + for<'de>Deserialize<'de>>(&self) -> Result<bool, BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        let state = backend.get_state::<T>(&self.task_id).await?;
        Ok(state == TaskState::Success || state == TaskState::Failure)
    }

    /// Get result of task
    pub async fn result<T: Send + Sync + Unpin + for<'de>Deserialize<'de>>(&self) -> Result<Option<T>, BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        Ok(backend.get_result(&self.task_id).await?)
    }

    /// Task's state
    pub async fn state<T: Send + Sync + Unpin + for<'de>Deserialize<'de>>(&self) -> Result<TaskState, BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        Ok(backend.get_state::<T>(&self.task_id).await?)
    }

    /// Returns true if task is succeeded
    pub async fn successful<T: Send + Sync + Unpin + for<'de>Deserialize<'de>>(&self) -> Result<bool, BackendError> {
        self.throw_if_backend_not_set()?;
        let backend = self.backend.clone().unwrap();
        let state = backend.get_state::<T>(&self.task_id).await?;
        Ok(state == TaskState::Success)
    }

    /// Task's ID
    pub fn task_id(&self) -> String {
        self.task_id.clone()
    }

    fn throw_if_backend_not_set(&self) -> Result<(), BackendError> {
        match &self.backend {
            Some(_) => Ok(()),
            None => Err(BackendError::NotSet)
        }
    }
}

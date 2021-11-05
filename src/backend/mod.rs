#[doc(hidden)]
pub mod empty;
pub(crate) mod mock;

use crate::error::BackendError;
use crate::task::TaskStatus;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A results [`Backend`] is used to store and retrive the results and status of the tasks.
#[async_trait]
pub trait Backend: Send + Sync + Sized {
    /// The builder type used to create the results backend with a custom configuration.
    type Builder: BackendBuilder<Backend = Self>;

    async fn mark_as_done<T: Send>(&self, task_id: &str, result: T) -> Result<(), BackendError> {
        self.store_result(task_id, Some(result), None, TaskStatus::Success).await
    }

    async fn mark_as_failure<T: Send>(&self, task_id: &str, traceback: String) -> Result<(), BackendError> {
        self.store_result::<T>(task_id, None as Option<_>, Some(traceback.to_owned()), TaskStatus::Failure).await
    }

    /// Update task state and result.
    async fn store_result<T: Send>(
        &self,
        task_id: &str,
        result: Option<T>,
        traceback: Option<String>,
        state: TaskStatus,
    ) -> Result<(), BackendError> {
        // TODO: Add retry
        self.store_result_inner(task_id, result, traceback, state).await
    }

    /// Update task state and result.
    async fn store_result_inner<T: Send>(&self, task_id: &str, result: Option<T>, traceback: Option<String>, state: TaskStatus) -> Result<(), BackendError>;

    /// Get task meta from backend.
    async fn get_task_meta<T>(&self, task_id: &str) -> Result<ResultMetadata<T>, BackendError>;

    /// Get current state of a given task.
    async fn get_state<T>(&self, task_id: &str) -> Result<TaskStatus, BackendError> {
        Ok(self.get_task_meta::<T>(task_id).await?.status)
    }

    /// Get result of a given task.
    async fn get_result<T>(&self, task_id: &str) -> Result<Option<T>, BackendError> {
        Ok(self.get_task_meta(task_id).await?.result)
    }
}

/// Metadata of the task stored in the storage used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultMetadata<T> {
    /// id of the task
    task_id: String,
    /// Current status of the task.
    status: TaskStatus,
    /// Result of the task.
    result: Option<T>,
    /// Date of culmination of the task
    date_done: Option<DateTime<Utc>>,
}

impl<T> ResultMetadata<T> {
    pub fn new(task_id: &str, result: T, status: TaskStatus) -> Self {
        let date_done = if let TaskStatus::Success = status {
            Some(Utc::now())
        } else {
            None
        };

        Self {
            status,
            result: Some(result),
            task_id: task_id.to_string(),
            date_done,
        }
    }
}

/// A [`BackendBuilder`] is used to create a type of results [`Backend`] with a custom configuration.
#[async_trait]
pub trait BackendBuilder {
    type Backend: Backend;

    /// Create a new `BackendBuilder`.
    fn new(broker_url: &str) -> Self;

    /// Construct the `Backend` with the given configuration.
    async fn build(&self, connection_timeout: u32) -> Result<Self::Backend, BackendError>;
}

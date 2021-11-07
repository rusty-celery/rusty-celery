#[doc(hidden)]
pub mod empty;

#[cfg(feature = "backend_mongo")]
pub mod mongo;

#[cfg(test)]
pub(crate) mod mock;

use crate::task::TaskState;
use crate::{error::BackendError, prelude::TaskError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A results [`Backend`] is used to store and retrive the results and status of the tasks.
#[async_trait]
pub trait Backend: Send + Sync + Sized {
    /// The builder type used to create the results backend with a custom configuration.
    type Builder: BackendBuilder<Backend = Self>;

    /// Add task to collection
    async fn add_task<T: Send + Sync + Unpin + Serialize>(
        &self,
        task_id: &str,
    ) -> Result<(), BackendError> {
        let metadata = ResultMetadata {
            status: TaskState::Pending,
            result: None,
            traceback: None,
            date_done: None,
        };
        self.store_result::<T>(task_id, metadata).await
    }

    /// Mark task as started to trace
    async fn mark_as_started<T: Send + Sync + Unpin + Serialize>(
        &self,
        task_id: &str,
    ) -> Result<(), BackendError> {
        let metadata = ResultMetadata {
            status: TaskState::Started,
            result: None,
            traceback: None,
            date_done: None,
        };
        self.store_result::<T>(task_id, metadata).await
    }

    /// Mark task as finished and save result
    async fn mark_as_done<T: Send + Sync + Unpin + Serialize>(
        &self,
        task_id: &str,
        result: T,
        date_done: DateTime<Utc>,
    ) -> Result<(), BackendError> {
        let metadata = ResultMetadata {
            status: TaskState::Success,
            result: Some(result),
            traceback: None,
            date_done: Some(date_done),
        };
        self.store_result(task_id, metadata).await
    }

    /// Mark task as failure and save error
    async fn mark_as_failure<T: Send + Sync + Unpin + Serialize>(
        &self,
        task_id: &str,
        traceback: TaskError,
        date_done: DateTime<Utc>,
    ) -> Result<(), BackendError> {
        let metadata = ResultMetadata {
            status: TaskState::Failure,
            result: None,
            traceback: Some(traceback),
            date_done: Some(date_done),
        };
        self.store_result::<T>(task_id, metadata).await
    }

    /// Update task state and result.
    async fn store_result<T: Send + Sync + Unpin + Serialize>(
        &self,
        task_id: &str,
        metadata: ResultMetadata<T>,
    ) -> Result<(), BackendError> {
        // TODO: Add retry
        self.store_result_inner(task_id, Some(metadata)).await
    }

    /// Forget task result
    async fn forget<T: Send + Sync + Unpin + Serialize>(
        &self,
        task_id: &str,
    ) -> Result<(), BackendError> {
        self.store_result_inner::<T>(task_id, None).await
    }

    /// Update task state and result.
    async fn store_result_inner<T: Send + Sync + Unpin + Serialize>(
        &self,
        task_id: &str,
        metadata: Option<ResultMetadata<T>>,
    ) -> Result<(), BackendError>;

    /// Get task meta from backend.
    async fn get_task_meta<T: Send + Sync + Unpin + for<'de> Deserialize<'de>>(
        &self,
        task_id: &str,
    ) -> Result<ResultMetadata<T>, BackendError>;

    /// Get current state of a given task.
    async fn get_state<T: Send + Sync + Unpin + for<'de> Deserialize<'de>>(
        &self,
        task_id: &str,
    ) -> Result<TaskState, BackendError> {
        Ok(self.get_task_meta::<T>(task_id).await?.status)
    }

    /// Get result of a given task.
    async fn get_result<T: Send + Sync + Unpin + for<'de> Deserialize<'de>>(
        &self,
        task_id: &str,
    ) -> Result<Option<T>, BackendError> {
        Ok(self.get_task_meta(task_id).await?.result)
    }

    /// Get result of a given task.
    async fn get_traceback<T: Send + Sync + Unpin + for<'de> Deserialize<'de>>(
        &self,
        task_id: &str,
    ) -> Result<Option<TaskError>, BackendError> {
        Ok(self.get_task_meta::<T>(task_id).await?.traceback)
    }
}

/// Metadata of the task stored in the storage used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultMetadata<T: Send + Sync + Unpin> {
    /// Current status of the task.
    status: TaskState,
    /// Result of the task.
    result: Option<T>,
    /// Error of the task.
    traceback: Option<TaskError>,
    /// Date of culmination of the task
    date_done: Option<DateTime<Utc>>,
}

/// A [`BackendBuilder`] is used to create a type of results [`Backend`] with a custom configuration.
#[async_trait]
pub trait BackendBuilder {
    type Backend: Backend;

    /// Create a new `BackendBuilder`.
    fn new(broker_url: &str) -> Self;

    /// Construct the `Backend` with the given configuration.
    async fn build(self, connection_timeout: u32) -> Result<Self::Backend, BackendError>;
}

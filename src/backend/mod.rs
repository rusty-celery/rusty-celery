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

    /// Update task state and result.
    async fn store_result(
        &self,
        task_id: &str,
        result: String,
        state: TaskStatus,
    ) -> Result<(), ()>;

    /// Get task meta from backend.
    async fn get_task_meta(&self, task_id: &str) -> Result<ResultMetadata, BackendError>;

    /// Get current state of a given task.
    async fn get_state(&self, task_id: &str) -> Result<TaskStatus, BackendError> {
        Ok(self.get_task_meta(task_id).await?.status)
    }

    /// Get result of a given task.
    async fn get_result(&self, task_id: &str) -> Result<Option<String>, BackendError> {
        Ok(self.get_task_meta(task_id).await?.result)
    }
}

/// Metadata of the task stored in the storage used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultMetadata {
    /// id of the task
    task_id: String,
    /// Current status of the task.
    status: TaskStatus,
    /// Result of the task.
    result: Option<String>,
    /// Date of culmination of the task
    date_done: Option<DateTime<Utc>>,
}

impl ResultMetadata {
    pub fn new(task_id: &str, result: String, status: TaskStatus) -> Self {
        let date_done = if let TaskStatus::Finished = status {
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

    /// Construct the `Backend` with the given configuration.
    async fn build(&self, connection_timeout: u32) -> Result<Self::Backend, ()>;
}

mod redis;

use std::sync::Arc;

use chrono::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    prelude::{BackendError, TaskError},
    task::{TaskResult, TaskReturns, TaskState},
};
pub use redis::RedisBackend;
use redis::RedisBackendBuilder;

use async_trait::async_trait;

/// A [`BackendBuilder`] is used to create a type of results [`Backend`] with a custom configuration.
#[async_trait]
pub trait BackendBuilder {
    /// Create a new `BackendBuilder`.
    fn new(broker_url: &str) -> Self
    where
        Self: Sized;

    /// Set database name.
    fn database(self: Box<Self>, database: &str) -> Box<dyn BackendBuilder>;

    /// Set database collection name.
    fn connection_timeout(self: Box<Self>, timeout: u32) -> Box<dyn BackendBuilder>;

    /// Set database collection name.
    fn taskmeta_collection(self: Box<Self>, collection_name: &str) -> Box<dyn BackendBuilder>;

    /// Construct the `Backend` with the given configuration.
    async fn build(self: Box<Self>) -> Result<Arc<dyn Backend>, BackendError>;
}

#[async_trait]
pub trait Backend: Send + Sync {
    /// Update task state and result.
    async fn store_task_meta(&self, task_id: &str, metadata: TaskMeta) -> Result<(), BackendError>;

    /// Get task meta from backend.
    async fn get_task_meta(&self, task_id: &str) -> Result<TaskMeta, BackendError>;

    /// Delete task meta from backend.
    async fn delete_task_meta(&self, task_id: &str) -> Result<(), BackendError>;

    async fn store_result(
        &self,
        task_id: &str,
        result: TaskResult<Box<dyn TaskReturns>>,
    ) -> Result<(), BackendError> {
        let (status, result) = match result {
            Ok(val) => (TaskState::Success, val.to_json()?),
            Err(err) => (TaskState::Failure, serde_json::to_value(err)?),
        };

        self.store_task_meta(
            task_id,
            TaskMeta {
                task_id: task_id.to_string(),
                status,
                result: Some(result),
                traceback: None,
                date_done: NaiveDateTime::from_timestamp_opt(chrono::Utc::now().timestamp(), 0),
            },
        )
        .await
    }

    /// Add task to collection
    async fn add_task(&self, task_id: &str) -> Result<(), BackendError> {
        let metadata = TaskMeta {
            task_id: task_id.to_string(),
            status: TaskState::Pending,
            result: None,
            traceback: None,
            date_done: None,
        };
        self.store_task_meta(task_id, metadata).await
    }

    /// Mark task as started to trace
    async fn mark_as_started(&self, task_id: &str) -> Result<(), BackendError> {
        let metadata = TaskMeta {
            task_id: task_id.to_string(),
            status: TaskState::Started,
            result: None,
            traceback: None,
            date_done: None,
        };
        self.store_task_meta(task_id, metadata).await
    }

    /// Mark task as failure and save error
    async fn mark_as_failure(
        &self,
        task_id: &str,
        error: TaskError,
        date_done: NaiveDateTime,
    ) -> Result<(), BackendError> {
        let metadata = TaskMeta {
            task_id: task_id.to_string(),
            status: TaskState::Failure,
            result: Some(serde_json::to_value(error)?),
            traceback: None,
            date_done: Some(date_done),
        };
        self.store_task_meta(task_id, metadata).await
    }
}

pub fn backend_builder_from_url(backend_url: impl AsRef<str>) -> Box<dyn BackendBuilder> {
    let backend_url = backend_url.as_ref();
    match backend_url.split_once("://") {
        Some(("redis", _)) => Box::new(RedisBackendBuilder::new(backend_url)),
        _ => panic!("Unsupported backend"),
    }
}

/// Metadata of the task stored in the storage used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMeta {
    /// Task's ID.
    pub(crate) task_id: String,
    /// Current status of the task.
    pub(crate) status: TaskState,
    /// Result of the task.
    pub result: Option<serde_json::Value>,
    /// Error of the task.
    pub(crate) traceback: Option<String>,
    /// Date of culmination of the task
    pub(crate) date_done: Option<NaiveDateTime>,
    // TODO
    // pub(crate) children: Option<Vec<String>>,
}

//! Provides the `Task` trait as well as options for configuring tasks.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::TaskError;

mod options;

pub use options::{TaskOptions, TaskSendOptions, TaskSendOptionsBuilder};

/// A `Task` represents a unit of work that a `Celery` app can produce or consume.
///
/// For more information see the [tasks chapter](https://rusty-celery.github.io/guide/defining-tasks.html)
/// in the Rusty Celery Book.
#[async_trait]
pub trait Task: Send + Sync {
    /// The name of the task. When a task is registered it will be registered with this name.
    const NAME: &'static str;

    /// For compatability with Python tasks. This keeps track of the order
    /// of arguments for the task so that the task can be called from Python with
    /// positional arguments.
    const ARGS: &'static [&'static str];

    /// The parameters of the task.
    type Params: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>;

    /// The return type of the task.
    type Returns: Send + Sync + std::fmt::Debug;

    /// Used to initialize a task instance from within a Celery app.
    fn within_app() -> Self;

    /// This function defines how a task executes.
    async fn run(&self, params: Self::Params) -> Result<Self::Returns, TaskError>;

    /// Callback that will run after a task fails.
    #[allow(unused_variables)]
    async fn on_failure(&self, err: &TaskError, task_id: &str, params: Self::Params) {}

    /// Callback that will run after a task completes successfully.
    #[allow(unused_variables)]
    async fn on_success(&self, returned: &Self::Returns, task_id: &str, params: Self::Params) {}

    /// Default timeout for this task.
    fn timeout(&self) -> Option<u32> {
        None
    }

    /// Default maximum number of retries for this task.
    fn max_retries(&self) -> Option<u32> {
        None
    }

    /// Default minimum retry delay (in seconds) for this task (default is 0).
    fn min_retry_delay(&self) -> Option<u32> {
        None
    }

    /// Default maximum retry delay (in seconds) for this task.
    fn max_retry_delay(&self) -> Option<u32> {
        None
    }

    /// Whether messages for this task will be acknowledged after the task has been executed,
    /// or before (the default behavior).
    fn acks_late(&self) -> Option<bool> {
        None
    }
}

pub struct TaskSignature<T>
where
    T: Task,
{
    pub params: T::Params,
}

impl<T> TaskSignature<T>
where
    T: Task,
{
    pub fn new(params: T::Params) -> Self {
        Self { params }
    }

    pub fn task_name() -> &'static str {
        T::NAME
    }
}

#[derive(Clone, Debug)]
pub(crate) enum TaskEvent {
    StatusChange(TaskStatus),
}

#[derive(Clone, Debug)]
pub(crate) enum TaskStatus {
    Pending,
    Finished,
}

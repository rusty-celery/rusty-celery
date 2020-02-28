//! Provides the `Task` trait as well as options for configuring tasks.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::TaskError;

mod options;

pub use options::{TaskOptions, TaskSendOptions, TaskSendOptionsBuilder};

/// A return type for a task.
pub type TaskResult<R> = Result<R, TaskError>;

/// A `Task` represents a unit of work that a `Celery` app can produce or consume.
///
/// The recommended way to create tasks is through the [`task`](attr.task.html) attribute macro, not by directly implementing
/// this trait. For more information see the [tasks chapter](https://rusty-celery.github.io/guide/defining-tasks.html)
/// in the Rusty Celery Book.
#[async_trait]
pub trait Task: Send + Sync {
    /// The name of the task. When a task is registered it will be registered with this name.
    const NAME: &'static str;

    /// For compatability with Python tasks. This keeps track of the order
    /// of arguments for the task so that the task can be called from Python with
    /// positional arguments.
    const ARGS: &'static [&'static str];

    /// Default task options.
    const DEFAULTS: TaskOptions = TaskOptions {
        timeout: None,
        max_retries: None,
        min_retry_delay: None,
        max_retry_delay: None,
        acks_late: None,
    };

    /// The parameters of the task.
    type Params: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>;

    /// The return type of the task.
    type Returns: Send + Sync + std::fmt::Debug;

    /// Used to initialize a task instance from a request.
    fn from_request() -> Self;

    /// This function defines how a task executes.
    async fn run(&self, params: Self::Params) -> TaskResult<Self::Returns>;

    /// Callback that will run after a task fails.
    #[allow(unused_variables)]
    async fn on_failure(&self, err: &TaskError, task_id: &str, params: Self::Params) {}

    /// Callback that will run after a task completes successfully.
    #[allow(unused_variables)]
    async fn on_success(&self, returned: &Self::Returns, task_id: &str, params: Self::Params) {}

    /// Returns the registered name of the task.
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// Default timeout for this task.
    fn timeout(&self) -> Option<u32> {
        Self::DEFAULTS.timeout
    }

    /// Default maximum number of retries for this task.
    fn max_retries(&self) -> Option<u32> {
        Self::DEFAULTS.max_retries
    }

    /// Default minimum retry delay (in seconds) for this task (default is 0).
    fn min_retry_delay(&self) -> Option<u32> {
        Self::DEFAULTS.min_retry_delay
    }

    /// Default maximum retry delay (in seconds) for this task.
    fn max_retry_delay(&self) -> Option<u32> {
        Self::DEFAULTS.max_retry_delay
    }

    /// Whether messages for this task will be acknowledged after the task has been executed,
    /// or before (the default behavior).
    fn acks_late(&self) -> Option<bool> {
        Self::DEFAULTS.acks_late
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

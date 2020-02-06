use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Error;

mod options;

pub use options::{TaskOptions, TaskSendOptions, TaskSendOptionsBuilder};

/// A `Task` represents a unit of work that a `Celery` app can produce or consume.
///
/// For more information see the [tasks chapter](https://rusty-celery.github.io/guide/defining-tasks.html)
/// in the Rusty Celery Book.
#[async_trait]
pub trait Task: Send + Sync + Serialize + for<'de> Deserialize<'de> {
    /// The name of the task. When a task is registered it will be registered with this name.
    const NAME: &'static str;

    /// For compatability with Python tasks. This keeps track of the order
    /// of arguments for the task so that the task can be called from Python with
    /// positional arguments.
    const ARGS: &'static [&'static str];

    /// The return type of the task.
    type Returns: Send + Sync + std::fmt::Debug;

    /// This function defines how a task executes.
    async fn run(mut self) -> Result<Self::Returns, Error>;

    /// Callback that will run after a task fails.
    /// It takes a reference to a `TaskContext` struct and the error returned from task.
    #[allow(unused_variables)]
    async fn on_failure(ctx: &TaskContext<'_>, err: &Error) {}

    /// Callback that will run after a task completes successfully.
    /// It takes a reference to a `TaskContext` struct and the returned value from task.
    #[allow(unused_variables)]
    async fn on_success(ctx: &TaskContext<'_>, returned: &Self::Returns) {}

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

/// Additional context sent to the `on_success` and `on_failure` task callbacks.
pub struct TaskContext<'a> {
    /// The correlation ID of the task.
    pub correlation_id: &'a str,
}

#[derive(Clone, Debug)]
pub(crate) enum TaskStatus {
    Pending,
    Finished,
}

#[derive(Clone, Debug)]
pub(crate) struct TaskEvent {
    pub(crate) status: TaskStatus,
}

impl TaskEvent {
    pub(crate) fn new(status: TaskStatus) -> Self {
        Self { status }
    }
}

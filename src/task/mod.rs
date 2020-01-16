use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// A `Task` represents a unit of work that a `Celery` app can produce or consume.
///
/// The recommended way to define a task is through the `task` procedural macro.
///
/// # Example
///
/// ```rust
/// use celery::{task, Error};
///
/// #[task(name = "add")]
/// fn Add(x: i32, y: i32) -> i32 {
///     x + y
/// }
/// ```
#[async_trait]
pub trait Task: Send + Sync + Serialize + for<'de> Deserialize<'de> {
    const NAME: &'static str;

    /// The return type of the task.
    type Returns: Send + Sync + std::fmt::Debug;

    /// For compatability with Python tasks.
    ///
    /// Tasks defined in Rust are serialized and deserialized from a mapping which is stored
    /// as the task `kwargs`. Therefore there is no concept of `args` (positional arguments)
    /// for Rust tasks, unlike Python tasks. While this is fine if you are producing tasks from Rust
    /// for a Rust or Python worker, issues arise if you are producing tasks from Python with
    /// positional arguments.
    ///
    /// In that case this function should return the field names of the task
    /// struct in the order in which they would appear as positional arguments.
    fn arg_names() -> Vec<String> {
        vec![]
    }

    /// This function defines how a task executes.
    async fn run(&mut self) -> Result<Self::Returns, Error>;

    /// This function can be overriden to provide custom logic that will run after a task fails.
    /// The argument to the function is the error returned by the task.
    #[allow(unused_variables)]
    async fn on_failure(&mut self, err: Error) -> Result<(), Error> {
        Ok(())
    }

    /// This function can be overriden to provide custom logic that will run after a task completes
    /// successfully. The argument to the function is the returned value of the task.
    #[allow(unused_variables)]
    async fn on_success(&mut self, returned: Self::Returns) -> Result<(), Error> {
        Ok(())
    }

    /// Set a default timeout for this task.
    fn timeout(&self) -> Option<usize> {
        None
    }

    /// Set a default maximum number of retries for this task.
    fn max_retries(&self) -> Option<usize> {
        None
    }

    /// Set a default minimum retry delay (in seconds) for this task (default is 0).
    fn min_retry_delay(&self) -> usize {
        0
    }

    /// Set a default maximum retry delay (in seconds) for this task (default is 3600 seconds).
    fn max_retry_delay(&self) -> usize {
        3600
    }
}

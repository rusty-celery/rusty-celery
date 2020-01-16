use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// A `Task` represents a unit of work that a `Celery` app can produce or consume.
///
/// The recommended way to define a task is through the [`task`](attr.task.html) procedural macro:
///
/// ```rust
/// use celery::task;
///
/// #[task(name = "add")]
/// fn add(x: i32, y: i32) -> i32 {
///     x + y
/// }
/// ```
///
/// However, if you need more fine-grained control, it is fairly straight forward
/// to implement a task directly. This would be equivalent to above:
///
/// ```rust
/// use async_trait::async_trait;
/// use serde::{Serialize, Deserialize};
/// use celery::{Task, Error};
///
/// #[allow(non_camel_case_types)]
/// #[derive(Serialize, Deserialize)]
/// struct add {
///     x: i32,
///     y: i32,
/// }
///
/// #[async_trait]
/// impl Task for add {
///     const NAME: &'static str = "add";
///     const ARGS: &'static [&'static str] = &["x", "y"];
///
///     type Returns = i32;
///
///     async fn run(&mut self) -> Result<Self::Returns, Error> {
///         Ok(self.x + self.y)
///     }
/// }
/// ```
#[async_trait]
pub trait Task: Send + Sync + Serialize + for<'de> Deserialize<'de> {
    /// The unique name of the task.
    const NAME: &'static str;

    /// For compatability with Python tasks. This keeps track of the position / order
    /// or arguments for the task.
    const ARGS: &'static [&'static str];

    /// The return type of the task.
    type Returns: Send + Sync + std::fmt::Debug;

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

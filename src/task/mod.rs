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
///
/// fn add(x: i32, y: i32) -> add {
///     add { x, y }
/// }
/// ```
///
/// # Error handling
///
/// As shown above, a task tries wrapping the return value in `Result<Self::Returns, Error>`
/// when it is executed. Therefore the recommended way to propogate errors is to use the `?`
/// operator within the task body.
///
/// For example:
///
/// ```rust
/// # use celery::task;
/// use failure::ResultExt;
///
/// #[task(name = "add")]
/// fn read_some_file() -> String {
///     tokio::fs::read_to_string("some_file")
///         .await
///         .context("File does not exist")?
/// }
/// ```
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

    /// Default timeout for this task.
    fn timeout(&self) -> Option<usize> {
        None
    }

    /// Default maximum number of retries for this task.
    fn max_retries(&self) -> Option<usize> {
        None
    }

    /// Default minimum retry delay (in seconds) for this task (default is 0).
    fn min_retry_delay(&self) -> usize {
        0
    }

    /// Default maximum retry delay (in seconds) for this task (default is 3600 seconds).
    fn max_retry_delay(&self) -> usize {
        3600
    }
}

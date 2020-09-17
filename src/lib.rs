//! A Rust implementation of [Celery](http://www.celeryproject.org/) for producing and consuming
//! asyncronous tasks with a distributed message queue.
//!
//! # Examples
//!
//! Define tasks by decorating functions with the [`task`](attr.task.html) attribute:
//!
//! ```rust
//! use celery::TaskResult;
//!
//! #[celery::task]
//! fn add(x: i32, y: i32) -> TaskResult<i32> {
//!     Ok(x + y)
//! }
//! ```
//!
//! Then create a [`Celery`](struct.Celery.html) app with the [`app`](macro.app.html)
//! macro and register your tasks with it:
//!
//! ```rust,no_run
//! # #[celery::task]
//! # fn add(x: i32, y: i32) -> celery::TaskResult<i32> {
//! #     Ok(x + y)
//! # }
//! let my_app = celery::app!(
//!     broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
//!     tasks = [add],
//!     task_routes = [],
//! );
//! ```
//!
//! The Celery app can be used as either a producer or consumer (worker). To send tasks to a
//! queue for a worker to consume, use the [`Celery::send_task`](struct.Celery.html#method.send_task) method:
//!
//! ```rust,no_run
//! # #[celery::task]
//! # fn add(x: i32, y: i32) -> celery::TaskResult<i32> {
//! #     Ok(x + y)
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), exitfailure::ExitFailure> {
//! # let my_app = celery::app!(
//! #     broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
//! #     tasks = [add],
//! #     task_routes = [],
//! # );
//! my_app.send_task(add::new(1, 2)).await?;
//! #   Ok(())
//! # }
//! ```
//!
//! And to act as worker and consume tasks sent to a queue by a producer, use the
//! [`Celery::consume`](struct.Celery.html#method.consume) method:
//!
//! ```rust,no_run
//! # #[celery::task]
//! # fn add(x: i32, y: i32) -> celery::TaskResult<i32> {
//! #     Ok(x + y)
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), exitfailure::ExitFailure> {
//! # let my_app = celery::app!(
//! #     broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
//! #     tasks = [add],
//! #     task_routes = [],
//! # );
//! my_app.consume().await?;
//! # Ok(())
//! # }
//! ```

#![doc(
    html_favicon_url = "https://structurely-images.s3-us-west-2.amazonaws.com/logos/rusty-celery.ico"
)]
#![doc(
    html_logo_url = "https://structurely-images.s3-us-west-2.amazonaws.com/logos/rusty-celery-4.png"
)]

mod app;
pub use app::{Celery, CeleryBuilder};
pub mod broker;
pub mod error;
pub use error::TaskResultExt;
pub mod protocol;
pub mod task;
pub use task::TaskResult;

#[cfg(feature = "codegen")]
mod codegen;

/// A procedural macro for generating a [`Task`](task/trait.Task.html) from a function.
///
/// If the annotated function has a return value, the return value must be a
/// [`TaskResult<R>`](task/type.TaskResult.html).
///
/// # Parameters
///
/// - `name`: The name to use when registering the task. Should be unique. If not given the name
/// will be set to the name of the function being decorated.
/// - `timeout`: Set a task-level [`TaskOptions::timeout`](task/struct.TaskOptions.html#structfield.timeout).
/// - `max_retries`: Set a task-level [`TaskOptions::max_retries`](task/struct.TaskOptions.html#structfield.max_retries).
/// - `min_retry_delay`: Set a task-level [`TaskOptions::min_retry_delay`](task/struct.TaskOptions.html#structfield.min_retry_delay).
/// - `max_retry_delay`: Set a task-level [`TaskOptions::max_retry_delay`](task/struct.TaskOptions.html#structfield.max_retry_delay).
/// - `retry_for_unexpected`: Set a task-level [`TaskOptions::retry_for_unexpected`](task/struct.TaskOptions.html#structfield.retry_for_unexpected).
/// - `acks_late`: Set a task-level [`TaskOptions::acks_late`](task/struct.TaskOptions.html#structfield.acks_late).
/// - `bind`: A bool. If true, the task will be run like an instance method and so the function's
/// first argument should be a reference to `Self`. Note however that Rust won't allow you to call
/// the argument `self`. Instead, you could use `task` or just `t`.
/// - `on_failure`: An async callback function to run when the task fails. Should accept a reference to
/// a task instance and a reference to a `TaskError`.
/// - `on_success`: An async callback function to run when the task succeeds. Should accept a reference to
/// a task instance and a reference to the value returned by the task.
///
/// For more information see the [tasks chapter](https://rusty-celery.github.io/guide/defining-tasks.html)
/// in the Rusty Celery Book.
///
/// ## Examples
///
/// Create a task named `add` with all of the default options:
///
/// ```rust
/// use celery::TaskResult;
///
/// #[celery::task]
/// fn add(x: i32, y: i32) -> TaskResult<i32> {
///     Ok(x + y)
/// }
/// ```
///
/// Use a name different from the function name:
///
/// ```rust
/// # use celery::TaskResult;
/// #[celery::task(name = "sum")]
/// fn add(x: i32, y: i32) -> TaskResult<i32> {
///     Ok(x + y)
/// }
/// ```
///
/// Customize the default retry behavior:
///
/// ```rust
/// # use celery::TaskResult;
/// #[celery::task(
///     timeout = 3,
///     max_retries = 100,
///     min_retry_delay = 1,
///     max_retry_delay = 60,
/// )]
/// async fn io_task() -> TaskResult<()> {
///     // Do some async IO work that could possible fail, such as an HTTP request...
///     Ok(())
/// }
/// ```
///
/// Bind the function to the task instance so it runs like an instance method:
///
/// ```rust
/// # use celery::task::{Task, TaskResult};
/// #[celery::task(bind = true)]
/// fn bound_task(task: &Self) {
///     println!("Hello, World! From {}", task.name());
/// }
/// ```
///
/// Run custom callbacks on failure and on success:
///
/// ```rust
/// # use celery::task::{Task, TaskResult};
/// # use celery::error::TaskError;
/// #[celery::task(on_failure = failure_callback, on_success = success_callback)]
/// fn task_with_callbacks() {}
///
/// async fn failure_callback<T: Task>(task: &T, err: &TaskError) {
///     println!("{} failed with {:?}", task.name(), err);
/// }
///
/// async fn success_callback<T: Task>(task: &T, ret: &T::Returns) {
///     println!("{} succeeded: {:?}", task.name(), ret);
/// }
/// ```
#[cfg(feature = "codegen")]
pub use codegen::task;

#[cfg(feature = "codegen")]
#[doc(hidden)]
pub mod export;

#[cfg(feature = "codegen")]
extern crate futures;

#[cfg(feature = "codegen")]
extern crate once_cell;

#[cfg(feature = "codegen")]
extern crate async_trait;

#[cfg(feature = "codegen")]
extern crate serde;

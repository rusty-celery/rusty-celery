//! A Rust implementation of [Celery](http://www.celeryproject.org/) for producing and consuming
//! asyncronous tasks with a distributed message queue.
//!
//! # Examples
//!
//! Define tasks by decorating functions with the [`task`](attr.task.html) attribute:
//!
//! ```rust
//! #[celery::task]
//! fn add(x: i32, y: i32) -> i32 {
//!     x + y
//! }
//! ```
//!
//! Then create a [`Celery`](struct.Celery.html) app with the [`app`](macro.app.html)
//! macro and register your tasks with it:
//!
//! ```rust,no_run
//! # #[celery::task]
//! # fn add(x: i32, y: i32) -> i32 {
//! #     x + y
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
//! # fn add(x: i32, y: i32) -> i32 {
//! #     x + y
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), exitfailure::ExitFailure> {
//! # let my_app = celery::app!(
//! #     broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
//! #     tasks = [add],
//! #     task_routes = [],
//! # );
//! my_app.send_task::<add>(add::s(1, 2)).await?;
//! #   Ok(())
//! # }
//! ```
//!
//! And to act as worker and consume tasks sent to a queue by a producer, use the
//! [`Celery::consume`](struct.Celery.html#method.consume) method:
//!
//! ```rust,no_run
//! # #[celery::task]
//! # fn add(x: i32, y: i32) -> i32 {
//! #     x + y
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

/////////////////
// Re-exports. //
/////////////////

#[cfg(feature = "codegen")]
extern crate futures;

#[cfg(feature = "codegen")]
extern crate once_cell;

#[cfg(feature = "codegen")]
extern crate async_trait;

#[cfg(feature = "codegen")]
extern crate serde;

/////////////////
// Submodules. //
/////////////////

mod app;

pub mod broker;

#[cfg(feature = "codegen")]
mod codegen;

pub mod error;

#[cfg(feature = "codegen")]
#[doc(hidden)]
pub mod export;

pub mod protocol;

pub mod task;

/////////////////
// Public API. //
/////////////////

pub use app::{Celery, CeleryBuilder};

#[cfg(feature = "codegen")]
pub use celery_codegen::task;

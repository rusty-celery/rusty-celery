//! A Rust implementation of [Celery](http://www.celeryproject.org/) for producing and consuming
//! asyncronous tasks with a distributed message queue.
//!
//! # Examples
//!
//! Create a task by decorating a function with the [`task`](attr.task.html) attribute:
//!
//! ```rust
//! # use celery::task;
//! #[task]
//! fn add(x: i32, y: i32) -> i32 {
//!     x + y
//! }
//! ```
//!
//! Then create a [`Celery`](struct.Celery.html) app with the [`celery_app`](macro.celery_app.html)
//! and register your tasks with it:
//!
//! ```rust,no_run
//! # use celery::{celery_app, task, AMQPBroker};
//! # #[task]
//! # fn add(x: i32, y: i32) -> i32 {
//! #     x + y
//! # }
//! let my_app = celery_app!(
//!     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
//!     tasks = [add],
//!     task_routes = [],
//! );
//! ```
//!
//! The Celery app can be used as either a producer or consumer (worker). To send tasks to a
//! queue for a worker to consume, use the [`Celery::send_task`](struct.Celery.html#method.send_task) method:
//!
//! ```rust,no_run
//! # use celery::{celery_app, task, AMQPBroker};
//! # #[task]
//! # fn add(x: i32, y: i32) -> i32 {
//! #     x + y
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), exitfailure::ExitFailure> {
//! # let my_app = celery_app!(
//! #     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
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
//! # use celery::{celery_app, task, AMQPBroker};
//! # #[task]
//! # fn add(x: i32, y: i32) -> i32 {
//! #     x + y
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), exitfailure::ExitFailure> {
//! # let my_app = celery_app!(
//! #     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
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

extern crate failure;
pub use failure::ResultExt;

#[cfg(feature = "codegen")]
extern crate once_cell;

#[cfg(feature = "codegen")]
extern crate async_trait;

#[cfg(feature = "codegen")]
extern crate serde;

/////////////////
// Submodules. //
/////////////////

// Defines the `Celery` app struct which the primary publich interface to Rusty Celery,
// used to produce or consume tasks.
mod app;

// The broker is an integral part of a `Celery` app. It provides the transport for
// producing and consuming tasks.
mod broker;

// Macro rules for quickly defining apps.
#[cfg(feature = "codegen")]
mod codegen;

// Defines the `Error` type used across Rusty Celery.
mod error;

// Used only by the codegen modules.
#[cfg(feature = "codegen")]
#[doc(hidden)]
pub mod export;

// Defines the Celery protocol.
pub mod protocol;

// Provides the `Task` trait. Tasks are then created by defining a struct and implementing
// this trait for the struct. However the `#[task]` macro provided by `celery-codegen`
// abstracts most of this away.
mod task;

/////////////////
// Public API. //
/////////////////

pub use app::{Celery, CeleryBuilder, Rule};
pub use broker::{
    amqp::{AMQPBroker, AMQPBrokerBuilder},
    Broker, BrokerBuilder,
};
pub use error::{Error, ErrorKind};
pub use task::{Task, TaskContext, TaskOptions, TaskSendOptions, TaskSendOptionsBuilder};

// Proc macros for defining tasks.
#[cfg(feature = "codegen")]
pub use celery_codegen::task;

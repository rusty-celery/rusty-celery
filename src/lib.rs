//! Celery for Rust.

#![doc(
    html_favicon_url = "https://structurely-images.s3-us-west-2.amazonaws.com/logos/rusty-celery.ico"
)]
#![doc(
    html_logo_url = "https://structurely-images.s3-us-west-2.amazonaws.com/logos/rusty-celery-4.png"
)]

/////////////////
// Re-exports. //
/////////////////

pub extern crate failure;
pub use failure::ResultExt;

/////////////////
// Submodules. //
/////////////////

// Defines the `Celery` app struct which the primary publich interface to Rusty Celery,
// used to produce or consume tasks.
mod app;

// The broker is an integral part of a `Celery` app. It provides the transport for
// producing and consuming tasks.
mod broker;

// Defines the `Error` type used across Rusty Celery.
mod error;

// Used only by the `celery-codegen` module.
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

pub use app::{Celery, CeleryBuilder};
pub use broker::{
    amqp::{AMQPBroker, AMQPBrokerBuilder},
    Broker,
};
#[cfg(feature = "codegen")]
pub use celery_codegen::task;
pub use error::{Error, ErrorKind};
pub use task::Task;

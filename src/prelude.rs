//! A "prelude" for users of the `celery` crate.

pub use crate::broker::AMQPBroker;
pub use crate::task::{Task, TaskResult, TaskResultExt};

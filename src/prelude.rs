//! A "prelude" for users of the `celery` crate.

#[cfg(feature = "broker_amqp")]
pub use crate::broker::AMQPBroker;
#[cfg(feature = "broker_redis")]
pub use crate::broker::RedisBroker;
pub use crate::error::*;
pub use crate::task::{Task, TaskResult, TaskResultExt};

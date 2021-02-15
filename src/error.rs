//! All error types used throughout the library.

use chrono::{DateTime, Utc};
use thiserror::Error;

/// Errors that can occur while creating or using a `Celery` app.
#[derive(Error, Debug)]
pub enum CeleryError {
    /// Raised when `Celery::consume_from` is given an empty array of queues.
    #[error("at least one queue required to consume from")]
    NoQueueToConsume,

    /// Forced shutdown.
    #[error("forced shutdown")]
    ForcedShutdown,

    /// Any other broker-level error that could happen when initializing or with an open
    /// connection.
    #[error("broker error")]
    BrokerError(#[from] BrokerError),

    /// Any other IO error that could occur.
    #[error("IO error")]
    IoError(#[from] std::io::Error),

    /// A protocol error.
    #[error("protocol error")]
    ProtocolError(#[from] ProtocolError),

    /// There is already a task registerd to this name.
    #[error("there is already a task registered as '{0}'")]
    TaskRegistrationError(String),

    #[error("received unregistered task {0}")]
    UnregisteredTaskError(String),
}

/// Errors that can occur while creating or using a `Beat` app.
#[derive(Error, Debug)]
pub enum BeatError {
    /// Any broker-level error.
    #[error("broker error")]
    BrokerError(#[from] BrokerError),

    /// A protocol error.
    #[error("protocol error")]
    ProtocolError(#[from] ProtocolError),

    /// An error with a task schedule.
    #[error("task schedule error")]
    ScheduleError(#[from] ScheduleError),
}

/// Errors that are related to task schedules.
#[derive(Error, Debug)]
pub enum ScheduleError {
    /// Error that can occur while creating a cron schedule.
    #[error("invalid cron schedule: {0}")]
    CronScheduleError(String),
}

/// Errors that can occur at the task level.
#[derive(Error, Debug)]
pub enum TaskError {
    /// An error that is expected to happen every once in a while.
    ///
    /// These errors will only be logged at the `WARN` level and will always trigger a task
    /// retry unless [`max_retries`](../task/struct.TaskOptions.html#structfield.max_retries)
    /// is set to 0 (or max retries is exceeded).
    ///
    /// A typical example is a task that makes an HTTP request to an external service.
    /// If that service is temporarily unavailable the task should raise an `ExpectedError`.
    ///
    /// Tasks are always retried with capped exponential backoff.
    #[error("task raised expected error: {0}")]
    ExpectedError(String),

    /// Should be used when a task encounters an error that is unexpected.
    ///
    /// These errors will always be logged at the `ERROR` level. The retry behavior
    /// when this error is encountered is determined by the
    /// [`TaskOptions::retry_for_unexpected`](../task/struct.TaskOptions.html#structfield.retry_for_unexpected)
    /// setting.
    #[error("task raised unexpected error: {0}")]
    UnexpectedError(String),

    /// Raised when a task runs over its time limit specified by the
    /// [`TaskOptions::time_limit`](../task/struct.TaskOptions.html#structfield.time_limit) setting.
    ///
    /// These errors are logged at the `ERROR` level but are otherwise treated like
    /// `ExpectedError`s in that they will trigger a retry when `max_retries` is anything but 0.
    ///
    /// Typically a task implementation doesn't need to return these errors directly
    /// because they will be raised automatically when the task runs over it's `time_limit`,
    /// provided the task yields control at some point (like with non-blocking IO).
    #[error("task timed out")]
    TimeoutError,

    /// A task can return this error variant to manually trigger a retry.
    ///
    /// This error variant should generally not be used directly. Instead, you should
    /// call the `Task::retry_with_countdown` or `Task::retry_with_eta` trait methods
    /// to manually trigger a retry from within a task.
    #[error("task retry triggered")]
    Retry(Option<DateTime<Utc>>),
}

/// Errors that can occur while tracing a task.
#[derive(Error, Debug)]
pub(crate) enum TraceError {
    /// Raised when a task throws an error while executing.
    #[error("task failed")]
    TaskError(TaskError),

    /// Raised when an expired task is received.
    #[error("task expired")]
    ExpirationError,

    /// Raised when a task should be retried.
    #[error("retrying task")]
    Retry(Option<DateTime<Utc>>),
}

/// Errors that can occur at the broker level.
#[derive(Error, Debug)]
pub enum BrokerError {
    /// Raised when a broker URL can't be parsed.
    #[error("invalid broker URL '{0}'")]
    InvalidBrokerUrl(String),

    /// The queue you're attempting to use has not been defined.
    #[error("unknown queue '{0}'")]
    UnknownQueue(String),

    /// Broker is disconnected.
    #[error("broker not connected")]
    NotConnected,

    /// Any IO error that could occur.
    #[error("IO error \"{0}\"")]
    IoError(#[from] std::io::Error),

    /// Deserilize error
    #[error("Deserialize error \"{0}\"")]
    DeserializeError(#[from] serde_json::Error),

    /// Routing pattern error
    #[error("Routing pattern error \"{0}\"")]
    BadRoutingPattern(#[from] BadRoutingPattern),

    /// Protocol error
    #[error("Protocol error \"{0}\"")]
    ProtocolError(#[from] ProtocolError),

    /// Any other AMQP error that could happen.
    #[error("AMQP error \"{0}\"")]
    AMQPError(#[from] lapin::Error),

    /// Any other Redis error that could happen.
    #[error("Redis error \"{0}\"")]
    RedisError(#[from] redis::RedisError),
}

impl BrokerError {
    pub fn is_connection_error(&self) -> bool {
        match self {
            BrokerError::IoError(_) | BrokerError::NotConnected => true,
            BrokerError::AMQPError(err) => matches!(
                err,
                lapin::Error::ProtocolError(_)
                    | lapin::Error::InvalidConnectionState(_)
                    | lapin::Error::InvalidChannelState(_)
            ),
            BrokerError::RedisError(err) => {
                err.is_connection_dropped() || err.is_connection_refusal()
            }
            _ => false,
        }
    }
}

/// Errors that can occur at the result backend level.
#[derive(Error, Debug)]
pub enum BackendError {
    /// Raised when a broker URL can't be parsed.
    #[error("invalid backend URL '{0}'")]
    InvalidBackendUrl(String),

    /// Broker is disconnected.
    #[error("backend not connected")]
    NotConnected,

    /// Any IO error that could occur.
    #[error("IO error \"{0}\"")]
    IoError(#[from] std::io::Error),

    /// Deserilize error
    #[error("Deserialize error \"{0}\"")]
    DeserializeError(#[from] serde_json::Error),

    /// Any other Redis error that could happen.
    #[error("Redis error \"{0}\"")]
    RedisError(#[from] redis::RedisError),
}

/// An invalid glob pattern for a routing rule.
#[derive(Error, Debug)]
#[error("invalid glob routing rule")]
pub struct BadRoutingPattern(#[from] globset::Error);

/// Errors that can occur due to messages not conforming to the protocol.
#[derive(Error, Debug)]
pub enum ProtocolError {
    /// Raised when a required message property is missing.
    #[error("missing required property '{0}'")]
    MissingRequiredProperty(String),

    /// Raised when the headers are missing altogether.
    #[error("missing headers")]
    MissingHeaders,

    /// Raised when a required message header is missing.
    #[error("missing required property '{0}'")]
    MissingRequiredHeader(String),

    /// Raised when serializing or de-serializing a message body fails.
    #[error("message body serialization error")]
    BodySerializationError(#[from] ContentTypeError),

    /// Raised when field value is invalid.
    #[error("invalid property '{0}'")]
    InvalidProperty(String),
}

impl From<serde_json::Error> for ProtocolError {
    fn from(err: serde_json::Error) -> Self {
        Self::from(ContentTypeError::from(err))
    }
}

#[cfg(any(test, feature = "extra_content_types"))]
impl From<serde_yaml::Error> for ProtocolError {
    fn from(err: serde_yaml::Error) -> Self {
        Self::from(ContentTypeError::from(err))
    }
}

#[cfg(any(test, feature = "extra_content_types"))]
impl From<serde_pickle::error::Error> for ProtocolError {
    fn from(err: serde_pickle::error::Error) -> Self {
        Self::from(ContentTypeError::from(err))
    }
}

#[cfg(any(test, feature = "extra_content_types"))]
impl From<rmp_serde::decode::Error> for ProtocolError {
    fn from(err: rmp_serde::decode::Error) -> Self {
        Self::from(ContentTypeError::from(err))
    }
}

#[cfg(any(test, feature = "extra_content_types"))]
impl From<rmp_serde::encode::Error> for ProtocolError {
    fn from(err: rmp_serde::encode::Error) -> Self {
        Self::from(ContentTypeError::from(err))
    }
}

#[cfg(any(test, feature = "extra_content_types"))]
impl From<rmpv::ext::Error> for ProtocolError {
    fn from(err: rmpv::ext::Error) -> Self {
        Self::from(ContentTypeError::from(err))
    }
}

#[derive(Error, Debug)]
pub enum ContentTypeError {
    #[error("JSON serialization error")]
    Json(#[from] serde_json::Error),

    #[cfg(any(test, feature = "extra_content_types"))]
    #[error("YAML serialization error")]
    Yaml(#[from] serde_yaml::Error),

    #[cfg(any(test, feature = "extra_content_types"))]
    #[error("Pickle serialization error")]
    Pickle(#[from] serde_pickle::error::Error),

    #[cfg(any(test, feature = "extra_content_types"))]
    #[error("MessagePack decoding error")]
    MsgPackDecode(#[from] rmp_serde::decode::Error),

    #[cfg(any(test, feature = "extra_content_types"))]
    #[error("MessagePack encoding error")]
    MsgPackEncode(#[from] rmp_serde::encode::Error),

    #[cfg(any(test, feature = "extra_content_types"))]
    #[error("MessagePack value error")]
    MsgPackValue(#[from] rmpv::ext::Error),

    #[error("Unknown content type error")]
    Unknown,
}

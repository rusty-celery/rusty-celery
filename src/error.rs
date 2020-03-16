//! Error types.

use chrono::{DateTime, Utc};
use failure::{Context, Fail};

/// Errors that can occur while creating or using a `Celery` app.
#[derive(Debug, Fail)]
pub enum CeleryError {
    /// The queue you're attempting to use has not been defined.
    #[fail(display = "Unknown queue '{}'", _0)]
    UnknownQueue(String),

    /// Raised when `Celery::consume_from` is given an empty array of queues.
    #[fail(display = "At least one queue required to consume from")]
    NoQueueToConsume,

    /// Forced shutdown.
    #[fail(display = "Forced shutdown")]
    ForcedShutdown,

    /// Any other broker-level error that could happen when initializing.
    #[fail(display = "{}", _0)]
    BrokerError(#[fail(cause)] BrokerError),

    /// Any other IO error that could occur.
    #[fail(display = "{}", _0)]
    IoError(#[fail(cause)] std::io::Error),

    /// A protocol error.
    #[fail(display = "Protocol error: {}", _0)]
    ProtocolError(#[fail(cause)] ProtocolError),

    /// An invalid glob pattern for a routing rule.
    #[fail(display = "Invalid glob routing rule '{}'", _0)]
    BadRoutingPattern(#[fail(cause)] globset::Error),

    /// There is already a task registerd to this name.
    #[fail(display = "There is already a task registered as '{}'", _0)]
    TaskRegistrationError(String),

    #[fail(display = "Received unregistered task {}", _0)]
    UnregisteredTaskError(String),
}

/// Errors that can occur at the task level.
#[derive(Debug, Fail)]
pub enum TaskError {
    /// An error that is expected to happen every once in a while.
    ///
    /// These errors will only be logged at the `WARN` level and will always trigger a task
    /// retry unless `max_retries` is set to `Some(0)` (or max retries is exceeded).
    ///
    /// A typical example is a task that makes an HTTP request to an external service.
    /// If that service is temporarily unavailable, the task should raise an `ExpectedError`.
    ///
    /// Tasks are always retried with capped exponential backoff.
    #[fail(display = "{}", _0)]
    ExpectedError(String),

    /// Should be used when a task encounters an error that is unexpected.
    ///
    /// These errors will always be logged at the `ERROR` level. The retry behavior
    /// when this error is encountered is determined by the
    /// [`Task::retry_for_unexpected`](trait.Task.html#method.retry_for_unexpected)
    /// setting.
    #[fail(display = "{}", _0)]
    UnexpectedError(String),

    /// Raised when a task runs over its time limit specified by the [`Task::timeout`](trait.Task.html#method.timeout)
    /// setting.
    ///
    /// These errors are logged at the `ERROR` level but are otherwise treated like
    /// `ExpectedError`s in that they will trigger a retry when `max_retries` is anything but
    /// `Some(0)`.
    ///
    /// Typically a task implementation doesn't need to return these errors directly
    /// because they will be raised automatically when the task runs over it's `timeout`,
    /// provided the task yields control at some point (like with non-blocking IO).
    #[fail(display = "Task timed out")]
    TimeoutError,
}

/// Errors that can occur while tracing a task.
#[derive(Debug, Fail)]
pub(crate) enum TraceError {
    /// Raised when a task throws an error while executing.
    #[fail(display = "Task failed with {}", _0)]
    TaskError(TaskError),

    /// Raised when an expired task is received.
    #[fail(display = "Task expired")]
    ExpirationError,

    /// Raised when a task should be retried.
    #[fail(display = "Retrying task")]
    Retry(Option<DateTime<Utc>>),
}

/// Errors that can occur at the broker level.
#[derive(Debug, Fail)]
pub enum BrokerError {
    /// Raised when a broker URL can't be parsed.
    #[fail(display = "Invalid broker URL: {}", _0)]
    InvalidBrokerUrl(String),

    /// The queue you're attempting to use has not been defined.
    #[fail(display = "Unknown queue '{}'", _0)]
    UnknownQueue(String),

    /// Broker is disconnected.
    #[fail(display = "Broker not connected")]
    NotConnected,

    /// Connection request timed-out.
    #[fail(display = "Connection request timed-out.")]
    ConnectTimeout,

    /// Broker connection refused.
    #[fail(display = "Broker connection refused")]
    ConnectionRefused,

    /// Broker IO error.
    #[fail(display = "Broker IO error")]
    IoError,

    /// Any other AMQP error that could happen.
    #[fail(display = "{}", _0)]
    AMQPError(#[fail(cause)] lapin::Error),
}

/// Errors that can occur due to messages not conforming to the protocol.
#[derive(Debug, Fail)]
pub enum ProtocolError {
    /// Raised when a required message property is missing.
    #[fail(display = "Missing required property '{}'", _0)]
    MissingRequiredProperty(String),

    /// Raised when the headers are missing altogether.
    #[fail(display = "Missing headers")]
    MissingHeaders,

    /// Raised when a required message header is missing.
    #[fail(display = "Missing required property '{}'", _0)]
    MissingRequiredHeader(String),

    /// Raised when serializing or de-serializing a message body fails.
    #[fail(display = "{}", _0)]
    BodySerializationError(#[fail(cause)] serde_json::Error),
}

impl From<BrokerError> for CeleryError {
    fn from(err: BrokerError) -> Self {
        Self::BrokerError(err)
    }
}

impl From<ProtocolError> for CeleryError {
    fn from(err: ProtocolError) -> Self {
        Self::ProtocolError(err)
    }
}

impl From<std::io::Error> for CeleryError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl From<globset::Error> for CeleryError {
    fn from(err: globset::Error) -> Self {
        Self::BadRoutingPattern(err)
    }
}

impl From<serde_json::Error> for ProtocolError {
    fn from(err: serde_json::Error) -> Self {
        Self::BodySerializationError(err)
    }
}

impl From<lapin::Error> for BrokerError {
    fn from(err: lapin::Error) -> Self {
        match err {
            lapin::Error::NotConnected => BrokerError::NotConnected,
            lapin::Error::IOError(_) => BrokerError::IoError,
            lapin::Error::ConnectionRefused => BrokerError::ConnectionRefused,
            _ => BrokerError::AMQPError(err),
        }
    }
}

impl From<Context<&str>> for TaskError {
    fn from(ctx: Context<&str>) -> Self {
        Self::UnexpectedError((*ctx.get_context()).into())
    }
}

/// Extension methods for `Result` types within a task body.
///
/// These methods can be used to convert a `Result<T, E>` to a `Result<T, TaskError>` with the
/// appropriate `TaskError` variant. The trait has a blanket implementation for any error type that implements
/// [`std::error::Error`](https://doc.rust-lang.org/std/error/trait.Error.html) or
/// [`failure::Fail`](https://docs.rs/failure/0.1.6/failure/trait.Fail.html).
pub trait TaskResultExt<T, E> {
    /// Convert the error type to a `TaskError::ExpectedError`.
    fn with_expected_err(self, context: &str) -> Result<T, TaskError>;

    /// Convert the error type to a `TaskError::UnexpectedError`.
    fn with_unexpected_err(self, context: &str) -> Result<T, TaskError>;
}

impl<T, E> TaskResultExt<T, E> for Result<T, E>
where
    E: Fail,
{
    fn with_expected_err(self, context: &str) -> Result<T, TaskError> {
        self.map_err(|_failure| TaskError::ExpectedError(context.into()))
    }

    fn with_unexpected_err(self, context: &str) -> Result<T, TaskError> {
        self.map_err(|_failure| TaskError::UnexpectedError(context.into()))
    }
}

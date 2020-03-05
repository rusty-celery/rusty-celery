//! Error types.

use chrono::{DateTime, Utc};
use failure::{Context, Fail};

/// Errors that can occur while creating or using a `Celery` app.
#[derive(Debug, Fail)]
pub enum CeleryError {
    /// The queue you're attempting to use has not been defined.
    #[fail(display = "Unknown queue '{}'", _0)]
    UnknownQueue(String),

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
    /// An error that is expected to happen every once in a while and should trigger
    /// the task to be retried without causes a fit.
    #[fail(display = "{}", _0)]
    ExpectedError(String),

    /// Should be used when a task encounters an error that is unexpected.
    #[fail(display = "{}", _0)]
    UnexpectedError(String),

    /// Raised when a task runs over its time limit.
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

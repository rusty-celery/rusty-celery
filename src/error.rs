//! Error types.

use chrono::{DateTime, Utc};
use failure::{Context, Fail};

/// Errors that can occur while creating or using a `Celery` app.
#[derive(Debug, Fail)]
pub enum CeleryError {
    /// The queue you're attempting to use has not been defined.
    #[fail(display = "CeleryError: unknown queue '{}'", _0)]
    UnknownQueue(String),

    /// Raised when `Celery::consume_from` is given an empty array of queues.
    #[fail(display = "CeleryError: at least one queue required to consume from")]
    NoQueueToConsume,

    /// Forced shutdown.
    #[fail(display = "CeleryError: forced shutdown")]
    ForcedShutdown,

    /// Any other broker-level error that could happen when initializing or with an open
    /// connection.
    #[fail(display = "CeleryError: broker error ({})", _0)]
    BrokerError(#[fail(cause)] BrokerError),

    /// Any other IO error that could occur.
    #[fail(display = "CeleryError: IO error ({})", _0)]
    IoError(#[fail(cause)] std::io::Error),

    /// A protocol error.
    #[fail(display = "CeleryError: protocol error ({})", _0)]
    ProtocolError(#[fail(cause)] ProtocolError),

    /// An invalid glob pattern for a routing rule.
    #[fail(display = "CeleryError: invalid glob routing rule '{}'", _0)]
    BadRoutingPattern(#[fail(cause)] globset::Error),

    /// There is already a task registerd to this name.
    #[fail(
        display = "CeleryError: there is already a task registered as '{}'",
        _0
    )]
    TaskRegistrationError(String),

    #[fail(display = "CeleryError: received unregistered task {}", _0)]
    UnregisteredTaskError(String),
}

/// Errors that can occur while creating or using a `Beat` app.
#[derive(Debug, Fail)]
pub enum BeatError {
    /// Any broker-level error.
    #[fail(display = "BeatError: {}. Cause: {}", _0, _1)]
    BrokerError(String, #[fail(cause)] BrokerError),

    /// A protocol error.
    #[fail(display = "BeatError: {}. Cause: {}", _0, _1)]
    ProtocolError(String, #[fail(cause)] ProtocolError),
}

/// Errors that can occur at the task level.
#[derive(Debug, Fail)]
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
    #[fail(display = "TaskError (expected): {}", _0)]
    ExpectedError(String),

    /// Should be used when a task encounters an error that is unexpected.
    ///
    /// These errors will always be logged at the `ERROR` level. The retry behavior
    /// when this error is encountered is determined by the
    /// [`TaskOptions::retry_for_unexpected`](../task/struct.TaskOptions.html#structfield.retry_for_unexpected)
    /// setting.
    #[fail(display = "TaskError: {}", _0)]
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
    #[fail(display = "TaskError: task timed out")]
    TimeoutError,

    /// A task can return this error variant to manually trigger a retry.
    ///
    /// This error variant should generally not be used directly. Instead, you should
    /// call the `Task::retry_with_countdown` or `Task::retry_with_eta` trait methods
    /// to manually trigger a retry from within a task.
    #[fail(display = "TaskError: retry triggered")]
    Retry(Option<DateTime<Utc>>),
}

/// Errors that can occur while tracing a task.
#[derive(Debug, Fail)]
pub(crate) enum TraceError {
    /// Raised when a task throws an error while executing.
    #[fail(display = "TraceError: task failed with {}", _0)]
    TaskError(TaskError),

    /// Raised when an expired task is received.
    #[fail(display = "TraceError: task expired")]
    ExpirationError,

    /// Raised when a task should be retried.
    #[fail(display = "retrying task")]
    Retry(Option<DateTime<Utc>>),
}

/// Errors that can occur at the broker level.
#[derive(Debug, Fail)]
pub enum BrokerError {
    /// Raised when a broker URL can't be parsed.
    #[fail(display = "BrokerError: invalid broker URL '{}'", _0)]
    InvalidBrokerUrl(String),

    /// The queue you're attempting to use has not been defined.
    #[fail(display = "BrokerError: unknown queue '{}'", _0)]
    UnknownQueue(String),

    /// Broker is disconnected.
    #[fail(display = "BrokerError: broker not connected")]
    NotConnected,

    /// Any IO error that could occur.
    #[fail(display = "BrokerError: IO error ({})", _0)]
    IoError(#[fail(cause)] std::io::Error),

    /// Any other AMQP error that could happen.
    #[fail(display = "BrokerError: AMQP error ({})", _0)]
    AMQPError(#[fail(cause)] lapin::Error),
}

impl BrokerError {
    pub fn is_connection_error(&self) -> bool {
        match self {
            BrokerError::IoError(_) | BrokerError::NotConnected => true,
            BrokerError::AMQPError(err) => match err {
                lapin::Error::ProtocolError(_) => true,
                lapin::Error::InvalidConnectionState(_) => true,
                lapin::Error::InvalidChannelState(_) => true,
                _ => false,
            },
            _ => false,
        }
    }
}

/// Errors that can occur due to messages not conforming to the protocol.
#[derive(Debug, Fail)]
pub enum ProtocolError {
    /// Raised when a required message property is missing.
    #[fail(display = "ProtocolError: missing required property '{}'", _0)]
    MissingRequiredProperty(String),

    /// Raised when the headers are missing altogether.
    #[fail(display = "ProtocolError: missing headers")]
    MissingHeaders,

    /// Raised when a required message header is missing.
    #[fail(display = "ProtocolError: missing required property '{}'", _0)]
    MissingRequiredHeader(String),

    /// Raised when serializing or de-serializing a message body fails.
    #[fail(display = "ProtocolError: serialization error ({})", _0)]
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
            lapin::Error::IOError(e) => BrokerError::IoError(std::io::Error::new(
                (*e).kind(),
                format!("{} from AMQP broker", *e),
            )),
            lapin::Error::InvalidConnectionState(lapin::ConnectionState::Closing)
            | lapin::Error::InvalidConnectionState(lapin::ConnectionState::Closed)
            | lapin::Error::InvalidConnectionState(lapin::ConnectionState::Error)
            | lapin::Error::InvalidChannelState(lapin::ChannelState::Closing)
            | lapin::Error::InvalidChannelState(lapin::ChannelState::Closed)
            | lapin::Error::InvalidChannelState(lapin::ChannelState::Error) => {
                BrokerError::NotConnected
            }
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

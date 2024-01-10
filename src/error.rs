//! All error types used throughout the library.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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

    /// Any other backend-level error that could happen when initializing or with an open
    /// connection.
    #[error("backend error")]
    BackendError(#[from] BackendError),

    /// Any other IO error that could occur.
    #[error("IO error")]
    IoError(#[from] std::io::Error),

    /// A protocol error.
    #[error("protocol error")]
    ProtocolError(#[from] ProtocolError),

    /// Task error.
    #[error("task error")]
    TaskError(#[from] TaskError),

    /// There is already a task registered to this name.
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskError {
    /// Distinguish the error in Rust
    #[serde(default)]
    pub kind: TaskErrorType,
    /// The Python exception type. For errors generated in Rust this will
    /// default to `Exception`.
    pub exc_type: String,
    /// The module in which the exception type can be found. Will be `builtins` for
    /// errors raised in Rust.
    pub exc_module: String,
    /// Error message
    pub exc_message: TaskErrorMessage,
    pub exc_cause: Option<String>,
    pub exc_traceback: Option<String>,
}

impl std::error::Error for TaskError {}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let TaskError {
            kind,
            exc_type,
            exc_message,
            exc_module,
            exc_cause: _,
            exc_traceback,
        } = self;

        match kind {
            TaskErrorType::Expected => writeln!(f, "expected error:")?,
            TaskErrorType::Unexpected => writeln!(f, "unexpected error:")?,
            TaskErrorType::MaxRetriesExceeded => writeln!(f, "max retries exceeded:")?,
            TaskErrorType::Other => writeln!(f, "{exc_type}:")?,
            _ => {}
        }

        if !exc_module.is_empty() && exc_module != "rust" {
            writeln!(f, "in module {exc_module}")?;
        }

        exc_message.print(f, 0)?;

        if let Some(trace) = exc_traceback {
            writeln!(f, "exc traceback: {trace}")?;
        }
        Ok(())
    }
}

impl TaskError {
    pub fn expected(msg: impl ToString) -> Self {
        TaskError {
            kind: TaskErrorType::Expected,
            exc_type: "Exception".to_string(),
            exc_module: "builtins".to_string(),
            exc_message: TaskErrorMessage::Text(msg.to_string()),
            exc_cause: None,
            exc_traceback: None,
        }
    }

    pub fn unexpected(msg: impl ToString) -> Self {
        TaskError {
            kind: TaskErrorType::Unexpected,
            exc_type: "Exception".to_string(),
            exc_module: "builtins".to_string(),
            exc_message: TaskErrorMessage::Text(msg.to_string()),
            exc_cause: None,
            exc_traceback: None,
        }
    }

    pub fn timeout() -> Self {
        TaskError {
            kind: TaskErrorType::Timeout,
            exc_type: "Exception".to_string(),
            exc_module: "builtins".to_string(),
            exc_message: TaskErrorMessage::Text("task timed out".to_string()),
            exc_cause: None,
            exc_traceback: None,
        }
    }

    pub fn retry(eta: Option<DateTime<Utc>>) -> Self {
        TaskError {
            kind: TaskErrorType::Retry(eta),
            exc_type: "Exception".to_string(),
            exc_module: "builtins".to_string(),
            exc_message: TaskErrorMessage::Text("task retry triggered".to_string()),
            exc_cause: None,
            exc_traceback: None,
        }
    }

    pub fn max_retries_exceeded(name: &str, id: &str) -> Self {
        TaskError {
            kind: TaskErrorType::MaxRetriesExceeded,
            exc_type: "MaxRetriesExceededError".to_string(),
            exc_module: "celery.exceptions".to_string(),
            exc_message: TaskErrorMessage::Text(format!("Can't retry {name}[{id}]")),
            exc_cause: None,
            exc_traceback: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskErrorType {
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
    Expected,
    /// Should be used when a task encounters an error that is unexpected.
    ///
    /// These errors will always be logged at the `ERROR` level. The retry behavior
    /// when this error is encountered is determined by the
    /// [`TaskOptions::retry_for_unexpected`](../task/struct.TaskOptions.html#structfield.retry_for_unexpected)
    /// setting.
    Unexpected,

    /// Raised when a task runs over its time limit specified by the
    /// [`TaskOptions::time_limit`](../task/struct.TaskOptions.html#structfield.time_limit) setting.
    ///
    /// These errors are logged at the `ERROR` level but are otherwise treated like
    /// `ExpectedError`s in that they will trigger a retry when `max_retries` is anything but 0.
    ///
    /// Typically a task implementation doesn't need to return these errors directly
    /// because they will be raised automatically when the task runs over it's `time_limit`,
    /// provided the task yields control at some point (like with non-blocking IO).
    Timeout,

    /// A task can return this error variant to manually trigger a retry.
    ///
    /// This error variant should generally not be used directly. Instead, you should
    /// call the `Task::retry_with_countdown` or `Task::retry_with_eta` trait methods
    /// to manually trigger a retry from within a task.
    Retry(Option<DateTime<Utc>>),

    MaxRetriesExceeded,

    #[default]
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TaskErrorMessage {
    Text(String),
    List(Vec<TaskErrorMessage>),
    Other(serde_json::Value),
}

impl std::fmt::Display for TaskErrorMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.print(f, 0)
    }
}

impl TaskErrorMessage {
    fn print(&self, f: &mut std::fmt::Formatter<'_>, indent: usize) -> std::fmt::Result {
        match self {
            TaskErrorMessage::Text(it) => {
                writeln!(f, "{}{it}", " ".repeat(indent))?;
            }
            TaskErrorMessage::List(it) => {
                for item in it {
                    item.print(f, indent + 2)?;
                }
            }
            TaskErrorMessage::Other(it) => {
                writeln!(f, "{}{it}", " ".repeat(indent))?;
            }
        }
        Ok(())
    }
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

impl TraceError {
    pub(crate) fn into_task_error(self) -> TaskError {
        match self {
            TraceError::TaskError(e) => e,
            TraceError::ExpirationError => TaskError::timeout(),
            TraceError::Retry(_eta) => unreachable!("retry should not be returned as error"),
        }
    }
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

    /// Deserialize error
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

/// Errors that can occur at the broker level.
#[derive(Error, Debug)]
pub enum BackendError {
    #[error("invalid broker URL '{0}'")]
    InvalidBrokerUrl(String),

    /// Any other Redis error that could happen.
    #[error("Redis error \"{0}\"")]
    RedisError(#[from] ::redis::RedisError),

    #[error("JSON serialization error")]
    Json(#[from] serde_json::Error),
}

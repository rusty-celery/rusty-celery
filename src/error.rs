use std::fmt;

use failure::{Backtrace, Context, Fail};

/// Any error that can occur while using `celery`.
#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>,
}

/// Error kinds that can occur while using `celery`.
#[derive(Debug, Fail)]
pub enum ErrorKind {
    #[fail(display = "Task named '{}' already exists", _0)]
    TaskAlreadyExists(String),

    #[fail(display = "{}", _0)]
    BrokerError(lapin::Error),

    #[fail(display = "{}", _0)]
    SerializationError(serde_json::Error),

    #[fail(display = "Broker not specified")]
    BrokerNotSpecified,
}

impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl Error {
    /// Get the inner `ErrorKind`.
    pub fn kind(&self) -> &ErrorKind {
        self.inner.get_context()
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<ErrorKind>> for Error {
    fn from(inner: Context<ErrorKind>) -> Error {
        Error { inner }
    }
}

impl From<lapin::Error> for Error {
    fn from(err: lapin::Error) -> Error {
        Error {
            inner: Context::new(ErrorKind::BrokerError(err)),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Error {
        Error {
            inner: Context::new(ErrorKind::SerializationError(err)),
        }
    }
}

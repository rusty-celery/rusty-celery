//! Provides the [`Task`] trait as well as options for configuring tasks.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use rand::distributions::{Distribution, Uniform};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::TaskError;

mod async_result;
mod options;
mod request;
mod signature;

pub use async_result::AsyncResult;
pub use options::TaskOptions;
pub use request::Request;
pub use signature::Signature;

/// The return type for a task.
pub type TaskResult<R> = Result<R, TaskError>;

#[doc(hidden)]
pub trait AsTaskResult {
    type Returns: Send + Sync + std::fmt::Debug;
}

impl<R> AsTaskResult for TaskResult<R>
where
    R: Send + Sync + std::fmt::Debug,
{
    type Returns = R;
}

pub trait TaskReturns: std::fmt::Debug + Send + Sync + 'static {
    fn to_json(&self) -> serde_json::Result<serde_json::Value>;

    fn from_json(json: serde_json::Value) -> serde_json::Result<Self>
    where
        Self: Sized;
}

impl<T> TaskReturns for T
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
{
    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn from_json(json: serde_json::Value) -> serde_json::Result<Self> {
        serde_json::from_value(json)
    }
}

/// A `Task` represents a unit of work that a `Celery` app can produce or consume.
///
/// The recommended way to create tasks is through the [`task`](../attr.task.html) attribute macro, not by directly implementing
/// this trait. For more information see the [tasks chapter](https://rusty-celery.github.io/guide/defining-tasks.html)
/// in the Rusty Celery Book.
#[async_trait]
pub trait Task: Send + Sync + std::marker::Sized {
    /// The name of the task. When a task is registered it will be registered with this name.
    const NAME: &'static str;

    /// For compatability with Python tasks. This keeps track of the order
    /// of arguments for the task so that the task can be called from Python with
    /// positional arguments.
    const ARGS: &'static [&'static str];

    /// Default task options.
    const DEFAULTS: TaskOptions = TaskOptions {
        time_limit: None,
        hard_time_limit: None,
        expires: None,
        max_retries: None,
        min_retry_delay: None,
        max_retry_delay: None,
        retry_for_unexpected: None,
        acks_late: None,
        content_type: None,
    };

    /// The parameters of the task.
    type Params: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>;

    /// The return type of the task.
    type Returns: TaskReturns;

    /// Used to initialize a task instance from a request.
    fn from_request(request: Request<Self>, options: TaskOptions) -> Self;

    /// Get a reference to the request used to create this task instance.
    fn request(&self) -> &Request<Self>;

    /// Get a reference to the task's configuration options.
    ///
    /// This is a product of both app-level task options and the options configured specifically
    /// for the given task. Options specified at the *task*-level take priority over options
    /// specified at the app level. So, if the task was defined like this:
    ///
    /// ```rust
    /// # use celery::prelude::*;
    /// #[celery::task(time_limit = 3)]
    /// fn add(x: i32, y: i32) -> TaskResult<i32> {
    ///     Ok(x + y)
    /// }
    /// ```
    ///
    /// But the `Celery` app was built with a `task_time_limit` of 5, then
    /// `Task::options().time_limit` would be `Some(3)`.
    fn options(&self) -> &TaskOptions;

    /// This function defines how a task executes.
    async fn run(&self, params: Self::Params) -> TaskResult<Self::Returns>;

    /// Callback that will run after a task fails.
    #[allow(unused_variables)]
    async fn on_failure(&self, err: &TaskError) {}

    /// Callback that will run after a task completes successfully.
    #[allow(unused_variables)]
    async fn on_success(&self, returned: &Self::Returns) {}

    /// Returns the registered name of the task.
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// This can be called from within a task function to trigger a retry in `countdown` seconds.
    fn retry_with_countdown(&self, countdown: u32) -> TaskResult<Self::Returns> {
        let eta = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(now) => {
                let now_secs = now.as_secs() as u32;
                let now_millis = now.subsec_millis();
                let eta_secs = now_secs + countdown;
                Some(DateTime::<Utc>::from_naive_utc_and_offset(
                    NaiveDateTime::from_timestamp_opt(eta_secs as i64, now_millis * 1000)
                        .ok_or_else(|| {
                            TaskError::unexpected(format!("Invalid countdown seconds {countdown}",))
                        })?,
                    Utc,
                ))
            }
            Err(_) => None,
        };
        Err(TaskError::retry(eta))
    }

    /// This can be called from within a task function to trigger a retry at the specified `eta`.
    fn retry_with_eta(&self, eta: DateTime<Utc>) -> TaskResult<Self::Returns> {
        Err(TaskError::retry(Some(eta)))
    }

    /// Get a future ETA at which time the task should be retried. By default this
    /// uses a capped exponential backoff strategy.
    fn retry_eta(&self) -> Option<DateTime<Utc>> {
        let retries = self.request().retries;
        let delay_secs = std::cmp::min(
            2u32.checked_pow(retries)
                .unwrap_or_else(|| self.max_retry_delay()),
            self.max_retry_delay(),
        );
        let delay_secs = std::cmp::max(delay_secs, self.min_retry_delay());
        let between = Uniform::from(0..1000);
        let mut rng = rand::thread_rng();
        let delay_millis = between.sample(&mut rng);
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(now) => {
                let now_secs = now.as_secs() as u32;
                let now_millis = now.subsec_millis();
                let eta_secs = now_secs + delay_secs;
                let eta_millis = now_millis + delay_millis;
                NaiveDateTime::from_timestamp_opt(eta_secs as i64, eta_millis * 1000)
                    .map(|eta| DateTime::<Utc>::from_naive_utc_and_offset(eta, Utc))
            }
            Err(_) => None,
        }
    }

    fn retry_for_unexpected(&self) -> bool {
        Self::DEFAULTS
            .retry_for_unexpected
            .or(self.options().retry_for_unexpected)
            .unwrap_or(true)
    }

    fn time_limit(&self) -> Option<u32> {
        self.request().time_limit.or_else(|| {
            // Take min or `time_limit` and `hard_time_limit`.
            let time_limit = Self::DEFAULTS.time_limit.or(self.options().time_limit);
            let hard_time_limit = Self::DEFAULTS
                .hard_time_limit
                .or(self.options().hard_time_limit);
            match (time_limit, hard_time_limit) {
                (Some(t1), Some(t2)) => Some(std::cmp::min(t1, t2)),
                (Some(t1), None) => Some(t1),
                (None, Some(t2)) => Some(t2),
                _ => None,
            }
        })
    }

    fn max_retries(&self) -> Option<u32> {
        Self::DEFAULTS.max_retries.or(self.options().max_retries)
    }

    fn min_retry_delay(&self) -> u32 {
        Self::DEFAULTS
            .min_retry_delay
            .or(self.options().min_retry_delay)
            .unwrap_or(0)
    }

    fn max_retry_delay(&self) -> u32 {
        Self::DEFAULTS
            .max_retry_delay
            .or(self.options().max_retry_delay)
            .unwrap_or(3600)
    }

    fn acks_late(&self) -> bool {
        Self::DEFAULTS
            .acks_late
            .or(self.options().acks_late)
            .unwrap_or(false)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum TaskEvent {
    StatusChange(TaskStatus),
}

#[derive(Clone, Debug)]
pub(crate) enum TaskStatus {
    Pending,
    Finished,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum TaskState {
    /// The task is waiting for execution.
    Pending,
    /// The task has been started.
    Started,
    /// The task is to be retried, possibly because of failure.
    Retry,
    /// The task failed with error, or has exceeded the retry limit.
    Failure,
    /// The task executed successfully.
    Success,
    /// Task was received by a worker (only used in events).
    Received,
    /// Task was revoked.
    Revoked,
}

/// Extension methods for `Result` types within a task body.
///
/// These methods can be used to convert a `Result<T, E>` to a `Result<T, TaskError>` with the
/// appropriate [`TaskError`] variant. The trait has a blanket implementation for any error type that implements
/// [`std::error::Error`](https://doc.rust-lang.org/std/error/trait.Error.html).
///
/// # Examples
///
/// ```rust
/// # use celery::prelude::*;
/// fn do_some_io() -> Result<(), std::io::Error> {
///     unimplemented!()
/// }
///
/// #[celery::task]
/// fn fallible_io_task() -> TaskResult<()> {
///     do_some_io().with_expected_err(|| "IO error")?;
///     Ok(())
/// }
/// ```
pub trait TaskResultExt<T, E, F, C> {
    /// Convert the error type to a [`TaskError::ExpectedError`].
    fn with_expected_err(self, f: F) -> Result<T, TaskError>;

    /// Convert the error type to a [`TaskError::UnexpectedError`].
    fn with_unexpected_err(self, f: F) -> Result<T, TaskError>;
}

impl<T, E, F, C> TaskResultExt<T, E, F, C> for Result<T, E>
where
    E: std::error::Error,
    F: FnOnce() -> C,
    C: std::fmt::Display + Send + Sync + 'static,
{
    fn with_expected_err(self, f: F) -> Result<T, TaskError> {
        self.map_err(|e| TaskError::expected(format!("{} ➥ Cause: {:?}", f(), e)))
    }

    fn with_unexpected_err(self, f: F) -> Result<T, TaskError> {
        self.map_err(|e| TaskError::unexpected(format!("{} ➥ Cause: {:?}", f(), e)))
    }
}

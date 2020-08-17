use super::Task;
use chrono::{DateTime, Utc};

/// Wraps the parameters and execution options for a single task invocation.
///
/// When you define a task through the [attribute macro](../attr.task.html), calling
/// `T::new(...)` with the arguments that your task function take will create a `Signature<T>`.
///
/// # Examples
///
/// ```rust
/// use celery::TaskResult;
///
/// #[celery::task]
/// fn add(x: i32, y: i32) -> TaskResult<i32> {
///     Ok(x + y)
/// }
///
/// let signature = add::new(1, 2);
/// assert_eq!(signature.params.x, 1);
/// assert_eq!(signature.params.y, 2);
/// ```
#[derive(Clone)]
pub struct Signature<T>
where
    T: Task,
{
    /// The parameters for the task invocation.
    pub params: T::Params,

    /// A queue to send the task to.
    pub queue: Option<String>,

    /// Time limit, in seconds, for the task execution. Overrides any app or task-level default time limits.
    pub time_limit: Option<u32>,

    /// The `time_limit` option is equivalent to the ["soft time
    /// limit"](https://docs.celeryproject.org/en/stable/userguide/workers.html#time-limits)
    /// option when sending tasks to a Python consumer.
    /// If you desire to set a "hard time limit", use this option.
    ///
    /// *Note that this is really only for compatability with Python workers*.
    /// `time_limit` and `hard_time_limit` are treated the same by Rust workers, and if both
    /// are set, the minimum of the two will be used.
    pub hard_time_limit: Option<u32>,

    /// The number of seconds to wait before executing the task. This is equivalent to setting
    /// [`eta`](struct.Signature.html#structfield.eta)
    /// to `current_time + countdown`.
    pub countdown: Option<u32>,

    /// A future ETA at which to execute the task.
    pub eta: Option<DateTime<Utc>>,

    /// A number of seconds until the task expires, at which point it should no longer
    /// be executed. This is equivalent to setting
    /// [`expires`](struct.Signature.html#structfield.expires)
    /// to `current_time + expires_in`.
    pub expires_in: Option<u32>,

    /// A future time at which the task will expire.
    pub expires: Option<DateTime<Utc>>,
}

impl<T> Signature<T>
where
    T: Task,
{
    /// Create a new `Signature` from task parameters.
    pub fn new(params: T::Params) -> Self {
        Self {
            params,
            queue: None,
            time_limit: None,
            hard_time_limit: None,
            countdown: None,
            eta: None,
            expires_in: None,
            expires: None,
        }
    }

    /// Get the name of the task.
    pub fn task_name() -> &'static str {
        T::NAME
    }

    /// Set the queue.
    pub fn with_queue(mut self, queue: &str) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Set a time limit (in seconds) for the task.
    pub fn with_time_limit(mut self, time_limit: u32) -> Self {
        self.time_limit = Some(time_limit);
        self
    }

    /// Set a hard time limit (in seconds) for the task.
    ///
    /// *Note that this is really only for compatability with Python workers*.
    /// `time_limit` and `hard_time_limit` are treated the same by Rust workers, and if both
    /// are set, the minimum of the two will be used.
    pub fn with_hard_time_limit(mut self, time_limit: u32) -> Self {
        self.hard_time_limit = Some(time_limit);
        self
    }

    /// Set the countdown.
    pub fn with_countdown(mut self, countdown: u32) -> Self {
        self.countdown = Some(countdown);
        self
    }

    /// Set the ETA.
    pub fn with_eta(mut self, eta: DateTime<Utc>) -> Self {
        self.eta = Some(eta);
        self
    }

    /// Set the number of seconds until the task expires.
    pub fn with_expires_in(mut self, expires_in: u32) -> Self {
        self.expires_in = Some(expires_in);
        self
    }

    /// Set the expiration time.
    pub fn with_expires(mut self, expires: DateTime<Utc>) -> Self {
        self.expires = Some(expires);
        self
    }
}

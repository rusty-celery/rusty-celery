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
/// #[celery::task]
/// fn add(x: i32, y: i32) -> i32 {
///     x + y
/// }
///
/// let signature = add::new(1, 2);
/// assert_eq!(add.params.x, 1);
/// assert_eq!(add.params.y, 2);
/// ```
pub struct Signature<T>
where
    T: Task,
{
    /// The parameters for the task invocation.
    pub params: T::Params,

    /// A queue to send the task to.
    pub queue: Option<String>,

    /// Timeout for the task execution. Overrides any app or task-level default timeouts.
    pub timeout: Option<u32>,

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
            timeout: None,
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

    /// Set the timeout.
    pub fn with_timeout(mut self, timeout: u32) -> Self {
        self.timeout = Some(timeout);
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

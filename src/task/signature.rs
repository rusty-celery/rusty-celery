use super::{Task, TaskOptions};
use crate::protocol::MessageContentType;
use chrono::{DateTime, Utc};

/// Wraps the parameters and execution options for a single task invocation.
///
/// When you define a task through the [`task`](macro@crate::task) attribute macro, calling
/// `T::new(...)` with the arguments that your task function take will create a
/// [`Signature<T>`](Signature).
///
/// # Examples
///
/// ```rust
/// use celery::prelude::*;
///
/// #[celery::task]
/// fn add(x: i32, y: i32) -> TaskResult<i32> {
///     Ok(x + y)
/// }
///
/// let signature = add::new(1, 2);
/// ```
#[derive(Clone)]
pub struct Signature<T>
where
    T: Task + 'static,
{
    /// The parameters for the task invocation.
    pub(crate) params: T::Params,

    /// A queue to send the task to.
    pub(crate) queue: Option<String>,

    /// The number of seconds to wait before executing the task. This is equivalent to setting
    /// [`eta`](struct.Signature.html#structfield.eta)
    /// to `current_time + countdown`.
    pub(crate) countdown: Option<u32>,

    /// A future ETA at which to execute the task.
    pub(crate) eta: Option<DateTime<Utc>>,

    /// A number of seconds until the task expires, at which point it should no longer
    /// be executed. This is equivalent to setting
    /// [`expires`](struct.Signature.html#structfield.expires)
    /// to `current_time + expires_in`.
    pub(crate) expires_in: Option<u32>,

    /// A future time at which the task will expire.
    pub(crate) expires: Option<DateTime<Utc>>,

    /// Additional options.
    pub(crate) options: TaskOptions,
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
            countdown: None,
            eta: None,
            expires_in: None,
            expires: None,
            options: T::DEFAULTS,
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

    /// Set the content type serialization format for the message body.
    pub fn with_content_type(mut self, content_type: MessageContentType) -> Self {
        self.options.content_type = Some(content_type);
        self
    }

    /// Set a time limit (in seconds) for the task.
    pub fn with_time_limit(mut self, time_limit: u32) -> Self {
        self.options.time_limit = Some(time_limit);
        self
    }

    /// Set a hard time limit (in seconds) for the task.
    ///
    /// *Note that this is really only for compatability with Python workers*.
    /// `time_limit` and `hard_time_limit` are treated the same by Rust workers, and if both
    /// are set, the minimum of the two will be used.
    pub fn with_hard_time_limit(mut self, time_limit: u32) -> Self {
        self.options.hard_time_limit = Some(time_limit);
        self
    }
}

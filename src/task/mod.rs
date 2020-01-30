use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// A `Task` represents a unit of work that a `Celery` app can produce or consume.
///
/// The recommended way to define a task is through the [`task`](attr.task.html) procedural macro:
///
/// ```rust
/// use celery::task;
///
/// #[task(name = "add")]
/// fn add(x: i32, y: i32) -> i32 {
///     x + y
/// }
/// ```
///
/// However, if you need more fine-grained control, it is fairly straight forward
/// to implement a task directly. This would be equivalent to above:
///
/// ```rust
/// use async_trait::async_trait;
/// use serde::{Serialize, Deserialize};
/// use celery::{Task, Error};
///
/// #[allow(non_camel_case_types)]
/// #[derive(Serialize, Deserialize)]
/// struct add {
///     x: i32,
///     y: i32,
/// }
///
/// #[async_trait]
/// impl Task for add {
///     const NAME: &'static str = "add";
///     const ARGS: &'static [&'static str] = &["x", "y"];
///
///     type Returns = i32;
///
///     async fn run(mut self) -> Result<Self::Returns, Error> {
///         Ok(self.x + self.y)
///     }
/// }
///
/// impl add {
///     fn s(x: i32, y: i32) -> Self {
///         Self { x, y }
///     }
/// }
/// ```
///
/// Within the [Celery protocol](https://docs.celeryproject.org/en/latest/internals/protocol.html#version-2)
/// the task parameters can be treated as either `args` (positional) or `kwargs` (key-word based).
/// So, for example, from Python the `add` task could be called like
///
/// ```python
/// celery_app.send_task("add", args=[1, 2])
/// ```
///
/// or
///
/// ```python
/// celery_app.send_task("add", kwargs={"x": 1, "y": 2})
/// ```
///
/// # Making task parameters optional
///
/// You can provide default values for task parameters through the
/// [deserialization mechanism](https://serde.rs/attr-default.html). Currently this means
/// you'll have to implement the task manually as opposed to using the `#[task]` macro.
///
/// So if we wanted to make the `y` parameter in the `add` task optional with a default
/// value of 0, we could add the `#[serde(default)]` macro to the `y` field:
///
/// ```rust
/// # use async_trait::async_trait;
/// # use serde::{Serialize, Deserialize};
/// # use celery::{Task, Error};
/// #[allow(non_camel_case_types)]
/// #[derive(Serialize, Deserialize)]
/// struct add {
///     x: i32,
///     #[serde(default)]
///     y: i32,
/// }
/// # #[async_trait]
/// # impl Task for add {
/// #     const NAME: &'static str = "add";
/// #     const ARGS: &'static [&'static str] = &["x", "y"];
/// #     type Returns = i32;
/// #     async fn run(mut self) -> Result<Self::Returns, Error> {
/// #         Ok(self.x + self.y)
/// #     }
/// # }
/// ```
///
/// # Error handling
///
/// As demonstrated above, the `#[task]` macro tries wrapping the return value in `Result<Self::Returns, Error>`
/// when it is ran. Therefore the recommended way to propogate errors when defining a task with
/// `#[task]` is to use `.context("...")?` on `Result` types within the task body.
///
/// For example:
///
/// ```rust
/// use celery::{task, ResultExt};
///
/// #[task(name = "add")]
/// fn read_some_file() -> String {
///     tokio::fs::read_to_string("some_file")
///         .await
///         .context("File does not exist")?
/// }
/// ```
///
/// The `.context` method on a `Result` comes from the [`ResultExt`](trait.ResultExt.html) trait.
/// This is used to provide additional human-readable context to the error and also
/// to convert it into the expected [`Error`](struct.Error.html) type.
#[async_trait]
pub trait Task: Send + Sync + Serialize + for<'de> Deserialize<'de> {
    /// The name of the task. When a task is registered it will be registered with this name.
    const NAME: &'static str;

    /// For compatability with Python tasks. This keeps track of the order
    /// of arguments for the task so that the task can be called from Python with
    /// positional arguments.
    const ARGS: &'static [&'static str];

    /// The return type of the task.
    type Returns: Send + Sync + std::fmt::Debug;

    /// This function defines how a task executes.
    async fn run(mut self) -> Result<Self::Returns, Error>;

    /// Callback that will run after a task fails.
    /// It takes a reference to a `TaskContext` struct and the error returned from task.
    #[allow(unused_variables)]
    async fn on_failure(ctx: &TaskContext<'_>, err: &Error) {}

    /// Callback that will run after a task completes successfully.
    /// It takes a reference to a `TaskContext` struct and the returned value from task.
    #[allow(unused_variables)]
    async fn on_success(ctx: &TaskContext<'_>, returned: &Self::Returns) {}

    /// Default timeout for this task.
    fn timeout(&self) -> Option<u32> {
        None
    }

    /// Default maximum number of retries for this task.
    fn max_retries(&self) -> Option<u32> {
        None
    }

    /// Default minimum retry delay (in seconds) for this task (default is 0).
    fn min_retry_delay(&self) -> Option<u32> {
        None
    }

    /// Default maximum retry delay (in seconds) for this task.
    fn max_retry_delay(&self) -> Option<u32> {
        None
    }
}

/// Additional context sent to the `on_success` and `on_failure` task callbacks.
pub struct TaskContext<'a> {
    /// The correlation ID of the task.
    pub correlation_id: &'a str,
}

/// General configuration options pertaining to a task.
#[derive(Copy, Clone, Default)]
pub struct TaskOptions {
    pub timeout: Option<u32>,
    pub max_retries: Option<u32>,
    pub min_retry_delay: u32,
    pub max_retry_delay: u32,
}

impl TaskOptions {
    pub(crate) fn overrides<T: Task>(&self, task: &T) -> Self {
        Self {
            timeout: task.timeout().or(self.timeout),
            max_retries: task.max_retries().or(self.max_retries),
            min_retry_delay: task.min_retry_delay().unwrap_or(self.min_retry_delay),
            max_retry_delay: task.max_retry_delay().unwrap_or(self.max_retry_delay),
        }
    }
}

#[derive(Default)]
struct TaskSendOptionsConfig {
    queue: Option<String>,
    timeout: Option<u32>,
    countdown: Option<u32>,
    eta: Option<DateTime<Utc>>,
    expires_in: Option<u32>,
    expires: Option<DateTime<Utc>>,
}

/// Used to create custom `TaskSendOptions`.
#[derive(Default)]
pub struct TaskSendOptionsBuilder {
    config: TaskSendOptionsConfig,
}

impl TaskSendOptionsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the queue to send the task to.
    pub fn queue(mut self, queue: String) -> Self {
        self.config.queue = Some(queue);
        self
    }

    /// Set a timeout (in seconds) for the task.
    pub fn timeout(mut self, timeout: u32) -> Self {
        self.config.timeout = Some(timeout);
        self
    }

    /// Set a countdown (in seconds) for the task. This is equivalent to setting
    /// the ETA to `now + countdown`.
    pub fn countdown(mut self, countdown: u32) -> Self {
        self.config.countdown = Some(countdown);
        self
    }

    /// Set task ETA. The task will be executed on or after the ETA.
    pub fn eta(mut self, eta: DateTime<Utc>) -> Self {
        self.config.eta = Some(eta);
        self
    }

    /// Set the task to expire in the given number of seconds from now.
    pub fn expires_in(mut self, seconds: u32) -> Self {
        self.config.expires_in = Some(seconds);
        self
    }

    /// Set the task to expire at the given time.
    pub fn expires(mut self, expires: DateTime<Utc>) -> Self {
        self.config.expires = Some(expires);
        self
    }

    /// Get a `TaskSendOptions` object with this configuration.
    pub fn build(self) -> TaskSendOptions {
        TaskSendOptions {
            queue: self.config.queue,
            timeout: self.config.timeout,
            countdown: self.config.countdown,
            eta: self.config.eta,
            expires_in: self.config.expires_in,
            expires: self.config.expires,
        }
    }
}

/// Options for sending a task. Used to override the defaults.
#[derive(Clone, Default)]
pub struct TaskSendOptions {
    pub queue: Option<String>,
    pub timeout: Option<u32>,
    pub countdown: Option<u32>,
    pub eta: Option<DateTime<Utc>>,
    pub expires_in: Option<u32>,
    pub expires: Option<DateTime<Utc>>,
}

impl TaskSendOptions {
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Get a builder for creating a custom `TaskSendOptions` object.
    pub fn builder() -> TaskSendOptionsBuilder {
        TaskSendOptionsBuilder::new()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum TaskStatus {
    Pending,
    Finished,
}

#[derive(Clone, Debug)]
pub(crate) struct TaskEvent {
    pub(crate) status: TaskStatus,
}

impl TaskEvent {
    pub(crate) fn new(status: TaskStatus) -> Self {
        Self { status }
    }
}

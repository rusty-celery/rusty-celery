use super::Task;
use chrono::{DateTime, Utc};

/// General configuration options pertaining to a task.
#[derive(Copy, Clone, Default)]
pub struct TaskOptions {
    pub timeout: Option<u32>,
    pub max_retries: Option<u32>,
    pub min_retry_delay: Option<u32>,
    pub max_retry_delay: Option<u32>,
    pub acks_late: Option<bool>,
}

impl TaskOptions {
    pub(crate) fn overrides<T: Task>(&self, task: &T) -> Self {
        Self {
            timeout: task.timeout().or(self.timeout),
            max_retries: task.max_retries().or(self.max_retries),
            min_retry_delay: task.min_retry_delay().or(self.min_retry_delay),
            max_retry_delay: task.max_retry_delay().or(self.max_retry_delay),
            acks_late: task.acks_late().or(self.acks_late),
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
    pub fn queue(mut self, queue: &str) -> Self {
        self.config.queue = Some(queue.into());
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

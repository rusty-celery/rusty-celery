use crate::protocol::MessageContentType;

/// Configuration options pertaining to a task.
///
/// These are set at either the app level (pertaining to all registered tasks),
/// the task level (pertaining to a specific task), or - in some cases - at
/// the request / signature level (pertaining only to an individual task request).
///
/// The order of precedence is determined by how specific the given configuration option is.
/// That is, options set at the request level have the highest precedence,
/// followed by options set at the task level, and lastly the app level.
///
/// For example, if `time_limit: Some(10)` is set at the app level through the
/// [`task_time_limit`](crate::CeleryBuilder::task_time_limit) option,
/// then every task will use a time limit of 10 seconds unless some other time limit is specified in the
/// [task definition](attr.task.html#parameters) or in a
/// [task signature](../task/struct.Signature.html#structfield.time_limit) for that task.
#[derive(Copy, Clone, Default)]
pub struct TaskOptions {
    /// Time limit for a task.
    ///
    /// If set to `Some(n)`, the task will be interrupted and fail with a
    /// [`TimeoutError`](error/enum.TaskError.html#variant.TimeoutError)
    /// if it runs longer than `n` seconds.
    ///
    /// This can be set with
    /// - [`task_time_limit`](crate::CeleryBuilder::task_time_limit) at the app level,
    /// - [`time_limit`](../attr.task.html#parameters) at the task level, and
    /// - [`with_time_limit`](crate::task::Signature::with_time_limit) at the request / signature level.
    ///
    /// If this option is left unspecified, the default behavior will be to enforce no time limit.
    ///
    /// *Note, however, that only non-blocking tasks can be interrupted, so it's important
    /// to use async functions within task implementations whenever they are available.*
    pub time_limit: Option<u32>,

    /// The [`time_limit`](TaskOptions::time_limit) option is equivalent to the ["soft time
    /// limit"](https://docs.celeryproject.org/en/stable/userguide/workers.html#time-limits)
    /// option when sending tasks to a Python consumer.
    /// If you desire to set a "hard time limit", use this option.
    ///
    /// *Note that this is really only for compatability with Python workers*.
    /// `time_limit` and `hard_time_limit` are treated the same by Rust workers, and if both
    /// are set, the minimum of the two will be used.
    ///
    /// This can be set with
    /// - [`task_hard_time_limit`](crate::CeleryBuilder::task_hard_time_limit) at the app level,
    /// - [`hard_time_limit`](../attr.task.html#parameters) at the task level, and
    /// - [`with_hard_time_limit`](crate::task::Signature::method.with_hard_time_limit) at the request / signature level.
    pub hard_time_limit: Option<u32>,

    /// Maximum number of retries for this task.
    ///
    /// This can be set with
    /// - [`task_max_retries`](crate::CeleryBuilder::task_max_retries) at the app level, and
    /// - [`max_retries`](../attr.task.html#parameters) at the task level.
    ///
    /// If this option is left unspecified, the default behavior will be to retry tasks indefinitely.
    pub max_retries: Option<u32>,

    /// Minimum retry delay (in seconds).
    ///
    /// This can be set with
    /// - [`task_min_retry_delay`](crate::CeleryBuilder::task_min_retry_delay) at the app level, and
    /// - [`min_retry_delay`](../attr.task.html#parameters) at the task level.
    ///
    /// If this option is left unspecified, the default behavior will be to use 0 as the
    /// minimum. In practice this means the first retry will usually be delayed only a couple hundred milliseconds.
    /// The delay for subsequent retries will increase exponentially (with random jitter) before
    /// maxing out at [`max_retry_delay`](TaskOptions::max_retry_delay).
    pub min_retry_delay: Option<u32>,

    /// Maximum retry delay (in seconds).
    ///
    /// This can be set with
    /// - [`task_max_retry_delay`](crate::CeleryBuilder::task_max_retry_delay) at the app level, and
    /// - [`max_retry_delay`](../attr.task.html#parameters) at the task level.
    ///
    /// If this option is left unspecified, the default behavior will be to cap delays at 1 hour
    /// (3600 seconds) with some random jitter.
    pub max_retry_delay: Option<u32>,

    /// Whether or not to retry the task when an [`UnexpectedError`](crate::error::TaskError::UnexpectedError)
    /// is returned.
    ///
    /// This can be set with
    /// - [`task_retry_for_unexpected`](crate::CeleryBuilder::task_retry_for_unexpected) at the app level, and
    /// - [`retry_for_unexpected`](../attr.task.html#parameters) at the task level.
    ///
    /// If this option is left unspecified, the default behavior will be to retry for these
    /// errors.
    pub retry_for_unexpected: Option<bool>,

    /// Whether messages will be acknowledged after the task has been executed or before.
    ///
    /// If your tasks are
    /// [idempotent](https://docs.celeryproject.org/en/stable/glossary.html#term-idempotent)
    /// then it is recommended that you set this to `true`.
    ///
    /// This can be set with
    /// - [`acks_late`](crate::CeleryBuilder::acks_late) at the app level, and
    /// - [`acks_late`](../attr.task.html#parameters) at the task level.
    ///
    /// If this option is left unspecified, the default behavior will be to ack early.
    pub acks_late: Option<bool>,

    /// Which serialization format to use for task messages.
    ///
    /// This can be set with
    /// - [`task_content_type`](crate::CeleryBuilder::task_content_type) at the app level, and
    /// - [`content_type`](../attr.task.html#parameters) at the task level.
    /// - [`with_content_type`](crate::task::Signature::with_content_type) at the request / signature level.
    pub content_type: Option<MessageContentType>,
}

impl TaskOptions {
    /// Update the fields in `self` with the fields in `other`.
    pub(crate) fn update(&mut self, other: &TaskOptions) {
        self.time_limit = self.time_limit.or(other.time_limit);
        self.hard_time_limit = self.hard_time_limit.or(other.hard_time_limit);
        self.max_retries = self.max_retries.or(other.max_retries);
        self.min_retry_delay = self.min_retry_delay.or(other.min_retry_delay);
        self.max_retry_delay = self.max_retry_delay.or(other.max_retry_delay);
        self.retry_for_unexpected = self.retry_for_unexpected.or(other.retry_for_unexpected);
        self.acks_late = self.acks_late.or(other.acks_late);
        self.content_type = self.content_type.or(other.content_type);
    }

    /// Override the fields in `other` with the fields in `self`.
    pub(crate) fn override_other(&self, other: &mut TaskOptions) {
        other.update(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update() {
        let mut options = TaskOptions {
            max_retries: Some(3),
            acks_late: Some(true),
            ..Default::default()
        };

        let other = TaskOptions {
            time_limit: Some(2),
            acks_late: Some(false),
            ..Default::default()
        };

        options.update(&other);
        assert_eq!(options.time_limit, Some(2));
        assert_eq!(options.max_retries, Some(3));
        assert_eq!(options.acks_late, Some(true));
    }
}

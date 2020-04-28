/// Configuration options pertaining to a task.
///
/// These are set at either the app level (pertaining to all registered tasks),
/// the task level (pertaining to a specific task), or - in some cases - at
/// the request / signature level (pertaining only to an individual task request).
///
/// The order of precedence is determined by how specific the given configuration option is.
/// That is, options set at the request level have the highest precendence,
/// followed by options set at the task level, and lastly the app level.
///
/// For example, if `timeout: Some(10)` is set at the app level through the
/// [`task_timeout` option](../struct.CeleryBuilder.html#method.task_timeout),
/// then every task will use a timeout of 10 seconds unless some other timeout is specified in the
/// [task definition](attr.task.html#parameters) or in a
/// [task signature](../task/struct.Signature.html#structfield.timeout) for that task.
#[derive(Copy, Clone, Default)]
pub struct TaskOptions {
    /// Timeout for a task.
    ///
    /// If set to `Some(n)`, the task will be interrupted and fail with a
    /// [`TimeoutError`](error/enum.TaskError.html#variant.TimeoutError)
    /// if it runs longer than `n` seconds.
    ///
    /// This can be set with
    /// - [`task_timeout`](../struct.CeleryBuilder.html#method.task_timeout) at the app level,
    /// - [`timeout`](../attr.task.html#parameters) at the task level, and
    /// - [`with_timeout`](../task/struct.Signature.html#method.with_timeout) at the request / signature level.
    ///
    /// If this option is left unspecified, the default behavior will be to enforce no timeout.
    ///
    /// *Note, however, that only non-blocking tasks can be interrupted, so it's important
    /// to use async functions within task implementations whenever they are available.*
    pub timeout: Option<u32>,

    /// Maximum number of retries for this task.
    ///
    /// This can be set with
    /// - [`task_max_retries`](../struct.CeleryBuilder.html#method.task_max_retries) at the app level, and
    /// - [`max_retries`](../attr.task.html#parameters) at the task level.
    ///
    /// If this option is left unspecified, the default behavior will be to retry tasks indefinitely.
    pub max_retries: Option<u32>,

    /// Minimum retry delay (in seconds).
    ///
    /// This can be set with
    /// - [`task_min_retry_delay`](../struct.CeleryBuilder.html#method.task_min_retry_delay) at the app level, and
    /// - [`min_retry_delay`](../attr.task.html#parameters) at the task level.
    ///
    /// If this option is left unspecified, the default behavior will be to use 0 as the
    /// minimum. In practice this means the first retry will usually be delayed only a couple hundred milliseconds.
    /// The delay for subsequent retries will increase exponentially (with random jitter) before
    /// maxing out at [`max_retry_delay`](#structfield.max_retry_delay).
    pub min_retry_delay: Option<u32>,

    /// Maximum retry delay (in seconds).
    ///
    /// This can be set with
    /// - [`task_max_retry_delay`](../struct.CeleryBuilder.html#method.task_max_retry_delay) at the app level, and
    /// - [`max_retry_delay`](../attr.task.html#parameters) at the task level.
    ///
    /// If this option is left unspecified, the default behavior will be to cap delays at 1 hour
    /// (3600 seconds) with some random jitter.
    pub max_retry_delay: Option<u32>,

    /// Whether or not to retry the task when an [`UnexpectedError`](error/enum.TaskError.html#variant.UnexpectedError)
    /// is returned.
    ///
    /// This can be set with
    /// - [`task_retry_for_unexpected`](../struct.CeleryBuilder.html#method.task_retry_for_unexpected) at the app level, and
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
    /// - [`acks_late`](../struct.CeleryBuilder.html#method.acks_late) at the app level, and
    /// - [`acks_late`](../attr.task.html#parameters) at the task level.
    ///
    /// If this option is left unspecified, the default behavior will be to ack early.
    pub acks_late: Option<bool>,
}

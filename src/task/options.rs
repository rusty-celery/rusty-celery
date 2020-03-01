/// General configuration options pertaining to a task.
#[derive(Copy, Clone, Default)]
pub struct TaskOptions {
    pub timeout: Option<u32>,
    pub max_retries: Option<u32>,
    pub min_retry_delay: Option<u32>,
    pub max_retry_delay: Option<u32>,
    pub acks_late: Option<bool>,
}

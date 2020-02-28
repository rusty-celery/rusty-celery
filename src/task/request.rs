use super::Task;
use crate::protocol::Message;
use chrono::{DateTime, Utc};
use std::time::SystemTime;

/// A `Request` contains information and state related to the currently executing task.
pub struct Request<T>
where
    T: Task,
{
    /// The unique ID of the executing task.
    pub id: String,

    /// The unique ID of the task's group, if this task is a member.
    pub group: Option<String>,

    /// The unique ID of the chord this task belongs to (if the task is part of the header).
    pub chord: Option<String>,

    /// Custom ID used for things like de-duplication. Usually the same as `id`.
    pub correlation_id: String,

    /// Parameters used to call this task.
    pub params: T::Params,

    /// Name of the host that sent this task.
    pub origin: Option<String>,

    /// How many times the current task has been retried.
    pub retries: u32,

    /// The original ETA of the task.
    pub eta: Option<DateTime<Utc>>,

    /// The original expiration time of the task.
    pub expires: Option<DateTime<Utc>>,

    /// Node name of the worker instance executing the task.
    pub hostname: Option<String>,

    /// Where to send reply to (queue name).
    pub reply_to: Option<String>,

    /// The time limit (in seconds) allocated for this task to execute.
    pub timeout: Option<u32>,
}

impl<T> Request<T>
where
    T: Task,
{
    pub fn new(m: Message, p: T::Params) -> Self {
        let timeout = match m.headers.timelimit {
            (Some(soft_timelimit), Some(hard_timelimit)) => {
                Some(std::cmp::min(soft_timelimit, hard_timelimit))
            }
            (Some(soft_timelimit), None) => Some(soft_timelimit),
            (None, Some(hard_timelimit)) => Some(hard_timelimit),
            _ => None,
        };
        Self {
            id: m.headers.id,
            group: m.headers.group,
            chord: None,
            correlation_id: m.properties.correlation_id,
            params: p,
            origin: m.headers.origin,
            retries: m.headers.retries.unwrap_or(0),
            eta: m.headers.eta,
            expires: m.headers.expires,
            hostname: None,
            reply_to: m.properties.reply_to,
            timeout,
        }
    }

    /// Check if the request is expired.
    pub fn is_expired(&self) -> bool {
        if let Some(expires) = self.expires {
            let now = DateTime::<Utc>::from(SystemTime::now());
            (now - expires).num_milliseconds() >= 0
        } else {
            false
        }
    }
}

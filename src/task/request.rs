use super::Task;
use crate::error::ProtocolError;
use crate::protocol::Message;
use chrono::{DateTime, Utc};
use std::convert::TryFrom;
use std::time::SystemTime;
use tokio::time::Duration;

/// A `Request` contains information and state related to the currently executing task.
#[derive(Clone)]
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
    pub time_limit: Option<u32>,
}

impl<T> Request<T>
where
    T: Task,
{
    pub fn new(m: Message, p: T::Params) -> Self {
        let time_limit = match m.headers.timelimit {
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
            time_limit,
        }
    }

    /// Check if the request has a future ETA.
    pub fn is_delayed(&self) -> bool {
        self.eta.is_some()
    }

    /// Get the TTL in seconds if the task has a future ETA.
    pub fn countdown(&self) -> Option<Duration> {
        if let Some(eta) = self.eta {
            let now = DateTime::<Utc>::from(SystemTime::now());
            let countdown = (eta - now).num_milliseconds();
            if countdown < 0 {
                None
            } else {
                Some(Duration::from_millis(countdown as u64))
            }
        } else {
            None
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

impl<T> TryFrom<Message> for Request<T>
where
    T: Task,
{
    type Error = ProtocolError;

    fn try_from(m: Message) -> Result<Self, Self::Error> {
        let body = m.body::<T>()?;
        let (task_params, _) = body.parts();
        Ok(Self::new(m, task_params))
    }
}

use super::Schedule;
use crate::{
    protocol::TryCreateMessage,
    task::{Signature, Task},
};
use std::{cmp::Ordering, time::SystemTime};

/// A task which is scheduled for execution. It contains the task to execute,
/// the queue where to send it and the schedule which determines when to do it.
pub struct ScheduledTask {
    pub name: String,
    pub message_factory: Box<dyn TryCreateMessage>,
    pub queue: String,
    pub schedule: Box<dyn Schedule>,
    pub total_run_count: u32,
    pub last_run_at: Option<SystemTime>,
    pub next_call_at: SystemTime,
}

impl ScheduledTask {
    pub fn new<T, S>(
        name: String,
        signature: Signature<T>,
        queue: String,
        schedule: S,
    ) -> ScheduledTask
    where
        T: Task + Clone + 'static,
        S: Schedule + 'static,
    {
        let next_call_at = schedule.next_call_at(None);
        ScheduledTask {
            name,
            message_factory: Box::new(signature),
            queue,
            schedule: Box::new(schedule),
            total_run_count: 0,
            last_run_at: None,
            next_call_at,
        }
    }

    pub fn next_call_at(&self) -> SystemTime {
        self.schedule.next_call_at(self.last_run_at)
    }
}

// We implement PartialEq, Eq, PartialOrd and Ord for ScheduledTask because we want to use it in a BinaryHeap.

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // We only care about next_call_at when we are comparing different tasks.
        // The comparison order is important (other is compared to self):
        // BinaryHeap is a max-heap by default, but we want a min-heap.
        other.next_call_at.cmp(&self.next_call_at)
    }
}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        other.cmp(self) == Ordering::Equal
    }
}

impl Eq for ScheduledTask {}

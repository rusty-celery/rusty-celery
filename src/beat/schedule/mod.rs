//! This module contains the definition of application-provided schedules.
//!
//! These structs have not changed a lot compared to Python: in Python there are three
//! different types of schedules: `schedule` (corresponding to [`DeltaSchedule`]),
//! `crontab` (corresponding to [`CronSchedule`]), `solar` (not implemented yet).
use std::time::{Duration, SystemTime};

mod cron;
pub use cron::CronSchedule;

/// The trait that all schedules implement.
pub trait Schedule {
    /// Compute when a task should be executed again.
    /// If this method returns `None` then the task should
    /// never run again and it is safe to remove it from the
    /// list of scheduled tasks.
    fn next_call_at(&self, last_run_at: Option<SystemTime>) -> Option<SystemTime>;
}

/// A schedule that can be used to execute tasks at regular intervals.
pub struct DeltaSchedule {
    interval: Duration,
}

impl DeltaSchedule {
    /// Create a new time delta schedule which can be used to execute a task
    /// forever, starting immediately and with the given `interval`
    /// between subsequent executions.
    pub fn new(interval: Duration) -> DeltaSchedule {
        DeltaSchedule { interval }
    }
}

impl Schedule for DeltaSchedule {
    fn next_call_at(&self, last_run_at: Option<SystemTime>) -> Option<SystemTime> {
        match last_run_at {
            Some(last_run_at) => Some(
                last_run_at
                    .checked_add(self.interval)
                    .expect("Invalid SystemTime encountered"),
            ),
            None => Some(SystemTime::now()),
        }
    }
}

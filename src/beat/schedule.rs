/// This module contains the definition of application-provided schedules.
///
/// This structs have not changed a lot compared to Python: in Python there are three
/// different types of schedules: `schedule` (corresponding to [`RegularSchedule`]),
/// `crontab` (not implemented yet), `solar` (not implemented yet).
use std::time::{Duration, SystemTime};

mod cron;
pub use cron::CronSchedule;

/// The trait that all schedules implement.
///
/// For now, we only support regular schedules.
pub trait Schedule {
    fn next_call_at(&self, last_run_at: Option<SystemTime>) -> Option<SystemTime>;
}

/// When using this schedule, tasks are executed at regular intervals.
pub struct RegularSchedule {
    interval: Duration,
}

impl RegularSchedule {
    pub fn new(interval: Duration) -> RegularSchedule {
        RegularSchedule { interval }
    }
}

impl Schedule for RegularSchedule {
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

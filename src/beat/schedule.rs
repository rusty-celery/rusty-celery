use std::time::{Duration, SystemTime};

/// All schedules implement this trait (regular, cron, solar).
/// For now, we only support regular schedules.
pub trait Schedule {
    fn next_call_at(&self, last_run_at: Option<SystemTime>) -> SystemTime;
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
    fn next_call_at(&self, last_run_at: Option<SystemTime>) -> SystemTime {
        match last_run_at {
            Some(last_run_at) => last_run_at.checked_add(self.interval).unwrap(),
            None => SystemTime::now(),
        }
    }
}

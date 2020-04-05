// TERMINOLOGY (for the moment, this corresponds 1on1 with Python implementation, but it will probably evolve):
// - schedule: the strategy used to decide when a task must be executed (each scheduled task has its
//   own schedule)
// - schedule entry: a task together with its schedule
// - scheduler: the component in charge of keeping track of tasks to execute
// - service: the background service that drives execution, calling the appropriate
//   methods of the scheduler in an infinite loop

// We change a little the architecture compared to Python. In Python,
// there is a Scheduler base class and all schedulers inherit from that.
// The main logic is in the scheduler though, so here we use a scheduler
// struct and a trait for the scheduler backend.

// In Python, there are three different types of schedules: `schedule`
// (just use a time delta), `crontab`, `solar`. They all inherit from
// `BaseSchedule`.

use futures::future::Future;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use tokio::time;

// The trait implemented by all schedules (regular, cron, solar).
// We will only have regular for now.
pub trait Schedule {
    fn is_due(&self, last_run_at: Option<SystemTime>) -> (bool, Duration);
}

// When using this schedule, tasks are executed at regular intervals.
struct RegularSchedule {
    interval: Duration,
}

impl RegularSchedule {
    fn new(interval: Duration) -> RegularSchedule {
        RegularSchedule { interval }
    }
}

impl Schedule for RegularSchedule {
    fn is_due(&self, last_run_at: Option<SystemTime>) -> (bool, Duration) {
        match last_run_at {
            Some(last_run_at) => {
                let next_run_at = last_run_at.checked_add(self.interval).unwrap();
                let now = SystemTime::now();
                if next_run_at <= now {
                    (true, Duration::from_secs(0))
                } else {
                    (false, next_run_at.duration_since(now).unwrap())
                }
            }
            None => (true, Duration::from_secs(0)),
        }
    }
}

// A task which is scheduled for execution. It contains the Future to execute,
// the schedule which determines when to run the task, and some other info.
struct ScheduleEntry {
    name: String,
    future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>, // TODO somehow this must be cloneable
    schedule: Box<dyn Schedule>,
    last_run_at: Option<SystemTime>,
    total_run_count: u32,
}

impl ScheduleEntry {
    fn new<F, S>(name: &str, future: F, schedule: S) -> ScheduleEntry
    where
        F: Future<Output = ()> + Send + 'static,
        S: Schedule + 'static,
    {
        ScheduleEntry {
            name: name.to_string(),
            future: Box::pin(future),
            schedule: Box::new(schedule),
            last_run_at: None,
            total_run_count: 0,
        }
    }
    fn is_due(&self) -> (bool, Duration) {
        self.schedule.is_due(self.last_run_at)
    }
}

// The Python impl stores a tuple with this structure inside the heap (see the Scheduler struct).
// We could directly add this info to ScheduleEntry, but for now we keep
// the same structure.
struct SortableScheduleEntry {
    is_due: bool,
    next_call_delay: Duration,
    entry: ScheduleEntry,
}

// TODO make impls coherent
impl PartialEq for SortableScheduleEntry {
    fn eq(&self, other: &Self) -> bool {
        // We should make sure that names are unique, or we should refine this implementation
        self.entry.name == other.entry.name
    }
}

impl Eq for SortableScheduleEntry {}

impl Ord for SortableScheduleEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // The order is important (other is compared to self):
        // BinaryHeap is a max-heap by default, but we want a min-heap.
        other.next_call_delay.cmp(&self.next_call_delay)
    }
}

impl PartialOrd for SortableScheduleEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Struct with all the main methods. It uses a min-heap to retrieve
// the task which should run next.
pub struct Scheduler {
    heap: BinaryHeap<SortableScheduleEntry>,
    default_sleep_interval: Duration,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler {
            heap: BinaryHeap::new(),
            default_sleep_interval: Duration::from_millis(500),
        }
    }

    // TODO Check the format of a schedule on file,
    // though we don't want to implement that now,
    // because it will require to serialize the task to run, somehow.
    // Better to start with a function which
    // the user must use to register scheduled tasks, and this
    // function will accept a Future, with all task arguments
    // already provided:
    pub fn add<F, S>(&mut self, name: &str, future: F, schedule: S)
    where
        F: Future<Output = ()> + Send + 'static,
        S: Schedule + 'static,
    {
        let entry = ScheduleEntry::new(name, future, schedule);
        // TODO ask is_due and next_call_delay to Schedule
        self.heap.push(SortableScheduleEntry {
            is_due: true,
            next_call_delay: Duration::from_secs(0),
            entry,
        });
    }

    fn tick(&mut self) -> Duration {
        // Here we want to pop a scheduled entry and execute it if it is due,
        // then push it back on the heap with an updated due time,
        // and finally return when the next entry is due (may be due immediately).
        // At least, this is what Python does.
        if let Some(SortableScheduleEntry {
            is_due,
            next_call_delay,
            entry,
        }) = self.heap.pop()
        {
            if is_due {
                let future = entry.future;
                tokio::spawn(async move { future.await });
                // TODO we have to add the entry back to the heap,
                // but currently we cannot
                // because we have moved the future out of it.
                Duration::from_secs(0) // Ask to immediately call tick again (that's what Python does, it may be improved?)
            } else {
                next_call_delay
            }
        } else {
            self.default_sleep_interval
        }
    }
}

// Not sure yet what logic should end up here.
trait SchedulerBackend {}

// The only Scheduler Backend implementation for now. It keeps
// all data in memory.
struct InMemoryBackend {}

// This is the structure that manages the main loop.
// The main method is `start`, which calls scheduler.tick,
// sleeps if necessary and then calls scheduler.sync.
// Not sure if it is necessary for it to be a struct,
// maybe just a function is enough.
// This should be moved to another file later (beat.rs?)
struct Service {
    scheduler: Scheduler,
}

impl Service {
    pub fn new(scheduler: Scheduler) -> Service {
        Service { scheduler }
    }

    pub async fn start(&mut self) -> ! {
        loop {
            let sleep_interval = self.scheduler.tick();
            println!("Now sleeping for {:?}", sleep_interval);
            time::delay_for(sleep_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests are only meant for debugging purposes (for now).

    async fn scheduled_task() {
        println!("I am a scheduled task");
    }

    #[tokio::test]
    async fn test_basic_flow() {
        let mut scheduler = Scheduler::new();
        scheduler.add(
            "Scheduled Task",
            scheduled_task(),
            RegularSchedule::new(Duration::from_millis(40)),
        );

        let mut service = Service::new(scheduler);
        let res = time::timeout(Duration::from_secs(1), service.start()).await;
        assert!(res.is_err()) // we get an error because the timeout always fires
    }
}

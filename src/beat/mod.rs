// TERMINOLOGY (for the moment, this is very similar to Python implementation, but it will probably evolve):
// - schedule: the strategy used to decide when a task must be executed (each scheduled task has its
//   own schedule)
// - schedule entry: a task together with its schedule
// - scheduler: the component in charge of keeping track of tasks to execute
// - beat service: the background service that drives execution, calling the appropriate
//   methods of the scheduler in an infinite loop (called just service in Python)

// We have changed a little the architecture compared to Python. In Python,
// there is a Scheduler base class and all schedulers inherit from that.
// The main logic is in the scheduler though, so here we use a scheduler
// struct and a trait for the scheduler backend (not implemented for now).

// In Python, there are three different types of schedules: `schedule`
// (just use a time delta), `crontab`, `solar`. They all inherit from
// `BaseSchedule`.

use crate::broker::Broker;
use crate::protocol::TryCreateMessage;
use crate::routing::{self, Rule};
use crate::task::{Signature, Task};
use log::{debug, info, warn};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime};
use tokio::time;

mod schedule;
pub use schedule::{RegularSchedule, Schedule};

const ZERO_SECS: Duration = Duration::from_secs(0);

/// A task which is scheduled for execution. It contains the task to execute,
/// the queue where to send it and the schedule which determines when to do it.
struct ScheduledTask {
    name: String,
    message_factory: Box<dyn TryCreateMessage>,
    queue: String,
    schedule: Box<dyn Schedule>,
    total_run_count: u32,
    last_run_at: Option<SystemTime>,
    next_call_at: SystemTime,
}

impl ScheduledTask {
    fn new<T, S>(name: String, signature: Signature<T>, queue: String, schedule: S) -> ScheduledTask
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

    fn next_call_at(&self) -> SystemTime {
        self.schedule.next_call_at(self.last_run_at)
    }
}

// TODO make impls coherent
impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        // We should make sure that names are unique, or we should refine this implementation
        self.name == other.name
    }
}

impl Eq for ScheduledTask {}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // The order is important (other is compared to self):
        // BinaryHeap is a max-heap by default, but we want a min-heap.
        other.next_call_at.cmp(&self.next_call_at)
    }
}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Struct with all the main methods. It uses a min-heap to retrieve
// the task which should run next.
pub struct Scheduler<B: Broker> {
    heap: BinaryHeap<ScheduledTask>,
    default_sleep_interval: Duration,
    broker: B,
    task_routes: Vec<Rule>,
    default_queue: String,
}

impl<B> Scheduler<B>
where
    B: Broker,
{
    pub(crate) fn new(broker: B, task_routes: Vec<Rule>, default_queue: String) -> Scheduler<B> {
        Scheduler {
            heap: BinaryHeap::new(),
            default_sleep_interval: Duration::from_millis(500),
            broker,
            task_routes,
            default_queue,
        }
    }

    // TODO Check the format of a schedule on file,
    // though we don't want to implement that now,
    // because it will require to serialize the task to run, somehow.
    // Better to start with a function which
    // the user must use to register scheduled tasks, and this
    // function will accept a Future, with all task arguments
    // already provided:
    fn schedule_task<T, S>(&mut self, name: String, signature: Signature<T>, schedule: S)
    where
        T: Task + Clone + 'static,
        S: Schedule + 'static,
    {
        let queue = match &signature.queue {
            Some(queue) => queue.to_string(),
            None => routing::route(T::NAME, &self.task_routes)
                .unwrap_or(&self.default_queue)
                .to_string(),
        };

        self.heap
            .push(ScheduledTask::new(name, signature, queue, schedule));
    }

    async fn tick(&mut self) -> Duration {
        // Here we want to pop a scheduled entry and execute it if it is due,
        // then push it back on the heap with an updated due time,
        // and finally return when the next entry is due (may be due immediately).
        // At least, this is what Python does.
        if let Some(mut scheduled_task) = self.heap.peek_mut() {
            let now = SystemTime::now();
            if scheduled_task.next_call_at <= now {
                let message = scheduled_task
                    .message_factory
                    .try_create_message()
                    .expect("TODO");
                let queue = &scheduled_task.queue;
                debug!("Sending task {} to {} queue", scheduled_task.name, queue);
                self.broker.send(&message, &queue).await.expect("TODO");

                scheduled_task.last_run_at.replace(now);
                scheduled_task.total_run_count += 1;
                scheduled_task.next_call_at = scheduled_task.next_call_at();

                ZERO_SECS // Ask to immediately call tick again
            } else {
                debug!("Too early, let's sleep more");
                scheduled_task.next_call_at.duration_since(now).unwrap()
            }
        } else {
            debug!("Nothing to do!");
            warn!(
                "Currently, it is not possible to schedule tasks while the beat service is running"
            );
            self.default_sleep_interval
        }
    }
}

// Not sure yet what logic should end up here.
// trait SchedulerBackend {}

// The only Scheduler Backend implementation for now. It keeps
// all data in memory.
// struct InMemoryBackend {}

// This is the structure that manages the main loop.
// The main method is `start`, which calls scheduler.tick,
// sleeps if necessary and then calls scheduler.sync.
// Not sure if it is necessary for it to be a struct,
// maybe just a function is enough.
// This should be moved to another file later (beat.rs?)
pub struct BeatService<B: Broker + 'static> {
    scheduler: Scheduler<B>,
}

impl<B> BeatService<B>
where
    B: Broker + 'static,
{
    pub fn new(scheduler: Scheduler<B>) -> BeatService<B> {
        BeatService { scheduler }
    }

    pub fn schedule_task<T, S>(&mut self, signature: Signature<T>, schedule: S)
    where
        T: Task + Clone + 'static,
        S: Schedule + 'static,
    {
        self.scheduler
            .schedule_task(Signature::<T>::task_name().to_string(), signature, schedule);
    }

    pub async fn start(&mut self) -> ! {
        info!("Starting beat service");
        loop {
            let sleep_interval = self.scheduler.tick().await;
            debug!("Now sleeping for {:?}", sleep_interval);
            time::delay_for(sleep_interval).await;
        }
    }
}

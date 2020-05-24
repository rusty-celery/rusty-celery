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
use crate::routing::{self, Rule};
use crate::task::{Signature, Task};
use log::{debug, info, warn};
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime};
use tokio::time;

mod schedule;
pub use schedule::{RegularSchedule, Schedule};

mod scheduled_task;
use scheduled_task::ScheduledTask;

const ZERO_SECS: Duration = Duration::from_secs(0);

/// A scheduler is in charge of executing scheduled tasks when they are due.
///
/// It is somehow similar to a future, in the sense that by itself it does nothing,
/// and execution is driven by an "executor" (the `BeatService`) which is in charge
/// of calling the scheduler tick.
///
/// Internally it uses a min-heap to store tasks and efficiently retrieve the ones
/// that are due for execution.
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

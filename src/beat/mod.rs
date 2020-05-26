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
use crate::{
    protocol::TryCreateMessage,
    task::{Signature, Task},
};
use log::{debug, error, info};
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime};
use tokio::time;

mod backend;
pub use backend::{InMemoryBackend, SchedulerBackend};

mod schedule;
pub use schedule::{RegularSchedule, Schedule};

mod scheduled_task;
use scheduled_task::ScheduledTask;

/// A scheduler is in charge of executing scheduled tasks when they are due.
///
/// It is somehow similar to a future, in the sense that by itself it does nothing,
/// and execution is driven by an "executor" (the `BeatService`) which is in charge
/// of calling the scheduler tick.
///
/// Internally it uses a min-heap to store tasks and efficiently retrieve the ones
/// that are due for execution.
pub(crate) struct Scheduler<B: Broker> {
    heap: BinaryHeap<ScheduledTask>,
    default_sleep_interval: Duration,
    broker: B,
}

impl<B> Scheduler<B>
where
    B: Broker,
{
    pub(crate) fn new(broker: B) -> Scheduler<B> {
        Scheduler {
            heap: BinaryHeap::new(),
            default_sleep_interval: Duration::from_millis(500),
            broker,
        }
    }

    fn schedule_task<S>(
        &mut self,
        name: String,
        message_factory: Box<dyn TryCreateMessage>,
        queue: String,
        schedule: S,
    ) where
        S: Schedule + 'static,
    {
        match schedule.next_call_at(None) {
            Some(next_call_at) => self.heap.push(ScheduledTask::new(
                name,
                message_factory,
                queue,
                schedule,
                next_call_at,
            )),
            None => debug!(
                "The schedule of task {} never scheduled the task to run, so it has been dropped.",
                name
            ),
        }
    }

    fn get_scheduled_tasks(&mut self) -> &mut BinaryHeap<ScheduledTask> {
        &mut self.heap
    }

    async fn tick(&mut self) -> SystemTime {
        let now = SystemTime::now();
        let scheduled_task = self.heap.pop();

        if let Some(mut scheduled_task) = scheduled_task {
            if scheduled_task.next_call_at <= now {
                self.send_scheduled_task(&mut scheduled_task).await;

                if let Some(next_call_at) = scheduled_task.next_call_at() {
                    scheduled_task.next_call_at = next_call_at;
                    self.heap.push(scheduled_task);
                } else {
                    debug!(
                        "Task {} is not scheduled to run anymore and will be dropped",
                        scheduled_task.name
                    );
                }
            }
        }

        self.next_tick_at(now)
    }

    async fn send_scheduled_task(&self, scheduled_task: &mut ScheduledTask) {
        let queue = &scheduled_task.queue;

        match scheduled_task.message_factory.try_create_message() {
            Ok(message) => {
                debug!("Sending task {} to {} queue", scheduled_task.name, queue);
                match self.broker.send(&message, &queue).await {
                    Ok(()) => {
                        scheduled_task.last_run_at.replace(SystemTime::now());
                        scheduled_task.total_run_count += 1;
                    }
                    Err(err) => {
                        error!(
                            "Cannot send message for task {}. Error: {}",
                            scheduled_task.name, err
                        );
                    }
                }
            }
            Err(err) => {
                error!(
                    "Cannot create message for task {}. Error: {}",
                    scheduled_task.name, err
                );
                // TODO should we remove the task in this case?
            }
        }
    }

    fn next_tick_at(&self, now: SystemTime) -> SystemTime {
        if let Some(scheduled_task) = self.heap.peek() {
            debug!(
                "Next scheduled task is at {:?}",
                scheduled_task.next_call_at
            );
            scheduled_task.next_call_at
        } else {
            debug!(
                "No scheduled tasks, sleeping for {:?}",
                self.default_sleep_interval
            );
            now + self.default_sleep_interval
        }
    }
}

/// The beat service is in charge of executing scheduled tasks when
/// they are due and add or remove tasks as required. It drives execution by
/// making the internal scheduler "tick", and updates the list of scheduled
/// tasks through a customizable scheduler backend.
pub struct BeatService<Br: Broker + 'static, Sb: SchedulerBackend> {
    scheduler: Scheduler<Br>,
    scheduler_backend: Sb,
    task_routes: Vec<Rule>,
    default_queue: String,
}

impl<Br> BeatService<Br, InMemoryBackend>
where
    Br: Broker + 'static,
{
    pub(crate) fn new(
        scheduler: Scheduler<Br>,
        task_routes: Vec<Rule>,
        default_queue: String,
    ) -> BeatService<Br, InMemoryBackend> {
        BeatService {
            scheduler,
            scheduler_backend: InMemoryBackend::new(),
            task_routes,
            default_queue,
        }
    }
}

impl<Br, Sb> BeatService<Br, Sb>
where
    Br: Broker + 'static,
    Sb: SchedulerBackend,
{
    // TODO add a function to create a BeatService with a custom scheduler backend

    #[allow(dead_code)]
    fn schedule_message_factory<S>(
        &mut self,
        name: String,
        message_factory: Box<dyn TryCreateMessage>,
        queue: Option<String>,
        schedule: S,
    ) where
        S: Schedule + 'static,
    {
        let queue = queue.unwrap_or(self.default_queue.clone());
        self.scheduler
            .schedule_task(name, message_factory, queue, schedule);
    }

    pub fn schedule_task<T, S>(&mut self, signature: Signature<T>, schedule: S)
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
        let message_factory = Box::new(signature);

        self.scheduler.schedule_task(
            Signature::<T>::task_name().to_string(),
            message_factory,
            queue,
            schedule,
        );
    }

    pub async fn start(&mut self) -> ! {
        info!("Starting beat service");
        loop {
            let next_tick_at = self.scheduler.tick().await;

            if self.scheduler_backend.should_sync() {
                self.scheduler_backend
                    .sync(self.scheduler.get_scheduled_tasks());
            }

            let now = SystemTime::now();
            if now < next_tick_at {
                let sleep_interval = next_tick_at.duration_since(now).expect(
                    "Unexpected error when unwrapping a SystemTime comparison that cannot fail",
                );
                debug!("Now sleeping for {:?}", sleep_interval);
                time::delay_for(sleep_interval).await;
            }
        }
    }
}

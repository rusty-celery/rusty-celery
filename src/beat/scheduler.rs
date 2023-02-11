use super::{scheduled_task::ScheduledTask, Schedule};
use crate::{broker::Broker, error::BeatError, protocol::TryCreateMessage};
use log::{debug, info};
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime};

const DEFAULT_SLEEP_INTERVAL: Duration = Duration::from_millis(500);

/// A [`Scheduler`] is in charge of executing scheduled tasks when they are due.
///
/// It is somehow similar to a future, in the sense that by itself it does nothing,
/// and execution is driven by an "executor" (the [`Beat`](super::Beat)) which
/// is in charge of calling the scheduler [`tick`](Scheduler::tick).
///
/// Internally it uses a min-heap to store tasks and efficiently retrieve the ones
/// that are due for execution.
pub struct Scheduler {
    heap: BinaryHeap<ScheduledTask>,
    default_sleep_interval: Duration,
    pub broker: Box<dyn Broker>,
}

impl Scheduler {
    /// Create a new scheduler which uses the given `broker`.
    pub fn new(broker: Box<dyn Broker>) -> Scheduler {
        Scheduler {
            heap: BinaryHeap::new(),
            default_sleep_interval: DEFAULT_SLEEP_INTERVAL,
            broker,
        }
    }

    /// Schedule the execution of a task.
    pub fn schedule_task<S>(
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

    /// Get all scheduled tasks.
    pub fn get_scheduled_tasks(&mut self) -> &mut BinaryHeap<ScheduledTask> {
        &mut self.heap
    }

    /// Get the time when the next task should be executed.
    fn next_task_time(&self, now: SystemTime) -> SystemTime {
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

    /// Tick once. This method checks if there is a scheduled task which is due
    /// for execution and, if so, sends it to the broker.
    /// It returns the time by which `tick` should be called again.
    pub async fn tick(&mut self) -> Result<SystemTime, BeatError> {
        let now = SystemTime::now();
        let next_task_time = self.next_task_time(now);

        if next_task_time <= now {
            let mut scheduled_task = self
                .heap
                .pop()
                .expect("No scheduled tasks found even though there should be");
            let result = self.send_scheduled_task(&mut scheduled_task).await;

            // Reschedule the task before checking if the task execution was successful.
            // TODO: we may have more fine-grained logic here and reschedule the task
            // only after examining the type of error.
            if let Some(rescheduled_task) = scheduled_task.reschedule_task() {
                self.heap.push(rescheduled_task);
            } else {
                debug!("A task is not scheduled to run anymore and will be dropped");
            }

            result?;
            Ok(self.next_task_time(now))
        } else {
            Ok(next_task_time)
        }
    }

    /// Send a task to the broker.
    async fn send_scheduled_task(
        &self,
        scheduled_task: &mut ScheduledTask,
    ) -> Result<(), BeatError> {
        let queue = &scheduled_task.queue;

        let message = scheduled_task.message_factory.try_create_message()?;

        info!(
            "Sending task {}[{}] to {} queue",
            scheduled_task.name,
            message.task_id(),
            queue
        );
        self.broker.send(&message, queue).await?;
        scheduled_task.last_run_at.replace(SystemTime::now());
        scheduled_task.total_run_count += 1;
        Ok(())
    }
}

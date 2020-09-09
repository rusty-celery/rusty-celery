use super::{scheduled_task::ScheduledTask, Schedule};
use crate::{broker::Broker, error::BeatError, protocol::TryCreateMessage};
use log::{debug, info};
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime};

/// A scheduler is in charge of executing scheduled tasks when they are due.
///
/// It is somehow similar to a future, in the sense that by itself it does nothing,
/// and execution is driven by an "executor" (the [`Beat`](struct.beat.html)) which
/// is in charge of calling the scheduler *tick*.
///
/// Internally it uses a min-heap to store tasks and efficiently retrieve the ones
/// that are due for execution.
pub struct Scheduler<B: Broker> {
    heap: BinaryHeap<ScheduledTask>,
    default_sleep_interval: Duration,
    pub broker: B,
}

impl<B> Scheduler<B>
where
    B: Broker,
{
    /// Create a new scheduler which uses the given `broker`.
    pub fn new(broker: B) -> Scheduler<B> {
        Scheduler {
            heap: BinaryHeap::new(),
            default_sleep_interval: Duration::from_millis(500),
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

    /// Tick once. This method checks if there is a scheduled task which is due
    /// for execution and, if so, sends it to the broker.
    /// It returns the time by which `tick` should be called again.
    pub async fn tick(&mut self) -> Result<SystemTime, BeatError> {
        let now = SystemTime::now();
        let scheduled_task = self.heap.pop();

        if let Some(mut scheduled_task) = scheduled_task {
            if scheduled_task.next_call_at <= now {
                let result = self.send_scheduled_task(&mut scheduled_task).await;

                // Reschedule the task before checking if the task execution was successful.
                // TODO: we may have more fine-grained logic here and reschedule the task
                // only after examining the type of error.
                if let Some(rescheduled_task) = scheduled_task.reschedule_task() {
                    self.heap.push(rescheduled_task);
                } else {
                    debug!("A task is not scheduled to run anymore and will be dropped");
                }

                result?
            }
        }

        if let Some(scheduled_task) = self.heap.peek() {
            debug!(
                "Next scheduled task is at {:?}",
                scheduled_task.next_call_at
            );
            Ok(scheduled_task.next_call_at)
        } else {
            debug!(
                "No scheduled tasks, sleeping for {:?}",
                self.default_sleep_interval
            );
            Ok(now + self.default_sleep_interval)
        }
    }

    /// Send a task to the broker.
    async fn send_scheduled_task(
        &self,
        scheduled_task: &mut ScheduledTask,
    ) -> Result<(), BeatError> {
        let queue = &scheduled_task.queue;

        let message = scheduled_task.message_factory.try_create_message()?;

        info!("Sending task {} to {} queue", scheduled_task.name, queue);
        self.broker.send(&message, &queue).await?;
        scheduled_task.last_run_at.replace(SystemTime::now());
        scheduled_task.total_run_count += 1;
        Ok(())
    }
}

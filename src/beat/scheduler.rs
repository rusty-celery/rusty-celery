use super::{scheduled_task::ScheduledTask, Schedule};
use crate::{broker::Broker, protocol::TryCreateMessage};
use log::{debug, error};
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
    broker: B,
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
    pub async fn tick(&mut self) -> SystemTime {
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

    /// Send a task to the broker.
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

    /// Check when the next tick is due.
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

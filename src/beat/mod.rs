//! Celery beat is an app that can automatically produce tasks at scheduled times.
//!
//! ### Terminology
//!
//! This is the terminology used in this module (with references to the corresponding names
//! in the Python implementation):
//! - schedule: the strategy used to decide when a task must be executed (each scheduled
//!   task has its own schedule);
//! - scheduled task: a task together with its schedule (it more or less corresponds to
//!   a *schedule entry* in Python);
//! - scheduler: the component in charge of keeping track of tasks to execute;
//! - scheduler backend: the component that updates the internal state of the scheduler according to
//!   to an external source of truth (e.g., a database); there is no equivalent in Python,
//!   due to the fact that another pattern is used (see below);
//! - beat: the service that drives the execution, calling the appropriate
//!   methods of the scheduler in an infinite loop (called just *service* in Python).
//!
//! The main difference with the architecture used in Python is that in Python
//! there is a base scheduler class which contains the scheduling logic, then different
//! implementations use different strategies to synchronize the scheduler.
//! Here instead we have only one scheduler struct, and the different backends
//! correspond to the different scheduler implementations in Python.

use crate::broker::{build_and_connect, configure_task_routes, Broker, BrokerBuilder, Queue};
use crate::routing::{self, Rule};
use crate::{
    error::{BeatError, BrokerError, CeleryError},
    protocol::MessageContentType,
    task::{Signature, Task, TaskOptions},
};
use log::{debug, error, info};
use std::time::SystemTime;
use tokio::time::{self, Duration};
mod scheduler;
pub use scheduler::Scheduler;

mod backend;
pub use backend::{LocalSchedulerBackend, SchedulerBackend};

mod schedule;
pub use schedule::{RegularSchedule, Schedule};

mod scheduled_task;
pub use scheduled_task::ScheduledTask;

struct Config<Bb>
where
    Bb: BrokerBuilder,
{
    name: String,
    broker_builder: Bb,
    broker_connection_timeout: u32,
    broker_connection_retry: bool,
    broker_connection_max_retries: u32,
    broker_connection_retry_delay: u32,
    default_queue: String,
    task_routes: Vec<(String, Queue)>,
    task_options: TaskOptions,
}

/// Used to create a `Beat` app with a custom configuration.
pub struct BeatBuilder<Bb, Sb>
where
    Bb: BrokerBuilder,
    Sb: SchedulerBackend,
{
    config: Config<Bb>,
    scheduler_backend: Sb,
}

impl<Bb> BeatBuilder<Bb, LocalSchedulerBackend>
where
    Bb: BrokerBuilder,
{
    /// Get a `BeatBuilder` for creating a `Beat` app with a default scheduler backend
    /// and a custom configuration.
    pub fn with_default_scheduler_backend(name: &str, broker_url: &str) -> Self {
        Self {
            config: Config {
                name: name.into(),
                broker_builder: Bb::new(broker_url),
                broker_connection_timeout: 2,
                broker_connection_retry: true,
                broker_connection_max_retries: 5,
                broker_connection_retry_delay: 5,
                default_queue: "celery".into(),
                task_routes: vec![],
                task_options: TaskOptions::default(),
            },
            scheduler_backend: LocalSchedulerBackend::new(),
        }
    }
}

impl<Bb, Sb> BeatBuilder<Bb, Sb>
where
    Bb: BrokerBuilder,
    Sb: SchedulerBackend,
{
    /// Get a `BeatBuilder` for creating a `Beat` app with a custom scheduler backend and
    /// a custom configuration.
    pub fn with_custom_scheduler_backend(
        name: &str,
        broker_url: &str,
        scheduler_backend: Sb,
    ) -> Self {
        Self {
            config: Config {
                name: name.into(),
                broker_builder: Bb::new(broker_url),
                broker_connection_timeout: 2,
                broker_connection_retry: true,
                broker_connection_max_retries: 5,
                broker_connection_retry_delay: 5,
                default_queue: "celery".into(),
                task_routes: vec![],
                task_options: TaskOptions::default(),
            },
            scheduler_backend,
        }
    }

    /// Set the name of the default queue to something other than "celery".
    pub fn default_queue(mut self, queue_name: &str) -> Self {
        self.config.default_queue = queue_name.into();
        self
    }

    /// Set the broker heartbeat. The default value depends on the broker implementation.
    pub fn heartbeat(mut self, heartbeat: Option<u16>) -> Self {
        self.config.broker_builder = self.config.broker_builder.heartbeat(heartbeat);
        self
    }

    /// Add a routing rule.
    pub fn task_route(mut self, pattern: &str, queue: &str) -> Self {
        self.config.task_routes.push((pattern.into(), queue.into()));
        self
    }

    /// Set a timeout in seconds before giving up establishing a connection to a broker.
    pub fn broker_connection_timeout(mut self, timeout: u32) -> Self {
        self.config.broker_connection_timeout = timeout;
        self
    }

    /// Set whether or not to automatically try to re-establish connection to the AMQP broker.
    pub fn broker_connection_retry(mut self, retry: bool) -> Self {
        self.config.broker_connection_retry = retry;
        self
    }

    /// Set the maximum number of retries before we give up trying to re-establish connection
    /// to the AMQP broker.
    pub fn broker_connection_max_retries(mut self, max_retries: u32) -> Self {
        self.config.broker_connection_max_retries = max_retries;
        self
    }

    /// Set the number of seconds to wait before re-trying the connection with the broker.
    pub fn broker_connection_retry_delay(mut self, retry_delay: u32) -> Self {
        self.config.broker_connection_retry_delay = retry_delay;
        self
    }

    /// Set a default content type of the message body serialization.
    pub fn task_content_type(mut self, content_type: MessageContentType) -> Self {
        self.config.task_options.content_type = Some(content_type);
        self
    }

    /// Construct a `Beat` app with the current configuration.
    pub async fn build(self) -> Result<Beat<Bb::Broker, Sb>, CeleryError> {
        // Declare default queue to broker.
        let broker_builder = self
            .config
            .broker_builder
            .declare_queue(Queue::new((*self.config.default_queue).to_string()));

        let (broker_builder, task_routes) =
            configure_task_routes(broker_builder, &self.config.task_routes)?;

        let broker = build_and_connect(
            broker_builder,
            self.config.broker_connection_timeout,
            if self.config.broker_connection_retry {
                self.config.broker_connection_max_retries
            } else {
                0
            },
            self.config.broker_connection_retry_delay,
        )
        .await?;

        let scheduler = Scheduler::new(broker);

        Ok(Beat {
            name: self.config.name,
            scheduler,
            scheduler_backend: self.scheduler_backend,
            task_routes,
            default_queue: self.config.default_queue,
            task_options: self.config.task_options,
            broker_connection_timeout: self.config.broker_connection_timeout,
            broker_connection_retry: self.config.broker_connection_retry,
            broker_connection_max_retries: self.config.broker_connection_max_retries,
            broker_connection_retry_delay: self.config.broker_connection_retry_delay,
        })
    }
}

/// A `Beat` app is used to send out scheduled tasks. This is the struct that is
/// created with the [`beat`](../macro.beat.html) macro.
///
/// It drives execution by making the internal scheduler "tick", and updates the list of scheduled
/// tasks through a customizable scheduler backend.
pub struct Beat<Br: Broker, Sb: SchedulerBackend> {
    pub name: String,
    scheduler: Scheduler<Br>,
    scheduler_backend: Sb,
    task_routes: Vec<Rule>,
    default_queue: String,
    task_options: TaskOptions,

    broker_connection_timeout: u32,
    broker_connection_retry: bool,
    broker_connection_max_retries: u32,
    broker_connection_retry_delay: u32,
}

impl<Br> Beat<Br, LocalSchedulerBackend>
where
    Br: Broker,
{
    /// Get a `BeatBuilder` for creating a `Beat` app with a custom configuration and a
    /// default scheduler backend.
    pub fn default_builder(
        name: &str,
        broker_url: &str,
    ) -> BeatBuilder<Br::Builder, LocalSchedulerBackend> {
        BeatBuilder::<Br::Builder, LocalSchedulerBackend>::with_default_scheduler_backend(
            name, broker_url,
        )
    }
}

impl<Br, Sb> Beat<Br, Sb>
where
    Br: Broker,
    Sb: SchedulerBackend,
{
    /// Get a `BeatBuilder` for creating a `Beat` app with a custom configuration and
    /// a custom scheduler backend.
    pub fn custom_builder(
        name: &str,
        broker_url: &str,
        scheduler_backend: Sb,
    ) -> BeatBuilder<Br::Builder, Sb> {
        BeatBuilder::<Br::Builder, Sb>::with_custom_scheduler_backend(
            name,
            broker_url,
            scheduler_backend,
        )
    }

    /// Schedule the execution of a task.
    pub fn schedule_task<T, S>(&mut self, mut signature: Signature<T>, schedule: S)
    where
        T: Task + Clone + 'static,
        S: Schedule + 'static,
    {
        signature.options.update(&self.task_options);
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

    /// Start the *beat*.
    pub async fn start(&mut self) -> Result<(), BeatError> {
        info!("Starting beat service");
        loop {
            let result = self.beat_loop().await;
            if !self.broker_connection_retry {
                return result;
            }

            if let Err(err) = result {
                match err {
                    BeatError::BrokerError(broker_err) => {
                        if broker_err.is_connection_error() {
                            error!("Broker connection failed");
                        } else {
                            return Err(BeatError::BrokerError(broker_err));
                        }
                    }
                    _ => return Err(err),
                };
            } else {
                return result;
            }

            let mut reconnect_successful: bool = false;
            for _ in 0..self.broker_connection_max_retries {
                info!("Trying to re-establish connection with broker");
                time::delay_for(Duration::from_secs(
                    self.broker_connection_retry_delay as u64,
                ))
                .await;

                match self
                    .scheduler
                    .broker
                    .reconnect(self.broker_connection_timeout)
                    .await
                {
                    Err(err) => {
                        if err.is_connection_error() {
                            continue;
                        }
                        return Err(BeatError::BrokerError(err));
                    }
                    Ok(_) => {
                        info!("Successfully reconnected with broker");
                        reconnect_successful = true;
                        break;
                    }
                };
            }

            if !reconnect_successful {
                return Err(BeatError::BrokerError(BrokerError::NotConnected));
            }
        }
    }

    async fn beat_loop(&mut self) -> Result<(), BeatError> {
        loop {
            let next_tick_at = self.scheduler.tick().await?;

            if self.scheduler_backend.should_sync() {
                self.scheduler_backend
                    .sync(self.scheduler.get_scheduled_tasks())?;
            }

            let now = SystemTime::now();
            if now < next_tick_at {
                let sleep_interval = next_tick_at.duration_since(now).expect(
                    "Unexpected error when unwrapping a SystemTime comparison that is not supposed to fail",
                );
                debug!("Now sleeping for {:?}", sleep_interval);
                time::delay_for(sleep_interval).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;

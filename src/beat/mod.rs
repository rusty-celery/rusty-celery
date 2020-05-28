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

use crate::broker::{build_and_connect, configure_task_routes, Broker, BrokerBuilder};
use crate::routing::{self, Rule};
use crate::{
    error::CeleryError,
    task::{Signature, Task},
};
use log::{debug, info};
use std::time::SystemTime;
use tokio::time;

mod scheduler;
use scheduler::Scheduler;

mod backend;
pub use backend::{InMemoryBackend, SchedulerBackend};

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
    default_queue: String,
    task_routes: Vec<(String, String)>,
}

pub struct BeatBuilder<Bb, Sb>
where
    Bb: BrokerBuilder,
    Sb: SchedulerBackend,
{
    config: Config<Bb>,
    scheduler_backend: Sb,
}

impl<Bb> BeatBuilder<Bb, InMemoryBackend>
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
                broker_connection_max_retries: 100,
                default_queue: "celery".into(),
                task_routes: vec![],
            },
            scheduler_backend: InMemoryBackend::new(),
        }
    }
}

impl<Bb, Sb> BeatBuilder<Bb, Sb>
where
    Bb: BrokerBuilder,
    Sb: SchedulerBackend,
{
    /// Get a `BeatBuilder` for creating a `Beat` app with custom scheduler backend and
    /// configuration.
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
                broker_connection_max_retries: 100,
                default_queue: "celery".into(),
                task_routes: vec![],
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

    /// Construct a `Beat` app with the current configuration.
    pub async fn build(self) -> Result<Beat<Bb::Broker, Sb>, CeleryError> {
        // Declare default queue to broker.
        let broker_builder = self
            .config
            .broker_builder
            .declare_queue(&self.config.default_queue);

        let (broker_builder, task_routes) =
            configure_task_routes(broker_builder, &self.config.task_routes)?;

        let broker = build_and_connect(
            broker_builder,
            self.config.broker_connection_timeout,
            self.config.broker_connection_retry,
            self.config.broker_connection_max_retries,
        )
        .await?;

        let scheduler = Scheduler::new(broker);

        Ok(Beat {
            name: self.config.name,
            scheduler,
            scheduler_backend: self.scheduler_backend,
            task_routes,
            default_queue: self.config.default_queue,
        })
    }
}

/// The beat service is in charge of executing scheduled tasks when
/// they are due and add or remove tasks as required. It drives execution by
/// making the internal scheduler "tick", and updates the list of scheduled
/// tasks through a customizable scheduler backend.
pub struct Beat<Br: Broker, Sb: SchedulerBackend> {
    pub name: String,
    scheduler: Scheduler<Br>,
    scheduler_backend: Sb,
    task_routes: Vec<Rule>,
    default_queue: String,
}

impl<Br> Beat<Br, InMemoryBackend>
where
    Br: Broker,
{
    /// Get a `BeatBuilder` for creating a `Beat` app with a custom configuration and a
    /// default scheduler backend.
    pub fn default_builder(
        name: &str,
        broker_url: &str,
    ) -> BeatBuilder<Br::Builder, InMemoryBackend> {
        BeatBuilder::<Br::Builder, InMemoryBackend>::with_default_scheduler_backend(
            name, broker_url,
        )
    }
}

impl<Br, Sb> Beat<Br, Sb>
where
    Br: Broker,
    Sb: SchedulerBackend,
{
    /// Get a `BeatBuilder` for creating a `Beat` app with custom configuration and scheduler backend.
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

use futures::StreamExt;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::RwLock;
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::{self, UnboundedSender};

mod routing;
mod trace;

use crate::broker::{Broker, BrokerBuilder};
use crate::error::{Error, ErrorKind};
use crate::protocol::{Message, TryIntoMessage};
use crate::task::{Task, TaskEvent, TaskOptions, TaskSendOptions, TaskStatus};
pub use routing::Rule;
use trace::{build_tracer, TraceBuilder, TracerTrait};

struct Config<Bb>
where
    Bb: BrokerBuilder,
{
    name: String,
    broker_builder: Bb,
    default_queue: String,
    task_options: TaskOptions,
    task_routes: Vec<Rule>,
}

/// Used to create a `Celery` app with a custom configuration.
pub struct CeleryBuilder<Bb>
where
    Bb: BrokerBuilder,
{
    config: Config<Bb>,
}

impl<Bb> CeleryBuilder<Bb>
where
    Bb: BrokerBuilder,
{
    /// Get a `CeleryBuilder` for creating a `Celery` app with a custom configuration.
    pub fn new(name: &str, broker_url: &str) -> Self {
        Self {
            config: Config {
                name: name.into(),
                broker_builder: Bb::new(broker_url),
                default_queue: "celery".into(),
                task_options: TaskOptions {
                    timeout: None,
                    max_retries: None,
                    min_retry_delay: 0,
                    max_retry_delay: 3600,
                },
                task_routes: vec![],
            },
        }
    }

    /// Set the name of the default queue.
    pub fn default_queue(mut self, queue_name: &str) -> Self {
        self.config.default_queue = queue_name.into();
        self
    }

    /// Set the prefetch count.
    pub fn prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.config.broker_builder = self.config.broker_builder.prefetch_count(prefetch_count);
        self
    }

    /// Set the broker heartbeat.
    pub fn heartbeat(mut self, heartbeat: Option<u16>) -> Self {
        self.config.broker_builder = self.config.broker_builder.heartbeat(heartbeat);
        self
    }

    /// Set a default timeout for tasks.
    pub fn task_timeout(mut self, task_timeout: u32) -> Self {
        self.config.task_options.timeout = Some(task_timeout);
        self
    }

    /// Set a default maximum number of retries for tasks.
    pub fn task_max_retries(mut self, task_max_retries: u32) -> Self {
        self.config.task_options.max_retries = Some(task_max_retries);
        self
    }

    /// Set a default minimum retry delay for tasks.
    pub fn task_min_retry_delay(mut self, task_min_retry_delay: u32) -> Self {
        self.config.task_options.min_retry_delay = task_min_retry_delay;
        self
    }

    /// Set a default maximum retry delay for tasks.
    pub fn task_max_retry_delay(mut self, task_max_retry_delay: u32) -> Self {
        self.config.task_options.max_retry_delay = task_max_retry_delay;
        self
    }

    /// Add a routing rule.
    pub fn task_route(mut self, rule: Rule) -> Self {
        self.config.task_routes.push(rule);
        self
    }

    /// Construct a `Celery` app with the current configuration .
    pub fn build(self) -> Result<Celery<Bb::Broker>, Error> {
        // Declare default queue to broker.
        let mut broker_builder = self
            .config
            .broker_builder
            .declare_queue(&self.config.default_queue);

        // Ensure all other queues mentioned in task_routes are declared to the broker.
        for rule in &self.config.task_routes {
            broker_builder = broker_builder.declare_queue(&rule.queue);
        }

        Ok(Celery {
            name: self.config.name,
            broker: broker_builder.build()?,
            default_queue: self.config.default_queue,
            task_trace_builders: RwLock::new(HashMap::new()),
            task_options: self.config.task_options,
            task_routes: self.config.task_routes,
        })
    }
}

/// A `Celery` app is used to produce or consume tasks asyncronously.
pub struct Celery<B: Broker> {
    /// An arbitrary, human-readable name for the app.
    pub name: String,

    /// The app's broker.
    pub broker: B,

    /// The default queue to send and receive from.
    pub default_queue: String,

    /// Default task options.
    pub task_options: TaskOptions,

    /// Mapping of task name to task tracer factory. Used to create a task tracer
    /// from an incoming message.
    task_trace_builders: RwLock<HashMap<String, TraceBuilder>>,

    /// A vector of routing rules in the order of their importance.
    task_routes: Vec<Rule>,
}

impl<B> Celery<B>
where
    B: Broker + 'static,
{
    /// Get a `CeleryBuilder` for creating a `Celery` app with a custom configuration.
    pub fn builder(name: &str, broker_url: &str) -> CeleryBuilder<B::Builder> {
        CeleryBuilder::<B::Builder>::new(name, broker_url)
    }

    /// Send a task to a remote worker with default options. Returns the correlation ID
    /// of the task if successful.
    pub async fn send_task<T: Task>(&self, task: T) -> Result<String, Error> {
        let queue = routing::route(T::NAME, &self.task_routes).unwrap_or(&self.default_queue);
        let options = TaskSendOptions::builder().queue(queue).build();
        self.send_task_with(task, &options).await
    }

    /// Send a task to a remote worker with custom options. Returns the correlation ID
    /// of the task if successful.
    pub async fn send_task_with<T: Task>(
        &self,
        task: T,
        options: &TaskSendOptions,
    ) -> Result<String, Error> {
        let message = Message::builder(task)?.task_send_options(options).build();
        debug!("Sending message {:?}", message);
        let queue = options.queue.as_ref().unwrap_or(&self.default_queue);
        self.broker.send(&message, queue).await?;
        Ok(message.properties.correlation_id)
    }

    /// Register a task.
    pub fn register_task<T: Task + 'static>(&self) -> Result<(), Error> {
        let mut task_trace_builders = self
            .task_trace_builders
            .write()
            .map_err(|_| Error::from(ErrorKind::SyncError))?;
        if task_trace_builders.contains_key(T::NAME) {
            Err(ErrorKind::TaskAlreadyExists(T::NAME.into()).into())
        } else {
            task_trace_builders.insert(T::NAME.into(), Box::new(build_tracer::<T>));
            info!("Registered task {}", T::NAME);
            Ok(())
        }
    }

    fn get_task_tracer(
        &self,
        message: Message,
        event_tx: UnboundedSender<TaskEvent>,
    ) -> Result<Box<dyn TracerTrait>, Error> {
        let task_trace_builders = self
            .task_trace_builders
            .read()
            .map_err(|_| Error::from(ErrorKind::SyncError))?;
        if let Some(build_tracer) = task_trace_builders.get(&message.headers.task) {
            Ok(build_tracer(message, self.task_options, event_tx)?)
        } else {
            Err(ErrorKind::UnregisteredTaskError(message.headers.task).into())
        }
    }

    /// Tries converting a delivery into a `Message`, executing the corresponding task,
    /// and communicating with the broker.
    async fn try_handle_delivery(
        &self,
        delivery_result: Result<B::Delivery, B::DeliveryError>,
        event_tx: UnboundedSender<TaskEvent>,
    ) -> Result<(), Error> {
        let delivery = delivery_result.map_err(|e| e.into())?;
        debug!("Received delivery: {:?}", delivery);

        // Coerce the delivery into a protocol message.
        let message = match delivery.try_into_message() {
            Ok(message) => message,
            Err(e) => {
                // This is a naughty message that we can't handle, so we'll ack it with
                // the broker so it gets deleted.
                self.broker.ack(delivery).await?;
                return Err(e);
            }
        };

        // Try deserializing the message to create a task wrapped in a task tracer.
        // (The tracer handles all of the logic of directly interacting with the task
        // to execute it and handle the post-execution functions).
        let mut tracer = match self.get_task_tracer(message, event_tx) {
            Ok(tracer) => tracer,
            Err(e) => {
                // Even though the message meta data was okay, we failed to deserialize
                // the body of the message for some reason, so ack it with the broker
                // to delete it and return an error.
                self.broker.ack(delivery).await?;
                return Err(e);
            }
        };

        if tracer.is_delayed() {
            // Task has an ETA, so we need to increment the prefetch count so that
            // we can receive other tasks while we wait for the ETA.
            if let Err(e) = self.broker.increase_prefetch_count().await {
                // If for some reason this operation fails, we should stop tracing
                // this task and send it back to the broker to retry.
                // Otherwise we could reach the prefetch_count and end up blocking
                // other deliveries if there are a high number of messages with a
                // future ETA.
                self.broker.retry(delivery, None).await?;
                return Err(e);
            };
        }

        // Try tracing the task now.
        // NOTE: we don't need to log errors from the trace here since the tracer
        // handles all errors at it's own level or the task level. In this function
        // we only log errors at the broker and delivery level.
        match tracer.trace().await {
            Ok(_) => {
                self.broker.ack(delivery).await?;
            }
            Err(e) => match e.kind() {
                // Retriable error -> retry the task.
                ErrorKind::Retry => {
                    let retry_eta = tracer.retry_eta();
                    self.broker.retry(delivery, retry_eta).await?
                }
                // Some other kind of error -> ack / delete the task.
                _ => self.broker.ack(delivery).await?,
            },
        };

        // If we had increased the prefetch count above due to a future ETA, we have
        // to decrease it back down to restore balance to the universe.
        if tracer.is_delayed() {
            self.broker.decrease_prefetch_count().await?;
        }

        Ok(())
    }

    /// Wraps `try_handle_delivery` to catch any and all errors that might occur.
    async fn handle_delivery(
        &self,
        delivery_result: Result<B::Delivery, B::DeliveryError>,
        event_tx: UnboundedSender<TaskEvent>,
    ) {
        if let Err(e) = self.try_handle_delivery(delivery_result, event_tx).await {
            error!("{}", e);
        }
    }

    /// Consume tasks from the default queue.
    pub async fn consume(&'static self) -> Result<(), Error> {
        Ok(self.consume_from(&self.default_queue).await?)
    }

    /// Consume tasks from a queue.
    #[allow(clippy::cognitive_complexity)]
    pub async fn consume_from(&'static self, queue: &str) -> Result<(), Error> {
        // Stream of deliveries from the queue.
        let mut deliveries = Box::pin(self.broker.consume(queue).await?);

        // Stream of OS signals.
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;

        // A sender and receiver for task related events.
        // NOTE: we can use an unbounded channel since we already have backpressure
        // from the `prefetch_count` setting.
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TaskEvent>();
        let mut pending_tasks = 0;

        // This is the main loop where we receive deliveries and pass them off
        // to be handled by spawning `self.handle_delivery`.
        // At the same time we are also listening for a SIGINT (Ctrl+C) interruption.
        // If that occurs we break from this loop and move to the warm shutdown loop
        // if there are still any pending tasks (tasks being executed, not including
        // tasks being delayed due to a future ETA).
        loop {
            select! {
                maybe_delivery_result = deliveries.next() => {
                    if let Some(delivery_result) = maybe_delivery_result {
                        let event_tx = event_tx.clone();
                        tokio::spawn(self.handle_delivery(delivery_result, event_tx));
                    }
                },
                _ = sigint.next() => {
                    warn!("Ope! Hitting Ctrl+C again will terminate all running tasks!");
                    info!("Warm shutdown...");
                    break;
                },
                _ = sigterm.next() => {
                    info!("Warm shutdown...");
                    break;
                },
                maybe_event = event_rx.next() => {
                    if let Some(event) = maybe_event {
                        debug!("Received task event {:?}", event);
                        match event.status {
                            TaskStatus::Pending => pending_tasks += 1,
                            TaskStatus::Finished => pending_tasks -= 1,
                        };
                    }
                },
            };
        }

        if pending_tasks > 0 {
            // Warm shutdown loop. When there are still pendings tasks we wait for them
            // to finish. We get updates about pending tasks through the `event_rx` channel.
            // We also watch for a second SIGINT, in which case we immediately shutdown.
            info!("Waiting on {} pending tasks...", pending_tasks);
            loop {
                select! {
                    _ = sigint.next() => {
                        warn!("Okay fine, shutting down now. See ya!");
                        return Err(ErrorKind::ForcedShutdown.into());
                    },
                    maybe_event = event_rx.next() => {
                        if let Some(event) = maybe_event {
                            debug!("Received task event {:?}", event);
                            match event.status {
                                TaskStatus::Pending => pending_tasks += 1,
                                TaskStatus::Finished => pending_tasks -= 1,
                            };
                            if pending_tasks <= 0 {
                                break;
                            }
                        }
                    },
                };
            }
        }

        info!("No more pending tasks. See ya!");

        Ok(())
    }
}

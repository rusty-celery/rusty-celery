use failure::Fail;
use futures::stream::StreamExt;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::convert::TryFrom;
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::RwLock;
use tokio::time::{self, Duration};

mod routing;
mod trace;

use crate::broker::{Broker, BrokerBuilder};
use crate::error::{BrokerError, CeleryError, TraceError};
use crate::protocol::{Message, TryCreateMessage};
use crate::task::{Signature, Task, TaskEvent, TaskOptions, TaskStatus};
use routing::Rule;
use trace::{build_tracer, TraceBuilder, TracerTrait};

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
                broker_connection_timeout: 2,
                broker_connection_retry: true,
                broker_connection_max_retries: 100,
                default_queue: "celery".into(),
                task_options: TaskOptions {
                    timeout: None,
                    max_retries: None,
                    min_retry_delay: Some(0),
                    max_retry_delay: Some(3600),
                    acks_late: Some(false),
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
        self.config.task_options.min_retry_delay = Some(task_min_retry_delay);
        self
    }

    /// Set a default maximum retry delay for tasks.
    pub fn task_max_retry_delay(mut self, task_max_retry_delay: u32) -> Self {
        self.config.task_options.max_retry_delay = Some(task_max_retry_delay);
        self
    }

    /// Set whether by default a task is acknowledged before or after execution.
    pub fn acks_late(mut self, acks_late: bool) -> Self {
        self.config.task_options.acks_late = Some(acks_late);
        self
    }

    /// Add a routing rule.
    pub fn task_route(mut self, pattern: &str, queue: &str) -> Result<Self, CeleryError> {
        let rule = Rule::new(pattern, queue)?;
        self.config.task_routes.push(rule);
        Ok(self)
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

    /// Construct a `Celery` app with the current configuration.
    pub async fn build(self) -> Result<Celery<Bb::Broker>, CeleryError> {
        // Declare default queue to broker.
        let mut broker_builder = self
            .config
            .broker_builder
            .declare_queue(&self.config.default_queue);

        // Ensure all other queues mentioned in task_routes are declared to the broker.
        for rule in &self.config.task_routes {
            broker_builder = broker_builder.declare_queue(&rule.queue);
        }

        // Try building / connecting to broker.
        let mut broker: Option<Bb::Broker> = None;
        let max_retries = if self.config.broker_connection_retry {
            self.config.broker_connection_max_retries
        } else {
            0
        };
        for i in 0..=max_retries {
            match time::timeout(
                Duration::from_secs(self.config.broker_connection_timeout as u64),
                broker_builder.build(),
            )
            .await
            .map_err(|_| BrokerError::ConnectTimeout)
            .and_then(|res| res)
            {
                Err(err) => match err {
                    BrokerError::ConnectTimeout
                    | BrokerError::ConnectionRefused
                    | BrokerError::IoError
                    | BrokerError::NotConnected => {
                        if i < max_retries {
                            error!("Failed to establish connection with broker, trying again in 200ms...")
                        }
                        time::delay_for(Duration::from_millis(200)).await;
                        continue;
                    }
                    _ => return Err(err.into()),
                },
                Ok(b) => {
                    broker = Some(b);
                    break;
                }
            };
        }

        Ok(Celery {
            name: self.config.name,
            broker: broker.ok_or_else(|| BrokerError::NotConnected)?,
            default_queue: self.config.default_queue,
            task_options: self.config.task_options,
            task_routes: self.config.task_routes,
            task_trace_builders: RwLock::new(HashMap::new()),
        })
    }
}

/// A `Celery` app is used to produce or consume tasks asynchronously. This is the struct that is
/// created with the [`app`](macro.app.html) macro.
pub struct Celery<B: Broker> {
    /// An arbitrary, human-readable name for the app.
    pub name: String,

    /// The app's broker.
    broker: B,

    /// The default queue to send and receive from.
    default_queue: String,

    /// Default task options.
    task_options: TaskOptions,

    /// A vector of routing rules in the order of their importance.
    task_routes: Vec<Rule>,

    /// Mapping of task name to task tracer factory. Used to create a task tracer
    /// from an incoming message.
    task_trace_builders: RwLock<HashMap<String, TraceBuilder>>,
}

impl<B> Celery<B>
where
    B: Broker,
{
    /// Get a `CeleryBuilder` for creating a `Celery` app with a custom configuration.
    pub fn builder(name: &str, broker_url: &str) -> CeleryBuilder<B::Builder> {
        CeleryBuilder::<B::Builder>::new(name, broker_url)
    }

    /// Send a task to a remote worker. Returns the task ID of the task if successful.
    pub async fn send_task<T: Task>(
        &self,
        mut task_sig: Signature<T>,
    ) -> Result<String, CeleryError> {
        let maybe_queue = task_sig.queue.take();
        let queue = maybe_queue.as_ref().map(|s| s.as_str()).unwrap_or_else(|| {
            routing::route(T::NAME, &self.task_routes).unwrap_or(&self.default_queue)
        });
        let message = Message::try_from(task_sig)?;
        info!(
            "Sending task {}[{}] to {}",
            T::NAME,
            message.task_id(),
            queue,
        );
        self.broker.send(&message, queue).await?;
        Ok(message.task_id().into())
    }

    /// Register a task.
    pub async fn register_task<T: Task + 'static>(&self) -> Result<(), CeleryError> {
        let mut task_trace_builders = self.task_trace_builders.write().await;
        if task_trace_builders.contains_key(T::NAME) {
            Err(CeleryError::TaskRegistrationError(T::NAME.into()))
        } else {
            task_trace_builders.insert(T::NAME.into(), Box::new(build_tracer::<T>));
            info!("Registered task {}", T::NAME);
            Ok(())
        }
    }

    async fn get_task_tracer(
        &self,
        message: Message,
        event_tx: UnboundedSender<TaskEvent>,
    ) -> Result<Box<dyn TracerTrait>, Box<dyn Fail>> {
        let task_trace_builders = self.task_trace_builders.read().await;
        if let Some(build_tracer) = task_trace_builders.get(&message.headers.task) {
            Ok(build_tracer(message, self.task_options, event_tx)
                .map_err(|e| Box::new(e) as Box<dyn Fail>)?)
        } else {
            Err(Box::new(CeleryError::UnregisteredTaskError(message.headers.task)) as Box<dyn Fail>)
        }
    }

    /// Tries converting a delivery into a `Message`, executing the corresponding task,
    /// and communicating with the broker.
    async fn try_handle_delivery(
        &self,
        delivery: B::Delivery,
        event_tx: UnboundedSender<TaskEvent>,
    ) -> Result<(), Box<dyn Fail>> {
        // Coerce the delivery into a protocol message.
        let message = match delivery.try_create_message() {
            Ok(message) => message,
            Err(e) => {
                // This is a naughty message that we can't handle, so we'll ack it with
                // the broker so it gets deleted.
                self.broker
                    .ack(&delivery)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Fail>)?;
                return Err(Box::new(e));
            }
        };

        // Try deserializing the message to create a task wrapped in a task tracer.
        // (The tracer handles all of the logic of directly interacting with the task
        // to execute it and run the post-execution functions).
        let mut tracer = match self.get_task_tracer(message, event_tx).await {
            Ok(tracer) => tracer,
            Err(e) => {
                // Even though the message meta data was okay, we failed to deserialize
                // the body of the message for some reason, so ack it with the broker
                // to delete it and return an error.
                self.broker
                    .ack(&delivery)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Fail>)?;
                return Err(Box::new(e));
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
                self.broker
                    .retry(&delivery, None)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Fail>)?;
                self.broker
                    .ack(&delivery)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Fail>)?;
                return Err(Box::new(e));
            };

            // Then wait for the task to be ready.
            tracer.wait().await;
        }

        // If acks_late is false, we acknowledge the message before tracing it.
        if !tracer.get_task_options().acks_late.unwrap_or_default() {
            self.broker
                .ack(&delivery)
                .await
                .map_err(|e| Box::new(e) as Box<dyn Fail>)?;
        }

        // Try tracing the task now.
        // NOTE: we don't need to log errors from the trace here since the tracer
        // handles all errors at it's own level or the task level. In this function
        // we only log errors at the broker and delivery level.
        if let Err(e) = tracer.trace().await {
            // If retry error -> retry the task.
            if let TraceError::Retry(retry_eta) = e {
                self.broker
                    .retry(&delivery, retry_eta)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Fail>)?;
            }
        }

        // If we have not done it before, we have to acknowledge the message now.
        if tracer.get_task_options().acks_late.unwrap_or_default() {
            self.broker
                .ack(&delivery)
                .await
                .map_err(|e| Box::new(e) as Box<dyn Fail>)?;
        }

        // If we had increased the prefetch count above due to a future ETA, we have
        // to decrease it back down to restore balance to the universe.
        if tracer.is_delayed() {
            self.broker
                .decrease_prefetch_count()
                .await
                .map_err(|e| Box::new(e) as Box<dyn Fail>)?;
        }

        Ok(())
    }

    /// Wraps `try_handle_delivery` to catch any and all errors that might occur.
    async fn handle_delivery(
        &self,
        delivery_result: Result<B::Delivery, B::DeliveryError>,
        event_tx: UnboundedSender<TaskEvent>,
    ) {
        match delivery_result {
            Ok(delivery) => {
                debug!("Received delivery: {:?}", delivery);
                if let Err(e) = self.try_handle_delivery(delivery, event_tx).await {
                    error!("{}", e);
                };
            }
            Err(e) => {
                error!("Deliver failed: {}", e);
            }
        };
    }

    /// Close channels and connections.
    pub async fn close(&self) -> Result<(), CeleryError> {
        Ok(self.broker.close().await?)
    }
}

impl<B> Celery<B>
where
    B: Broker + 'static,
{
    /// Consume tasks from the default queue.
    pub async fn consume(&'static self) -> Result<(), CeleryError> {
        Ok(self.consume_from(&self.default_queue).await?)
    }

    /// Consume tasks from a queue.
    #[allow(clippy::cognitive_complexity)]
    pub async fn consume_from(&'static self, queue: &str) -> Result<(), CeleryError> {
        info!("Consuming from {}", queue);

        // Stream of errors from broker. The capacity here is arbitrary because a single
        // error from the broker should trigger this method to return early.
        let (broker_error_tx, mut broker_error_rx) = mpsc::channel::<()>(100);

        // Stream of deliveries from the queue.
        let mut deliveries = Box::pin(
            self.broker
                .consume(
                    queue,
                    Box::new(move || {
                        if broker_error_tx.clone().try_send(()).is_err() {
                            error!("Failed to send broker error event");
                        };
                    }),
                )
                .await?,
        );

        // Stream of OS signals.
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;

        // A sender and receiver for task related events.
        // NOTE: we can use an unbounded channel since we already have backpressure
        // from the `prefetch_count` setting.
        let (task_event_tx, mut task_event_rx) = mpsc::unbounded_channel::<TaskEvent>();
        let mut pending_tasks = 0;

        // This is the main loop where we receive deliveries and pass them off
        // to be handled by spawning `self.handle_delivery`.
        // At the same time we are also listening for a SIGINT (Ctrl+C) or SIGTERM interruption.
        // If that occurs we break from this loop and move to the warm shutdown loop
        // if there are still any pending tasks (tasks being executed, not including
        // tasks being delayed due to a future ETA).
        loop {
            select! {
                maybe_delivery_result = deliveries.next() => {
                    if let Some(delivery_result) = maybe_delivery_result {
                        let task_event_tx = task_event_tx.clone();
                        tokio::spawn(self.handle_delivery(delivery_result, task_event_tx));
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
                maybe_task_event = task_event_rx.next() => {
                    if let Some(event) = maybe_task_event {
                        debug!("Received task event {:?}", event);
                        match event {
                            TaskEvent::StatusChange(TaskStatus::Pending) => pending_tasks += 1,
                            TaskEvent::StatusChange(TaskStatus::Finished) => pending_tasks -= 1,
                        };
                    }
                },
                maybe_broker_error = broker_error_rx.next() => {
                    if maybe_broker_error.is_some() {
                        error!("Broker lost connection");
                        return Err(BrokerError::NotConnected.into());
                    }
                }
            };
        }

        if pending_tasks > 0 {
            // Warm shutdown loop. When there are still pendings tasks we wait for them
            // to finish. We get updates about pending tasks through the `task_event_rx` channel.
            // We also watch for a second SIGINT or SIGTERM, in which case we immediately shutdown.
            info!("Waiting on {} pending tasks...", pending_tasks);
            loop {
                select! {
                    _ = sigint.next() => {
                        warn!("Okay fine, shutting down now. See ya!");
                        return Err(CeleryError::ForcedShutdown);
                    },
                    maybe_event = task_event_rx.next() => {
                        if let Some(event) = maybe_event {
                            debug!("Received task event {:?}", event);
                            match event {
                                TaskEvent::StatusChange(TaskStatus::Pending) => pending_tasks += 1,
                                TaskEvent::StatusChange(TaskStatus::Finished) => pending_tasks -= 1,
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

use futures::{select, StreamExt};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::RwLock;
use tokio::signal::unix::{signal, SignalKind};

mod trace;

use crate::protocol::{Message, MessageBody, TryIntoMessage};
use crate::{Broker, Error, ErrorKind, Task};
use trace::{build_tracer, TraceBuilder, TracerTrait};
struct Config<B>
where
    B: Broker + 'static,
{
    name: String,
    broker: B,
    default_queue_name: String,
    task_options: TaskOptions,
}

#[derive(Copy, Clone, Default)]
pub(crate) struct TaskOptions {
    timeout: Option<usize>,
    max_retries: Option<usize>,
    min_retry_delay: usize,
    max_retry_delay: usize,
}

impl TaskOptions {
    fn overrides<T: Task>(&self, task: &T) -> Self {
        Self {
            timeout: task.timeout().or(self.timeout),
            max_retries: task.max_retries().or(self.max_retries),
            min_retry_delay: task.min_retry_delay().unwrap_or(self.min_retry_delay),
            max_retry_delay: task.max_retry_delay().unwrap_or(self.max_retry_delay),
        }
    }
}

/// Used to create a `Celery` app with a custom configuration.
pub struct CeleryBuilder<B>
where
    B: Broker + 'static,
{
    config: Config<B>,
}

impl<B> CeleryBuilder<B>
where
    B: Broker + 'static,
{
    /// Get a `CeleryBuilder` for creating a `Celery` app with a custom configuration.
    fn new(name: &str, broker: B) -> Self {
        Self {
            config: Config {
                name: name.into(),
                broker,
                default_queue_name: "celery".into(),
                task_options: TaskOptions {
                    timeout: None,
                    max_retries: None,
                    min_retry_delay: 0,
                    max_retry_delay: 3600,
                },
            },
        }
    }

    /// Set the name of the default queue.
    pub fn default_queue_name(mut self, queue_name: &str) -> Self {
        self.config.default_queue_name = queue_name.into();
        self
    }

    /// Set a default timeout for tasks.
    pub fn task_timeout(mut self, task_timeout: usize) -> Self {
        self.config.task_options.timeout = Some(task_timeout);
        self
    }

    /// Set a default maximum number of retries for tasks.
    pub fn task_max_retries(mut self, task_max_retries: usize) -> Self {
        self.config.task_options.max_retries = Some(task_max_retries);
        self
    }

    /// Set a default minimum retry delay for tasks.
    pub fn task_min_retry_delay(mut self, task_min_retry_delay: usize) -> Self {
        self.config.task_options.min_retry_delay = task_min_retry_delay;
        self
    }

    /// Set a default maximum retry delay for tasks.
    pub fn task_max_retry_delay(mut self, task_max_retry_delay: usize) -> Self {
        self.config.task_options.max_retry_delay = task_max_retry_delay;
        self
    }

    /// Construct a `Celery` app with the current configuration .
    pub fn build(self) -> Celery<B> {
        Celery {
            name: self.config.name,
            broker: self.config.broker,
            default_queue_name: self.config.default_queue_name,
            task_trace_builders: RwLock::new(HashMap::new()),
            task_options: self.config.task_options,
        }
    }
}

/// A `Celery` app is used to produce or consume tasks asyncronously.
pub struct Celery<B: Broker> {
    pub name: String,
    pub broker: B,
    pub default_queue_name: String,
    task_trace_builders: RwLock<HashMap<String, TraceBuilder>>,
    task_options: TaskOptions,
}

impl<B> Celery<B>
where
    B: Broker + 'static,
{
    /// Get a `CeleryBuilder` for creating a `Celery` app with a custom configuration.
    pub fn builder(name: &str, broker: B) -> CeleryBuilder<B> {
        CeleryBuilder::new(name, broker)
    }

    /// Create a new `Celery` app with the given name and broker.
    pub fn new(name: &str, broker: B) -> Self {
        Self::builder(name, broker).build()
    }

    /// Send a task to a remote worker.
    pub async fn send_task<T: Task>(&self, task: T, queue: &str) -> Result<(), Error> {
        let body = MessageBody::new(task);
        let data = serde_json::to_vec(&body)?;
        let message = Message::builder(T::NAME, data).build();
        debug!("Sending message {:?}", message);
        self.broker.send(&message, queue).await
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

    fn get_task_tracer(&self, message: Message) -> Result<Box<dyn TracerTrait>, Error> {
        let task_trace_builders = self
            .task_trace_builders
            .read()
            .map_err(|_| Error::from(ErrorKind::SyncError))?;
        if let Some(build_tracer) = task_trace_builders.get(&message.headers.task) {
            Ok(build_tracer(message, self.task_options)?)
        } else {
            Err(ErrorKind::UnregisteredTaskError(message.headers.task).into())
        }
    }

    /// Trie converting a delivery into a `Message`, executing the corresponding task,
    /// and communicating with the broker.
    async fn try_handle_delivery(
        &self,
        delivery_result: Result<B::Delivery, B::DeliveryError>,
    ) -> Result<(), Error> {
        let delivery = delivery_result.map_err(|e| e.into())?;
        debug!("Received delivery: {:?}", delivery);

        let message = match delivery.try_into_message() {
            Ok(message) => message,
            Err(e) => {
                self.broker.ack(delivery).await?;
                return Err(e);
            }
        };

        let mut tracer = match self.get_task_tracer(message) {
            Ok(tracer) => tracer,
            Err(e) => {
                self.broker.ack(delivery).await?;
                return Err(e);
            }
        };

        if tracer.is_delayed() {
            // Task has an ETA, so we need to increment the prefetch count.
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

        match tracer.trace().await {
            Ok(_) => {
                self.broker.ack(delivery).await?;
            }
            Err(e) => match e.kind() {
                ErrorKind::Retry => {
                    let retry_eta = tracer.retry_eta();
                    self.broker.retry(delivery, retry_eta).await?
                }
                _ => self.broker.ack(delivery).await?,
            },
        };

        if tracer.is_delayed() {
            self.broker.decrease_prefetch_count().await?;
        }

        Ok(())
    }

    /// Wraps `try_handle_delivery` to catch any and all errors that might occur.
    async fn handle_delivery(&self, delivery_result: Result<B::Delivery, B::DeliveryError>) {
        if let Err(e) = self.try_handle_delivery(delivery_result).await {
            error!("{}", e);
        }
    }

    /// Consume tasks from a queue.
    pub async fn consume(&'static self, queue: &str) -> Result<(), Error> {
        let consumer = self.broker.consume(queue).await?;
        let mut consumer = Box::pin(consumer.fuse());
        // TODO: handle error instead of using 'unwrap'.
        let mut signal_stream = signal(SignalKind::interrupt()).unwrap().fuse();
        loop {
            select! {
                maybe_delivery_result = consumer.next() => {
                    if let Some(delivery_result) = maybe_delivery_result {
                        tokio::spawn(self.handle_delivery(delivery_result));
                    }
                },
                _ = signal_stream.next() => {
                    warn!("Received interrupt, shutting down...");
                    break;
                },
            };
        }
        Ok(())
    }
}

use futures_util::stream::StreamExt;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::RwLock;
use tokio::time::{self, Duration, Instant};

use crate::protocol::{Message, MessageBody, TryIntoMessage};
use crate::{Broker, Error, ErrorKind, Task};

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
pub struct TaskOptions {
    pub timeout: Option<usize>,
    pub max_retries: Option<usize>,
    pub min_retry_delay: usize,
    pub max_retry_delay: usize,
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
            task_executers: RwLock::new(HashMap::new()),
            task_options: self.config.task_options,
        }
    }
}

type TaskExecutionOutput = Result<(), Error>;
type TaskExecutorResult = Pin<Box<dyn Future<Output = TaskExecutionOutput>>>;
type TaskExecutor = Box<dyn Fn(Message, TaskOptions) -> TaskExecutorResult>;

/// A `Celery` app is used to produce or consume tasks asyncronously.
pub struct Celery<B: Broker> {
    pub name: String,
    pub broker: B,
    pub default_queue_name: String,
    task_executers: RwLock<HashMap<String, TaskExecutor>>,
    pub task_options: TaskOptions,
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
        let mut task_executers = self.task_executers.write().unwrap();
        if task_executers.contains_key(T::NAME) {
            Err(ErrorKind::TaskAlreadyExists(T::NAME.into()).into())
        } else {
            task_executers.insert(
                T::NAME.into(),
                Box::new(|message, options| Box::pin(Self::task_executer::<T>(message, options))),
            );
            Ok(())
        }
    }

    /// Wraps the execution of a task, catching and logging errors and then running
    /// the appropriate post-execution functions.
    async fn task_executer<T: Task + 'static>(
        message: Message,
        options: TaskOptions,
    ) -> Result<(), Error> {
        let body = MessageBody::<T>::from_raw_data(&message.raw_data)?;
        let mut task = body.1;
        let options = options.overrides(&task);
        if let Some(countdown) = message.countdown() {
            info!(
                "Task {}[{}] received, ETA: {}",
                T::NAME,
                message.properties.correlation_id,
                message.headers.eta.unwrap()
            );
            time::delay_for(countdown).await;
        } else {
            info!(
                "Task {}[{}] received",
                T::NAME,
                message.properties.correlation_id
            );
        }
        let start = Instant::now();
        let result = match options.timeout {
            Some(secs) => {
                debug!("Executing task with {} second timeout", secs);
                let duration = Duration::from_secs(secs as u64);
                time::timeout(duration, task.run()).into_inner().await
            }
            None => task.run().await,
        };
        let duration = start.elapsed();
        match result {
            Ok(returned) => {
                info!(
                    "Task {}[{}] succeeded in {}s: {:?}",
                    T::NAME,
                    message.properties.correlation_id,
                    duration.as_secs_f32(),
                    returned
                );
                task.on_success(&returned).await;
                Ok(())
            }
            Err(e) => {
                match e.kind() {
                    ErrorKind::ExpectedError(reason) => {
                        warn!(
                            "Task {}[{}] raised expected: {}",
                            T::NAME,
                            message.properties.correlation_id,
                            reason
                        );
                    }
                    ErrorKind::UnexpectedError(reason) => {
                        error!(
                            "Task {}[{}] raised unexpected: {}",
                            T::NAME,
                            message.properties.correlation_id,
                            reason
                        );
                    }
                    _ => {
                        error!(
                            "Task {}[{}] failed: {}",
                            T::NAME,
                            message.properties.correlation_id,
                            e
                        );
                    }
                };
                task.on_failure(&e).await;
                if let Some(max_retries) = options.max_retries {
                    let retries = match message.headers.retries {
                        Some(n) => n,
                        None => 0,
                    };
                    if retries >= max_retries {
                        return Err(e);
                    }
                }
                info!(
                    "Task {}[{}] retrying",
                    T::NAME,
                    message.properties.correlation_id
                );
                Err(ErrorKind::Retry.into())
            }
        }
    }

    /// Converts a delivery into a `Message`, executes the task, and communicates
    /// with the broker.
    async fn consume_delivery(
        &self,
        delivery_result: Result<B::Delivery, B::DeliveryError>,
    ) -> Result<(), Error> {
        match delivery_result {
            Ok(delivery) => {
                debug!("Received delivery: {:?}", delivery);
                let message = delivery.try_into_message()?;
                let task_executers = self.task_executers.read().unwrap();
                match task_executers.get(&message.headers.task) {
                    Some(executor) => {
                        match executor(message, self.task_options).await {
                            Ok(_) => {
                                self.broker.ack(delivery).await?;
                            }
                            Err(e) => match e.kind() {
                                ErrorKind::Retry => self.broker.retry(delivery).await?,
                                _ => self.broker.ack(delivery).await?,
                            },
                        };
                        Ok(())
                    }
                    None => {
                        self.broker.ack(delivery).await?;
                        debug!("Delivery acked");
                        Err(ErrorKind::UnregisteredTaskError(message.headers.task).into())
                    }
                }
            }
            Err(e) => {
                error!("Delivery error occurred");
                Err(e.into())
            }
        }
    }

    /// Wraps `consume_delivery` to catch any and all errors that might occur.
    async fn handle_delivery(&self, delivery_result: Result<B::Delivery, B::DeliveryError>) {
        if let Err(e) = self.consume_delivery(delivery_result).await {
            error!("{}", e);
        }
    }

    /// Consume tasks from a queue.
    pub async fn consume(&self, queue: &str) -> Result<(), Error> {
        let consumer = self.broker.consume(queue).await?;
        consumer
            .for_each_concurrent(
                None, // limit of concurrent tasks.
                |delivery_result| self.handle_delivery(delivery_result),
            )
            .await;
        Ok(())
    }
}

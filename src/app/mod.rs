use futures_util::stream::StreamExt;
use log::{debug, error};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use crate::protocol::{Message, MessageBody, TryIntoMessage};
use crate::{Broker, Error, ErrorKind, Task};

struct Config<B>
where
    B: Broker + 'static,
{
    // App level configurations.
    name: String,
    broker: B,
    default_queue_name: String,

    // Default task configurations.
    task_timeout: Option<usize>,
    task_max_retries: Option<usize>,
    task_min_retry_delay: Option<usize>,
    task_max_retry_delay: Option<usize>,
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

                task_timeout: None,
                task_max_retries: None,
                task_min_retry_delay: None,
                task_max_retry_delay: None,
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
        self.config.task_timeout = Some(task_timeout);
        self
    }

    /// Set a default maximum number of retries for tasks.
    pub fn task_max_retries(mut self, task_max_retries: usize) -> Self {
        self.config.task_max_retries = Some(task_max_retries);
        self
    }

    /// Set a default minimum retry delay for tasks.
    pub fn task_min_retry_delay(mut self, task_min_retry_delay: usize) -> Self {
        self.config.task_min_retry_delay = Some(task_min_retry_delay);
        self
    }

    /// Set a default maximum retry delay for tasks.
    pub fn task_max_retry_delay(mut self, task_max_retry_delay: usize) -> Self {
        self.config.task_max_retry_delay = Some(task_max_retry_delay);
        self
    }

    /// Construct a `Celery` app with the current configuration .
    pub fn build(self) -> Celery<B> {
        Celery {
            name: self.config.name,
            broker: self.config.broker,
            default_queue_name: self.config.default_queue_name,
            tasks: HashMap::new(),

            task_timeout: self.config.task_timeout,
            task_max_retries: self.config.task_max_retries,
            task_min_retry_delay: self.config.task_min_retry_delay,
            task_max_retry_delay: self.config.task_max_retry_delay,
        }
    }
}

type TaskExecutionOutput = Result<(), Error>;
type TaskExecutorResult = Pin<Box<dyn Future<Output = TaskExecutionOutput>>>;
type TaskExecutor = Box<dyn Fn(Vec<u8>) -> TaskExecutorResult>;

/// A `Celery` app is used to produce or consume tasks asyncronously.
pub struct Celery<B: Broker> {
    // App level configurations.
    pub name: String,
    pub broker: B,
    pub default_queue_name: String,
    tasks: HashMap<String, TaskExecutor>,

    // Default task configurations.
    pub task_timeout: Option<usize>,
    pub task_max_retries: Option<usize>,
    pub task_min_retry_delay: Option<usize>,
    pub task_max_retry_delay: Option<usize>,
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
    pub fn register_task<T: Task + 'static>(&mut self) -> Result<(), Error> {
        if self.tasks.contains_key(T::NAME) {
            Err(ErrorKind::TaskAlreadyExists(T::NAME.into()).into())
        } else {
            self.tasks.insert(
                T::NAME.into(),
                Box::new(|data| Box::pin(Self::task_executer::<T>(data))),
            );
            Ok(())
        }
    }

    async fn task_executer<T: Task + 'static>(data: Vec<u8>) -> Result<(), Error> {
        let body = MessageBody::<T>::from_raw_data(&data)?;
        let mut task = body.1;
        match task.run().await {
            Ok(returned) => {
                debug!("Task returned {:?}", returned);
                task.on_success(returned).await
            }
            Err(e) => {
                error!("Task failed {}", e);
                task.on_failure(e).await
            }
        }
    }

    async fn execute_task(&self, task_name: &str, data: Vec<u8>) -> Result<(), Error> {
        (self.tasks[task_name])(data).await
    }

    async fn consume_delivery(
        &self,
        delivery_result: Result<B::Delivery, B::DeliveryError>,
    ) -> Result<(), Error> {
        match delivery_result {
            Ok(delivery) => {
                debug!("Handling delivery {:?}", delivery);
                let message = delivery.try_into_message()?;
                debug!("Handling message {:?}", message);
                self.execute_task(&message.headers.task, message.raw_data)
                    .await?;
                debug!("Acknowledging message");
                self.broker.ack(delivery).await?;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

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

use async_trait::async_trait;
use failure::Error;
use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Queue};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use crate::protocol::TaskPayloadBody;
use crate::task::Task;

#[async_trait]
pub trait TaskFactory {
    async fn run(&self, body: Vec<u8>) -> Result<(), Error>;
}

async fn execute_task<T: Task + 'static>(body: Vec<u8>) -> Result<(), Error> {
    let payload: TaskPayloadBody<T> = serde_json::from_slice(&body).unwrap();
    let mut task = payload.1;
    match task.run().await {
        Ok(_) => task.on_success().await,
        Err(e) => task.on_failure(e).await,
    }
}

struct Config {
    // App level configurations.
    broker: Option<String>,
    default_queue_name: String,
    default_queue_options: QueueDeclareOptions,

    // Default task configurations.
    task_timeout: Option<usize>,
    task_max_retries: Option<usize>,
    task_min_retry_delay: Option<usize>,
    task_max_retry_delay: Option<usize>,
}

/// Used to create a `Celery` app with a custom configuration.
pub struct CeleryBuilder {
    config: Config,
}

impl Default for CeleryBuilder {
    fn default() -> Self {
        Self {
            config: Config {
                broker: None,
                default_queue_name: "celery".into(),
                default_queue_options: QueueDeclareOptions {
                    passive: false,
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    nowait: false,
                },

                task_timeout: None,
                task_max_retries: None,
                task_min_retry_delay: None,
                task_max_retry_delay: None,
            },
        }
    }
}

impl CeleryBuilder {
    /// Set the broker URL.
    pub fn broker(mut self, broker: String) -> Self {
        self.config.broker = Some(broker);
        self
    }

    /// Set the name of the default queue.
    pub fn default_queue_name(mut self, queue_name: String) -> Self {
        self.config.default_queue_name = queue_name;
        self
    }

    /// Set configuration options for the default queue.
    pub fn default_queue_options(mut self, options: &QueueDeclareOptions) -> Self {
        self.config.default_queue_options = options.clone();
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
    pub async fn build(self, name: String) -> Result<Celery, Error> {
        let mut conn: Option<Connection> = None;
        let mut channel: Option<Channel> = None;
        let mut default_queue: Option<Queue> = None;
        if let Some(ref broker) = self.config.broker {
            conn = Some(Connection::connect(&broker, ConnectionProperties::default()).await?);
            channel = Some(conn.as_ref().unwrap().create_channel().await?);
            default_queue = Some(
                channel
                    .as_ref()
                    .unwrap()
                    .queue_declare(
                        &self.config.default_queue_name,
                        self.config.default_queue_options.clone(),
                        FieldTable::default(),
                    )
                    .await?,
            );
        }
        Ok(Celery {
            name,
            broker: self.config.broker,
            default_queue_name: self.config.default_queue_name,
            default_queue_options: self.config.default_queue_options,

            task_timeout: self.config.task_timeout,
            task_max_retries: self.config.task_max_retries,
            task_min_retry_delay: self.config.task_min_retry_delay,
            task_max_retry_delay: self.config.task_max_retry_delay,

            conn,
            channel,
            default_queue,
            tasks: HashMap::new(),
        })
    }
}

pub struct Celery {
    // App level configurations.
    pub name: String,
    pub broker: Option<String>,
    pub default_queue_name: String,
    pub default_queue_options: QueueDeclareOptions,

    // Default task configurations.
    pub task_timeout: Option<usize>,
    pub task_max_retries: Option<usize>,
    pub task_min_retry_delay: Option<usize>,
    pub task_max_retry_delay: Option<usize>,

    pub conn: Option<Connection>,
    channel: Option<Channel>,
    pub default_queue: Option<Queue>,
    tasks: HashMap<String, Box<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Error>>>>>>,
}

impl Celery {
    /// Get a `CeleryBuilder` for creating a `Celery` app with a custom configuration.
    pub fn builder() -> CeleryBuilder {
        CeleryBuilder::default()
    }

    /// Create a new `Celery` app with the given name.
    pub async fn new(name: String) -> Result<Self, Error> {
        Self::builder().build(name).await
    }

    /// Send a task to a remote worker.
    pub async fn send_task<T: Task>(&self, task: T) -> Result<(), Error> {
        let payload = TaskPayloadBody::new(task);
        self.channel
            .as_ref()
            .unwrap()
            .basic_publish(
                "",
                &self.default_queue_name,
                BasicPublishOptions::default(),
                serde_json::to_vec(&payload)?,
                BasicProperties::default(),
            )
            .await?;
        Ok(())
    }

    pub fn register_task<T: Task + 'static>(&mut self, name: String) {
        self.tasks.insert(name, Box::new(|body| Box::pin(execute_task::<T>(body))));
    }

    pub async fn execute_task(&self, task_name: String, body: Vec<u8>) -> Result<(), Error> {
        (self.tasks[&task_name])(body).await
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Serialize, Deserialize)]
    struct TestTask {
        a: i32,
    }
}

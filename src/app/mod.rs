use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Queue};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use crate::error::{Error, ErrorKind};
use crate::protocol::TaskPayloadBody;
use crate::task::Task;

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
    pub fn broker(mut self, broker: &str) -> Self {
        self.config.broker = Some(broker.into());
        self
    }

    /// Set the name of the default queue.
    pub fn default_queue_name(mut self, queue_name: &str) -> Self {
        self.config.default_queue_name = queue_name.into();
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
    pub async fn build(self, name: &str) -> Result<Celery, Error> {
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
            name: name.into(),
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

type TaskExecutionOutput = Result<(), Error>;
type TaskExecutorResult = Pin<Box<dyn Future<Output = TaskExecutionOutput>>>;
type TaskExecutor = Box<dyn Fn(Vec<u8>) -> TaskExecutorResult>;

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
    tasks: HashMap<String, TaskExecutor>,
}

impl Celery {
    /// Get a `CeleryBuilder` for creating a `Celery` app with a custom configuration.
    pub fn builder() -> CeleryBuilder {
        CeleryBuilder::default()
    }

    /// Create a new `Celery` app with the given name.
    pub async fn new(name: &str) -> Result<Self, Error> {
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

    /// Register a task.
    pub fn register_task<T: Task + 'static>(&mut self, name: &str) -> Result<(), Error> {
        if self.tasks.contains_key(name) {
            Err(ErrorKind::TaskAlreadyExists(name.into()).into())
        } else {
            self.tasks.insert(
                name.into(),
                Box::new(|body| Box::pin(Self::task_executer::<T>(body))),
            );
            Ok(())
        }
    }

    async fn task_executer<T: Task + 'static>(body: Vec<u8>) -> Result<(), Error> {
        let payload: TaskPayloadBody<T> = serde_json::from_slice(&body).unwrap();
        let mut task = payload.1;
        match task.run().await {
            Ok(returned) => task.on_success(returned).await,
            Err(e) => task.on_failure(e).await,
        }
    }

    pub async fn execute_task(&self, task_name: &str, body: Vec<u8>) -> Result<(), Error> {
        (self.tasks[task_name])(body).await
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Serialize, Deserialize)]
    struct TestTask {
        a: i32,
    }

    #[async_trait]
    impl Task for TestTask {
        type Returns = ();

        async fn run(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_register_tasks() {
        let mut celery = Celery::new("test_app").await.unwrap();
        celery.register_task::<TestTask>("test_task").unwrap();
        let payload = TaskPayloadBody::new(TestTask { a: 0 });
        let body = serde_json::to_vec(&payload).unwrap();
        celery.execute_task("test_task", body).await.unwrap();
    }
}

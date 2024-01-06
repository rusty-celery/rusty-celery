use std::sync::Arc;

use async_trait::async_trait;
use redis::{aio::ConnectionManager, Client};

use super::{Backend, BackendBuilder, BackendError, TaskMeta};

pub struct RedisBackendBuilder {
    backend_url: String,
    #[allow(dead_code)]
    connection_timeout: Option<u32>,
    taskmeta_collection: String,
}

#[async_trait]
impl BackendBuilder for RedisBackendBuilder {
    /// Create new `RedisBackendBuilder`.
    fn new(backend_url: &str) -> Self {
        Self {
            backend_url: backend_url.to_string(),
            connection_timeout: None,
            taskmeta_collection: "celery-task-meta".to_string(),
        }
    }

    fn database(self: Box<Self>, _: &str) -> Box<dyn BackendBuilder> {
        self
    }

    fn connection_timeout(self: Box<Self>, timeout: u32) -> Box<dyn BackendBuilder> {
        Box::new(Self {
            connection_timeout: Some(timeout),
            ..*self
        })
    }

    fn taskmeta_collection(self: Box<Self>, col: &str) -> Box<dyn BackendBuilder> {
        Box::new(Self {
            taskmeta_collection: col.to_string(),
            ..*self
        })
    }

    /// Create new `RedisBackend`.
    async fn build(self: Box<Self>) -> Result<Arc<dyn Backend>, BackendError> {
        let Self {
            backend_url,
            connection_timeout: _,
            taskmeta_collection,
        } = *self;

        let client = Client::open(backend_url.as_str())
            .map_err(|_| BackendError::InvalidBrokerUrl(backend_url))?;

        log::info!("Creating tokio manager");
        let manager = client.get_tokio_connection_manager().await?;

        Ok(Arc::new(RedisBackend {
            _client: client,
            manager,
            taskmeta_collection,
        }))
    }
}

pub struct RedisBackend {
    _client: Client,
    manager: ConnectionManager,
    taskmeta_collection: String,
}

#[async_trait]
impl Backend for RedisBackend {
    /// Store the task meta into redis and notify pubsub subscribers waiting for
    /// the task id.
    async fn store_task_meta(
        &self,
        task_id: &str,
        task_meta: TaskMeta,
    ) -> Result<(), BackendError> {
        let task_meta = serde_json::to_string(&task_meta)?;
        let key = format!("{}-{}", self.taskmeta_collection, task_id);

        log::debug!("Storing task meta into {key}");
        log::trace!("  task meta value {task_meta:#?}");

        let _ret: () = redis::cmd("SET")
            .arg(&key)
            .arg(&task_meta)
            .query_async(&mut self.manager.clone())
            .await?;

        let _ret: () = redis::cmd("PUBLISH")
            .arg(key)
            .arg(&task_meta)
            .query_async(&mut self.manager.clone())
            .await?;

        Ok(())
    }

    /// Retrieve task metadata and deserialize the result value
    async fn get_task_meta(&self, task_id: &str) -> Result<TaskMeta, BackendError> {
        let key = format!("{}-{}", self.taskmeta_collection, task_id);
        let raw: String = redis::cmd("GET")
            .arg(key)
            .query_async(&mut self.manager.clone())
            .await?;
        Ok(serde_json::from_str(&raw)?)
    }

    /// Delete task metadata from redis.
    async fn delete_task_meta(&self, task_id: &str) -> Result<(), BackendError> {
        let key = format!("{}-{}", self.taskmeta_collection, task_id);
        Ok(redis::cmd("DEL")
            .arg(key)
            .query_async(&mut self.manager.clone())
            .await?)
    }
}

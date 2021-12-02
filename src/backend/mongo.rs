use super::{Backend, BackendBuilder, BackendError, ResultMetadata};
use crate::task::TaskState;
use std::time::Duration;

use async_trait::async_trait;
use mongodb::{bson::doc, options::ClientOptions, Client, Database};
use serde::{de::DeserializeOwned, Serialize};

pub struct MongoBackendBuilder {
    backend_url: String,
    database: String,
    taskmeta_collection: String,
}

pub struct MongoBackend {
    database: Database,
    collection_name: String,
}

#[async_trait]
impl BackendBuilder for MongoBackendBuilder {
    type Backend = MongoBackend;

    /// Create new `MongoBackendBuilder`.
    fn new(backend_url: &str) -> Self {
        Self {
            backend_url: backend_url.to_string(),
            database: "celery".to_string(),
            taskmeta_collection: "celery_taskmeta".to_string(),
        }
    }

    fn database(mut self, database: &str) -> Self {
        self.database = database.to_string();
        self
    }

    fn taskmeta_collection(mut self, collection_name: &str) -> Self {
        self.taskmeta_collection = collection_name.to_string();
        self
    }

    /// Create new `MongoBackend`.
    async fn build(self, connection_timeout: u32) -> Result<Self::Backend, BackendError> {
        let mut client_options = ClientOptions::parse(&self.backend_url).await?;
        client_options.app_name = Some("celery".to_string());
        client_options.connect_timeout = Some(Duration::from_secs(connection_timeout as u64));
        let client = Client::with_options(client_options)?;

        Ok(MongoBackend {
            database: client.database(self.database.as_str()),
            collection_name: self.taskmeta_collection,
        })
    }
}

#[async_trait]
impl Backend for MongoBackend {
    type Builder = MongoBackendBuilder;

    async fn store_result_inner<T: Send + Sync + Unpin + Serialize>(
        &self,
        task_id: &str,
        metadata: Option<ResultMetadata<T>>,
    ) -> Result<(), BackendError> {
        let collection = self.database.collection(&self.collection_name);
        let filter = doc! { "task_id": task_id };
        if let None = metadata {
            collection.delete_one(filter, None).await?;
            return Ok(());
        }

        let metadata = metadata.unwrap();
        if metadata.status == TaskState::Pending {
            collection.insert_one(metadata, None).await?;
            return Ok(());
        }

        collection.replace_one(filter, metadata, None).await?;

        Ok(())
    }

    async fn get_task_meta<T: Send + Sync + Unpin + DeserializeOwned>(
        &self,
        task_id: &str,
    ) -> Result<ResultMetadata<T>, BackendError> {
        let collection = self.database.collection(&self.collection_name);
        let filter = doc! { "task_id": task_id };
        match collection.find_one(filter, None).await? {
            Some(doc) => Ok(doc),
            None => Err(BackendError::DocumentNotFound(task_id.to_string())),
        }
    }
}

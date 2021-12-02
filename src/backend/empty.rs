use crate::backend::ResultMetadata;
use crate::prelude::BackendError;
use super::{Backend, BackendBuilder};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

pub struct EmptyBackend;
pub struct EmptyBackendBuilder;

#[async_trait]
impl BackendBuilder for EmptyBackendBuilder {
    type Backend = EmptyBackend;

    fn new(_: &str) -> Self {
        unimplemented!()
    }

    fn database(self, _: &str) -> Self {
        self
    }

    fn taskmeta_collection(self, _: &str) -> Self {
        self
    }

    async fn build(self, _: u32) -> Result<Self::Backend, BackendError> {
        unimplemented!()
    }
}

#[async_trait]
impl Backend for EmptyBackend {
    type Builder = EmptyBackendBuilder;

    async fn store_result_inner<T: Send + Sync + Unpin + Serialize>(&self, _: &str, _: Option<ResultMetadata<T>>) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn get_task_meta<T: Send + Sync + Unpin + DeserializeOwned>(&self, _: &str) -> Result<super::ResultMetadata<T>, crate::prelude::BackendError> {
        unimplemented!()
    }
}

use super::{Backend, BackendBuilder, BackendError, ResultMetadata};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

pub(crate) struct MockBackend;
pub(crate) struct MockBackendBuilder;

#[async_trait]
impl BackendBuilder for MockBackendBuilder {
    type Backend = MockBackend;

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
impl Backend for MockBackend {
    type Builder = MockBackendBuilder;

    async fn store_result_inner<T: Send + Sync + Unpin + Serialize>(
        &self,
        _: &str,
        _: Option<ResultMetadata<T>>,
    ) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn get_task_meta<T: Send + Sync + Unpin + DeserializeOwned>(
        &self,
        _: &str,
    ) -> Result<super::ResultMetadata<T>, crate::prelude::BackendError> {
        unimplemented!()
    }
}

use crate::task::TaskStatus;
use crate::prelude::BackendError;
use super::{Backend, BackendBuilder};
use async_trait::async_trait;

pub(crate) struct MockBackend;
pub(crate) struct MockBackendBuilder;

#[async_trait]
impl BackendBuilder for MockBackendBuilder {
    type Backend = MockBackend;

    fn new(_: &str) -> Self {
        unimplemented!()
    }

    async fn build(&self, _: u32) -> Result<Self::Backend, BackendError> {
        unimplemented!()
    }
}

#[async_trait]
impl Backend for MockBackend {
    type Builder = MockBackendBuilder;

    async fn store_result_inner<T: Send>(&self, _: &str, _: Option<T>, _: Option<String>, _: TaskStatus) -> Result<(), BackendError> {
        unimplemented!()
    }

    async fn get_task_meta<T>(&self, _: &str) -> Result<super::ResultMetadata<T>, crate::prelude::BackendError> {
        unimplemented!()
    }
}

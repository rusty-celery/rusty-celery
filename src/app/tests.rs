use super::Celery;
use crate::broker::mock::MockBroker;
use crate::task::{Request, Task, TaskOptions, TaskResult};
use async_trait::async_trait;
use futures::executor::block_on;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

static MOCK_APP: Lazy<Celery<MockBroker>> = Lazy::new(|| block_on(build_app()));

async fn build_app() -> Celery<MockBroker> {
    let celery = Celery::<MockBroker>::builder("mock-app", "mock://localhost:8000")
        .build()
        .await
        .unwrap();
    celery.register_task::<AddTask>().await.unwrap();
    celery
}

struct AddTask {
    request: Request<Self>,
    options: TaskOptions,
}

#[async_trait]
impl Task for AddTask {
    const NAME: &'static str = "add";
    const ARGS: &'static [&'static str] = &["x", "y"];

    type Params = AddParams;
    type Returns = i32;

    fn from_request(request: Request<Self>, options: TaskOptions) -> Self {
        Self { request, options }
    }

    fn request(&self) -> &Request<Self> {
        &self.request
    }

    fn options(&self) -> &TaskOptions {
        &self.options
    }

    async fn run(&self, params: Self::Params) -> TaskResult<Self::Returns> {
        Ok(params.x + params.y)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct AddParams {
    x: i32,
    y: i32,
}

#[test]
fn test_app_name() {
    assert!(MOCK_APP.name == "mock-app");
}

#[tokio::test]
async fn test_add_task_registered() {
    assert!(MOCK_APP
        .task_trace_builders
        .read()
        .await
        .contains_key("add"));
}

use super::Celery;
use crate::broker::mock::MockBroker;
use crate::protocol::MessageContentType;
use crate::task::{Request, Signature, Task, TaskOptions, TaskResult};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use futures::executor::block_on;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

// Basic app with all defaults.
static MOCK_BASIC_APP: Lazy<Celery<MockBroker>> = Lazy::new(|| block_on(build_basic_app()));

// App with some custom configured options.
static MOCK_CONFIGURED_APP: Lazy<Celery<MockBroker>> =
    Lazy::new(|| block_on(build_configured_app()));

async fn build_basic_app() -> Celery<MockBroker> {
    let celery = Celery::<MockBroker>::builder("mock-app", "mock://localhost:8000")
        .build()
        .await
        .unwrap();
    celery.register_task::<AddTask>().await.unwrap();
    celery.register_task::<MultiplyTask>().await.unwrap();
    celery
}

async fn build_configured_app() -> Celery<MockBroker> {
    let celery = Celery::<MockBroker>::builder("mock-app", "mock://localhost:8000")
        .task_time_limit(10)
        .task_max_retries(100)
        .task_content_type(MessageContentType::Yaml)
        .build()
        .await
        .unwrap();
    celery.register_task::<AddTask>().await.unwrap();
    celery.register_task::<MultiplyTask>().await.unwrap();
    celery
}

// We can't use the #[task] macro from inside the crate, unfortunately, so we have
// to implement tasks the old fashion way.

struct AddTask {
    request: Request<Self>,
    options: TaskOptions,
}

impl AddTask {
    fn new(x: i32, y: i32) -> Signature<Self> {
        Signature::<Self>::new(AddParams { x, y })
    }
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

struct MultiplyTask {
    request: Request<Self>,
    options: TaskOptions,
}

impl MultiplyTask {
    fn new(x: i32, y: i32) -> Signature<Self> {
        Signature::<Self>::new(MultiplyParams { x, y })
    }
}

#[async_trait]
impl Task for MultiplyTask {
    const NAME: &'static str = "multiply";
    const ARGS: &'static [&'static str] = &["x", "y"];
    const DEFAULTS: TaskOptions = TaskOptions {
        time_limit: Some(5),
        hard_time_limit: Some(10),
        max_retries: Some(1000),
        min_retry_delay: None,
        max_retry_delay: None,
        retry_for_unexpected: None,
        acks_late: None,
        content_type: None,
    };

    type Params = MultiplyParams;
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
        Ok(params.x * params.y)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct MultiplyParams {
    x: i32,
    y: i32,
}

#[test]
fn test_app_name() {
    assert!(MOCK_BASIC_APP.name == "mock-app");
}

#[tokio::test]
async fn test_add_task_registered() {
    assert!(MOCK_BASIC_APP
        .task_trace_builders
        .read()
        .await
        .contains_key("add"));
}

#[tokio::test]
async fn test_send_task() {
    let result = MOCK_BASIC_APP.send_task(AddTask::new(1, 2)).await.unwrap();
    let sent_tasks = MOCK_BASIC_APP.broker.sent_tasks.read().await;
    let message = sent_tasks.get(&result.task_id).unwrap();
    assert!(&message.headers.task == "add");
}

#[tokio::test]
async fn test_send_task_with_countdown() {
    let result = MOCK_BASIC_APP
        .send_task(AddTask::new(1, 2).with_countdown(2))
        .await
        .unwrap();
    let sent_tasks = MOCK_BASIC_APP.broker.sent_tasks.read().await;
    let message = sent_tasks.get(&result.task_id).unwrap();
    assert!(&message.headers.eta.is_some());
}

#[tokio::test]
async fn test_send_task_with_eta() {
    let result = MOCK_BASIC_APP
        .send_task(AddTask::new(1, 2).with_eta(DateTime::<Utc>::from(SystemTime::now())))
        .await
        .unwrap();
    let sent_tasks = MOCK_BASIC_APP.broker.sent_tasks.read().await;
    let message = sent_tasks.get(&result.task_id).unwrap();
    assert!(&message.headers.eta.is_some());
}

#[tokio::test]
async fn test_send_task_with_expires_in() {
    let result = MOCK_BASIC_APP
        .send_task(AddTask::new(1, 2).with_expires_in(10))
        .await
        .unwrap();
    let sent_tasks = MOCK_BASIC_APP.broker.sent_tasks.read().await;
    let message = sent_tasks.get(&result.task_id).unwrap();
    assert!(&message.headers.expires.is_some());
}

#[tokio::test]
async fn test_send_task_with_expires() {
    let dt = DateTime::<Utc>::from(SystemTime::now()) + Duration::seconds(10);
    let result = MOCK_BASIC_APP
        .send_task(AddTask::new(1, 2).with_expires(dt))
        .await
        .unwrap();
    let sent_tasks = MOCK_BASIC_APP.broker.sent_tasks.read().await;
    let message = sent_tasks.get(&result.task_id).unwrap();
    assert!(&message.headers.expires.is_some());
}

#[tokio::test]
async fn test_send_task_with_content_type() {
    let result = MOCK_BASIC_APP
        .send_task(AddTask::new(1, 2).with_content_type(MessageContentType::Yaml))
        .await
        .unwrap();
    let sent_tasks = MOCK_BASIC_APP.broker.sent_tasks.read().await;
    let message = sent_tasks.get(&result.task_id).unwrap();
    assert!(&message.properties.content_type == "application/x-yaml");
}

#[tokio::test]
async fn test_send_task_with_time_limit() {
    let result = MOCK_BASIC_APP
        .send_task(AddTask::new(1, 2).with_time_limit(5))
        .await
        .unwrap();
    let sent_tasks = MOCK_BASIC_APP.broker.sent_tasks.read().await;
    let message = sent_tasks.get(&result.task_id).unwrap();
    assert!(message.headers.timelimit == (None, Some(5)));
}

#[tokio::test]
async fn test_send_task_with_hard_time_limit() {
    let result = MOCK_BASIC_APP
        .send_task(AddTask::new(1, 2).with_hard_time_limit(5))
        .await
        .unwrap();
    let sent_tasks = MOCK_BASIC_APP.broker.sent_tasks.read().await;
    let message = sent_tasks.get(&result.task_id).unwrap();
    assert!(message.headers.timelimit == (Some(5), None));
}

#[tokio::test]
async fn test_configured_app_send_task_app_defaults() {
    let result = MOCK_CONFIGURED_APP
        .send_task(AddTask::new(1, 2))
        .await
        .unwrap();
    let sent_tasks = MOCK_CONFIGURED_APP.broker.sent_tasks.read().await;
    let message = sent_tasks.get(&result.task_id).unwrap();
    assert!(message.headers.timelimit == (None, Some(10)));
    assert!(&message.properties.content_type == "application/x-yaml");
}

#[tokio::test]
async fn test_configured_app_send_task_task_defaults() {
    let result = MOCK_CONFIGURED_APP
        .send_task(MultiplyTask::new(1, 2))
        .await
        .unwrap();
    let sent_tasks = MOCK_CONFIGURED_APP.broker.sent_tasks.read().await;
    let message = sent_tasks.get(&result.task_id).unwrap();
    assert!(message.headers.timelimit == (Some(10), Some(5)));
    assert!(&message.properties.content_type == "application/x-yaml");
}

#[tokio::test]
async fn test_configured_app_send_task_request_overrides() {
    let result = MOCK_CONFIGURED_APP
        .send_task(
            MultiplyTask::new(1, 2)
                .with_time_limit(2)
                .with_content_type(MessageContentType::Json),
        )
        .await
        .unwrap();
    let sent_tasks = MOCK_CONFIGURED_APP.broker.sent_tasks.read().await;
    let message = sent_tasks.get(&result.task_id).unwrap();
    assert!(message.headers.timelimit == (Some(10), Some(2)));
    assert!(&message.properties.content_type == "application/json");
}

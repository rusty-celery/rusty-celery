#![allow(non_upper_case_globals)]

use async_trait::async_trait;
use celery::error::TaskError;
use celery::task::{Request, Signature, Task, TaskOptions};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::time::{self, Duration};

static SUCCESSES: Lazy<Mutex<HashMap<String, Result<i32, TaskError>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[allow(non_camel_case_types)]
struct add {
    request: Request<Self>,
    options: TaskOptions,
}

#[derive(Clone, Serialize, Deserialize)]
struct AddParams {
    x: i32,
    y: i32,
}

impl add {
    fn new(x: i32, y: i32) -> Signature<Self> {
        Signature::<Self>::new(AddParams { x, y })
    }
}

#[async_trait]
impl Task for add {
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

    async fn run(&self, params: Self::Params) -> Result<Self::Returns, TaskError> {
        Ok(params.x + params.y)
    }

    async fn on_success(&self, returned: &Self::Returns) {
        SUCCESSES
            .lock()
            .unwrap()
            .insert(self.request().id.clone(), Ok(*returned));
    }
}

#[tokio::test]
async fn test_amqp_broker() {
    let my_app = celery::app!(
        broker = AMQP { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/my_vhost".into()) },
        tasks = [add],
        task_routes = [
            "add" => "celery",
            "backend.*" => "backend",
            "ml.*" => "ml"
        ],
    );

    // Send task to queue.
    let send_result = my_app.send_task(add::new(1, 2)).await;
    assert!(send_result.is_ok());
    let correlation_id = send_result.unwrap();

    // Consume task from queue. We wrap this in `time::timeout(...)` because otherwise
    // `consume` will keep waiting for more tasks indefinitely.
    let result = time::timeout(Duration::from_secs(1), my_app.consume()).await;

    // `result` should be a timeout error, otherwise `consume` ended early which means
    // there must have been an error there.
    assert!(result.is_err());

    let successes = SUCCESSES.lock().unwrap();

    // Check that the "add" task succeeded.
    assert!(!successes.is_empty());
    assert!(successes[&correlation_id].is_ok());
    assert_eq!(successes[&correlation_id].as_ref().unwrap(), &3);
}

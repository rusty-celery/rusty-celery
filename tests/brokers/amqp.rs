#![allow(non_upper_case_globals)]

use async_trait::async_trait;
use celery::{celery_app, AMQPBroker, Error, Rule, Task, TaskContext};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::time::{self, Duration};

lazy_static! {
    static ref SUCCESSES: Mutex<HashMap<String, Result<i32, Error>>> = Mutex::new(HashMap::new());
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize)]
struct add {
    x: i32,
    y: i32,
}

#[async_trait]
impl Task for add {
    const NAME: &'static str = "add";
    const ARGS: &'static [&'static str] = &["x", "y"];

    type Returns = i32;

    async fn run(mut self) -> Result<Self::Returns, Error> {
        Ok(self.x + self.y)
    }

    async fn on_success(ctx: &TaskContext<'_>, returned: &Self::Returns) {
        SUCCESSES
            .lock()
            .unwrap()
            .insert(ctx.correlation_id.into(), Ok(*returned));
    }
}

impl add {
    fn new(x: i32, y: i32) -> Self {
        Self { x, y }
    }
}

celery_app!(
    my_app,
    AMQPBroker { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/my_vhost".into()) },
    tasks = [add],
    queue = "backend",
    queue = "ml",
    task_route = Rule::new("add", "celery").unwrap(),
    task_route = Rule::new("backend*", "backend").unwrap(),
    task_route = Rule::new("ml*", "ml").unwrap(),
);

#[tokio::test]
async fn test_rust_to_rust() {
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

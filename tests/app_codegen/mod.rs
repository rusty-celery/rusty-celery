//! These tests should be compiled but not run.

use std::sync::Arc;
use tokio::runtime::Runtime;

#[test]
fn test_basic_use() {
    let rt = Arc::new(Runtime::new().unwrap());
    rt.block_on(async {
        let _app = celery::app!(
            runtime = rt.clone(),
            broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
            tasks = [],
            task_routes = []
        );
    });
}

#[test]
fn test_basic_use_with_trailing_comma() {
    let rt = Arc::new(Runtime::new().unwrap());
    rt.block_on(async {
        let _app = celery::app!(
            runtime = rt.clone(),
            broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
            tasks = [],
            task_routes = [],
        );
    });
}

#[test]
fn test_with_options() {
    let rt = Arc::new(Runtime::new().unwrap());
    rt.block_on(async {
        let _app = celery::app!(
            runtime = rt.clone(),
            broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
            tasks = [],
            task_routes = [],
            task_time_limit = 2
        );
    });
}

#[test]
fn test_with_options_and_trailing_comma() {
    let rt = Arc::new(Runtime::new().unwrap());
    rt.block_on(async {
        let _app = celery::app!(
            runtime = rt.clone(),
            broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
            tasks = [],
            task_routes = [],
            task_time_limit = 2,
        );
    });
}

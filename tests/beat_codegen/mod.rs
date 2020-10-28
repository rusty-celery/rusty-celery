use celery::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[test]
fn test_basic_use() {
    let rt = Arc::new(Runtime::new().unwrap());
    rt.block_on(async {
        let _beat = celery::beat!(
            runtime = rt.clone(),
            broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
            task_routes = []
        );
    });
}

#[test]
fn test_basic_use_with_trailing_comma() {
    let rt = Arc::new(Runtime::new().unwrap());
    rt.block_on(async {
        let _beat = celery::beat!(
            runtime = rt.clone(),
            broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
            task_routes = [],
        );
    });
}

#[test]
fn test_with_options() {
    let rt = Arc::new(Runtime::new().unwrap());
    rt.block_on(async {
        let _beat = celery::beat!(
            runtime = rt.clone(),
            broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
            task_routes = [],
            default_queue = "celery"
        );
    });
}

#[test]
fn test_with_options_and_trailing_comma() {
    let rt = Arc::new(Runtime::new().unwrap());
    rt.block_on(async {
        let _beat = celery::beat!(
            runtime = rt.clone(),
            broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
            task_routes = [],
            default_queue = "celery",
        );
    });
}

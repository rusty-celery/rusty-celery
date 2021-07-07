//! These tests should be compiled but not run.

use celery::prelude::*;

#[tokio::test]
async fn test_basic_use() {
    let _app = celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [],
        task_routes = []
    );
}

#[tokio::test]
async fn test_basic_use_with_variable() {
    let connection_string = std::env::var("AMQP_ADDR").unwrap();
    let _app = celery::app!(
        broker = AMQPBroker { connection_string },
        tasks = [],
        task_routes = []
    );
}

#[tokio::test]
async fn test_basic_use_with_trailing_comma() {
    let _app = celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [],
        task_routes = [],
    );
}

#[tokio::test]
async fn test_with_options() {
    let _app = celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [],
        task_routes = [],
        task_time_limit = 2
    );
}

#[tokio::test]
async fn test_with_options_and_trailing_comma() {
    let _app = celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [],
        task_routes = [],
        task_time_limit = 2,
    );
}

use celery::prelude::*;

#[tokio::test]
async fn test_basic_use() {
    let _beat = celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        task_routes = []
    );
}

#[tokio::test]
async fn test_basic_use_with_trailing_comma() {
    let _beat = celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        task_routes = [],
    );
}

#[tokio::test]
async fn test_with_options() {
    let _beat = celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        task_routes = [],
        default_queue = "celery"
    );
}

#[tokio::test]
async fn test_with_options_and_trailing_comma() {
    let _beat = celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        task_routes = [],
        default_queue = "celery",
    );
}

#![allow(non_upper_case_globals)]

use celery::{celery_app, task, AMQPBroker};

#[task]
fn add(x: i32, y: i32) -> i32 {
    x + y
}

celery_app!(
    my_app,
    AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
    tasks = [add],
);

#[tokio::test]
async fn test_send_task() {
    assert!(my_app.send_task(add(1, 2)).await.is_ok());
}

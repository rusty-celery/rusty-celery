use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::time::{self, Duration};

use celery::{prelude::*, task::AsyncResult, Celery};

#[celery::task(name = "sucessful_task")]
pub async fn sucessful_task(a: i32, b: i32) -> TaskResult<i32> {
    Ok(a + b)
}

#[celery::task(name = "task_with_retry_failure", max_retries = 5)]
pub async fn task_with_retry_failure() -> TaskResult<i32> {
    Err(TaskError::retry(Some(
        chrono::Utc::now() + chrono::Duration::milliseconds(100),
    )))
}

#[celery::task(name = "task_with_expires_failure")]
pub async fn task_with_expires_failure() -> TaskResult<i32> {
    println!("running task_with_expires_failure");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    Err(TaskError::expected("failure expected"))
}

async fn app() -> Result<Arc<Celery>, CeleryError> {
    celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672//".into()) },
        backend = RedisBAckend { std::env::var("REDIS_ADDR").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into()) },
        tasks = [sucessful_task, task_with_retry_failure, task_with_expires_failure],
        task_routes = ["*" => "celery"],
        prefetch_count = 2
    ).await
}

#[tokio::test]
async fn test() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();
    let app = app().await?;
    let result: Result<AsyncResult<i32>, CeleryError> =
        app.send_task(sucessful_task::new(1, 2)).await;
    let Ok(result) = result else {
        panic!("Failed to send task");
    };
    let _ = time::timeout(Duration::from_secs(1), app.consume()).await;
    let result = result.get().fetch().await;
    assert!(matches!(result, Ok(3)));
    Ok(())
}

#[tokio::test]
async fn test_max_retries() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();
    let app = app().await?;
    let result: Result<AsyncResult<i32>, CeleryError> =
        app.send_task(task_with_retry_failure::new()).await;
    let Ok(result) = result else {
        panic!("Failed to send task");
    };
    let _ = time::timeout(Duration::from_secs(2), app.consume()).await;
    let result = result.get().fetch().await;
    assert!(matches!(
        result,
        Err(CeleryError::TaskError(TaskError {
            kind: TaskErrorType::MaxRetriesExceeded,
            ..
        }))
    ));
    Ok(())
}

#[tokio::test]
async fn test_expires() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();
    let app = app().await?;
    let result: Result<AsyncResult<i32>, CeleryError> = app
        .send_task(task_with_expires_failure::new().with_expires_in(1))
        .await;
    let Ok(result) = result else {
        panic!("Failed to send task");
    };
    let _ = time::timeout(Duration::from_secs(2), app.consume()).await;
    let result = result.get().fetch().await;
    assert!(matches!(
        result,
        Err(CeleryError::TaskError(TaskError {
            kind: TaskErrorType::Timeout,
            ..
        }))
    ));
    Ok(())
}

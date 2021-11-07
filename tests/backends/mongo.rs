use celery::{prelude::{AMQPBroker, BackendError, CeleryError, MongoBackend, TaskError, TaskResult}, task::TaskState};

use std::time::Duration;

use tokio::time::{sleep, timeout};
use serial_test::serial;

#[celery::task]
async fn long_running_task() -> TaskResult<()> {
    sleep(Duration::from_secs(2)).await;
    Ok(())
}

#[celery::task]
async fn simple_task() -> TaskResult<u8> {
    Ok(42)
}

#[celery::task]
async fn failed_task() -> TaskResult<()> {
    Err(TaskError::ExpectedError("lol".to_string()))
}

fn broker_connection_string() -> String {
    std::env::var("AMQP_ADDR1").unwrap_or_else(|_| "amqp://127.0.0.1:5673//".into())
}

fn backend_connection_string() -> String {
    std::env::var("MONGO_ADDR").unwrap_or_else(|_| "mongodb://127.0.0.1:27017/".into())
}

#[tokio::test]
async fn test_execute_long_task_returns_pending() -> Result<(), CeleryError> {
    let my_app = celery::app!(
        broker = AMQPBroker { broker_connection_string() },
        backend = MongoBackend { backend_connection_string() },
        tasks = [long_running_task],
        task_routes = [
            "*" => "celery",
        ]
    )
    .await
    .unwrap();

    let task = my_app.send_task(long_running_task::new()).await?;
    let failed = task.failed::<()>().await?;
    let ready = task.ready::<()>().await?;
    let result = task.result::<()>().await?;
    let state = task.state::<()>().await?;
    let successful = task.successful::<()>().await?;
    let task_id = task.task_id();

    assert!(!task_id.is_empty());
    assert!(!failed);
    assert!(!ready);
    assert_eq!(None, result);
    assert_eq!(TaskState::Pending, state);
    assert!(!successful);
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_execute_simple_task_returns_result() -> Result<(), CeleryError> {
    let my_app = celery::app!(
        broker = AMQPBroker { broker_connection_string() },
        backend = MongoBackend { backend_connection_string() },
        tasks = [simple_task],
        task_routes = [
            "*" => "celery",
        ]
    )
    .await
    .unwrap();

    let task = my_app.send_task(simple_task::new()).await?;
    let _ = timeout(Duration::from_secs(1), my_app.consume()).await;
    let failed = task.failed::<u8>().await?;
    let ready = task.ready::<u8>().await?;
    let result = task.result::<u8>().await?;
    let traceback = task.traceback::<u8>().await?;
    let state = task.state::<u8>().await?;
    let successful = task.successful::<u8>().await?;
    let task_id = task.task_id();

    assert!(!task_id.is_empty());
    assert!(!failed);
    assert!(ready);
    assert_eq!(Some(42), result);
    assert!(traceback.is_none());
    assert_eq!(TaskState::Success, state);
    assert!(successful);
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_execute_failed_task_returns_error() -> Result<(), CeleryError> {
    let my_app = celery::app!(
        broker = AMQPBroker { broker_connection_string() },
        backend = MongoBackend { backend_connection_string() },
        tasks = [failed_task],
        task_routes = [
            "*" => "celery",
        ]
    )
    .await
    .unwrap();

    let task = my_app.send_task(failed_task::new()).await?;
    let _ = timeout(Duration::from_secs(1), my_app.consume()).await;
    let failed = task.failed::<u8>().await?;
    let ready = task.ready::<u8>().await?;
    let result = task.result::<u8>().await?;
    let traceback = task.traceback::<u8>().await?;
    let state = task.state::<u8>().await?;
    let successful = task.successful::<u8>().await?;
    let task_id = task.task_id();

    assert!(!task_id.is_empty());
    assert!(failed);
    assert!(ready);
    assert!(result.is_none());
    assert_eq!(format!("{}", TaskError::ExpectedError("lol".to_string())), format!("{}", traceback.unwrap()));
    assert_eq!(TaskState::Failure, state);
    assert!(!successful);
    Ok(())
}

#[tokio::test]
async fn test_forget_simple_task_deletes_document() -> Result<(), CeleryError> {
    let my_app = celery::app!(
        broker = AMQPBroker { broker_connection_string() },
        backend = MongoBackend { backend_connection_string() },
        tasks = [simple_task],
        task_routes = [
            "*" => "celery",
        ]
    )
    .await
    .unwrap();
    let task = my_app.send_task(simple_task::new()).await?;
    let task_id = task.task_id();
    task.forget::<u8>().await?;

    let result = task.result::<u8>().await;

    assert_eq!(format!("{}", BackendError::DocumentNotFound(task_id.to_string())), format!("{}", result.err().unwrap()));
    Ok(())
}

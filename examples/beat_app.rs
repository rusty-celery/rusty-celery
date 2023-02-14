#![allow(unused_variables)]

use anyhow::Result;
use celery::beat::{CronSchedule, DeltaSchedule};
use celery::task::TaskResult;
use env_logger::Env;
use tokio::time::Duration;

const QUEUE_NAME: &str = "celery";

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    unimplemented!()
}

#[celery::task]
fn long_running_task(secs: Option<u64>) -> TaskResult<()> {
    unimplemented!()
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // Build a `Beat` with a default scheduler backend.
    let mut beat = celery::beat!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/my_vhost".into()) },
        tasks = [
            "add" => {
                add,
                schedule = DeltaSchedule::new(Duration::from_secs(5)),
                args = (1, 2),
            },
            "long_running" => {
                long_running_task,
                schedule = CronSchedule::from_string("*/2 * * * *")?,
                args = (Some(1),),
            }
        ],
        task_routes = [
            "*" => QUEUE_NAME,
        ],
    ).await?;

    beat.start().await?;

    Ok(())
}

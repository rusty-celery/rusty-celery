#![allow(unused_variables)]

use anyhow::Result;
use celery::beat::RegularSchedule;
use celery::broker::AMQPBroker;
use celery::task::TaskResult;
use env_logger::Env;
use std::sync::Arc;
use tokio::runtime::Runtime;
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

async fn tokio_main(rt: Arc<Runtime>) -> Result<()> {
    // Build a `Beat` with a default scheduler backend.
    let mut beat = celery::beat!(
        runtime = rt,
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/my_vhost".into()) },
        task_routes = [
            "*" => QUEUE_NAME,
        ],
    ).await?;

    // Add scheduled tasks to the default `Beat` and start it.
    let add_schedule = RegularSchedule::new(Duration::from_secs(5));
    beat.schedule_task(add::new(1, 2), add_schedule);

    let long_running_schedule = RegularSchedule::new(Duration::from_secs(10));
    beat.schedule_task(long_running_task::new(Some(1)), long_running_schedule);

    beat.start().await?;

    Ok(())
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let rt = Arc::new(Runtime::new()?);
    rt.block_on(tokio_main(rt.clone()))?;
    Ok(())
}

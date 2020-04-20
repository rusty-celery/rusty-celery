#![allow(unused_variables)]

use celery::task::TaskResult;
use celery::scheduler::{Service, Scheduler, RegularSchedule};
use env_logger::Env;
use exitfailure::ExitFailure;
use tokio::time::Duration;

const QUEUE_NAME: &str = "scheduled";

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    unimplemented!()
}

#[celery::task]
fn subtract(x: i32, y: i32) -> TaskResult<i32> {
    unimplemented!()
}

#[tokio::main]
async fn main() -> Result<(), ExitFailure> {
    env_logger::from_env(Env::default().default_filter_or("info")).init();
    let my_app = celery::app!(
        broker = AMQP { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/my_vhost".into()) },
        tasks = [
            add,
        ],
        task_routes = [
            "*" => QUEUE_NAME,
        ],
        prefetch_count = 2,
        heartbeat = Some(10),
    );

    let broker = my_app.get_broker();
    let mut scheduler = Scheduler::new(broker);

    let add_schedule = RegularSchedule::new(Duration::from_secs(1));
    scheduler.add("add_task", add::new(1, 2), add_schedule);

    let subtract_schedule = RegularSchedule::new(Duration::from_millis(700));
    scheduler.add("subtract_task", subtract::new(2, 6), subtract_schedule);

    let mut beat_service = Service::new(scheduler);
    beat_service.start().await;

    Ok(())
}

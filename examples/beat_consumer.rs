use celery::task::TaskResult;
use env_logger::Env;
use exitfailure::ExitFailure;
use log::info;

const QUEUE_NAME: &str = "beat_queue";

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    info!("Adding {} and {}", x, y);
    Ok(x + y)
}

#[celery::task]
fn subtract(x: i32, y: i32) -> TaskResult<i32> {
    info!("Subtracting {} and {}", x, y);
    Ok(x - y)
}

#[tokio::main]
async fn main() -> Result<(), ExitFailure> {
    env_logger::from_env(Env::default().default_filter_or("info")).init();
    let my_app = celery::app!(
        broker = AMQP { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/my_vhost".into()) },
        tasks = [
            add,
            subtract,
        ],
        task_routes = [
            "*" => QUEUE_NAME,
        ],
        prefetch_count = 2,
        heartbeat = Some(10),
    );

    my_app.consume_from(&[QUEUE_NAME]).await?;

    my_app.close().await?;

    Ok(())
}

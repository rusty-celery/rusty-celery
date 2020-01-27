#![allow(non_upper_case_globals)]

use async_trait::async_trait;
use celery::{celery_app, task, AMQPBroker, ErrorKind};
use exitfailure::ExitFailure;
use structopt::StructOpt;
use tokio::time::{self, Duration};

// This generates the task struct and impl with the name set to the function name "add"
#[task]
fn add(x: i32, y: i32) -> i32 {
    x + y
}

// Demonstrates a task that raises an error, and also how to customize task options.
// In this case we override the default `max_retries`.
#[task(max_retries = 3)]
fn buggy_task() {
    #[allow(clippy::try_err)]
    Err(ErrorKind::UnexpectedError("a bug caused this".into()))?
}

// Demonstrates a long running IO-bound task. By increasing the prefetch count, an arbitrary
// number of these number can execute concurrently.
#[task]
fn long_running_task() {
    time::delay_for(Duration::from_secs(10)).await;
}

// Initialize a Celery app bound to `my_app`.
celery_app!(
    my_app,
    AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
    tasks = [add, buggy_task, long_running_task],
    prefetch_count = 2,
);

#[derive(Debug, StructOpt)]
#[structopt(
    name = "celery_app",
    about = "Run a Rust Celery producer or consumer.",
    setting = structopt::clap::AppSettings::ColoredHelp,
)]
enum CeleryOpt {
    Consume,
    Produce,
}

#[tokio::main]
async fn main() -> Result<(), ExitFailure> {
    env_logger::init();
    let opt = CeleryOpt::from_args();

    match opt {
        CeleryOpt::Consume => {
            my_app.consume("celery", None).await?;
        }
        CeleryOpt::Produce => {
            my_app.send_task(add(1, 2)).await?;
        }
    };

    Ok(())
}

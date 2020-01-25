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

#[task(max_retries = 3)]
fn buggy_task() {
    #[allow(clippy::try_err)]
    Err(ErrorKind::UnexpectedError("a bug caused this".into()))?
}

#[task]
fn long_running_task() {
    time::delay_for(Duration::from_secs(10)).await;
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "celery",
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

    // Initialize a Celery app named 'app'.
    celery_app!(
        app,
        AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
        tasks = [add, buggy_task, long_running_task],
    );

    match opt {
        CeleryOpt::Consume => {
            app.consume("celery").await?;
        }
        CeleryOpt::Produce => {
            app.send_task(add(1, 2), "celery").await?;
        }
    };

    Ok(())
}

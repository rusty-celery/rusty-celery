use async_trait::async_trait;
use celery::{task, AMQPBroker, Celery, ErrorKind};
use exitfailure::ExitFailure;
use lazy_static::lazy_static;
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

static QUEUE: &str = "celery";

#[tokio::main]
async fn main() -> Result<(), ExitFailure> {
    env_logger::init();
    let opt = CeleryOpt::from_args();

    // The Celery app instance needs to have a static lifetime in order to be able
    // to consume from a queue, since consuming spawns async tasks that are bound
    // to the app.
    lazy_static! {
        static ref CELERY: Celery<AMQPBroker> = {
            let broker_url =
                std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
            let broker = AMQPBroker::builder(&broker_url)
                .queue(QUEUE)
                .prefetch_count(2)
                .build()
                .unwrap();
            let celery = Celery::builder("celery", broker)
                .default_queue_name(QUEUE)
                .build();
            celery.register_task::<add>().unwrap();
            celery.register_task::<buggy_task>().unwrap();
            celery.register_task::<long_running_task>().unwrap();
            celery
        };
    }

    match opt {
        CeleryOpt::Consume => {
            CELERY.consume(QUEUE).await?;
        }
        CeleryOpt::Produce => {
            CELERY.send_task(add(1, 2), QUEUE).await?;
        }
    };

    Ok(())
}

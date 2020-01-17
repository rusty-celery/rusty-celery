use async_trait::async_trait;
use celery::AMQPBroker;
use celery::{task, Celery};
use exitfailure::ExitFailure;
use structopt::StructOpt;

#[task(name = "add")]
fn add(x: i32, y: i32) -> i32 {
    x + y
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

    let broker_url =
        std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
    let queue = "celery";

    let broker = AMQPBroker::builder(&broker_url)
        .queue(queue)
        .build()
        .await?;
    let mut celery = Celery::builder("celery", broker)
        .default_queue_name(queue)
        .build();
    celery.register_task::<add>()?;

    match opt {
        CeleryOpt::Consume => {
            celery.consume(queue).await?;
        }
        CeleryOpt::Produce => {
            celery.send_task(add(1, 2), queue).await?;
        }
    };

    Ok(())
}

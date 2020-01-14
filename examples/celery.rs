use async_trait::async_trait;
use celery::amqp::AMQPBroker;
use celery::{Celery, Error, Task};
use exitfailure::ExitFailure;
use log::info;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(Serialize, Deserialize)]
struct AddTask {
    x: i32,
    y: i32,
}

#[async_trait]
impl Task for AddTask {
    type Returns = i32;

    async fn run(&mut self) -> Result<i32, Error> {
        Ok(self.x + self.y)
    }
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
    let mut celery = Celery::<AMQPBroker>::builder()
        .default_queue_name(queue)
        .build("consumer", broker);
    celery.register_task::<AddTask>("add")?;

    match opt {
        CeleryOpt::Consume => {
            info!("Consuming tasks");
            celery.consume(queue).await?;
        }
        CeleryOpt::Produce => {
            info!("Producing tasks");
        }
    };

    Ok(())
}

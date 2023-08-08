use anyhow::Result;
use tokio::time::{self, Duration};

use structopt::StructOpt;

#[celery::task]
async fn say_hello(name: String) {
    time::sleep(Duration::from_secs(5)).await;
    println!("Hello {name}");
    println!("Async consumer task finished");
}

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
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let opt = CeleryOpt::from_args();

    let my_app = celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672".into()) },
        tasks = [
            say_hello,
        ],
        // This just shows how we can route certain tasks to certain queues based
        // on glob matching.
        task_routes = [
            "*" => "celery",
        ],
        prefetch_count = 2,
        heartbeat = Some(10),
    ).await?;

    match opt {
        // Runs the consumer part
        CeleryOpt::Consume => {
            my_app.display_pretty().await;
            my_app.consume_from(&["celery"]).await?;
        }
        // Runs the producer part
        CeleryOpt::Produce => {
            my_app.send_task(say_hello::new("Rust".to_owned())).await?;
        }
    };

    my_app.close().await?;
    Ok(())
}

#![allow(non_upper_case_globals)]
#![allow(clippy::unit_arg)]

use async_trait::async_trait;
use celery::{celery_app, task, AMQPBroker};
use exitfailure::ExitFailure;
use structopt::StructOpt;
use tokenizers::models::bpe::BPE;
use tokenizers::tokenizer::{EncodeInput, Tokenizer};
use tokio::time::Instant;

lazy_static::lazy_static! {
    static ref tokenizer: Tokenizer = {
        let bpe = BPE::from_files("examples/gpt2-vocab.json", "examples/gpt2-merges.txt")
            .unwrap()
            .cache_capacity(0)
            .build()
            .unwrap();
        Tokenizer::new(Box::new(bpe))
    };
}

const S: &str = "Hello, World!
We're going to tokenize some text to see how long it takes to do
very computationally heavy tasks such as tokenization with a BPE model.
BPE is pretty cool. It was actually adapted from a simple compression algorithm.
Basically, the idea is to recursively find the most common combinations of word pieces
in a corpus. During training, when the next most common combination, that combination
is added to the vocabulary and all instances of that combination are then merged into
the new vocab entry. Then those steps are repeated until a vocabulary of a desired
size is created.";

const N: u32 = 1000;

#[task]
fn tokenize(text: String) {
    tokenizer.encode(EncodeInput::Single(text)).unwrap();
}

#[task]
fn spawn_tokenize(text: String) {
    let text = text.clone();
    tokio::task::spawn_blocking(|| tokenizer.encode(EncodeInput::Single(text)).unwrap()).await?;
}

celery_app!(
    benchmark_app,
    AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
    tasks = [tokenize, spawn_tokenize],
    prefetch_count = 10,
    default_queue_name = "benches",
);

#[derive(Debug, StructOpt)]
#[structopt(
    name = "runtime_benchmark",
    about = "Benchmarking spawn vs not spawning in separate thread.",
    setting = structopt::clap::AppSettings::ColoredHelp,
)]
enum BenchOpt {
    NoSpawn,
    Spawn,
}

#[tokio::main]
async fn main() -> Result<(), ExitFailure> {
    env_logger::init();
    let opt = BenchOpt::from_args();

    for _ in 0..N {
        match opt {
            BenchOpt::NoSpawn => benchmark_app.send_task(tokenize(S.into())).await?,
            BenchOpt::Spawn => benchmark_app.send_task(spawn_tokenize(S.into())).await?,
        };
    }

    let start = Instant::now();
    benchmark_app.consume("benches", Some(N)).await?;
    let duration = start.elapsed();
    println!("Duration: {:?}", duration);

    Ok(())
}

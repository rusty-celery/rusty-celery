#![allow(non_upper_case_globals)]

use async_trait::async_trait;
use celery::{celery_app, task, AMQPBroker};
use exitfailure::ExitFailure;
use tokio::time::{self, Duration};
use tokenizers::tokenizer::{Tokenizer, EncodeInput, Encoding};
use tokenizers::models::bpe::BPE;

lazy_static! {
    static ref tokenizer: Tokenizer = {
        let bpe = BPE::from_files("gp2-vocab.json", "gpt2-merges.txt").unwrap().unk_token("[UNK]".into()).build().unwrap();
        Tokenizer::new(Box::new(bpe))
    };
}

const S: &str = "Hello, World!";
const N: u32 = 10;

#[task]
fn tokenize(text: String) -> Encoding {
    let text = text.clone();
    tokenizer.encode(EncodeInput::Single(text)).unwrap()
}

#[task]
fn spawn_tokenize(text: String) -> Encoding {
    let text = text.clone();
    tokio::task::spawn_blocking(|| {
        tokenizer.encode(EncodeInput::Single(text)).unwrap()
    }).await?
}

celery_app!(
    benchmark_app,
    AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
    tasks = [tokenize, spawn_tokenize],
    prefetch_count = 4,
    default_queue_name = "benches",
);

#[tokio::main]
async fn main() -> Result<(), ExitFailure> {
    env_logger::init();

    for _ in 0..N {
        benchmark_app.send_task(tokenize(S.into())).await?;
    }
    benchmark_app.consume("benches", Some(N)).await?;

    Ok(())
}

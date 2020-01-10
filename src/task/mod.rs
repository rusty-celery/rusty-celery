use async_trait::async_trait;
use failure::Error;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait Task: Send + Sync + Serialize + for<'de> Deserialize<'de> {
    async fn run(&mut self) -> Result<(), Error>;

    async fn on_failure(&mut self, err: Error) -> Result<(), Error> {
        Err(err)
    }

    async fn on_success(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn timeout(&self) -> Option<usize> {
        None
    }

    fn max_retries(&self) -> Option<usize> {
        None
    }

    fn min_retry_delay(&self) -> usize {
        0
    }

    fn max_retry_delay(&self) -> usize {
        3600
    }
}

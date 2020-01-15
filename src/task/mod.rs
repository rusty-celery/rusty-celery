use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// A `Task` represents a unit of work that a `Celery` app can produce or consume.
#[async_trait]
pub trait Task: Send + Sync + Serialize + for<'de> Deserialize<'de> {
    const NAME: &'static str;

    type Returns: Send + Sync + std::fmt::Debug;

    fn arg_names() -> Vec<String> {
        vec![]
    }

    async fn run(&mut self) -> Result<Self::Returns, Error>;

    async fn on_failure(&mut self, err: Error) -> Result<(), Error> {
        Err(err)
    }

    #[allow(unused_variables)]
    async fn on_success(&mut self, returned: Self::Returns) -> Result<(), Error> {
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

pub trait ArgsCompat {
    fn args() -> Vec<String>;
}

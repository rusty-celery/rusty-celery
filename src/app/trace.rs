use async_trait::async_trait;
use log::{debug, error, info, warn};
use tokio::time::{self, Duration, Instant};

use super::TaskOptions;
use crate::protocol::{Message, MessageBody};
use crate::{Error, ErrorKind, Task};

pub(super) type TraceBuilderResult = Result<Box<dyn TracerTrait>, Error>;

pub(super) fn build_tracer<T: Task + Send + 'static>(
    message: Message,
    options: TaskOptions,
) -> TraceBuilderResult {
    Ok(Box::new(Tracer::<T>::new(message, options)?))
}

#[async_trait]
pub(super) trait TracerTrait: Send {
    async fn trace(&mut self) -> Result<(), Error>;
}

pub(super) struct Tracer<T>
where
    T: Task,
{
    task: T,
    message: Message,
    options: TaskOptions,
}

impl<T> Tracer<T>
where
    T: Task,
{
    fn new(message: Message, options: TaskOptions) -> Result<Self, Error> {
        let body = MessageBody::<T>::from_raw_data(&message.raw_data)?;
        let task = body.1;
        let options = options.overrides(&task);
        Ok(Self {
            task,
            message,
            options,
        })
    }
}

#[async_trait]
impl<T> TracerTrait for Tracer<T>
where
    T: Task,
{
    /// Wraps the execution of a task, catching and logging errors and then running
    /// the appropriate post-execution functions.
    async fn trace(&mut self) -> Result<(), Error> {
        if let Some(countdown) = self.message.countdown() {
            info!(
                "Task {}[{}] received, ETA: {}",
                T::NAME,
                self.message.properties.correlation_id,
                self.message.headers.eta.unwrap()
            );
            time::delay_for(countdown).await;
        } else {
            info!(
                "Task {}[{}] received",
                T::NAME,
                self.message.properties.correlation_id
            );
        }
        let start = Instant::now();
        let result = match self.options.timeout {
            Some(secs) => {
                debug!("Executing task with {} second timeout", secs);
                let duration = Duration::from_secs(secs as u64);
                time::timeout(duration, self.task.run()).into_inner().await
            }
            None => self.task.run().await,
        };
        let duration = start.elapsed();
        match result {
            Ok(returned) => {
                info!(
                    "Task {}[{}] succeeded in {}s: {:?}",
                    T::NAME,
                    self.message.properties.correlation_id,
                    duration.as_secs_f32(),
                    returned
                );
                self.task.on_success(&returned).await;
                Ok(())
            }
            Err(e) => {
                match e.kind() {
                    ErrorKind::ExpectedError(reason) => {
                        warn!(
                            "Task {}[{}] raised expected: {}",
                            T::NAME,
                            self.message.properties.correlation_id,
                            reason
                        );
                    }
                    ErrorKind::UnexpectedError(reason) => {
                        error!(
                            "Task {}[{}] raised unexpected: {}",
                            T::NAME,
                            self.message.properties.correlation_id,
                            reason
                        );
                    }
                    _ => {
                        error!(
                            "Task {}[{}] failed: {}",
                            T::NAME,
                            self.message.properties.correlation_id,
                            e
                        );
                    }
                };
                self.task.on_failure(&e).await;
                if let Some(max_retries) = self.options.max_retries {
                    let retries = match self.message.headers.retries {
                        Some(n) => n,
                        None => 0,
                    };
                    if retries >= max_retries {
                        return Err(e);
                    }
                }
                info!(
                    "Task {}[{}] retrying",
                    T::NAME,
                    self.message.properties.correlation_id
                );
                Err(ErrorKind::Retry.into())
            }
        }
    }
}

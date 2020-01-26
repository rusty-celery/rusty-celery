use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use log::{debug, error, info, warn};
use rand::distributions::{Distribution, Uniform};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{self, Duration, Instant};

use super::{TaskEvent, TaskOptions, TaskStatus};
use crate::protocol::{Message, MessageBody};
use crate::{Error, ErrorKind, Task};

/// A `Tracer` provides the API through which a `Celery` application interacts with its tasks.
///
/// A `Tracer` is tied to a task and is responsible for executing it directly, catching
/// and handling any errors, logging, and running the `on_failure` or `on_success` post-execution
/// methods. It communicates its progress and the results back to the application through
/// the `event_tx` channel and the return value of `Tracer::trace`, respectively.
pub(super) struct Tracer<T>
where
    T: Task,
{
    task: T,
    message: Message,
    options: TaskOptions,
    countdown: Option<Duration>,
    event_tx: UnboundedSender<TaskEvent>,
}

impl<T> Tracer<T>
where
    T: Task,
{
    fn new(
        message: Message,
        options: TaskOptions,
        event_tx: UnboundedSender<TaskEvent>,
    ) -> Result<Self, Error> {
        let body = MessageBody::<T>::from_raw_data(&message.raw_data)?;
        let task = body.1;
        let options = options.overrides(&task);
        let countdown = message.countdown();
        Ok(Self {
            task,
            message,
            options,
            countdown,
            event_tx,
        })
    }
}

#[async_trait]
impl<T> TracerTrait for Tracer<T>
where
    T: Task,
{
    async fn trace(&mut self) -> Result<(), Error> {
        if let Some(countdown) = self.countdown {
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

        if self.is_expired() {
            warn!(
                "Task {}[{}] expired, discarding",
                T::NAME,
                self.message.properties.correlation_id,
            );
            return Err(ErrorKind::TaskExpiredError.into());
        }

        self.event_tx
            .send(TaskEvent::new(TaskStatus::Pending))
            .unwrap_or_else(|_| {
                // This really shouldn't happen. If it does, there's probably much
                // bigger things to worry about like running out of memory.
                error!("Failed sending task event");
            });

        let start = Instant::now();
        let result = match self.options.timeout {
            Some(secs) => {
                debug!("Executing task with {} second timeout", secs);
                let duration = Duration::from_secs(secs as u64);
                time::timeout(duration, self.task.run())
                    .await
                    .unwrap_or_else(|_| Err(Error::from(ErrorKind::TimeoutError)))
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

                self.event_tx
                    .send(TaskEvent::new(TaskStatus::Finished))
                    .unwrap_or_else(|_| {
                        error!("Failed sending task event");
                    });

                Ok(())
            }
            Err(e) => {
                match e.kind() {
                    ErrorKind::ExpectedError(reason) => {
                        warn!(
                            "Task {}[{}] failed with expected error: {}",
                            T::NAME,
                            self.message.properties.correlation_id,
                            reason
                        );
                    }
                    ErrorKind::TimeoutError => {
                        error!(
                            "Task {}[{}] timed out",
                            T::NAME,
                            self.message.properties.correlation_id,
                        );
                    }
                    _ => {
                        error!(
                            "Task {}[{}] failed with unexpected error: {}",
                            T::NAME,
                            self.message.properties.correlation_id,
                            e
                        );
                    }
                };

                self.task.on_failure(&e).await;

                self.event_tx
                    .send(TaskEvent::new(TaskStatus::Finished))
                    .unwrap_or_else(|_| {
                        error!("Failed sending task event");
                    });

                let retries = match self.message.headers.retries {
                    Some(n) => n,
                    None => 0,
                };
                if let Some(max_retries) = self.options.max_retries {
                    if retries >= max_retries {
                        warn!(
                            "Task {}[{}] retries exceeded",
                            T::NAME,
                            self.message.properties.correlation_id
                        );
                        return Err(e);
                    }
                    info!(
                        "Task {}[{}] retrying ({} / {})",
                        T::NAME,
                        self.message.properties.correlation_id,
                        retries + 1,
                        max_retries,
                    );
                } else {
                    info!(
                        "Task {}[{}] retrying ({} / inf)",
                        T::NAME,
                        self.message.properties.correlation_id,
                        retries + 1,
                    );
                }

                Err(ErrorKind::Retry.into())
            }
        }
    }

    fn retry_eta(&self) -> Option<DateTime<Utc>> {
        let retries = self.message.headers.retries.unwrap_or(0);
        let delay_secs = std::cmp::min(
            2usize
                .checked_pow(retries as u32)
                .unwrap_or_else(|| self.options.max_retry_delay),
            self.options.max_retry_delay,
        );
        let delay_secs = std::cmp::max(delay_secs, self.options.min_retry_delay);
        let between = Uniform::from(0..1000);
        let mut rng = rand::thread_rng();
        let delay_millis = between.sample(&mut rng);
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(now) => {
                let now_secs = now.as_secs() as usize;
                let now_millis = now.subsec_millis();
                let eta_secs = now_secs + delay_secs;
                let eta_millis = now_millis + delay_millis;
                Some(DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp(eta_secs as i64, eta_millis * 1000),
                    Utc,
                ))
            }
            Err(_) => None,
        }
    }

    fn is_delayed(&self) -> bool {
        self.countdown.is_some()
    }

    fn is_expired(&self) -> bool {
        self.message.is_expired()
    }
}

#[async_trait]
pub(super) trait TracerTrait: Send {
    /// Wraps the execution of a task, catching and logging errors and then running
    /// the appropriate post-execution functions.
    async fn trace(&mut self) -> Result<(), Error>;

    /// Get the ETA for a retry with exponential backoff.
    fn retry_eta(&self) -> Option<DateTime<Utc>>;

    fn is_delayed(&self) -> bool;

    fn is_expired(&self) -> bool;
}

pub(super) type TraceBuilderResult = Result<Box<dyn TracerTrait>, Error>;

pub(super) type TraceBuilder = Box<
    dyn Fn(Message, TaskOptions, UnboundedSender<TaskEvent>) -> TraceBuilderResult
        + Send
        + Sync
        + 'static,
>;

pub(super) fn build_tracer<T: Task + Send + 'static>(
    message: Message,
    options: TaskOptions,
    event_tx: UnboundedSender<TaskEvent>,
) -> TraceBuilderResult {
    Ok(Box::new(Tracer::<T>::new(message, options, event_tx)?))
}

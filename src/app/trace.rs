use async_trait::async_trait;
use log::{debug, error, info, warn};
use std::convert::TryFrom;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{self, Duration, Instant};

use crate::error::{ProtocolError, TaskError, TraceError};
use crate::protocol::Message;
use crate::task::{Request, Task, TaskEvent, TaskOptions, TaskStatus};

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
    event_tx: UnboundedSender<TaskEvent>,
}

impl<T> Tracer<T>
where
    T: Task,
{
    #![allow(clippy::unnecessary_wraps)]
    fn new(task: T, event_tx: UnboundedSender<TaskEvent>) -> Result<Self, ProtocolError> {
        if let Some(eta) = task.request().eta {
            info!(
                "Task {}[{}] received, ETA: {}",
                task.name(),
                task.request().id,
                eta
            );
        } else {
            info!("Task {}[{}] received", task.name(), task.request().id);
        }

        Ok(Self { task, event_tx })
    }
}

#[async_trait]
impl<T> TracerTrait for Tracer<T>
where
    T: Task,
{
    async fn trace(&mut self) -> Result<(), TraceError> {
        if self.is_expired() {
            warn!(
                "Task {}[{}] expired, discarding",
                self.task.name(),
                &self.task.request().id,
            );
            return Err(TraceError::ExpirationError);
        }

        self.event_tx
            .send(TaskEvent::StatusChange(TaskStatus::Pending))
            .unwrap_or_else(|_| {
                // This really shouldn't happen. If it does, there's probably much
                // bigger things to worry about like running out of memory.
                error!("Failed sending task event");
            });

        let start = Instant::now();
        let result = match self.task.time_limit() {
            Some(secs) => {
                debug!("Executing task with {} second time limit", secs);
                let duration = Duration::from_secs(secs as u64);
                time::timeout(duration, self.task.run(self.task.request().params.clone()))
                    .await
                    .unwrap_or(Err(TaskError::TimeoutError))
            }
            None => self.task.run(self.task.request().params.clone()).await,
        };
        let duration = start.elapsed();

        match result {
            Ok(returned) => {
                info!(
                    "Task {}[{}] succeeded in {}s: {:?}",
                    self.task.name(),
                    &self.task.request().id,
                    duration.as_secs_f32(),
                    returned
                );

                // Run success callback.
                self.task.on_success(&returned).await;

                self.event_tx
                    .send(TaskEvent::StatusChange(TaskStatus::Finished))
                    .unwrap_or_else(|_| {
                        error!("Failed sending task event");
                    });

                Ok(())
            }
            Err(e) => {
                let (should_retry, retry_eta) = match e {
                    TaskError::ExpectedError(ref reason) => {
                        warn!(
                            "Task {}[{}] failed with expected error: {}",
                            self.task.name(),
                            &self.task.request().id,
                            reason
                        );
                        (true, None)
                    }
                    TaskError::UnexpectedError(ref reason) => {
                        error!(
                            "Task {}[{}] failed with unexpected error: {}",
                            self.task.name(),
                            &self.task.request().id,
                            reason
                        );
                        (self.task.retry_for_unexpected(), None)
                    }
                    TaskError::TimeoutError => {
                        error!(
                            "Task {}[{}] timed out after {}s",
                            self.task.name(),
                            &self.task.request().id,
                            duration.as_secs_f32(),
                        );
                        (true, None)
                    }
                    TaskError::Retry(eta) => {
                        error!(
                            "Task {}[{}] triggered retry",
                            self.task.name(),
                            &self.task.request().id,
                        );
                        (true, eta)
                    }
                };

                // Run failure callback.
                self.task.on_failure(&e).await;

                self.event_tx
                    .send(TaskEvent::StatusChange(TaskStatus::Finished))
                    .unwrap_or_else(|_| {
                        error!("Failed sending task event");
                    });

                if !should_retry {
                    return Err(TraceError::TaskError(e));
                }

                let retries = self.task.request().retries;
                if let Some(max_retries) = self.task.max_retries() {
                    if retries >= max_retries {
                        warn!(
                            "Task {}[{}] retries exceeded",
                            self.task.name(),
                            &self.task.request().id,
                        );
                        return Err(TraceError::TaskError(e));
                    }
                    info!(
                        "Task {}[{}] retrying ({} / {})",
                        self.task.name(),
                        &self.task.request().id,
                        retries + 1,
                        max_retries,
                    );
                } else {
                    info!(
                        "Task {}[{}] retrying ({} / inf)",
                        self.task.name(),
                        &self.task.request().id,
                        retries + 1,
                    );
                }

                Err(TraceError::Retry(
                    retry_eta.or_else(|| self.task.retry_eta()),
                ))
            }
        }
    }

    async fn wait(&self) {
        if let Some(countdown) = self.task.request().countdown() {
            time::sleep(countdown).await;
        }
    }

    fn is_delayed(&self) -> bool {
        self.task.request().is_delayed()
    }

    fn is_expired(&self) -> bool {
        self.task.request().is_expired()
    }

    fn acks_late(&self) -> bool {
        self.task.acks_late()
    }
}

#[async_trait]
pub(super) trait TracerTrait: Send + Sync {
    /// Wraps the execution of a task, catching and logging errors and then running
    /// the appropriate post-execution functions.
    async fn trace(&mut self) -> Result<(), TraceError>;

    /// Wait until the task is due.
    async fn wait(&self);

    fn is_delayed(&self) -> bool;

    fn is_expired(&self) -> bool;

    fn acks_late(&self) -> bool;
}

pub(super) type TraceBuilderResult = Result<Box<dyn TracerTrait>, ProtocolError>;

pub(super) type TraceBuilder = Box<
    dyn Fn(Message, TaskOptions, UnboundedSender<TaskEvent>, String) -> TraceBuilderResult
        + Send
        + Sync
        + 'static,
>;

pub(super) fn build_tracer<T: Task + Send + 'static>(
    message: Message,
    mut options: TaskOptions,
    event_tx: UnboundedSender<TaskEvent>,
    hostname: String,
) -> TraceBuilderResult {
    // Build request object.
    let mut request = Request::<T>::try_from(message)?;
    request.hostname = Some(hostname);

    // Override app-level options with task-level options.
    T::DEFAULTS.override_other(&mut options);

    // Now construct the task from the request and options.
    // It seems redundant to construct a request just to use it to construct a task,
    // but the task keeps the request object so the task implementation can access
    // it.
    let task = T::from_request(request, options);

    Ok(Box::new(Tracer::<T>::new(task, event_tx)?))
}

#![allow(clippy::unit_arg)]
/// NOTE: The tests in this module are time-sensitive.
///
/// Errors in the order of 1-2 milliseconds are expected, so checks
/// are written to have a tolerance of at least 10 milliseconds.
use super::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;

use crate::broker::Queue;
use crate::error::{BrokerError, ProtocolError};
use crate::{
    protocol::{Message, TryDeserializeMessage},
    task::{Request, TaskOptions, TaskResult},
};
use std::fmt::{self, Display};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::SystemTime,
};
use tokio::time::{self, Duration};

#[tokio::test]
async fn test_task_with_regular_schedule() {
    // Create a dummy broker for this test.
    let sent_messages = Arc::new(Mutex::new(Vec::new()));
    let dummy_broker = DummyBroker {
        sent_messages: sent_messages.clone(),
    };

    // Configure a dummy queue for the tasks.
    let task_routes = vec![Rule::new("dummy_*", "dummy_queue").unwrap()];

    let mut beat: Beat<DummyBroker, LocalSchedulerBackend> = Beat {
        name: "dummy_beat".to_string(),
        scheduler: Scheduler::new(dummy_broker),
        scheduler_backend: LocalSchedulerBackend::new(),
        task_routes,
        default_queue: "celery".to_string(),
        task_options: TaskOptions::default(),
        broker_connection_timeout: 5,
        broker_connection_retry: true,
        broker_connection_max_retries: 5,
        broker_connection_retry_delay: 5,
    };

    beat.schedule_task(
        Signature::<DummyTask>::new(()),
        RegularSchedule::new(Duration::from_millis(20)),
    );

    let start_time = SystemTime::now();
    let result = time::timeout(Duration::from_millis(50), beat.start()).await;

    assert!(result.is_err()); // The beat should only stop because of the timeout

    let tasks: Vec<_> = sent_messages.lock().unwrap().drain(..).collect();

    // Check that the tasks have been executed the correct number of times.
    assert_eq!(
        3,
        tasks.len(),
        "This test is time-sensitive, there may be spurious failures"
    );

    // Check that the tasks have been sent to the correct queue.
    for (_, queue, _) in tasks.iter() {
        assert_eq!("dummy_queue", queue.as_str());
    }

    // Check that the tasks executed (approximately) on time.
    assert!(tasks[0].2.duration_since(start_time).unwrap() < Duration::from_millis(10));
    assert!(tasks[1].2.duration_since(start_time).unwrap() < Duration::from_millis(30));
}

#[tokio::test]
async fn test_scheduling_two_tasks() {
    // Create a dummy broker for this test.
    let sent_messages = Arc::new(Mutex::new(Vec::new()));
    let dummy_broker = DummyBroker {
        sent_messages: sent_messages.clone(),
    };

    // Configure dummy queues for the tasks.
    let task_routes = vec![
        Rule::new("dummy_task2", "dummy_queue2").unwrap(),
        Rule::new("dummy_*", "dummy_queue").unwrap(),
    ];
    let mut beat: Beat<DummyBroker, LocalSchedulerBackend> = Beat {
        name: "dummy_beat".to_string(),
        scheduler: Scheduler::new(dummy_broker),
        scheduler_backend: LocalSchedulerBackend::new(),
        task_routes,
        default_queue: "celery".to_string(),
        task_options: TaskOptions::default(),
        broker_connection_timeout: 5,
        broker_connection_retry: true,
        broker_connection_max_retries: 5,
        broker_connection_retry_delay: 5,
    };

    beat.schedule_task(
        Signature::<DummyTask>::new(()),
        RegularSchedule::new(Duration::from_millis(60)),
    );
    beat.schedule_task(
        Signature::<DummyTask2>::new(()),
        RegularSchedule::new(Duration::from_millis(43)),
    );

    let result = time::timeout(Duration::from_millis(200), beat.start()).await;

    assert!(result.is_err()); // The beat should only stop because of the timeout

    // Separate DummyTask from DummyTask2.
    let (task1, task2): (Vec<_>, Vec<_>) = sent_messages
        .lock()
        .unwrap()
        .drain(..)
        .partition(|x| &x.0.headers.task == "dummy_task");

    // Check that the tasks have been executed the correct number of times.
    assert_eq!(
        4,
        task1.len(),
        "This test is time-sensitive, there may be spurious failures"
    );
    assert_eq!(
        5,
        task2.len(),
        "This test is time-sensitive, there may be spurious failures"
    );

    // Check that the tasks have been sent to the correct queue.
    for (_, queue, _) in task1 {
        assert_eq!("dummy_queue", queue.as_str());
    }
    for (_, queue, _) in task2 {
        assert_eq!("dummy_queue2", queue.as_str());
    }
}

/* **************************
IMPLEMENTATION OF DUMMY TASKS
*************************** */

#[derive(Clone)]
struct DummyTask {}

#[async_trait]
impl Task for DummyTask {
    const NAME: &'static str = "dummy_task";
    const ARGS: &'static [&'static str] = &[];
    type Params = ();
    type Returns = ();

    fn from_request(_request: Request<Self>, _options: TaskOptions) -> Self {
        unimplemented!()
    }

    fn request(&self) -> &Request<Self> {
        unimplemented!()
    }

    fn options(&self) -> &TaskOptions {
        unimplemented!()
    }

    async fn run(&self, _params: Self::Params) -> TaskResult<Self::Returns> {
        unimplemented!()
    }
}

#[derive(Clone)]
struct DummyTask2 {}

#[async_trait]
impl Task for DummyTask2 {
    const NAME: &'static str = "dummy_task2";
    const ARGS: &'static [&'static str] = &[];
    type Params = ();
    type Returns = ();

    fn from_request(_request: Request<Self>, _options: TaskOptions) -> Self {
        unimplemented!()
    }

    fn request(&self) -> &Request<Self> {
        unimplemented!()
    }

    fn options(&self) -> &TaskOptions {
        unimplemented!()
    }

    async fn run(&self, _params: Self::Params) -> TaskResult<Self::Returns> {
        unimplemented!()
    }
}

/* ***************************
IMPLEMENTATION OF DUMMY BROKER
**************************** */

/// This broker can be used to test that the beat scheduler calls `send`
/// with the correct queue and at the expected time.
#[derive(Debug)]
struct DummyBroker {
    sent_messages: Arc<Mutex<Vec<(Message, String, SystemTime)>>>,
}

#[async_trait]
impl Broker for DummyBroker {
    type Builder = DummyBrokerBuilder;
    type Delivery = DummyDelivery;
    type DeliveryError = DummyDeliveryError;
    type DeliveryStream = DummyDeliveryStream;

    fn safe_url(&self) -> String {
        "dummy://user:***@dummy-broker/".into()
    }

    async fn consume<E: Fn(BrokerError) + Send + Sync + 'static>(
        &self,
        _queue: &str,
        _handler: Box<E>,
    ) -> Result<Self::DeliveryStream, BrokerError> {
        unimplemented!()
    }

    async fn ack(&self, _delivery: &Self::Delivery) -> Result<(), BrokerError> {
        unimplemented!()
    }

    async fn retry(
        &self,
        _delivery: &Self::Delivery,
        _eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        unimplemented!()
    }

    async fn send(&self, message: &Message, queue: &str) -> Result<(), BrokerError> {
        let now = SystemTime::now();
        self.sent_messages
            .lock()
            .unwrap()
            .push((message.clone(), queue.to_string(), now));
        Ok(())
    }

    async fn increase_prefetch_count(&self) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn decrease_prefetch_count(&self) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn reconnect(&self, _connection_timeout: u32) -> Result<(), BrokerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct DummyDelivery {}

impl TryDeserializeMessage for DummyDelivery {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct DummyDeliveryError {}

impl Display for DummyDeliveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DummyDeliveryError")
    }
}

#[derive(Debug)]
struct DummyDeliveryStream {}

impl Stream for DummyDeliveryStream {
    type Item = Result<DummyDelivery, DummyDeliveryError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        unimplemented!()
    }
}

/// This builder is implemented because it is required by the contract
/// of the `Broker` trait, but it is not meant to be used.
#[derive(Debug)]
struct DummyBrokerBuilder {}

#[async_trait]
impl BrokerBuilder for DummyBrokerBuilder {
    type Broker = DummyBroker;

    fn new(_broker_url: &str) -> Self {
        unimplemented!()
    }

    fn prefetch_count(self, _prefetch_count: u16) -> Self {
        unimplemented!()
    }

    fn declare_queue(self, _queue: Queue) -> Self {
        unimplemented!()
    }

    fn heartbeat(self, _heartbeat: Option<u16>) -> Self {
        unimplemented!()
    }

    async fn build(&self, _connection_timeout: u32) -> Result<Self::Broker, BrokerError> {
        unimplemented!()
    }
}

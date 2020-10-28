#![allow(clippy::unit_arg)]
/// NOTE: The tests in this module are time-sensitive.
///
/// Errors in the order of 1-2 milliseconds are expected, so checks
/// are written to have a tolerance of at least 10 milliseconds.
use super::*;
use async_trait::async_trait;

use crate::{
    broker::mock::*,
    task::{Request, TaskOptions, TaskResult},
};
use std::{
    sync::Arc,
    time::SystemTime,
};
use tokio::runtime::Runtime;
use tokio::time::{self, Duration};

#[test]
fn test_task_with_regular_schedule() {
    let rt = Arc::new(Runtime::new().unwrap());
    rt.block_on(async {
        // Create a dummy broker for this test.
        let dummy_broker = MockBroker::new();

        // Configure a dummy queue for the tasks.
        let task_routes = vec![Rule::new("dummy_*", "dummy_queue").unwrap()];

        let mut beat: Beat<MockBroker, LocalSchedulerBackend> = Beat {
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
            runtime: rt.clone(),
        };

        beat.schedule_task(
            Signature::<DummyTask>::new(()),
            RegularSchedule::new(Duration::from_millis(20)),
        );

        let start_time = SystemTime::now();
        let result = time::timeout(Duration::from_millis(50), beat.start()).await;

        assert!(result.is_err()); // The beat should only stop because of the timeout

        let mut tasks: Vec<_> = beat
            .scheduler
            .broker
            .sent_tasks
            .write()
            .await
            .drain()
            .collect();
        tasks.sort_by(|a, b| a.1.2.cmp(&b.1.2));

        // Check that the tasks have been executed the correct number of times.
        assert_eq!(
            3,
            tasks.len(),
            "This test is time-sensitive, there may be spurious failures"
        );

        // Check that the tasks have been sent to the correct queue.
        for (_, (_, queue, _)) in tasks.iter() {
            assert_eq!("dummy_queue", queue.as_str());
        }

        // Check that the tasks executed (approximately) on time.
        assert!(tasks[0].1.2.duration_since(start_time).unwrap() < Duration::from_millis(10));
        assert!(tasks[1].1.2.duration_since(start_time).unwrap() < Duration::from_millis(30));
    });
}

#[test]
fn test_scheduling_two_tasks() {
    let rt = Arc::new(Runtime::new().unwrap());
    rt.block_on(async {
        // Create a dummy broker for this test.
        let dummy_broker = MockBroker::new();

        // Configure dummy queues for the tasks.
        let task_routes = vec![
            Rule::new("dummy_task2", "dummy_queue2").unwrap(),
            Rule::new("dummy_*", "dummy_queue").unwrap(),
        ];
        let mut beat: Beat<MockBroker, LocalSchedulerBackend> = Beat {
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
            runtime: rt.clone(),
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
        let (task1, task2): (Vec<_>, Vec<_>) = beat
            .scheduler
            .broker
            .sent_tasks
            .write()
            .await
            .drain()
            .partition(|x| &x.1.0.headers.task == "dummy_task");

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
        for (_, (_, queue, _)) in task1 {
            assert_eq!("dummy_queue", queue.as_str());
        }
        for (_, (_, queue, _)) in task2 {
            assert_eq!("dummy_queue2", queue.as_str());
        }
    });
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

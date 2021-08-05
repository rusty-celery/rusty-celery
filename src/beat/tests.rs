#![allow(clippy::unit_arg)]
/// NOTE: Some tests in this module are time-sensitive.
///
/// Errors in the order of 1-2 milliseconds are expected, so checks
/// are written to have a tolerance of at least 10 milliseconds.
use super::*;
use crate::{
    broker::mock::*,
    task::{Request, TaskOptions, TaskResult},
};
use async_trait::async_trait;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::SystemTime;
use tokio::time::{self, Duration};

/// We test that a task is sent to the correct queue and executed
/// the correct amount of times. We also check that executions are
/// reasonably on time.
#[tokio::test]
async fn test_task_with_delta_schedule() {
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
        max_sleep_duration: None,
    };

    beat.schedule_task(
        Signature::<DummyTask>::new(()),
        DeltaSchedule::new(Duration::from_millis(20)),
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
    tasks.sort_by(|a, b| (a.1).2.cmp(&(b.1).2));

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
    assert!((tasks[0].1).2.duration_since(start_time).unwrap() < Duration::from_millis(10));
    assert!((tasks[1].1).2.duration_since(start_time).unwrap() < Duration::from_millis(30));
}

/// We test that two different tasks are executed the correct amount of times
/// and that they are sent to the correct queues.
#[tokio::test]
async fn test_scheduling_two_tasks() {
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
        max_sleep_duration: None,
    };

    beat.schedule_task(
        Signature::<DummyTask>::new(()),
        DeltaSchedule::new(Duration::from_millis(60)),
    );
    beat.schedule_task(
        Signature::<DummyTask2>::new(()),
        DeltaSchedule::new(Duration::from_millis(43)),
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
        .partition(|x| &(x.1).0.headers.task == "dummy_task");

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
}

struct TenMillisSchedule {}

impl Schedule for TenMillisSchedule {
    fn next_call_at(&self, _last_run_at: Option<SystemTime>) -> Option<SystemTime> {
        Some(SystemTime::now() + Duration::from_millis(10))
    }
}

/// This is a regression test for https://github.com/rusty-celery/rusty-celery/issues/199
#[tokio::test]
async fn test_task_with_delayed_first_run() {
    // Create a dummy beat for this test.
    let dummy_broker = MockBroker::new();
    let task_routes = vec![Rule::new("*", "dummy_queue").unwrap()];
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
        max_sleep_duration: None,
    };

    // Schedule a task that will not execute immediately.
    beat.schedule_task(Signature::<DummyTask>::new(()), TenMillisSchedule {});

    let result = time::timeout(Duration::from_millis(50), beat.start()).await;

    assert!(result.is_err()); // The beat should only stop because of the timeout

    let task_count = beat
        .scheduler
        .broker
        .sent_tasks
        .write()
        .await
        .drain()
        .count();

    // There a was a bug that caused the task to be dropped without being executed
    // if it was not scheduled to run immediately. Hence here we check that
    // the task has been executed at least once.
    assert!(
        task_count > 0,
        "A task that was supposed to run at least once did not run."
    );
}

/// The beat always ticks once when started and then ticks again depending on when
/// the next task to execute is and depending on the value of max_sleep_duration.
///
/// We are going to test that the beat ticks at least twice during the test execution,
/// even if there are no scheduled tasks and the default sleep time (currently fixed to
/// 500 milliseconds) for the scheduler is longer than the timeout.
#[tokio::test]
async fn test_beat_max_sleep_duration() {
    let max_sleep_duration = Duration::from_millis(1);
    let test_timeout = Duration::from_millis(20);
    let num_sync_calls = Rc::new(RefCell::new(0));

    // Define a dummy SchedulerBackend that we need to check that sync is called
    // even if there is no task which is ready to be sent.
    struct DummySchedulerBackend {
        num_sync_calls: Rc<RefCell<usize>>,
    }

    impl SchedulerBackend for DummySchedulerBackend {
        fn should_sync(&self) -> bool {
            true
        }

        fn sync(
            &mut self,
            _scheduled_tasks: &mut std::collections::BinaryHeap<ScheduledTask>,
        ) -> Result<(), BeatError> {
            *self.num_sync_calls.borrow_mut() += 1;
            Ok(())
        }
    }

    // Create a dummy beat for this test.
    let dummy_broker = MockBroker::new();
    let mut beat: Beat<MockBroker, DummySchedulerBackend> = Beat {
        name: "dummy_beat".to_string(),
        scheduler: Scheduler::new(dummy_broker),
        scheduler_backend: DummySchedulerBackend {
            num_sync_calls: Rc::clone(&num_sync_calls),
        },
        task_routes: vec![],
        default_queue: "celery".to_string(),
        task_options: TaskOptions::default(),
        broker_connection_timeout: 5,
        broker_connection_retry: true,
        broker_connection_max_retries: 5,
        broker_connection_retry_delay: 5,
        max_sleep_duration: Some(max_sleep_duration),
    };

    let result = time::timeout(test_timeout, beat.start()).await;

    assert!(result.is_err()); // The beat should only stop because of the timeout

    // Check that sync has been called as expected.
    use std::ops::Deref;
    assert!(*num_sync_calls.borrow().deref() >= 2);
}

////// IMPLEMENTATION OF DUMMY TASKS THAT CAN BE USED BY TESTS //////
/*
 * These tasks are not supposed to run, but can be sent to a dummy broker
 * to check that they are scheduled for execution.
 */

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

// TERMINOLOGY (for the moment, this is very similar to Python implementation, but it will probably evolve):
// - schedule: the strategy used to decide when a task must be executed (each scheduled task has its
//   own schedule)
// - schedule entry: a task together with its schedule
// - scheduler: the component in charge of keeping track of tasks to execute
// - beat service: the background service that drives execution, calling the appropriate
//   methods of the scheduler in an infinite loop (called just service in Python)

// We have changed a little the architecture compared to Python. In Python,
// there is a Scheduler base class and all schedulers inherit from that.
// The main logic is in the scheduler though, so here we use a scheduler
// struct and a trait for the scheduler backend (not implemented for now).

// In Python, there are three different types of schedules: `schedule`
// (just use a time delta), `crontab`, `solar`. They all inherit from
// `BaseSchedule`.

use crate::broker::Broker;
use crate::protocol::Message;
use crate::routing::Rule;
use crate::task::{Signature, Task};
use log::{debug, info, warn};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::convert::TryFrom;
use std::time::{Duration, SystemTime};
use tokio::time;

const ZERO_SECS: Duration = Duration::from_secs(0);

// The trait implemented by all schedules (regular, cron, solar).
// We will only have regular for now.
pub trait Schedule {
    fn next_call_at(&self, last_run_at: Option<SystemTime>) -> SystemTime;
}

// When using this schedule, tasks are executed at regular intervals.
pub struct RegularSchedule {
    interval: Duration,
}

impl RegularSchedule {
    pub fn new(interval: Duration) -> RegularSchedule {
        RegularSchedule { interval }
    }
}

impl Schedule for RegularSchedule {
    fn next_call_at(&self, last_run_at: Option<SystemTime>) -> SystemTime {
        match last_run_at {
            Some(last_run_at) => last_run_at.checked_add(self.interval).unwrap(),
            None => SystemTime::now(),
        }
    }
}

// TODO it is enough for the queue to be a reference, but a little refactor is required
type Queue = String;

// This is a factory for messages: ideally the Scheduler would store signatures, but
// a signature is parameterized by its underlying task. So we store these factories
// instead.
trait MessageFactoryTrait {
    // For the moment, we conveniently return the queue too, but it could be a separate method.
    fn make(&self, task_routes: &[Rule], default_queue: &str) -> (Message, Queue);
}

struct MessageFactory<T>
where
    T: Task + Clone,
{
    signature: Signature<T>,
}

impl<T> MessageFactory<T>
where
    T: Task + Clone,
{
    fn new(signature: Signature<T>) -> MessageFactory<T> {
        MessageFactory { signature }
    }
}

impl<T> MessageFactoryTrait for MessageFactory<T>
where
    T: Task + Clone,
{
    fn make(&self, task_routes: &[Rule], default_queue: &str) -> (Message, Queue) {
        let mut cloned_signature = self.signature.clone();
        let maybe_queue = cloned_signature.queue.take();

        let queue = maybe_queue.unwrap_or_else(|| {
            crate::routing::route(T::NAME, task_routes)
                .unwrap_or(default_queue)
                .to_string()
        });
        let message = Message::try_from(cloned_signature).expect("TODO");
        (message, queue)
    }
}

// A task which is scheduled for execution. It contains the task to execute,
// the schedule which determines when to run the task, and some other info.
struct ScheduleEntry {
    name: String,
    signature: Box<dyn MessageFactoryTrait>,
    schedule: Box<dyn Schedule>,
    last_run_at: Option<SystemTime>,
    total_run_count: u32,
}

impl ScheduleEntry {
    fn new<T, S>(name: &str, signature: Signature<T>, schedule: S) -> ScheduleEntry
    where
        T: Task + Clone + 'static,
        S: Schedule + 'static,
    {
        ScheduleEntry {
            name: name.to_string(),
            signature: Box::new(MessageFactory::new(signature)),
            schedule: Box::new(schedule),
            last_run_at: None,
            total_run_count: 0,
        }
    }
    fn next_call_at(&self) -> SystemTime {
        self.schedule.next_call_at(self.last_run_at)
    }
}

// The Python impl stores a tuple with this structure inside the heap (see the Scheduler struct).
// We could directly add this info to ScheduleEntry, but for now we keep
// the same structure.
struct SortableScheduleEntry {
    next_call_at: SystemTime,
    entry: ScheduleEntry,
}

// TODO make impls coherent
impl PartialEq for SortableScheduleEntry {
    fn eq(&self, other: &Self) -> bool {
        // We should make sure that names are unique, or we should refine this implementation
        self.entry.name == other.entry.name
    }
}

impl Eq for SortableScheduleEntry {}

impl Ord for SortableScheduleEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // The order is important (other is compared to self):
        // BinaryHeap is a max-heap by default, but we want a min-heap.
        other.next_call_at.cmp(&self.next_call_at)
    }
}

impl PartialOrd for SortableScheduleEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Struct with all the main methods. It uses a min-heap to retrieve
// the task which should run next.
pub struct Scheduler<B: Broker> {
    heap: BinaryHeap<SortableScheduleEntry>,
    default_sleep_interval: Duration,
    broker: B,
    task_routes: Vec<Rule>,
    default_queue: String,
}

impl<B> Scheduler<B>
where
    B: Broker,
{
    pub(crate) fn new(broker: B, task_routes: Vec<Rule>, default_queue: String) -> Scheduler<B> {
        Scheduler {
            heap: BinaryHeap::new(),
            default_sleep_interval: Duration::from_millis(500),
            broker,
            task_routes,
            default_queue,
        }
    }

    // TODO Check the format of a schedule on file,
    // though we don't want to implement that now,
    // because it will require to serialize the task to run, somehow.
    // Better to start with a function which
    // the user must use to register scheduled tasks, and this
    // function will accept a Future, with all task arguments
    // already provided:
    fn schedule_task<T, S>(&mut self, name: &str, signature: Signature<T>, schedule: S)
    where
        T: Task + Clone + 'static,
        S: Schedule + 'static,
    {
        let entry = ScheduleEntry::new(name, signature, schedule);
        self.heap.push(SortableScheduleEntry {
            next_call_at: entry.next_call_at(),
            entry,
        });
    }

    async fn tick(&mut self) -> Duration {
        // Here we want to pop a scheduled entry and execute it if it is due,
        // then push it back on the heap with an updated due time,
        // and finally return when the next entry is due (may be due immediately).
        // At least, this is what Python does.
        if let Some(mut sortable_entry) = self.heap.peek_mut() {
            let now = SystemTime::now();
            if sortable_entry.next_call_at <= now {
                // Here we use the message factory to create a message
                // and send it to the queue.
                let (message, queue) = sortable_entry
                    .entry
                    .signature
                    .make(&self.task_routes, &self.default_queue);
                info!("Sending task to {}", queue,);
                self.broker.send(&message, &queue).await.expect("TODO");

                sortable_entry.entry.last_run_at.replace(now);
                sortable_entry.entry.total_run_count += 1;
                sortable_entry.next_call_at = sortable_entry.entry.next_call_at();

                ZERO_SECS // Ask to immediately call tick again (that's what Python does, it may be improved?)
            } else {
                debug!("Too early, let's sleep more");
                sortable_entry.next_call_at.duration_since(now).unwrap()
            }
        } else {
            debug!("Nothing to do!");
            warn!(
                "Currently, it is not possible to schedule tasks while the beat service is running"
            );
            self.default_sleep_interval
        }
    }
}

// Not sure yet what logic should end up here.
// trait SchedulerBackend {}

// The only Scheduler Backend implementation for now. It keeps
// all data in memory.
// struct InMemoryBackend {}

// This is the structure that manages the main loop.
// The main method is `start`, which calls scheduler.tick,
// sleeps if necessary and then calls scheduler.sync.
// Not sure if it is necessary for it to be a struct,
// maybe just a function is enough.
// This should be moved to another file later (beat.rs?)
pub struct BeatService<B: Broker + 'static> {
    scheduler: Scheduler<B>,
}

impl<B> BeatService<B>
where
    B: Broker + 'static,
{
    pub fn new(scheduler: Scheduler<B>) -> BeatService<B> {
        BeatService { scheduler }
    }

    pub fn schedule_task<T, S>(&mut self, name: &str, signature: Signature<T>, schedule: S)
    where
        T: Task + Clone + 'static,
        S: Schedule + 'static,
    {
        self.scheduler.schedule_task(name, signature, schedule);
    }

    pub async fn start(&mut self) -> ! {
        info!("Starting beat service");
        loop {
            let sleep_interval = self.scheduler.tick().await;
            debug!("Now sleeping for {:?}", sleep_interval);
            time::delay_for(sleep_interval).await;
        }
    }
}

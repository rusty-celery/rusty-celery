use super::scheduled_task::ScheduledTask;
use std::collections::BinaryHeap;

pub trait SchedulerBackend {
    fn should_sync(&self) -> bool;

    // TODO: For the moment, this method can only add new tasks. We should think how tasks can
    // be removed too.
    fn sync(&mut self, scheduled_tasks: &mut BinaryHeap<ScheduledTask>);

    // Maybe we should consider some methods to inform the backend that a task has been executed.
    // Not sure about what Python does, but at least it keeps a counter with the number of executed tasks,
    // and the backend has access to that.
}

// The only Scheduler Backend implementation for now. It does basically nothing.
pub struct InMemoryBackend {}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self {}
    }
}

impl SchedulerBackend for InMemoryBackend {
    fn should_sync(&self) -> bool {
        false
    }

    #[allow(unused_variables)]
    fn sync(&mut self, scheduled_tasks: &mut BinaryHeap<ScheduledTask>) {
        todo!()
    }
}

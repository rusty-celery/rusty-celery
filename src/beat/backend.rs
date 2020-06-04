/// This module contains the definition of application-provided scheduler backends.
use super::scheduled_task::ScheduledTask;
use std::collections::BinaryHeap;

/// This trait is implemented by all `Scheduler` backends.
pub trait SchedulerBackend {
    /// Check whether the internal state of the scheduler should be synchronized.
    /// If this method returns `true`, then `sync` will be called as soon as possible.
    fn should_sync(&self) -> bool;

    /// Synchronize the internal state of the scheduler.
    ///
    /// This method is called in the pauses between scheduled tasks. Synchronization should
    /// be as quick as possible, as it may otherwise delay the execution of due tasks.
    /// If synchronization is slow, it should be done incrementally (i.e., it should span
    /// multiple calls to `sync`).
    ///
    /// This method will not be called if `should_sync` returns `false`.
    fn sync(&mut self, scheduled_tasks: &mut BinaryHeap<ScheduledTask>);

    // Maybe we should consider some methods to inform the backend that a task has been executed.
    // Not sure about what Python does, but at least it keeps a counter with the number of executed tasks,
    // and the backend has access to that.
}

/// The only scheduler backend implementation for now. It does basically nothing.
pub struct DummyBackend {}

#[allow(clippy::new_without_default)]
impl DummyBackend {
    pub fn new() -> Self {
        Self {}
    }
}

impl SchedulerBackend for DummyBackend {
    fn should_sync(&self) -> bool {
        false
    }

    #[allow(unused_variables)]
    fn sync(&mut self, scheduled_tasks: &mut BinaryHeap<ScheduledTask>) {
        unimplemented!()
    }
}

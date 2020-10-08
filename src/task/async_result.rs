/// An [`AsyncResult`] is a handle for the result of a task.
#[derive(Debug, Clone)]
pub struct AsyncResult {
    pub task_id: String,
}

impl AsyncResult {
    pub fn new(task_id: &str) -> Self {
        Self {
            task_id: task_id.into(),
        }
    }
}

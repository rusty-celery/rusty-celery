use super::Task;

/// Wraps the parameters and execution options for a single task invocation.
pub struct Signature<T>
where
    T: Task,
{
    pub params: T::Params,
}

impl<T> Signature<T>
where
    T: Task,
{
    pub fn new(params: T::Params) -> Self {
        Self { params }
    }

    pub fn task_name() -> &'static str {
        T::NAME
    }
}

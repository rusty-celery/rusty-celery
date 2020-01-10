//! Defines the celery protocol.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct TaskPayloadBody<T>(Vec<u8>, pub(crate) T, TaskPayloadBodyEmbed);

impl<T> TaskPayloadBody<T> {
    pub(crate) fn new(task: T) -> Self {
        Self(vec![], task, TaskPayloadBodyEmbed::default())
    }
}

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct TaskPayloadBodyEmbed {
    callbacks: Option<String>,
    errbacks: Option<String>,
    chain: Option<String>,
    chord: Option<String>,
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::error::Error;
    use crate::task::Task;

    #[derive(Serialize, Deserialize)]
    struct TestTask {
        a: i32,
    }

    #[async_trait]
    impl Task for TestTask {
        type Returns = ();

        async fn run(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn test_serialize_body() {
        let body = TaskPayloadBody::new(TestTask { a: 0 });
        let serialized = serde_json::to_string(&body).unwrap();
        assert_eq!(
            serialized,
            "[[],{\"a\":0},{\"callbacks\":null,\"errbacks\":null,\"chain\":null,\"chord\":null}]"
        );
    }
}

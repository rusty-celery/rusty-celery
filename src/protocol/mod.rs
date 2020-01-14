//! Defines the [celery protocol](http://docs.celeryproject.org/en/latest/internals/protocol.html).

use serde::{Deserialize, Serialize};

use crate::Error;

#[derive(Debug)]
pub struct Message {
    pub properties: MessageProperties,
    pub headers: MessageHeaders,
    pub raw_data: Vec<u8>,
}

pub trait TryIntoMessage {
    fn try_into_message(&self) -> Result<Message, Error>;
}

#[derive(Debug)]
pub struct MessageProperties {
    pub correlation_id: String,
    pub content_type: String,
    pub content_encoding: String,
    pub reply_to: Option<String>,
}

#[derive(Debug)]
pub struct MessageHeaders {
    pub id: String,
    pub task: String,
    pub lang: Option<String>,
    pub root_id: Option<String>,
    pub parent_id: Option<String>,
    pub group: Option<String>,
    pub meth: Option<String>,
    pub shadow: Option<String>,
    pub eta: Option<String>,
    pub expires: Option<String>,
    pub retries: Option<usize>,
    pub timelimit: (Option<usize>, Option<usize>),
    pub argsrepr: Option<String>,
    pub kwargsrepr: Option<String>,
    pub origin: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageBody<T>(Vec<u8>, pub T, MessageBodyEmbed);

impl<T> MessageBody<T> {
    pub fn new(task: T) -> Self {
        Self(vec![], task, MessageBodyEmbed::default())
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MessageBodyEmbed {
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
        let body = MessageBody::new(TestTask { a: 0 });
        let serialized = serde_json::to_string(&body).unwrap();
        assert_eq!(
            serialized,
            "[[],{\"a\":0},{\"callbacks\":null,\"errbacks\":null,\"chain\":null,\"chord\":null}]"
        );
    }
}

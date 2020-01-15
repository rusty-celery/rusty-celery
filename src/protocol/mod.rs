//! Defines the [celery protocol](http://docs.celeryproject.org/en/latest/internals/protocol.html).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Error;

struct Config {
    correlation_id: String,
    content_type: String,
    content_encoding: String,
    reply_to: Option<String>,
    task: String,
    raw_data: Vec<u8>,
}

pub struct MessageBuilder {
    config: Config,
}

impl MessageBuilder {
    pub fn new(task: &str, data: Vec<u8>) -> Self {
        let mut buffer = Uuid::encode_buffer();
        let uuid = Uuid::new_v4().to_hyphenated().encode_lower(&mut buffer);
        Self {
            config: Config {
                correlation_id: uuid.to_owned(),
                content_type: "application/json".into(),
                content_encoding: "utf-8".into(),
                reply_to: None,
                task: task.into(),
                raw_data: data,
            },
        }
    }

    pub fn build(self) -> Message {
        Message {
            properties: MessageProperties {
                correlation_id: self.config.correlation_id.clone(),
                content_type: self.config.content_type,
                content_encoding: self.config.content_encoding,
                reply_to: self.config.reply_to,
            },
            headers: MessageHeaders {
                id: self.config.correlation_id.clone(),
                task: self.config.task,
                ..Default::default()
            },
            raw_data: self.config.raw_data,
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct Message {
    pub properties: MessageProperties,
    pub headers: MessageHeaders,
    pub raw_data: Vec<u8>,
}

impl Message {
    pub fn builder(task: &str, data: Vec<u8>) -> MessageBuilder {
        MessageBuilder::new(task, data)
    }

    pub fn new(task: &str, data: Vec<u8>) -> Self {
        Self::builder(task, data).build()
    }
}

pub trait TryIntoMessage {
    fn try_into_message(&self) -> Result<Message, Error>;
}

#[derive(Eq, PartialEq, Debug)]
pub struct MessageProperties {
    pub correlation_id: String,
    pub content_type: String,
    pub content_encoding: String,
    pub reply_to: Option<String>,
}

#[derive(Eq, PartialEq, Debug, Default)]
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
    pub retries: Option<u32>,
    pub timelimit: (Option<u32>, Option<u32>),
    pub argsrepr: Option<String>,
    pub kwargsrepr: Option<String>,
    pub origin: Option<String>,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct MessageBody<T>(Vec<u8>, pub T, MessageBodyEmbed);

impl<T> MessageBody<T> {
    pub fn new(task: T) -> Self {
        Self(vec![], task, MessageBodyEmbed::default())
    }
}

#[derive(Eq, PartialEq, Debug, Default, Serialize, Deserialize)]
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
        const NAME: &'static str = "test";

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

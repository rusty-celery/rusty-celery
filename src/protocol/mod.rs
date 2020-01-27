//! Defines the [Celery protocol](http://docs.celeryproject.org/en/latest/internals/protocol.html).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Duration;
use uuid::Uuid;

use crate::error::Error;
use crate::task::{Task, TaskOptions, TaskSendOptions};

/// Create a message with a custom configuration.
pub struct MessageBuilder {
    message: Message,
}

impl MessageBuilder {
    /// Get a new `MessageBuilder` from a task.
    pub fn new<T: Task>(task: T) -> Result<Self, Error> {
        // Serialize the task into the message body.
        let body = MessageBody::new(task);
        let data = serde_json::to_vec(&body).unwrap();

        // Create random correlation id.
        let mut buffer = Uuid::encode_buffer();
        let uuid = Uuid::new_v4().to_hyphenated().encode_lower(&mut buffer);
        let correlation_id = uuid.to_owned();

        Ok(Self {
            message: Message {
                properties: MessageProperties {
                    correlation_id: correlation_id.clone(),
                    content_type: "application/json".into(),
                    content_encoding: "utf-8".into(),
                    reply_to: None,
                },
                headers: MessageHeaders {
                    id: correlation_id,
                    task: T::NAME.into(),
                    ..Default::default()
                },
                raw_data: data,
            },
        })
    }

    pub fn task_options(mut self, options: &TaskOptions) -> Self {
        self.message.headers.timelimit = (options.timeout, options.timeout);
        self
    }

    pub fn task_send_options(mut self, options: &TaskSendOptions) -> Self {
        self.message.headers.timelimit = (options.timeout, options.timeout);
        self
    }

    /// Get the `Message` with the custom configuration.
    pub fn build(self) -> Message {
        self.message
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct Message {
    pub properties: MessageProperties,
    pub headers: MessageHeaders,
    pub raw_data: Vec<u8>,
}

impl Message {
    pub fn builder<T: Task>(task: T) -> Result<MessageBuilder, Error> {
        MessageBuilder::new::<T>(task)
    }

    pub fn new<T: Task>(task: T) -> Result<Self, Error> {
        Ok(Self::builder(task)?.build())
    }

    /// Get the TTL countdown.
    pub fn countdown(&self) -> Option<Duration> {
        if let Some(eta) = self.headers.eta {
            let eta_millis = eta.timestamp_millis();
            if eta_millis < 0 {
                // Invalid ETA.
                return None;
            }
            let eta_millis = eta_millis as u64;
            match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(now) => {
                    let now_millis = now.as_millis() as u64;
                    if eta_millis < now_millis {
                        None
                    } else {
                        Some(Duration::from_millis(eta_millis - now_millis))
                    }
                }
                Err(_) => None,
            }
        } else {
            None
        }
    }

    /// Check if the message is expired.
    pub fn is_expired(&self) -> bool {
        if let Some(dt) = self.headers.expires {
            let expires_millis = dt.timestamp_millis();
            if expires_millis < 0 {
                // Invalid.
                return false;
            }
            let expires_millis = expires_millis as u64;
            if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
                let now_millis = now.as_millis() as u64;
                now_millis >= expires_millis
            } else {
                false
            }
        } else {
            false
        }
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
    pub eta: Option<DateTime<Utc>>,
    pub expires: Option<DateTime<Utc>>,
    pub retries: Option<u32>,
    pub timelimit: (Option<u32>, Option<u32>),
    pub argsrepr: Option<String>,
    pub kwargsrepr: Option<String>,
    pub origin: Option<String>,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct MessageBody<T>(Vec<u8>, pub T, MessageBodyEmbed);

impl<T> MessageBody<T>
where
    T: Task,
{
    pub fn new(task: T) -> Self {
        Self(vec![], task, MessageBodyEmbed::default())
    }

    pub fn from_raw_data(data: &[u8]) -> Result<Self, Error> {
        let value: Value = serde_json::from_slice(&data)?;
        if let Value::Array(ref vec) = value {
            if let [Value::Array(ref args), Value::Object(ref kwargs), Value::Object(ref embed)] =
                vec[..]
            {
                if !args.is_empty() {
                    // Non-empty args, need to try to coerce them into kwargs.
                    let mut kwargs = kwargs.clone();
                    let embed = embed.clone();
                    let arg_names = T::ARGS;
                    for (i, arg) in args.iter().enumerate() {
                        if let Some(arg_name) = arg_names.get(i) {
                            kwargs.insert((*arg_name).into(), arg.clone());
                        } else {
                            break;
                        }
                    }
                    return Ok(Self(
                        vec![],
                        serde_json::from_value::<T>(Value::Object(kwargs))?,
                        serde_json::from_value::<MessageBodyEmbed>(Value::Object(embed))?,
                    ));
                }
            }
        }
        Ok(serde_json::from_value::<Self>(value)?)
    }
}

#[derive(Eq, PartialEq, Debug, Default, Serialize, Deserialize)]
pub struct MessageBodyEmbed {
    #[serde(default)]
    callbacks: Option<String>,

    #[serde(default)]
    errbacks: Option<String>,

    #[serde(default)]
    chain: Option<String>,

    #[serde(default)]
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
        const ARGS: &'static [&'static str] = &["a"];

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

    #[test]
    fn test_deserialize_body_with_args() {
        let data = "[[1],{},{}]";
        let body = MessageBody::<TestTask>::from_raw_data(data.as_bytes()).unwrap();
        assert_eq!(body.1.a, 1);
    }
}

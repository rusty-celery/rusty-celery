//! Defines the Celery protocol.
//!
//! The top part of the protocol is the [`Message` struct](struct.Message.html), which builds on
//! top of the protocol for a broker. This is why a broker's [delivery
//! type](../trait.Broker.html#associatedtype.Delivery) must implement
//! [`TryIntoMessage`](trait.TryIntoMessage.html).

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
                raw_body: data,
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

/// A `Message` is the core of the Celery protocol and is built on top of a `Broker`'s protocol.
/// Every message corresponds to a task.
///
/// Note that the `raw_body` field is the serialized form of a [`MessageBody`](struct.MessageBody.html)
/// so that a worker can read the meta data of a message without having to deserialize the body
/// first.
#[derive(Eq, PartialEq, Debug)]
pub struct Message {
    /// Message properties correspond to the equivalent AMQP delivery properties.
    pub properties: MessageProperties,

    /// Message headers contain additional meta data pertaining to the Celery protocol.
    pub headers: MessageHeaders,

    /// A serialized [`MessageBody`](struct.MessageBody.html).
    pub raw_body: Vec<u8>,
}

impl Message {
    pub fn builder<T: Task>(task: T) -> Result<MessageBuilder, Error> {
        MessageBuilder::new::<T>(task)
    }

    pub fn new<T: Task>(task: T) -> Result<Self, Error> {
        Ok(Self::builder(task)?.build())
    }

    /// Try deserializing the body.
    pub fn body<T: Task>(&self) -> Result<MessageBody<T>, Error> {
        let value: Value = serde_json::from_slice(&self.raw_body)?;
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
                    return Ok(MessageBody(
                        vec![],
                        serde_json::from_value::<T>(Value::Object(kwargs))?,
                        serde_json::from_value::<MessageBodyEmbed>(Value::Object(embed))?,
                    ));
                }
            }
        }
        Ok(serde_json::from_value::<MessageBody<T>>(value)?)
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

/// Message meta data pertaining to the broker.
#[derive(Eq, PartialEq, Debug)]
pub struct MessageProperties {
    /// A unique ID associated with the task.
    pub correlation_id: String,

    /// The MIME type of the body.
    pub content_type: String,

    /// The encoding of the body.
    pub content_encoding: String,

    /// Used by the RPC backend when failures are reported by the parent process.
    pub reply_to: Option<String>,
}

/// Additional meta data pertaining to the Celery protocol.
#[derive(Eq, PartialEq, Debug, Default)]
pub struct MessageHeaders {
    /// The correlation ID of the task.
    pub id: String,

    /// The name of the task.
    pub task: String,

    /// The programming language associated with the task.
    pub lang: Option<String>,

    /// The first task in the work-flow.
    pub root_id: Option<String>,

    /// The ID of the task that called this task within a work-flow.
    pub parent_id: Option<String>,

    /// TODO
    pub group: Option<String>,

    /// Currently unused but could be used in the future to specify class+method pairs.
    pub meth: Option<String>,

    /// Modifies the task name that is used in logs.
    pub shadow: Option<String>,

    /// A future time after which the task should be executed.
    pub eta: Option<DateTime<Utc>>,

    /// A future time after which the task should be discarded if it hasn't executed
    /// yet.
    pub expires: Option<DateTime<Utc>>,

    /// The number of times the task has been retried without success.
    pub retries: Option<u32>,

    /// A tuple specifying the soft and hard time limits.
    pub timelimit: (Option<u32>, Option<u32>),

    /// A string representation of the positional arguments of the task.
    pub argsrepr: Option<String>,

    /// A string representation of the keyword arguments of the task.
    pub kwargsrepr: Option<String>,

    /// A string representing the node that produced the task.
    pub origin: Option<String>,
}

/// The body of a message. Contains the task itself as well as callback / errback
/// signatures and work-flow primitives.
#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct MessageBody<T>(Vec<u8>, pub(crate) T, pub(crate) MessageBodyEmbed);

impl<T> MessageBody<T>
where
    T: Task,
{
    pub fn new(task: T) -> Self {
        Self(vec![], task, MessageBodyEmbed::default())
    }

    pub fn parts(self) -> (T, MessageBodyEmbed) {
        (self.1, self.2)
    }
}

/// Contains callback / errback signatures and work-flow primitives.
#[derive(Eq, PartialEq, Debug, Default, Serialize, Deserialize)]
pub struct MessageBodyEmbed {
    /// An array of serialized signatures of tasks to call with the result of this task.
    #[serde(default)]
    pub callbacks: Option<Vec<String>>,

    /// An array of serialized signatures of tasks to call if this task results in an error.
    ///
    /// Note that `errbacks` work differently from `callbacks` because the error returned by
    /// a task may not be serializable. Therefore the `errbacks` tasks are passed the task ID
    /// instead of the error itself.
    #[serde(default)]
    pub errbacks: Option<Vec<String>>,

    /// An array of serialized signatures of the remaining tasks in the chain.
    #[serde(default)]
    pub chain: Option<Vec<String>>,

    /// The serialized signature of the chord callback.
    #[serde(default)]
    pub chord: Option<String>,
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
        let message = Message {
            properties: MessageProperties {
                correlation_id: "aaa".into(),
                content_type: "application/json".into(),
                content_encoding: "utf-8".into(),
                reply_to: None,
            },
            headers: MessageHeaders {
                id: "aaa".into(),
                task: "TestTask".into(),
                ..Default::default()
            },
            raw_body: Vec::from(&b"[[1],{},{}]"[..]),
        };
        let body = message.body::<TestTask>().unwrap();
        assert_eq!(body.1.a, 1);
    }
}

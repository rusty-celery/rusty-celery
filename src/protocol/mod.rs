//! Defines the Celery protocol.
//!
//! The top part of the protocol is the [`Message` struct](struct.Message.html), which builds on
//! top of the protocol for a broker. This is why a broker's [delivery
//! type](../broker/trait.Broker.html#associatedtype.Delivery) must implement
//! [`TryCreateMessage`](trait.TryCreateMessage.html).

use chrono::{self, DateTime, Utc};
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::SystemTime;
use tokio::time::Duration;
use uuid::Uuid;

use crate::error::ProtocolError;
use crate::task::{Task, TaskOptions, TaskSendOptions, TaskSignature};

/// Create a message with a custom configuration.
pub struct MessageBuilder {
    message: Message,
}

impl MessageBuilder {
    /// Create a new `MessageBuilder` with a given correlation ID, task name, and serialized body.
    pub fn new(correlation_id: String, task_name: String, raw_body: Vec<u8>) -> Self {
        Self {
            message: Message {
                properties: MessageProperties {
                    correlation_id: correlation_id.clone(),
                    content_type: "application/json".into(),
                    content_encoding: "utf-8".into(),
                    reply_to: None,
                },
                headers: MessageHeaders {
                    id: correlation_id,
                    task: task_name,
                    ..Default::default()
                },
                raw_body,
            },
        }
    }

    /// Get a new `MessageBuilder` from a task.
    pub fn from_task<T: Task>(task_sig: TaskSignature<T>) -> Result<Self, ProtocolError> {
        // Create random correlation id.
        let mut buffer = Uuid::encode_buffer();
        let uuid = Uuid::new_v4().to_hyphenated().encode_lower(&mut buffer);
        let correlation_id = uuid.to_owned();

        let body = MessageBody::new(task_sig);

        Ok(Self::new(
            correlation_id,
            T::NAME.into(),
            serde_json::to_vec(&body)?,
        ))
    }

    pub fn task_options(mut self, options: &TaskOptions) -> Self {
        self.message.headers.timelimit = (options.timeout, options.timeout);
        self
    }

    pub fn task_send_options(mut self, options: &TaskSendOptions) -> Self {
        self.message.headers.timelimit = (options.timeout, options.timeout);

        // Set ETA.
        if let Some(eta) = options.eta {
            self.message.headers.eta = Some(eta);
        } else if let Some(countdown) = options.countdown {
            let now = DateTime::<Utc>::from(SystemTime::now());
            let eta = now + chrono::Duration::seconds(countdown as i64);
            self.message.headers.eta = Some(eta);
        }

        // Set expiration time.
        if let Some(expires) = options.expires {
            self.message.headers.expires = Some(expires);
        } else if let Some(expires_in) = options.expires_in {
            let now = DateTime::<Utc>::from(SystemTime::now());
            let expires = now + chrono::Duration::seconds(expires_in as i64);
            self.message.headers.expires = Some(expires);
        }

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
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct Message {
    /// Message properties correspond to the equivalent AMQP delivery properties.
    pub properties: MessageProperties,

    /// Message headers contain additional meta data pertaining to the Celery protocol.
    pub headers: MessageHeaders,

    /// A serialized [`MessageBody`](struct.MessageBody.html).
    pub raw_body: Vec<u8>,
}

impl Message {
    pub fn builder<T: Task>(task_sig: TaskSignature<T>) -> Result<MessageBuilder, ProtocolError> {
        MessageBuilder::from_task(task_sig)
    }

    pub fn new<T: Task>(task_sig: TaskSignature<T>) -> Result<Self, ProtocolError> {
        Ok(Self::builder(task_sig)?.build())
    }

    /// Try deserializing the body.
    pub fn body<T: Task>(&self) -> Result<MessageBody<T>, ProtocolError> {
        let value: Value = serde_json::from_slice(&self.raw_body)?;
        debug!("Deserialized message body: {:?}", value);
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
                        serde_json::from_value::<T::Params>(Value::Object(kwargs))?,
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
            let now = DateTime::<Utc>::from(SystemTime::now());
            let countdown = (eta - now).num_milliseconds();
            if countdown < 0 {
                None
            } else {
                Some(Duration::from_millis(countdown as u64))
            }
        } else {
            None
        }
    }

    /// Check if the message is expired.
    pub fn is_expired(&self) -> bool {
        if let Some(expires) = self.headers.expires {
            let now = DateTime::<Utc>::from(SystemTime::now());
            (now - expires).num_milliseconds() >= 0
        } else {
            false
        }
    }
}

/// A trait for attempting to create a `Message` from `self`. This is required to be implemented
/// on a broker's `Delivery` type.
pub trait TryCreateMessage {
    fn try_create_message(&self) -> Result<Message, ProtocolError>;
}

impl TryCreateMessage for Message {
    fn try_create_message(&self) -> Result<Message, ProtocolError> {
        Ok(self.clone())
    }
}

/// Message meta data pertaining to the broker.
#[derive(Eq, PartialEq, Debug, Clone)]
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
#[derive(Eq, PartialEq, Debug, Default, Clone)]
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
pub struct MessageBody<T: Task>(Vec<u8>, pub(crate) T::Params, pub(crate) MessageBodyEmbed);

impl<T> MessageBody<T>
where
    T: Task,
{
    pub fn new(task_sig: TaskSignature<T>) -> Self {
        Self(vec![], task_sig.params, MessageBodyEmbed::default())
    }

    pub fn parts(self) -> (T::Params, MessageBodyEmbed) {
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
    use crate::error::TaskError;
    use crate::task::Task;

    #[derive(Clone, Serialize, Deserialize)]
    struct TestTaskParams {
        a: i32,
    }

    struct TestTask;

    #[async_trait]
    impl Task for TestTask {
        const NAME: &'static str = "test";
        const ARGS: &'static [&'static str] = &["a"];

        type Params = TestTaskParams;
        type Returns = ();

        fn within_app() -> Self {
            Self {}
        }

        async fn run(&self, _params: Self::Params) -> Result<(), TaskError> {
            Ok(())
        }
    }

    #[test]
    fn test_serialize_body() {
        let body = MessageBody::new(TaskSignature::<TestTask>::new(TestTaskParams { a: 0 }));
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

//! Defines the Celery protocol.
//!
//! The top part of the protocol is the [`Message` struct](struct.Message.html), which builds on
//! top of the protocol for a broker. This is why a broker's [delivery
//! type](../broker/trait.Broker.html#associatedtype.Delivery) must implement
//! [`TryCreateMessage`](trait.TryCreateMessage.html).

use chrono::{DateTime, Duration, Utc};
use log::debug;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::process;
use std::time::SystemTime;
use uuid::Uuid;

use crate::error::{FormatError, ProtocolError};
use crate::task::{Signature, Task};

static ORIGIN: Lazy<Option<String>> = Lazy::new(|| {
    hostname::get()
        .ok()
        .and_then(|sys_hostname| sys_hostname.into_string().ok())
        .map(|sys_hostname| format!("gen{}@{}", process::id(), sys_hostname))
});

/// Serialization formats supported for message body
pub enum MessageFormat {
    Json,
    Yaml,
    Pickle,
    MsgPack,
}

/// Create a message with a custom configuration.
pub struct MessageBuilder<T>
where
    T: Task,
{
    message: Message,
    params: Option<T::Params>,
}

impl<T> MessageBuilder<T>
where
    T: Task,
{
    /// Create a new `MessageBuilder` with a given task ID.
    pub fn new(id: String) -> Self {
        Self {
            message: Message {
                properties: MessageProperties {
                    correlation_id: id.clone(),
                    content_type: "application/json".into(),
                    content_encoding: "utf-8".into(),
                    reply_to: None,
                },
                headers: MessageHeaders {
                    id,
                    task: T::NAME.into(),
                    origin: ORIGIN.to_owned(),
                    ..Default::default()
                },
                raw_body: Vec::new(),
            },
            params: None,
        }
    }
    /// set which serialization method is used in the body
    /// if feature "extra_formats" is not enabled, this function
    /// does not do anything and json is used.
    #[cfg(any(test, feature = "extra_formats"))]
    pub fn serializer(mut self, format: MessageFormat) -> Self {
        use MessageFormat::*;
        let format_name = match format {
            Json => "application/json",
            Yaml => "application/x-yaml",
            Pickle => "application/x-python-serialize",
            MsgPack => "application/x-msgpack",
        };
        self.message.properties.content_type = format_name.into();
        self
    }

    pub fn time_limit(mut self, time_limit: u32) -> Self {
        self.message.headers.timelimit.1 = Some(time_limit);
        self
    }

    pub fn hard_time_limit(mut self, time_limit: u32) -> Self {
        self.message.headers.timelimit.0 = Some(time_limit);
        self
    }

    pub fn eta(mut self, eta: DateTime<Utc>) -> Self {
        self.message.headers.eta = Some(eta);
        self
    }

    pub fn countdown(self, countdown: u32) -> Self {
        let now = DateTime::<Utc>::from(SystemTime::now());
        let eta = now + Duration::seconds(countdown as i64);
        self.eta(eta)
    }

    pub fn expires(mut self, expires: DateTime<Utc>) -> Self {
        self.message.headers.expires = Some(expires);
        self
    }

    pub fn expires_in(self, expires_in: u32) -> Self {
        let now = DateTime::<Utc>::from(SystemTime::now());
        let expires = now + Duration::seconds(expires_in as i64);
        self.expires(expires)
    }

    pub fn params(mut self, params: T::Params) -> Self {
        self.params = Some(params);
        self
    }

    /// Get the `Message` with the custom configuration.
    pub fn build(mut self) -> Result<Message, ProtocolError> {
        if let Some(params) = self.params.take() {
            let body = MessageBody::<T>::new(params);

            let raw_body = match self.message.properties.content_type.as_str() {
                "application/json" => serde_json::to_vec(&body)?,
                #[cfg(any(test, feature = "extra_formats"))]
                "application/x-yaml" => serde_yaml::to_vec(&body)?,
                #[cfg(any(test, feature = "extra_formats"))]
                "application/x-python-serialize" => serde_pickle::to_vec(&body, false)?,
                #[cfg(any(test, feature = "extra_formats"))]
                "application/x-msgpack" => rmp_serde::to_vec(&body)?,
                _ => {
                    return Err(ProtocolError::BodySerializationError(FormatError::Unknown));
                }
            };
            self.message.raw_body = raw_body;
        };
        Ok(self.message)
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
    /// Try deserializing the body.
    pub fn body<T: Task>(&self) -> Result<MessageBody<T>, ProtocolError> {
        match self.properties.content_type.as_str() {
            "application/json" => {
                use serde_json::{from_slice, from_value, Value};
                let value: Value = from_slice(&self.raw_body)?;
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
                                from_value::<T::Params>(Value::Object(kwargs))?,
                                from_value::<MessageBodyEmbed>(Value::Object(embed))?,
                            ));
                        }
                    }
                }
                Ok(from_value::<MessageBody<T>>(value)?)
            }
            #[cfg(any(test, feature = "extra_formats"))]
            "application/x-yaml" => {
                use serde_yaml::{from_slice, from_value, Value};
                let value: Value = from_slice(&self.raw_body)?;
                debug!("Deserialized message body: {:?}", value);
                if let Value::Sequence(ref vec) = value {
                    if let [Value::Sequence(ref args), Value::Mapping(ref kwargs), Value::Mapping(ref embed)] =
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
                                from_value::<T::Params>(Value::Mapping(kwargs))?,
                                from_value::<MessageBodyEmbed>(Value::Mapping(embed))?,
                            ));
                        }
                    }
                }
                Ok(from_value(value)?)
            }
            #[cfg(any(test, feature = "extra_formats"))]
            "application/x-python-serialize" => {
                use serde_pickle::{from_slice, from_value, HashableValue, Value};
                let value: Value = from_slice(&self.raw_body)?;
                // debug!("Deserialized message body: {:?}", value);
                if let Value::List(ref vec) = value {
                    if let [Value::List(ref args), Value::Dict(ref kwargs), Value::Dict(ref embed)] =
                        vec[..]
                    {
                        if !args.is_empty() {
                            // Non-empty args, need to try to coerce them into kwargs.
                            let mut kwargs = kwargs.clone();
                            let embed = embed.clone();
                            let arg_names = T::ARGS;
                            for (i, arg) in args.iter().enumerate() {
                                if let Some(arg_name) = arg_names.get(i) {
                                    let key = HashableValue::String((*arg_name).into());
                                    kwargs.insert(key, arg.clone());
                                } else {
                                    break;
                                }
                            }
                            return Ok(MessageBody(
                                vec![],
                                from_value::<T::Params>(Value::Dict(kwargs))?,
                                from_value::<MessageBodyEmbed>(Value::Dict(embed))?,
                            ));
                        }
                    }
                }
                Ok(from_value(value)?)
            }
            #[cfg(any(test, feature = "extra_formats"))]
            "application/x-msgpack" => {
                use rmp_serde::from_slice;
                use rmpv::{ext::from_value, Value};
                let value: Value = from_slice(&self.raw_body)?;
                debug!("Deserialized message body: {:?}", value);
                if let Value::Array(ref vec) = value {
                    if let [Value::Array(ref args), Value::Map(ref kwargs), Value::Map(ref embed)] =
                        vec[..]
                    {
                        if !args.is_empty() {
                            // Non-empty args, need to try to coerce them into kwargs.
                            let mut kwargs = kwargs.clone();
                            let embed = embed.clone();
                            let arg_names = T::ARGS;
                            for (i, arg) in args.iter().enumerate() {
                                if let Some(arg_name) = arg_names.get(i) {
                                    // messagepack is storing the map as a vec where each item
                                    // is a tuple of (key, value). here we will look for an item
                                    // with the matching key and replace it, or insert a new entry
                                    // at the end of the vec
                                    let existing_entry = kwargs
                                        .iter()
                                        .enumerate()
                                        .filter(|(_, (key, _))| {
                                            if let Value::String(key) = key {
                                                if let Some(key) = key.as_str() {
                                                    key == *arg_name
                                                } else {
                                                    false
                                                }
                                            } else {
                                                false
                                            }
                                        })
                                        .map(|(i, _)| i)
                                        .next();
                                    if let Some(index) = existing_entry {
                                        kwargs[index] = ((*arg_name).into(), arg.clone());
                                    } else {
                                        kwargs.push(((*arg_name).into(), arg.clone()));
                                    }
                                } else {
                                    break;
                                }
                            }
                            return Ok(MessageBody(
                                vec![],
                                from_value::<T::Params>(Value::Map(kwargs))?,
                                from_value::<MessageBodyEmbed>(Value::Map(embed))?,
                            ));
                        }
                    }
                }
                Ok(from_value(value)?)
            }
            _ => Err(ProtocolError::BodySerializationError(FormatError::Unknown)),
        }
    }

    /// Get thet task ID.
    pub fn task_id(&self) -> &str {
        &self.headers.id
    }
}

impl<T> TryFrom<Signature<T>> for Message
where
    T: Task,
{
    type Error = ProtocolError;

    /// Get a new `MessageBuilder` from a task signature.
    fn try_from(mut task_sig: Signature<T>) -> Result<Self, Self::Error> {
        // Create random correlation id.
        let mut buffer = Uuid::encode_buffer();
        let uuid = Uuid::new_v4().to_hyphenated().encode_lower(&mut buffer);
        let id = uuid.to_owned();

        let mut builder = MessageBuilder::<T>::new(id);

        if let Some(time_limit) = task_sig.time_limit.take() {
            builder = builder.time_limit(time_limit);
        }

        if let Some(time_limit) = task_sig.hard_time_limit.take() {
            builder = builder.hard_time_limit(time_limit);
        }

        if let Some(countdown) = task_sig.countdown.take() {
            builder = builder.countdown(countdown);
        } else if task_sig.eta.is_some() {
            builder = builder.eta(task_sig.eta.take().unwrap());
        }

        if let Some(expires_in) = task_sig.expires_in.take() {
            builder = builder.expires_in(expires_in);
        } else if task_sig.expires.is_some() {
            builder = builder.expires(task_sig.expires.take().unwrap());
        }

        builder.params(task_sig.params).build()
    }
}

/// A trait for attempting to create a `Message` from `self`. This will be implemented
/// by types that can act like message "factories", like for instance the `Signature` type.
pub trait TryCreateMessage {
    fn try_create_message(&self) -> Result<Message, ProtocolError>;
}

impl<T> TryCreateMessage for Signature<T>
where
    T: Task + Clone,
{
    /// Creating a message from a signature without consuming the signature requires cloning it.
    /// For one-shot conversions, directly use `Message::try_from` instead.
    fn try_create_message(&self) -> Result<Message, ProtocolError> {
        Message::try_from(self.clone())
    }
}

/// A trait for attempting to deserialize a `Message` from `self`. This is required to be implemented
/// on a broker's `Delivery` type.
pub trait TryDeserializeMessage {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError>;
}

/// Message meta data pertaining to the broker.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct MessageProperties {
    /// A unique ID associated with the task, usually the same as `MessageHeaders::id`.
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
    /// A unique ID of the task.
    pub id: String,

    /// The name of the task.
    pub task: String,

    /// The programming language associated with the task.
    pub lang: Option<String>,

    /// The first task in the work-flow.
    pub root_id: Option<String>,

    /// The ID of the task that called this task within a work-flow.
    pub parent_id: Option<String>,

    /// The unique ID of the task's group, if this task is a member.
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

    /// A tuple specifying the hard and soft time limits, respectively.
    ///
    /// *Note that as of writting this, the Python celery docs actually have a typo where it says
    /// these are reversed.*
    pub timelimit: (Option<u32>, Option<u32>),

    /// A string representation of the positional arguments of the task.
    pub argsrepr: Option<String>,

    /// A string representation of the keyword arguments of the task.
    pub kwargsrepr: Option<String>,

    /// A string representing the nodename of the process that produced the task.
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
    pub fn new(params: T::Params) -> Self {
        Self(vec![], params, MessageBodyEmbed::default())
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
    use crate::task::{Request, Task, TaskOptions};

    #[derive(Clone, Serialize, Deserialize)]
    struct TestTaskParams {
        a: i32,
    }

    struct TestTask {
        request: Request<Self>,
        options: TaskOptions,
    }

    #[async_trait]
    impl Task for TestTask {
        const NAME: &'static str = "test";
        const ARGS: &'static [&'static str] = &["a"];

        type Params = TestTaskParams;
        type Returns = ();

        fn from_request(request: Request<Self>, options: TaskOptions) -> Self {
            Self { request, options }
        }

        fn request(&self) -> &Request<Self> {
            &self.request
        }

        fn options(&self) -> &TaskOptions {
            &self.options
        }

        async fn run(&self, _params: Self::Params) -> Result<(), TaskError> {
            Ok(())
        }
    }

    const JSON: &str =
        "[[],{\"a\":4},{\"callbacks\":null,\"errbacks\":null,\"chain\":null,\"chord\":null}]";
    #[test]
    fn test_serialize_body() {
        let body = MessageBody::<TestTask>::new(TestTaskParams { a: 4 });
        let serialized = serde_json::to_string(&body).unwrap();
        assert_eq!(serialized, JSON);
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
            raw_body: Vec::from(&JSON[..]),
        };
        let body = message.body::<TestTask>().unwrap();
        assert_eq!(body.1.a, 4);
    }

    const YAML: &str = "---\n- []\n- a: 4\n- callbacks: ~\n  errbacks: ~\n  chain: ~\n  chord: ~";

    #[test]
    fn test_yaml_serialize_body() {
        let body = MessageBody::<TestTask>::new(TestTaskParams { a: 4 });
        let serialized = serde_yaml::to_string(&body).unwrap();
        assert_eq!(serialized, YAML);
    }

    #[test]
    fn test_yaml_deserialize_body_with_args() {
        let message = Message {
            properties: MessageProperties {
                correlation_id: "aaa".into(),
                content_type: "application/x-yaml".into(),
                content_encoding: "utf-8".into(),
                reply_to: None,
            },
            headers: MessageHeaders {
                id: "aaa".into(),
                task: "TestTask".into(),
                ..Default::default()
            },
            raw_body: Vec::from(&YAML[..]),
        };
        let body = message.body::<TestTask>().unwrap();
        assert_eq!(body.1.a, 4);
    }

    const PICKLE: &[u8] = b"\x80\x02(]}(X\x01\x00\x00\x00aJ\x04\x00\x00\x00u}(X\x09\x00\x00\x00callbacksNX\x08\x00\x00\x00errbacksNX\x05\x00\x00\x00chainNX\x05\x00\x00\x00chordNut.";
    #[test]
    fn test_pickle_serialize_body() {
        let body = MessageBody::<TestTask>::new(TestTaskParams { a: 4 });
        let serialized = serde_pickle::to_vec(&body, false).unwrap();
        // println!("{}", String::from_utf8(serialized.split_off(1)).unwrap());
        assert_eq!(serialized, PICKLE.to_vec());
    }

    #[test]
    fn test_pickle_deserialize_body_with_args() {
        let message = Message {
            properties: MessageProperties {
                correlation_id: "aaa".into(),
                content_type: "application/x-python-serialize".into(),
                content_encoding: "utf-8".into(),
                reply_to: None,
            },
            headers: MessageHeaders {
                id: "aaa".into(),
                task: "TestTask".into(),
                ..Default::default()
            },
            raw_body: PICKLE.to_vec(),
        };
        let body = message.body::<TestTask>().unwrap();
        assert_eq!(body.1.a, 4);
    }

    const MSGPACK: &[u8] = &[147, 144, 145, 4, 148, 192, 192, 192, 192];
    #[test]
    fn test_msgpack_serialize_body() {
        let body = MessageBody::<TestTask>::new(TestTaskParams { a: 4 });
        let serialized = rmp_serde::to_vec(&body).unwrap();
        // println!("{:?}", serialized);
        assert_eq!(serialized, MSGPACK);
    }

    #[test]
    fn test_msgpack_deserialize_body_with_args() {
        let message = Message {
            properties: MessageProperties {
                correlation_id: "aaa".into(),
                content_type: "application/x-msgpack".into(),
                content_encoding: "utf-8".into(),
                reply_to: None,
            },
            headers: MessageHeaders {
                id: "aaa".into(),
                task: "TestTask".into(),
                ..Default::default()
            },
            raw_body: MSGPACK.to_vec(),
        };
        let body = message.body::<TestTask>().unwrap();
        assert_eq!(body.1.a, 4);
    }
}

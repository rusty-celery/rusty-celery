//! Defines the Celery protocol.
//!
//! The top part of the protocol is the [`Message`] struct, which builds on
//! top of the protocol for a broker. This is why a broker's [`Delivery`](crate::broker::Broker::Delivery)
//! type must implement [`TryCreateMessage`].

use base64::{
    alphabet,
    engine::{general_purpose::PAD, GeneralPurpose},
    Engine,
};
use chrono::{DateTime, Duration, Utc};
use log::{debug, warn};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, from_value, json, Value};
use std::convert::TryFrom;
use std::process;
use std::time::SystemTime;
use uuid::Uuid;

use crate::error::{ContentTypeError, ProtocolError};
use crate::task::{Signature, Task};

pub(crate) const ENGINE: GeneralPurpose = GeneralPurpose::new(&alphabet::STANDARD, PAD);

static ORIGIN: Lazy<Option<String>> = Lazy::new(|| {
    hostname::get()
        .ok()
        .and_then(|sys_hostname| sys_hostname.into_string().ok())
        .map(|sys_hostname| format!("gen{}@{}", process::id(), sys_hostname))
});

/// Serialization formats supported for message body.
#[derive(Default, Copy, Clone)]
pub enum MessageContentType {
    #[default]
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
    /// Set which serialization method is used in the body.
    ///
    /// JSON is the default, and is also the only option unless the feature "extra_content_types" is enabled.
    #[cfg(any(test, feature = "extra_content_types"))]
    pub fn content_type(mut self, content_type: MessageContentType) -> Self {
        use MessageContentType::*;
        let content_type_name = match content_type {
            Json => "application/json",
            Yaml => "application/x-yaml",
            Pickle => "application/x-python-serialize",
            MsgPack => "application/x-msgpack",
        };
        self.message.properties.content_type = content_type_name.into();
        self
    }

    pub fn content_encoding(mut self, content_encoding: String) -> Self {
        self.message.properties.content_encoding = content_encoding;
        self
    }

    pub fn correlation_id(mut self, correlation_id: String) -> Self {
        self.message.properties.correlation_id = correlation_id;
        self
    }

    pub fn reply_to(mut self, reply_to: String) -> Self {
        self.message.properties.reply_to = Some(reply_to);
        self
    }

    pub fn id(mut self, id: String) -> Self {
        self.message.headers.id = id;
        self
    }

    pub fn task(mut self, task: String) -> Self {
        self.message.headers.task = task;
        self
    }

    pub fn lang(mut self, lang: String) -> Self {
        self.message.headers.lang = Some(lang);
        self
    }

    pub fn root_id(mut self, root_id: String) -> Self {
        self.message.headers.root_id = Some(root_id);
        self
    }

    pub fn parent_id(mut self, parent_id: String) -> Self {
        self.message.headers.parent_id = Some(parent_id);
        self
    }

    pub fn group(mut self, group: String) -> Self {
        self.message.headers.group = Some(group);
        self
    }

    pub fn meth(mut self, meth: String) -> Self {
        self.message.headers.meth = Some(meth);
        self
    }

    pub fn shadow(mut self, shadow: String) -> Self {
        self.message.headers.shadow = Some(shadow);
        self
    }

    pub fn retries(mut self, retries: u32) -> Self {
        self.message.headers.retries = Some(retries);
        self
    }

    pub fn argsrepr(mut self, argsrepr: String) -> Self {
        self.message.headers.argsrepr = Some(argsrepr);
        self
    }

    pub fn kwargsrepr(mut self, kwargsrepr: String) -> Self {
        self.message.headers.kwargsrepr = Some(kwargsrepr);
        self
    }

    pub fn origin(mut self, origin: String) -> Self {
        self.message.headers.origin = Some(origin);
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
                #[cfg(any(test, feature = "extra_content_types"))]
                "application/x-yaml" => {
                    let mut vec = Vec::with_capacity(128);
                    serde_yaml::to_writer(&mut vec, &body)?;
                    vec
                }
                #[cfg(any(test, feature = "extra_content_types"))]
                "application/x-python-serialize" => {
                    serde_pickle::to_vec(&body, serde_pickle::SerOptions::new())?
                }
                #[cfg(any(test, feature = "extra_content_types"))]
                "application/x-msgpack" => rmp_serde::to_vec(&body)?,
                _ => {
                    return Err(ProtocolError::BodySerializationError(
                        ContentTypeError::Unknown,
                    ));
                }
            };
            self.message.raw_body = raw_body;
        };
        Ok(self.message)
    }
}

/// A [`Message`] is the core of the Celery protocol and is built on top of a [`Broker`](crate::broker::Broker)'s protocol.
/// Every message corresponds to a task.
///
/// Note that the [`raw_body`](Message::raw_body) field is the serialized form of a [`MessageBody`]
/// so that a worker can read the meta data of a message without having to deserialize the body
/// first.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct Message {
    /// Message properties correspond to the equivalent AMQP delivery properties.
    pub properties: MessageProperties,

    /// Message headers contain additional meta data pertaining to the Celery protocol.
    pub headers: MessageHeaders,

    /// A serialized [`MessageBody`].
    pub raw_body: Vec<u8>,
}

impl Message {
    /// Try deserializing the body.
    pub fn body<T: Task>(&self) -> Result<MessageBody<T>, ProtocolError> {
        match self.properties.content_type.as_str() {
            "application/json" => {
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
            #[cfg(any(test, feature = "extra_content_types"))]
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
            #[cfg(any(test, feature = "extra_content_types"))]
            "application/x-python-serialize" => {
                use serde_pickle::{from_slice, from_value, DeOptions, HashableValue, Value};
                let value: Value = from_slice(&self.raw_body, DeOptions::new())?;
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
            #[cfg(any(test, feature = "extra_content_types"))]
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
            _ => Err(ProtocolError::BodySerializationError(
                ContentTypeError::Unknown,
            )),
        }
    }

    /// Get the task ID.
    pub fn task_id(&self) -> &str {
        &self.headers.id
    }

    pub fn json_serialized(&self) -> Result<Vec<u8>, ProtocolError> {
        let root_id = match &self.headers.root_id {
            Some(root_id) => json!(root_id.clone()),
            None => Value::Null,
        };
        let reply_to = match &self.properties.reply_to {
            Some(reply_to) => json!(reply_to.clone()),
            None => Value::Null,
        };
        let eta = match self.headers.eta {
            Some(time) => json!(time.to_rfc3339()),
            None => Value::Null,
        };
        let expires = match self.headers.expires {
            Some(time) => json!(time.to_rfc3339()),
            None => Value::Null,
        };
        let mut buffer = Uuid::encode_buffer();
        let uuid = Uuid::new_v4().hyphenated().encode_lower(&mut buffer);
        let delivery_tag = uuid.to_owned();
        let msg_json_value = json!({
            "body": ENGINE.encode(self.raw_body.clone()),
            "content-encoding": self.properties.content_encoding.clone(),
            "content-type": self.properties.content_type.clone(),
            "headers": {
                "id": self.headers.id.clone(),
                "task": self.headers.task.clone(),
                "lang": self.headers.lang.clone(),
                "root_id": root_id,
                "parent_id": self.headers.parent_id.clone(),
                "group": self.headers.group.clone(),
                "meth": self.headers.meth.clone(),
                "shadow": self.headers.shadow.clone(),
                "eta": eta,
                "expires": expires,
                "retries": self.headers.retries.clone(),
                "timelimit": self.headers.timelimit.clone(),
                "argsrepr": self.headers.argsrepr.clone(),
                "kwargsrepr": self.headers.kwargsrepr.clone(),
                "origin": self.headers.origin.clone()
            },
            "properties": json!({
                "correlation_id": self.properties.correlation_id.clone(),
                "reply_to": reply_to,
                "delivery_tag": delivery_tag,
                "body_encoding": "base64",
            })
        });
        let res = serde_json::to_string(&msg_json_value)?;
        Ok(res.into_bytes())
    }
}

impl<T> TryFrom<Signature<T>> for Message
where
    T: Task,
{
    type Error = ProtocolError;

    /// Get a new [`MessageBuilder`] from a task signature.
    fn try_from(mut task_sig: Signature<T>) -> Result<Self, Self::Error> {
        // Create random correlation id.
        let mut buffer = Uuid::encode_buffer();
        let uuid = Uuid::new_v4().hyphenated().encode_lower(&mut buffer);
        let id = uuid.to_owned();

        let mut builder = MessageBuilder::<T>::new(id);

        // 'countdown' arbitrarily takes priority over 'eta'.
        if let Some(countdown) = task_sig.countdown.take() {
            builder = builder.countdown(countdown);
            if task_sig.eta.is_some() {
                warn!(
                    "Task {} specified both a 'countdown' and an 'eta'. Ignoring 'eta'.",
                    T::NAME
                )
            }
        } else if let Some(eta) = task_sig.eta.take() {
            builder = builder.eta(eta);
        }

        // 'expires_in' arbitrarily takes priority over 'expires'.
        if let Some(expires_in) = task_sig.expires_in.take() {
            builder = builder.expires_in(expires_in);
            if task_sig.expires.is_some() {
                warn!(
                    "Task {} specified both 'expires_in' and 'expires'. Ignoring 'expires'.",
                    T::NAME
                )
            }
        } else if let Some(expires) = task_sig.expires.take() {
            builder = builder.expires(expires);
        }

        #[cfg(any(test, feature = "extra_content_types"))]
        if let Some(content_type) = task_sig.options.content_type {
            builder = builder.content_type(content_type);
        }

        if let Some(time_limit) = task_sig.options.time_limit.take() {
            builder = builder.time_limit(time_limit);
        }

        if let Some(time_limit) = task_sig.options.hard_time_limit.take() {
            builder = builder.hard_time_limit(time_limit);
        }

        builder.params(task_sig.params).build()
    }
}

/// A trait for attempting to create a [`Message`] from `self`. This will be implemented
/// by types that can act like message "factories", like for instance the
/// [`Signature`](crate::task::Signature) type.
pub trait TryCreateMessage {
    fn try_create_message(&self) -> Result<Message, ProtocolError>;
}

impl<T> TryCreateMessage for Signature<T>
where
    T: Task + Clone,
{
    /// Creating a message from a signature without consuming the signature requires cloning it.
    /// For one-shot conversions, directly use [`Message::try_from`] instead.
    fn try_create_message(&self) -> Result<Message, ProtocolError> {
        Message::try_from(self.clone())
    }
}

/// A trait for attempting to deserialize a [`Message`] from `self`. This is required to be implemented
/// on a broker's [`Delivery`](crate::broker::Broker::Delivery) type.
pub trait TryDeserializeMessage {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError>;
}

/// Message meta data pertaining to the broker.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct MessageProperties {
    /// A unique ID associated with the task, usually the same as [`MessageHeaders::id`].
    pub correlation_id: String,

    /// The MIME type of the body.
    pub content_type: String,

    /// The encoding of the body.
    pub content_encoding: String,

    /// Used by the RPC backend when failures are reported by the parent process.
    pub reply_to: Option<String>,
}

/// Additional meta data pertaining to the Celery protocol.
#[derive(Eq, PartialEq, Debug, Default, Deserialize, Clone)]
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
    /// *Note that as of writing this, the Python celery docs actually have a typo where it says
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BodyEncoding {
    Base64,
}
#[derive(Debug, Clone, Deserialize)]
pub struct DeliveryProperties {
    pub correlation_id: String,
    pub reply_to: Option<String>,
    pub delivery_tag: String,
    pub body_encoding: BodyEncoding,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Delivery {
    pub body: String,
    #[serde(rename = "content-encoding")]
    pub content_encoding: String,
    #[serde(rename = "content-type")]
    pub content_type: String,
    pub headers: MessageHeaders,
    pub properties: DeliveryProperties,
}

impl TryDeserializeMessage for Delivery {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        let raw_body = match self.properties.body_encoding {
            BodyEncoding::Base64 => ENGINE
                .decode(self.body.clone())
                .map_err(|e| ProtocolError::InvalidProperty(format!("body error: {e}")))?,
        };
        Ok(Message {
            properties: MessageProperties {
                correlation_id: self.properties.correlation_id.clone(),
                content_type: self.content_type.clone(),
                content_encoding: self.content_encoding.clone(),
                reply_to: self.properties.reply_to.clone(),
            },
            headers: MessageHeaders {
                id: self.headers.id.clone(),
                task: self.headers.task.clone(),
                lang: self.headers.lang.clone(),
                root_id: self.headers.root_id.clone(),
                parent_id: self.headers.parent_id.clone(),
                group: self.headers.group.clone(),
                meth: self.headers.meth.clone(),
                shadow: self.headers.shadow.clone(),
                eta: self.headers.eta,
                expires: self.headers.expires,
                retries: self.headers.retries,
                timelimit: self.headers.timelimit,
                argsrepr: self.headers.argsrepr.clone(),
                kwargsrepr: self.headers.kwargsrepr.clone(),
                origin: self.headers.origin.clone(),
            },
            raw_body,
        })
    }
}

#[cfg(test)]
mod tests;

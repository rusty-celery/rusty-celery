use amq_protocol_types::AMQPValue;
use async_trait::async_trait;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Queue};
use std::collections::HashMap;

use super::Broker;
use crate::protocol::{Message, MessageBody, MessageHeaders, MessageProperties, TryIntoMessage};
use crate::{Error, ErrorKind, Task};

struct Config {
    broker_url: String,
    prefetch_count: Option<u16>,
    queues: HashMap<String, QueueDeclareOptions>,
}

pub struct AMQPBrokerBuilder {
    config: Config,
}

impl AMQPBrokerBuilder {
    pub fn new(broker_url: &str) -> Self {
        Self {
            config: Config {
                broker_url: broker_url.into(),
                prefetch_count: Some(1),
                queues: HashMap::new(),
            },
        }
    }

    pub fn prefetch_count(mut self, prefetch_count: Option<u16>) -> Self {
        self.config.prefetch_count = prefetch_count;
        self
    }

    pub fn queue(mut self, name: &str) -> Self {
        self.config.queues.insert(
            name.into(),
            QueueDeclareOptions {
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: false,
            },
        );
        self
    }

    pub async fn build(self) -> Result<AMQPBroker, Error> {
        let conn =
            Connection::connect(&self.config.broker_url, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;
        if let Some(prefetch_count) = self.config.prefetch_count {
            channel
                .basic_qos(prefetch_count, BasicQosOptions::default())
                .await?;
        }
        let mut queues: HashMap<String, Queue> = HashMap::new();
        for (queue_name, queue_options) in &self.config.queues {
            let queue = channel
                .queue_declare(queue_name, queue_options.clone(), FieldTable::default())
                .await?;
            queues.insert(queue_name.into(), queue);
        }
        Ok(AMQPBroker { channel, queues })
    }
}

pub struct AMQPBroker {
    channel: Channel,
    queues: HashMap<String, Queue>,
}

impl AMQPBroker {
    pub fn builder(broker_url: &str) -> AMQPBrokerBuilder {
        AMQPBrokerBuilder::new(broker_url)
    }
}

#[async_trait]
impl Broker for AMQPBroker {
    type Delivery = lapin::message::Delivery;
    type DeliveryError = lapin::Error;
    type Consumer = lapin::Consumer;
    type ConsumerIterator = lapin::ConsumerIterator;

    async fn consume(&self, queue: &str) -> Result<Self::Consumer, Error> {
        let queue = self
            .queues
            .get(queue)
            .ok_or_else::<Error, _>(|| ErrorKind::UnknownQueueError(queue.into()).into())?;
        self.channel
            .basic_consume(
                queue,
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| e.into())
    }

    async fn ack(&self, delivery: Self::Delivery) -> Result<(), Error> {
        self.channel
            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
            .await
            .map_err(|e| e.into())
    }

    async fn send_task<T: Task>(&self, body: MessageBody<T>, queue: &str) -> Result<(), Error> {
        self.channel
            .basic_publish(
                "",
                queue,
                BasicPublishOptions::default(),
                serde_json::to_vec(&body)?,
                BasicProperties::default(),
            )
            .await?;
        Ok(())
    }
}

impl TryIntoMessage for lapin::message::Delivery {
    fn try_into_message(&self) -> Result<Message, Error> {
        let headers = self
            .properties
            .headers()
            .as_ref()
            .ok_or_else::<Error, _>(|| {
                ErrorKind::AMQPMessageParseError("missing headers".into()).into()
            })?;
        Ok(Message {
            properties: MessageProperties {
                correlation_id: self
                    .properties
                    .correlation_id()
                    .as_ref()
                    .map(|v| v.to_string())
                    .ok_or_else::<Error, _>(|| {
                        ErrorKind::AMQPMessageParseError("missing correlation_id".into()).into()
                    })?,
                content_type: self
                    .properties
                    .content_type()
                    .as_ref()
                    .map(|v| v.to_string())
                    .ok_or_else::<Error, _>(|| {
                        ErrorKind::AMQPMessageParseError("missing content_type".into()).into()
                    })?,
                content_encoding: self
                    .properties
                    .content_encoding()
                    .as_ref()
                    .map(|v| v.to_string())
                    .ok_or_else::<Error, _>(|| {
                        ErrorKind::AMQPMessageParseError("missing content_encoding".into()).into()
                    })?,
                reply_to: self.properties.reply_to().as_ref().map(|v| v.to_string()),
            },
            headers: MessageHeaders {
                id: headers
                    .inner()
                    .get("id")
                    .and_then(|v| match v {
                        AMQPValue::ShortString(s) => Some(s.to_string()),
                        AMQPValue::LongString(s) => Some(s.to_string()),
                        _ => None,
                    })
                    .ok_or_else::<Error, _>(|| {
                        ErrorKind::AMQPMessageParseError("invalid or missing 'id'".into()).into()
                    })?,
                task: headers
                    .inner()
                    .get("task")
                    .and_then(|v| match v {
                        AMQPValue::ShortString(s) => Some(s.to_string()),
                        AMQPValue::LongString(s) => Some(s.to_string()),
                        _ => None,
                    })
                    .ok_or_else::<Error, _>(|| {
                        ErrorKind::AMQPMessageParseError("invalid or missing 'task'".into()).into()
                    })?,
                lang: headers.inner().get("task").and_then(|v| match v {
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    AMQPValue::LongString(s) => Some(s.to_string()),
                    _ => None,
                }),
                root_id: headers.inner().get("root_id").and_then(|v| match v {
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    AMQPValue::LongString(s) => Some(s.to_string()),
                    _ => None,
                }),
                parent_id: headers.inner().get("parent_id").and_then(|v| match v {
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    AMQPValue::LongString(s) => Some(s.to_string()),
                    _ => None,
                }),
                group: headers.inner().get("group").and_then(|v| match v {
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    AMQPValue::LongString(s) => Some(s.to_string()),
                    _ => None,
                }),
                meth: headers.inner().get("meth").and_then(|v| match v {
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    AMQPValue::LongString(s) => Some(s.to_string()),
                    _ => None,
                }),
                shadow: headers.inner().get("shadow").and_then(|v| match v {
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    AMQPValue::LongString(s) => Some(s.to_string()),
                    _ => None,
                }),
                eta: headers.inner().get("eta").and_then(|v| match v {
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    AMQPValue::LongString(s) => Some(s.to_string()),
                    _ => None,
                }),
                expires: headers.inner().get("expires").and_then(|v| match v {
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    AMQPValue::LongString(s) => Some(s.to_string()),
                    _ => None,
                }),
                retries: headers.inner().get("retries").and_then(|v| match v {
                    AMQPValue::ShortShortInt(n) => Some(*n as usize),
                    AMQPValue::ShortShortUInt(n) => Some(*n as usize),
                    AMQPValue::ShortInt(n) => Some(*n as usize),
                    AMQPValue::ShortUInt(n) => Some(*n as usize),
                    AMQPValue::LongInt(n) => Some(*n as usize),
                    AMQPValue::LongUInt(n) => Some(*n as usize),
                    AMQPValue::LongLongInt(n) => Some(*n as usize),
                    _ => None,
                }),
                timelimit: headers
                    .inner()
                    .get("timelimit")
                    .and_then(|v| match v {
                        AMQPValue::FieldArray(a) => {
                            let a = a.as_slice().to_vec();
                            if a.len() == 2 {
                                let soft = match a[0] {
                                    AMQPValue::ShortShortInt(n) => Some(n as usize),
                                    AMQPValue::ShortShortUInt(n) => Some(n as usize),
                                    AMQPValue::ShortInt(n) => Some(n as usize),
                                    AMQPValue::ShortUInt(n) => Some(n as usize),
                                    AMQPValue::LongInt(n) => Some(n as usize),
                                    AMQPValue::LongUInt(n) => Some(n as usize),
                                    AMQPValue::LongLongInt(n) => Some(n as usize),
                                    _ => None,
                                };
                                let hard = match a[1] {
                                    AMQPValue::ShortShortInt(n) => Some(n as usize),
                                    AMQPValue::ShortShortUInt(n) => Some(n as usize),
                                    AMQPValue::ShortInt(n) => Some(n as usize),
                                    AMQPValue::ShortUInt(n) => Some(n as usize),
                                    AMQPValue::LongInt(n) => Some(n as usize),
                                    AMQPValue::LongUInt(n) => Some(n as usize),
                                    AMQPValue::LongLongInt(n) => Some(n as usize),
                                    _ => None,
                                };
                                Some((soft, hard))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .unwrap_or((None, None)),
                argsrepr: headers.inner().get("argsrepr").and_then(|v| match v {
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    AMQPValue::LongString(s) => Some(s.to_string()),
                    _ => None,
                }),
                kwargsrepr: headers.inner().get("kwargsrepr").and_then(|v| match v {
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    AMQPValue::LongString(s) => Some(s.to_string()),
                    _ => None,
                }),
                origin: headers.inner().get("origin").and_then(|v| match v {
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    AMQPValue::LongString(s) => Some(s.to_string()),
                    _ => None,
                }),
            },
            raw_data: self.data.clone(),
        })
    }
}

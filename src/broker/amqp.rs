//! AMQP broker.

use amq_protocol_types::{AMQPValue, FieldArray};
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::executor;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Queue};
use log::debug;
use std::collections::HashMap;

use super::{Broker, BrokerBuilder};
use crate::protocol::{Message, MessageHeaders, MessageProperties, TryIntoMessage};
use crate::{Error, ErrorKind};

struct Config {
    broker_url: String,
    prefetch_count: u16,
    queues: HashMap<String, QueueDeclareOptions>,
}

/// Builds an AMQP broker with a custom configuration.
pub struct AMQPBrokerBuilder {
    config: Config,
}

impl AMQPBrokerBuilder {
    /// Create a new `AMQPBrokerBuilder`.
    pub fn new(broker_url: &str) -> Self {
        Self {
            config: Config {
                broker_url: broker_url.into(),
                prefetch_count: 10,
                queues: HashMap::new(),
            },
        }
    }
}

impl BrokerBuilder for AMQPBrokerBuilder {
    type Broker = AMQPBroker;

    /// Set the worker [prefetch
    /// count](https://www.rabbitmq.com/confirms.html#channel-qos-prefetch).
    fn prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.config.prefetch_count = prefetch_count;
        self
    }

    /// Add / register a queue.
    fn queue(mut self, name: &str) -> Self {
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

    /// Build an `AMQPBroker`.
    fn build(self) -> Result<AMQPBroker, Error> {
        let conn = executor::block_on(Connection::connect(
            &self.config.broker_url,
            ConnectionProperties::default(),
        ))?;
        let channel = executor::block_on(conn.create_channel())?;
        let mut queues: HashMap<String, Queue> = HashMap::new();
        for (queue_name, queue_options) in &self.config.queues {
            let queue = executor::block_on(channel.queue_declare(
                queue_name,
                queue_options.clone(),
                FieldTable::default(),
            ))?;
            queues.insert(queue_name.into(), queue);
        }
        let broker = AMQPBroker {
            channel,
            queues,
            prefetch_count: std::sync::Mutex::new(self.config.prefetch_count),
        };
        executor::block_on(broker.set_prefetch_count(self.config.prefetch_count))?;
        Ok(broker)
    }
}

/// An AMQP broker.
pub struct AMQPBroker {
    channel: Channel,
    queues: HashMap<String, Queue>,
    prefetch_count: std::sync::Mutex<u16>,
}

impl AMQPBroker {
    async fn set_prefetch_count(&self, prefetch_count: u16) -> Result<(), Error> {
        debug!("Setting prefetch count to {}", prefetch_count);
        self.channel
            .basic_qos(prefetch_count, BasicQosOptions { global: true })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Broker for AMQPBroker {
    type Builder = AMQPBrokerBuilder;
    type Delivery = lapin::message::Delivery;
    type DeliveryError = lapin::Error;
    type DeliveryStream = lapin::Consumer;

    /// Get an `AMQPBrokerBuilder` for creating an AMQP broker with a custom configuration.
    fn builder(broker_url: &str) -> AMQPBrokerBuilder {
        AMQPBrokerBuilder::new(broker_url)
    }

    async fn consume(&self, queue: &str) -> Result<Self::DeliveryStream, Error> {
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

    async fn retry(
        &self,
        delivery: Self::Delivery,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let mut message = delivery.try_into_message()?;
        message.headers.retries = match message.headers.retries {
            Some(retries) => Some(retries + 1),
            None => Some(1),
        };
        if let Some(dt) = eta {
            message.headers.eta = Some(dt);
        };
        self.send(&message, delivery.routing_key.as_str()).await?;
        self.ack(delivery).await
    }

    async fn send(&self, message: &Message, queue: &str) -> Result<(), Error> {
        let properties = message.delivery_properties();
        debug!("properties: {:?}", properties);
        self.channel
            .basic_publish(
                "",
                queue,
                BasicPublishOptions::default(),
                message.raw_data.clone(),
                properties,
            )
            .await?;

        Ok(())
    }

    async fn increase_prefetch_count(&self) -> Result<(), Error> {
        let new_count = {
            let mut prefetch_count = self
                .prefetch_count
                .lock()
                .map_err(|_| Error::from(ErrorKind::SyncError))?;
            if *prefetch_count < std::u16::MAX {
                let new_count = *prefetch_count + 1;
                *prefetch_count = new_count;
                new_count
            } else {
                std::u16::MAX
            }
        };
        self.set_prefetch_count(new_count).await?;
        Ok(())
    }

    async fn decrease_prefetch_count(&self) -> Result<(), Error> {
        let new_count = {
            let mut prefetch_count = self
                .prefetch_count
                .lock()
                .map_err(|_| Error::from(ErrorKind::SyncError))?;
            if *prefetch_count > 1 {
                let new_count = *prefetch_count - 1;
                *prefetch_count = new_count;
                new_count
            } else {
                0u16
            }
        };
        if new_count > 0 {
            self.set_prefetch_count(new_count).await?;
        }
        Ok(())
    }
}

impl Message {
    fn delivery_properties(&self) -> BasicProperties {
        let mut properties = BasicProperties::default()
            .with_correlation_id(self.properties.correlation_id.clone().into())
            .with_content_type(self.properties.content_type.clone().into())
            .with_content_encoding(self.properties.content_encoding.clone().into())
            .with_headers(self.delivery_headers())
            .with_priority(0)
            .with_delivery_mode(2);
        if let Some(ref reply_to) = self.properties.reply_to {
            properties = properties.with_reply_to(reply_to.clone().into());
        }
        properties
    }

    fn delivery_headers(&self) -> FieldTable {
        let mut headers = FieldTable::default();
        headers.insert(
            "id".into(),
            AMQPValue::LongString(self.headers.id.clone().into()),
        );
        headers.insert(
            "task".into(),
            AMQPValue::LongString(self.headers.task.clone().into()),
        );
        if let Some(ref lang) = self.headers.lang {
            headers.insert("lang".into(), AMQPValue::LongString(lang.clone().into()));
        }
        if let Some(ref root_id) = self.headers.root_id {
            headers.insert(
                "root_id".into(),
                AMQPValue::LongString(root_id.clone().into()),
            );
        }
        if let Some(ref parent_id) = self.headers.parent_id {
            headers.insert(
                "parent_id".into(),
                AMQPValue::LongString(parent_id.clone().into()),
            );
        }
        if let Some(ref group) = self.headers.group {
            headers.insert("group".into(), AMQPValue::LongString(group.clone().into()));
        }
        if let Some(ref meth) = self.headers.meth {
            headers.insert("meth".into(), AMQPValue::LongString(meth.clone().into()));
        }
        if let Some(ref shadow) = self.headers.shadow {
            headers.insert(
                "shadow".into(),
                AMQPValue::LongString(shadow.clone().into()),
            );
        }
        if let Some(ref eta) = self.headers.eta {
            headers.insert(
                "eta".into(),
                AMQPValue::LongString(eta.to_rfc3339_opts(SecondsFormat::Millis, false).into()),
            );
        }
        if let Some(ref expires) = self.headers.expires {
            headers.insert(
                "expires".into(),
                AMQPValue::LongString(expires.to_rfc3339_opts(SecondsFormat::Millis, false).into()),
            );
        }
        if let Some(retries) = self.headers.retries {
            headers.insert("retries".into(), AMQPValue::LongUInt(retries as u32));
        }
        let mut timelimit = FieldArray::default();
        if let Some(t) = self.headers.timelimit.0 {
            timelimit.push(AMQPValue::LongUInt(t));
        } else {
            timelimit.push(AMQPValue::Void);
        }
        if let Some(t) = self.headers.timelimit.1 {
            timelimit.push(AMQPValue::LongUInt(t));
        } else {
            timelimit.push(AMQPValue::Void);
        }
        headers.insert("timelimit".into(), AMQPValue::FieldArray(timelimit));
        if let Some(ref argsrepr) = self.headers.argsrepr {
            headers.insert(
                "argsrepr".into(),
                AMQPValue::LongString(argsrepr.clone().into()),
            );
        }
        if let Some(ref kwargsrepr) = self.headers.kwargsrepr {
            headers.insert(
                "kwargsrepr".into(),
                AMQPValue::LongString(kwargsrepr.clone().into()),
            );
        }
        if let Some(ref origin) = self.headers.origin {
            headers.insert(
                "origin".into(),
                AMQPValue::LongString(origin.clone().into()),
            );
        }
        headers
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
                lang: headers.inner().get("lang").and_then(|v| match v {
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
                eta: headers.inner().get("eta").and_then(|v| {
                    let eta_string = match v {
                        AMQPValue::ShortString(s) => Some(s.to_string()),
                        AMQPValue::LongString(s) => Some(s.to_string()),
                        _ => None,
                    };
                    if let Some(s) = eta_string {
                        match DateTime::parse_from_rfc3339(&s) {
                            Ok(dt) => Some(DateTime::<Utc>::from(dt)),
                            _ => None,
                        }
                    } else {
                        None
                    }
                }),
                expires: headers.inner().get("expires").and_then(|v| {
                    let expires_string = match v {
                        AMQPValue::ShortString(s) => Some(s.to_string()),
                        AMQPValue::LongString(s) => Some(s.to_string()),
                        _ => None,
                    };
                    if let Some(s) = expires_string {
                        match DateTime::parse_from_rfc3339(&s) {
                            Ok(dt) => Some(DateTime::<Utc>::from(dt)),
                            _ => None,
                        }
                    } else {
                        None
                    }
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
                                    AMQPValue::ShortShortInt(n) => Some(n as u32),
                                    AMQPValue::ShortShortUInt(n) => Some(n as u32),
                                    AMQPValue::ShortInt(n) => Some(n as u32),
                                    AMQPValue::ShortUInt(n) => Some(n as u32),
                                    AMQPValue::LongInt(n) => Some(n as u32),
                                    AMQPValue::LongUInt(n) => Some(n as u32),
                                    AMQPValue::LongLongInt(n) => Some(n as u32),
                                    _ => None,
                                };
                                let hard = match a[1] {
                                    AMQPValue::ShortShortInt(n) => Some(n as u32),
                                    AMQPValue::ShortShortUInt(n) => Some(n as u32),
                                    AMQPValue::ShortInt(n) => Some(n as u32),
                                    AMQPValue::ShortUInt(n) => Some(n as u32),
                                    AMQPValue::LongInt(n) => Some(n as u32),
                                    AMQPValue::LongUInt(n) => Some(n as u32),
                                    AMQPValue::LongLongInt(n) => Some(n as u32),
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

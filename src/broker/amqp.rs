//! AMQP broker.

use amq_protocol::{
    types::{AMQPValue, FieldArray},
    uri::AMQPUri,
};
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Queue};
use log::debug;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::Mutex;

use super::{Broker, BrokerBuilder};
use crate::error::{BrokerError, ProtocolError};
use crate::protocol::{Message, MessageHeaders, MessageProperties, TryCreateMessage};

struct Config {
    broker_url: String,
    prefetch_count: u16,
    queues: HashMap<String, QueueDeclareOptions>,
    heartbeat: Option<u16>,
}

/// Builds an AMQP broker with a custom configuration.
pub struct AMQPBrokerBuilder {
    config: Config,
}

#[async_trait]
impl BrokerBuilder for AMQPBrokerBuilder {
    type Broker = AMQPBroker;

    /// Create a new `AMQPBrokerBuilder`.
    fn new(broker_url: &str) -> Self {
        Self {
            config: Config {
                broker_url: broker_url.into(),
                prefetch_count: 10,
                queues: HashMap::new(),
                heartbeat: Some(60),
            },
        }
    }

    /// Set the worker [prefetch
    /// count](https://www.rabbitmq.com/confirms.html#channel-qos-prefetch).
    fn prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.config.prefetch_count = prefetch_count;
        self
    }

    /// Declare a queue.
    fn declare_queue(mut self, name: &str) -> Self {
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

    /// Set the heartbeat.
    fn heartbeat(mut self, heartbeat: Option<u16>) -> Self {
        self.config.heartbeat = heartbeat;
        self
    }

    /// Build an `AMQPBroker`.
    async fn build(&self) -> Result<AMQPBroker, BrokerError> {
        let mut uri = AMQPUri::from_str(&self.config.broker_url)
            .map_err(|_| BrokerError::InvalidBrokerUrl(self.config.broker_url.clone()))?;
        uri.query.heartbeat = self.config.heartbeat;
        let conn = Connection::connect_uri(uri, ConnectionProperties::default()).await?;
        let consume_channel = conn.create_channel().await?;
        let produce_channel = Mutex::new(conn.create_channel().await?);
        let mut queues: HashMap<String, Queue> = HashMap::new();
        for (queue_name, queue_options) in &self.config.queues {
            let queue = consume_channel
                .queue_declare(queue_name, queue_options.clone(), FieldTable::default())
                .await?;
            queues.insert(queue_name.into(), queue);
        }
        let broker = AMQPBroker {
            conn,
            consume_channel,
            produce_channel,
            consume_channel_write_lock: Mutex::new(0),
            queues,
            prefetch_count: Mutex::new(self.config.prefetch_count),
        };
        broker
            .set_prefetch_count(self.config.prefetch_count)
            .await?;
        Ok(broker)
    }
}

/// An AMQP broker.
pub struct AMQPBroker {
    /// Broker connection.
    conn: Connection,

    /// Channel to consume messages from.
    consume_channel: Channel,

    /// Channel to produce messages from.
    ///
    /// To avoid race conditions when writing to the channel we have to wrap it
    /// in a mutex.
    produce_channel: Mutex<Channel>,

    /// Like the `produce_channel`, we have to be careful to avoid race conditions
    /// when writing to the `cosume_channel`, like when ack-ing or setting the
    /// `prefetch_count` (these have to be done through the same channel that consumes
    /// the messages). But we can't put `consume_channel` itself inside of a Mutex since a worker
    /// that is consuming would always own the lock on the mutex, so we'd never be able
    /// to acquire it for anything else. Hence this dummy Mutex that we only
    /// try to acquire when writing.
    consume_channel_write_lock: Mutex<u8>,

    /// Mapping of queue name to Queue struct.
    queues: HashMap<String, Queue>,

    /// Need to keep track of prefetch count. We put this behind a mutex to get interior
    /// mutability.
    prefetch_count: Mutex<u16>,
}

impl AMQPBroker {
    async fn set_prefetch_count(&self, prefetch_count: u16) -> Result<(), BrokerError> {
        debug!("Setting prefetch count to {}", prefetch_count);
        let _lock = self.consume_channel_write_lock.lock().await;
        self.consume_channel
            .basic_qos(prefetch_count, BasicQosOptions { global: true })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Broker for AMQPBroker {
    type Builder = AMQPBrokerBuilder;
    type Delivery = Delivery;
    type DeliveryError = lapin::Error;
    type DeliveryStream = lapin::Consumer;

    async fn consume<E: Fn() + Send + 'static>(
        &self,
        queue: &str,
        handler: Box<E>,
    ) -> Result<Self::DeliveryStream, BrokerError> {
        self.conn.on_error(handler);
        let queue = self
            .queues
            .get(queue)
            .ok_or_else::<BrokerError, _>(|| BrokerError::UnknownQueue(queue.into()))?;
        self.consume_channel
            .basic_consume(
                queue,
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| e.into())
    }

    async fn ack(&self, delivery: &Self::Delivery) -> Result<(), BrokerError> {
        let _lock = self.consume_channel_write_lock.lock().await;
        self.consume_channel
            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
            .await
            .map_err(|e| e.into())
    }

    async fn retry(
        &self,
        delivery: &Self::Delivery,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        let mut headers = delivery
            .properties
            .headers()
            .clone()
            .unwrap_or_else(FieldTable::default);

        // Increment the number of retries.
        let retries = match get_header_u32(&headers, "retries") {
            Some(retries) => retries + 1,
            None => 1,
        };
        headers.insert("retries".into(), AMQPValue::LongUInt(retries));

        // Set the ETA.
        if let Some(dt) = eta {
            headers.insert(
                "eta".into(),
                AMQPValue::LongString(dt.to_rfc3339_opts(SecondsFormat::Millis, false).into()),
            );
        };

        let properties = delivery.properties.clone().with_headers(headers);
        self.produce_channel
            .lock()
            .await
            .basic_publish(
                "",
                delivery.routing_key.as_str(),
                BasicPublishOptions::default(),
                delivery.data.clone(),
                properties,
            )
            .await?;

        Ok(())
    }

    async fn send(&self, message: &Message, queue: &str) -> Result<(), BrokerError> {
        let properties = message.delivery_properties();
        debug!("Sending AMQP message with: {:?}", properties);
        self.produce_channel
            .lock()
            .await
            .basic_publish(
                "",
                queue,
                BasicPublishOptions::default(),
                message.raw_body.clone(),
                properties,
            )
            .await?;
        Ok(())
    }

    async fn increase_prefetch_count(&self) -> Result<(), BrokerError> {
        let new_count = {
            let mut prefetch_count = self.prefetch_count.lock().await;
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

    async fn decrease_prefetch_count(&self) -> Result<(), BrokerError> {
        let new_count = {
            let mut prefetch_count = self.prefetch_count.lock().await;
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

    async fn close(&self) -> Result<(), BrokerError> {
        // 320 reply-code = "connection-forced", operator intervened.
        // For reference see https://www.rabbitmq.com/amqp-0-9-1-reference.html#domain.reply-code
        let _lock = self.consume_channel_write_lock.lock().await;
        self.conn.close(320, "").await?;
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
            headers.insert("retries".into(), AMQPValue::LongUInt(retries));
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

impl TryCreateMessage for Delivery {
    fn try_create_message(&self) -> Result<Message, ProtocolError> {
        let headers = self
            .properties
            .headers()
            .as_ref()
            .ok_or_else(|| ProtocolError::MissingHeaders)?;
        Ok(Message {
            properties: MessageProperties {
                correlation_id: self
                    .properties
                    .correlation_id()
                    .as_ref()
                    .map(|v| v.to_string())
                    .ok_or_else(|| {
                        ProtocolError::MissingRequiredProperty("correlation_id".into())
                    })?,
                content_type: self
                    .properties
                    .content_type()
                    .as_ref()
                    .map(|v| v.to_string())
                    .ok_or_else(|| ProtocolError::MissingRequiredProperty("content_type".into()))?,
                content_encoding: self
                    .properties
                    .content_encoding()
                    .as_ref()
                    .map(|v| v.to_string())
                    .ok_or_else(|| {
                        ProtocolError::MissingRequiredProperty("content_encoding".into())
                    })?,
                reply_to: self.properties.reply_to().as_ref().map(|v| v.to_string()),
            },
            headers: MessageHeaders {
                id: get_header_str_required(headers, "id")?,
                task: get_header_str_required(headers, "task")?,
                lang: get_header_str(headers, "lang"),
                root_id: get_header_str(headers, "root_id"),
                parent_id: get_header_str(headers, "parent_id"),
                group: get_header_str(headers, "group"),
                meth: get_header_str(headers, "meth"),
                shadow: get_header_str(headers, "shadow"),
                eta: get_header_dt(headers, "eta"),
                expires: get_header_dt(headers, "expires"),
                retries: get_header_u32(headers, "retries"),
                timelimit: headers
                    .inner()
                    .get("timelimit")
                    .and_then(|v| match v {
                        AMQPValue::FieldArray(a) => {
                            let a = a.as_slice().to_vec();
                            if a.len() == 2 {
                                let soft = amqp_value_to_u32(&a[0]);
                                let hard = amqp_value_to_u32(&a[1]);
                                Some((soft, hard))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .unwrap_or((None, None)),
                argsrepr: get_header_str(headers, "argsrepr"),
                kwargsrepr: get_header_str(headers, "kwargsrepr"),
                origin: get_header_str(headers, "origin"),
            },
            raw_body: self.data.clone(),
        })
    }
}

fn get_header_str(headers: &FieldTable, key: &str) -> Option<String> {
    headers.inner().get(key).and_then(|v| match v {
        AMQPValue::ShortString(s) => Some(s.to_string()),
        AMQPValue::LongString(s) => Some(s.to_string()),
        _ => None,
    })
}

fn get_header_str_required(headers: &FieldTable, key: &str) -> Result<String, ProtocolError> {
    get_header_str(headers, key).ok_or_else(|| ProtocolError::MissingRequiredHeader(key.into()))
}

fn get_header_dt(headers: &FieldTable, key: &str) -> Option<DateTime<Utc>> {
    if let Some(s) = get_header_str(headers, key) {
        match DateTime::parse_from_rfc3339(&s) {
            Ok(dt) => Some(DateTime::<Utc>::from(dt)),
            _ => None,
        }
    } else {
        None
    }
}

fn get_header_u32(headers: &FieldTable, key: &str) -> Option<u32> {
    headers.inner().get(key).and_then(amqp_value_to_u32)
}

fn amqp_value_to_u32(v: &AMQPValue) -> Option<u32> {
    match v {
        AMQPValue::ShortShortInt(n) => Some(*n as u32),
        AMQPValue::ShortShortUInt(n) => Some(*n as u32),
        AMQPValue::ShortInt(n) => Some(*n as u32),
        AMQPValue::ShortUInt(n) => Some(*n as u32),
        AMQPValue::LongInt(n) => Some(*n as u32),
        AMQPValue::LongUInt(n) => Some(*n as u32),
        AMQPValue::LongLongInt(n) => Some(*n as u32),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lapin::types::ShortString;
    use std::time::SystemTime;

    #[test]
    /// Tests conversion between Message -> Delivery -> Message.
    fn test_conversion() {
        let now = DateTime::<Utc>::from(SystemTime::now());

        // HACK: round this to milliseconds because that will happen during conversion
        // from message -> delivery.
        let now_str = now.to_rfc3339_opts(SecondsFormat::Millis, false);
        let now = DateTime::<Utc>::from(DateTime::parse_from_rfc3339(&now_str).unwrap());

        let message = Message {
            properties: MessageProperties {
                correlation_id: "aaa".into(),
                content_type: "application/json".into(),
                content_encoding: "utf-8".into(),
                reply_to: Some("bbb".into()),
            },
            headers: MessageHeaders {
                id: "aaa".into(),
                task: "add".into(),
                lang: Some("rust".into()),
                root_id: Some("aaa".into()),
                parent_id: Some("000".into()),
                group: Some("A".into()),
                meth: Some("method_name".into()),
                shadow: Some("add-these".into()),
                eta: Some(now),
                expires: Some(now),
                retries: Some(1),
                timelimit: (Some(30), Some(60)),
                argsrepr: Some("(1)".into()),
                kwargsrepr: Some("{'y': 2}".into()),
                origin: Some("gen123@piper".into()),
            },
            raw_body: vec![],
        };

        let delivery = Delivery {
            delivery_tag: 0,
            exchange: ShortString::from(""),
            routing_key: ShortString::from("celery"),
            redelivered: false,
            properties: message.delivery_properties(),
            data: vec![],
        };

        let message2 = delivery.try_create_message();
        assert!(message2.is_ok());

        let message2 = message2.unwrap();
        assert_eq!(message, message2);
    }
}

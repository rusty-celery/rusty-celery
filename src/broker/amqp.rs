//! AMQP broker.

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::Stream;
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldArray, FieldTable};
use lapin::uri::{self, AMQPUri};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Queue};
use log::debug;
use std::collections::HashMap;
use std::str::FromStr;
use std::task::Poll;
use tokio::sync::{Mutex, RwLock};

use super::{Broker, BrokerBuilder, DeliveryError, DeliveryStream};
use crate::error::{BrokerError, ProtocolError};
use crate::protocol::{Message, MessageHeaders, MessageProperties, TryDeserializeMessage};
use tokio_executor_trait::Tokio as TokioExecutor;

#[cfg(test)]
use std::any::Any;

struct Consumer {
    wrapped: lapin::Consumer,
}
impl DeliveryStream for Consumer {}
impl DeliveryError for lapin::Error {}

#[async_trait]
impl super::Delivery for Delivery {
    async fn resend(
        &self,
        broker: &dyn Broker,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        let mut message = self.try_deserialize_message()?;
        message.headers.eta = eta;
        // Increment the number of retries.
        message.headers.retries = Some(message.headers.retries.map_or(1, |retry| retry + 1));
        broker.send(&message, self.routing_key.as_str()).await
    }
    async fn remove(&self) -> Result<(), BrokerError> {
        todo!()
    }
    async fn ack(&self) -> Result<(), BrokerError> {
        lapin::acker::Acker::ack(self, BasicAckOptions::default()).await?;
        Ok(())
    }
}

impl Stream for Consumer {
    type Item = Result<Box<dyn super::Delivery>, Box<dyn DeliveryError>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::option::Option<<Self as futures::Stream>::Item>> {
        use futures_lite::stream::StreamExt;

        if let Poll::Ready(ret) = self.wrapped.poll_next(cx) {
            if let Some(result) = ret {
                match result {
                    Ok(x) => Poll::Ready(Some(Ok(Box::new(x)))),
                    Err(x) => Poll::Ready(Some(Err(Box::new(x)))),
                }
            } else {
                Poll::Ready(None)
            }
        } else {
            Poll::Pending
        }
    }
}

struct Config {
    broker_url: String,
    prefetch_count: u16,
    queues: HashMap<String, QueueDeclareOptions>,
    heartbeat: Option<u16>,
}

/// Builds an [`AMQPBroker`] with a custom configuration.
pub struct AMQPBrokerBuilder {
    config: Config,
}

fn create_base_connection_properties() -> ConnectionProperties {
    // See https://github.com/amqp-rs/reactor-trait/issues/1#issuecomment-1033473197
    ConnectionProperties::default().with_executor(TokioExecutor::current())
}

#[cfg(unix)]
fn create_connection_properties() -> ConnectionProperties {
    create_base_connection_properties().with_reactor(tokio_reactor_trait::Tokio)
}
#[cfg(windows)]
fn create_connection_properties() -> ConnectionProperties {
    create_base_connection_properties()
}

#[async_trait]
impl BrokerBuilder for AMQPBrokerBuilder {
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
    fn prefetch_count(mut self: Box<Self>, prefetch_count: u16) -> Box<dyn BrokerBuilder> {
        self.config.prefetch_count = prefetch_count;
        self
    }

    /// Declare a queue.
    fn declare_queue(mut self: Box<Self>, name: &str) -> Box<dyn BrokerBuilder> {
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
    fn heartbeat(mut self: Box<Self>, heartbeat: Option<u16>) -> Box<dyn BrokerBuilder> {
        self.config.heartbeat = heartbeat;
        self
    }

    /// Build an `AMQPBroker`.
    async fn build(&self, connection_timeout: u32) -> Result<Box<dyn Broker>, BrokerError> {
        let mut uri = AMQPUri::from_str(&self.config.broker_url)
            .map_err(|_| BrokerError::InvalidBrokerUrl(self.config.broker_url.clone()))?;
        uri.query.heartbeat = self.config.heartbeat;
        uri.query.connection_timeout = Some((connection_timeout as u64) * 1000);

        let conn = Connection::connect_uri(uri.clone(), create_connection_properties()).await?;

        let consume_channel = conn.create_channel().await?;
        let produce_channel = conn.create_channel().await?;

        let mut queues: HashMap<String, Queue> = HashMap::new();
        for (queue_name, queue_options) in &self.config.queues {
            let queue = consume_channel
                .queue_declare(queue_name, *queue_options, FieldTable::default())
                .await?;
            queues.insert(queue_name.into(), queue);
        }

        let broker = AMQPBroker {
            uri,
            conn: Mutex::new(conn),
            consume_channel: RwLock::new(consume_channel),
            produce_channel: RwLock::new(produce_channel),
            queues: RwLock::new(queues),
            queue_declare_options: self.config.queues.clone(),
            prefetch_count: Mutex::new(self.config.prefetch_count),
        };
        broker
            .set_prefetch_count(self.config.prefetch_count)
            .await?;
        Ok(Box::new(broker))
    }
}

/// An AMQP broker.
pub struct AMQPBroker {
    uri: AMQPUri,

    /// Broker connection.
    ///
    /// This is only wrapped in a Mutex for interior mutability.
    conn: Mutex<Connection>,

    /// Channel to consume messages from.
    consume_channel: RwLock<Channel>,

    /// Channel to produce messages from.
    ///
    /// This is only wrapped in RwLock for interior mutability.
    produce_channel: RwLock<Channel>,

    /// Mapping of queue name to Queue struct.
    ///
    /// This is only wrapped in RwLock for interior mutability.
    queues: RwLock<HashMap<String, Queue>>,

    queue_declare_options: HashMap<String, QueueDeclareOptions>,

    /// Need to keep track of prefetch count. We put this behind a mutex to get interior
    /// mutability.
    prefetch_count: Mutex<u16>,
}

impl AMQPBroker {
    async fn set_prefetch_count(&self, prefetch_count: u16) -> Result<(), BrokerError> {
        debug!("Setting prefetch count to {}", prefetch_count);
        self.consume_channel
            .read()
            .await
            .basic_qos(prefetch_count, BasicQosOptions { global: true })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Broker for AMQPBroker {
    fn safe_url(&self) -> String {
        format!(
            "{}://{}:***@{}:{}/{}",
            match self.uri.scheme {
                uri::AMQPScheme::AMQP => "amqp",
                _ => "amqps",
            },
            self.uri.authority.userinfo.username,
            self.uri.authority.host,
            self.uri.authority.port,
            self.uri.vhost,
        )
    }

    async fn consume(
        &self,
        queue: &str,
        error_handler: Box<dyn Fn(BrokerError) + Send + Sync + 'static>,
    ) -> Result<(String, Box<dyn DeliveryStream>), BrokerError> {
        self.conn
            .lock()
            .await
            .on_error(move |e| error_handler(BrokerError::from(e)));
        let queues = self.queues.read().await;
        let queue = queues
            .get(queue)
            .ok_or_else::<BrokerError, _>(|| BrokerError::UnknownQueue(queue.into()))?;
        let consumer = Consumer {
            wrapped: self
                .consume_channel
                .read()
                .await
                .basic_consume(
                    queue.name().as_str(),
                    "",
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await?,
        };
        Ok((consumer.wrapped.tag().to_string(), Box::new(consumer)))
    }

    async fn cancel(&self, consumer_tag: &str) -> Result<(), BrokerError> {
        let consume_channel = self.consume_channel.write().await;
        consume_channel
            .basic_cancel(consumer_tag, BasicCancelOptions::default())
            .await?;
        Ok(())
    }

    async fn ack(&self, delivery: &dyn super::Delivery) -> Result<(), BrokerError> {
        delivery.ack().await
    }

    async fn retry(
        &self,
        delivery: &dyn super::Delivery,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        delivery.resend(self, eta).await?;
        Ok(())
    }

    async fn send(&self, message: &Message, queue: &str) -> Result<(), BrokerError> {
        let properties = message.delivery_properties();
        debug!("Sending AMQP message with: {:?}", properties);
        self.produce_channel
            .read()
            .await
            .basic_publish(
                "",
                queue,
                BasicPublishOptions::default(),
                &message.raw_body.clone()[..],
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
        let consume_channel = self.consume_channel.write().await;
        let produce_channel = self.produce_channel.write().await;
        let conn = self.conn.lock().await;

        if consume_channel.status().connected() {
            debug!("Closing consumer channel...");
            consume_channel.close(200, "OK").await?;
        }

        if produce_channel.status().connected() {
            debug!("Closing producer channel...");
            produce_channel.close(200, "OK").await?;
        }

        if conn.status().connected() {
            debug!("Closing connection...");
            conn.close(200, "OK").await?;
        }

        Ok(())
    }

    /// Try reconnecting in the event of some sort of connection error.
    async fn reconnect(&self, connection_timeout: u32) -> Result<(), BrokerError> {
        let mut conn = self.conn.lock().await;
        if !conn.status().connected() {
            debug!("Attempting to reconnect to broker");
            let mut uri = self.uri.clone();
            uri.query.connection_timeout = Some(connection_timeout as u64);
            *conn = Connection::connect_uri(uri, create_connection_properties()).await?;

            let mut consume_channel = self.consume_channel.write().await;
            let mut produce_channel = self.produce_channel.write().await;
            let mut queues = self.queues.write().await;

            *consume_channel = conn.create_channel().await?;
            *produce_channel = conn.create_channel().await?;

            queues.clear();
            for (queue_name, queue_options) in &self.queue_declare_options {
                let queue = consume_channel
                    .queue_declare(queue_name, *queue_options, FieldTable::default())
                    .await?;
                queues.insert(queue_name.into(), queue);
            }
        }

        Ok(())
    }

    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
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

impl TryDeserializeMessage for (Channel, Delivery) {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        self.1.try_deserialize_message()
    }
}

impl TryDeserializeMessage for Delivery {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        let headers = self
            .properties
            .headers()
            .as_ref()
            .ok_or(ProtocolError::MissingHeaders)?;
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
        AMQPValue::LongUInt(n) => Some(*n),
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
            acker: Default::default(),
        };

        let message2 = delivery.try_deserialize_message();
        assert!(message2.is_ok());

        let message2 = message2.unwrap();
        assert_eq!(message, message2);
    }
}

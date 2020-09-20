//! Redis broker.
#![allow(dead_code)]
use crate::protocol::TryDeserializeMessage;
use redis::RedisResult;
use crate::protocol::MessageHeaders;
use crate::protocol::MessageProperties;
use std::fmt;
use std::clone::Clone;
use futures::Stream;
use std::collections::HashSet;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crate::protocol::{Message};
use crate::error::{BrokerError, ProtocolError};
use tokio::sync::Mutex;
use uuid::Uuid;
use log::{debug, warn};

use super::{Broker, BrokerBuilder};
use redis::aio::{MultiplexedConnection};
use redis::RedisError;
use redis::Client;
use serde_json::json;
use serde_json::value::Value;
use serde::Deserialize;


struct Config {
    broker_url: String,
    prefetch_count: u16,
    queues: HashSet<String>,
    heartbeat: Option<u16>,
}   

pub struct RedisBrokerBuilder{
    config: Config,
}

#[async_trait]
impl BrokerBuilder for RedisBrokerBuilder {
    type Broker = RedisBroker;

    /// Create a new `BrokerBuilder`.
    fn new(broker_url: &str) -> Self{
        RedisBrokerBuilder{
            config: Config{
                broker_url: broker_url.into(),
                prefetch_count: 10,
                queues: HashSet::new(),
                heartbeat: Some(60),
            }
        }
    }

    /// Set the prefetch count.
    fn prefetch_count(mut self, prefetch_count: u16) -> Self{
        self.config.prefetch_count = prefetch_count;
        self
    }

    /// Declare a queue.
    fn declare_queue(mut self, name: &str) -> Self{
        self.config.queues.insert(name.into());
        self
    }

    /// Set the heartbeat.
    fn heartbeat(mut self, heartbeat: Option<u16>) -> Self{
        warn!("Setting heartbeat on redis broker has no effect on anything");
        self.config.heartbeat = heartbeat;
        self
    }

    /// Construct the `Broker` with the given configuration.
    async fn build(&self, connection_timeout: u32) -> Result<Self::Broker, BrokerError>{
        let mut queues: HashSet<String> = HashSet::new();
        for queue_name in &self.config.queues{
            queues.insert(queue_name.into());
        }

        let client = Client::open(&self.config.broker_url[..])
            .map_err(|_| BrokerError::InvalidBrokerUrl(self.config.broker_url.clone()))?;

        let multiplexed_conn = client
                    .get_multiplexed_async_std_connection()
                    .await
                    .map_err(|err| BrokerError::RedisError(err))?;
        // let blocking_conn = client.get_connection().unwrap();

        let consume_channel = Channel{
            client: client.clone(),
            prefetch_count: self.config.prefetch_count,
        };

        Ok(RedisBroker{
            uri: self.config.broker_url.clone(),
            conn: Mutex::new(multiplexed_conn.clone()),
            queues: queues,
            prefetch_count: Mutex::new(self.config.prefetch_count),
            consume_channel: consume_channel,
            consume_channel_write_lock: Mutex::new(0),
        })
    }
}

pub struct RedisBroker{
    uri: String,
    /// Broker connection.
    conn: Mutex<MultiplexedConnection>,
    consume_channel: Channel,
    /// Mapping of queue name to Queue struct.
    consume_channel_write_lock: Mutex<u8>,
    queues: HashSet<String>,

    /// Need to keep track of prefetch count. We put this behind a mutex to get interior
    /// mutability.
    prefetch_count: Mutex<u16>,
}

pub struct Channel{
    client: Client,
    prefetch_count: u16,
}

impl fmt::Debug for Channel{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result{
        write!(f, "Channel {{ client: MultiplexedConnection }}")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeliveryHeaders{
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

#[derive(Debug, Deserialize, Clone)]
pub struct Delivery{
    // pub delivery_tag: u64,
    // pub exchange: String,
    // pub routing_key: String,
    // pub redelivered: bool,
    // pub properties: MessageProperties,
    // pub headers: MessageHeaders,
    // pub data: Vec<u8>
    pub body: Vec<u8>,
    pub content_encoding: String,
    pub content_type: String,
    pub correlation_id: String,
    pub reply_to: Option<String>,
    pub delivery_tag: String,
    pub headers: DeliveryHeaders,
}

// impl Clone for Delivery{
//     fn clone(&self) -> Delivery{
//         Delivery{
//             headers: self.headers.clone(),
//             delivery_tag: self.delivery_tag.clone(),
//             exchange: self.exchange.clone(),
//             routing_key: self.routing_key.clone(),
//             redelivered: self.redelivered.clone(),
//             properties: self.properties.clone(),
//             data: self.data.clone()
//         }
//     }
// }

impl Delivery{
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError>{
        Ok(Message {
            properties: MessageProperties {
                correlation_id: self
                    .correlation_id.clone(),
                content_type: self
                    .content_type.clone(),
                content_encoding: self
                    .content_encoding.clone(),
                reply_to: self.reply_to.clone(),
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
                eta: self.headers.eta.clone(),
                expires: self.headers.expires.clone(),
                retries: self.headers.retries.clone(),
                timelimit: self.headers.timelimit.clone(),
                argsrepr: self.headers.argsrepr.clone(),
                kwargsrepr: self.headers.kwargsrepr.clone(),
                origin: self.headers.origin.clone(),
            },
            raw_body: self.body.clone(),
        })
    }
}

pub struct Consumer{
    channel: Channel,
    queue_name: String,
    // processing_queue_name: String,
}


impl TryDeserializeMessage for (Channel, Delivery) {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        self.1.try_deserialize_message()
    }
}

impl Clone for Channel{
    fn clone(&self) -> Channel{
        Channel{client: self.client.clone(), prefetch_count: self.prefetch_count}
    }
}

// impl Clone for Delivery{
//     fn clone(&self) -> Delivery{
//         todo!()
//     }
// }

impl Stream for Consumer{
    type Item = Result<(Channel, Delivery), RedisError>;
    fn poll_next(mut self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<std::option::Option<<Self as futures::Stream>::Item>> { 
        // execute pipeline
        // - get from queue
        // - add delivery tag in processing unacked_index_key sortedlist
        // - add delivery tag, msg in processing hashset unacked_key
        // TODO: Check prefetch count
        let item: RedisResult<String> = redis::cmd("RPOP")
            .arg(&self.queue_name[..])
            .query(&mut self.channel.client);
        let item: String = match item{
            Ok(json_str) => json_str,
            Err(e) => {
                match e.kind(){
                    redis::ErrorKind::TypeError => {
                        // It returns `Nil` which means list / queue is empty.
                        debug!("End of queue: {}", self.queue_name);
                        return std::task::Poll::Pending;
                    },
                    _ => {
                        eprintln!("Error receiving message: {:?}", e);
                        return std::task::Poll::Ready(Some(Err(e)));
                    }
                }
            }
        };
        let delivery: Delivery = serde_json::from_str(&item[..]).unwrap();
        debug!("Received msg: {} / {}", delivery.delivery_tag, delivery.headers.task);
        // TODO: add to pending hashmap.
        // let _hashresult = redis::cmd("HSET")
        //     .arg(&self.process_map_name[..])
        //     .arg();
        return std::task::Poll::Ready(Some(Ok((self.channel.clone(), delivery))));
    }
}


#[async_trait]
impl Broker for RedisBroker {    
    /// The builder type used to create the broker with a custom configuration.
    type Builder = RedisBrokerBuilder;
    type Delivery = (Channel, Delivery);
    type DeliveryError = RedisError;
    type DeliveryStream = Consumer;

    /// Returns a builder for creating a broker with a custom configuration.
    fn builder(broker_url: &str) -> Self::Builder {
        Self::Builder::new(broker_url)
    }

    /// Consume messages from a queue.
    ///
    /// If the connection is successful, this should return a future stream of `Result`s where an `Ok`
    /// value is a [`Self::Delivery`](trait.Broker.html#associatedtype.Delivery)
    /// type that can be coerced into a [`Message`](protocol/struct.Message.html)
    /// and an `Err` value is a
    /// [`Self::DeliveryError`](trait.Broker.html#associatedtype.DeliveryError) type.
    async fn consume<E: Fn(BrokerError) + Send + Sync + 'static>(
        &self,
        queue: &str,
        _handler: Box<E>,
    ) -> Result<Self::DeliveryStream, BrokerError> { 
        let consumer = Consumer{
            channel: self.consume_channel.clone(),
            queue_name: String::from(queue),
        };
        Ok(consumer)
    }

    /// Acknowledge a [`Delivery`](trait.Broker.html#associatedtype.Delivery) for deletion.
    async fn ack(&self, _delivery: &Self::Delivery) -> Result<(), BrokerError> {
        println!("TODO: ack");
        Ok(())
    }

    /// Retry a delivery.
    async fn retry(
        &self,
        _delivery: &Self::Delivery,
        _eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        println!("TODO: retry");
        Ok(())
    }

    /// Send a [`Message`](protocol/struct.Message.html) into a queue.
    async fn send(&self, message: &Message, queue: &str) -> Result<(), BrokerError> {
        // TODO: Message delivery_properties need to be included too?
        let ser_msg = message.json_serialized().expect("Error in serializing message");
        let _result = redis::cmd("LPUSH")
            .arg(String::from(queue))
            .arg(ser_msg)
            .query_async(&mut *self.conn.lock().await)
            .await.map_err(|err| BrokerError::RedisError(err))?;
        return Ok(());

    }

    /// Increase the `prefetch_count`. This has to be done when a task with a future
    /// ETA is consumed.
    async fn increase_prefetch_count(&self) -> Result<(), BrokerError>{
        println!("TODO: increase_prefetch_count");
        Ok(())
    }

    /// Decrease the `prefetch_count`. This has to be done after a task with a future
    /// ETA is executed.
    async fn decrease_prefetch_count(&self) -> Result<(), BrokerError>{
        println!("TODO: decrease_prefetch_count");
        Ok(())
    }

    /// Clone all channels and connection.
    async fn close(&self) -> Result<(), BrokerError>{
        println!("TODO: close");
        Ok(())
    }

    fn safe_url(&self) -> String{
        println!("TODO: safe_url");
        self.uri.clone()
    }

    async fn reconnect(&self, _connection_timeout: u32) -> Result<(), BrokerError>{
        println!("TODO: reconnect");
        todo!()
    }
}

impl Message{
    pub fn json_serialized(&self) -> Result<Vec<u8>, serde_json::error::Error>{
        let root_id = match &self.headers.root_id{
            Some(root_id) => json!(root_id.clone()),
            None => Value::Null
        };
        let reply_to = match &self.properties.reply_to{
            Some(reply_to) => json!(reply_to.clone()),
            None => Value::Null
        };
        let eta = match self.headers.eta{
            Some(time) => json!(time.to_rfc3339()),
            None => Value::Null
        };
        let expires = match self.headers.expires{
            Some(time) => json!(time.to_rfc3339()),
            None => Value::Null
        };
        let mut buffer = Uuid::encode_buffer();
        let uuid = Uuid::new_v4().to_hyphenated().encode_lower(&mut buffer);
        let delivery_tag = uuid.to_owned();
        let msg_json_value = json!({
            "body": self.raw_body.clone(),
            "content_encoding": self.properties.content_encoding.clone(),
            "content_type": self.properties.content_type.clone(),
            "correlation_id": self.properties.correlation_id.clone(),
            "reply_to": reply_to,
            "delivery_tag": delivery_tag,
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
            }
        });
        let res = serde_json::to_string(&msg_json_value)?;
        Ok(res.into_bytes())
        // Ok(res.bytes())
    }
}

#[cfg(test)]
mod tests{
    use crate::protocol::MessageHeaders;
    use crate::protocol::MessageProperties;
    use crate::protocol::Message;
    use chrono::{DateTime, SecondsFormat, Utc};
    use std::time::SystemTime;

    #[test]
    /// Tests message serialization.
    fn test_serialization(){
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
        let ser_msg_result = message.json_serialized();
        assert!(ser_msg_result.is_ok());
        let ser_msg = ser_msg_result.unwrap();
        let ser_msg_json: serde_json::Value = serde_json::from_slice(&ser_msg[..]).unwrap();
        assert_eq!(ser_msg_json["content_encoding"], String::from("utf-8"));
        assert_eq!(ser_msg_json["content_type"], String::from("application/json"));
        assert_eq!(ser_msg_json["correlation_id"], String::from("aaa"));
        assert_eq!(ser_msg_json["reply_to"], String::from("bbb"));
        assert_ne!(ser_msg_json["delivery_tag"], "");
        assert_eq!(ser_msg_json["headers"]["id"], String::from("aaa"));
        assert_eq!(ser_msg_json["headers"]["task"], String::from("add"));
        assert_eq!(ser_msg_json["headers"]["lang"], String::from("rust"));
        assert_eq!(ser_msg_json["headers"]["root_id"], String::from("aaa"));
        assert_eq!(ser_msg_json["headers"]["parent_id"], String::from("000"));
        assert_eq!(ser_msg_json["headers"]["group"], String::from("A"));
        assert_eq!(ser_msg_json["headers"]["meth"], String::from("method_name"));
        assert_eq!(ser_msg_json["headers"]["shadow"], String::from("add-these"));
        assert_eq!(ser_msg_json["headers"]["retries"], 1);
        assert_eq!(ser_msg_json["headers"]["eta"], now_str);
        assert_eq!(ser_msg_json["headers"]["expires"], now_str);
        assert_eq!(ser_msg_json["headers"]["timelimit"][0], 30);
        assert_eq!(ser_msg_json["headers"]["timelimit"][1], 60);
        assert_eq!(ser_msg_json["headers"]["argsrepr"], "(1)");
        assert_eq!(ser_msg_json["headers"]["kwargsrepr"], "{'y': 2}");
        assert_eq!(ser_msg_json["headers"]["origin"], "gen123@piper");
        let body = serde_json::to_vec(&ser_msg_json["body"]).unwrap();
        // match "[]"
        assert_eq!(body.len(), 2);
        assert_eq!(body[0], 91);
        assert_eq!(body[1], 93);
    }
}



//! Redis broker.
#![allow(dead_code)]
use super::{Broker, BrokerBuilder};
use crate::error::{BrokerError, ProtocolError};
use crate::protocol::Delivery;
use crate::protocol::Message;
use crate::protocol::TryDeserializeMessage;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use log::{debug, warn, error};
use redis::aio::ConnectionManager;
use redis::Client;
use redis::RedisError;
use std::clone::Clone;
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::task::{Poll, Waker};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;

struct Config {
    broker_url: String,
    prefetch_count: u16,
    queues: HashSet<String>,
    heartbeat: Option<u16>,
}

pub struct RedisBrokerBuilder {
    config: Config,
}

#[async_trait]
impl BrokerBuilder for RedisBrokerBuilder {
    type Broker = RedisBroker;

    /// Create a new `BrokerBuilder`.
    fn new(broker_url: &str) -> Self {
        RedisBrokerBuilder {
            config: Config {
                broker_url: broker_url.into(),
                prefetch_count: 10,
                queues: HashSet::new(),
                heartbeat: Some(60),
            },
        }
    }

    /// Set the prefetch count.
    fn prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.config.prefetch_count = prefetch_count;
        self
    }

    /// Declare a queue.
    fn declare_queue(mut self, name: &str) -> Self {
        self.config.queues.insert(name.into());
        self
    }

    /// Set the heartbeat.
    fn heartbeat(mut self, heartbeat: Option<u16>) -> Self {
        warn!("Setting heartbeat on redis broker has no effect on anything");
        self.config.heartbeat = heartbeat;
        self
    }

    /// Construct the `Broker` with the given configuration.
    async fn build(&self, _connection_timeout: u32) -> Result<Self::Broker, BrokerError> {
        let mut queues: HashSet<String> = HashSet::new();
        for queue_name in &self.config.queues {
            queues.insert(queue_name.into());
        }

        let client = Client::open(&self.config.broker_url[..])
            .map_err(|_| BrokerError::InvalidBrokerUrl(self.config.broker_url.clone()))?;

        // let blocking_conn = client.get_connection().unwrap();

        let manager = client.get_tokio_connection_manager().await?;

        let (tx, rx) = channel(1);
        Ok(RedisBroker {
            uri: self.config.broker_url.clone(),
            queues,
            client,
            manager,
            prefetch_count: Arc::new(AtomicU16::new(self.config.prefetch_count)),
            pending_tasks: Arc::new(AtomicU16::new(0)),
            waker_rx: Mutex::new(rx),
            waker_tx: tx,
        })
    }
}

pub struct RedisBroker {
    uri: String,
    /// Broker connection.
    client: Client,
    manager: ConnectionManager,
    /// Mapping of queue name to Queue struct.
    queues: HashSet<String>,

    /// Need to keep track of prefetch count. We put this behind a mutex to get interior
    /// mutability.
    prefetch_count: Arc<AtomicU16>,
    pending_tasks: Arc<AtomicU16>,
    waker_rx: Mutex<Receiver<Waker>>,
    waker_tx: Sender<Waker>,
}

#[derive(Clone)]
pub struct Channel {
    connection: ConnectionManager,
    queue_name: String,
}

impl fmt::Debug for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Channel {{ {} }}", self.queue_name)
    }
}

impl Channel {
    fn new(connection: ConnectionManager, queue_name: String) -> Self {
        Self {
            connection,
            queue_name,
        }
    }

    fn process_map_name(&self) -> String {
        format!("_celery.{}_process_map", self.queue_name)
    }

    async fn fetch_task(
        mut self,
        send_waker: Option<(Sender<Waker>, Waker)>,
    ) -> Result<Delivery, BrokerError> {
        if let Some((mut sender, waker)) = send_waker {
            sender.send(waker).await.unwrap();
            futures::pending!();
        }
        loop {
            let rez: Result<Option<String>, RedisError> = redis::cmd("RPOP")
                .arg(&self.queue_name)
                .query_async(&mut self.connection)
                .await;
            match rez {
                Ok(None) => tokio::time::delay_for(tokio::time::Duration::from_millis(1000)).await,
                Ok(Some(rez)) => {
                    let delivery: Delivery = serde_json::from_str(&rez[..])?;
                    debug!(
                        "Received msg: {} / {}",
                        delivery.delivery_tag, delivery.headers.task
                    );
                    let _set_rez: u32 = redis::cmd("HSET")
                        .arg(&self.process_map_name())
                        .arg(&delivery.correlation_id)
                        .arg(&rez)
                        .query_async(&mut self.connection)
                        .await?;
                    break Ok(delivery);
                }
                Err(err) => break Err(err.into()),
            }
        }
    }

    async fn send_task(mut self, message: &Message) -> Result<(), BrokerError> {
        Ok(redis::cmd("LPUSH")
            .arg(&self.queue_name)
            .arg(message.json_serialized()?)
            .query_async(&mut self.connection)
            .await?)
    }

    async fn resend_task(&self, delivery: &Delivery) -> Result<(), BrokerError> {
        let mut message = delivery.clone().try_deserialize_message()?;
        let retries = message.headers.retries.unwrap_or_default();
        message.headers.retries = Some(retries + 1);
        self.clone().send_task(&message).await?;
        Ok(())
    }

    async fn remove_task(&self, delivery: &Delivery) -> Result<(), BrokerError> {
        redis::cmd("HDEL")
            .arg(&self.process_map_name())
            .arg(&delivery.correlation_id)
            .query_async(&mut self.connection.clone())
            .await?;
        Ok(())
    }
}

type ConsumerOutput = Result<Delivery, BrokerError>;
type ConsumerOutputFuture = Box<dyn Future<Output = ConsumerOutput>>;

pub struct Consumer {
    channel: Channel,
    error_handler: Box<dyn Fn(BrokerError) + Send + Sync + 'static>,
    polled_pop: Option<std::pin::Pin<ConsumerOutputFuture>>,
    pending_tasks: Arc<AtomicU16>,
    waker_tx: Sender<Waker>,
    prefetch_count: Arc<AtomicU16>,
}

impl TryDeserializeMessage for (Channel, Delivery) {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        self.1.try_deserialize_message()
    }
}


impl Stream for Consumer {
    type Item = Result<(Channel, Delivery), BrokerError>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::option::Option<<Self as futures::Stream>::Item>> {
        // execute pipeline
        // - get from queue
        // - add delivery tag in processing unacked_index_key sortedlist
        // - add delivery tag, msg in processing hashset unacked_key
        let mut polled_pop = if self.polled_pop.is_none() {
            let send_waker = if self.pending_tasks.load(Ordering::SeqCst)
                >= self.prefetch_count.load(Ordering::SeqCst)
            {
                debug!("Pending tasks filled prefetch limit");
                let sender = self.waker_tx.clone();
                Some((sender, cx.waker().clone()))
            } else {
                None
            };
            Box::pin(self.channel.clone().fetch_task(send_waker))
        } else {
            self.polled_pop.take().unwrap()
        };
        if let Poll::Ready(item) = Future::poll(polled_pop.as_mut(), cx) {
            match item {
                Ok(item) => {
                    self.pending_tasks.fetch_add(1, Ordering::SeqCst);
                    Poll::Ready(Some(Ok((self.channel.clone(), item))))
                }
                Err(err) => {
                    (self.error_handler)(err);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        } else {
            self.polled_pop = Some(polled_pop);
            Poll::Pending
        }
    }
}

#[async_trait]
impl Broker for RedisBroker {
    /// The builder type used to create the broker with a custom configuration.
    type Builder = RedisBrokerBuilder;
    type Delivery = (Channel, Delivery);
    type DeliveryError = BrokerError;
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
        error_handler: Box<E>,
    ) -> Result<Self::DeliveryStream, BrokerError> {
        let consumer = Consumer {
            channel: Channel {
                connection: self.manager.clone(),
                queue_name: queue.to_string(),
            },
            error_handler,
            polled_pop: None,
            prefetch_count: Arc::clone(&self.prefetch_count),
            pending_tasks: Arc::clone(&self.pending_tasks),
            waker_tx: self.waker_tx.clone(),
        };
        Ok(consumer)
    }

    /// Acknowledge a [`Delivery`](trait.Broker.html#associatedtype.Delivery) for deletion.
    async fn ack(&self, delivery: &Self::Delivery) -> Result<(), BrokerError> {
        self.pending_tasks.fetch_sub(1, Ordering::SeqCst);
        let (channel, delivery) = delivery;
        channel.remove_task(delivery).await?;
        let mut waker_rx = self.waker_rx.lock().await;
        if let Ok(waker) = waker_rx.try_recv() {
            waker.wake();
        }
        Ok(())
    }

    /// Retry a delivery.
    async fn retry(
        &self,
        delivery: &Self::Delivery,
        _eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        let (channel, delivery_msg) = delivery;
        channel.resend_task(delivery_msg).await?;
        self.ack(delivery).await?;
        Ok(())
    }

    /// Send a [`Message`](protocol/struct.Message.html) into a queue.
    async fn send(&self, message: &Message, queue: &str) -> Result<(), BrokerError> {
        Channel::new(self.manager.clone(), queue.to_string())
            .send_task(message)
            .await?;
        Ok(())
    }

    /// Increase the `prefetch_count`. This has to be done when a task with a future
    /// ETA is consumed.
    async fn increase_prefetch_count(&self) -> Result<(), BrokerError> {
        self.prefetch_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Decrease the `prefetch_count`. This has to be done after a task with a future
    /// ETA is executed.
    async fn decrease_prefetch_count(&self) -> Result<(), BrokerError> {
        self.prefetch_count.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    /// Clone all channels and connection.
    async fn close(&self) -> Result<(), BrokerError> {
        Ok(())
    }

    fn safe_url(&self) -> String {
        let parsed_url = redis::parse_redis_url(&self.uri[..]);
        match parsed_url{
            Ok(url) => {
                format!(
                    "{}://{}:***@{}:{}/{}",
                    url.scheme(),
                    url.username(),
                    url.host_str().unwrap(),
                    url.port().unwrap(),
                    url.path(),
                )
            },
            Err(err) =>{
                error!("Invalid redis url. Error: {:?}", err);
                String::from("")
            }
        }
    }

    async fn reconnect(&self, connection_timeout: u32) -> Result<(), BrokerError> {
        // Stop additional task fetching
        let old_prefetch_count = self.prefetch_count.fetch_and(0, Ordering::SeqCst);
        let mut conn = self.manager.clone();
        let timeed_out = false;
        loop {
            let rez: Result<String, RedisError> = redis::cmd("PING").query_async(&mut conn).await;
            match rez {
                Ok(rez) => {
                    if rez.eq("PONG") {
                        self.prefetch_count
                            .store(old_prefetch_count, Ordering::SeqCst);
                        return Ok(());
                    } else {
                        tokio::time::delay_for(tokio::time::Duration::from_secs(
                            connection_timeout as u64,
                        ))
                        .await;
                        continue;
                    }
                }
                Err(e) => {
                    if !timeed_out {
                        tokio::time::delay_for(tokio::time::Duration::from_secs(
                            connection_timeout as u64,
                        ))
                        .await;
                        continue;
                    }
                    self.prefetch_count
                        .store(old_prefetch_count, Ordering::SeqCst);
                    return Err(e.into());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::Message;
    use crate::protocol::MessageHeaders;
    use crate::protocol::MessageProperties;
    use chrono::{DateTime, SecondsFormat, Utc};
    use std::time::SystemTime;

    #[test]
    /// Tests message serialization.
    fn test_serialization() {
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
        assert_eq!(
            ser_msg_json["content_type"],
            String::from("application/json")
        );
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

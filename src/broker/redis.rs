//! Redis broker.
#![allow(dead_code)]
use super::{Broker, BrokerBuilder, DeliveryError, DeliveryStream};
use crate::error::{BrokerError, ProtocolError};
use crate::protocol::Delivery;
use crate::protocol::Message;
use crate::protocol::TryDeserializeMessage;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use log::{debug, error, warn};
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
use uuid::Uuid;

#[cfg(test)]
use std::any::Any;

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
    fn prefetch_count(mut self: Box<Self>, prefetch_count: u16) -> Box<dyn BrokerBuilder> {
        self.config.prefetch_count = prefetch_count;
        self
    }

    /// Declare a queue.
    fn declare_queue(mut self: Box<Self>, name: &str) -> Box<dyn BrokerBuilder> {
        self.config.queues.insert(name.into());
        self
    }

    /// Set the heartbeat.
    fn heartbeat(mut self: Box<Self>, heartbeat: Option<u16>) -> Box<dyn BrokerBuilder> {
        warn!("Setting heartbeat on redis broker has no effect on anything");
        self.config.heartbeat = heartbeat;
        self
    }

    /// Construct the `Broker` with the given configuration.
    async fn build(&self, _connection_timeout: u32) -> Result<Box<dyn Broker>, BrokerError> {
        let mut queues: HashSet<String> = HashSet::new();
        for queue_name in &self.config.queues {
            queues.insert(queue_name.into());
        }
        println!("Creating client");
        let client = Client::open(&self.config.broker_url[..])
            .map_err(|_| BrokerError::InvalidBrokerUrl(self.config.broker_url.clone()))?;

        // let blocking_conn = client.get_connection().unwrap();

        println!("Creating tokio manager");
        let manager = client.get_tokio_connection_manager().await?;

        println!("Creating mpsc channel");
        let (tx, rx) = channel(1);
        println!("Creating broker");
        Ok(Box::new(RedisBroker {
            uri: self.config.broker_url.clone(),
            queues,
            client,
            manager,
            prefetch_count: Arc::new(AtomicU16::new(self.config.prefetch_count)),
            pending_tasks: Arc::new(AtomicU16::new(0)),
            waker_rx: Mutex::new(rx),
            waker_tx: tx,
        }))
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
        if let Some((sender, waker)) = send_waker {
            sender.send(waker).await.unwrap();
            futures::pending!();
        }
        loop {
            let rez: Result<Option<String>, RedisError> = redis::cmd("RPOP")
                .arg(&self.queue_name)
                .query_async(&mut self.connection)
                .await;
            match rez {
                Ok(None) => tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await,
                Ok(Some(rez)) => {
                    let delivery: Delivery = serde_json::from_str(&rez[..])?;
                    debug!(
                        "Received msg: {} / {}",
                        delivery.properties.delivery_tag, delivery.headers.task
                    );
                    let _set_rez: u32 = redis::cmd("HSET")
                        .arg(&self.process_map_name())
                        .arg(&delivery.properties.correlation_id)
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
            .arg(&delivery.properties.correlation_id)
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

impl DeliveryStream for Consumer {}

#[async_trait]
impl super::Delivery for (Channel, Delivery) {
    async fn resend(
        &self,
        _broker: &dyn Broker,
        _eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        self.0.resend_task(&self.1).await?;
        Ok(())
    }

    async fn remove(&self) -> Result<(), BrokerError> {
        self.0.remove_task(&self.1).await?;
        Ok(())
    }

    async fn ack(&self) -> Result<(), BrokerError> {
        todo!()
    }
}

impl TryDeserializeMessage for (Channel, Delivery) {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        self.1.try_deserialize_message()
    }
}

impl Stream for Consumer {
    type Item = Result<Box<dyn super::Delivery>, Box<dyn DeliveryError>>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::option::Option<<Self as futures::Stream>::Item>> {
        // execute pipeline
        // - get from queue
        // - add delivery tag in processing unacked_index_key sortedlist
        // - add delivery tag, msg in processing hashset unacked_key
        if self.pending_tasks.load(Ordering::SeqCst) >= self.prefetch_count.load(Ordering::SeqCst)
            && self.prefetch_count.load(Ordering::SeqCst) > 0
        {
            debug!("Pending tasks limit reached");
            return Poll::Pending;
        }
        let mut polled_pop = if self.polled_pop.is_none() {
            Box::pin(self.channel.clone().fetch_task(None))
        } else {
            self.polled_pop.take().unwrap()
        };
        if let Poll::Ready(item) = Future::poll(polled_pop.as_mut(), cx) {
            match item {
                Ok(item) => {
                    self.pending_tasks.fetch_add(1, Ordering::SeqCst);
                    Poll::Ready(Some(Ok(Box::new((self.channel.clone(), item)))))
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
    /// Consume messages from a queue.
    ///
    /// If the connection is successful, this should return a future stream of `Result`s where an `Ok`
    /// value is a [`Self::Delivery`](trait.Broker.html#associatedtype.Delivery)
    /// type that can be coerced into a [`Message`](protocol/struct.Message.html)
    /// and an `Err` value is a
    /// [`Self::DeliveryError`](trait.Broker.html#associatedtype.DeliveryError) type.
    async fn consume(
        &self,
        queue: &str,
        error_handler: Box<dyn Fn(BrokerError) + Send + Sync + 'static>,
    ) -> Result<(String, Box<dyn DeliveryStream>), BrokerError> {
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

        // Create unique consumer tag.
        let mut buffer = Uuid::encode_buffer();
        let uuid = Uuid::new_v4().hyphenated().encode_lower(&mut buffer);
        let consumer_tag = uuid.to_owned();

        Ok((consumer_tag, Box::new(consumer)))
    }

    async fn cancel(&self, _consumer_tag: &str) -> Result<(), BrokerError> {
        Ok(())
    }

    /// Acknowledge a [`Delivery`](trait.Broker.html#associatedtype.Delivery) for deletion.
    async fn ack(&self, delivery: &dyn super::Delivery) -> Result<(), BrokerError> {
        self.pending_tasks.fetch_sub(1, Ordering::SeqCst);
        delivery.remove().await?;
        let mut waker_rx = self.waker_rx.lock().await;
        // work around for try_recv. We do not care if a waker is available after this check.
        let dummy_waker = futures::task::noop_waker_ref();
        let mut dummy_ctx = std::task::Context::from_waker(dummy_waker);
        if let Poll::Ready(Some(waker)) = waker_rx.poll_recv(&mut dummy_ctx) {
            waker.wake();
        }
        Ok(())
    }

    /// Retry a delivery.
    async fn retry(
        &self,
        delivery: &dyn super::Delivery,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        delivery.resend(self, eta).await?;
        // self.ack(delivery).await?;
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
        let mut conn = self.manager.clone();
        redis::cmd("QUIT").query_async(&mut conn).await?;
        Ok(())
    }

    fn safe_url(&self) -> String {
        let parsed_url = redis::parse_redis_url(&self.uri[..]);
        match parsed_url {
            Some(url) => format!(
                "{}://{}:***@{}:{}/{}",
                url.scheme(),
                url.username(),
                url.host_str().unwrap(),
                url.port().unwrap(),
                url.path(),
            ),
            None => {
                error!("Invalid redis url.");
                String::from("")
            }
        }
    }

    async fn reconnect(&self, connection_timeout: u32) -> Result<(), BrokerError> {
        // Stop additional task fetching
        let old_prefetch_count = self.prefetch_count.fetch_and(0, Ordering::SeqCst);
        let mut conn = self.manager.clone();
        let timed_out = false;
        loop {
            let rez: Result<String, RedisError> = redis::cmd("PING").query_async(&mut conn).await;
            match rez {
                Ok(rez) => {
                    if rez.eq("PONG") {
                        self.prefetch_count
                            .store(old_prefetch_count, Ordering::SeqCst);
                        return Ok(());
                    } else {
                        tokio::time::sleep(tokio::time::Duration::from_secs(
                            connection_timeout as u64,
                        ))
                        .await;
                        continue;
                    }
                }
                Err(e) => {
                    if !timed_out {
                        tokio::time::sleep(tokio::time::Duration::from_secs(
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

    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

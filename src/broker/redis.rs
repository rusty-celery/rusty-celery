//! Redis broker.
#![allow(dead_code)]
use std::fmt;
use std::clone::Clone;
use futures::Stream;
use std::collections::HashSet;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crate::protocol::{Message, TryCreateMessage};
use crate::error::{BrokerError, ProtocolError};
use log::warn;
use tokio::sync::Mutex;
use lapin::message::Delivery;

use super::{Broker, BrokerBuilder};
use redis::aio::{MultiplexedConnection};
use redis::RedisError;
use redis::Client;
use std::cell::RefCell;


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
    async fn build(&self) -> Result<Self::Broker, BrokerError>{
        let mut queues: HashSet<String> = HashSet::new();
        for queue_name in &self.config.queues{
            queues.insert(queue_name.into());
        }

        let client = Client::open(&self.config.broker_url[..])
            .map_err(|_| BrokerError::InvalidBrokerUrl(self.config.broker_url.clone()))?;

        Ok(RedisBroker{
            client: Mutex::new(
                RefCell::new(
                    client
                        .get_multiplexed_async_std_connection()
                        .await
                        .map_err(|err| BrokerError::RedisError(err))?
                )
            ),
            queues: queues,
            prefetch_count: Mutex::new(self.config.prefetch_count),
        })
    }
}

pub struct RedisBroker{
    /// Broker connection.
    client: Mutex<RefCell<MultiplexedConnection>>,

    /// Mapping of queue name to Queue struct.
    queues: HashSet<String>,

    /// Need to keep track of prefetch count. We put this behind a mutex to get interior
    /// mutability.
    prefetch_count: Mutex<u16>,
}

pub struct Channel{
    client: MultiplexedConnection,
}

impl fmt::Debug for Channel{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result{
        write!(f, "Channel {{ client: MultiplexedConnection }}")
    }
}

// #[derive(Debug)]
// pub struct Delivery{}

pub struct Consumer{}

impl TryCreateMessage for (Channel, Delivery){
    fn try_create_message(&self) -> Result<Message, ProtocolError> {
        self.1.try_create_message()
    }
}

impl Clone for Channel{
    fn clone(&self) -> Channel{
        Channel{client: self.client.clone()}
    }
}

// impl Clone for Delivery{
//     fn clone(&self) -> Delivery{
//         todo!()
//     }
// }

impl Stream for Consumer{
    type Item = Result<(Channel, Delivery), RedisError>;
    fn poll_next(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<std::option::Option<<Self as futures::Stream>::Item>> { 
        // execute pipeline
        // - get from queue
        // - add delivery tag in processing unacked_index_key sortedlist
        // - add delivery tag, msg in processing hashset unacked_key
        todo!()
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
        _queue: &str,
        _handler: Box<E>,
    ) -> Result<Self::DeliveryStream, BrokerError> { 
        todo!()
    }

    /// Acknowledge a [`Delivery`](trait.Broker.html#associatedtype.Delivery) for deletion.
    async fn ack(&self, _delivery: &Self::Delivery) -> Result<(), BrokerError> { 
        todo!()
    }

    /// Retry a delivery.
    async fn retry(
        &self,
        _delivery: &Self::Delivery,
        _eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        todo!()
    }

    /// Send a [`Message`](protocol/struct.Message.html) into a queue.
    async fn send(&self, message: &Message, queue: &str) -> Result<(), BrokerError> {
        let result = redis::cmd("LPUSH")
            .arg(String::from(queue))
            .arg(message.raw_body.clone())
            .query_async(&mut *self.client.lock().await.borrow_mut())
            .await.map_err(|err| BrokerError::RedisError(err))?;
        return Ok(());

    }

    /// Increase the `prefetch_count`. This has to be done when a task with a future
    /// ETA is consumed.
    async fn increase_prefetch_count(&self) -> Result<(), BrokerError>{
        todo!()
    }

    /// Decrease the `prefetch_count`. This has to be done after a task with a future
    /// ETA is executed.
    async fn decrease_prefetch_count(&self) -> Result<(), BrokerError>{
        todo!()
    }

    /// Clone all channels and connection.
    async fn close(&self) -> Result<(), BrokerError>{
        todo!()
    }
}

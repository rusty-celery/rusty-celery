//! The broker is an integral part of a [`Celery`](crate::Celery) app. It provides the transport for messages that
//! encode tasks.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use log::error;
use tokio::time::{self, Duration};

use crate::error::BrokerError;
use crate::{
    protocol::{Message, TryDeserializeMessage},
    routing::Rule,
};

mod amqp;
mod redis;
pub use self::redis::{RedisBroker, RedisBrokerBuilder};
pub use amqp::{AMQPBroker, AMQPBrokerBuilder};

#[cfg(test)]
pub mod mock;
#[cfg(test)]
use std::any::Any;

/// The type representing a successful delivery.
#[async_trait]
pub trait Delivery: TryDeserializeMessage + Send + Sync + std::fmt::Debug {
    async fn resend(
        &self,
        broker: &dyn Broker,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError>;
    async fn remove(&self) -> Result<(), BrokerError>;
    async fn ack(&self) -> Result<(), BrokerError>;
}

/// The error type of an unsuccessful delivery.
pub trait DeliveryError: std::fmt::Display + Send + Sync {}

/// The stream type that the [`Celery`](crate::Celery) app will consume deliveries from.
pub trait DeliveryStream:
    Stream<Item = Result<Box<dyn Delivery>, Box<dyn DeliveryError>>> + Unpin
{
}

/// A message [`Broker`] is used as the transport for producing or consuming tasks.
#[async_trait]
pub trait Broker: Send + Sync {
    /// Return a string representation of the broker URL with any sensitive information
    /// redacted.
    fn safe_url(&self) -> String;

    /// Consume messages from a queue.
    ///
    /// If the connection is successful, this should return a unique consumer tag and a
    /// corresponding stream of `Result`s where an `Ok`
    /// value is a [`Self::Delivery`](trait.Broker.html#associatedtype.Delivery)
    /// type that can be coerced into a [`Message`](protocol/struct.Message.html)
    /// and an `Err` value is a
    /// [`Self::DeliveryError`](trait.Broker.html#associatedtype.DeliveryError) type.
    async fn consume(
        &self,
        queue: &str,
        error_handler: Box<dyn Fn(BrokerError) + Send + Sync + 'static>,
    ) -> Result<(String, Box<dyn DeliveryStream>), BrokerError>;

    /// Cancel the consumer with the given `consumer_tag`.
    async fn cancel(&self, consumer_tag: &str) -> Result<(), BrokerError>;

    /// Acknowledge a [`Delivery`](trait.Broker.html#associatedtype.Delivery) for deletion.
    async fn ack(&self, delivery: &dyn Delivery) -> Result<(), BrokerError>;

    /// Retry a delivery.
    async fn retry(
        &self,
        delivery: &dyn Delivery,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError>;

    /// Send a [`Message`](protocol/struct.Message.html) into a queue.
    async fn send(&self, message: &Message, queue: &str) -> Result<(), BrokerError>;

    /// Increase the `prefetch_count`. This has to be done when a task with a future
    /// ETA is consumed.
    async fn increase_prefetch_count(&self) -> Result<(), BrokerError>;

    /// Decrease the `prefetch_count`. This has to be done after a task with a future
    /// ETA is executed.
    async fn decrease_prefetch_count(&self) -> Result<(), BrokerError>;

    /// Clone all channels and connection.
    async fn close(&self) -> Result<(), BrokerError>;

    /// Try reconnecting in the event of some sort of connection error.
    async fn reconnect(&self, connection_timeout: u32) -> Result<(), BrokerError>;

    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

/// A [`BrokerBuilder`] is used to create a type of broker with a custom configuration.
#[async_trait]
pub trait BrokerBuilder: Send + Sync {
    /// Create a new `BrokerBuilder`.
    fn new(broker_url: &str) -> Self
    where
        Self: Sized;

    /// Set the prefetch count.
    fn prefetch_count(self: Box<Self>, prefetch_count: u16) -> Box<dyn BrokerBuilder>;

    /// Declare a queue.
    fn declare_queue(self: Box<Self>, name: &str) -> Box<dyn BrokerBuilder>;

    /// Set the heartbeat.
    fn heartbeat(self: Box<Self>, heartbeat: Option<u16>) -> Box<dyn BrokerBuilder>;

    /// Construct the `Broker` with the given configuration.
    async fn build(&self, connection_timeout: u32) -> Result<Box<dyn Broker>, BrokerError>;
}

pub(crate) fn broker_builder_from_url(broker_url: &str) -> Box<dyn BrokerBuilder> {
    match broker_url.split_once("://") {
        Some(("amqp", _)) => Box::new(AMQPBrokerBuilder::new(broker_url)),
        Some(("redis", _)) => Box::new(RedisBrokerBuilder::new(broker_url)),
        #[cfg(test)]
        Some(("mock", _)) => Box::new(mock::MockBrokerBuilder::new(broker_url)),
        _ => panic!("Unsupported broker"),
    }
}

// TODO: this function consumes the broker_builder, which results in a not so ergonomic API.
// Can it be improved?
/// A utility function to configure the task routes on a broker builder.
pub(crate) fn configure_task_routes(
    mut broker_builder: Box<dyn BrokerBuilder>,
    task_routes: &[(String, String)],
) -> Result<(Box<dyn BrokerBuilder>, Vec<Rule>), BrokerError> {
    let mut rules: Vec<Rule> = Vec::with_capacity(task_routes.len());
    for (pattern, queue) in task_routes {
        let rule = Rule::new(pattern, queue)?;
        rules.push(rule);
        // Ensure all other queues mentioned in task_routes are declared to the broker.
        broker_builder = broker_builder.declare_queue(queue);
    }

    Ok((broker_builder, rules))
}

/// A utility function that can be used to build a broker
/// and initialize the connection.
pub(crate) async fn build_and_connect(
    broker_builder: Box<dyn BrokerBuilder>,
    connection_timeout: u32,
    connection_max_retries: u32,
    connection_retry_delay: u32,
) -> Result<Box<dyn Broker>, BrokerError> {
    let mut broker: Option<Box<dyn Broker>> = None;

    for _ in 0..connection_max_retries {
        match broker_builder.build(connection_timeout).await {
            Err(err) => {
                if err.is_connection_error() {
                    error!("{}", err);
                    error!(
                        "Failed to establish connection with broker, trying again in {}s...",
                        connection_retry_delay
                    );
                    time::sleep(Duration::from_secs(connection_retry_delay as u64)).await;
                    continue;
                }
                return Err(err);
            }
            Ok(b) => {
                broker = Some(b);
                break;
            }
        };
    }

    broker.ok_or_else(|| {
        error!("Failed to establish connection with broker");
        BrokerError::NotConnected
    })
}

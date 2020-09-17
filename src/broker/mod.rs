//! The broker is an integral part of a `Celery` app. It provides the transport for messages that
//! encode tasks.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;

use crate::error::BrokerError;
use crate::protocol::{Message, TryCreateMessage};

mod amqp;
mod redis;
pub use amqp::{AMQPBroker, AMQPBrokerBuilder};
pub use self::redis::{RedisBroker, RedisBrokerBuilder};

/// A message `Broker` is used as the transport for producing or consuming tasks.
#[async_trait]
pub trait Broker: Send + Sync + Sized {
    /// The builder type used to create the broker with a custom configuration.
    type Builder: BrokerBuilder<Broker = Self>;

    /// The type representing a successful delivery.
    type Delivery: TryCreateMessage + Send + Sync + Clone + std::fmt::Debug;

    /// The error type of an unsuccessful delivery.
    type DeliveryError: std::fmt::Display + Send + Sync;

    /// The stream type that the `Celery` app will consume deliveries from.
    type DeliveryStream: Stream<Item = Result<Self::Delivery, Self::DeliveryError>>;

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
        handler: Box<E>,
    ) -> Result<Self::DeliveryStream, BrokerError>;

    /// Acknowledge a [`Delivery`](trait.Broker.html#associatedtype.Delivery) for deletion.
    async fn ack(&self, delivery: &Self::Delivery) -> Result<(), BrokerError>;

    /// Retry a delivery.
    async fn retry(
        &self,
        delivery: &Self::Delivery,
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
}

/// A `BrokerBuilder` is used to create a type of broker with a custom configuration.
#[async_trait]
pub trait BrokerBuilder {
    type Broker: Broker;

    /// Create a new `BrokerBuilder`.
    fn new(broker_url: &str) -> Self;

    /// Set the prefetch count.
    fn prefetch_count(self, prefetch_count: u16) -> Self;

    /// Declare a queue.
    fn declare_queue(self, name: &str) -> Self;

    /// Set the heartbeat.
    fn heartbeat(self, heartbeat: Option<u16>) -> Self;

    /// Construct the `Broker` with the given configuration.
    async fn build(&self) -> Result<Self::Broker, BrokerError>;
}

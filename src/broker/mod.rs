use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;

use crate::protocol::{Message, TryIntoMessage};
use crate::Error;

pub mod amqp;

/// A message `Broker` is used as the transport for producing or consuming tasks.
#[async_trait]
pub trait Broker: Send + Sync {
    /// The builder type used to create the broker with a custom configuration.
    type Builder: BrokerBuilder;

    /// The type representing a successful delivery.
    type Delivery: TryIntoMessage + Send + Sync + Clone + std::fmt::Debug;

    /// The error type of an unsuccessful delivery.
    type DeliveryError: Into<Error> + Send + Sync;

    /// The stream type that the `Celery` app will consume deliveries from.
    type DeliveryStream: Stream<Item = Result<Self::Delivery, Self::DeliveryError>>;

    /// Returns a builder for creating a broker with a custom configuration.
    fn builder(broker_url: &str) -> Self::Builder;

    /// Consume messages from a queue.
    ///
    /// If the connection is successful, this should return a future stream of `Result`s where an `Ok`
    /// value is a [`Self::Delivery`](trait.Broker.html#associatedtype.Delivery)
    /// type that can be coerced into a [`Message`](protocol/struct.Message.html)
    /// and an `Err` value is a
    /// [`Self::DeliveryError`](trait.Broker.html#associatedtype.DeliveryError)
    /// type that can be coerced into an [`Error`](struct.Error.html).
    async fn consume(&self, queue: &str) -> Result<Self::DeliveryStream, Error>;

    /// Acknowledge a [`Delivery`](trait.Broker.html#associatedtype.Delivery) for deletion.
    async fn ack(&self, delivery: Self::Delivery) -> Result<(), Error>;

    /// Retry a delivery.
    async fn retry(
        &self,
        delivery: Self::Delivery,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), Error>;

    /// Send a [`Message`](protocol/struct.Message.html) into a queue.
    async fn send(&self, message: &Message, queue: &str) -> Result<(), Error>;

    async fn increase_prefetch_count(&self) -> Result<(), Error>;

    async fn decrease_prefetch_count(&self) -> Result<(), Error>;
}

/// A `BrokerBuilder` is used to create a type of broker with a custom configuration.
pub trait BrokerBuilder {
    type Broker: Broker;

    /// Set the prefetch count.
    fn prefetch_count(self, prefetch_count: u16) -> Self;

    /// Register a queue.
    fn queue(self, name: &str) -> Self;

    /// Construct the `Broker` with the given configuration.
    fn build(self) -> Result<Self::Broker, Error>;
}

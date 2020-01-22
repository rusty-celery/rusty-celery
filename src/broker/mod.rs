use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;

use crate::protocol::{Message, TryIntoMessage};
use crate::Error;

pub mod amqp;

/// A message `Broker` is used as the transport for producing or consuming tasks.
#[async_trait]
pub trait Broker: Send + Sync {
    type Delivery: TryIntoMessage + Send + Sync + Clone + std::fmt::Debug;
    type DeliveryError: Into<Error> + Send + Sync;
    type Consumer: IntoIterator<
            Item = Result<Self::Delivery, Self::DeliveryError>,
            IntoIter = Self::ConsumerIterator,
        > + Stream<Item = Result<Self::Delivery, Self::DeliveryError>>;
    type ConsumerIterator: Iterator<Item = Result<Self::Delivery, Self::DeliveryError>>;

    /// Consume messages from a queue.
    ///
    /// If the connection is successful, this should return a future stream of `Result`s where an `Ok`
    /// value is a [`Self::Delivery`](trait.Broker.html#associatedtype.Delivery)
    /// type that can be coerced into a [`Message`](protocol/struct.Message.html)
    /// and an `Err` value is a
    /// [`Self::DeliveryError`](trait.Broker.html#associatedtype.DeliveryError)
    /// type that can be coerced into an [`Error`](struct.Error.html).
    async fn consume(&self, queue: &str) -> Result<Self::Consumer, Error>;

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

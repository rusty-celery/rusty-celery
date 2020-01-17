use async_trait::async_trait;
use futures_util::stream::{Stream, StreamExt};

use crate::protocol::{Message, TryIntoMessage};
use crate::Error;

/// A message `Broker` is used as the transport for producing or consuming tasks.
#[async_trait]
pub trait Broker {
    type Delivery: TryIntoMessage + Clone + std::fmt::Debug;
    type DeliveryError: Into<Error>;
    type Consumer: IntoIterator<
            Item = Result<Self::Delivery, Self::DeliveryError>,
            IntoIter = Self::ConsumerIterator,
        > + Stream<Item = Result<Self::Delivery, Self::DeliveryError>>
        + StreamExt;
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

    /// Requeue a delivery. Used when a task fails are should be retried.
    async fn requeue(&self, delivery: Self::Delivery) -> Result<(), Error>;

    /// Send a [`Message`](protocol/struct.Message.html) into a queue.
    async fn send(&self, message: &Message, queue: &str) -> Result<(), Error>;
}

pub mod amqp;

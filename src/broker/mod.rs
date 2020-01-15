use async_trait::async_trait;
use futures_util::stream::{Stream, StreamExt};

use crate::protocol::{Message, TryIntoMessage};
use crate::Error;

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

    async fn consume(&self, queue: &str) -> Result<Self::Consumer, Error>;

    async fn ack(&self, delivery: Self::Delivery) -> Result<(), Error>;

    async fn send(&self, message: &Message, queue: &str) -> Result<(), Error>;
}

pub mod amqp;

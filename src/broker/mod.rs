use async_trait::async_trait;
use futures_util::stream::{Stream, StreamExt};

use crate::protocol::{MessageBody, TryIntoMessage};
use crate::{Error, Task};

#[async_trait]
pub trait BrokerBuilder {
    type Broker: Broker;

    fn new(broker_url: &str) -> Self;

    fn prefetch_count(self, prefetch_count: Option<u16>) -> Self;

    fn queue(self, name: &str) -> Self;

    async fn build(self) -> Result<Self::Broker, Error>;
}

#[async_trait]
pub trait Broker {
    type Builder: BrokerBuilder;
    type Delivery: TryIntoMessage + Clone;
    type DeliveryError: Into<Error>;
    type Consumer: IntoIterator<Item = Result<Self::Delivery, Self::DeliveryError>, IntoIter = Self::ConsumerIterator>
        + Stream<Item = Result<Self::Delivery, Self::DeliveryError>>
        + StreamExt;
    type ConsumerIterator: Iterator<Item = Result<Self::Delivery, Self::DeliveryError>>;

    async fn consume(&self, queue: &str) -> Result<Self::Consumer, Error>;

    async fn ack(&self, delivery: Self::Delivery) -> Result<(), Error>;

    async fn send_task<T: Task>(&self, body: MessageBody<T>, queue: &str) -> Result<(), Error>;

    fn builder(broker_url: &str) -> Self::Builder;
}

mod amqp;

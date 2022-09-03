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

/// A message [`Broker`] is used as the transport for producing or consuming tasks.
#[async_trait]
pub trait Broker: Send + Sync + Sized {
    /// The builder type used to create the broker with a custom configuration.
    type Builder: BrokerBuilder<Broker = Self>;

    /// The type representing a successful delivery.
    type Delivery: TryDeserializeMessage + Send + Sync + std::fmt::Debug;

    /// The error type of an unsuccessful delivery.
    type DeliveryError: std::fmt::Display + Send + Sync;

    /// The stream type that the [`Celery`](crate::Celery) app will consume deliveries from.
    type DeliveryStream: Stream<Item = Result<Self::Delivery, Self::DeliveryError>>;

    /// Returns a builder for creating a broker with a custom configuration.
    fn builder(broker_url: &str) -> Self::Builder {
        Self::Builder::new(broker_url)
    }

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
    async fn consume<E: Fn(BrokerError) + Send + Sync + 'static>(
        &self,
        queue: &str,
        error_handler: Box<E>,
    ) -> Result<(String, Self::DeliveryStream), BrokerError>;

    /// Cancel the consumer with the given `consumer_tag`.
    async fn cancel(&self, consumer_tag: &str) -> Result<(), BrokerError>;

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

    /// Try reconnecting in the event of some sort of connection error.
    async fn reconnect(&self, connection_timeout: u32) -> Result<(), BrokerError>;
}

/// A [`BrokerBuilder`] is used to create a type of broker with a custom configuration.
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
    async fn build(&self, connection_timeout: u32) -> Result<Self::Broker, BrokerError>;
}

// TODO: this function consumes the broker_builder, which results in a not so ergonomic API.
// Can it be improved?
/// A utility function to configure the task routes on a broker builder.
pub(crate) fn configure_task_routes<Bb: BrokerBuilder>(
    mut broker_builder: Bb,
    task_routes: &[(String, String)],
) -> Result<(Bb, Vec<Rule>), BrokerError> {
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
pub(crate) async fn build_and_connect<Bb: BrokerBuilder>(
    broker_builder: Bb,
    connection_timeout: u32,
    connection_max_retries: u32,
    connection_retry_delay: u32,
) -> Result<Bb::Broker, BrokerError> {
    let mut broker: Option<Bb::Broker> = None;

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

    Ok(broker.ok_or_else(|| {
        error!("Failed to establish connection with broker");
        BrokerError::NotConnected
    })?)
}

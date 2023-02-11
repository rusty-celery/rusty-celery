//! Defines mock broker that can be used to test other components that rely on a broker.

use super::{Broker, BrokerBuilder, Delivery, DeliveryStream};
use crate::error::{BrokerError, ProtocolError};
use crate::protocol::{Message, TryDeserializeMessage};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{
    task::{Context, Poll},
    Stream,
};
use std::collections::HashMap;
use std::time::SystemTime;
use tokio::sync::RwLock;

#[cfg(test)]
use std::any::Any;

pub struct MockBrokerBuilder;

#[async_trait]
impl BrokerBuilder for MockBrokerBuilder {
    #[allow(unused)]
    fn new(broker_url: &str) -> Self {
        Self {}
    }

    #[allow(unused)]
    fn prefetch_count(self: Box<Self>, prefetch_count: u16) -> Box<dyn BrokerBuilder> {
        self
    }

    #[allow(unused)]
    fn declare_queue(self: Box<Self>, name: &str) -> Box<dyn BrokerBuilder> {
        self
    }

    #[allow(unused)]
    fn heartbeat(self: Box<Self>, heartbeat: Option<u16>) -> Box<dyn BrokerBuilder> {
        self
    }

    #[allow(unused)]
    async fn build(&self, connection_timeout: u32) -> Result<Box<dyn Broker>, BrokerError> {
        Ok(Box::new(MockBroker::new()))
    }
}

#[derive(Default)]
pub struct MockBroker {
    /// Holds a mapping of all sent tasks.
    ///
    /// The keys are the task IDs, and the values are tuples of the message object,
    /// queue it was sent to, and time it was sent.
    pub sent_tasks: RwLock<HashMap<String, (Message, String, SystemTime)>>,
}

impl MockBroker {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn reset(&self) {
        self.sent_tasks.write().await.clear();
    }
}

#[async_trait]
impl Broker for MockBroker {
    fn safe_url(&self) -> String {
        "mock://fake-url:8000/".into()
    }

    #[allow(unused)]
    async fn consume(
        &self,
        queue: &str,
        error_handler: Box<dyn Fn(BrokerError) + Send + Sync + 'static>,
    ) -> Result<(String, Box<dyn DeliveryStream>), BrokerError> {
        unimplemented!();
    }

    #[allow(unused)]
    async fn cancel(&self, consumer_tag: &str) -> Result<(), BrokerError> {
        Ok(())
    }

    #[allow(unused)]
    async fn ack(&self, delivery: &dyn Delivery) -> Result<(), BrokerError> {
        Ok(())
    }

    #[allow(unused)]
    async fn retry(
        &self,
        delivery: &dyn Delivery,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        Ok(())
    }

    #[allow(unused)]
    async fn send(&self, message: &Message, queue: &str) -> Result<(), BrokerError> {
        self.sent_tasks.write().await.insert(
            message.task_id().into(),
            (message.clone(), queue.into(), SystemTime::now()),
        );
        Ok(())
    }

    async fn increase_prefetch_count(&self) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn decrease_prefetch_count(&self) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), BrokerError> {
        Ok(())
    }

    #[allow(unused)]
    async fn reconnect(&self, connection_timeout: u32) -> Result<(), BrokerError> {
        Ok(())
    }

    #[cfg(test)]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[derive(Debug, Clone)]
pub struct MockDelivery;

impl TryDeserializeMessage for MockDelivery {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        unimplemented!();
    }
}

pub struct MockMessageStream;

impl Stream for MockMessageStream {
    type Item = Result<MockDelivery, ProtocolError>;

    #[allow(unused)]
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        unimplemented!();
    }
}

//! Defines mock broker that can be used to test other components that rely on a broker.

use super::{Broker, BrokerBuilder};
use crate::broker::Queue;
use crate::error::{BrokerError, ProtocolError};
use crate::protocol::{Message, TryDeserializeMessage};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{
    task::{Context, Poll},
    Stream,
};
use std::collections::HashMap;
use tokio::sync::RwLock;

pub struct MockBrokerBuilder;

#[async_trait]
impl BrokerBuilder for MockBrokerBuilder {
    type Broker = MockBroker;

    #[allow(unused)]
    fn new(broker_url: &str) -> Self {
        Self {}
    }

    #[allow(unused)]
    fn prefetch_count(self, prefetch_count: u16) -> Self {
        self
    }

    #[allow(unused)]
    fn declare_queue(self, name: Queue) -> Self {
        self
    }

    #[allow(unused)]
    fn heartbeat(self, heartbeat: Option<u16>) -> Self {
        self
    }

    #[allow(unused)]
    async fn build(&self, connection_timeout: u32) -> Result<Self::Broker, BrokerError> {
        Ok(MockBroker::new())
    }
}

#[derive(Default)]
pub struct MockBroker {
    pub sent_tasks: RwLock<HashMap<String, Message>>,
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
    type Builder = MockBrokerBuilder;
    type Delivery = Delivery;
    type DeliveryError = ProtocolError;
    type DeliveryStream = MockMessageStream;

    fn safe_url(&self) -> String {
        "mock://fake-url:8000/".into()
    }

    #[allow(unused)]
    async fn consume<E: Fn(BrokerError) + Send + Sync + 'static>(
        &self,
        queue: &str,
        error_handler: Box<E>,
    ) -> Result<Self::DeliveryStream, BrokerError> {
        unimplemented!();
    }

    #[allow(unused)]
    async fn ack(&self, delivery: &Self::Delivery) -> Result<(), BrokerError> {
        Ok(())
    }

    #[allow(unused)]
    async fn retry(
        &self,
        delivery: &Self::Delivery,
        eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        Ok(())
    }

    #[allow(unused)]
    async fn send(&self, message: &Message, queue: &str) -> Result<(), BrokerError> {
        self.sent_tasks
            .write()
            .await
            .insert(message.task_id().into(), message.clone());
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
}

#[derive(Debug, Clone)]
pub struct Delivery;

impl TryDeserializeMessage for Delivery {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        unimplemented!();
    }
}

pub struct MockMessageStream;

impl Stream for MockMessageStream {
    type Item = Result<Delivery, ProtocolError>;

    #[allow(unused)]
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        unimplemented!();
    }
}

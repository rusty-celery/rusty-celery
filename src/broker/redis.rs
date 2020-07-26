use std::collections::HashSet;
use super::{Broker, BrokerBuilder};
use redis::Client;

use log::{debug, warning};
use tokio::sync::Mutex;

struct Config {
    broker_url: String,
    prefetch_count: u16,
    queues: HashSet<String>,
    heartbeat: Option<u16>,
}   

pub struct RedisBrokerBuilder{
    config: Config,
}

#[async_trait]
impl BrokerBuilder for RedisBrokerBuilder {
    type Broker: RedisBroker;

    /// Create a new `BrokerBuilder`.
    fn new(broker_url: &str) -> Self{
        RedisBrokerBuilder{
            config: Config{
                broker_url: broker_url,
                prefetch_count: 10,
                queues: HashSet::new(),
                heartbeat: Some(60),
            }
        }
    };

    /// Set the prefetch count.
    fn prefetch_count(mut self, prefetch_count: u16) -> Self{
        self.config.prefetch_count = prefetch_count;
        self
    }

    /// Declare a queue.
    fn declare_queue(mut self, name: &str) -> Self{
        self.config.queues.insert(name.into());
        self
    }

    /// Set the heartbeat.
    fn heartbeat(mut self, heartbeat: Option<u16>) -> Self{
        log::warning("Setting heartbeat on redis broker has no effect on anything");
        self.config.heartbeat = heartbeat;
        self
    }

    /// Construct the `Broker` with the given configuration.
    async fn build(&self) -> Result<Self::Broker, BrokerError>{
        let mut queues = HashSet::new();
        for queue_name in &self.config.queues{
            queues.insert(queue_name.into());
        }
        Some(RedisBroker{
            client: Client::open(&self.config.broker_url[..])?,
            queues: self.config.queues,
            prefetch_count: Mutex::new(self.config.prefetch_count),
        })
    }
}

pub struct RedisBroker{
    /// Broker connection.
    client: Client,

    /// Mapping of queue name to Queue struct.
    queues: HashSet<String>,

    /// Need to keep track of prefetch count. We put this behind a mutex to get interior
    /// mutability.
    prefetch_count: Mutex<u16>,
}



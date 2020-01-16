#![doc(
    html_favicon_url = "https://structurely-images.s3-us-west-2.amazonaws.com/logos/rusty-celery.ico"
)]
#![doc(
    html_logo_url = "https://structurely-images.s3-us-west-2.amazonaws.com/logos/rusty-celery-4.png"
)]

mod app;
mod broker;
mod error;
pub mod protocol;
mod task;

pub use app::Celery;
pub use broker::{
    amqp::{AMQPBroker, AMQPBrokerBuilder},
    Broker,
};
pub use error::{Error, ErrorKind};
pub use task::Task;

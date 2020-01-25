#[macro_export]
macro_rules! celery_app {
    (
        $name:ident,
        $broker_type:ty,
        broker_url = $broker_url:expr,
        tasks = [ $( $t:ty ),* ],
    ) => {
        use celery::{Celery, Broker, BrokerBuilder};
        use lazy_static::lazy_static;

        lazy_static! {
            static ref $name: Celery<$broker_type> = {
                let broker_url = $broker_url;

                let mut builder = Celery::builder::<<$broker_type as Broker>::Builder>("celery", &broker_url);
                let celery: Celery<$broker_type> = builder.build().unwrap();

                $(
                    celery.register_task::<$t>().unwrap();
                )*

                celery
            };
        }
    };
    (
        $name:ident,
        $broker_type:ty,
        broker_url = $broker_url:expr,
        tasks = [ $( $t:ty ),* ],
        $( $x:ident = $y:expr, )*
    ) => {
        use celery::{Celery, Broker, BrokerBuilder};
        use lazy_static::lazy_static;

        lazy_static! {
            static ref $name: Celery<$broker_type> = {
                let broker_url = $broker_url;

                let mut builder = Celery::builder::<<$broker_type as Broker>::Builder>("celery", &broker_url);

                $(
                    builder = builder.$x($y);
                )*

                let celery: Celery<$broker_type> = builder.build().unwrap();

                $(
                    celery.register_task::<$t>().unwrap();
                )*

                celery
            };
        }
    };
}

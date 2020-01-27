/// A macro for creating a `Celery` app.
///
/// # Examples
///
/// At a minimum the `celery_app!` macro requires 2 arguments:
/// an identity to assign the application to (`app` in this case becomes a static reference variable to
/// the application) and a broker type with an expression for the broker URL in brackets.
///
/// Following the broker type you can also pass a list of tasks to register as in this example.
///
/// ```rust,no_run
/// use celery::{celery_app, task, AMQPBroker};
///
/// #[task]
/// fn add(x: i32, y: i32) -> i32 {
///     x + y
/// }
///
/// # #[macro_use] extern crate celery;
/// # fn main() {
/// celery_app!(
///     app,
///     AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [ add ],
/// );
/// # }
/// ```
///
/// Following the optional `tasks` parameter, there are a number of other optional parameters that
/// may appear in arbitrary order, all of which correspond to a method on the
/// [`CeleryBuilder`](struct.CeleryBuilder.html) struct.
/// For example `task_timeout = 3` or `prefetch_count = 2`.
#[macro_export]
macro_rules! celery_app {
    (
        $name:ident,
        $broker_type:ty { $broker_url:expr },
        tasks = [ $( $t:ty ),* ],
        $( $x:ident = $y:expr, )*
    ) => {
        use celery::{Celery, Broker, BrokerBuilder};

        lazy_static::lazy_static! {
            static ref $name: Celery<$broker_type> = {
                let broker_url = $broker_url;

                let mut builder = Celery::<$broker_type>::builder("celery", &broker_url);

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
    (
        $name:ident,
        $broker_type:ty { $broker_url:expr },
        $( $x:ident = $y:expr, )*
    ) => {
        use celery::{Celery, Broker, BrokerBuilder};
        use lazy_static::lazy_static;

        lazy_static! {
            static ref $name: Celery<$broker_type> = {
                let broker_url = $broker_url;

                let mut builder = Celery::<$broker_type>::builder("celery", &broker_url);

                $(
                    builder = builder.$x($y);
                )*

                let celery: Celery<$broker_type> = builder.build().unwrap();

                celery
            };
        }
    };
}

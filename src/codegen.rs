#[doc(hidden)]
#[macro_export]
macro_rules! __celery_app_internal {
    (
        ($($vis:tt)*) $name:ident,
        $broker_type:ty { $broker_url:expr },
        [ $( $t:ty ),* ],
        [ $( $pattern:expr => $queue:expr ),* ],
        $( $x:ident = $y:expr, )*
    ) => {
        use celery::{Celery, Broker, BrokerBuilder};

        celery::export::lazy_static! {
            static ref $name: Celery<$broker_type> = {
                let broker_url = $broker_url;

                let mut builder = Celery::<$broker_type>::builder("celery", &broker_url);

                $(
                    builder = builder.$x($y);
                )*

                $(
                    let rule = celery::Rule::new($pattern, $queue).unwrap();
                    builder = builder.task_route(rule);
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

/// A macro for creating a `Celery` app.
///
/// At a minimum the `celery_app!` macro requires 4 arguments:
/// - an identifier to assign the application to (`app` in this case becomes a static reference variable to
/// the application),
/// - a broker type with an expression for the broker URL in brackets,
/// - a list of tasks to register, and
/// - a list of routing rules.
///
/// For example:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// use celery::{celery_app, task, AMQPBroker};
///
/// #[task]
/// fn add(x: i32, y: i32) -> i32 {
///     x + y
/// }
///
/// # fn main() {
/// celery_app!(
///     app,
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [ add ],
///     task_routes = [ "*" => "celery" ],
/// );
/// # }
/// ```
///
/// Following the task routing rules there are a number of other optional parameters that
/// may appear in arbitrary order, all of which correspond to a method on the
/// [`CeleryBuilder`](struct.CeleryBuilder.html) struct such as `task_timeout`:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # use celery::{celery_app, AMQPBroker};
/// # fn main() {
/// celery_app!(
///     app,
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [],
///     task_routes = [],
///     task_timeout = 2,
/// );
/// # }
/// ```
///
/// It also valid to prefix the app identifier with visibility qualifiers:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # use celery::{celery_app, AMQPBroker};
/// # fn main() {
/// celery_app!(
///     pub(crate) app,
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [],
///     task_routes = [],
/// );
/// # }
/// ```
#[macro_export]
macro_rules! celery_app {
    (
        $name:ident,
        broker = $broker_type:ty { $broker_url:expr },
        tasks = [ $( $t:ty ),* ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* ],
        $( $x:ident = $y:expr, )*
    ) => {
        $crate::__celery_app_internal!(
            () $name,
            $broker_type { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
    (
        pub $name:ident,
        broker = $broker_type:ty { $broker_url:expr },
        tasks = [ $( $t:ty ),* ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* ],
        $( $x:ident = $y:expr, )*
    ) => {
        $crate::__celery_app_internal!(
            (pub) $name,
            $broker_type { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
    (
        pub ($($vis:tt)+) $name:ident,
        broker = $broker_type:ty { $broker_url:expr },
        tasks = [ $( $t:ty ),* ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* ],
        $( $x:ident = $y:expr, )*
    ) => {
        $crate::__celery_app_internal!(
            (pub ($($vis)+)) $name,
            $broker_type { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
}

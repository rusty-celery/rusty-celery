#[doc(hidden)]
#[macro_export]
macro_rules! __celery_app_internal {
    (
        $broker_type:ty { $broker_url:expr },
        [ $( $t:ty ),* ],
        [ $( $pattern:expr => $queue:expr ),* ],
        $( $x:ident = $y:expr, )*
    ) => {{
        static CELERY_APP: $crate::export::OnceCell<$crate::Celery<$broker_type>> =
            $crate::export::OnceCell::new();
        CELERY_APP.get_or_init(|| {
            let broker_url = $broker_url;

            let mut builder = $crate::Celery::<$broker_type>::builder("celery", &broker_url);

            $(
                builder = builder.$x($y);
            )*

            $(
                let rule = $crate::Rule::new($pattern, $queue).unwrap();
                builder = builder.task_route(rule);
            )*

            let celery: $crate::Celery<$broker_type> = builder.build().unwrap();

            $(
                celery.register_task::<$t>().unwrap();
            )*

            celery
        })
    }};
}

/// A macro for creating a `Celery` app.
///
/// At a minimum the `celery_app!` macro requires 3 arguments:
/// - a broker type with an expression for the broker URL in brackets,
/// - a list of tasks to register, and
/// - a list of routing rules in the form of `pattern => queue`.
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
/// let app = celery_app!(
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
/// let app = celery_app!(
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [],
///     task_routes = [],
///     task_timeout = 2,
/// );
/// # }
/// ```
#[macro_export]
macro_rules! celery_app {
    (
        broker = $broker_type:ty { $broker_url:expr },
        tasks = [ $( $t:ty ),* $(,)? ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ],
        $( $x:ident = $y:expr, )*
    ) => {
        $crate::__celery_app_internal!(
            $broker_type { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
    (
        broker = $broker_type:ty { $broker_url:expr },
        tasks = [ $( $t:ty ),* $(,)? ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ],
        $( $x:ident = $y:expr, )*
    ) => {
        $crate::__celery_app_internal!(
            $broker_type { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
    (
        broker = $broker_type:ty { $broker_url:expr },
        tasks = [ $( $t:ty ),* $(,)? ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ],
        $( $x:ident = $y:expr, )*
    ) => {
        $crate::__celery_app_internal!(
            $broker_type { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
}

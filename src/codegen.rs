pub use celery_codegen::task;

#[doc(hidden)]
#[macro_export]
macro_rules! __app_internal {
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
                builder = builder.task_route($pattern, $queue);
            )*

            let celery: $crate::Celery<$broker_type> = $crate::export::block_on(builder.build()).unwrap();

            $(
                $crate::export::block_on(celery.register_task::<$t>()).unwrap();
            )*

            celery
        })
    }};
}

/// A macro for creating a [`Celery`](struct.Celery.html) app.
///
/// At a minimum the `app!` macro requires these 3 arguments (in order):
/// - `broker`: a broker type (currently only AMQP is supported) with an expression for the broker URL in brackets,
/// - `tasks`: a list of tasks to register, and
/// - `task_routes`: a list of routing rules in the form of `pattern => queue`.
///
/// These arguments can be given with or without their keywords.
///
/// # Optional parameters
///
/// Following the task routing rules there are a number of other optional parameters that
/// may appear in arbitrary order (all of which correspond to a method on the
/// [`CeleryBuilder`](struct.CeleryBuilder.html) struct):
///
/// - `default_queue`: Set the
/// [`CeleryBuilder::default_queue`](struct.CeleryBuilder.html#method.default_queue).
/// - `prefetch_count`: Set the [`CeleryBuilder::prefect_count`](struct.CeleryBuilder.html#method.prefect_count).
/// - `heartbeat`: Set the [`CeleryBuilder::heartbeat`](struct.CeleryBuilder.html#method.heartbeat).
/// - `task_timeout`: Set an app-level [`TaskOptions::timeout`](task/struct.TaskOptions.html#structfield.timeout).
/// - `task_max_retries`: Set an app-level [`TaskOptions::max_retries`](task/struct.TaskOptions.html#structfield.max_retries).
/// - `task_min_retry_delay`: Set an app-level [`TaskOptions::min_retry_delay`](task/struct.TaskOptions.html#structfield.min_retry_delay).
/// - `task_max_retry_delay`: Set an app-level [`TaskOptions::max_retry_delay`](task/struct.TaskOptions.html#structfield.max_retry_delay).
/// - `task_retry_for_unexpected`: Set an app-level [`TaskOptions::retry_for_unexpected`](task/struct.TaskOptions.html#structfield.retry_for_unexpected).
/// - `acks_late`: Set an app-level [`TaskOptions::acks_late`](task/struct.TaskOptions.html#structfield.acks_late).
/// - `broker_connection_timeout`: Set the
/// [`CeleryBuilder::broker_connection_timeout`](struct.CeleryBuilder.html#method.broker_connection_timeout).
/// - `broker_connection_max_retries`: Set the
/// [`CeleryBuilder::broker_connection_max_retries`](struct.CeleryBuilder.html#method.broker_connection_max_retries).
///
/// # Examples
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// use celery::TaskResult;
///
/// #[celery::task]
/// fn add(x: i32, y: i32) -> TaskResult<i32> {
///     Ok(x + y)
/// }
///
/// # fn main() {
/// let app = celery::app!(
///     AMQP { std::env::var("AMQP_ADDR").unwrap() },
///     [ add ],
///     [ "*" => "celery" ],
/// );
/// # }
/// ```
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # fn main() {
/// let app = celery::app!(
///     broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [],
///     task_routes = [],
///     task_timeout = 2
/// );
/// # }
/// ```
#[macro_export]
macro_rules! app {
    // Just required fields without trailing comma.
    (
        $(broker =)? AMQP { $broker_url:expr },
        $(tasks =)? [ $( $t:ty ),* $(,)? ],
        $(task_routes =)? [ $( $pattern:expr => $queue:expr ),* $(,)? ]
    ) => {
        $crate::__app_internal!(
            $crate::broker::AMQPBroker { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
        );
    };
    // Required fields with trailing comma and possibly additional options.
    (
        $(broker =)? AMQP { $broker_url:expr },
        $(tasks =)? [ $( $t:ty ),* $(,)? ],
        $(task_routes =)? [ $( $pattern:expr => $queue:expr ),* $(,)? ],
        $( $x:ident = $y:expr ),* $(,)?
    ) => {
        $crate::__app_internal!(
            $crate::broker::AMQPBroker { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
}

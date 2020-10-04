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
            async fn _build_app() -> $crate::Celery::<$broker_type> {
                let broker_url = $broker_url;

                let mut builder = $crate::Celery::<$broker_type>::builder("celery", &broker_url);

                $(
                    builder = builder.$x($y);
                )*

                $(
                    builder = builder.task_route($pattern, $queue);
                )*

                let celery: $crate::Celery<$broker_type> = builder.build().await.unwrap();

                $(
                    celery.register_task::<$t>().await.unwrap();
                )*

                celery
            }

            $crate::export::block_on(_build_app())
        })
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __beat_internal {
    (
        $broker_type:ty { $broker_url:expr },
        [ $( $pattern:expr => $queue:expr ),* ],
        $scheduler_backend:expr,
        $( $x:ident = $y:expr, )*
    ) => {{
        let broker_url = $broker_url;

        let mut builder = $crate::beat::Beat::<$broker_type, _>::custom_builder("beat", &broker_url, $scheduler_backend);

        $(
            builder = builder.$x($y);
        )*

        $(
            builder = builder.task_route($pattern, $queue);
        )*

        $crate::export::block_on(builder.build()).unwrap()
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
/// - `task_time_limit`: Set an app-level [`TaskOptions::time_limit`](task/struct.TaskOptions.html#structfield.time_limit).
/// - `task_hard_time_limit`: Set an app-level [`TaskOptions::hard_time_limit`](task/struct.TaskOptions.html#structfield.hard_time_limit).
/// - `task_max_retries`: Set an app-level [`TaskOptions::max_retries`](task/struct.TaskOptions.html#structfield.max_retries).
/// - `task_min_retry_delay`: Set an app-level [`TaskOptions::min_retry_delay`](task/struct.TaskOptions.html#structfield.min_retry_delay).
/// - `task_max_retry_delay`: Set an app-level [`TaskOptions::max_retry_delay`](task/struct.TaskOptions.html#structfield.max_retry_delay).
/// - `task_retry_for_unexpected`: Set an app-level [`TaskOptions::retry_for_unexpected`](task/struct.TaskOptions.html#structfield.retry_for_unexpected).
/// - `acks_late`: Set an app-level [`TaskOptions::acks_late`](task/struct.TaskOptions.html#structfield.acks_late).
/// - `broker_connection_timeout`: Set the
/// [`CeleryBuilder::broker_connection_timeout`](struct.CeleryBuilder.html#method.broker_connection_timeout).
/// - `broker_connection_retry`: Set the
/// [`CeleryBuilder::broker_connection_retry`](struct.CeleryBuilder.html#method.broker_connection_retry).
/// - `broker_connection_max_retries`: Set the
/// [`CeleryBuilder::broker_connection_max_retries`](struct.CeleryBuilder.html#method.broker_connection_max_retries).
/// - `queues`: Set the
/// [`CeleryBuilder::queues`](struct.CeleryBuilder.html#method.queues).
/// # Examples
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// use celery::prelude::*;
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
///     task_time_limit = 2
/// );
/// # }
/// ```
#[macro_export]
macro_rules! app {
    (
        $(broker =)? AMQP { $broker_url:expr },
        $(tasks =)? [ $( $t:ty ),* $(,)? ],
        $(task_routes =)? [ $( $pattern:expr => $queue:expr ),* $(,)? ]
        $(, $x:ident = $y:expr )* $(,)?
    ) => {
        $crate::__app_internal!(
            $crate::broker::AMQPBroker { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
}

// TODO add support for scheduling tasks here.
/// A macro for creating a [`Beat`](beat/struct.Beat.html) app.
///
/// At a minimum the `beat!` macro requires these 2 arguments (in order):
/// - `broker`: a broker type (currently only AMQP is supported) with an expression for the broker URL in brackets,
/// - `task_routes`: a list of routing rules in the form of `pattern => queue`.
///
/// These arguments can be given with or without their keywords.
///
/// # Custom scheduler backend
///
/// A custom scheduler backend can be given as third argument (with or without using the keyword `scheduler_backend`).
/// If not given, the default [`LocalSchedulerBackend`](struct.LocalSchedulerBackend.html) will be used.
///
/// # Optional parameters
///
/// A number of other optional parameters can be passed as last arguments and in arbitrary order
/// (all of which correspond to a method on the [`BeatBuilder`](beat/struct.BeatBuilder.html) struct):
///
/// - `default_queue`: Set the
/// [`BeatBuilder::default_queue`](beat/struct.BeatBuilder.html#method.default_queue).
/// - `heartbeat`: Set the [`BeatBuilder::heartbeat`](beat/struct.BeatBuilder.html#method.heartbeat).
/// - `broker_connection_timeout`: Set the
/// [`BeatBuilder::broker_connection_timeout`](beat/struct.BeatBuilder.html#method.broker_connection_timeout).
/// - `broker_connection_retry`: Set the
/// [`BeatBuilder::broker_connection_retry`](beat/struct.BeatBuilder.html#method.broker_connection_retry).
/// - `broker_connection_max_retries`: Set the
/// [`BeatBuilder::broker_connection_max_retries`](beat/struct.BeatBuilder.html#method.broker_connection_max_retries).
///
/// # Examples
///
/// Create a `beat` which will send all messages to the `celery` queue:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # fn main() {
/// let beat = celery::beat!(
///     AMQP { std::env::var("AMQP_ADDR").unwrap() },
///     [ "*" => "celery" ],
/// );
/// # }
/// ```
///
/// Create a `beat` with optional parameters:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # fn main() {
/// let beat = celery::beat!(
///     broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
///     task_routes = [],
///     default_queue = "beat_queue"
/// );
/// # }
/// ```
///
/// Create a `beat` with a custom scheduler backend:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// use celery::{beat::ScheduledTask, beat::SchedulerBackend, error::BeatError};
/// use std::collections::BinaryHeap;
///
/// struct CustomSchedulerBackend {}
///
/// impl SchedulerBackend for CustomSchedulerBackend {
///     fn should_sync(&self) -> bool {
///         unimplemented!()
///     }
///
///     fn sync(&mut self, scheduled_tasks: &mut BinaryHeap<ScheduledTask>) -> Result<(), BeatError> {
///         unimplemented!()
///     }
/// }
///
/// # fn main() {
/// let beat = celery::beat!(
///     broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
///     task_routes = [
///         "*" => "beat_queue",
///     ],
///     scheduler_backend = { CustomSchedulerBackend {} }
/// );
/// # }
/// ```
#[macro_export]
macro_rules! beat {
    (
        $(broker =)? AMQP { $broker_url:expr },
        $(task_routes =)? [ $( $pattern:expr => $queue:expr ),* $(,)? ],
        $(scheduler_backend =)? { $scheduler_backend:expr }
        $(, $x:ident = $y:expr )* $(,)?
    ) => {
        $crate::__beat_internal!(
            $crate::broker::AMQPBroker { $broker_url },
            [ $( $pattern => $queue ),* ],
            $scheduler_backend,
            $( $x = $y, )*
        );
    };
    (
        $(broker =)? AMQP { $broker_url:expr },
        $(task_routes =)? [ $( $pattern:expr => $queue:expr ),* $(,)? ]
        $(, $x:ident = $y:expr )* $(,)?
    ) => {
        $crate::__beat_internal!(
            $crate::broker::AMQPBroker { $broker_url },
            [ $( $pattern => $queue ),* ],
            $crate::beat::LocalSchedulerBackend::new(),
            $( $x = $y, )*
        );
    };
}

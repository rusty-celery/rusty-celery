pub use celery_codegen::task;

#[doc(hidden)]
#[macro_export]
macro_rules! __app_internal {
    (
        $broker_type:ty { $broker_url:expr },
        $backend_url:expr,
        [ $( $t:ty ),* ],
        [ $( $pattern:expr => $queue:expr ),* ],
        $( $x:ident = $y:expr, )*
    ) => {{
        async fn _build_app(mut builder: $crate::CeleryBuilder) ->
            $crate::export::Result<$crate::export::Arc<$crate::Celery>> {
            let celery: $crate::Celery = builder.build().await?;

            $(
                celery.register_task::<$t>().await?;
            )*

            Ok($crate::export::Arc::new(celery))
        }

        let broker_url = $broker_url;

        let mut builder = $crate::CeleryBuilder::new("celery", &broker_url, $backend_url);

        $(
            builder = builder.$x($y);
        )*

        $(
            builder = builder.task_route($pattern, $queue);
        )*

        _build_app(builder)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __beat_internal {
    (
        $broker_type:ty { $broker_url:expr },
        $scheduler_backend_type:ty { $scheduler_backend:expr },
        [
            $( $task_name:expr => {
                $task_type:ty,
                $schedule:expr,
                ( $( $task_arg:expr ),* $(,)?)
            } ),*
        ],
        [ $( $pattern:expr => $queue:expr ),* ],
        $( $x:ident = $y:expr, )*
    ) => {{
        async fn _build_beat(mut builder: $crate::beat::BeatBuilder::<$scheduler_backend_type>) ->
            $crate::export::BeatResult<$crate::beat::Beat::<$scheduler_backend_type>> {
            let mut beat = builder.build().await?;

            $(
                beat.schedule_named_task($task_name.to_string(), <$task_type>::new( $( $task_arg ),* ), $schedule);
            )*

            Ok(beat)
        }

        let broker_url = $broker_url;

        let mut builder = $crate::beat::Beat::<$scheduler_backend_type>::custom_builder("beat", &broker_url, $scheduler_backend);

        $(
            builder = builder.$x($y);
        )*

        $(
            builder = builder.task_route($pattern, $queue);
        )*

        _build_beat(builder)
    }};
}

/// A macro for creating a [`Celery`](struct.Celery.html) app.
///
/// At a minimum the `app!` macro requires these 3 arguments (in order):
/// - `broker`: a broker type (currently only AMQP is supported) with an expression for the broker URL in brackets,
/// - `tasks`: a list of tasks to register, and
/// - `task_routes`: a list of routing rules in the form of `pattern => queue`.
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
///
/// # Examples
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # use anyhow::Result;
/// use celery::prelude::*;
///
/// #[celery::task]
/// fn add(x: i32, y: i32) -> TaskResult<i32> {
///     Ok(x + y)
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let app = celery::app!(
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [ add ],
///     task_routes = [ "*" => "celery" ],
/// ).await?;
/// # Ok(())
/// # }
/// ```
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # use anyhow::Result;
/// # use celery::prelude::*;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let app = celery::app!(
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [],
///     task_routes = [],
///     task_time_limit = 2
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! app {
    (
        broker = $broker_type:ty { $broker_url:expr },
        tasks = [ $( $t:ty ),* $(,)? ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ]
        $(, $x:ident = $y:expr )* $(,)?
    ) => {
        $crate::__app_internal!(
            $broker_type { $broker_url },
            Option::<&str>::None,
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };

    (
        broker = $broker_type:ty { $broker_url:expr },
        backend = $backend_type:ty { $backend_url:expr },
        tasks = [ $( $t:ty ),* $(,)? ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ]
        $(, $x:ident = $y:expr )* $(,)?
    ) => {
        $crate::__app_internal!(
            $broker_type { $broker_url },
            Some($backend_url),
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
}

/// A macro for creating a [`Beat`](beat/struct.Beat.html) app.
///
/// At a minimum the `beat!` macro requires these 3 arguments (in order):
/// - `broker`: a broker type (currently only AMQP is supported) with an expression for the broker URL in brackets,
/// - `tasks`: a list of tasks together with their relative schedules (can be empty),
/// - `task_routes`: a list of routing rules in the form of `pattern => queue`.
///
/// # Tasks
///
/// An entry in the task list has the following components:
/// - The name of the task,
/// - The instance of the task to execute,
/// - The task schedule, which can be one of the provided schedules (e.g., [`CronSchedule`](crate::beat::CronSchedule))
///   or any other struct that implements [`Schedule`](crate::beat::Schedule),
/// - A list of arguments for the task in the form of a comma-separated list surrounded by parenthesis.
///
/// # Custom scheduler backend
///
/// A custom scheduler backend can be given as the second argument.
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
/// # use anyhow::Result;
/// # use celery::prelude::*;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let beat = celery::beat!(
///     broker = AMQPBroker{ std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [],
///     task_routes = [ "*" => "celery" ],
/// ).await?;
/// # Ok(())
/// # }
/// ```
///
/// Create a `beat` with a scheduled task:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # use anyhow::Result;
/// # use celery::prelude::*;
/// # use celery::beat::CronSchedule;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// #[celery::task]
/// fn add(x: i32, y: i32) -> TaskResult<i32> {
///     // It is enough to provide the implementation to the worker,
///     // the beat does not need it.
///     unimplemented!()
/// }
///
/// let beat = celery::beat!(
///     broker = AMQPBroker{ std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [
///         "add_task" => {
///             add,
///             schedule = CronSchedule::from_string("*/3 * * * mon-fri")?,
///             args = (1, 2)
///         }
///     ],
///     task_routes = [ "*" => "celery" ],
/// ).await?;
/// # Ok(())
/// # }
/// ```
///
/// Create a `beat` with optional parameters:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # use anyhow::Result;
/// # use celery::prelude::*;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let beat = celery::beat!(
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [],
///     task_routes = [],
///     default_queue = "beat_queue"
/// ).await?;
/// # Ok(())
/// # }
/// ```
///
/// Create a `beat` with a custom scheduler backend:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # use anyhow::Result;
/// use celery::prelude::*;
/// use celery::beat::*;
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
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let beat = celery::beat!(
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     scheduler_backend = CustomSchedulerBackend { CustomSchedulerBackend {} },
///     tasks = [],
///     task_routes = [
///         "*" => "beat_queue",
///     ],
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! beat {
    (
        broker = $broker_type:ty { $broker_url:expr },
        tasks = [
            $( $task_name:expr => {
                $task_type:ty,
                schedule = $schedule:expr,
                args = $args:tt $(,)?
            } ),* $(,)?
        ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ]
        $(, $x:ident = $y:expr )* $(,)?
    ) => {
        $crate::__beat_internal!(
            $broker_type { $broker_url },
            $crate::beat::LocalSchedulerBackend { $crate::beat::LocalSchedulerBackend::new() },
            [ $ (
                $task_name => {
                    $task_type,
                    $schedule,
                    $args
                }
            ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
    (
        broker = $broker_type:ty { $broker_url:expr },
        scheduler_backend = $scheduler_backend_type:ty { $scheduler_backend:expr },
        tasks = [
            $( $task_name:expr => {
                $task_type:ty,
                schedule = $schedule:expr,
                args = $args:tt $(,)?
            } ),* $(,)?
        ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ]
        $(, $x:ident = $y:expr )* $(,)?
    ) => {
        $crate::__beat_internal!(
            $broker_type { $broker_url },
            $scheduler_backend_type { $scheduler_backend },
            [ $ (
                $task_name => {
                    $task_type,
                    $schedule,
                    $args
                }
            ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
}

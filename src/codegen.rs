pub use celery_codegen::task;

#[doc(hidden)]
#[macro_export]
macro_rules! __app_internal {
    (
        $runtime:expr,
        $broker_type:ty { $broker_url:expr },
        [ $( $t:ty ),* ],
        [ $( $pattern:expr => $queue:expr ),* ],
        $( $x:ident = $y:expr, )*
    ) => {{
        async fn _build_app(runtime: $crate::export::Arc<$crate::export::Runtime>) ->
            $crate::export::Result<$crate::export::Arc<$crate::Celery::<$broker_type>>> {

            let broker_url = $broker_url;

            let mut builder = $crate::Celery::<$broker_type>::builder("celery", &broker_url);

            $(
                builder = builder.$x($y);
            )*

            $(
                builder = builder.task_route($pattern, $queue);
            )*

            let celery: $crate::Celery<$broker_type> = builder.build(runtime).await?;

            $(
                celery.register_task::<$t>().await?;
            )*

            Ok($crate::export::Arc::new(celery))
        }

        _build_app($runtime)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __beat_internal {
    (
        $runtime:expr,
        $broker_type:ty { $broker_url:expr },
        $scheduler_backend_type:ty { $scheduler_backend:expr },
        [ $( $pattern:expr => $queue:expr ),* ],
        $( $x:ident = $y:expr, )*
    ) => {{
        async fn _build_beat(runtime: $crate::export::Arc<$crate::export::Runtime>) ->
            $crate::export::Result<$crate::beat::Beat::<$broker_type, $scheduler_backend_type>> {

            let broker_url = $broker_url;

            let mut builder = $crate::beat::Beat::<$broker_type, $scheduler_backend_type>::custom_builder("beat", &broker_url, $scheduler_backend);

            $(
                builder = builder.$x($y);
            )*

            $(
                builder = builder.task_route($pattern, $queue);
            )*

            Ok(builder.build(runtime).await?)
        }

        _build_beat($runtime)
    }};
}

/// A macro for creating a [`Celery`](struct.Celery.html) app.
///
/// At a minimum the `app!` macro requires these 4 arguments (in order):
/// - `runtime`: a `tokio` async runtime, `Arc<tokio::runtime::Runtime>`,
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
///
/// # Examples
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # use anyhow::Result;
/// # use std::sync::Arc;
/// # use tokio::runtime::Runtime;
/// use celery::prelude::*;
///
/// #[celery::task]
/// fn add(x: i32, y: i32) -> TaskResult<i32> {
///     Ok(x + y)
/// }
///
/// # fn main() -> Result<()> {
/// # let rt = Arc::new(Runtime::new()?);
/// # rt.block_on(async {
/// let app = celery::app!(
///     runtime = rt.clone(),
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [ add ],
///     task_routes = [ "*" => "celery" ],
/// ).await?;
/// # Ok(())
/// # })
/// # }
/// ```
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # use anyhow::Result;
/// # use std::sync::Arc;
/// # use tokio::runtime::Runtime;
/// # use celery::prelude::*;
/// # fn main() -> Result<()> {
/// # let rt = Arc::new(Runtime::new().unwrap());
/// # rt.block_on(async {
/// let app = celery::app!(
///     runtime = rt.clone(),
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [],
///     task_routes = [],
///     task_time_limit = 2
/// ).await?;
/// # Ok(())
/// # })
/// # }
/// ```
#[macro_export]
macro_rules! app {
    (
        runtime = $runtime:expr,
        broker = $broker_type:ty { $broker_url:expr },
        tasks = [ $( $t:ty ),* $(,)? ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ]
        $(, $x:ident = $y:expr )* $(,)?
    ) => {
        $crate::__app_internal!(
            $runtime,
            $broker_type { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
}

// TODO add support for scheduling tasks here.
/// A macro for creating a [`Beat`](beat/struct.Beat.html) app.
///
/// At a minimum the `beat!` macro requires these 3 arguments (in order):
/// - `runtime`: a `tokio` async runtime, `Arc<tokio::runtime::Runtime>`,
/// - `broker`: a broker type (currently only AMQP is supported) with an expression for the broker URL in brackets,
/// - `task_routes`: a list of routing rules in the form of `pattern => queue`.
///
/// These arguments can be given with or without their keywords.
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
/// # use std::sync::Arc;
/// # use tokio::runtime::Runtime;
/// # fn main() -> Result<()> {
/// # let rt = Arc::new(Runtime::new().unwrap());
/// # rt.block_on(async {
/// let beat = celery::beat!(
///     runtime = rt.clone(),
///     broker = AMQPBroker{ std::env::var("AMQP_ADDR").unwrap() },
///     task_routes = [ "*" => "celery" ],
/// ).await?;
/// # Ok(())
/// # })
/// # }
/// ```
///
/// Create a `beat` with optional parameters:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # use anyhow::Result;
/// # use celery::prelude::*;
/// # use std::sync::Arc;
/// # use tokio::runtime::Runtime;
/// # fn main() -> Result<()> {
/// # let rt = Arc::new(Runtime::new().unwrap());
/// # rt.block_on(async {
/// let beat = celery::beat!(
///     runtime = rt.clone(),
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     task_routes = [],
///     default_queue = "beat_queue"
/// ).await?;
/// # Ok(())
/// # })
/// # }
/// ```
///
/// Create a `beat` with a custom scheduler backend:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// # use anyhow::Result;
/// # use std::sync::Arc;
/// # use tokio::runtime::Runtime;
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
/// # fn main() -> Result<()> {
/// # let rt = Arc::new(Runtime::new().unwrap());
/// # rt.block_on(async {
/// let beat = celery::beat!(
///     runtime = rt.clone(),
///     broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
///     scheduler_backend = CustomSchedulerBackend { CustomSchedulerBackend {} },
///     task_routes = [
///         "*" => "beat_queue",
///     ],
/// ).await?;
/// # Ok(())
/// # })
/// # }
/// ```
#[macro_export]
macro_rules! beat {
    (
        runtime = $runtime:expr,
        broker = $broker_type:ty { $broker_url:expr },
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ]
        $(, $x:ident = $y:expr )* $(,)?
    ) => {
        $crate::__beat_internal!(
            $runtime,
            $broker_type { $broker_url },
            $crate::beat::LocalSchedulerBackend { $crate::beat::LocalSchedulerBackend::new() },
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
    (
        runtime = $runtime:expr,
        broker = $broker_type:ty { $broker_url:expr },
        scheduler_backend = $scheduler_backend_type:ty { $scheduler_backend:expr },
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ]
        $(, $x:ident = $y:expr )* $(,)?
    ) => {
        $crate::__beat_internal!(
            $runtime,
            $broker_type { $broker_url },
            $scheduler_backend_type { $scheduler_backend },
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
}

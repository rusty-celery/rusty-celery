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
/// At a minimum the `app!` macro requires 3 arguments:
/// - a broker type (currently only AMQP is supported) with an expression for the broker URL in brackets,
/// - a list of tasks to register, and
/// - a list of routing rules in the form of `pattern => queue`.
///
/// For example:
///
/// ```rust,no_run
/// # #[macro_use] extern crate celery;
/// #[celery::task]
/// fn add(x: i32, y: i32) -> i32 {
///     x + y
/// }
///
/// # fn main() {
/// let app = celery::app!(
///     broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
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
/// # fn main() {
/// let app = celery::app!(
///     broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
///     tasks = [],
///     task_routes = [],
///     task_timeout = 2,
/// );
/// # }
/// ```
#[macro_export]
macro_rules! app {
    (
        broker = AMQP { $broker_url:expr },
        tasks = [ $( $t:ty ),* $(,)? ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ],
        $( $x:ident = $y:expr, )*
    ) => {
        $crate::__app_internal!(
            $crate::broker::AMQPBroker { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
    (
        broker = AMQP { $broker_url:expr },
        tasks = [ $( $t:ty ),* $(,)? ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ],
        $( $x:ident = $y:expr, )*
    ) => {
        $crate::__app_internal!(
            $crate::broker::AMQPBroker { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
    (
        broker = AMQP { $broker_url:expr },
        tasks = [ $( $t:ty ),* $(,)? ],
        task_routes = [ $( $pattern:expr => $queue:expr ),* $(,)? ],
        $( $x:ident = $y:expr, )*
    ) => {
        $crate::__app_internal!(
            $crate::broker::AMQPBroker { $broker_url },
            [ $( $t ),* ],
            [ $( $pattern => $queue ),* ],
            $( $x = $y, )*
        );
    };
}

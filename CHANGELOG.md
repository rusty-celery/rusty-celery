# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [0.2.3] - 2019-03-10

### Changed

- `CeleryBuilder::task_route` now infallible. Error could be raised during the `build` phase instead.
- `Celery::consume_from` will return `Err(CeleryError::NoQueueToConsume)` if the slice of queues is empty.

## [0.2.2] - 2019-03-06

### Changed

- `Celery::consume_from` now takes multiple queues instead of just a single queue.
- Retry ETA method moved from tracer to task so that it can be customized.
- `TaskError` variants restricted to only `ExpectedError`, `UnexpectedError`, and `TimeoutError`. The `Retry` and `ExpirationError` variants moved to a new (non-public) error type: `TracerError`.

## [0.2.1] - 2019-03-05

### Added

- `on_failure` and `on_success` options to `task` attribute macro.

### Changed

- Removed `task_id` and `params` arguments to `on_failure` and `on_success` callbacks, since those can be gotten from the request object.

## [0.2.0] - 2019-03-02

### Added

- A `Signature` struct with includes task execution options (previously the fields in `TaskSendOptions`).
- A `bind` argument to the `task` macro. When `bind = true` is given, the task will be run as an instance method.
- A `Request` struct.

### Changed

- `protocol::TryIntoMessage` trait renamed to `TryCreateMessage` and the one trait function `try_into_message` renamed to `try_create_message` to better reflect the fact that the trait function does not consume `self`.
- Task parameters are now separated from task struct.
- Task callback methods `on_failure` and `on_success` are now instance methods.
- `Celery::send_task` now takes a `Signature` instead of a `Task`.
- When tasks are defined through the `task` macro by annoting a function, that function needs to be explicity marked async for the function to use async / await syntax.

### Removed

- `TaskContext` struct.
- `TaskSendOptions`.
- `Celery::send_task_with`.

## [0.2.0-alpha.2] - 2019-02-24

### Changed

- Uses of `std::sync::Mutex` and `std::sync::RwLock` changed to their async-aware equivalents from `tokio`.
- The `Celery::register_task` method is now an async function due to the above.
- Fixed bug where tasks with a future ETA were acked before they were due, resulting in such tasks being lost if the worker was shutdown before they were due.

### Removed

- The `SyncError` variants have been removed.

## [0.2.0-alpha.1] - 2019-02-19

### Changed

- `Celery::consume_from` now only accepts a single queue (once again) since there was a critical bug when we allowed consuming from multiple queues.

## [0.2.0-alpha.0] - 2019-02-17

### Added

- Several error enums: `CeleryError`, `TaskError`, `BrokerError`, `ProtocolError`.
- `TaskResultExt` for easily converting the error type in a `Result` to a `TaskError` variant.

### Changed

- The `error` module.
- The structure of the public API is now more compact. We expose a few more modules, including `broker`, `task`, and `error` instead of exporting all of the public elements from the crate root.
- The `app` macro (previously `celery_app`) no longer takes an actual broker type (like `AMQPBroker`) for the `broker` parameter. Instead you can just use the literal token `AMQP`. This means one less import for the user.

### Removed

- The `Error` type.
- The `celery_app` macro has been renamed to just `app`.
- The `ResultExt` re-export.

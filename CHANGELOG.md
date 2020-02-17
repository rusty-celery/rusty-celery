# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

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

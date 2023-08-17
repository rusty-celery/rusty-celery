<div align="center">
    <br>
    <img src="img/rusty-celery-logo-transparent.png"/>
    <br>
    <br>
    <p>
    A Rust implementation of <a href="https://github.com/celery/celery">Celery</a> for producing and consuming asynchronous tasks with a distributed message queue.
    </p>
    <hr/>
</div>
<p align="center">
    <a href="https://github.com/rusty-celery/rusty-celery/actions">
        <img alt="Build" src="https://github.com/rusty-celery/rusty-celery/workflows/CI/badge.svg?event=push&branch=main">
    </a>
    <a href="https://github.com/rusty-celery/rusty-celery/blob/main/LICENSE">
        <img alt="License" src="https://img.shields.io/github/license/rusty-celery/rusty-celery.svg?color=blue&cachedrop">
    </a>
    <a href="https://crates.io/crates/celery">
        <img alt="Crates" src="https://img.shields.io/crates/v/celery.svg?color=blue">
    </a>
    <a href="https://docs.rs/celery/">
        <img alt="Docs" src="https://img.shields.io/badge/docs.rs-API%20docs-blue">
    </a>
    <a href="https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aissue+is%3Aopen+label%3A%22Status%3A+Help+Wanted%22">
        <img alt="Help wanted" src="https://img.shields.io/github/issues/rusty-celery/rusty-celery/Status%3A%20Help%20Wanted?label=Help%20Wanted">
    </a>
</p>
<br/>


We welcome contributions from everyone regardless of your experience level with Rust. For complete beginners, see [HACKING_QUICKSTART.md](https://github.com/rusty-celery/rusty-celery/blob/main/HACKING_QUICKSTART.md).

If you already know the basics of Rust but are new to Celery, check out the [Rusty Celery Book](https://rusty-celery.github.io/) or the original Python [Celery Project](http://www.celeryproject.org/).

## Quick start

Define tasks by decorating functions with the [`task`](https://docs.rs/celery/*/celery/attr.task.html) attribute.

```rust
use celery::prelude::*;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}
```

Create an app with the [`app`](https://docs.rs/celery/*/celery/macro.celery_app.html) macro
and register your tasks with it:

```rust
let my_app = celery::app!(
    broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap() },
    tasks = [add],
    task_routes = [
        "*" => "celery",
    ],
).await?;
```

Then send tasks to a queue with

```rust
my_app.send_task(add::new(1, 2)).await?;
```

And consume tasks as a worker from a queue with

```rust
my_app.consume().await?;
```

## Examples

The [`examples/`](https://github.com/rusty-celery/rusty-celery/tree/main/examples) directory contains:

- a simple Celery app implemented in Rust using an AMQP broker ([`examples/basic`](https://github.com/rusty-celery/rusty-celery/tree/main/examples/basic)),
- an advanced Celery app implemented in Rust using an AMQP broker ([`examples/advanced`](https://github.com/rusty-celery/rusty-celery/tree/main/examples/advanced/)).
- and a Beat app implemented in Rust ([`examples/beat_app`](https://github.com/rusty-celery/rusty-celery/tree/main/examples/beat_app/)).
- the same Celery app implemented in Python ([`examples/advanced/celery_app.py`](https://github.com/rusty-celery/rusty-celery/blob/main/examples/advanced/celery_app.py)),

### Prerequisites

If you already have an AMQP broker running you can set the environment variable `AMQP_ADDR` to your broker's URL (e.g., `amqp://localhost:5672//`, where
the second slash at the end is the name of the [default vhost](https://www.rabbitmq.com/access-control.html#default-state)).
Otherwise simply run the helper script:

```bash
./scripts/brokers/amqp.sh
```

This will download and run the official [RabbitMQ](https://www.rabbitmq.com/) image (RabbitMQ is a popular AMQP broker).

### Run the examples

![](./img/demo.gif)

The example folder contains various example projects. You can follow the Readme file in those folders and run example programs.

## Road map and current state

✅ = Supported and mostly stable, although there may be a few incomplete features.<br/>
⚠️ = Partially implemented and under active development.<br/>
🔴 = Not supported yet but on-deck to be implemented soon.

### Core

|                  | Status  | Tracking  |
| ---------------- |:-------:| --------- |
| Protocol         | ⚠️      | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Protocol%20Feature?label=Issues)](https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aissue+label%3A%22Protocol+Feature%22+is%3Aopen) |
| Producers        | ✅      | |
| Consumers        | ✅      | |
| Brokers          | ✅      | |
| Beat             | ✅      | |
| Backends         | 🔴      | |
| [Baskets](https://github.com/rusty-celery/rusty-celery/issues/53) | 🔴      | |

### Brokers

|       | Status | Tracking |
| ----- |:------:| -------- |
| AMQP  | ✅     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Broker%3A%20AMQP?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Broker%3A%20AMQP) |
| Redis | ✅     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Broker%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Broker%3A%20Redis) |

### Backends

|             | Status | Tracking |
| ----------- |:------:| -------- |
| RPC         | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20RPC?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20RPC) |
| Redis       | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20Redis) |

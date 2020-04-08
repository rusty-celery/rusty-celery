<div align="center">
    <br>
    <img src="img/rusty-celery-logo.png" width="400"/>
    <p>
    A Rust implementation of <a href="https://github.com/celery/celery">Celery</a> for producing and consuming asyncronous tasks with a distributed message queue.
    </p>
    <hr/>
</div>
<p align="center">
    <a href="https://github.com/rusty-celery/rusty-celery/actions">
        <img alt="Build" src="https://github.com/rusty-celery/rusty-celery/workflows/CI/badge.svg?event=push&branch=master">
    </a>
    <a href="https://github.com/rusty-celery/rusty-celery/blob/master/LICENSE">
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
    <a href="https://discord.gg/PV3azbB">
        <img alt="Discord" src="https://img.shields.io/discord/689533070247723078?logo=discord">
    </a>
</p>
<br/>


We welcome contributions from everyone regardless of your experience level with Rust. For complete beginners, see [HACKING_QUICKSTART.md](https://github.com/rusty-celery/rusty-celery/blob/master/HACKING_QUICKSTART.md).

If you already know the basics of Rust, the [Rusty Celery Book](https://rusty-celery.github.io/) is the best place to start. If you're coming from Python you may also be interested to know [what's different](https://rusty-celery.github.io/coming-from-python/index.html). And if you've never heard of Celery, the official [Celery Project](http://www.celeryproject.org/) is a great source of tutorials and overviews.

## Quick start

Define tasks by decorating functions with the [`task`](https://docs.rs/celery/*/celery/attr.task.html) attribute.

```rust
use celery::TaskResult;

#[celery::task]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}
```

Create an app with the [`app`](https://docs.rs/celery/*/celery/macro.celery_app.html) macro
and register your tasks with it:

```rust
let my_app = celery::app!(
    broker = AMQP { std::env::var("AMQP_ADDR").unwrap() },
    tasks = [add],
    task_routes = [
        "*" => "celery",
    ],
);
```

Then send tasks to a queue with

```rust
my_app.send_task(add::new(1, 2)).await?;
```

And consume tasks as a worker from a queue with

```rust
my_app.consume().await?;
```

The `./examples` directory contains a simple Celery app that is implemented in both Rust ([celery_app.rs](https://github.com/rusty-celery/rusty-celery/blob/master/examples/celery_app.rs)) and Python ([celery_app.py](https://github.com/rusty-celery/rusty-celery/blob/master/examples/celery_app.py)) using an AMQP broker. 

If you already have an AMQP broker running you can set the environment variable `AMQP_ADDR` to your broker's URL (e.g., `amqp://localhost:5672//`, where
the second slash at the end is the name of the [default vhost](https://www.rabbitmq.com/access-control.html#default-state)).
Otherwise simply run the helper script:
```bash
./scripts/brokers/amqp.sh
```

This will download and run the official [RabbitMQ](https://www.rabbitmq.com/) image (RabbitMQ is a popular AMQP broker). Then from a separate terminal run the script:

```bash
./examples/python_to_rust.sh
```

This sends a series of tasks from the Python app to the Rust app. You can also send tasks from Rust to Python by running:

```bash
./examples/rust_to_python.sh
```

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
| Backends         | 🔴      | |
| [Beat](https://github.com/rusty-celery/rusty-celery/issues/55)    | 🔴      | |
| [Baskets](https://github.com/rusty-celery/rusty-celery/issues/53) | 🔴      | |

### Brokers

|       | Status | Tracking |
| ----- |:------:| -------- |
| AMQP  | ✅     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Broker%3A%20AMQP?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Broker%3A%20AMQP) |
| Redis | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Broker%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Broker%3A%20Redis) |

### Backends

|             | Status | Tracking |
| ----------- |:------:| -------- |
| RPC         | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20RPC?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20RPC) |
| Redis       | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20Redis) |

## Team

[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/0)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/0)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/1)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/1)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/2)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/2)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/3)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/3)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/4)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/4)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/5)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/5)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/6)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/6)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/7)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/7)

Rusty Celery is an open source community effort. It is also backed by [Structurely](https://structurely.com/), a start-up building conversational AI that has been using Python Celery in production since 2017.

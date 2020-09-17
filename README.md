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

If you already know the basics of Rust but are new to Celery, check out the [Rusty Celery Book](https://rusty-celery.github.io/) or the original Python [Celery Project](http://www.celeryproject.org/).

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

## Examples

The `./examples` directory contains:

- a simple Celery app implemented in Rust using an AMQP broker ([celery_app.rs](https://github.com/rusty-celery/rusty-celery/blob/master/examples/celery_app.rs));
- the same Celery app implemented in Python ([celery_app.py](https://github.com/rusty-celery/rusty-celery/blob/master/examples/celery_app.py));
- and a Beat app implemented in Rust ([beat_app.rs](https://github.com/rusty-celery/rusty-celery/blob/master/examples/beat_app.rs)).

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

#### Run Rust Celery app

You can consume tasks with:

```bash
cargo run --example celery_app consume
```

And you can produce tasks with:

```bash
cargo run --example celery_app produce
```

#### Run Python Celery app

Similarly, you can consume or produce tasks from Python by running


```bash
python examples/celery_app.py consume
```

or

```bash
python examples/celery_app.py produce
```

You'll need to have Python 3 installed, along with the requirements listed in the `requirements.txt` file.

#### Run Rust Beat app

You can start the Rust beat with:

```bash
cargo run --example beat_app
```

And then you can consume tasks from Rust or Python as explained above.

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
| Redis | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Broker%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Broker%3A%20Redis) |

### Backends

|             | Status | Tracking |
| ----------- |:------:| -------- |
| RPC         | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20RPC?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20RPC) |
| Redis       | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20Redis) |

## Team

[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/0)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/0)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/1)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/1)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/2)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/2)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/3)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/3)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/4)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/4)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/5)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/5)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/6)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/6)[![](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/images/7)](https://sourcerer.io/fame/epwalsh/rusty-celery/rusty-celery/links/7)

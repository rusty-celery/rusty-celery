<p align="center" style="font-size:300px;">
    <br>
    <img src="https://structurely-images.s3-us-west-2.amazonaws.com/logos/rusty-celery.png" width="600"/>
    <br>
<p>
<p align="center">
    <a href="https://github.com/rusty-celery/rusty-celery/actions">
        <img alt="Build" src="https://github.com/rusty-celery/rusty-celery/workflows/CI/badge.svg">
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
        <img alt="Docs" src="https://img.shields.io/github/issues/rusty-celery/rusty-celery/Status%3A%20Help%20Wanted?label=Help%20Wanted">
    </a>
</p>

A Rust implementation of [Celery](https://github.com/celery/celery) for producing and consuming asyncronous tasks with a distributed message queue.

We welcome contributions from everyone regardless of your experience level with Rust. For complete beginners, see [HACKING_QUICKSTART.md](https://github.com/rusty-celery/rusty-celery/blob/master/HACKING_QUICKSTART.md).

If you already know the basics of Rust, the [Rusty Celery Book](https://rusty-celery.github.io/) is the best place to start. It contains numerous examples and guides to best practices. If you're coming from Python you may also be interested to know [what's different](https://rusty-celery.github.io/coming-from-python/index.html). And if you've never heard of Celery, the official [Celery Project](http://www.celeryproject.org/) is a great source of tutorials and overviews.

## Quick start

The `./examples` directory contains a simple Celery app that is implemented in both Rust ([celery_app.rs](https://github.com/rusty-celery/rusty-celery/blob/master/examples/celery_app.rs)) and Python ([celery_app.py](https://github.com/rusty-celery/rusty-celery/blob/master/examples/celery_app.py)) using an AMQP broker. 

Before running the examples you'll need to have an AMQP broker running. [RabbitMQ](https://www.rabbitmq.com/) is a popular choice and provides a [Docker image](https://hub.docker.com/_/rabbitmq) that you can run locally. Another option would be to use a hosting provider like [CloudAMQP](https://www.cloudamqp.com/), which provides a free tier broker for development purposes.

Once you have your broker running, set the environment variable `AMQP_ADDR` to your broker URL. I also recommend setting `RUST_LOG=info`. Then you can send a task from Python to Rust by running the script

```bash
./examples/python_to_rust.sh
```

And send a task from Rust to Python by running

```bash
./examples/rust_to_python.sh
```

## Road map and current state

🟢 = Supported and mostly stable, although there may be a few incomplete features.<br/>
🟠 = Partially implemented and under active development.<br/>
🔴 = Not supported yet but on-deck to be implemented soon.

### Core

|                  | Status  | Tracking  |
| ---------------- |:-------:| --------- |
| Protocol         | 🟠      | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Protocol%20Feature?label=Issues)](https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aissue+label%3A%22Protocol+Feature%22+is%3Aopen) |
| Producers        | 🟢      | |
| Consumers        | 🟢      | |
| Brokers          | 🟢      | |
| Backends         | 🟠      | |
| [Beat](https://github.com/rusty-celery/rusty-celery/issues/55)    | 🔴      | |
| [Baskets](https://github.com/rusty-celery/rusty-celery/issues/53) | 🔴      | |

### Brokers

|       | Status | Tracking |
| ----- |:------:| -------- |
| AMQP  | 🟢     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Broker%3A%20AMQP?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Broker%3A%20AMQP) |
| Redis | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Broker%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Broker%3A%20Redis) |

### Backends

|             | Status | Tracking |
| ----------- |:------:| -------- |
| RPC         | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20RPC?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20RPC) |
| Redis       | 🔴     | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20Redis) |

## Team

Rusty Celery is an open source project backed by [Structurely](https://structurely.com/). Structurely is a start-up building customizable AI inside sales agents that has been using Celery in production back to circa 2017.

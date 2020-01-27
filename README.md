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

- Send tasks from Python to Rust or vice versa ↔️
- Use provided brokers and backends (coming soon) or implement your own 🔧
- Natural async / await interface 🎇
- High performance and safe 🔥 🔥 🔥

Inspired by, well... Celery, but also [batch-rs](https://github.com/kureuil/batch-rs) especially for it's awesome macro implementation.

## Quick links

- [API docs](https://docs.rs/celery/)
- [The Rusty Celery Book](https://rusty-celery.github.io/) (work in progress)
- [Library on crates.io](https://crates.io/crates/celery)
- [Contributing](https://github.com/rusty-celery/rusty-celery/blob/master/CONTRIBUTING.md)

## Getting started

The `./examples` directory contains a simple Celery app that is implemented in both Rust ([celery_app.rs](https://github.com/rusty-celery/rusty-celery/blob/master/examples/celery_app.rs)) and Python ([celery_app.py](https://github.com/rusty-celery/rusty-celery/blob/master/examples/celery_app.py)) using an AMQP broker. 

Before running the examples you'll need to have the broker set up. RabbitMQ is a popular implementation of the AMQP protocol and provides a [Docker image](https://hub.docker.com/_/rabbitmq) that you can run locally. Another option would be to use a hosting provider like [CloudAMQP](https://www.cloudamqp.com/), which provides a free tier broker for development purposes.

Once you have your broker running, set the environment variable `AMQP_ADDR` to your broker URL. I also recommend setting `RUST_LOG=debug`. Then you can send a task from Python to Rust by running the script

```bash
./examples/python_to_rust.sh
```

And send a task from Rust to Python by running

```bash
./examples/rust_to_python.sh
```

## Road map and current state

### Functionality

|               | Implemented   | In-progress  |
| ------------- | ------------- | ------------ |
| Core protocol | [![](https://img.shields.io/github/issues-closed/rusty-celery/rusty-celery/Protocol%20Feature?label=Issues&color=success)](https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aissue+label%3A%22Protocol+Feature%22+is%3Aclosed) | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Protocol%20Feature?label=Issues)](https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aissue+label%3A%22Protocol+Feature%22+is%3Aopen) |
| Protocol enhancements | [![](https://img.shields.io/github/issues-closed/rusty-celery/rusty-celery/Protocol%20Enhancement?label=Issues&color=success)](https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aissue+label%3A%22Protocol+Enhancement%22+is%3Aclosed) | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Protocol%20Enhancement?label=Issues)](https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aissue+label%3A%22Protocol+Enhancement%22+is%3Aopen) |

### Brokers

|       | Status | Tracking |
| ----- | ------ | -------- |
| AMQP  | [![Supported](https://img.shields.io/badge/Status-Supported-success)](https://docs.rs/celery/0.1.0-alpha.6/celery/struct.AMQPBroker.html) | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Broker%3A%20AMQP?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Broker%3A%20AMQP) |
| Redis | [![On deck](https://img.shields.io/badge/Status-On%20Deck-yellow)](https://github.com/rusty-celery/rusty-celery/issues/6) | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Broker%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Broker%3A%20Redis) |

### Backends

|             | Status | Tracking |
| ----------- | ------ | -------- |
| RPC (AMQP)  | [![On deck](https://img.shields.io/badge/Status-On%20Deck-yellow)](https://github.com/rusty-celery/rusty-celery/issues/67) | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20RPC?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20RPC) |
| Redis       | [![On deck](https://img.shields.io/badge/Status-On%20Deck-yellow)](https://github.com/rusty-celery/rusty-celery/issues/68) | [![](https://img.shields.io/github/issues/rusty-celery/rusty-celery/Backend%3A%20Redis?label=Issues)](https://github.com/rusty-celery/rusty-celery/labels/Backend%3A%20Redis) |

## Team

Rusty Celery is an open source project backed by [Structurely](https://structurely.com/). Structurely is a start-up building customizable AI inside sales agents that has been using Celery in production back to circa 2017.

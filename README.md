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
</p>

A Rust implementation of [Celery](https://github.com/celery/celery) for producing and consuming asyncronous tasks with a distributed message queue.

Inspired by, well... Celery, but also [batch-rs](https://github.com/kureuil/batch-rs) especially for it's awesome macro implementation.

## Features

- Send tasks from Python to Rust or vice versa ↔️
- Use provided brokers and backends (coming soon) or implement your own 🔧
- Natural async / await interface 🎇
- High performance and safe 🔥 🔥 🔥

## Quick links

- [API docs](https://docs.rs/celery/)
- [The Rusty Celery Book](https://rusty-celery.github.io/) (work in progress)
- [Library on crates.io](https://crates.io/crates/celery)
- [Issues and pending features](https://github.com/rusty-celery/rusty-celery/issues)
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

## Team

Rusty Celery is an open source project backed by [Structurely](https://structurely.com/). Structurely is a start-up building customizable AI inside sales agents that has been using Celery in production back to circa 2017.

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
        <img alt="Crates" src="https://img.shields.io/crates/v/cached-path.svg">
    </a>
</p>

A Rust implementation of the [Celery](https://github.com/celery/celery) protocol, fully compatible with Python workers and producers.

Inspired by, well... Celery, but also [batch-rs](https://github.com/kureuil/batch-rs) especially for it's awesome macro implementation.

## Features

- Send tasks from Python to Rust or vice versa :left_right_arrow:
- Use provided brokers and backends or implement your own :wrench:
- Natural async / await interface :sparkler:
- Easily customizable with callbacks :telephone:
- High performance and safe :fire: :fire: :fire:

## Examples

The `./examples` directory contains a simple Celery app that is implemented in both Python and Rust using an AMQP broker. Before running the examples you'll need to set the environment variable `AMQP_ADDR` to your broker URL.

Then send a task from Python to Rust by running

```
bash ./examples/python_to_rust.sh
```

Similary, send a task from Rust to Python by running

```
bash ./examples/rust_to_python.sh
```

## Missing features

`rusty-celery` is still in the early stages and therefore there are several features of the Celery protocol that are still missing. To see what's currently missing search for issues with the label ["Protocol Feature"](https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aopen+is%3Aissue+label%3A%22Protocol+Feature%22).

## Contributing

We welcome contributors! If you're interesting in contributing, a good place to start would be any issue marked with the label ["Status: Help Wanted"](https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aopen+is%3Aissue+label%3A%22Status%3A+Help+Wanted%22).

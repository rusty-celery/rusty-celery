# Beat App Example for Rusty Celery

This example demonstrates how to use the `celery` crate to build a Celery beat scheduler for scheduling and managing periodic tasks. The Celery beat scheduler allows you to define tasks that run at specified intervals or according to a cron-like schedule.

## Prerequisites
Before running the example, make sure you have the following prerequisites installed:

- RabbitMQ or another AMQP broker: This example assumes that you have a running RabbitMQ server or another AMQP broker. If you don't have one already, you can install RabbitMQ following the instructions on [https://www.rabbitmq.com/download.html](https://www.rabbitmq.com/download.html).


## Environment variable
Create a .env file in the current directory (from where you are reading this README.md file). and add the following environment variable to specify the AMQP broker address. Replace "amqp://127.0.0.1:5672" with the actual address of your RabbitMQ server if needed:

```env
AMQP_ADDR=amqp://127.0.0.1:5672
```


## Running the Program 
```bash
cargo run 
```

And then you can consume tasks from Rust or Python as mentioned in the advanced examples folder.

# Advanced Example for Rusty Celery

## Prerequisites
Before running the example, make sure you have the following prerequisites installed:

- RabbitMQ or another AMQP broker: This example assumes that you have a running RabbitMQ server or another AMQP broker. If you don't have one already, you can install RabbitMQ following the instructions on [https://www.rabbitmq.com/download.html](https://www.rabbitmq.com/download.html).


## Environment variable
Create a .env file in the current directory (from where you are reading this README.md file). and add the following environment variable to specify the AMQP broker address. Replace "amqp://127.0.0.1:5672" with the actual address of your RabbitMQ server if needed:

```env
AMQP_ADDR=amqp://127.0.0.1:5672
```


## Running the consumer
(make sure to run the consumer before producer)
```bash
# This will listens for incoming tasks on the celery queue
cargo run -- consume
```

## Running the Producer
```bash
# This will send the task request
cargo run -- produce add
```


#### Run Python Celery app

Similarly, you can consume or produce tasks from Python by running


```bash
python celery_app.py consume [task_name]
```

or

```bash
python celery_app.py produce
```

You'll need to have Python 3 installed, along with the requirements listed in the `requirements.txt` file.  You'll also have to provide a task name. This example implements 4 tasks: `add`, `buggy_task`, `long_running_task` and `bound_task

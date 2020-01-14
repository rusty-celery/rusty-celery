import os

from celery import Celery


app = Celery("celery", broker=os.environ.get("AMQP_ADDR", "amqp://127.0.0.1:5672"))
app.conf.update(result_backend=None, task_ignore_result=True)


@app.task(name="add")
def add(x=0, y=0):
    return x + y


if __name__ == "__main__":
    add.apply_async(kwargs={"x": 1, "y": 2})

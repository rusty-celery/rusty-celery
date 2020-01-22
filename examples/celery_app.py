import os

from celery import Celery


app = Celery("celery", broker=os.environ.get("AMQP_ADDR", "amqp://127.0.0.1:5672"))
app.conf.update(result_backend=None, task_ignore_result=True)


@app.task(name="add")
def add(x, y):
    return x + y


if __name__ == "__main__":
    add.apply_async(args=[1, 2], countdown=6)
    add.apply_async(args=[1, 2], countdown=7)
    add.apply_async(args=[1, 2], countdown=5)
    add.apply_async(args=[1, 2], countdown=4)
    add.apply_async(args=[2, 2])
    #  app.send_task("buggy_task")

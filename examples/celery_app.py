import os

from celery import Celery


my_app = Celery("celery", broker=os.environ.get("AMQP_ADDR", "amqp://127.0.0.1:5672"))
my_app.conf.update(result_backend=None, task_ignore_result=True)


@my_app.task(name="add")
def add(x, y):
    return x + y


if __name__ == "__main__":
    #  add.apply_async(args=[1, 0], countdown=5, expires=2)
    add.apply_async(args=[1, 1], countdown=5)
    #  add.apply_async(args=[1, 1])
    #  add.apply_async(args=[1, 2], countdown=10)
    #  add.apply_async(args=[1, 3], countdown=10)
    #  add.apply_async(args=[2, 4], countdown=10)
    #  add.apply_async(args=[2, 5], countdown=10)
    #  app.send_task("buggy_task")
    #  app.send_task("long_running_task")

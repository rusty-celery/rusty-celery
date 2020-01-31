import os

from celery import Celery


my_app = Celery(
    "celery", broker=os.environ.get("AMQP_ADDR", "amqp://127.0.0.1:5672/my_vhost")
)
my_app.conf.update(result_backend=None, task_ignore_result=True)


@my_app.task(name="add")
def add(x, y):
    return x + y


if __name__ == "__main__":
    print(f"Sending task add[{add.apply_async(args=[1, 0], countdown=5)}]")
    #  print(f"Sending task buggy_task[{my_app.send_task('buggy_task')}]")
    print(f"Sending task long_running_task[{my_app.send_task('long_running_task')}]")
    print(
        f"Sending task long_running_task[{my_app.send_task('long_running_task', args=[3])}]"
    )

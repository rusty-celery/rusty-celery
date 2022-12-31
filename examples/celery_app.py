#!/usr/bin/env python3

import argparse
import os
import sys
import time

from celery import Celery
from celery.bin.celery import main as _main


my_app = Celery("celery", broker=os.environ.get("AMQP_ADDR", "amqp://127.0.0.1:5672"))
my_app.conf.update(
    result_backend=None,
    task_ignore_result=True,
    task_routes=(
        [("buggy_task", {"queue": "buggy-queue"})],
        [("*", {"queue": "celery"})],
    ),
)


# NOTE: we have to set the name for tasks manually in order to match the names
# of the Rust tasks. Otherwise the task names here would be prefixed with 'celery.'.
@my_app.task(name="add")
def add(x, y):
    return x + y


@my_app.task(
    name="buggy_task",
    max_retries=3,
    autoretry_for=(RuntimeError,),
    retry_backoff=True,
)
def buggy_task():
    raise RuntimeError("This error is part of the example: it is used to showcase error handling")


@my_app.task(name="long_running_task", max_retries=2)
def long_running_task(secs: int = 10):
    time.sleep(secs)


@my_app.task(name="bound_task", bind=True)
def bound_task(task):
    # Print some info about the request for debugging.
    print(task.request.origin)
    print(task.request.hostname)


def parse_args():
    parser = argparse.ArgumentParser(
        "celery_app", description="Run a Python Celery producer or consumer"
    )
    parser.add_argument("mode", choices=["consume", "produce"])
    parser.add_argument(
        "task", nargs="*", choices=["add", "buggy_task", "long_running_task", "bound_task"]
    )
    return parser.parse_args()


def main():
    opts = parse_args()
    if opts.mode == "consume":
        sys.argv = [
            "celery",
            "--app=celery_app.my_app",
            "worker",
            "-Q=celery,buggy-queue",
            "-Ofair",
            "--loglevel=info",
        ]
        _main()
    else:
        if opts.task:
            for task in opts.task:
                if task == "add":
                    add.apply_async(args=(1, 0))
                elif task == "buggy_task":
                    buggy_task.apply_async()
                elif task == "long_running_task":
                    long_running_task.apply_async()
                else:
                    buggy_task.apply_async()
        else:
            # Basic task sending.
            add.apply_async(args=(1, 0))
            bound_task.apply_async()

            # Send with additional options like `countdown`.
            add.apply_async(args=(1, 3), countdown=3)

            # Send the buggy task that will fail and be retried a few times.
            buggy_task.apply_async()

            # Send the long running task that will fail with a timeout error.
            long_running_task.apply_async(args=(3,), time_limit=2)


if __name__ == "__main__":
    main()

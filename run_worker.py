"""This file is used to run a worker for the celery app."""
import socket

from celery_app import app
from cron_tasks import apply_migrations

if __name__ == "__main__":
    print("starting worker")
    worker_key = "default"
    worker = app.Worker(
        hostname=f"{worker_key}@{socket.gethostname()}",
        queues=[],
        optimization="default",
        detach=True,
        loglevel="DEBUG",
        concurrency=2,
        max_tasks_per_child=10,
    )
    apply_migrations()
    worker.start()

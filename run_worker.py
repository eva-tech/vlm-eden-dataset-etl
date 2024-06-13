"""This file is used to run a worker for the celery app."""
import os
import socket

import sentry_sdk
from dotenv import load_dotenv

from celery_app import app
from cron_tasks import apply_migrations

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.info("starting worker")
    load_dotenv()
    sentry_sdk.init(
        dsn=os.getenv("SENTRY_DNS"),
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
    )
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

"""Celey configuration file."""

import os

from celery.schedules import crontab

accept_content = ["json"]
broker_url = os.getenv("REDIS_URL")

task_serializer = "json"
task_acks_late = True

result_serializer = "json"
result_backend = os.getenv("REDIS_URL")

worker_enable_remote_control = True
worker_send_task_events = True
worker_prefetch_multiplier = 1  # set this value to 1 to configure priority queue's

timezone = "America/Mexico_City"
enable_utc = True

beat_schedule = {
    "discover_chest_dicom_studies": {
        "task": "tasks.discover_chest_dicom_studies",
        "schedule": crontab(minute="0", hour="2"),  # every day at 2am
    },
}

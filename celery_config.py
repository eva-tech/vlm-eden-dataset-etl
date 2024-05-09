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
    "elt_task": {"task": "cron_tasks.run_etl", "schedule": 600},  # every 10 minutes
    "elt_validation_task": {
        "task": "cron_tasks.fetch_no_synced_data",
        "schedule": crontab(minute="0", hour="7"),  # every day at 1am
    },
    "sync_organizations_task": {
        "task": "cron_tasks.sync_organizations_task",
        "schedule": crontab(minute="0", hour="6"),  # every day at 12am
    },
}

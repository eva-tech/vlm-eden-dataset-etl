"""
This module configures the Celery application to handle asynchronous tasks.

It utilizes the Celery library and loads environment variables from a .env file.
"""

from celery import Celery
from dotenv import load_dotenv

load_dotenv()
app = Celery("tasks")

default_config = "celery_config"

app.config_from_object(default_config)

app.conf.update({"CELERY_IMPORTS": ["tasks", "cron_tasks"]})

app.conf.task_track_started = True
app.conf.task_send_sent_event = True
app.conf.worker_send_task_events = True

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

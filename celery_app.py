from celery import Celery

app = Celery('tasks')

default_config = "celery_config"

app.config_from_object(default_config)

app.conf.update(
    {
        "CELERY_IMPORTS": [
            "tasks",
        ]
    }
)

app.conf.task_track_started = True
app.conf.task_send_sent_event = True
app.conf.worker_send_task_events = True

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()



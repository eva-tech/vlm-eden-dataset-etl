"""
This module provides the Celery app for cron/scheduled tasks.

It imports the Celery app from celery_app to make it available
when using 'celery -A cron_tasks' commands.
"""

from celery_app import app

# Export the app so it can be used with 'celery -A cron_tasks'
__all__ = ["app"]


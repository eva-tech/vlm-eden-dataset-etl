#!/bin/sh

celery -A cron_tasks flower --conf=./flower_config.py & celery -A cron_tasks beat -l INFO -s /tmp/celerybeat-schedule
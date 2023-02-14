#!/bin/sh

celery -A cron_tasks flower & celery -A cron_tasks beat -l INFO

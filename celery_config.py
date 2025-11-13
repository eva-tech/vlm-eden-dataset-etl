"""Celery configuration file."""

import os

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

# No scheduled tasks - ETL pipeline is run manually via extract_and_upload_dicom_reports.py
beat_schedule = {}

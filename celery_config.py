"""Celey configuration file."""

accept_content = ["json"]
broker_url = 'redis://redis_intelligence:6379'

task_serializer = "json"
task_acks_late = True

result_serializer = "json"
result_backend = 'redis://redis_intelligence:6379'

worker_enable_remote_control = True
worker_send_task_events = True
worker_prefetch_multiplier = 1  # set this value to 1 to configure priority queue's

timezone = "America/Mexico_City"
enable_utc = True

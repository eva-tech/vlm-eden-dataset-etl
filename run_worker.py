import socket

from celery_app import app

if __name__ == "__main__":
    print("starting worker")
    worker_key = "default"
    worker = app.Worker(
        hostname=f"{worker_key}@{socket.gethostname()}",
        queues=[],
        optimization="fair",
        detach=True,
        loglevel="DEBUG",
        concurrency=2,
        max_tasks_per_child=10,
    )
    worker.start()

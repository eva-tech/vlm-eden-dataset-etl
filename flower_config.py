"""Flower configuration file."""
import os

BROKER_URL = os.getenv("BROKER_URL")
FLOWER_USER = os.getenv("FLOWER_USER")
FLOWER_PASSWORD = os.getenv("FLOWER_PASSWORD")
LOGGING_LEVEL = os.getenv("LOGGING_LEVEL")
SECRET_KEY = os.getenv("SECRET_KEY")

flower_columns = [
    "name",
    "uuid",
    "state",
    "args",
    "kwargs",
    "result",
    "received",
    "started",
    "runtime",
    "worker",
    "retries",
    "exception",
    "expires",
    "eta",
    "revoked",
]

broker = BROKER_URL
logging = LOGGING_LEVEL
persistent = True
basic_auth = [f"{FLOWER_USER}:{FLOWER_PASSWORD}"]
purge_offline_workers = 60
cookie_secret = SECRET_KEY
max_tasks = 100000
tasks_columns = ",".join(flower_columns)

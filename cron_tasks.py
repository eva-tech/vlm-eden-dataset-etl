"""
This module handles ETL (Extract, Transform, Load) tasks for fetching and synchronizing data.

It utilizes Celery for task scheduling and relies on other modules and tasks for specific functionalities.
"""
import logging
import os

from dotenv import load_dotenv
from psycopg2 import extras
from yoyo import get_backend, read_migrations

from celery_app import app
from database import (
    create_connection_to_destination,
    create_connection_to_source,
    run_general_migrations,
)
from queries.schema_organizations import create_schema, organizations_with_product
from tasks import (
    sync_data_from_by_organization,
    sync_organizations,
    sync_pending_data_by_organization,
)
from utils import get_schema_name

logger = logging.getLogger()


def run_migrations():
    """
    Run database migrations using the yoyo library.

    Reads migrations from the "./migrations/" directory and applies them to the destination database.
    """
    conn_source = create_connection_to_source()
    host_and_database = f"{os.getenv('DESTINATION_DATABASE_HOST')}/{os.getenv('DESTINATION_DATABASE_NAME')}"
    user_and_password = f"{os.getenv('DESTINATION_DATABASE_USER')}:{os.getenv('DESTINATION_DATABASE_PASS')}"
    destination_database = f"postgresql://{user_and_password}@{host_and_database}"
    migrations = read_migrations("./migrations/")

    cursor_source = conn_source.cursor(cursor_factory=extras.RealDictCursor)
    cursor_source.execute(organizations_with_product, ("intelligence",))

    conn_destination = create_connection_to_destination()
    cursor_destination = conn_destination.cursor(cursor_factory=extras.RealDictCursor)
    for organization in cursor_source.fetchall():
        schema_name = get_schema_name(organization["slug"])
        sql = create_schema.format(name=schema_name)
        cursor_destination.execute(sql)
        conn_destination.commit()

        backend = get_backend(f"{destination_database}?schema={schema_name}")
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))

    conn_source.close()
    conn_destination.close()


def organization_with_intelligence():
    """
    Retrieve organizations with a product of "intelligence" from the source database.

    Returns:
        list: List of organizations as dictionaries.
    """
    conn_source = create_connection_to_source()
    try:
        cursor_source = conn_source.cursor(cursor_factory=extras.RealDictCursor)
        cursor_source.execute(organizations_with_product, ("intelligence",))
        return cursor_source.fetchall()
    finally:
        conn_source.close()


def fetch_dim_data():
    """
    Fetch dimension data for organizations with the "intelligence" product.

    Schedules the "sync_data_from_by_organization" Celery task for each organization.
    """
    sync_organizations.delay()
    organizations = organization_with_intelligence()
    for organization in organizations:
        sync_data_from_by_organization.delay(organization["id"], organization["slug"])


@app.task
def fetch_no_synced_data():
    """
    Fetch data for organizations with the "intelligence" product that hasn't been synchronized yet.

    Schedules the "sync_pending_data_by_organization" Celery task for each organization.
    """
    organizations = organization_with_intelligence()
    for organization in organizations:
        sync_pending_data_by_organization.delay(
            organization["id"], organization["slug"]
        )


@app.task
def run_etl():
    """
    Run the ETL process.

    - Loads environment variables from the .env file.
    - Fetches dimension data.
    """
    load_dotenv()
    run_migrations()
    fetch_dim_data()


def apply_migrations():
    """
    Apply migrations to the destination database.

    - Loads environment variables from the .env file.
    - Runs database migrations.
    """
    logger.info("starting running migrations")
    load_dotenv()
    run_general_migrations()
    logger.info("finished running migrations")

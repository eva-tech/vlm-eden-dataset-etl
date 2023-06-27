import os

from dotenv import load_dotenv
from psycopg2 import extras
from yoyo import read_migrations, get_backend

from celery_app import app
from database import create_connection_to_source, create_connection_to_destination
from queries.schema_organizations import organizations_with_product, create_schema
from utils import get_schema_name
from tasks import sync_data_from_by_organization, sync_pending_data_by_organization


def run_migrations():
    conn_source = create_connection_to_source()
    host_and_database = f"{os.getenv('DESTINATION_DATABASE_HOST')}/{os.getenv('DESTINATION_DATABASE_NAME')}"
    user_and_password = f"{os.getenv('DESTINATION_DATABASE_USER')}:{os.getenv('DESTINATION_DATABASE_PASS')}"
    destination_database = f"postgresql://{user_and_password}@{host_and_database}"
    migrations = read_migrations('./migrations/')

    cursor_source = conn_source.cursor(cursor_factory=extras.RealDictCursor)
    cursor_source.execute(organizations_with_product, ("intelligence",))

    conn_destination = create_connection_to_destination()
    cursor_destination = conn_destination.cursor(cursor_factory=extras.RealDictCursor)
    for organization in cursor_source.fetchall():
        schema_name = get_schema_name(organization['slug'])
        sql = create_schema.format(name=schema_name)
        cursor_destination.execute(sql)
        conn_destination.commit()

        backend = get_backend(f"{destination_database}?schema={schema_name}")
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))

    conn_source.close()
    conn_destination.close()


def organization_with_intelligence():
    conn_source = create_connection_to_source()
    try:
        cursor_source = conn_source.cursor(cursor_factory=extras.RealDictCursor)
        cursor_source.execute(organizations_with_product, ("intelligence",))
        return cursor_source.fetchall()
    finally:
        conn_source.close()

def fetch_dim_data():
    organizations = organization_with_intelligence()
    for organization in organizations:
        sync_data_from_by_organization.delay(organization['id'], organization['slug'])

@app.task
def fetch_no_synced_data():
    organizations = organization_with_intelligence()
    for organization in organizations:
        sync_pending_data_by_organization.delay(organization['id'], organization['slug'])


@app.task
def run_etl():
    load_dotenv()
    run_migrations()
    print("ready migrations")
    fetch_dim_data()

"""This module contains functions to create connections to the source and destination databases."""

import os

import psycopg2
from dotenv import load_dotenv
from yoyo import get_backend, read_migrations


def create_connection_to_source():
    """Create a connection to the source database.

    :return: psycopg2 connection
    """
    load_dotenv()

    conn = psycopg2.connect(
        database=os.getenv("SOURCE_DATABASE_NAME"),
        user=os.getenv("SOURCE_DATABASE_USER"),
        password=os.getenv("SOURCE_DATABASE_PASS"),
        host=os.getenv("SOURCE_DATABASE_HOST"),
        port=os.getenv("SOURCE_DATABASE_PORT"),
    )
    return conn


def create_connection_to_destination():
    """Create a connection to the destination database.

    :return:    psycopg2 connection
    """
    load_dotenv()

    conn = psycopg2.connect(
        database=os.getenv("DESTINATION_DATABASE_NAME"),
        user=os.getenv("DESTINATION_DATABASE_USER"),
        password=os.getenv("DESTINATION_DATABASE_PASS"),
        host=os.getenv("DESTINATION_DATABASE_HOST"),
        port=os.getenv("DESTINATION_DATABASE_PORT"),
    )
    return conn


def run_general_migrations():
    """
    Run database migrations using the yoyo library.

    Reads migrations from the "./general_migrations/" directory and applies them to the destination database.
    """
    host_and_database = f"{os.getenv('DESTINATION_DATABASE_HOST')}/{os.getenv('DESTINATION_DATABASE_NAME')}"
    user_and_password = f"{os.getenv('DESTINATION_DATABASE_USER')}:{os.getenv('DESTINATION_DATABASE_PASS')}"
    destination_database = f"postgresql://{user_and_password}@{host_and_database}"
    migrations = read_migrations("./general_migrations/")

    conn_destination = create_connection_to_destination()
    backend = get_backend(destination_database)
    backend.apply_migrations(backend.to_apply(migrations))

    conn_destination.close()

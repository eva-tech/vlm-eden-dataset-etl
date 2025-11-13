"""This module contains functions to create connections to the source and destination databases."""

import os

import psycopg2
from dotenv import load_dotenv


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

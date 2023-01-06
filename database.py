import os

import psycopg2
from dotenv import load_dotenv


def create_connection_to_source():
    load_dotenv()

    conn = psycopg2.connect(database=os.getenv('SOURCE_DATABASE_NAME'),
                            user=os.getenv('SOURCE_DATABASE_USER'),
                            password=os.getenv('SOURCE_DATABASE_PASS'),
                            host=os.getenv('SOURCE_DATABASE_HOST'),
                            port=os.getenv('SOURCE_DATABASE_PORT'))
    return conn


def create_connection_to_destination():
    load_dotenv()

    conn = psycopg2.connect(database=os.getenv('DESTINATION_DATABASE_NAME'),
                            user=os.getenv('DESTINATION_DATABASE_USER'),
                            password=os.getenv('DESTINATION_DATABASE_PASS'),
                            host=os.getenv('DESTINATION_DATABASE_HOST'),
                            port=os.getenv('DESTINATION_DATABASE_PORT'))
    return conn

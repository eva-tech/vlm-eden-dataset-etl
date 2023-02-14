import os
from datetime import datetime

from dotenv import load_dotenv
from psycopg2 import extras
from yoyo import read_migrations, get_backend

from database import create_connection_to_source, create_connection_to_destination
from queries.schema_organizations import organizations_with_product, create_schema
from utils import get_schema_name
from tasks import sync_data_from_by_organization




if __name__ == "__main__":
    start_time = datetime.now()
    load_dotenv()
    run_migrations()
    print("ready migrations")
    fetch_dim_data()
    print("--- TOTAL %s seconds ---" % (datetime.now() - start_time))

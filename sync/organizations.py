"""Sync organizations from source to destination."""

from psycopg2 import extras, sql

from queries.schema_organizations import (
    all_organizations,
    insert_organizations,
    insert_organizations_template,
)
from sync.database_breach import DatabaseBridge


class SyncOrganizations:
    """Syncs organizations from source to destination."""

    TABLE_NAME = "organizations"

    def __init__(self, bridge: DatabaseBridge):
        """Initialize SyncBase class."""
        self.bridge = bridge
        self.destination_conn = self.bridge.destination_conn
        self.source_cursor = self.bridge.new_cursor(self.bridge.source_conn)
        self.destination_cursor = self.bridge.new_cursor(self.bridge.destination_conn)

    def retrieve_data(self):
        """Retrieve data from source and insert into destination."""
        self.source_cursor.execute(all_organizations)
        data = self.source_cursor.fetchall()
        sql_query = sql.SQL(insert_organizations)
        extras.execute_values(
            self.destination_cursor,
            sql_query,
            data,
            template=insert_organizations_template,
            page_size=1000,
        )
        self.destination_conn.commit()

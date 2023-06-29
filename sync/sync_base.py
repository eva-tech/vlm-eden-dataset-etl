"""Base class for syncing data from source to destination database."""
from dataclasses import dataclass
from datetime import datetime

from psycopg2 import sql

from queries.sync_records import get_last_sync_date, insert_last_sync_data
from sync.database_breach import DatabaseBridge


@dataclass
class OrganizationData:
    """Dataclass for organization data."""

    organization_id: str
    schema_name: str


class SyncBase:
    """Base class for syncing data from source to destination database."""

    def __init__(self, organization_data: OrganizationData, bridge: DatabaseBridge):
        """Initialize SyncBase class."""
        self.organization_id = organization_data.organization_id
        self.schema_name = organization_data.schema_name
        self.bridge = bridge
        self.destination_conn = self.bridge.destination_conn
        self.source_cursor = self.bridge.new_cursor(self.bridge.source_conn)
        self.destination_cursor = self.bridge.new_cursor(self.bridge.destination_conn)

    def record_sync(self, table: str, date: datetime, records_synced: int) -> None:
        """Record sync date and number of records synced."""
        sql_query = sql.SQL(insert_last_sync_data).format(
            schema=sql.Identifier(self.schema_name)
        )
        self.destination_cursor.execute(
            sql_query,
            {
                "table_name": table,
                "last_sync_date": date,
                "records_synced": records_synced,
            },
        )
        self.destination_conn.commit()

    def get_last_sync_date(self, table_name: str) -> datetime:
        """Get last sync date from destination database."""
        sql_query = sql.SQL(get_last_sync_date).format(
            schema=sql.Identifier(self.schema_name)
        )
        self.destination_cursor.execute(sql_query, {"table_name": table_name})
        result = self.destination_cursor.fetchone()
        return result["max"] or datetime(2019, 1, 1)

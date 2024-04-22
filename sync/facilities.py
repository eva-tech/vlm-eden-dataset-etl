"""Syncs facilities from source to destination."""
from datetime import datetime

from psycopg2 import extras, sql

from queries.dim_facitities import (
    get_all_facilities,
    insert_facilities,
    insert_facilities_template,
)
from sync.constants import BATCH_1000
from sync.sync_base import SyncBase


class SyncFacilities(SyncBase):
    """Syncs facilities from source to destination."""

    TABLE_NAME = "dim_facilities"

    def retrieve_data(self):
        """Retrieve data from source and insert into destination."""
        query_date = datetime.now()
        last_sync = self.get_last_sync_date(self.TABLE_NAME)

        self.source_cursor.execute(
            get_all_facilities,
            {"organization_id": self.organization_id, "date": last_sync},
        )
        data = self.source_cursor.fetchall()
        sql_query = sql.SQL(insert_facilities).format(sql.Identifier(self.schema_name))
        extras.execute_values(
            self.destination_cursor,
            sql_query,
            data,
            template=insert_facilities_template,
            page_size=BATCH_1000,
        )

        self.destination_conn.commit()
        self.record_sync(self.TABLE_NAME, query_date, len(data))

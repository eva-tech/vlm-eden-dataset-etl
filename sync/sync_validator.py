"""Sync Validator module."""
from datetime import datetime, timedelta

from psycopg2 import extras, sql

from queries.fact_studies import get_studies_by_date, get_studies_by_not_ids
from sync.studies import SyncStudies
from sync.sync_base import SyncBase


class SyncValidator(SyncBase):
    """Sync missing studies for the principal sync."""

    TABLE_NAME = "studies_laggard"

    def __init__(self, organization_data, bridge):
        """Initialize SyncValidator."""
        super().__init__(organization_data, bridge)
        self.sync_studies = SyncStudies(
            organization_data=organization_data, bridge=bridge
        )

    def retrieve_data(self):
        """Retrieve data from source and insert into destination."""
        today = datetime.now()
        two_days = timedelta(days=2)
        start_date = today - two_days
        end_date = today

        cursor_destination = self.destination_conn.cursor(
            cursor_factory=extras.RealDictCursor
        )
        sql_query = sql.SQL(get_studies_by_date).format(
            schema=sql.Identifier(self.schema_name)
        )
        cursor_destination.execute(
            sql_query, {"start_date": start_date, "end_date": end_date}
        )
        data_studies_ids = cursor_destination.fetchall()
        ids = [data["external_id"] for data in data_studies_ids]

        self.source_cursor.execute(
            get_studies_by_not_ids,
            {
                "ids": tuple(ids) or None,
                "organization_id": self.organization_id,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        data_studies = self.source_cursor.fetchall()
        pending_ids = [data["id"] for data in data_studies]
        self.sync_studies.sync_studies_by_ids(pending_ids, start_date)

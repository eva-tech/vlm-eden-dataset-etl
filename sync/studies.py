"""Sync studies from source to destination."""

from datetime import datetime
from typing import List

from psycopg2 import extras, sql

from queries.fact_studies import get_studies, insert_studies, insert_studies_template
from sync.constants import BATCH_200
from sync.sync_base import SyncBase


class SyncStudies(SyncBase):
    """Sync studies from source to destination."""

    TABLE_NAME = "fact_studies"

    def retrieve_data(self):
        """Retrieve data from source and insert into destination."""
        query_date = datetime.now()

        last_sync = self.get_last_sync_date(self.TABLE_NAME)
        sql_query = sql.SQL(get_studies).format(extra_filter=sql.SQL(""))
        self.source_cursor.execute(
            sql_query, {"organization_id": self.organization_id, "date": last_sync}
        )

        data_studies = self.source_cursor.fetchall()
        sql_query = sql.SQL(insert_studies).format(
            schema=sql.Identifier(self.schema_name)
        )
        template = sql.SQL(insert_studies_template).format(
            schema=sql.Identifier(self.schema_name)
        )
        extras.execute_values(
            self.destination_cursor,
            sql_query,
            data_studies,
            template=template,
            page_size=BATCH_200,
        )

        self.destination_conn.commit()

        self.record_sync(self.TABLE_NAME, query_date, len(data_studies))

    def sync_studies_by_ids(self, studies_ids: List[str], date: datetime):
        """Sync studies from source to destination taking the ids as filter."""
        if not studies_ids:
            return
        sql_query = sql.SQL(get_studies).format(
            extra_filter=sql.SQL("and ps.id in %(ids)s")
        )
        self.source_cursor.execute(
            sql_query,
            {
                "organization_id": self.organization_id,
                "date": date,
                "ids": tuple(studies_ids),
            },
        )
        data_studies = self.source_cursor.fetchall()

        sql_query = sql.SQL(insert_studies).format(
            schema=sql.Identifier(self.schema_name)
        )
        template = sql.SQL(insert_studies_template).format(
            schema=sql.Identifier(self.schema_name)
        )
        extras.execute_values(
            self.destination_cursor,
            sql_query,
            data_studies,
            template=template,
            page_size=BATCH_200,
        )

        self.destination_conn.commit()

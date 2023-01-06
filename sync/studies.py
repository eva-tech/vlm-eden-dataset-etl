from datetime import datetime

from psycopg2 import sql, extras

from queries.fact_studies import get_studies, insert_studies, insert_studies_template
from sync.sync_base import SyncBase


class SyncStudies(SyncBase):

    TABLE_NAME = "fact_studies"

    def retrieve_data(self):
        query_date = datetime.now()

        last_sync = self.get_last_sync_date(self.TABLE_NAME)
        self.source_cursor.execute(get_studies, {
            "organization_id": self.organization_id,
            "date": last_sync
        })

        data_studies = self.source_cursor.fetchall()
        sql_query = sql.SQL(insert_studies).format(schema=sql.Identifier(self.schema_name))
        template = sql.SQL(insert_studies_template).format(schema=sql.Identifier(self.schema_name))
        extras.execute_values(
            self.destination_cursor, sql_query, data_studies, template=template, page_size=200
        )
        self.destination_conn.commit()

        self.record_sync(self.TABLE_NAME, query_date, len(data_studies))

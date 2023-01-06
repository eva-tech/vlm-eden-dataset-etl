from datetime import datetime

from psycopg2 import sql, extras

from queries.dim_practitioners import get_all_practitioners, insert_practitioners, insert_practitioners_template
from sync.sync_base import SyncBase


class SyncPractitioners(SyncBase):
    TABLE_NAME = "dim_practitioners"

    def retrieve_data(self):
        query_date = datetime.now()
        last_sync = self.get_last_sync_date(self.TABLE_NAME)

        self.source_cursor.execute(get_all_practitioners, {"organization_id": self.organization_id, "date": last_sync})
        data_practitioners = self.source_cursor.fetchall()
        sql_query = sql.SQL(insert_practitioners).format(sql.Identifier(self.schema_name))
        extras.execute_values(
            self.destination_cursor, sql_query, data_practitioners, template=insert_practitioners_template,
            page_size=100
        )
        self.destination_conn.commit()

        self.record_sync(self.TABLE_NAME, query_date, len(data_practitioners))

from datetime import datetime

from psycopg2 import sql, extras

from queries.dim_technicians import get_all_technicians, insert_technicians, insert_technicians_template
from sync.sync_base import SyncBase


class SyncTechnicians(SyncBase):
    TABLE_NAME = "dim_technicians"

    def retrieve_data(self):
        query_date = datetime.now()
        last_sync = self.get_last_sync_date(self.TABLE_NAME)

        self.source_cursor.execute(get_all_technicians, {"organization_id": self.organization_id, "date": last_sync})
        data = self.source_cursor.fetchall()
        sql_query = sql.SQL(insert_technicians).format(schema=sql.Identifier(self.schema_name))
        template = sql.SQL(insert_technicians_template).format(schema=sql.Identifier(self.schema_name))
        extras.execute_values(
            self.destination_cursor, sql_query, data, template=template, page_size=100
        )

        self.destination_conn.commit()
        self.record_sync(self.TABLE_NAME, query_date, len(data))

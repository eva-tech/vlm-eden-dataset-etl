import uuid
from datetime import datetime
from typing import List

from psycopg2 import sql, extras

from queries.dim_modalities import get_modalities_list, insert_modalities, get_modalities_from_studies, \
    insert_modalities_template
from sync.sync_base import SyncBase


class SyncModalities(SyncBase):
    TABLE_NAME = "dim_modalities"

    def get_multiple_modalities(self, modalities_from_studies) -> List:
        multiple_modalities = []
        for modalities in modalities_from_studies:
            arr_modalities = modalities['modalities'].split(",")
            if len(arr_modalities) > 1:
                multiple_modalities.append(arr_modalities)
        return multiple_modalities

    def sync_modalities_from_studies(self, data_modalities, last_sync, date):

        self.source_cursor.execute(get_modalities_from_studies,
                                   {"organization_id": self.organization_id, "date": last_sync})
        modalities_from_studies = self.source_cursor.fetchall()
        empty_modality = {
            "id": "",
            "name": "",
            "name_es": "",
            "identifier": "",
            "description": "",
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        tmp_modality = empty_modality.copy()
        to_insert = []
        for multi_mod in self.get_multiple_modalities(modalities_from_studies):
            for data in data_modalities:
                if data["identifier"] in multi_mod:
                    tmp_modality["id"] = str(uuid.uuid4())
                    tmp_modality["name"] = ",".join(sorted([x for x in [data["name"] or data["identifier"], tmp_modality["name"] or tmp_modality["identifier"]] if x]))
                    tmp_modality["identifier"] = ",".join(
                        sorted([x for x in [data["identifier"], tmp_modality["identifier"]] if x]))
                    tmp_modality["description"] = ",".join(
                        sorted([x for x in [data["description"] or "", tmp_modality["description"]] if x]))

            if tmp_modality != empty_modality:
                to_insert.append(tmp_modality)
                tmp_modality = empty_modality.copy()

        if len(to_insert) >= 1:
            sql_query = sql.SQL(insert_modalities).format(schema=sql.Identifier(self.schema_name))
            extras.execute_values(
                self.destination_cursor, sql_query, to_insert, template=insert_modalities_template, page_size=100
            )

        self.destination_conn.commit()

        total_records = len(to_insert) + len(data_modalities)
        self.record_sync(self.TABLE_NAME, date, total_records)

    def sync_original_modalities(self, data_modalities):
        for modality in data_modalities:
            modality["name"] = modality["name"] or modality["identifier"]
        sql_query = sql.SQL(insert_modalities).format(schema=sql.Identifier(self.schema_name))
        extras.execute_values(
            self.destination_cursor, sql_query, data_modalities, template=insert_modalities_template, page_size=100
        )
        self.destination_conn.commit()

    def retrieve_data(self):
        date = datetime.now()
        last_sync = self.get_last_sync_date(self.TABLE_NAME)

        self.source_cursor.execute(get_modalities_list)
        data_modalities = self.source_cursor.fetchall()
        self.sync_original_modalities(data_modalities)
        self.sync_modalities_from_studies(data_modalities, last_sync, date)

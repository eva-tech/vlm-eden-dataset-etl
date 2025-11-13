"""Sync modalities from source to destination database."""

import uuid
from datetime import datetime, timedelta
from typing import List

from psycopg2 import extras, sql

from queries.dim_modalities import (
    fix_names_template,
    get_dim_modalities,
    get_modalities_from_studies,
    get_modalities_list,
    insert_modalities,
    insert_modalities_template,
)
from sync.constants import BATCH_100
from sync.studies import SyncStudies
from sync.sync_base import SyncBase
from utils import combine_and_sort_dictionary_values


class SyncModalities(SyncBase):
    """Sync modalities from source to destination database."""

    TABLE_NAME = "dim_modalities"

    def get_multiple_modalities(self, modalities_from_studies) -> List:
        """Get multiple modalities from studies."""
        multiple_modalities = []
        for modalities in modalities_from_studies:
            arr_modalities = modalities["modalities"].split(",")
            if len(arr_modalities) > 1:
                multiple_modalities.append(arr_modalities)
        return multiple_modalities

    def sync_modalities_from_studies(self, data_modalities, last_sync, date):
        """Sync modalities from studies."""
        self.source_cursor.execute(
            get_modalities_from_studies,
            {"organization_id": self.organization_id, "date": last_sync},
        )
        modalities_from_studies = self.source_cursor.fetchall()
        empty_modality = {
            "id": "",
            "name": "",
            "name_es": "",
            "name_pt": "",
            "identifier": "",
            "description": "",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        tmp_modality = empty_modality.copy()
        to_insert = []
        for multi_mod in self.get_multiple_modalities(modalities_from_studies):
            for data in data_modalities:
                if data["identifier"] in multi_mod:
                    tmp_modality["id"] = str(uuid.uuid4())
                    tmp_modality["name_es"] = combine_and_sort_dictionary_values(
                        data, tmp_modality, ["name_es", "name"]
                    )
                    tmp_modality["name_pt"] = combine_and_sort_dictionary_values(
                        data, tmp_modality, ["name_pt", "name"]
                    )
                    tmp_modality["name"] = combine_and_sort_dictionary_values(
                        data, tmp_modality, ["name", "identifier"]
                    )
                    tmp_modality["identifier"] = ",".join(
                        sorted(
                            [
                                item
                                for item in [
                                    data["identifier"],
                                    tmp_modality["identifier"],
                                ]
                                if item
                            ]
                        )
                    )
                    tmp_modality["description"] = ",".join(
                        sorted(
                            [
                                item
                                for item in [
                                    data["description"] or "",
                                    tmp_modality["description"],
                                ]
                                if item
                            ]
                        )
                    )

            if tmp_modality["identifier"] in [mod["identifier"] for mod in to_insert]:
                """If the modality is already in the list, skip it"""
                tmp_modality = empty_modality.copy()
                continue

            if tmp_modality != empty_modality:
                to_insert.append(tmp_modality)
                tmp_modality = empty_modality.copy()

        if len(to_insert) >= 1:
            sql_query = sql.SQL(insert_modalities).format(
                schema=sql.Identifier(self.schema_name)
            )
            extras.execute_values(
                self.destination_cursor,
                sql_query,
                to_insert,
                template=insert_modalities_template,
                page_size=BATCH_100,
            )

        self.destination_conn.commit()

        total_records = len(to_insert) + len(data_modalities)
        self.record_sync(self.TABLE_NAME, date, total_records)

    def sync_original_modalities(self, data_modalities):
        """Sync original modalities."""
        for modality in data_modalities:
            modality["name_es"] = modality["name_es"] or modality["name"]
            modality["name_pt"] = modality["name_pt"] or modality["name"]
            modality["name"] = modality["name"] or modality["identifier"]
        sql_query = sql.SQL(insert_modalities).format(
            schema=sql.Identifier(self.schema_name)
        )
        extras.execute_values(
            self.destination_cursor,
            sql_query,
            data_modalities,
            template=insert_modalities_template,
            page_size=BATCH_100,
        )
        self.destination_conn.commit()

    def sync_names(self, current_modalities, data_modalities):
        """Sync names so that if a name, name_es, or name_pt value is updated in the original modalities.

        the name and name_es columns for modalities
        that contain that original modality are also updated.
        """
        empty_modality = {
            "id": "",
            "name": "",
            "name_es": "",
            "name_pt": "",
            "identifier": "",
            "description": "",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        tmp_modality = empty_modality.copy()
        for modality in current_modalities:
            if "," in modality["identifier"]:
                arr_modalities = modality["identifier"].split(",")
                for data in data_modalities:
                    if data["identifier"] in arr_modalities:
                        tmp_modality["name_es"] = combine_and_sort_dictionary_values(
                            data, tmp_modality, ["name_es", "name"]
                        )
                        tmp_modality["name_pt"] = combine_and_sort_dictionary_values(
                            data, tmp_modality, ["name_pt", "name"]
                        )
                        tmp_modality["name"] = combine_and_sort_dictionary_values(
                            data, tmp_modality, ["name", "identifier"]
                        )
                modality["name_es"] = tmp_modality["name_es"]
                modality["name_pt"] = tmp_modality["name_pt"]
                modality["name"] = tmp_modality["name"]
                tmp_modality = empty_modality.copy()
            else:
                modality["name_es"] = (
                    modality["name_es"] or modality["name"] or modality["identifier"]
                )
                modality["name_pt"] = (
                    modality["name_pt"] or modality["name"] or modality["identifier"]
                )
                modality["name"] = modality["name"] or modality["identifier"]
        sql_query = sql.SQL(insert_modalities).format(
            schema=sql.Identifier(self.schema_name)
        )
        extras.execute_values(
            self.destination_cursor,
            sql_query,
            current_modalities,
            template=fix_names_template,
            page_size=BATCH_100,
        )
        self.destination_conn.commit()

    def retrieve_data(self):
        """Retrieve data from source and sync it to destination."""
        date = datetime.now()
        last_sync = self.get_last_sync_date(SyncStudies.TABLE_NAME)
        last_sync_one_hour_ago = last_sync - timedelta(hours=1)

        self.source_cursor.execute(get_modalities_list)
        data_modalities = self.source_cursor.fetchall()
        self.sync_original_modalities(data_modalities)
        self.sync_modalities_from_studies(data_modalities, last_sync_one_hour_ago, date)
        self.destination_cursor.execute(
            sql.SQL(get_dim_modalities).format(schema=sql.Identifier(self.schema_name))
        )
        current_modalities = self.destination_cursor.fetchall()
        self.sync_names(current_modalities, data_modalities)

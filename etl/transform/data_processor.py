"""Data processing module for transforming query results into structured data."""

import logging
import os
from typing import Dict, List, Set

logger = logging.getLogger(__name__)


class DataProcessor:
    """Processes query results and extracts structured data."""

    def process_batch(self, batch: List[Dict], processed_file_keys: Set[str]) -> tuple:
        """Process a batch of query results and extract file and report data.

        :param batch: List of query result dictionaries
        :param processed_file_keys: Set of already processed file keys (for idempotency)
        :return: Tuple of (file_data_dict, report_fields_by_study_dict, processed_keys)
        """
        logger.info(f"Processing batch of {len(batch)} rows...")

        # Group by file and aggregate report fields at study level
        file_data = {}
        report_fields_by_study = {}
        processed_file_keys_in_batch = []

        for row in batch:
            study_id = row.get('study_id')
            series_id = row.get('series_id')
            instance_id = row.get('instance_id')
            file_path = row.get('file_path')
            file_url = row.get('file_url')
            field_value = row.get('field_value')
            field_created_at = row.get('field_created_at')

            # Create composite key for the file
            file_key = (study_id, series_id, instance_id, file_path)
            file_key_str = str(file_key)

            # Skip if already processed (idempotency)
            if file_key_str in processed_file_keys:
                continue

            # Store file data
            if file_key not in file_data:
                file_data[file_key] = {
                    'study_id': study_id,
                    'series_id': series_id,
                    'instance_id': instance_id,
                    'series_number': row.get('series_number'),
                    'instance_number': row.get('instance_number'),
                    'file_path': file_path,
                    'file_url': file_url,
                    'field_created_at': field_created_at,
                }

            # Aggregate report fields at STUDY level
            if study_id not in report_fields_by_study:
                report_fields_by_study[study_id] = []

            if field_value:
                existing_values = [rv['value'] for rv in report_fields_by_study[study_id]]
                if field_value not in existing_values:
                    report_fields_by_study[study_id].append({
                        'value': field_value,
                        'created_at': field_created_at
                    })
                    if field_created_at and (not file_data[file_key]['field_created_at'] or
                                             field_created_at > file_data[file_key]['field_created_at']):
                        file_data[file_key]['field_created_at'] = field_created_at

            processed_file_keys_in_batch.append(file_key_str)

        logger.info(f"Processed batch: {len(file_data)} files, {len(report_fields_by_study)} studies")
        return file_data, report_fields_by_study, processed_file_keys_in_batch

    def prepare_csv_rows(
        self,
        file_data: Dict,
        report_fields_by_study: Dict,
        processed_file_keys: Set[str],
        output_dir: str
    ) -> List[Dict]:
        """Prepare CSV rows from processed file data.

        :param file_data: Dictionary of file data keyed by file_key
        :param report_fields_by_study: Dictionary of report fields by study_id
        :param processed_file_keys: Set of already processed file keys
        :param output_dir: Output directory for local file paths
        :return: List of CSV row dictionaries
        """
        csv_rows = []

        for file_key, file_info in file_data.items():
            # Skip if already processed (idempotency)
            if str(file_key) in processed_file_keys:
                continue

            csv_row = {
                'study_id': file_info['study_id'],
                'series_number': file_info['series_number'],
                'instance_number': file_info['instance_number'],
                'instance_id': file_info['instance_id'],
                'file_path': file_info['file_path'],
                'field_created_at': file_info['field_created_at'],
                'file_url': file_info['file_url'],
                'report_value': None,
            }

            # Get report field values for this study
            study_id = file_info['study_id']
            report_values = report_fields_by_study.get(study_id, [])
            if report_values:
                csv_row['report_value'] = ' | '.join([rv['value'] for rv in report_values if rv['value']])

            # Prepare local file path if file_url exists
            file_url = file_info['file_url']
            if file_url:
                instance_id_str = str(file_info['instance_id'])
                file_name = f"{instance_id_str}.dcm"
                file_dir = os.path.join(output_dir, 'dicom_files')
                local_file_path = os.path.join(file_dir, file_name)
                csv_row['local_file_path'] = local_file_path
            else:
                csv_row['local_file_path'] = None

            csv_rows.append(csv_row)

        return csv_rows


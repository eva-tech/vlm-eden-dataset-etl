"""Unit tests for data processing logic."""

import pytest

from etl.transform.data_processor import DataProcessor

pytestmark = pytest.mark.unit


class TestDataProcessor:
    """Test cases for DataProcessor."""

    def test_process_batch_empty(self):
        """Test processing empty batch."""
        processor = DataProcessor()
        file_data, report_fields, processed_keys = processor.process_batch([], set())
        assert file_data == {}
        assert report_fields == {}
        assert processed_keys == []

    def test_process_batch_single_file(self):
        """Test processing batch with single file."""
        processor = DataProcessor()
        batch = [{
            'study_id': 1,
            'series_id': 1,
            'instance_id': 1,
            'file_path': '/path/to/file.dcm',
            'file_url': 'https://example.com/file.dcm',
            'field_value': 'Report text',
            'field_created_at': '2024-01-01',
            'series_number': 1,
            'instance_number': 1,
        }]
        file_data, report_fields, processed_keys = processor.process_batch(batch, set())
        
        assert len(file_data) == 1
        assert len(report_fields) == 1
        assert len(processed_keys) == 1
        assert 1 in report_fields
        assert report_fields[1][0]['value'] == 'Report text'

    def test_process_batch_idempotency(self):
        """Test that already processed files are skipped."""
        processor = DataProcessor()
        batch = [{
            'study_id': 1,
            'series_id': 1,
            'instance_id': 1,
            'file_path': '/path/to/file.dcm',
            'file_url': 'https://example.com/file.dcm',
            'field_value': 'Report text',
            'field_created_at': '2024-01-01',
            'series_number': 1,
            'instance_number': 1,
        }]
        
        # First processing
        file_data1, report_fields1, processed_keys1 = processor.process_batch(batch, set())
        file_key = str((1, 1, 1, '/path/to/file.dcm'))
        
        # Second processing with same file key
        file_data2, report_fields2, processed_keys2 = processor.process_batch(
            batch, {file_key}
        )
        
        # Should skip the file
        assert len(file_data2) == 0
        assert len(processed_keys2) == 0

    def test_process_batch_multiple_report_fields(self):
        """Test aggregating multiple report fields for same study."""
        processor = DataProcessor()
        batch = [
            {
                'study_id': 1,
                'series_id': 1,
                'instance_id': 1,
                'file_path': '/path/to/file1.dcm',
                'file_url': 'https://example.com/file1.dcm',
                'field_value': 'Report 1',
                'field_created_at': '2024-01-01',
                'series_number': 1,
                'instance_number': 1,
            },
            {
                'study_id': 1,
                'series_id': 1,
                'instance_id': 2,
                'file_path': '/path/to/file2.dcm',
                'file_url': 'https://example.com/file2.dcm',
                'field_value': 'Report 2',
                'field_created_at': '2024-01-02',
                'series_number': 1,
                'instance_number': 2,
            },
        ]
        file_data, report_fields, processed_keys = processor.process_batch(batch, set())
        
        # Both files should be processed
        assert len(file_data) == 2
        # Both report fields should be aggregated for study 1
        assert len(report_fields[1]) == 2
        assert 'Report 1' in [rf['value'] for rf in report_fields[1]]
        assert 'Report 2' in [rf['value'] for rf in report_fields[1]]

    def test_prepare_csv_rows(self):
        """Test preparing CSV rows from processed data."""
        processor = DataProcessor()
        file_data = {
            (1, 1, 1, '/path/to/file.dcm'): {
                'study_id': 1,
                'series_id': 1,
                'instance_id': 1,
                'series_number': 1,
                'instance_number': 1,
                'file_path': '/path/to/file.dcm',
                'file_url': 'https://example.com/file.dcm',
                'field_created_at': '2024-01-01',
            }
        }
        report_fields_by_study = {
            1: [{'value': 'Report text', 'created_at': '2024-01-01'}]
        }
        
        csv_rows = processor.prepare_csv_rows(
            file_data, report_fields_by_study, set(), '/tmp'
        )
        
        assert len(csv_rows) == 1
        assert csv_rows[0]['study_id'] == 1
        assert csv_rows[0]['instance_id'] == 1
        assert csv_rows[0]['report_value'] == 'Report text'
        assert csv_rows[0]['file_url'] == 'https://example.com/file.dcm'


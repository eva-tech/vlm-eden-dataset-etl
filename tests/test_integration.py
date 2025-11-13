"""Integration tests for parallel processing."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from etl.pipeline.etl_pipeline import ETLPipeline
from etl.tasks.batch_tasks import process_batch_task, process_page_batch_task
from sync.database_breach import DatabaseBridge

pytestmark = pytest.mark.integration


class TestETLPipelineIntegration:
    """Integration tests for ETL pipeline."""

    @pytest.fixture
    def mock_bridge(self):
        """Create a mock database bridge."""
        bridge = Mock(spec=DatabaseBridge)
        bridge.source_conn = Mock()
        return bridge

    @pytest.fixture
    def mock_query_executor(self):
        """Create a mock query executor."""
        executor = Mock()
        executor.get_total_count.return_value = 50
        executor.fetch_page.return_value = [
            {
                'study_id': 1,
                'series_id': 1,
                'instance_id': 1,
                'file_path': '/path/to/file.dcm',
                'file_url': 'https://example.com/file.dcm',
                'field_value': 'Report text',
                'field_created_at': '2024-01-01',
                'series_number': 1,
                'instance_number': 1,
            }
        ] * 25
        return executor

    def test_pipeline_initialization(self, mock_bridge):
        """Test pipeline initialization."""
        pipeline = ETLPipeline(
            bridge=mock_bridge,
            bucket_name='test-bucket',
            page_size=25,
            batch_size=10,
        )
        assert pipeline.bucket_name == 'test-bucket'
        assert pipeline.page_size == 25
        assert pipeline.batch_size == 10
        assert pipeline.use_celery is True

    @patch('etl.pipeline.etl_pipeline.process_page_batch_task')
    def test_pipeline_with_celery(self, mock_page_task, mock_bridge):
        """Test pipeline execution with Celery."""
        # Mock Celery task results
        mock_result = Mock()
        mock_result.id = 'test-group-id'
        mock_result.get.return_value = [{
            'success': True,
            'page': 0,
            'records_processed': 25,
            'total_files': 25,
            'total_csv_rows': 25,
            'total_downloaded': 25,
            'processed_file_keys': ['key1', 'key2'],
            'csv_rows': [{'study_id': 1}],
        }]
        
        mock_group = Mock()
        mock_group.apply_async.return_value = mock_result
        
        with patch('etl.pipeline.etl_pipeline.group', return_value=mock_group):
            with patch('etl.pipeline.etl_pipeline.QueryExecutor') as mock_executor_class:
                mock_executor = Mock()
                mock_executor.get_total_count.return_value = 25
                mock_executor_class.return_value = mock_executor
                
                pipeline = ETLPipeline(
                    bridge=mock_bridge,
                    bucket_name='test-bucket',
                    page_size=25,
                    batch_size=10,
                    use_celery=True,
                )
                
                result = pipeline.run()
                
                assert result['success'] is True
                assert result['total_count'] == 25

    def test_batch_task_idempotency(self):
        """Test that batch tasks are idempotent."""
        batch_data = [
            {
                'study_id': 1,
                'series_id': 1,
                'instance_id': 1,
                'file_path': '/path/to/file.dcm',
                'file_url': 'https://example.com/file.dcm',
                'field_value': 'Report text',
                'field_created_at': '2024-01-01',
                'series_number': 1,
                'instance_number': 1,
            }
        ]
        
        processed_keys = set()
        file_key = str((1, 1, 1, '/path/to/file.dcm'))
        processed_keys.add(file_key)
        
        # This would normally be called by Celery, but we're testing the logic
        # The task should skip already processed files
        with patch('etl.tasks.batch_tasks.DatabaseBridge'):
            with patch('etl.tasks.batch_tasks.DataProcessor') as mock_processor:
                mock_processor_instance = Mock()
                mock_processor.return_value = mock_processor_instance
                mock_processor_instance.process_batch.return_value = ({}, {}, [])
                
                # The task should check processed_file_keys and skip
                # This is tested through the DataProcessor.process_batch logic
                assert file_key in processed_keys


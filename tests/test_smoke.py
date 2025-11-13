"""Smoke tests for main CLI entrypoint."""

import pytest
import sys
from unittest.mock import Mock, patch, MagicMock

from extract_and_upload_dicom_reports import main

pytestmark = pytest.mark.smoke


class TestSmokeTests:
    """Smoke tests to verify end-to-end functionality."""

    @patch('extract_and_upload_dicom_reports.DatabaseBridge')
    @patch('extract_and_upload_dicom_reports.ETLPipeline')
    def test_main_success(self, mock_pipeline_class, mock_bridge_class):
        """Test main function with successful execution."""
        # Setup mocks
        mock_bridge = Mock()
        mock_bridge_class.return_value = mock_bridge
        
        mock_pipeline = Mock()
        mock_pipeline.run.return_value = {'success': True, 'total_count': 100}
        mock_pipeline_class.return_value = mock_pipeline
        
        # Test main function
        with patch('sys.exit') as mock_exit:
            main()
            mock_exit.assert_called_once_with(0)
            mock_pipeline.run.assert_called_once()

    @patch('extract_and_upload_dicom_reports.DatabaseBridge')
    @patch('extract_and_upload_dicom_reports.ETLPipeline')
    def test_main_failure(self, mock_pipeline_class, mock_bridge_class):
        """Test main function with failed execution."""
        # Setup mocks
        mock_bridge = Mock()
        mock_bridge_class.return_value = mock_bridge
        
        mock_pipeline = Mock()
        mock_pipeline.run.return_value = {'success': False, 'error': 'Test error'}
        mock_pipeline_class.return_value = mock_pipeline
        
        # Test main function
        with patch('sys.exit') as mock_exit:
            main()
            mock_exit.assert_called_once_with(1)
            mock_pipeline.run.assert_called_once()

    @patch('extract_and_upload_dicom_reports.DatabaseBridge')
    def test_main_exception_handling(self, mock_bridge_class):
        """Test main function exception handling."""
        # Setup mock to raise exception
        mock_bridge_class.side_effect = Exception('Database connection failed')
        
        # Test main function
        with patch('sys.exit') as mock_exit:
            main()
            mock_exit.assert_called_once_with(1)

    @patch('extract_and_upload_dicom_reports.DatabaseBridge')
    @patch('extract_and_upload_dicom_reports.ETLPipeline')
    def test_main_cleanup(self, mock_pipeline_class, mock_bridge_class):
        """Test that database connections are closed in finally block."""
        # Setup mocks
        mock_bridge = Mock()
        mock_bridge_class.return_value = mock_bridge
        
        mock_pipeline = Mock()
        mock_pipeline.run.return_value = {'success': True}
        mock_pipeline_class.return_value = mock_pipeline
        
        # Test main function
        with patch('sys.exit'):
            main()
            mock_bridge.close_connections.assert_called_once()

    @patch('extract_and_upload_dicom_reports.os.getenv')
    @patch('extract_and_upload_dicom_reports.DatabaseBridge')
    @patch('extract_and_upload_dicom_reports.ETLPipeline')
    def test_main_env_config(self, mock_pipeline_class, mock_bridge_class, mock_getenv):
        """Test that environment variables are used correctly."""
        # Setup mocks
        mock_getenv.return_value = 'test-bucket'
        mock_bridge = Mock()
        mock_bridge_class.return_value = mock_bridge
        
        mock_pipeline = Mock()
        mock_pipeline.run.return_value = {'success': True}
        mock_pipeline_class.return_value = mock_pipeline
        
        # Test main function
        with patch('sys.exit'):
            main()
            mock_getenv.assert_called_with('GCS_BUCKET_NAME', 'gcs-bucket-name')
            mock_pipeline_class.assert_called_once_with(
                bridge=mock_bridge,
                bucket_name='test-bucket',
            )


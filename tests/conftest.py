"""Pytest configuration and shared fixtures."""

import pytest
from unittest.mock import Mock

from sync.database_breach import DatabaseBridge


@pytest.fixture
def mock_database_bridge():
    """Create a mock database bridge for testing."""
    bridge = Mock(spec=DatabaseBridge)
    bridge.source_conn = Mock()
    bridge.destination_conn = None
    bridge.new_cursor = Mock(return_value=Mock())
    bridge.close_connections = Mock()
    return bridge


@pytest.fixture
def sample_query_result():
    """Sample query result for testing."""
    return {
        'study_id': 1,
        'series_id': 1,
        'instance_id': 1,
        'file_path': '/path/to/file.dcm',
        'file_url': 'https://example.com/file.dcm',
        'field_value': 'Sample report text',
        'field_created_at': '2024-01-01T00:00:00',
        'series_number': 1,
        'instance_number': 1,
        'study_description': 'Test Study',
        'series_description': 'Test Series',
    }


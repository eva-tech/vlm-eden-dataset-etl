"""Query execution module for database queries."""

import logging
from typing import Dict, List

from queries.get_chest_dicom_files_and_reports import (
    get_all_studies_count,
    get_all_studies_data,
)
from sync.database_breach import DatabaseBridge

logger = logging.getLogger(__name__)


class QueryExecutor:
    """Executes queries to extract DICOM data from the database."""

    def __init__(self, bridge: DatabaseBridge):
        """Initialize query executor.

        :param bridge: DatabaseBridge instance for database connections
        """
        self.bridge = bridge

    def get_total_count(self) -> int:
        """Get total count of records for pagination.

        :return: Total count of records
        """
        cursor = self.bridge.new_cursor(self.bridge.source_conn)
        try:
            cursor.execute(get_all_studies_count)
            result = cursor.fetchone()
            return result['total_count'] if result else 0
        except Exception as e:
            logger.error(f"Error getting total count: {str(e)}")
            return 0
        finally:
            cursor.close()

    def fetch_page(self, page: int, page_size: int) -> List[Dict]:
        """Fetch a page of results from the query.

        :param page: Page number (0-indexed)
        :param page_size: Number of records per page
        :return: List of result dictionaries
        """
        logger.info(f"Fetching page {page} (offset={page * page_size}, limit={page_size})")
        cursor = self.bridge.new_cursor(self.bridge.source_conn)

        try:
            offset = page * page_size
            cursor.execute(get_all_studies_data, (offset, page_size))
            results = cursor.fetchall()
            logger.info(f"Fetched {len(results)} rows from page {page}")
            return results
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
        finally:
            cursor.close()


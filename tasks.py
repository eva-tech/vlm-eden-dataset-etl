"""This file contains legacy Celery tasks.

NOTE: This file is kept for backward compatibility but is no longer actively used.
The new ETL pipeline uses etl/tasks/batch_tasks.py for batch processing.

If you need to run the ETL pipeline, use:
    python extract_and_upload_dicom_reports.py

This file may be removed in a future version.
"""

import logging

logger = logging.getLogger(__name__)

# Legacy tasks have been removed.
# The new ETL pipeline is in etl/tasks/batch_tasks.py
# Run the pipeline using: python extract_and_upload_dicom_reports.py

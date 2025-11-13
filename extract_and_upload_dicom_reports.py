#!/usr/bin/env python3
"""
Main entrypoint for DICOM ETL pipeline.

This script orchestrates the extraction, transformation, and loading of DICOM files
and reports from the database to Google Cloud Storage.
"""

import logging
import os
import sys

from dotenv import load_dotenv

from etl.pipeline.etl_pipeline import ETLPipeline
from sync.database_breach import DatabaseBridge

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main execution function."""
    bridge = None
    pipeline = None

    try:
        # Initialize services
        bridge = DatabaseBridge()
        bucket_name = os.getenv("GCS_BUCKET_NAME", "gcs-bucket-name")

        # Initialize pipeline
        pipeline = ETLPipeline(
            bridge=bridge,
            bucket_name=bucket_name,
        )

        # Run pipeline
        result = pipeline.run()

        if result.get('success'):
            logger.info("Pipeline completed successfully")
            sys.exit(0)
        else:
            logger.error(f"Pipeline failed: {result.get('error')}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if bridge:
            bridge.close_connections()
        # Optionally cleanup - comment out if you want to keep files
        # if pipeline:
        #     pipeline.cleanup()


if __name__ == "__main__":
    main()

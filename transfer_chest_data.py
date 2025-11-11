#!/usr/bin/env python3
"""Script to transfer chest CR/DX DICOM files and reports from S3 to GCS."""

import logging
import sys

from sync.chest_transfer_pipeline import ChestTransferPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Main function to execute the transfer."""
    logger.info("Starting chest CR/DX data transfer pipeline")

    pipeline = None
    try:
        pipeline = ChestTransferPipeline()
        result = pipeline.transfer_all_chest_data()

        # Print summary
        print("\n" + "=" * 90)
        print("TRANSFER SUMMARY")
        print("=" * 90)
        print(f"Success: {result['success']}")
        print(f"DICOM Files: {result['dicom_files_transferred']}/{result['dicom_files_processed']} transferred")
        print(f"Reports: {result['reports_transferred']}/{result['reports_processed']} transferred")
        print(f"Unique Studies: {result['studies_processed']}")
        print(f"Errors: {len(result['errors'])}")

        if result["errors"]:
            print("\nErrors:")
            for error in result["errors"][:10]:  # Show first 10 errors
                print(f"  - {error}")
            if len(result["errors"]) > 10:
                print(f"  ... and {len(result['errors']) - 10} more errors")

        print("=" * 90)

        return 0 if result["success"] else 1

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        return 1

    finally:
        if pipeline:
            pipeline.close()


if __name__ == "__main__":
    sys.exit(main())


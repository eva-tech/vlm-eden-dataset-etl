"""ETL pipeline orchestration module with Celery-based batch processing."""

import csv
import json
import logging
import os
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

from celery import group
from dotenv import load_dotenv

from etl.batching.batch_creator import BatchCreator
from etl.extract.query_executor import QueryExecutor
from etl.load.gcs_uploader import GCSUploader
from etl.tasks.batch_tasks import process_page_batch_task
from sync.database_breach import DatabaseBridge

load_dotenv()
logger = logging.getLogger(__name__)

# Configuration
PAGE_SIZE = 25
BATCH_SIZE = 25
PROGRESS_FILE = "extraction_progress.json"


class ETLPipeline:
    """Orchestrates the ETL process: extract, transform, and load using Celery tasks."""

    def __init__(
        self,
        bridge: DatabaseBridge,
        bucket_name: str,
        output_dir: Optional[str] = None,
        page_size: int = PAGE_SIZE,
        batch_size: int = BATCH_SIZE,
        use_celery: bool = True,
    ):
        """Initialize ETL pipeline.

        :param bridge: DatabaseBridge instance
        :param bucket_name: GCS bucket name
        :param output_dir: Output directory for temporary files
        :param page_size: Number of records per page
        :param batch_size: Number of records per batch
        :param use_celery: Whether to use Celery for parallel batch processing
        """
        self.bridge = bridge
        self.bucket_name = bucket_name
        self.output_dir = output_dir or tempfile.mkdtemp(prefix="dicom_extract_")
        self.page_size = page_size
        self.batch_size = batch_size
        self.use_celery = use_celery

        # Initialize components
        self.query_executor = QueryExecutor(bridge)
        self.batch_creator = BatchCreator(batch_size)
        self.gcs_uploader = GCSUploader(bucket_name)

        # File paths
        self.csv_path = os.path.join(self.output_dir, "dicom-reports-extracted-sample.csv")
        self.progress_file = os.path.join(self.output_dir, PROGRESS_FILE)

        # Progress tracking
        self.start_time = None
        self.processed_file_keys: Set[str] = set()
        self.all_csv_rows: List[Dict] = []

        # Statistics
        self.total_files_to_download = 0
        self.total_files_downloaded = 0
        self.total_files_to_upload = 0
        self.total_files_uploaded = 0

        # Create output directories
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(self.output_dir, 'dicom_files')).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(self.output_dir, 'images_jpg')).mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Using Celery for batch processing: {self.use_celery}")

    def format_time(self, seconds: float) -> str:
        """Format seconds into human-readable time string."""
        return str(timedelta(seconds=int(seconds)))

    def load_progress(self) -> dict:
        """Load progress from previous run."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    logger.info(f"Loaded progress: page={progress.get('current_page', 0)}, "
                              f"processed_files={progress.get('processed_file_count', 0)}")
                    self.processed_file_keys = set(progress.get('processed_file_keys', []))
                    return progress
            except Exception as e:
                logger.warning(f"Failed to load progress: {str(e)}")
        return {
            "current_page": 0,
            "processed_file_count": 0,
            "processed_file_keys": [],
            "csv_rows": [],
        }

    def save_progress(self, progress: dict):
        """Save progress to file."""
        try:
            progress['processed_file_keys'] = list(self.processed_file_keys)
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save progress: {str(e)}")

    def save_csv_rows(self, csv_rows: List[Dict], append: bool = False):
        """Save CSV rows to file."""
        if not csv_rows:
            return

        # Get all unique keys
        fieldnames = set()
        for row in csv_rows:
            fieldnames.update(row.keys())
        fieldnames = sorted(fieldnames)

        file_mode = 'a' if append else 'w'
        with open(self.csv_path, file_mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not append:
                writer.writeheader()
            writer.writerows(csv_rows)

        logger.info(f"{'Appended' if append else 'Saved'} {len(csv_rows)} rows to CSV")

    def upload_to_gcs(self):
        """Upload CSV file and downloaded DICOM files to GCS."""
        logger.info("Uploading files to GCS...")

        # Upload CSV file
        csv_gcs_path = "eden-dataset-vlms/sample-test/dicom-reports-extracted-sample.csv"
        self.gcs_uploader.upload_file(self.csv_path, csv_gcs_path)

        # Upload DICOM files
        dicom_files_dir = os.path.join(self.output_dir, 'dicom_files')
        if os.path.exists(dicom_files_dir):
            file_count = sum(1 for root, dirs, files in os.walk(dicom_files_dir) for f in files)
            self.total_files_to_upload = file_count
            logger.info(f"Found {file_count} DICOM files to upload")
            self.gcs_uploader.upload_directory_contents(
                dicom_files_dir, "eden-dataset-vlms/sample-test/dicom-files/"
            )
            self.total_files_uploaded = file_count

        # Upload JPG images if they exist
        jpg_files_dir = os.path.join(self.output_dir, 'images_jpg')
        if os.path.exists(jpg_files_dir):
            file_count = sum(1 for root, dirs, files in os.walk(jpg_files_dir) for f in files)
            if file_count > 0:
                logger.info(f"Found {file_count} JPG images to upload")
                self.gcs_uploader.upload_directory_contents(
                    jpg_files_dir, "eden-dataset-vlms/sample-test/images-jpg/"
                )
            else:
                logger.info("No JPG images to upload (directory is empty)")

    def cleanup(self):
        """Clean up temporary files."""
        import shutil
        if os.path.exists(self.output_dir):
            logger.info(f"Cleaning up temporary directory: {self.output_dir}")
            shutil.rmtree(self.output_dir)

    def run(self) -> Dict:
        """Run the complete ETL pipeline using Celery for parallel batch processing.

        :return: Dictionary with execution statistics
        """
        overall_start_time = time.time()
        self.start_time = overall_start_time
        logger.info("=" * 70)
        logger.info("Starting DICOM and reports extraction process with Celery batch processing")
        logger.info("=" * 70)

        try:
            # Load progress
            progress = self.load_progress()
            start_page = progress.get('current_page', 0)

            # Get total count
            logger.info("Fetching total record count...")
            count_start = time.time()
            total_count = self.query_executor.get_total_count()
            count_elapsed = time.time() - count_start
            total_pages = (total_count + self.page_size - 1) // self.page_size

            logger.info("=" * 70)
            logger.info("EXTRACTION STATISTICS")
            logger.info("=" * 70)
            logger.info(f"Total records: {total_count}")
            logger.info(f"Total pages: {total_pages}")
            logger.info(f"Page size: {self.page_size}")
            logger.info(f"Batch size: {self.batch_size}")
            logger.info(f"Starting from page: {start_page + 1}")
            logger.info(f"Using Celery: {self.use_celery}")
            logger.info(f"Count query time: {self.format_time(count_elapsed)} ({count_elapsed:.2f} seconds)")
            logger.info("=" * 70)

            # Process pages using Celery tasks
            extraction_start = time.time()
            
            if self.use_celery:
                # Dispatch pages as Celery tasks for parallel processing
                page_tasks = []
                for page in range(start_page, total_pages):
                    page_tasks.append(
                        process_page_batch_task.s(
                            page=page,
                            page_size=self.page_size,
                            batch_size=self.batch_size,
                            output_dir=self.output_dir,
                            bucket_name=self.bucket_name,
                            processed_file_keys=list(self.processed_file_keys),
                        )
                    )

                logger.info(f"Dispatching {len(page_tasks)} page tasks to Celery workers...")
                job = group(*page_tasks)
                result = job.apply_async()
                
                # Wait for all pages to complete
                logger.info(f"Waiting for page processing to complete (task group: {result.id})...")
                page_results = result.get(timeout=3600)  # 1 hour timeout

                # Aggregate results from all pages
                total_records = 0
                total_files = 0
                total_csv_rows = 0
                total_downloaded = 0
                successful_pages = 0

                for page_result in page_results:
                    if page_result.get('success'):
                        successful_pages += 1
                        total_records += page_result.get('records_processed', 0)
                        total_files += page_result.get('total_files', 0)
                        total_csv_rows += page_result.get('total_csv_rows', 0)
                        total_downloaded += page_result.get('total_downloaded', 0)
                        
                        # Update processed file keys
                        self.processed_file_keys.update(page_result.get('processed_file_keys', []))
                        
                        # Collect CSV rows
                        self.all_csv_rows.extend(page_result.get('csv_rows', []))

                # Save all CSV rows
                if self.all_csv_rows:
                    self.save_csv_rows(self.all_csv_rows, append=False)

                # Update progress
                progress['processed_file_keys'] = list(self.processed_file_keys)
                progress['processed_file_count'] = len(self.processed_file_keys)
                progress['current_page'] = total_pages
                self.save_progress(progress)

                self.total_files_downloaded = total_downloaded
                logger.info(f"Processed {successful_pages}/{len(page_results)} pages successfully")
            else:
                # Fallback to synchronous processing (for testing or when Celery is unavailable)
                logger.warning("Celery not enabled, using synchronous processing")
                for page in range(start_page, total_pages):
                    logger.info(f"Processing page {page + 1}/{total_pages}")
                    # This would use the old synchronous method
                    # For now, we'll skip this path as it's not the focus of PR2

            extraction_elapsed = time.time() - extraction_start
            logger.info("")
            logger.info("=" * 70)
            logger.info("EXTRACTION PHASE COMPLETE")
            logger.info("=" * 70)
            logger.info(f"Extraction time: {self.format_time(extraction_elapsed)} ({extraction_elapsed:.2f} seconds)")

            # Upload to GCS
            logger.info("")
            logger.info("=" * 70)
            logger.info("Starting GCS upload...")
            logger.info("=" * 70)
            upload_start = time.time()
            self.upload_to_gcs()
            upload_elapsed = time.time() - upload_start

            # Final summary
            total_elapsed = time.time() - overall_start_time
            logger.info("")
            logger.info("=" * 70)
            logger.info("EXTRACTION AND UPLOAD COMPLETE")
            logger.info("=" * 70)
            logger.info("TIMING SUMMARY:")
            logger.info(f"  Count query: {self.format_time(count_elapsed)} ({count_elapsed:.2f}s)")
            logger.info(f"  Extraction: {self.format_time(extraction_elapsed)} ({extraction_elapsed:.2f}s)")
            logger.info(f"  Upload: {self.format_time(upload_elapsed)} ({upload_elapsed:.2f}s)")
            logger.info(f"  TOTAL TIME: {self.format_time(total_elapsed)} ({total_elapsed:.2f} seconds)")
            logger.info("")
            logger.info("STATISTICS SUMMARY:")
            logger.info(f"  Total records: {total_count}")
            logger.info(f"  Processed files: {len(self.processed_file_keys)}")
            logger.info(f"  Files downloaded: {self.total_files_downloaded}")
            logger.info(f"  Files uploaded: {self.total_files_uploaded}")
            logger.info(f"  CSV rows: {len(self.all_csv_rows)}")
            logger.info("=" * 70)

            return {
                'success': True,
                'total_count': total_count,
                'total_pages': total_pages,
                'processed_files': len(self.processed_file_keys),
                'downloaded_files': self.total_files_downloaded,
                'uploaded_files': self.total_files_uploaded,
                'csv_rows': len(self.all_csv_rows),
                'total_time': total_elapsed,
            }

        except Exception as e:
            logger.error(f"Error in ETL pipeline: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e),
            }

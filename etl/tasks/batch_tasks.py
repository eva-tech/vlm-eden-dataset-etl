"""Celery tasks for batch processing in the ETL pipeline."""

import logging
import os
from typing import Dict, List

from celery import group
from celery_app import app

from etl.batching.batch_creator import BatchCreator
from etl.extract.data_fetcher import DataFetcher
from etl.extract.query_executor import QueryExecutor
from etl.transform.data_processor import DataProcessor
from etl.transform.dicom_converter import DICOMConverter
from sync.database_breach import DatabaseBridge

logger = logging.getLogger(__name__)


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_batch_task(
    self,
    batch_id: str,
    batch_data: List[Dict],
    processed_file_keys: List[str],
    output_dir: str,
    bucket_name: str,
) -> Dict:
    """Process a batch of query results: extract, transform, and prepare for loading.

    This task is idempotent - it checks processed_file_keys to avoid duplicate processing.

    :param self: Celery task instance (for retries)
    :param batch_id: Unique identifier for this batch
    :param batch_data: List of query result dictionaries
    :param processed_file_keys: List of already processed file keys (for idempotency)
    :param output_dir: Output directory for temporary files
    :param bucket_name: GCS bucket name
    :return: Dictionary with batch processing results and statistics
    """
    bridge = None
    try:
        logger.info(f"Processing batch {batch_id} with {len(batch_data)} records")

        # Initialize components
        bridge = DatabaseBridge()
        data_processor = DataProcessor()
        data_fetcher = DataFetcher()
        processed_keys_set = set(processed_file_keys)

        # Process batch
        file_data, report_fields_by_study, batch_processed_keys = data_processor.process_batch(
            batch_data, processed_keys_set
        )

        # Prepare CSV rows
        csv_rows = data_processor.prepare_csv_rows(
            file_data, report_fields_by_study, processed_keys_set, output_dir
        )

        # Download files
        download_tasks = []
        for csv_row in csv_rows:
            if csv_row.get('file_url') and csv_row.get('local_file_path'):
                download_tasks.append((csv_row['file_url'], csv_row['local_file_path']))

        # Download and convert files
        dicom_converter = DICOMConverter(quality=1.0)
        images_jpg_dir = os.path.join(output_dir, 'images_jpg')
        os.makedirs(images_jpg_dir, exist_ok=True)
        
        downloaded_count = 0
        converted_count = 0
        for file_url, local_path in download_tasks:
            if data_fetcher.download_file(file_url, local_path):
                downloaded_count += 1
                # Update CSV row
                for row in csv_rows:
                    if row.get('local_file_path') == local_path:
                        row['downloaded'] = True
                        # Convert to JPG if instance_id is available
                        instance_id = row.get('instance_id')
                        if instance_id:
                            jpg_path = os.path.join(images_jpg_dir, f"{instance_id}.jpg")
                            if dicom_converter.convert(local_path, jpg_path):
                                converted_count += 1
                                row['jpg_path'] = jpg_path
                        break

        result = {
            "success": True,
            "batch_id": batch_id,
            "records_processed": len(batch_data),
            "files_found": len(file_data),
            "csv_rows": len(csv_rows),
            "files_downloaded": downloaded_count,
            "files_converted": converted_count,
            "processed_file_keys": list(batch_processed_keys),
            "csv_rows_data": csv_rows,
        }

        logger.info(
            f"Batch {batch_id} completed: {downloaded_count}/{len(download_tasks)} files downloaded, "
            f"{converted_count} files converted to JPG, {len(csv_rows)} CSV rows prepared"
        )

        return result

    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        return {
            "success": False,
            "batch_id": batch_id,
            "error": str(e),
            "records_processed": 0,
            "files_found": 0,
            "csv_rows": 0,
            "files_downloaded": 0,
        }
    finally:
        if bridge:
            bridge.close_connections()


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_batch_task_async(
    self,
    batch_id: str,
    batch_data: List[Dict],
    processed_file_keys: List[str],
    output_dir: str,
    bucket_name: str,
) -> Dict:
    """Async wrapper for process_batch_task (for future use if needed).

    :param self: Celery task instance (for retries)
    :param batch_id: Unique identifier for this batch
    :param batch_data: List of query result dictionaries
    :param processed_file_keys: List of already processed file keys (for idempotency)
    :param output_dir: Output directory for temporary files
    :param bucket_name: GCS bucket name
    :return: Dictionary with batch processing results and statistics
    """
    try:
        return process_batch_task(
            batch_id=batch_id,
            batch_data=batch_data,
            processed_file_keys=processed_file_keys,
            output_dir=output_dir,
            bucket_name=bucket_name,
        )
    except Exception as e:
        logger.error(f"Error in async batch task {batch_id}: {str(e)}")
        try:
            raise self.retry(exc=e)
        except self.MaxRetriesExceededError:
            return {
                "success": False,
                "batch_id": batch_id,
                "error": str(e),
                "records_processed": 0,
                "files_found": 0,
                "csv_rows": 0,
                "files_downloaded": 0,
            }


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def download_and_convert_batch_task(
    self,
    batch_id: str,
    file_tasks: List[Dict],
    output_dir: str,
) -> Dict:
    """Download and convert DICOM files to JPG for a batch.

    :param self: Celery task instance (for retries)
    :param batch_id: Unique identifier for this batch
    :param file_tasks: List of file task dictionaries with file_url, local_path, instance_id
    :param output_dir: Output directory for files
    :return: Dictionary with download and conversion results
    """
    try:
        logger.info(f"Downloading and converting batch {batch_id} with {len(file_tasks)} files")

        data_fetcher = DataFetcher()
        dicom_converter = DICOMConverter()

        downloaded_count = 0
        converted_count = 0
        errors = []

        for file_task in file_tasks:
            file_url = file_task.get('file_url')
            local_path = file_task.get('local_path')
            instance_id = file_task.get('instance_id')

            if not file_url or not local_path:
                continue

            # Download file
            if data_fetcher.download_file(file_url, local_path):
                downloaded_count += 1

                # Convert to JPG if instance_id provided
                if instance_id:
                    jpg_path = os.path.join(output_dir, 'images_jpg', f"{instance_id}.jpg")
                    if dicom_converter.convert(local_path, jpg_path):
                        converted_count += 1
            else:
                errors.append(f"Failed to download {file_url}")

        result = {
            "success": True,
            "batch_id": batch_id,
            "files_total": len(file_tasks),
            "files_downloaded": downloaded_count,
            "files_converted": converted_count,
            "errors": errors,
        }

        logger.info(
            f"Batch {batch_id} download/conversion completed: "
            f"{downloaded_count}/{len(file_tasks)} downloaded, "
            f"{converted_count} converted"
        )

        return result

    except Exception as e:
        logger.error(f"Error in download_and_convert_batch_task for batch {batch_id}: {str(e)}")
        try:
            raise self.retry(exc=e)
        except self.MaxRetriesExceededError:
            return {
                "success": False,
                "batch_id": batch_id,
                "error": str(e),
                "files_downloaded": 0,
                "files_converted": 0,
            }


@app.task(bind=True, max_retries=2, default_retry_delay=30)
def process_page_batch_task(
    self,
    page: int,
    page_size: int,
    batch_size: int,
    output_dir: str,
    bucket_name: str,
    processed_file_keys: List[str],
) -> Dict:
    """Process a page of query results by splitting into batches and processing in parallel.

    :param self: Celery task instance (for retries)
    :param page: Page number (0-indexed)
    :param page_size: Number of records per page
    :param batch_size: Number of records per batch
    :param output_dir: Output directory for temporary files
    :param bucket_name: GCS bucket name
    :param processed_file_keys: List of already processed file keys
    :return: Dictionary with page processing results and statistics
    """
    bridge = None
    try:
        logger.info(f"Processing page {page} with page_size={page_size}, batch_size={batch_size}")

        # Initialize components
        bridge = DatabaseBridge()
        query_executor = QueryExecutor(bridge)
        batch_creator = BatchCreator(batch_size)

        # Fetch page
        page_results = query_executor.fetch_page(page, page_size)

        if not page_results:
            logger.info(f"No results for page {page}")
            return {
                "success": True,
                "page": page,
                "batches_processed": 0,
                "records_processed": 0,
                "total_files": 0,
                "total_csv_rows": 0,
            }

        # Split into batches
        batches = batch_creator.create_batches(page_results)

        # Process batches synchronously within this task (to avoid nested .get() calls)
        # Batches are processed sequentially, but pages run in parallel
        total_records = 0
        total_files = 0
        total_csv_rows = 0
        total_downloaded = 0
        all_processed_keys = []
        all_csv_rows = []

        for batch_num, batch in enumerate(batches, 1):
            batch_id = f"page_{page}_batch_{batch_num}"
            logger.info(f"Processing batch {batch_num}/{len(batches)} in page {page}")
            
            # Process batch synchronously (can't use nested Celery groups with .get())
            batch_result = process_batch_task(
                batch_id=batch_id,
                batch_data=batch,
                processed_file_keys=processed_file_keys,
                output_dir=output_dir,
                bucket_name=bucket_name,
            )
            
            if batch_result.get('success'):
                total_records += batch_result.get('records_processed', 0)
                total_files += batch_result.get('files_found', 0)
                total_csv_rows += batch_result.get('csv_rows', 0)
                total_downloaded += batch_result.get('files_downloaded', 0)
                all_processed_keys.extend(batch_result.get('processed_file_keys', []))
                all_csv_rows.extend(batch_result.get('csv_rows_data', []))

        return {
            "success": True,
            "page": page,
            "batches_processed": len(batches),
            "records_processed": total_records,
            "total_files": total_files,
            "total_csv_rows": total_csv_rows,
            "total_downloaded": total_downloaded,
            "processed_file_keys": list(set(all_processed_keys)),
            "csv_rows": all_csv_rows,
        }

    except Exception as e:
        logger.error(f"Error processing page {page}: {str(e)}")
        try:
            raise self.retry(exc=e)
        except self.MaxRetriesExceededError:
            return {
                "success": False,
                "page": page,
                "error": str(e),
                "batches_processed": 0,
                "records_processed": 0,
            }
    finally:
        if bridge:
            bridge.close_connections()


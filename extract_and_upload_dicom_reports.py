#!/usr/bin/env python3
"""
Script to extract DICOM files and reports from query results with pagination and batching.
Downloads DICOM files using Celery tasks and uploads everything to GCS bucket.
"""

import csv
import json
import logging
import os
import sys
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv
# Removed celery import - using direct downloads instead

from queries.get_chest_dicom_files_and_reports import get_all_studies_data, get_all_studies_count
from sync.database_breach import DatabaseBridge
from sync.gcs_service import GCSService
from celery import group
from tasks import convert_dicom_to_jpg, upload_file_to_gcs

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PAGE_SIZE = 25
BATCH_SIZE = 25
PROGRESS_FILE = "extraction_progress.json"


class DICOMReportsExtractor:
    """Extracts DICOM files and reports from query results with pagination and batching."""
    
    def __init__(self, gcs_service: GCSService, output_dir: Optional[str] = None):
        self.gcs_service = gcs_service
        self.output_dir = output_dir or tempfile.mkdtemp(prefix="dicom_extract_")
        self.csv_path = os.path.join(self.output_dir, "dicom-reports-extracted-sample.csv")
        self.progress_file = os.path.join(self.output_dir, PROGRESS_FILE)
        self.downloaded_files = []
        self.processed_file_keys = set()  # Track processed files to avoid duplicates
        
        # Progress tracking
        self.start_time = None
        self.extraction_start_time = None
        self.download_start_time = None
        self.upload_start_time = None
        self.total_files_to_download = 0
        self.total_files_downloaded = 0
        self.total_files_to_upload = 0
        self.total_files_uploaded = 0
        self.total_jpg_converted = 0
        self.total_jpg_uploaded = 0
        
        # Create output directories if they don't exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(self.output_dir, 'dicom_files')).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(self.output_dir, 'images_jpg')).mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {self.output_dir}")
    
    def format_time(self, seconds: float) -> str:
        """Format seconds into human-readable time string."""
        return str(timedelta(seconds=int(seconds)))
    
    def print_progress(self, current: int, total: int, prefix: str = "Progress", suffix: str = ""):
        """Print progress bar."""
        if total == 0:
            return
        
        percent = (current / total) * 100
        bar_length = 50
        filled_length = int(bar_length * current / total)
        bar = '=' * filled_length + '-' * (bar_length - filled_length)
        
        print(f'\r{prefix}: [{bar}] {current}/{total} ({percent:.1f}%) {suffix}', end='', flush=True)
        if current == total:
            print()  # New line when complete
    
    def load_progress(self) -> dict:
        """Load progress from previous run."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    logger.info(f"Loaded progress: page={progress.get('current_page', 0)}, "
                              f"processed_files={progress.get('processed_file_count', 0)}")
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
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save progress: {str(e)}")
    
    def get_total_count(self, bridge: DatabaseBridge) -> int:
        """Get total count of records for pagination."""
        cursor = bridge.new_cursor(bridge.source_conn)
        try:
            cursor.execute(get_all_studies_count)
            result = cursor.fetchone()
            return result['total_count'] if result else 0
        except Exception as e:
            logger.error(f"Error getting total count: {str(e)}")
            return 0
        finally:
            cursor.close()
    
    def fetch_query_results_page(self, bridge: DatabaseBridge, page: int, page_size: int) -> List[Dict]:
        """Fetch a page of results from the query."""
        logger.info(f"Fetching page {page} (offset={page * page_size}, limit={page_size})")
        cursor = bridge.new_cursor(bridge.source_conn)
        
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
    
    def extract_and_process_batch(self, batch: List[Dict], progress: dict) -> tuple:
        """Extract DICOM files and reports from a batch of query results.
        
        Returns: (csv_rows, processed_file_keys)
        """
        logger.info(f"Processing batch of {len(batch)} rows...")
        
        # Group by file and aggregate report fields at study level
        file_data = {}
        report_fields_by_study = {}
        processed_file_keys_in_batch = []
        
        for row in batch:
            study_id = row.get('study_id')
            series_id = row.get('series_id')
            instance_id = row.get('instance_id')
            file_path = row.get('file_path')
            file_url = row.get('file_url')
            field_value = row.get('field_value')
            field_created_at = row.get('field_created_at')
            
            # Create composite key for the file
            file_key = (study_id, series_id, instance_id, file_path)
            file_key_str = str(file_key)
            
            # Skip if already processed
            if file_key_str in progress.get('processed_file_keys', []):
                continue
            
            # Store file data
            if file_key not in file_data:
                file_data[file_key] = {
                    'study_id': study_id,
                    'series_id': series_id,
                    'instance_id': instance_id,
                    'series_number': row.get('series_number'),
                    'instance_number': row.get('instance_number'),
                    'file_path': file_path,
                    'file_url': file_url,
                    'field_created_at': field_created_at,
                }
            
            # Aggregate report fields at STUDY level
            if study_id not in report_fields_by_study:
                report_fields_by_study[study_id] = []
            
            if field_value:
                existing_values = [rv['value'] for rv in report_fields_by_study[study_id]]
                if field_value not in existing_values:
                    report_fields_by_study[study_id].append({
                        'value': field_value,
                        'created_at': field_created_at
                    })
                    if field_created_at and (not file_data[file_key]['field_created_at'] or 
                                             field_created_at > file_data[file_key]['field_created_at']):
                        file_data[file_key]['field_created_at'] = field_created_at
        
        # Prepare CSV rows and queue downloads
        csv_rows = []
        download_tasks = []
        
        for file_key, file_info in file_data.items():
            # Skip if already processed
            if str(file_key) in progress.get('processed_file_keys', []):
                continue
            
            csv_row = {
                'study_id': file_info['study_id'],
                'series_number': file_info['series_number'],
                'instance_number': file_info['instance_number'],
                'instance_id': file_info['instance_id'],  # Add instance_id column
                'file_path': file_info['file_path'],
                'field_created_at': file_info['field_created_at'],
                'file_url': file_info['file_url'],
                'report_value': None,
            }
            
            # Get report field values for this study
            study_id = file_info['study_id']
            report_values = report_fields_by_study.get(study_id, [])
            if report_values:
                csv_row['report_value'] = ' | '.join([rv['value'] for rv in report_values if rv['value']])
            
            # Prepare download task if file_url exists
            file_url = file_info['file_url']
            if file_url:
                instance_id_str = str(file_info['instance_id'])
                file_path = file_info['file_path'] or ''
                
                # Save as dicom_files/instance_id.dcm (flat structure)
                file_name = f"{instance_id_str}.dcm"
                
                file_dir = os.path.join(self.output_dir, 'dicom_files')
                local_file_path = os.path.join(file_dir, file_name)
                
                # Queue download task
                download_tasks.append((file_url, local_file_path))
                csv_row['local_file_path'] = local_file_path
                csv_row['downloaded'] = False  # Will be updated after download completes
            else:
                csv_row['local_file_path'] = None
                csv_row['downloaded'] = False
            
            csv_rows.append(csv_row)
            processed_file_keys_in_batch.append(file_key_str)
        
        # Download files directly (synchronously)
        if download_tasks:
            logger.info(f"Downloading {len(download_tasks)} files...")
            self.total_files_to_download += len(download_tasks)
            
            for i, (file_url, local_path) in enumerate(download_tasks):
                try:
                    if self.download_file(file_url, local_path):
                        if i < len(csv_rows):
                            csv_rows[i]['downloaded'] = True
                            self.downloaded_files.append(local_path)
                        self.total_files_downloaded += 1
                    else:
                        if i < len(csv_rows):
                            csv_rows[i]['downloaded'] = False
                    
                    # Print progress
                    self.print_progress(
                        self.total_files_downloaded,
                        self.total_files_to_download,
                        prefix="Downloading DICOM files",
                        suffix=f"({self.total_files_downloaded}/{self.total_files_to_download})"
                    )
                except Exception as e:
                    logger.error(f"Error downloading {file_url}: {str(e)}")
                    if i < len(csv_rows):
                        csv_rows[i]['downloaded'] = False
            
            # Convert DICOM files to JPG using Celery tasks (after all downloads complete)
            if download_tasks:
                logger.info("Converting DICOM files to JPG...")
                conversion_tasks = []
                for csv_row in csv_rows:
                    if csv_row.get('downloaded') and csv_row.get('local_file_path'):
                        dicom_path = csv_row['local_file_path']
                        instance_id = csv_row.get('instance_id')
                        if instance_id:
                            jpg_path = os.path.join(self.output_dir, 'images_jpg', f"{instance_id}.jpg")
                            conversion_tasks.append((dicom_path, jpg_path, instance_id))
                
                if conversion_tasks:
                    logger.info(f"Queuing {len(conversion_tasks)} DICOM to JPG conversion tasks...")
                    conversion_job = group(
                        convert_dicom_to_jpg.s(dicom_path, jpg_path, instance_id)
                        for dicom_path, jpg_path, instance_id in conversion_tasks
                    )
                    conversion_result = conversion_job.apply_async()
                    
                    logger.info(f"Waiting for conversions to complete (task group: {conversion_result.id})...")
                    try:
                        conversion_results = conversion_result.get(timeout=600)  # 10 minute timeout
                        successful_conversions = sum(1 for r in conversion_results if r.get('success'))
                        self.total_jpg_converted = successful_conversions
                        logger.info(f"DICOM to JPG conversions: {successful_conversions}/{len(conversion_tasks)} successful")
                    except Exception as e:
                        logger.error(f"Error waiting for conversions: {str(e)}")
        
        logger.info(f"Processed batch: {len(csv_rows)} rows, {len(download_tasks)} downloads")
        return csv_rows, processed_file_keys_in_batch
    
    def save_csv_rows(self, csv_rows: List[Dict], append: bool = False):
        """Save CSV rows to file (append or overwrite)."""
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
    
    def download_file(self, url: str, output_path: str) -> bool:
        """Download a file from URL to local path."""
        try:
            logger.debug(f"Downloading {url} to {output_path}")
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded: {url} -> {output_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download {url}: {str(e)}")
            return False
    
    def upload_to_gcs(self):
        """Upload CSV file and downloaded DICOM files to GCS bucket."""
        logger.info("Uploading files to GCS...")
        
        # Upload CSV file
        csv_gcs_path = "eden-dataset-vlms/sample-test/dicom-reports-extracted-sample.csv"
        logger.info(f"Uploading CSV to gs://{self.gcs_service.destination_bucket_name}/{csv_gcs_path}")
        
        try:
            import subprocess
            import shutil
            
            gsutil_path = "/usr/local/google-cloud-sdk/bin/gsutil"
            if not os.path.exists(gsutil_path):
                gsutil_path = shutil.which("gsutil") or "gsutil"
            
            destination = f"gs://{self.gcs_service.destination_bucket_name}/{csv_gcs_path}"
            cmd = [gsutil_path, "cp", self.csv_path, destination]
            
            env = os.environ.copy()
            # Ensure GOOGLE_APPLICATION_CREDENTIALS is set if available
            app_default_creds = "/root/.config/gcloud/application_default_credentials.json"
            if os.path.exists(app_default_creds) and "GOOGLE_APPLICATION_CREDENTIALS" not in env:
                env["GOOGLE_APPLICATION_CREDENTIALS"] = app_default_creds
            result = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
            logger.info(f"CSV uploaded successfully to {destination}")
        except Exception as e:
            logger.error(f"Failed to upload CSV: {str(e)}")
            raise
        
        # Upload DICOM files directly using gsutil
        dicom_files_dir = os.path.join(self.output_dir, 'dicom_files')
        if os.path.exists(dicom_files_dir):
            logger.info(f"Uploading DICOM files from {dicom_files_dir}")
            
            # Count files to upload
            file_count = 0
            for root, dirs, files in os.walk(dicom_files_dir):
                file_count += len(files)
            
            self.total_files_to_upload = file_count
            logger.info(f"Found {file_count} DICOM files to upload")
            
            try:
                import subprocess
                import shutil
                
                gsutil_path = "/usr/local/google-cloud-sdk/bin/gsutil"
                if not os.path.exists(gsutil_path):
                    gsutil_path = shutil.which("gsutil") or "gsutil"
                
                # Upload directory contents directly to the specified path (not the directory itself)
                # Use dicom_files_dir/. to copy contents without creating nested directory
                destination = f"gs://{self.gcs_service.destination_bucket_name}/eden-dataset-vlms/sample-test/dicom-files/"
                # Copy contents of dicom_files directory directly into dicom-files/ (not creating nested dicom_files/)
                # The trailing /. tells gsutil to copy contents, not the directory itself
                cmd = [gsutil_path, "-m", "cp", "-r", os.path.join(dicom_files_dir, "."), destination]
                
                upload_start = time.time()
                env = os.environ.copy()
                # Ensure GOOGLE_APPLICATION_CREDENTIALS is set if available
                app_default_creds = "/root/.config/gcloud/application_default_credentials.json"
                if os.path.exists(app_default_creds) and "GOOGLE_APPLICATION_CREDENTIALS" not in env:
                    env["GOOGLE_APPLICATION_CREDENTIALS"] = app_default_creds
                result = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
                upload_elapsed = time.time() - upload_start
                
                self.total_files_uploaded = file_count
                logger.info(f"DICOM files uploaded successfully to {destination}")
                logger.info(f"Upload time: {self.format_time(upload_elapsed)} ({upload_elapsed:.2f} seconds)")
            except Exception as e:
                logger.error(f"Failed to upload DICOM files: {str(e)}")
                raise
        else:
            logger.warning(f"DICOM files directory not found: {dicom_files_dir}")
        
        # Upload JPG images to GCS
        jpg_files_dir = os.path.join(self.output_dir, 'images_jpg')
        if os.path.exists(jpg_files_dir):
            logger.info(f"Uploading JPG images from {jpg_files_dir}")
            
            # Collect all JPG files
            jpg_files = []
            for root, dirs, files in os.walk(jpg_files_dir):
                for file in files:
                    if file.endswith('.jpg'):
                        local_path = os.path.join(root, file)
                        # Upload as images-jpg/instance_id.jpg
                        gcs_path = f"eden-dataset-vlms/sample-test/images-jpg/{file}"
                        jpg_files.append((local_path, gcs_path))
            
            if jpg_files:
                logger.info(f"Queuing {len(jpg_files)} JPG file uploads...")
                jpg_upload_job = group(
                    upload_file_to_gcs.s(local_path, gcs_path, self.gcs_service.destination_bucket_name)
                    for local_path, gcs_path in jpg_files
                )
                jpg_upload_result = jpg_upload_job.apply_async()
                
                logger.info(f"Waiting for JPG uploads to complete (task group: {jpg_upload_result.id})...")
                try:
                    jpg_upload_results = jpg_upload_result.get(timeout=600)  # 10 minute timeout
                    successful_jpg_uploads = sum(1 for r in jpg_upload_results if r.get('success'))
                    logger.info(f"JPG images uploaded: {successful_jpg_uploads}/{len(jpg_files)} successful")
                except Exception as e:
                    logger.error(f"Error waiting for JPG uploads: {str(e)}")
        else:
            logger.warning(f"JPG images directory not found: {jpg_files_dir}")
    
    def cleanup(self):
        """Clean up temporary files."""
        import shutil
        if os.path.exists(self.output_dir):
            logger.info(f"Cleaning up temporary directory: {self.output_dir}")
            shutil.rmtree(self.output_dir)


def main():
    """Main execution function with pagination and batching."""
    overall_start_time = time.time()
    logger.info("=" * 70)
    logger.info("Starting DICOM and reports extraction process with pagination and batching")
    logger.info("=" * 70)
    
    bridge = None
    extractor = None
    
    try:
        # Initialize services
        extractor = DICOMReportsExtractor(GCSService())
        extractor.start_time = overall_start_time
        bridge = DatabaseBridge()
        
        # Load progress
        progress = extractor.load_progress()
        start_page = progress.get('current_page', 0)
        processed_file_keys = set(progress.get('processed_file_keys', []))
        
        # Get total count
        logger.info("Fetching total record count...")
        count_start = time.time()
        total_count = extractor.get_total_count(bridge)
        count_elapsed = time.time() - count_start
        total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        
        logger.info("=" * 70)
        logger.info("EXTRACTION STATISTICS")
        logger.info("=" * 70)
        logger.info(f"Total records: {total_count}")
        logger.info(f"Total pages: {total_pages}")
        logger.info(f"Page size: {PAGE_SIZE}")
        logger.info(f"Batch size: {BATCH_SIZE}")
        logger.info(f"Starting from page: {start_page + 1}")
        logger.info(f"Count query time: {extractor.format_time(count_elapsed)} ({count_elapsed:.2f} seconds)")
        logger.info("=" * 70)
        
        # Extraction phase
        extraction_start = time.time()
        extractor.extraction_start_time = extraction_start
        
        # Process pages
        for page in range(start_page, total_pages):
            logger.info("")
            logger.info("=" * 70)
            logger.info(f"Processing page {page + 1}/{total_pages} ({((page + 1) / total_pages * 100):.1f}%)")
            logger.info("=" * 70)
            
            page_start = time.time()
            
            # Fetch page
            page_results = extractor.fetch_query_results_page(bridge, page, PAGE_SIZE)
            
            if not page_results:
                logger.info(f"No results for page {page}, skipping")
                continue
            
            # Process in batches
            for batch_start in range(0, len(page_results), BATCH_SIZE):
                batch = page_results[batch_start:batch_start + BATCH_SIZE]
                batch_num = batch_start // BATCH_SIZE + 1
                total_batches = (len(page_results) + BATCH_SIZE - 1) // BATCH_SIZE
                
                logger.info(f"Processing batch {batch_num}/{total_batches} "
                          f"(rows {batch_start} to {batch_start + len(batch) - 1})")
                
                # Extract and process batch
                csv_rows, batch_processed_keys = extractor.extract_and_process_batch(batch, progress)
                
                if csv_rows:
                    # Save CSV rows (append after first page)
                    extractor.save_csv_rows(csv_rows, append=(page > 0 or batch_start > 0))
                    
                    # Update progress - track processed files from the batch
                    for file_key_str in batch_processed_keys:
                        processed_file_keys.add(file_key_str)
                    
                    progress['processed_file_keys'] = list(processed_file_keys)
                    progress['processed_file_count'] = len(processed_file_keys)
            
            page_elapsed = time.time() - page_start
            logger.info(f"Page {page + 1} completed in {extractor.format_time(page_elapsed)} ({page_elapsed:.2f} seconds)")
            
            # Update page progress
            progress['current_page'] = page + 1
            extractor.save_progress(progress)
        
        extraction_elapsed = time.time() - extraction_start
        logger.info("")
        logger.info("=" * 70)
        logger.info("EXTRACTION PHASE COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Extraction time: {extractor.format_time(extraction_elapsed)} ({extraction_elapsed:.2f} seconds)")
        logger.info(f"Total files to download: {extractor.total_files_to_download}")
        logger.info(f"Total files downloaded: {extractor.total_files_downloaded}")
        logger.info(f"Total CSV rows: {progress.get('processed_file_count', 0)}")
        logger.info("=" * 70)
        
        # Upload to GCS
        logger.info("")
        logger.info("=" * 70)
        logger.info("Starting GCS upload...")
        logger.info("=" * 70)
        upload_start = time.time()
        extractor.upload_start_time = upload_start
        extractor.upload_to_gcs()
        upload_elapsed = time.time() - upload_start
        
        # Final summary
        total_elapsed = time.time() - overall_start_time
        logger.info("")
        logger.info("=" * 70)
        logger.info("EXTRACTION AND UPLOAD COMPLETE")
        logger.info("=" * 70)
        logger.info("TIMING SUMMARY:")
        logger.info(f"  Count query: {extractor.format_time(count_elapsed)} ({count_elapsed:.2f}s)")
        logger.info(f"  Extraction: {extractor.format_time(extraction_elapsed)} ({extraction_elapsed:.2f}s)")
        logger.info(f"  Upload: {extractor.format_time(upload_elapsed)} ({upload_elapsed:.2f}s)")
        logger.info(f"  TOTAL TIME: {extractor.format_time(total_elapsed)} ({total_elapsed:.2f} seconds)")
        logger.info("")
        logger.info("FILES SUMMARY:")
        logger.info(f"  CSV file: {extractor.csv_path}")
        logger.info(f"  Downloaded DICOM files: {len(extractor.downloaded_files)}")
        logger.info(f"  Total processed files: {progress.get('processed_file_count', 0)}")
        logger.info(f"  Files uploaded to GCS: {extractor.total_files_uploaded}")
        logger.info("")
        logger.info("GCS LOCATIONS:")
        logger.info(f"  CSV: gs://{extractor.gcs_service.destination_bucket_name}/eden-dataset-vlms/sample-test/dicom-reports-extracted-sample.csv")
        logger.info(f"  DICOM files: gs://{extractor.gcs_service.destination_bucket_name}/eden-dataset-vlms/sample-test/dicom-files/")
        logger.info(f"  JPG images: gs://{extractor.gcs_service.destination_bucket_name}/eden-dataset-vlms/sample-test/images-jpg/")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if bridge:
            bridge.close_connections()
        # Optionally cleanup - comment out if you want to keep files
        # if extractor:
        #     extractor.cleanup()


if __name__ == "__main__":
    main()

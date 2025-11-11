#!/usr/bin/env python3
"""
Script to extract DICOM files and reports from query results,
download DICOM files, and upload everything to GCS bucket.
"""

import csv
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse
import requests
from dotenv import load_dotenv

from queries.get_chest_dicom_files_and_reports import get_all_studies_data
from sync.database_breach import DatabaseBridge
from sync.gcs_service import GCSService

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DICOMReportsExtractor:
    """Extracts DICOM files and reports from query results and uploads to GCS."""
    
    def __init__(self, gcs_service: GCSService, output_dir: Optional[str] = None):
        self.gcs_service = gcs_service
        self.output_dir = output_dir or tempfile.mkdtemp(prefix="dicom_extract_")
        self.csv_path = os.path.join(self.output_dir, "dicom-reports-extracted-sample.csv")
        self.downloaded_files = []
        
        # Create output directory if it doesn't exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {self.output_dir}")
    
    def fetch_query_results(self, bridge: DatabaseBridge) -> List[Dict]:
        """Fetch results from the query."""
        logger.info("Fetching query results...")
        cursor = bridge.new_cursor(bridge.source_conn)
        
        try:
            cursor.execute(get_all_studies_data)
            results = cursor.fetchall()
            logger.info(f"Fetched {len(results)} rows from query")
            return results
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
        finally:
            cursor.close()
    
    def download_file(self, url: str, output_path: str) -> bool:
        """Download a file from URL to local path."""
        try:
            logger.debug(f"Downloading {url} to {output_path}")
            response = requests.get(url, stream=True, timeout=30)
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
    
    def extract_and_process(self, results: List[Dict]) -> List[Dict]:
        """Extract DICOM files and reports, download files, and prepare CSV data."""
        logger.info("Processing query results...")
        
        # Group by file and aggregate report fields at study level
        # Use a composite key: (study_id, series_id, instance_id, file_path)
        file_data = {}
        report_fields_by_study = {}  # Aggregate at study level, not file level
        
        for row in results:
            study_id = row.get('study_id')
            series_id = row.get('series_id')
            instance_id = row.get('instance_id')
            file_path = row.get('file_path')
            file_url = row.get('file_url')
            field_value = row.get('field_value')
            field_created_at = row.get('field_created_at')
            
            # Create composite key for the file
            file_key = (study_id, series_id, instance_id, file_path)
            
            # Store file data (will be overwritten if duplicate, but that's OK)
            if file_key not in file_data:
                file_data[file_key] = {
                    'study_id': study_id,
                    'series_id': series_id,
                    'instance_id': instance_id,
                    'series_number': row.get('series_number'),
                    'instance_number': row.get('instance_number'),
                    'file_path': file_path,
                    'file_url': file_url,
                    'field_created_at': field_created_at,  # Keep the latest one
                }
            
            # Aggregate report fields at STUDY level (not file level)
            # Report fields belong to studies, so all files from a study share the same report fields
            if study_id not in report_fields_by_study:
                report_fields_by_study[study_id] = []
            
            if field_value:  # Only add non-null values
                # Avoid duplicates by checking if value already exists
                existing_values = [rv['value'] for rv in report_fields_by_study[study_id]]
                if field_value not in existing_values:
                    report_fields_by_study[study_id].append({
                        'value': field_value,
                        'created_at': field_created_at
                    })
                    # Update field_created_at to the latest one
                    if field_created_at and (not file_data[file_key]['field_created_at'] or 
                                             field_created_at > file_data[file_key]['field_created_at']):
                        file_data[file_key]['field_created_at'] = field_created_at
        
        # Combine file data with aggregated report fields
        csv_rows = []
        file_counter = 0
        
        for file_key, file_info in file_data.items():
            # Extract fields as specified: ps.id, pser.dicom_number, pi.dicom_number, pif.file, prf.created_at
            csv_row = {
                'study_id': file_info['study_id'],  # ps.id
                'series_number': file_info['series_number'],  # pser.dicom_number
                'instance_number': file_info['instance_number'],  # pi.dicom_number
                'file_path': file_info['file_path'],  # pif.file (file path)
                'field_created_at': file_info['field_created_at'],  # prf.created_at (latest)
                'file_url': file_info['file_url'],  # DICOM file URL
                'report_value': None,  # Will be set below
            }
            
            # Get report field values for this study (report fields are study-level, not file-level)
            study_id = file_info['study_id']
            report_values = report_fields_by_study.get(study_id, [])
            if report_values:
                # Combine all report field values for this study
                csv_row['report_value'] = ' | '.join([rv['value'] for rv in report_values if rv['value']])
            else:
                csv_row['report_value'] = None
            
            # Download DICOM file if file_url exists
            file_url = file_info['file_url']
            if file_url:
                try:
                    # Generate a safe filename
                    study_id = str(file_info['study_id'])
                    series_id = str(file_info['series_id'])
                    instance_id = str(file_info['instance_id'])
                    # Extract filename from file_path or file_url
                    file_path = file_info['file_path'] or ''
                    file_url = file_info['file_url'] or ''
                    if file_path:
                        file_name = os.path.basename(file_path) or 'file.dcm'
                    elif file_url:
                        file_name = os.path.basename(file_url.split('?')[0]) or 'file.dcm'
                    else:
                        file_name = 'file.dcm'
                    
                    # Create directory structure: study_id/series_id/instance_id/
                    file_dir = os.path.join(
                        self.output_dir,
                        'dicom_files',
                        study_id,
                        series_id,
                        instance_id
                    )
                    local_file_path = os.path.join(file_dir, file_name)
                    
                    if self.download_file(file_url, local_file_path):
                        csv_row['local_file_path'] = local_file_path
                        csv_row['downloaded'] = True
                        self.downloaded_files.append(local_file_path)
                        file_counter += 1
                    else:
                        csv_row['local_file_path'] = None
                        csv_row['downloaded'] = False
                except Exception as e:
                    logger.error(f"Error processing file_url {file_url}: {str(e)}")
                    csv_row['local_file_path'] = None
                    csv_row['downloaded'] = False
            else:
                csv_row['local_file_path'] = None
                csv_row['downloaded'] = False
            
            csv_rows.append(csv_row)
        
        logger.info(f"Processed {len(csv_rows)} rows, downloaded {file_counter} files")
        return csv_rows
    
    def save_to_csv(self, csv_rows: List[Dict]):
        """Save extracted data to CSV file."""
        logger.info(f"Saving {len(csv_rows)} rows to CSV: {self.csv_path}")
        
        if not csv_rows:
            logger.warning("No rows to save to CSV")
            return
        
        # Get all unique keys from all rows
        fieldnames = set()
        for row in csv_rows:
            fieldnames.update(row.keys())
        
        fieldnames = sorted(fieldnames)
        
        with open(self.csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
        
        logger.info(f"CSV saved to {self.csv_path}")
    
    def upload_to_gcs(self):
        """Upload CSV file and downloaded DICOM files to GCS bucket."""
        logger.info("Uploading files to GCS...")
        
        # Upload CSV file to: gs://ai-training-dev/eden-dataset-vlms/dicom-files/sample-test/dicom-reports-extracted-sample.csv
        csv_gcs_path = "eden-dataset-vlms/dicom-files/sample-test/dicom-reports-extracted-sample.csv"
        logger.info(f"Uploading CSV to gs://{self.gcs_service.destination_bucket_name}/{csv_gcs_path}")
        
        try:
            # Use gsutil to upload
            import subprocess
            gsutil_path = "/usr/local/google-cloud-sdk/bin/gsutil"
            if not os.path.exists(gsutil_path):
                import shutil
                gsutil_path = shutil.which("gsutil") or "gsutil"
            
            destination = f"gs://{self.gcs_service.destination_bucket_name}/{csv_gcs_path}"
            cmd = [gsutil_path, "cp", self.csv_path, destination]
            
            env = os.environ.copy()
            result = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
            logger.info(f"CSV uploaded successfully to {destination}")
        except Exception as e:
            logger.error(f"Failed to upload CSV: {str(e)}")
            raise
        
        # Upload DICOM files to: gs://ai-training-dev/eden-dataset-vlms/dicom-files/sample-test/
        dicom_files_dir = os.path.join(self.output_dir, 'dicom_files')
        if os.path.exists(dicom_files_dir):
            logger.info(f"Uploading DICOM files from {dicom_files_dir}")
            
            try:
                import subprocess
                gsutil_path = "/usr/local/google-cloud-sdk/bin/gsutil"
                if not os.path.exists(gsutil_path):
                    import shutil
                    gsutil_path = shutil.which("gsutil") or "gsutil"
                
                # Upload directory recursively to the specified path
                destination = f"gs://{self.gcs_service.destination_bucket_name}/eden-dataset-vlms/dicom-files/sample-test/"
                # Use -r flag to upload directory recursively
                cmd = [gsutil_path, "-m", "cp", "-r", dicom_files_dir, destination]
                
                env = os.environ.copy()
                result = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
                logger.info(f"DICOM files uploaded successfully to {destination}")
            except Exception as e:
                logger.error(f"Failed to upload DICOM files: {str(e)}")
                raise
        else:
            logger.warning(f"DICOM files directory not found: {dicom_files_dir}")
    
    def cleanup(self):
        """Clean up temporary files."""
        import shutil
        if os.path.exists(self.output_dir):
            logger.info(f"Cleaning up temporary directory: {self.output_dir}")
            shutil.rmtree(self.output_dir)


def main():
    """Main execution function."""
    logger.info("Starting DICOM and reports extraction process")
    
    bridge = None
    extractor = None
    
    try:
        # Initialize services
        bridge = DatabaseBridge()
        gcs_service = GCSService()
        extractor = DICOMReportsExtractor(gcs_service)
        
        # Fetch query results
        results = extractor.fetch_query_results(bridge)
        
        if not results:
            logger.warning("No results returned from query")
            return
        
        # Extract and process
        csv_rows = extractor.extract_and_process(results)
        
        # Save to CSV
        extractor.save_to_csv(csv_rows)
        
        # Upload to GCS
        extractor.upload_to_gcs()
        
        logger.info("=" * 70)
        logger.info("EXTRACTION AND UPLOAD COMPLETE")
        logger.info("=" * 70)
        logger.info(f"CSV file: {extractor.csv_path}")
        logger.info(f"Downloaded files: {len(extractor.downloaded_files)}")
        logger.info(f"CSV rows: {len(csv_rows)}")
        
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


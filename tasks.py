"""This file contains the tasks that will be executed by celery."""

import logging

from celery import group
from celery_app import app
from sync.database_breach import DatabaseBridge
from sync.dicom_pipeline import StudyDiscovery, StudyProcessor

logger = logging.getLogger(__name__)


@app.task
def discover_chest_dicom_studies(limit: int = None) -> dict:
    """Discover eligible chest DICOM studies and queue them for processing.

    This task:
    1. Finds eligible studies (CR/DX modalities, chest body part)
    2. Queues processing tasks for each study

    :param limit: Optional limit on number of studies to process
    :return: Dictionary with discovery results and queued study count
    """
    bridge = DatabaseBridge()
    try:
        discovery = StudyDiscovery(bridge)
        
        # Get eligible studies (CR/DX modalities, chest body parts)
        studies = discovery.get_eligible_studies(limit=limit)
        logger.info(f"Found {len(studies)} eligible studies")
        
        if not studies:
            logger.info("No eligible studies found for processing")
            return {
                "success": True,
                "studies_found": 0,
                "studies_queued": 0,
            }
        
        # Queue processing tasks for each study
        # Using group for parallel processing
        job = group(
            process_study_to_gcs.s(
                study_id=str(study["study_id"]),
                study_data=dict(study),
            )
            for study in studies
        )
        
        result = job.apply_async()
        
        logger.info(
            f"Queued {len(studies)} studies for processing "
            f"(task group: {result.id})"
        )
        
        return {
            "success": True,
            "studies_found": len(studies),
            "studies_queued": len(studies),
            "task_group_id": result.id,
        }
    except Exception as e:
        logger.error(f"Error in discover_chest_dicom_studies: {str(e)}")
        return {
            "success": False,
            "error": str(e),
        }
    finally:
        bridge.close_connections()


@app.task
def process_study_to_gcs(study_id: str, study_data: dict = None) -> dict:
    """Process a single study: extract DICOM files and reports, upload to GCS.

    :param study_id: Study UUID as string
    :param study_data: Optional study data dictionary (if not provided, will fetch)
    :return: Processing result dictionary
    """
    bridge = DatabaseBridge()
    try:
        processor = StudyProcessor(bridge)
        
        if study_data:
            # Use provided study data
            result = processor.process_study(study_data)
        else:
            # Fetch study data by ID
            result = processor.process_study_by_id(study_id)
        
        logger.info(
            f"Study {study_id} processing completed: "
            f"success={result.get('success')}, "
            f"dicom_files={result.get('dicom_files_uploaded', 0)}, "
            f"reports={result.get('reports_uploaded', 0)}"
        )
        
        return result
    except Exception as e:
        logger.error(f"Error processing study {study_id}: {str(e)}")
        return {
            "study_id": study_id,
            "success": False,
            "error": str(e),
        }
    finally:
        bridge.close_connections()


@app.task
def process_study_batch_to_gcs(study_ids: list) -> dict:
    """Process a batch of studies in parallel.

    :param study_ids: List of study UUID strings
    :return: Dictionary with batch processing results
    """
    logger.info(f"Processing batch of {len(study_ids)} studies")
    
    # Create a group of tasks
    job = group(
        process_study_to_gcs.s(study_id=study_id) for study_id in study_ids
    )
    
    result = job.apply_async()
    
    return {
        "success": True,
        "studies_count": len(study_ids),
        "task_group_id": result.id,
    }


@app.task
def download_dicom_file(file_url: str, local_file_path: str) -> dict:
    """Download a single DICOM file from URL to local path.

    :param file_url: URL of the DICOM file to download
    :param local_file_path: Local path where the file should be saved
    :return: Dictionary with download result
    """
    import os
    import requests
    
    try:
        logger.info(f"Downloading {file_url} to {local_file_path}")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        # Download file
        response = requests.get(file_url, stream=True, timeout=60)
        response.raise_for_status()
        
        with open(local_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        file_size = os.path.getsize(local_file_path)
        logger.info(f"Downloaded {file_url} -> {local_file_path} ({file_size} bytes)")
        
        return {
            "success": True,
            "file_url": file_url,
            "local_file_path": local_file_path,
            "file_size": file_size,
        }
    except Exception as e:
        logger.error(f"Failed to download {file_url}: {str(e)}")
        return {
            "success": False,
            "file_url": file_url,
            "local_file_path": local_file_path,
            "error": str(e),
        }


@app.task
def convert_dicom_to_jpg(dicom_path: str, jpg_path: str, instance_id: str) -> dict:
    """Convert a DICOM file to JPG using dcm2jpg.

    :param dicom_path: Path to the DICOM file
    :param jpg_path: Path where the JPG file should be saved
    :param instance_id: Instance ID for the file
    :return: Dictionary with conversion result
    """
    import os
    import subprocess
    import tempfile
    import shutil
    
    try:
        if not os.path.exists(dicom_path):
            logger.error(f"DICOM file not found: {dicom_path}")
            return {
                "success": False,
                "dicom_path": dicom_path,
                "jpg_path": jpg_path,
                "error": "DICOM file not found",
            }
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(jpg_path), exist_ok=True)
        
        # Use temporary file for output as specified
        temp_output = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
        temp_output_path = temp_output.name
        temp_output.close()
        
        # Find dcm2jpg - try multiple possible paths
        dcm2jpg_path = None
        for path in ["/usr/local/dcm2jpg/bin/dcm2jpg.sh", "/usr/local/bin/dcm2jpg", "dcm2jpg", "dcm2jpg.sh"]:
            if path in ["dcm2jpg", "dcm2jpg.sh"]:
                found = shutil.which(path)
                if found:
                    dcm2jpg_path = found
                    break
            elif os.path.exists(path):
                dcm2jpg_path = path
                break
        
        if not dcm2jpg_path:
            dcm2jpg_path = "dcm2jpg"  # Fallback
        
        # Run dcm2jpg with quality 1.0
        # Command: dcm2jpg -q 1.0 /path/to/input/file.dcm /tmp/output_tmp_file_<>.jpg
        cmd = [dcm2jpg_path, "-q", "1.0", dicom_path, temp_output_path]
        
        logger.info(f"Converting {dicom_path} to JPG...")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
        )
        
        # Move temp file to final location
        if os.path.exists(temp_output_path):
            shutil.move(temp_output_path, jpg_path)
            file_size = os.path.getsize(jpg_path)
            logger.info(f"Converted {dicom_path} -> {jpg_path} ({file_size} bytes)")
            
            return {
                "success": True,
                "dicom_path": dicom_path,
                "jpg_path": jpg_path,
                "instance_id": instance_id,
                "file_size": file_size,
            }
        else:
            logger.error(f"Conversion failed: output file not created")
            return {
                "success": False,
                "dicom_path": dicom_path,
                "jpg_path": jpg_path,
                "error": "Output file not created",
            }
            
    except subprocess.CalledProcessError as e:
        logger.error(f"dcm2jpg error converting {dicom_path}: {e.stderr or e.stdout or str(e)}")
        # Clean up temp file if it exists
        if 'temp_output_path' in locals() and os.path.exists(temp_output_path):
            os.unlink(temp_output_path)
        return {
            "success": False,
            "dicom_path": dicom_path,
            "jpg_path": jpg_path,
            "error": e.stderr or e.stdout or str(e),
        }
    except Exception as e:
        logger.error(f"Unexpected error converting {dicom_path}: {str(e)}")
        # Clean up temp file if it exists
        if 'temp_output_path' in locals() and os.path.exists(temp_output_path):
            os.unlink(temp_output_path)
        return {
            "success": False,
            "dicom_path": dicom_path,
            "jpg_path": jpg_path,
            "error": str(e),
        }


@app.task
def upload_file_to_gcs(local_file_path: str, gcs_path: str, bucket_name: str) -> dict:
    """Upload a file to Google Cloud Storage.

    :param local_file_path: Local path of the file to upload
    :param gcs_path: GCS path (without gs:// prefix)
    :param bucket_name: GCS bucket name
    :return: Dictionary with upload result
    """
    import os
    import subprocess
    import shutil
    
    try:
        if not os.path.exists(local_file_path):
            logger.error(f"File not found: {local_file_path}")
            return {
                "success": False,
                "local_file_path": local_file_path,
                "error": "File not found",
            }
        
        # Find gsutil
        gsutil_path = None
        for path in ["/usr/local/google-cloud-sdk/bin/gsutil", "/root/google-cloud-sdk/bin/gsutil", "gsutil"]:
            if os.path.exists(path) if not path == "gsutil" else shutil.which(path):
                gsutil_path = path if path != "gsutil" else shutil.which(path)
                break
        
        if not gsutil_path:
            gsutil_path = "gsutil"
        
        destination = f"gs://{bucket_name}/{gcs_path}"
        cmd = [gsutil_path, "cp", local_file_path, destination]
        
        env = os.environ.copy()
        result = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
        
        logger.info(f"Uploaded {local_file_path} to {destination}")
        return {
            "success": True,
            "local_file_path": local_file_path,
            "gcs_path": gcs_path,
            "destination": destination,
        }
    except Exception as e:
        logger.error(f"Failed to upload {local_file_path} to GCS: {str(e)}")
        return {
            "success": False,
            "local_file_path": local_file_path,
            "gcs_path": gcs_path,
            "error": str(e),
        }

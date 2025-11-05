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
    1. Queries for doctors ranked 2-6 by total signed chest studies
    2. Finds eligible studies (CR/DX modalities, chest body part)
    3. Queues processing tasks for each study

    :param limit: Optional limit on number of studies to process
    :return: Dictionary with discovery results and queued study count
    """
    bridge = DatabaseBridge()
    try:
        discovery = StudyDiscovery(bridge)
        
        # Get ranked doctors
        doctors = discovery.get_ranked_doctors()
        logger.info(f"Found {len(doctors)} ranked doctors")
        
        # Get eligible studies
        studies = discovery.get_eligible_studies(limit=limit)
        logger.info(f"Found {len(studies)} eligible studies")
        
        if not studies:
            logger.info("No eligible studies found for processing")
            return {
                "success": True,
                "doctors_found": len(doctors),
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
            "doctors_found": len(doctors),
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

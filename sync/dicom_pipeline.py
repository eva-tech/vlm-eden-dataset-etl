"""DICOM extraction pipeline for chest studies."""

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID

from psycopg2 import extras

from queries.chest_dicom_studies import (
    get_dicom_images_for_study,
    get_eligible_chest_studies,
    get_report_for_study,
    get_ranked_doctors,
    get_series_for_study,
)
from sync.database_breach import DatabaseBridge
from sync.gcs_service import GCSService

logger = logging.getLogger(__name__)


class StudyDiscovery:
    """Discovers eligible chest DICOM studies for extraction."""

    def __init__(self, bridge: DatabaseBridge):
        """Initialize study discovery.

        :param bridge: Database bridge for database connections
        """
        self.bridge = bridge
        self.source_cursor = bridge.new_cursor(bridge.source_conn)

    def get_ranked_doctors(self) -> List[Dict]:
        """Get list of doctors ranked by total signed chest studies.

        :return: List of doctor dictionaries with rank information
        """
        try:
            self.source_cursor.execute(get_ranked_doctors)
            doctors = self.source_cursor.fetchall()
            logger.info(f"Found {len(doctors)} ranked doctors")
            return doctors
        except Exception as e:
            logger.error(f"Error fetching ranked doctors: {str(e)}")
            return []

    def get_eligible_studies(
        self, limit: Optional[int] = None
    ) -> List[Dict]:
        """Get eligible chest DICOM studies (CR and DX modalities, chest body parts).

        :param limit: Optional limit on number of studies to return
        :return: List of eligible study dictionaries
        """
        try:
            query = get_eligible_chest_studies
            if limit:
                # Remove trailing semicolon and whitespace, then add LIMIT
                query = query.rstrip().rstrip(';')
                query = f"{query} LIMIT {limit}"

            self.source_cursor.execute(query)
            studies = self.source_cursor.fetchall()
            logger.info(
                f"Found {len(studies)} eligible chest studies for extraction"
            )
            return studies
        except Exception as e:
            logger.error(f"Error fetching eligible studies: {str(e)}")
            return []

    def get_study_count(self) -> int:
        """Get count of eligible studies.

        :return: Count of eligible studies
        """
        try:
            self.source_cursor.execute(
                f"SELECT COUNT(*) as count FROM ({get_eligible_chest_studies}) as subquery"
            )
            result = self.source_cursor.fetchone()
            return result["count"] if result else 0
        except Exception as e:
            logger.error(f"Error getting study count: {str(e)}")
            return 0


class StudyProcessor:
    """Processes individual studies for DICOM extraction and GCS upload."""

    def __init__(
        self, bridge: DatabaseBridge, gcs_service: Optional[GCSService] = None
    ):
        """Initialize study processor.

        :param bridge: Database bridge for database connections
        :param gcs_service: Optional GCS service (creates new one if not provided)
        """
        self.bridge = bridge
        self.source_cursor = bridge.new_cursor(bridge.source_conn)
        self.gcs_service = gcs_service or GCSService()

    def process_study(self, study_data: Dict) -> Dict:
        """Process a single study: extract DICOM files and reports, upload to GCS.

        :param study_data: Study data dictionary from discovery
        :return: Processing result dictionary with success status and details
        """
        study_id = study_data["study_id"]
        organization_id = str(study_data["organization_id"])

        logger.info(
            f"Processing study {study_id} for organization {organization_id}"
        )

        result = {
            "study_id": str(study_id),
            "organization_id": organization_id,
            "success": False,
            "dicom_files_uploaded": 0,
            "reports_uploaded": 0,
            "metadata_uploaded": False,
            "errors": [],
        }

        try:
            # Upload metadata
            if self._upload_study_metadata(study_data, result):
                result["metadata_uploaded"] = True

            # Upload report
            if self._upload_report(study_data, result):
                result["reports_uploaded"] = 1

            # Upload DICOM files
            dicom_count = self._upload_dicom_files(study_data, result)
            result["dicom_files_uploaded"] = dicom_count

            # Mark as successful if at least metadata or report was uploaded
            result["success"] = (
                result["metadata_uploaded"] or result["reports_uploaded"] > 0
            )

            logger.info(
                f"Completed processing study {study_id}: "
                f"{result['dicom_files_uploaded']} DICOM files, "
                f"{result['reports_uploaded']} reports"
            )

        except Exception as e:
            error_msg = f"Error processing study {study_id}: {str(e)}"
            logger.error(error_msg)
            result["errors"].append(error_msg)

        return result

    def _upload_study_metadata(
        self, study_data: Dict, result: Dict
    ) -> bool:
        """Upload study metadata to GCS.

        :param study_data: Study data dictionary
        :param result: Result dictionary to update
        :return: True if successful
        """
        try:
            study_id = str(study_data["study_id"])
            organization_id = str(study_data["organization_id"])

            # Prepare metadata (convert UUIDs to strings for JSON serialization)
            metadata = {
                "study_id": study_id,
                "organization_id": organization_id,
                "organization_slug": study_data.get("organization_slug"),
                "modalities": study_data.get("modalities"),
                "dicom_description": study_data.get("dicom_description"),
                "created_at": study_data.get("created_at").isoformat()
                if study_data.get("created_at")
                else None,
                "updated_at": study_data.get("updated_at").isoformat()
                if study_data.get("updated_at")
                else None,
                "dicom_date_time": study_data.get("dicom_date_time").isoformat()
                if study_data.get("dicom_date_time")
                else None,
                "report_id": str(study_data.get("report_id")),
                "signed_at": study_data.get("signed_at").isoformat()
                if study_data.get("signed_at")
                else None,
                "signed_by_practitioner_id": str(
                    study_data.get("signed_by_practitioner_id")
                ),
                "signed_by_user_id": str(study_data.get("signed_by_user_id")),
                "signed_by_name": study_data.get("signed_by_name"),
                "signed_by_email": study_data.get("signed_by_email"),
                "body_part_name": study_data.get("body_part_name"),
                "body_part_identifier": study_data.get("body_part_identifier"),
            }

            return self.gcs_service.upload_report_metadata(
                metadata, organization_id, study_id
            )
        except Exception as e:
            error_msg = f"Error uploading metadata: {str(e)}"
            logger.error(error_msg)
            result["errors"].append(error_msg)
            return False

    def _upload_report(self, study_data: Dict, result: Dict) -> bool:
        """Upload report to GCS.

        :param study_data: Study data dictionary
        :param result: Result dictionary to update
        :return: True if successful
        """
        try:
            study_id = str(study_data["study_id"])
            organization_id = str(study_data["organization_id"])
            report_id = str(study_data.get("report_id", ""))
            report_content = study_data.get("report_content", "")

            if not report_content:
                logger.warning(
                    f"No report content found for study {study_id}"
                )
                return False

            signed_at = None
            if study_data.get("signed_at"):
                signed_at = study_data["signed_at"].isoformat()

            return self.gcs_service.upload_report(
                report_content,
                organization_id,
                study_id,
                report_id,
                signed_at,
            )
        except Exception as e:
            error_msg = f"Error uploading report: {str(e)}"
            logger.error(error_msg)
            result["errors"].append(error_msg)
            return False

    def _upload_dicom_files(self, study_data: Dict, result: Dict) -> int:
        """Upload DICOM files for a study to GCS.

        :param study_data: Study data dictionary
        :param result: Result dictionary to update
        :return: Number of files successfully uploaded
        """
        study_id = study_data["study_id"]
        organization_id = str(study_data["organization_id"])
        uploaded_count = 0

        try:
            # First try to get DICOM images directly
            self.source_cursor.execute(
                get_dicom_images_for_study, {"study_id": study_id}
            )
            dicom_images = self.source_cursor.fetchall()

            if not dicom_images:
                # If no direct images, try to get series information
                logger.info(
                    f"No direct DICOM images found for study {study_id}, "
                    "trying series information"
                )
                self.source_cursor.execute(
                    get_series_for_study, {"study_id": study_id}
                )
                series_list = self.source_cursor.fetchall()

                if not series_list:
                    logger.warning(
                        f"No DICOM series found for study {study_id}"
                    )
                    return 0

                # If we have series but no images, log a warning
                # The actual file extraction would need to be implemented
                # based on how DICOM files are stored in your system
                logger.warning(
                    f"Found {len(series_list)} series for study {study_id}, "
                    "but DICOM file extraction needs to be implemented "
                    "based on your storage system"
                )
                return 0

            # Process each DICOM image
            for image_data in dicom_images:
                file_path = image_data.get("file_path")

                if not file_path:
                    logger.warning(
                        f"No file path found for image {image_data.get('image_id')}"
                    )
                    continue

                # Check if file exists locally
                if not os.path.exists(file_path):
                    logger.warning(f"DICOM file not found at path: {file_path}")
                    # Note: If files are in a different location or storage,
                    # you may need to implement custom file retrieval logic here
                    continue

                # Upload to GCS
                success = self.gcs_service.upload_dicom_file(
                    file_path,
                    organization_id,
                    str(study_id),
                    str(image_data.get("series_id", "")),
                    str(image_data.get("image_id", "")),
                    image_data.get("instance_number"),
                )

                if success:
                    uploaded_count += 1
                else:
                    error_msg = (
                        f"Failed to upload DICOM file: {file_path}"
                    )
                    result["errors"].append(error_msg)

            logger.info(
                f"Uploaded {uploaded_count} DICOM files for study {study_id}"
            )

        except Exception as e:
            error_msg = f"Error uploading DICOM files: {str(e)}"
            logger.error(error_msg)
            result["errors"].append(error_msg)

        return uploaded_count

    def process_study_by_id(self, study_id: str) -> Dict:
        """Process a study by ID (fetch study data first).

        :param study_id: Study UUID
        :return: Processing result dictionary
        """
        try:
            # Use the same query but filter by study_id
            # The query already has ORDER BY, so we add WHERE before it
            query = get_eligible_chest_studies.replace(
                "ORDER BY",
                "AND ps.id = %(study_id)s::uuid ORDER BY"
            )
            self.source_cursor.execute(
                query,
                {"study_id": study_id},
            )
            study_data = self.source_cursor.fetchone()

            if not study_data:
                return {
                    "study_id": study_id,
                    "success": False,
                    "error": "Study not found or not eligible",
                }

            return self.process_study(study_data)
        except Exception as e:
            logger.error(f"Error processing study by ID {study_id}: {str(e)}")
            return {
                "study_id": study_id,
                "success": False,
                "error": str(e),
            }

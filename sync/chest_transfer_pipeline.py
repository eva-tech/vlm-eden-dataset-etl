"""Pipeline to transfer chest CR/DX DICOM files and reports from S3 to GCS."""

import logging
from typing import Dict, List

from sync.database_breach import DatabaseBridge
from sync.gcs_service import GCSService
from queries.get_chest_dicom_files_and_reports import (
    get_chest_dicom_files,
    get_chest_reports,
)

logger = logging.getLogger(__name__)


class ChestTransferPipeline:
    """Pipeline to transfer chest CR/DX DICOM files and reports from S3 to GCS."""

    def __init__(self, bridge: DatabaseBridge = None, gcs_service: GCSService = None):
        """Initialize the transfer pipeline.

        :param bridge: Database bridge (creates new one if not provided)
        :param gcs_service: GCS service (creates new one if not provided)
        """
        self.bridge = bridge or DatabaseBridge()
        self.gcs_service = gcs_service or GCSService()
        self.source_cursor = self.bridge.new_cursor(self.bridge.source_conn)

    def transfer_all_chest_data(self) -> Dict:
        """Transfer all chest CR/DX DICOM files and reports from S3 to GCS.

        :return: Dictionary with transfer results
        """
        logger.info("Starting chest CR/DX data transfer from S3 to GCS")

        result = {
            "success": True,
            "dicom_files_processed": 0,
            "dicom_files_transferred": 0,
            "reports_processed": 0,
            "reports_transferred": 0,
            "studies_processed": set(),
            "errors": [],
        }

        try:
            # Get all DICOM files
            logger.info("Fetching chest CR/DX DICOM files...")
            self.source_cursor.execute(get_chest_dicom_files)
            dicom_files = self.source_cursor.fetchall()
            logger.info(f"Found {len(dicom_files)} DICOM files to transfer")

            # Process each DICOM file
            for file_data in dicom_files:
                result["dicom_files_processed"] += 1
                result["studies_processed"].add(str(file_data["study_id"]))

                try:
                    success = self._transfer_dicom_file(file_data)
                    if success:
                        result["dicom_files_transferred"] += 1
                    else:
                        result["errors"].append(
                            f"Failed to transfer DICOM file {file_data['file_id']}"
                        )
                except Exception as e:
                    error_msg = f"Error transferring DICOM file {file_data['file_id']}: {str(e)}"
                    logger.error(error_msg)
                    result["errors"].append(error_msg)

            # Get all reports
            logger.info("Fetching chest CR/DX reports...")
            self.source_cursor.execute(get_chest_reports)
            reports = self.source_cursor.fetchall()
            logger.info(f"Found {len(reports)} reports to transfer")

            # Process each report
            for report_data in reports:
                result["reports_processed"] += 1
                result["studies_processed"].add(str(report_data["study_id"]))

                try:
                    success = self._transfer_report(report_data)
                    if success:
                        result["reports_transferred"] += 1
                    else:
                        result["errors"].append(
                            f"Failed to transfer report {report_data['report_id']}"
                        )
                except Exception as e:
                    error_msg = f"Error transferring report {report_data['report_id']}: {str(e)}"
                    logger.error(error_msg)
                    result["errors"].append(error_msg)

            result["studies_processed"] = len(result["studies_processed"])

            logger.info(
                f"Transfer completed: {result['dicom_files_transferred']}/{result['dicom_files_processed']} "
                f"DICOM files, {result['reports_transferred']}/{result['reports_processed']} reports, "
                f"{result['studies_processed']} unique studies"
            )

        except Exception as e:
            error_msg = f"Error in transfer pipeline: {str(e)}"
            logger.error(error_msg)
            result["success"] = False
            result["errors"].append(error_msg)

        return result

    def _transfer_dicom_file(self, file_data: Dict) -> bool:
        """Transfer a single DICOM file from S3 to GCS.

        :param file_data: Dictionary with file information from query
        :return: True if successful, False otherwise
        """
        study_id = str(file_data["study_id"])
        organization_id = str(file_data["organization_id"])
        series_id = str(file_data["series_id"])
        instance_id = str(file_data["instance_id"])
        file_path = file_data["file_path"]
        instance_number = file_data.get("instance_number")

        logger.debug(
            f"Transferring DICOM file: study={study_id}, series={series_id}, "
            f"instance={instance_id}, file={file_path}"
        )

        # The file_path from the database is the S3 key
        # Use it directly as the source S3 key
        s3_key = file_path.lstrip("/")

        # Upload using the GCS service
        # The service will handle S3 to GCS copying
        return self.gcs_service.upload_dicom_file(
            dicom_path=s3_key,  # Pass S3 key as the path
            organization_id=organization_id,
            study_id=study_id,
            series_id=series_id,
            image_id=instance_id,
            instance_number=instance_number,
        )

    def _transfer_report(self, report_data: Dict) -> bool:
        """Transfer a single report from S3 to GCS.

        :param report_data: Dictionary with report information from query
        :return: True if successful, False otherwise
        """
        study_id = str(report_data["study_id"])
        organization_id = str(report_data["organization_id"])
        report_id = str(report_data["report_id"])
        pdf_file = report_data.get("report_pdf_file")
        signed_at = report_data.get("signed_at")

        logger.debug(
            f"Transferring report: study={study_id}, report={report_id}, "
            f"pdf_file={pdf_file}"
        )

        # If there's a PDF file, it might be an S3 key
        # Otherwise, we'll need to handle report content differently
        if pdf_file:
            # PDF file path might be an S3 key
            s3_key = pdf_file.lstrip("/")
            # Use the upload_report method with the S3 key as content
            # The service will handle S3 to GCS copying
            signed_at_str = signed_at.isoformat() if signed_at else None
            return self.gcs_service.upload_report(
                report_content=s3_key,  # Pass S3 key as content
                organization_id=organization_id,
                study_id=study_id,
                report_id=report_id,
                signed_at=signed_at_str,
            )
        else:
            logger.warning(f"No PDF file found for report {report_id}")
            return False

    def close(self):
        """Close database connections."""
        self.bridge.close_connections()


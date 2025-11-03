"""S3 service for uploading DICOM files and reports to S3."""

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


class S3Service:
    """Service for handling S3 operations."""

    def __init__(
        self,
        bucket_name: str = None,
        region_name: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ):
        """Initialize S3 service.

        :param bucket_name: S3 bucket name (defaults to s3-vlms-datasets)
        :param region_name: AWS region name
        :param aws_access_key_id: AWS access key ID
        :param aws_secret_access_key: AWS secret access key
        """
        self.bucket_name = bucket_name or os.getenv(
            "S3_BUCKET_NAME", "s3-vlms-datasets"
        )
        self.region_name = region_name or os.getenv("AWS_REGION_NAME", "us-east-1")

        # Initialize S3 client
        try:
            self.s3_client = boto3.client(
                "s3",
                region_name=self.region_name,
                aws_access_key_id=aws_access_key_id
                or os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=aws_secret_access_key
                or os.getenv("AWS_SECRET_ACCESS_KEY"),
            )
            logger.info(f"S3 service initialized for bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {str(e)}")
            raise

    def upload_file(
        self,
        file_path: str,
        s3_key: str,
        metadata: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Upload a file to S3.

        :param file_path: Local file path to upload
        :param s3_key: S3 object key (path)
        :param metadata: Optional metadata to attach to the object
        :return: True if successful, False otherwise
        """
        try:
            extra_args = {}
            if metadata:
                extra_args["Metadata"] = metadata

            self.s3_client.upload_file(
                file_path,
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args,
            )
            logger.info(f"Successfully uploaded {file_path} to s3://{self.bucket_name}/{s3_key}")
            return True
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return False
        except ClientError as e:
            logger.error(f"AWS error uploading {file_path}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading {file_path}: {str(e)}")
            return False

    def upload_fileobj(
        self,
        file_obj,
        s3_key: str,
        metadata: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Upload a file-like object to S3.

        :param file_obj: File-like object to upload
        :param s3_key: S3 object key (path)
        :param metadata: Optional metadata to attach to the object
        :return: True if successful, False otherwise
        """
        try:
            extra_args = {}
            if metadata:
                extra_args["Metadata"] = metadata

            self.s3_client.upload_fileobj(
                file_obj,
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args,
            )
            logger.info(
                f"Successfully uploaded file object to s3://{self.bucket_name}/{s3_key}"
            )
            return True
        except ClientError as e:
            logger.error(f"AWS error uploading file object: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading file object: {str(e)}")
            return False

    def upload_bytes(
        self,
        data: bytes,
        s3_key: str,
        metadata: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Upload bytes data to S3.

        :param data: Bytes data to upload
        :param s3_key: S3 object key (path)
        :param metadata: Optional metadata to attach to the object
        :return: True if successful, False otherwise
        """
        try:
            import io

            file_obj = io.BytesIO(data)
            return self.upload_fileobj(file_obj, s3_key, metadata)
        except Exception as e:
            logger.error(f"Error uploading bytes: {str(e)}")
            return False

    def upload_dicom_file(
        self,
        dicom_path: str,
        organization_id: str,
        study_id: str,
        series_id: str,
        image_id: str,
        instance_number: Optional[int] = None,
    ) -> bool:
        """Upload a DICOM file to S3 with organized path structure.

        :param dicom_path: Local path to DICOM file
        :param organization_id: Organization UUID
        :param study_id: Study UUID
        :param series_id: Series ID
        :param image_id: Image ID
        :param instance_number: Optional instance number
        :return: True if successful, False otherwise
        """
        # Construct S3 key: organization_id/study_id/dicom_files/series_id/image_id.dcm
        filename = f"{image_id}.dcm"
        if instance_number is not None:
            filename = f"{instance_number:04d}_{filename}"

        s3_key = f"{organization_id}/{study_id}/dicom_files/{series_id}/{filename}"

        metadata = {
            "organization_id": organization_id,
            "study_id": study_id,
            "series_id": series_id,
            "image_id": image_id,
        }
        if instance_number is not None:
            metadata["instance_number"] = str(instance_number)

        return self.upload_file(dicom_path, s3_key, metadata)

    def upload_report(
        self,
        report_content: str,
        organization_id: str,
        study_id: str,
        report_id: str,
        signed_at: Optional[str] = None,
    ) -> bool:
        """Upload a report to S3.

        :param report_content: Report content (text/HTML)
        :param organization_id: Organization UUID
        :param study_id: Study UUID
        :param report_id: Report ID
        :param signed_at: Optional signed timestamp
        :return: True if successful, False otherwise
        """
        # Construct S3 key: organization_id/study_id/reports/report_id.txt
        s3_key = f"{organization_id}/{study_id}/reports/{report_id}.txt"

        metadata = {
            "organization_id": organization_id,
            "study_id": study_id,
            "report_id": report_id,
        }
        if signed_at:
            metadata["signed_at"] = signed_at

        return self.upload_bytes(
            report_content.encode("utf-8"), s3_key, metadata
        )

    def upload_report_metadata(
        self,
        study_data: Dict,
        organization_id: str,
        study_id: str,
    ) -> bool:
        """Upload study metadata as JSON to S3.

        :param study_data: Dictionary containing study metadata
        :param organization_id: Organization UUID
        :param study_id: Study UUID
        :return: True if successful, False otherwise
        """
        try:
            import json

            s3_key = f"{organization_id}/{study_id}/metadata.json"
            json_data = json.dumps(study_data, indent=2, default=str).encode("utf-8")

            metadata = {
                "organization_id": organization_id,
                "study_id": study_id,
            }

            return self.upload_bytes(json_data, s3_key, metadata)
        except Exception as e:
            logger.error(f"Error uploading metadata: {str(e)}")
            return False

    def check_file_exists(self, s3_key: str) -> bool:
        """Check if a file exists in S3.

        :param s3_key: S3 object key
        :return: True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            logger.error(f"Error checking file existence: {str(e)}")
            return False

    def list_study_files(self, organization_id: str, study_id: str) -> List[str]:
        """List all files for a study in S3.

        :param organization_id: Organization UUID
        :param study_id: Study UUID
        :return: List of S3 keys
        """
        try:
            prefix = f"{organization_id}/{study_id}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )

            if "Contents" not in response:
                return []

            return [obj["Key"] for obj in response["Contents"]]
        except ClientError as e:
            logger.error(f"Error listing study files: {str(e)}")
            return []

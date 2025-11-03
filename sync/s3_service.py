"""S3 service for copying DICOM files and reports from origin bucket to destination bucket."""

import json
import logging
import os
import subprocess
from typing import Dict, List, Optional

from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


class S3Service:
    """Service for handling S3 operations using AWS CLI."""

    def __init__(
        self,
        origin_bucket_name: str = None,
        destination_bucket_name: str = None,
        region_name: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ):
        """Initialize S3 service.

        :param origin_bucket_name: Origin S3 bucket name (defaults to s3-origin-bucket_name)
        :param destination_bucket_name: Destination S3 bucket name (defaults to s3-bucket-name)
        :param region_name: AWS region name
        :param aws_access_key_id: AWS access key ID
        :param aws_secret_access_key: AWS secret access key
        """
        self.origin_bucket_name = origin_bucket_name or os.getenv(
            "S3_ORIGIN_BUCKET_NAME", "s3-origin-bucket_name"
        )
        self.destination_bucket_name = destination_bucket_name or os.getenv(
            "S3_BUCKET_NAME", "s3-bucket-name"
        )
        self.region_name = region_name or os.getenv("AWS_REGION_NAME", "us-east-1")

        # Set AWS credentials in environment if provided
        if aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID"):
            os.environ["AWS_ACCESS_KEY_ID"] = (
                aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
            )
        if aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY"):
            os.environ["AWS_SECRET_ACCESS_KEY"] = (
                aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
            )
        if self.region_name:
            os.environ["AWS_DEFAULT_REGION"] = self.region_name

        logger.info(
            f"S3 service initialized: {self.origin_bucket_name} -> {self.destination_bucket_name}"
        )

    def _copy_s3_object(
        self,
        source_key: str,
        destination_key: str,
        metadata: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Copy an object from origin bucket to destination bucket using AWS CLI.

        :param source_key: S3 object key in origin bucket
        :param destination_key: S3 object key in destination bucket
        :param metadata: Optional metadata (note: AWS CLI cp doesn't support metadata directly)
        :return: True if successful, False otherwise
        """
        try:
            source_path = f"s3://{self.origin_bucket_name}/{source_key}"
            destination_path = f"s3://{self.destination_bucket_name}/{destination_key}"

            # Build AWS CLI command
            cmd = ["aws", "s3", "cp", source_path, destination_path]

            # Add metadata if provided (using metadata-directive)
            if metadata:
                # Convert metadata dict to AWS CLI format
                metadata_str = ",".join([f"{k}={v}" for k, v in metadata.items()])
                cmd.extend(["--metadata", metadata_str])
                cmd.extend(["--metadata-directive", "REPLACE"])

            # Execute command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )

            logger.info(
                f"Successfully copied s3://{self.origin_bucket_name}/{source_key} "
                f"to s3://{self.destination_bucket_name}/{destination_key}"
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"AWS CLI error copying {source_key}: {e.stderr or e.stdout or str(e)}"
            )
            return False
        except Exception as e:
            logger.error(f"Unexpected error copying {source_key}: {str(e)}")
            return False

    def _check_s3_object_exists(self, bucket_name: str, s3_key: str) -> bool:
        """Check if an object exists in S3 bucket using AWS CLI.

        :param bucket_name: S3 bucket name
        :param s3_key: S3 object key
        :return: True if object exists, False otherwise
        """
        try:
            source_path = f"s3://{bucket_name}/{s3_key}"
            cmd = ["aws", "s3", "ls", source_path]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            return len(result.stdout.strip()) > 0
        except subprocess.CalledProcessError:
            return False
        except Exception as e:
            logger.error(f"Error checking object existence: {str(e)}")
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
        """Copy a DICOM file from origin bucket to destination bucket with organized path structure.

        If dicom_path is a local file, it will be uploaded to origin bucket first, then copied to destination.
        If dicom_path is an S3 key, it will be copied directly from origin to destination.

        :param dicom_path: Local file path or S3 key path in origin bucket
        :param organization_id: Organization UUID
        :param study_id: Study UUID
        :param series_id: Series ID
        :param image_id: Image ID
        :param instance_number: Optional instance number
        :return: True if successful, False otherwise
        """
        # Construct destination S3 key: organization_id/study_id/dicom_files/series_id/image_id.dcm
        filename = f"{image_id}.dcm"
        if instance_number is not None:
            filename = f"{instance_number:04d}_{filename}"

        destination_key = f"{organization_id}/{study_id}/dicom_files/{series_id}/{filename}"

        # Check if dicom_path is a local file or S3 key
        if dicom_path.startswith("s3://") or dicom_path.startswith(organization_id + "/"):
            # It's an S3 key, extract the key
            source_key = (
                dicom_path.replace(f"s3://{self.origin_bucket_name}/", "")
                .replace(f"s3://{self.destination_bucket_name}/", "")
                .lstrip("/")
            )
        elif os.path.exists(dicom_path):
            # It's a local file, upload to origin bucket first
            origin_key = f"{organization_id}/{study_id}/dicom_files/{series_id}/{filename}"
            source_path = f"s3://{self.origin_bucket_name}/{origin_key}"

            cmd = ["aws", "s3", "cp", dicom_path, source_path]
            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, check=True
                )
                source_key = origin_key
            except subprocess.CalledProcessError as e:
                logger.error(
                    f"Failed to upload {dicom_path} to origin bucket: {e.stderr or str(e)}"
                )
                return False
        else:
            # Try to construct source key from provided path
            # Assume it's already an S3 key format
            source_key = dicom_path.lstrip("/")

        metadata = {
            "organization_id": organization_id,
            "study_id": study_id,
            "series_id": series_id,
            "image_id": image_id,
        }
        if instance_number is not None:
            metadata["instance_number"] = str(instance_number)

        return self._copy_s3_object(source_key, destination_key, metadata)

    def upload_report(
        self,
        report_content: str,
        organization_id: str,
        study_id: str,
        report_id: str,
        signed_at: Optional[str] = None,
    ) -> bool:
        """Upload report content to origin bucket, then copy to destination bucket.

        If report_content is an S3 key (starts with 's3://' or organization_id), it will be copied directly.
        Otherwise, content will be written to a temp file and uploaded to origin first.

        :param report_content: Report content (text) or S3 key path
        :param organization_id: Organization UUID
        :param study_id: Study UUID
        :param report_id: Report ID
        :param signed_at: Optional signed timestamp
        :return: True if successful, False otherwise
        """
        # Construct destination S3 key: organization_id/study_id/reports/report_id.txt
        destination_key = f"{organization_id}/{study_id}/reports/{report_id}.txt"

        # Check if report_content is an S3 key/path
        if report_content.startswith("s3://") or report_content.startswith(
            organization_id + "/"
        ):
            # It's an S3 key, copy directly
            source_key = (
                report_content.replace(f"s3://{self.origin_bucket_name}/", "")
                .replace(f"s3://{self.destination_bucket_name}/", "")
                .lstrip("/")
            )
        else:
            # It's content, need to upload to origin first
            import tempfile

            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp:
                tmp.write(report_content)
                tmp_path = tmp.name

            try:
                # Upload to origin bucket first
                origin_key = f"{organization_id}/{study_id}/reports/{report_id}.txt"
                source_path = f"s3://{self.origin_bucket_name}/{origin_key}"

                cmd = ["aws", "s3", "cp", tmp_path, source_path]
                result = subprocess.run(
                    cmd, capture_output=True, text=True, check=True
                )
                source_key = origin_key
            finally:
                # Clean up temp file
                os.unlink(tmp_path)

        metadata = {
            "organization_id": organization_id,
            "study_id": study_id,
            "report_id": report_id,
        }
        if signed_at:
            metadata["signed_at"] = signed_at

        return self._copy_s3_object(source_key, destination_key, metadata)

    def upload_report_metadata(
        self,
        study_data: Dict,
        organization_id: str,
        study_id: str,
    ) -> bool:
        """Copy study metadata JSON from origin bucket to destination bucket.

        :param study_data: Dictionary containing study metadata (used for validation)
        :param organization_id: Organization UUID
        :param study_id: Study UUID
        :return: True if successful, False otherwise
        """
        try:
            # Construct source and destination keys
            source_key = f"{organization_id}/{study_id}/metadata.json"
            destination_key = f"{organization_id}/{study_id}/metadata.json"

            metadata = {
                "organization_id": organization_id,
                "study_id": study_id,
            }

            # Check if source exists first
            if not self._check_s3_object_exists(self.origin_bucket_name, source_key):
                logger.warning(
                    f"Metadata file not found in origin bucket: {source_key}. "
                    "Skipping copy."
                )
                return False

            return self._copy_s3_object(source_key, destination_key, metadata)
        except Exception as e:
            logger.error(f"Error copying metadata: {str(e)}")
            return False

    def check_file_exists(self, s3_key: str, bucket_name: str = None) -> bool:
        """Check if a file exists in S3 bucket.

        :param s3_key: S3 object key
        :param bucket_name: Bucket name (defaults to destination bucket)
        :return: True if file exists, False otherwise
        """
        bucket = bucket_name or self.destination_bucket_name
        return self._check_s3_object_exists(bucket, s3_key)

    def list_study_files(
        self, organization_id: str, study_id: str, bucket_name: str = None
    ) -> List[str]:
        """List all files for a study in S3 using AWS CLI.

        :param organization_id: Organization UUID
        :param study_id: Study UUID
        :param bucket_name: Bucket name (defaults to destination bucket)
        :return: List of S3 keys
        """
        try:
            bucket = bucket_name or self.destination_bucket_name
            prefix = f"{organization_id}/{study_id}/"
            source_path = f"s3://{bucket}/{prefix}"

            cmd = ["aws", "s3", "ls", source_path, "--recursive"]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )

            # Parse output - each line is: DATE TIME SIZE KEY
            keys = []
            for line in result.stdout.strip().split("\n"):
                if line.strip():
                    # Extract key (everything after the size)
                    parts = line.split()
                    if len(parts) >= 4:
                        key = " ".join(parts[3:])  # Handle keys with spaces
                        keys.append(key)

            return keys
        except subprocess.CalledProcessError as e:
            logger.error(f"Error listing study files: {e.stderr or str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error listing study files: {str(e)}")
            return []

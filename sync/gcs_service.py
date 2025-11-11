"""Service for copying DICOM files and reports from S3 origin bucket to GCS destination bucket."""

import json
import logging
import os
import subprocess
from typing import Dict, List, Optional

from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


class GCSService:
    """Service for handling file copying from S3 to GCS using gsutil."""

    def __init__(
        self,
        origin_bucket_name: str = None,
        destination_bucket_name: str = None,
        gcs_credentials_path: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_region_name: str = None,
    ):
        """Initialize service for copying from S3 to GCS.

        :param origin_bucket_name: Origin S3 bucket name (defaults to S3_ORIGIN_BUCKET_NAME)
        :param destination_bucket_name: Destination GCS bucket name (defaults to GCS_BUCKET_NAME)
        :param gcs_credentials_path: Path to GCS service account JSON key file (optional, uses Application Default Credentials if not provided)
        :param aws_access_key_id: AWS access key ID
        :param aws_secret_access_key: AWS secret access key
        :param aws_region_name: AWS region name
        """
        self.origin_bucket_name = origin_bucket_name or os.getenv(
            "S3_ORIGIN_BUCKET_NAME", "s3-origin-bucket-name"
        )
        self.destination_bucket_name = destination_bucket_name or os.getenv(
            "GCS_BUCKET_NAME", "gcs-bucket-name"
        )
        self.gcs_credentials_path = gcs_credentials_path or os.getenv(
            "GCS_CREDENTIALS_PATH"
        )
        self.aws_region_name = aws_region_name or os.getenv("AWS_REGION_NAME", "us-east-1")

        # Set GCS credentials if provided
        if self.gcs_credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.gcs_credentials_path
            # Also activate service account for gcloud/gsutil
            try:
                gcloud_path = "/usr/local/google-cloud-sdk/bin/gcloud"
                if not os.path.exists(gcloud_path):
                    # Try alternative path
                    gcloud_path = "/root/google-cloud-sdk/bin/gcloud"
                if os.path.exists(gcloud_path):
                    subprocess.run(
                        [gcloud_path, "auth", "activate-service-account", "--key-file", self.gcs_credentials_path],
                        capture_output=True,
                        check=False,  # Don't fail if this doesn't work
                    )
            except Exception as e:
                logger.warning(f"Could not activate service account: {str(e)}")
        
        # If no explicit credentials path, try to use application-default credentials
        if not self.gcs_credentials_path:
            app_default_creds = "/root/.config/gcloud/application_default_credentials.json"
            if os.path.exists(app_default_creds):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = app_default_creds
                logger.info("Using application-default credentials from /root/.config/gcloud/")

        # Set AWS credentials in environment if provided
        if aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID"):
            os.environ["AWS_ACCESS_KEY_ID"] = (
                aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
            )
        if aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY"):
            os.environ["AWS_SECRET_ACCESS_KEY"] = (
                aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
            )
        if self.aws_region_name:
            os.environ["AWS_DEFAULT_REGION"] = self.aws_region_name

        logger.info(
            f"Service initialized: s3://{self.origin_bucket_name} -> gs://{self.destination_bucket_name}"
        )

    def _copy_s3_to_gcs_object(
        self,
        source_key: str,
        destination_key: str,
        metadata: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Copy an object from S3 origin bucket to GCS destination bucket using gsutil.

        gsutil supports cross-cloud copying directly from S3 to GCS.

        :param source_key: S3 object key in origin bucket
        :param destination_key: GCS object key in destination bucket
        :param metadata: Optional metadata (note: gsutil cp supports metadata with -h flag)
        :return: True if successful, False otherwise
        """
        try:
            source_path = f"s3://{self.origin_bucket_name}/{source_key}"
            destination_path = f"gs://{self.destination_bucket_name}/{destination_key}"

            # Find gsutil - try multiple possible paths
            import shutil
            gsutil_path = None
            for path in ["/usr/local/google-cloud-sdk/bin/gsutil", "/root/google-cloud-sdk/bin/gsutil", "gsutil"]:
                if os.path.exists(path) if not path == "gsutil" else shutil.which(path):
                    gsutil_path = path if path != "gsutil" else shutil.which(path)
                    break
            if not gsutil_path:
                gsutil_path = "gsutil"  # Fallback - will fail with clear error

            # Build gsutil command (gsutil can copy directly from S3 to GCS)
            cmd = [gsutil_path, "cp", source_path, destination_path]

            # Add metadata if provided (using -h flag for custom metadata)
            if metadata:
                for key, value in metadata.items():
                    # gsutil uses -h flag for custom metadata: -h "key:value"
                    cmd.extend(["-h", f"{key}:{value}"])

            # Set environment variables for subprocess
            env = os.environ.copy()
            # Ensure GOOGLE_APPLICATION_CREDENTIALS is set if we have it
            if "GOOGLE_APPLICATION_CREDENTIALS" not in env:
                app_default_creds = "/root/.config/gcloud/application_default_credentials.json"
                if os.path.exists(app_default_creds):
                    env["GOOGLE_APPLICATION_CREDENTIALS"] = app_default_creds
            
            # Ensure AWS credentials are set for S3 access
            if "AWS_ACCESS_KEY_ID" in os.environ:
                env["AWS_ACCESS_KEY_ID"] = os.environ["AWS_ACCESS_KEY_ID"]
            if "AWS_SECRET_ACCESS_KEY" in os.environ:
                env["AWS_SECRET_ACCESS_KEY"] = os.environ["AWS_SECRET_ACCESS_KEY"]
            if "AWS_DEFAULT_REGION" in os.environ:
                env["AWS_DEFAULT_REGION"] = os.environ["AWS_DEFAULT_REGION"]
            elif "AWS_REGION_NAME" in os.environ:
                env["AWS_DEFAULT_REGION"] = os.environ["AWS_REGION_NAME"]
            
            # Execute command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                env=env,
            )

            logger.info(
                f"Successfully copied s3://{self.origin_bucket_name}/{source_key} "
                f"to gs://{self.destination_bucket_name}/{destination_key}"
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"gsutil error copying {source_key}: {e.stderr or e.stdout or str(e)}"
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

    def _check_gcs_object_exists(self, bucket_name: str, gcs_key: str) -> bool:
        """Check if an object exists in GCS bucket using gsutil.

        :param bucket_name: GCS bucket name
        :param gcs_key: GCS object key
        :return: True if object exists, False otherwise
        """
        try:
            source_path = f"gs://{bucket_name}/{gcs_key}"
            # Find gsutil - try multiple possible paths
            import shutil
            gsutil_path = None
            for path in ["/usr/local/google-cloud-sdk/bin/gsutil", "/root/google-cloud-sdk/bin/gsutil", "gsutil"]:
                if os.path.exists(path) if not path == "gsutil" else shutil.which(path):
                    gsutil_path = path if path != "gsutil" else shutil.which(path)
                    break
            if not gsutil_path:
                gsutil_path = "gsutil"  # Fallback - will fail with clear error
            cmd = [gsutil_path, "ls", source_path]

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
        """Copy a DICOM file from S3 origin bucket to GCS destination bucket with organized path structure.

        If dicom_path is a local file, it will be uploaded to S3 origin bucket first, then copied to GCS destination.
        If dicom_path is an S3 key, it will be copied directly from S3 origin to GCS destination.

        :param dicom_path: Local file path or S3 key path in origin bucket
        :param organization_id: Organization UUID
        :param study_id: Study UUID
        :param series_id: Series ID
        :param image_id: Image ID
        :param instance_number: Optional instance number
        :return: True if successful, False otherwise
        """
        # Construct destination GCS key: organization_id/study_id/dicom_files/series_id/image_id.dcm
        filename = f"{image_id}.dcm"
        if instance_number is not None:
            filename = f"{instance_number:04d}_{filename}"

        destination_key = f"{organization_id}/{study_id}/dicom_files/{series_id}/{filename}"

        # Check if dicom_path is a local file or S3 key
        if dicom_path.startswith("s3://") or dicom_path.startswith(
            organization_id + "/"
        ):
            # It's an S3 key, extract the key
            source_key = (
                dicom_path.replace(f"s3://{self.origin_bucket_name}/", "")
                .replace(f"gs://{self.destination_bucket_name}/", "")
                .lstrip("/")
            )
        elif os.path.exists(dicom_path):
            # It's a local file, upload to S3 origin bucket first
            origin_key = (
                f"{organization_id}/{study_id}/dicom_files/{series_id}/{filename}"
            )
            source_path = f"s3://{self.origin_bucket_name}/{origin_key}"

            cmd = ["aws", "s3", "cp", dicom_path, source_path]
            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, check=True
                )
                source_key = origin_key
            except subprocess.CalledProcessError as e:
                logger.error(
                    f"Failed to upload {dicom_path} to S3 origin bucket: {e.stderr or str(e)}"
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

        return self._copy_s3_to_gcs_object(source_key, destination_key, metadata)

    def upload_report(
        self,
        report_content: str,
        organization_id: str,
        study_id: str,
        report_id: str,
        signed_at: Optional[str] = None,
    ) -> bool:
        """Upload report content to S3 origin bucket, then copy to GCS destination bucket.

        If report_content is an S3 key (starts with 's3://' or organization_id), it will be copied directly.
        Otherwise, content will be written to a temp file and uploaded to S3 origin first.

        :param report_content: Report content (text) or S3 key path
        :param organization_id: Organization UUID
        :param study_id: Study UUID
        :param report_id: Report ID
        :param signed_at: Optional signed timestamp
        :return: True if successful, False otherwise
        """
        # Construct destination GCS key: organization_id/study_id/reports/report_id.txt
        destination_key = f"{organization_id}/{study_id}/reports/{report_id}.txt"

        # Check if report_content is an S3 key/path
        if report_content.startswith("s3://") or report_content.startswith(
            organization_id + "/"
        ):
            # It's an S3 key, copy directly
            source_key = (
                report_content.replace(f"s3://{self.origin_bucket_name}/", "")
                .replace(f"gs://{self.destination_bucket_name}/", "")
                .lstrip("/")
            )
        else:
            # It's content, need to upload to S3 origin first
            import tempfile

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", delete=False
            ) as tmp:
                tmp.write(report_content)
                tmp_path = tmp.name

            try:
                # Upload to S3 origin bucket first
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

        return self._copy_s3_to_gcs_object(source_key, destination_key, metadata)

    def upload_report_metadata(
        self,
        study_data: Dict,
        organization_id: str,
        study_id: str,
    ) -> bool:
        """Copy study metadata JSON from S3 origin bucket to GCS destination bucket.

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

            # Check if source exists in S3 origin bucket first
            if not self._check_s3_object_exists(self.origin_bucket_name, source_key):
                logger.warning(
                    f"Metadata file not found in S3 origin bucket: {source_key}. "
                    "Skipping copy."
                )
                return False

            return self._copy_s3_to_gcs_object(source_key, destination_key, metadata)
        except Exception as e:
            logger.error(f"Error copying metadata: {str(e)}")
            return False

    def check_file_exists(self, key: str, bucket_name: str = None) -> bool:
        """Check if a file exists in destination GCS bucket.

        :param key: Object key
        :param bucket_name: Bucket name (defaults to destination bucket)
        :return: True if file exists, False otherwise
        """
        bucket = bucket_name or self.destination_bucket_name
        return self._check_gcs_object_exists(bucket, key)

    def list_study_files(
        self, organization_id: str, study_id: str, bucket_name: str = None
    ) -> List[str]:
        """List all files for a study in GCS using gsutil.

        :param organization_id: Organization UUID
        :param study_id: Study UUID
        :param bucket_name: Bucket name (defaults to destination bucket)
        :return: List of GCS keys
        """
        try:
            bucket = bucket_name or self.destination_bucket_name
            prefix = f"{organization_id}/{study_id}/"
            source_path = f"gs://{bucket}/{prefix}"

            # Find gsutil - try multiple possible paths
            import shutil
            gsutil_path = None
            for path in ["/usr/local/google-cloud-sdk/bin/gsutil", "/root/google-cloud-sdk/bin/gsutil", "gsutil"]:
                if os.path.exists(path) if not path == "gsutil" else shutil.which(path):
                    gsutil_path = path if path != "gsutil" else shutil.which(path)
                    break
            if not gsutil_path:
                gsutil_path = "gsutil"  # Fallback - will fail with clear error
            cmd = [gsutil_path, "ls", source_path, "-r"]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )

            # Parse output - gsutil ls returns full gs:// paths
            keys = []
            for line in result.stdout.strip().split("\n"):
                if line.strip() and line.startswith("gs://"):
                    # Extract key (remove gs://bucket-name/ prefix)
                    key = line.replace(f"gs://{bucket}/", "").lstrip("/")
                    if key:  # Only add non-empty keys
                        keys.append(key)

            return keys
        except subprocess.CalledProcessError as e:
            logger.error(f"Error listing study files: {e.stderr or str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error listing study files: {str(e)}")
            return []


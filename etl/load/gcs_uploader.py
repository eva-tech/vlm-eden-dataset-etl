"""GCS upload module for uploading files to Google Cloud Storage."""

import logging
import os
import shutil
import subprocess
from typing import List, Tuple

logger = logging.getLogger(__name__)


class GCSUploader:
    """Uploads files to Google Cloud Storage."""

    def __init__(self, bucket_name: str):
        """Initialize GCS uploader.

        :param bucket_name: GCS bucket name
        """
        self.bucket_name = bucket_name
        self.gsutil_path = self._find_gsutil()

    def _find_gsutil(self) -> str:
        """Find gsutil executable path.

        :return: Path to gsutil executable
        """
        for path in ["/usr/local/google-cloud-sdk/bin/gsutil", "/root/google-cloud-sdk/bin/gsutil", "gsutil"]:
            if os.path.exists(path) if not path == "gsutil" else shutil.which(path):
                return path if path != "gsutil" else shutil.which(path)
        return "gsutil"  # Fallback

    def upload_file(self, local_path: str, gcs_path: str) -> bool:
        """Upload a single file to GCS.

        :param local_path: Local file path
        :param gcs_path: GCS path (without gs:// prefix)
        :return: True if successful, False otherwise
        """
        try:
            if not os.path.exists(local_path):
                logger.error(f"File not found: {local_path}")
                return False

            destination = f"gs://{self.bucket_name}/{gcs_path}"
            cmd = [self.gsutil_path, "cp", local_path, destination]

            env = os.environ.copy()
            # Ensure GOOGLE_APPLICATION_CREDENTIALS is set if available
            app_default_creds = "/root/.config/gcloud/application_default_credentials.json"
            if os.path.exists(app_default_creds) and "GOOGLE_APPLICATION_CREDENTIALS" not in env:
                env["GOOGLE_APPLICATION_CREDENTIALS"] = app_default_creds

            subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
            logger.info(f"Uploaded {local_path} to {destination}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {local_path} to GCS: {str(e)}")
            return False

    def upload_directory_contents(self, local_dir: str, gcs_prefix: str) -> bool:
        """Upload all contents of a directory to GCS.

        :param local_dir: Local directory path
        :param gcs_prefix: GCS path prefix (without gs:// prefix)
        :return: True if successful, False otherwise
        """
        try:
            if not os.path.exists(local_dir):
                logger.warning(f"Directory not found: {local_dir}")
                return False

            destination = f"gs://{self.bucket_name}/{gcs_prefix}"
            # Use . to copy contents without creating nested directory
            cmd = [self.gsutil_path, "-m", "cp", "-r", os.path.join(local_dir, "."), destination]

            env = os.environ.copy()
            app_default_creds = "/root/.config/gcloud/application_default_credentials.json"
            if os.path.exists(app_default_creds) and "GOOGLE_APPLICATION_CREDENTIALS" not in env:
                env["GOOGLE_APPLICATION_CREDENTIALS"] = app_default_creds

            subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
            logger.info(f"Uploaded directory contents from {local_dir} to {destination}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload directory contents: {str(e)}")
            return False


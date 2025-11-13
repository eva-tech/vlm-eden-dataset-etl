"""Data fetching module for downloading DICOM files."""

import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class DataFetcher:
    """Fetches DICOM files from URLs."""

    def download_file(self, url: str, output_path: str, timeout: int = 60) -> bool:
        """Download a file from URL to local path.

        :param url: URL of the file to download
        :param output_path: Local path where the file should be saved
        :param timeout: Request timeout in seconds
        :return: True if successful, False otherwise
        """
        try:
            logger.debug(f"Downloading {url} to {output_path}")
            response = requests.get(url, stream=True, timeout=timeout)
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


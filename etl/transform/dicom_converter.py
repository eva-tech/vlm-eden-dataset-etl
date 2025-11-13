"""DICOM to JPG conversion module."""

import logging
import os
import shutil
import subprocess
import tempfile
from typing import Optional

logger = logging.getLogger(__name__)


class DICOMConverter:
    """Converts DICOM files to JPG format."""

    def __init__(self, quality: float = 1.0):
        """Initialize DICOM converter.

        :param quality: JPEG quality (0.0 to 1.0)
        """
        self.quality = quality
        self.dcm2jpg_path = self._find_dcm2jpg()

    def _find_dcm2jpg(self) -> str:
        """Find dcm2jpg executable path.

        :return: Path to dcm2jpg executable
        """
        for path in ["/usr/local/dcm2jpg/bin/dcm2jpg.sh", "/usr/local/bin/dcm2jpg", "dcm2jpg", "dcm2jpg.sh"]:
            if path in ["dcm2jpg", "dcm2jpg.sh"]:
                found = shutil.which(path)
                if found:
                    return found
            elif os.path.exists(path):
                return path
        return "dcm2jpg"  # Fallback

    def convert(self, dicom_path: str, jpg_path: str, timeout: int = 60) -> bool:
        """Convert a DICOM file to JPG.

        :param dicom_path: Path to the DICOM file
        :param jpg_path: Path where the JPG file should be saved
        :param timeout: Conversion timeout in seconds
        :return: True if successful, False otherwise
        """
        try:
            if not os.path.exists(dicom_path):
                logger.error(f"DICOM file not found: {dicom_path}")
                return False

            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(jpg_path), exist_ok=True)

            # Use temporary file for output
            temp_output = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
            temp_output_path = temp_output.name
            temp_output.close()

            # Run dcm2jpg with specified quality
            cmd = [self.dcm2jpg_path, "-q", str(self.quality), dicom_path, temp_output_path]

            logger.info(f"Converting {dicom_path} to JPG...")
            subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=timeout,
            )

            # Move temp file to final location
            if os.path.exists(temp_output_path):
                shutil.move(temp_output_path, jpg_path)
                file_size = os.path.getsize(jpg_path)
                logger.info(f"Converted {dicom_path} -> {jpg_path} ({file_size} bytes)")
                return True
            else:
                logger.error(f"Conversion failed: output file not created")
                return False

        except subprocess.CalledProcessError as e:
            logger.error(f"dcm2jpg error converting {dicom_path}: {e.stderr or e.stdout or str(e)}")
            # Clean up temp file if it exists
            if 'temp_output_path' in locals() and os.path.exists(temp_output_path):
                os.unlink(temp_output_path)
            return False
        except Exception as e:
            logger.error(f"Unexpected error converting {dicom_path}: {str(e)}")
            # Clean up temp file if it exists
            if 'temp_output_path' in locals() and os.path.exists(temp_output_path):
                os.unlink(temp_output_path)
            return False


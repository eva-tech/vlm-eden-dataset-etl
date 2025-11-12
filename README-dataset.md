# EDEN Dataset VLM - Sample Test Dataset

## Overview

This dataset contains DICOM medical imaging files and their corresponding JPEG conversions, along with extracted report metadata. The dataset is stored in Google Cloud Storage at:

**GCS Location:** `gs://ai-training-dev/eden-dataset-vlms/sample-test/`

## Folder Structure

```
gs://ai-training-dev/eden-dataset-vlms/sample-test/
├── README-dataset.md                          # This documentation file
├── dicom-reports-extracted-sample.csv         # Metadata CSV file
├── dicom-files/                               # DICOM image files directory
│   ├── {instance_id}.dcm                      # DICOM files (one per instance)
│   └── ...
└── images-jpg/                                # JPEG converted images directory
    ├── {instance_id}.jpg                      # JPEG files (one per instance)
    └── ...
```

## File Types and Counts

### 1. CSV Metadata File
- **File:** `dicom-reports-extracted-sample.csv`
- **Type:** Comma-separated values (CSV)
- **Count:** 1 file
- **Description:** Contains metadata for all DICOM files including:
  - `study_id`: Unique study identifier
  - `series_number`: Series number within the study
  - `instance_id`: Unique instance identifier (used as filename)
  - `instance_number`: Instance number within the series
  - `file_path`: Original file path in the source system
  - `file_url`: URL to download the original DICOM file
  - `report_value`: Extracted report text from `pacs_report_fields.value`
  - `field_created_at`: Timestamp when the report field was created
  - `downloaded`: Boolean indicating if the file was successfully downloaded
  - `local_file_path`: Local path where the file was stored during processing

### 2. DICOM Files
- **Location:** `dicom-files/`
- **Type:** DICOM (Digital Imaging and Communications in Medicine) format
- **Extension:** `.dcm`
- **Naming Convention:** `{instance_id}.dcm`
- **Count:** 8 files (as of latest extraction)
- **Description:** Original DICOM medical imaging files. Each file is named using its unique `instance_id` UUID. Files are stored in a flat structure without subdirectories.

### 3. JPEG Images
- **Location:** `images-jpg/`
- **Type:** JPEG image format
- **Extension:** `.jpg`
- **Naming Convention:** `{instance_id}.jpg`
- **Count:** 16 files (as of latest extraction)
- **Description:** JPEG conversions of the DICOM files, created using `dcm2jpg` tool with quality setting of 1.0. Each JPEG file corresponds to a DICOM file and shares the same `instance_id` as the filename. The higher count may be due to multiple extraction runs.

## Data Source

The dataset is extracted from a PACS (Picture Archiving and Communication System) database:
- **Source Database:** PostgreSQL database with PACS schema
- **Studies Selected:** 8 random studies from `pacs_studies` table
- **Extraction Method:** SQL queries joining multiple tables:
  - `pacs_studies`
  - `pacs_series`
  - `pacs_instances`
  - `pacs_instance_files`
  - `pacs_reports`
  - `pacs_report_fields`

## File Naming

All files use the `instance_id` (UUID format) as their filename:
- DICOM files: `{instance_id}.dcm`
- JPEG files: `{instance_id}.jpg`

This allows easy correlation between:
- CSV metadata rows
- DICOM files
- JPEG converted images

## Processing Pipeline

1. **Extraction:** Random selection of 8 studies from `pacs_studies`
2. **Download:** DICOM files downloaded from source URLs
3. **Conversion:** DICOM files converted to JPEG using `dcm2jpg -q 1.0`
4. **Upload:** All files uploaded to GCS bucket

## Usage

### Accessing Files

```bash
# List all files
gsutil ls -r gs://ai-training-dev/eden-dataset-vlms/sample-test/

# Download CSV file
gsutil cp gs://ai-training-dev/eden-dataset-vlms/sample-test/dicom-reports-extracted-sample.csv .

# Download all DICOM files
gsutil -m cp gs://ai-training-dev/eden-dataset-vlms/sample-test/dicom-files/*.dcm ./local_dicom/

# Download all JPEG files
gsutil -m cp gs://ai-training-dev/eden-dataset-vlms/sample-test/images-jpg/*.jpg ./local_jpg/
```

### Correlating Files

To find the JPEG file for a specific DICOM file (or vice versa), use the `instance_id` from the CSV file:

```python
import pandas as pd

# Load CSV
df = pd.read_csv('dicom-reports-extracted-sample.csv')

# Get instance_id for a specific row
instance_id = df.iloc[0]['instance_id']

# Corresponding files:
# DICOM: gs://ai-training-dev/eden-dataset-vlms/sample-test/dicom-files/{instance_id}.dcm
# JPEG:  gs://ai-training-dev/eden-dataset-vlms/sample-test/images-jpg/{instance_id}.jpg
```

## Notes

- The dataset is a sample/test dataset extracted from a larger PACS system
- Files are randomly selected from available studies
- JPEG conversion quality is set to maximum (1.0) for best image quality
- The CSV file contains all metadata needed to correlate files and understand the dataset structure

## Last Updated

This dataset was last updated during the extraction process. File counts may vary if the extraction script is run multiple times.


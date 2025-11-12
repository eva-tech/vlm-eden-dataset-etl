# VLM Eden Dataset ETL System

A specialized ETL system built with Python and Celery that extracts DICOM medical imaging files and reports from a PostgreSQL PACS database, converts DICOM files to JPEG images, and uploads everything to Google Cloud Storage (GCS) for Vision Language Model (VLM) dataset creation.

## Overview

This system extracts DICOM files and associated reports from a PACS database, downloads the DICOM files, converts them to JPEG format using dcm4che, and uploads all files (DICOM, JPEG, and CSV metadata) to a GCS bucket with organized structure.

### Key Features

- **DICOM Extraction**: Extracts DICOM files and reports from PostgreSQL PACS database
- **DICOM to JPEG Conversion**: Converts DICOM files to JPEG using dcm4che toolkit
- **Parallel Processing**: Uses Celery tasks for asynchronous DICOM conversion and file uploads
- **Progress Tracking**: Implements pagination and batching with progress tracking and resumption capability
- **GCS Upload**: Uploads DICOM files, JPEG images, and CSV metadata to GCS bucket
- **Error Handling**: Robust error handling and logging throughout the pipeline

## Repository Structure

```
vlm-eden-dataset-etl/
├── extract_and_upload_dicom_reports.py  # Main extraction script
├── queries/                              # SQL query definitions
│   └── get_chest_dicom_files_and_reports.py  # DICOM extraction queries
├── sync/                                 # Core modules
│   ├── database_breach.py               # Database connection bridge
│   └── gcs_service.py                   # GCS upload service
├── tasks.py                             # Celery task definitions
│   ├── convert_dicom_to_jpg            # DICOM to JPEG conversion task
│   └── upload_file_to_gcs              # GCS file upload task
├── celery_app.py                        # Celery application configuration
├── celery_config.py                     # Celery task scheduling
├── database.py                          # Database connection utilities
├── run_worker.py                        # Celery worker entry point
├── Dockerfile                           # Docker image with dcm4che and gsutil
├── docker-compose.yml                   # Docker Compose configuration
├── Makefile                             # Make commands for development
├── requirements.txt                     # Python dependencies
├── README.md                            # This file
└── README-dataset.md                    # Dataset documentation (in GCS bucket)
```

## Prerequisites

- Python 3.10+
- PostgreSQL database (source database with PACS data)
- Redis (for Celery broker and result backend)
- Google Cloud Storage bucket access (destination bucket)
- Docker and Docker Compose (for local development)
- Google Cloud SDK (gsutil) - installed in Docker image
- Java JDK and Maven - for dcm4che (installed in Docker image)

## Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd vlm-eden-dataset-etl
```

### 2. Environment Configuration

Create a `.env` file with the following variables:

#### Database Configuration
```env
# Source Database (PACS database)
SOURCE_DATABASE_NAME=your_source_db
SOURCE_DATABASE_USER=your_source_user
SOURCE_DATABASE_PASS=your_source_password
SOURCE_DATABASE_HOST=your_source_host
SOURCE_DATABASE_PORT=5432
```

#### Redis Configuration
```env
REDIS_URL=redis://localhost:6379/0
```

#### Google Cloud Storage Configuration
```env
# Destination GCS bucket name
GCS_BUCKET_NAME=ai-training-dev

# Optional: Path to GCS service account JSON key file
# If not provided, uses Application Default Credentials
GCS_CREDENTIALS_PATH=/path/to/service-account-key.json
```

#### Other Configuration
```env
SENTRY_DNS=your_sentry_dsn  # Optional, for error tracking
FLOWER_USER=admin
FLOWER_PASSWORD=your_password
SECRET_KEY=your_secret_key
LOGGING_LEVEL=INFO
```

### 3. Build and Start Docker Containers

The Docker image includes:
- Python 3.10
- dcm4che toolkit (for DICOM to JPEG conversion)
- Google Cloud SDK (gsutil)
- All Python dependencies

```bash
# Build Docker images (includes dcm4che installation)
make build

# Start all services
make up

# Or manually:
docker-compose build
docker-compose up -d
```

This will start:
- Redis (broker)
- Celery Worker
- Celery Beat (scheduler)
- Flower (task monitoring)

### 4. Configure GCS Authentication

Authenticate with Google Cloud Storage:

```bash
# Interactive authentication (opens browser)
docker exec -it celery_worker_intelligence gcloud auth application-default login

# Or use service account key file (set GCS_CREDENTIALS_PATH in .env)
```

## Usage

### DICOM Extraction and Upload Pipeline

The main extraction script (`extract_and_upload_dicom_reports.py`) performs the following:

1. **Query Database**: Extracts DICOM files and reports from randomly selected studies
2. **Download DICOM Files**: Downloads DICOM files from source URLs
3. **Convert to JPEG**: Converts DICOM files to JPEG using dcm4che (via Celery tasks)
4. **Generate CSV**: Creates CSV file with metadata and report values
5. **Upload to GCS**: Uploads DICOM files, JPEG images, and CSV to GCS bucket

### Running the Extraction Script

#### Basic Execution

```bash
# Run the extraction script inside the Docker container
docker exec celery_worker_intelligence python extract_and_upload_dicom_reports.py
```

#### Configuration

The script can be configured by modifying `queries/get_chest_dicom_files_and_reports.py`:

- **Number of Studies**: Modify the `LIMIT` clause in the query (currently set to 8 random studies)
- **Pagination**: Adjust `PAGE_SIZE` and `BATCH_SIZE` in `extract_and_upload_dicom_reports.py`

Example query modification:
```python
# In queries/get_chest_dicom_files_and_reports.py
WITH all_studies AS (
  SELECT id
  FROM pacs_studies
  WHERE deleted = FALSE
  ORDER BY RANDOM()
  LIMIT 8  # Change this number to select more/fewer studies
)
```

### Understanding the Output

#### Console Output

The script provides detailed progress information:

```
======================================================================
Starting DICOM and reports extraction process with pagination and batching
======================================================================
EXTRACTION STATISTICS
======================================================================
Total records: 8
Total pages: 1
Page size: 25
Batch size: 25
Starting from page: 1
Count query time: 0:00:00 (0.19 seconds)
======================================================================

Processing page 1/1 (100.0%)
Downloading DICOM files: [==================================================] 8/8 (100.0%)
Converting DICOM files to JPG...
DICOM to JPG conversions: 8/8 successful

EXTRACTION PHASE COMPLETE
======================================================================
Extraction time: 0:00:05 (5.47 seconds)
Total files to download: 8
Total files downloaded: 8
======================================================================

Starting GCS upload...
CSV uploaded successfully
DICOM files uploaded successfully
JPG images uploaded: 8/8 successful

EXTRACTION AND UPLOAD COMPLETE
======================================================================
TIMING SUMMARY:
  Count query: 0:00:00 (0.19s)
  Extraction: 0:00:05 (5.47s)
  Upload: 0:00:12 (12.06s)
  TOTAL TIME: 0:00:18 (18.61 seconds)

FILES SUMMARY:
  CSV file: /tmp/dicom_extract_xxxxx/dicom-reports-extracted-sample.csv
  Downloaded DICOM files: 8
  Files uploaded to GCS: 8

GCS LOCATIONS:
  CSV: gs://ai-training-dev/eden-dataset-vlms/sample-test/dicom-reports-extracted-sample.csv
  DICOM files: gs://ai-training-dev/eden-dataset-vlms/sample-test/dicom-files/
  JPG images: gs://ai-training-dev/eden-dataset-vlms/sample-test/images-jpg/
======================================================================
```

#### GCS Bucket Structure

After execution, files are organized in GCS as follows:

```
gs://ai-training-dev/eden-dataset-vlms/sample-test/
├── README-dataset.md                          # Dataset documentation
├── dicom-reports-extracted-sample.csv         # Metadata CSV file
├── dicom-files/                               # DICOM files directory
│   ├── {instance_id}.dcm                      # DICOM files (flat structure)
│   └── ...
└── images-jpg/                                # JPEG images directory
    ├── {instance_id}.jpg                      # JPEG files (flat structure)
    └── ...
```

#### CSV File Structure

The CSV file (`dicom-reports-extracted-sample.csv`) contains the following columns:

- `study_id`: Unique study identifier
- `series_number`: Series number within the study
- `instance_id`: Unique instance identifier (used as filename)
- `instance_number`: Instance number within the series
- `file_path`: Original file path in the source system
- `file_url`: URL to download the original DICOM file
- `report_value`: Extracted report text from `pacs_report_fields.value` (aggregated at study level)
- `field_created_at`: Timestamp when the report field was created
- `downloaded`: Boolean indicating if the file was successfully downloaded
- `local_file_path`: Local path where the file was stored during processing

#### File Naming Convention

All files use the `instance_id` (UUID format) as their filename:
- **DICOM files**: `{instance_id}.dcm` → `gs://.../dicom-files/{instance_id}.dcm`
- **JPEG files**: `{instance_id}.jpg` → `gs://.../images-jpg/{instance_id}.jpg`

This allows easy correlation between:
- CSV metadata rows
- DICOM files
- JPEG converted images

### Interpreting Results

#### Success Indicators

1. **Extraction Phase**:
   - All DICOM files downloaded successfully
   - All DICOM to JPEG conversions successful
   - CSV file created with all rows

2. **Upload Phase**:
   - CSV file uploaded to GCS
   - All DICOM files uploaded to GCS
   - All JPEG images uploaded to GCS

#### Progress Tracking

The script creates a progress file (`extraction_progress.json`) that allows resuming from where it left off if interrupted. The progress file tracks:
- Current page number
- Processed file keys
- CSV rows written

#### Error Handling

- Failed downloads are logged but don't stop the process
- Failed conversions are logged with error details
- Failed uploads are logged with error details
- Check logs for specific error messages

### Verifying Uploads

#### List Files in GCS

```bash
# List all files in the sample-test folder
docker exec celery_worker_intelligence gsutil ls -r gs://ai-training-dev/eden-dataset-vlms/sample-test/

# Count DICOM files
docker exec celery_worker_intelligence gsutil ls gs://ai-training-dev/eden-dataset-vlms/sample-test/dicom-files/*.dcm | wc -l

# Count JPEG files
docker exec celery_worker_intelligence gsutil ls gs://ai-training-dev/eden-dataset-vlms/sample-test/images-jpg/*.jpg | wc -l
```

#### Download Files for Verification

```bash
# Download CSV file
docker exec celery_worker_intelligence gsutil cp gs://ai-training-dev/eden-dataset-vlms/sample-test/dicom-reports-extracted-sample.csv .

# Download a specific DICOM file
docker exec celery_worker_intelligence gsutil cp gs://ai-training-dev/eden-dataset-vlms/sample-test/dicom-files/{instance_id}.dcm .

# Download a specific JPEG file
docker exec celery_worker_intelligence gsutil cp gs://ai-training-dev/eden-dataset-vlms/sample-test/images-jpg/{instance_id}.jpg .
```

## Monitoring

### Flower Dashboard

Access the Flower dashboard for Celery task monitoring:

```
http://localhost:5555/flower
```

Login with credentials from your `.env` file.

### Logs

View logs using Docker Compose:

```bash
make logs

# Or directly:
docker-compose logs -f celery_worker_intelligence
```

### Task Status

Check Celery task results:
- Via Flower dashboard
- In Redis (using Redis CLI)
- In console output during script execution

## Project Structure Details

### Main Scripts

- **`extract_and_upload_dicom_reports.py`**: Main extraction script that orchestrates the entire pipeline
- **`queries/get_chest_dicom_files_and_reports.py`**: SQL queries for extracting DICOM files and reports
- **`tasks.py`**: Celery tasks for DICOM to JPEG conversion and GCS uploads

### Core Modules

- **`sync/database_breach.py`**: Database connection management
- **`sync/gcs_service.py`**: GCS service for file uploads

### Docker Configuration

- **`Dockerfile`**: Includes dcm4che installation, Google Cloud SDK, and Python dependencies
- **`docker-compose.yml`**: Orchestrates Redis, Celery worker, Celery beat, and Flower services

## Development

### Running Locally

```bash
# Start services
make up

# Access worker bash
make bash

# View logs
make logs

# Rebuild after code changes
make build
make up
```

### Modifying the Query

To change which studies are extracted, edit `queries/get_chest_dicom_files_and_reports.py`:

```python
# Select all studies (remove LIMIT)
WITH all_studies AS (
  SELECT id
  FROM pacs_studies
  WHERE deleted = FALSE
)

# Select specific number of random studies
WITH all_studies AS (
  SELECT id
  FROM pacs_studies
  WHERE deleted = FALSE
  ORDER BY RANDOM()
  LIMIT 8  # Change this number
)
```

### Adjusting Batch Sizes

Modify pagination and batch sizes in `extract_and_upload_dicom_reports.py`:

```python
# Configuration
PAGE_SIZE = 25    # Records per page from database
BATCH_SIZE = 25   # Records processed per batch
```

## Troubleshooting

### Common Issues

#### Database Connection Errors

- Verify database credentials in `.env`
- Check network connectivity to database server
- Ensure PostgreSQL is accepting connections
- Test connection: `docker exec celery_worker_intelligence python -c "from sync.database_breach import DatabaseBridge; bridge = DatabaseBridge(); print('Connected')"`

#### GCS Upload Failures

- **Authentication Issues**:
  - Run: `docker exec -it celery_worker_intelligence gcloud auth application-default login`
  - Or set `GCS_CREDENTIALS_PATH` in `.env` to a service account key file
  - Verify credentials: `docker exec celery_worker_intelligence gcloud auth list`

- **Permission Issues**:
  - Ensure the authenticated account has Storage Object Admin permissions
  - Check bucket exists: `gsutil ls gs://ai-training-dev/`
  - Test upload: `gsutil cp test.txt gs://ai-training-dev/test/`

#### DICOM Conversion Failures

- **dcm2jpg Not Found**:
  - Rebuild Docker image: `make build`
  - Verify dcm2jpg installation: `docker exec celery_worker_intelligence which dcm2jpg`
  - Test conversion: `docker exec celery_worker_intelligence dcm2jpg --help`

- **Conversion Errors**:
  - Check DICOM file validity
  - Verify Java is installed: `docker exec celery_worker_intelligence java -version`
  - Check Celery worker logs for detailed error messages

#### Celery Tasks Not Running

- Check Redis connection: `docker exec celery_worker_intelligence redis-cli ping`
- Verify Celery worker is running: `docker ps | grep celery`
- Check logs: `make logs`
- Restart services: `make down && make up`

### Debugging

Enable debug logging:

```env
LOGGING_LEVEL=DEBUG
```

View detailed task execution in Flower dashboard or check Celery logs.

## Configuration

### Study Selection

Modify `queries/get_chest_dicom_files_and_reports.py` to change study selection criteria:

- **Random Selection**: Uses `ORDER BY RANDOM() LIMIT N`
- **All Studies**: Remove `ORDER BY RANDOM() LIMIT` clause
- **Specific Criteria**: Add `WHERE` clauses to filter studies

### DICOM to JPEG Conversion

The conversion uses dcm4che's `dcm2jpg` tool with quality 1.0 (maximum quality). To modify:

Edit `tasks.py` in the `convert_dicom_to_jpg` function:

```python
# Change quality (0.0 to 1.0)
cmd = [dcm2jpg_path, "-q", "1.0", dicom_path, temp_output_path]
```

### GCS Paths

Modify GCS destination paths in `extract_and_upload_dicom_reports.py`:

```python
# CSV path
csv_gcs_path = "eden-dataset-vlms/sample-test/dicom-reports-extracted-sample.csv"

# DICOM files path
destination = f"gs://{bucket}/eden-dataset-vlms/sample-test/dicom-files/"

# JPEG images path
gcs_path = f"eden-dataset-vlms/sample-test/images-jpg/{file}"
```

## Performance Considerations

- **Pagination**: Large datasets are processed in pages to avoid database overload
- **Batching**: Records are processed in batches for efficient memory usage
- **Parallel Conversion**: DICOM to JPEG conversions run in parallel via Celery
- **Parallel Uploads**: JPEG file uploads run in parallel via Celery
- **Progress Tracking**: Script can resume from last checkpoint if interrupted

## Security

- Never commit `.env` files (already in `.gitignore`)
- Use service accounts for GCS access when possible
- Rotate GCS credentials regularly
- Limit GCS bucket access to write permissions only
- Use least-privilege principles for database access

## Additional Resources

- **Dataset Documentation**: See `README-dataset.md` in the GCS bucket for detailed dataset structure
- **GCS Location**: `gs://ai-training-dev/eden-dataset-vlms/sample-test/README-dataset.md`

## Support

For issues or questions:
- Check logs: `make logs`
- Review Flower dashboard: `http://localhost:5555/flower`
- Verify GCS credentials and permissions
- Test database connections
- Check Docker container status: `docker ps`

# VLM Eden Dataset ETL System

A specialized ETL system built with Python and Celery that extracts DICOM medical imaging files and reports from a PostgreSQL PACS database, converts DICOM files to JPEG images, and uploads everything to Google Cloud Storage (GCS) for Vision Language Model (VLM) dataset creation.

## Overview

This system extracts DICOM files and associated reports from a PACS database, downloads the DICOM files, converts them to JPEG format using dcm4che, and uploads all files (DICOM, JPEG, and CSV metadata) to a GCS bucket with organized structure.

### Key Features

- **Modular Architecture**: Clean separation of concerns with extract, transform, load, batching, and pipeline modules
- **Celery-Based Parallel Processing**: Batches run in parallel across multiple Celery workers for improved performance
- **Idempotent Operations**: Safe to rerun without data duplication
- **Automatic Retries**: Tasks automatically retry on failure with exponential backoff
- **Progress Tracking**: Resume from checkpoints if interrupted
- **Comprehensive Logging**: Detailed logging and metrics for observability

## Architecture

### Module Structure

The ETL system is organized into modular components:

```
vlm-eden-dataset-etl/
├── etl/                              # ETL package
│   ├── extract/                      # Extraction module
│   │   ├── query_executor.py        # Database query execution
│   │   └── data_fetcher.py          # File downloading
│   ├── transform/                    # Transformation module
│   │   ├── data_processor.py        # Data processing and CSV preparation
│   │   └── dicom_converter.py       # DICOM to JPG conversion
│   ├── load/                         # Loading module
│   │   └── gcs_uploader.py          # GCS file uploads
│   ├── batching/                     # Batching module
│   │   └── batch_creator.py         # Batch creation and management
│   ├── tasks/                        # Celery tasks module
│   │   └── batch_tasks.py           # Batch processing tasks
│   └── pipeline/                     # Pipeline orchestration
│       └── etl_pipeline.py          # Main ETL pipeline orchestrator
├── extract_and_upload_dicom_reports.py  # Main entrypoint
├── queries/                          # SQL query definitions
├── sync/                             # Legacy modules (database, GCS service)
├── tasks.py                          # Legacy Celery tasks
└── celery_app.py                     # Celery application configuration
```

### ETL Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         ETL Pipeline Flow                         │
└─────────────────────────────────────────────────────────────────┘

1. INITIALIZATION
   └─> ETLPipeline.__init__()
       ├─> Initialize QueryExecutor
       ├─> Initialize BatchCreator
       ├─> Initialize GCSUploader
       └─> Create output directories

2. EXTRACTION PHASE
   └─> ETLPipeline.run()
       ├─> QueryExecutor.get_total_count()  [Get total records]
       │
       └─> For each page (parallel via Celery):
           └─> process_page_batch_task.s()
               ├─> QueryExecutor.fetch_page()  [Fetch page data]
               │
               └─> For each batch (parallel via Celery):
                   └─> process_batch_task.s()
                       ├─> DataProcessor.process_batch()  [Process data]
                       ├─> DataProcessor.prepare_csv_rows()  [Prepare CSV]
                       └─> DataFetcher.download_file()  [Download DICOM]

3. TRANSFORMATION PHASE (within batches)
   └─> DataProcessor.process_batch()
       ├─> Group files by study/series/instance
       ├─> Aggregate report fields at study level
       └─> Prepare CSV rows with metadata

4. LOADING PHASE
   └─> ETLPipeline.upload_to_gcs()
       ├─> GCSUploader.upload_file()  [Upload CSV]
       ├─> GCSUploader.upload_directory_contents()  [Upload DICOM files]
       └─> GCSUploader.upload_directory_contents()  [Upload JPG images]

5. COMPLETION
   └─> Aggregate statistics
       └─> Return summary with counts and timing
```

### Parallel Processing Architecture

```
Main Process (ETLPipeline)
│
├─> Dispatches page tasks to Celery (group of tasks)
│   │
│   └─> Celery Worker 1: process_page_batch_task (page 0)
│       └─> Dispatches batch tasks (group of tasks)
│           ├─> Celery Worker 1: process_batch_task (batch 0)
│           ├─> Celery Worker 2: process_batch_task (batch 1)
│           └─> Celery Worker 3: process_batch_task (batch 2)
│
├─> Celery Worker 2: process_page_batch_task (page 1)
│   └─> Dispatches batch tasks...
│
└─> Celery Worker N: process_page_batch_task (page N)
    └─> Dispatches batch tasks...

All tasks run in parallel, results are aggregated when complete.
```

## Repository Structure

```
vlm-eden-dataset-etl/
├── extract_and_upload_dicom_reports.py  # Main entrypoint
├── etl/                                  # ETL package (new modular structure)
│   ├── extract/                          # Extraction modules
│   ├── transform/                        # Transformation modules
│   ├── load/                             # Loading modules
│   ├── batching/                         # Batching utilities
│   ├── tasks/                            # Celery batch tasks
│   └── pipeline/                         # Pipeline orchestration
├── queries/                              # SQL query definitions
│   └── get_chest_dicom_files_and_reports.py
├── sync/                                 # Legacy modules
│   ├── database_breach.py               # Database connection bridge
│   └── gcs_service.py                   # GCS service (legacy)
├── tasks.py                             # Legacy Celery tasks
├── celery_app.py                        # Celery application configuration
├── celery_config.py                     # Celery task scheduling
├── database.py                          # Database connection utilities
├── run_worker.py                        # Celery worker entry point
├── Dockerfile                           # Docker image configuration
├── docker-compose.yml                   # Docker Compose configuration
├── Makefile                             # Make commands for development
├── requirements.txt                     # Python dependencies
├── requirements-dev.txt                 # Development dependencies
├── scripts/                              # Utility scripts
│   ├── lint.sh                          # Linting script
│   ├── lint-fix.sh                      # Auto-fix linting issues
│   ├── celery.sh                        # Celery worker script
│   └── flower.sh                        # Flower dashboard script
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
- Java JDK - for dcm4che (installed in Docker image)

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
- Google Cloud SDK (gsutil)
- Java JDK (for dcm4che support)
- All Python dependencies

```bash
# Build Docker images
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

### Running the ETL Pipeline

#### Basic Execution

```bash
# Run the extraction script inside the Docker container
docker exec celery_worker_intelligence python extract_and_upload_dicom_reports.py
```

The pipeline will:
1. **Extract**: Query database for DICOM files and reports
2. **Transform**: Process data, download files, prepare CSV
3. **Load**: Upload CSV, DICOM files, and JPG images to GCS

#### Configuration

The pipeline can be configured by modifying query parameters and batch sizes:

**Study Selection** (in `queries/get_chest_dicom_files_and_reports.py`):
```python
# Select 8 random studies
WITH all_studies AS (
  SELECT id
  FROM pacs_studies
  WHERE deleted = FALSE
  ORDER BY RANDOM()
  LIMIT 8
)
```

**Batch Sizes** (in `etl/pipeline/etl_pipeline.py`):
```python
PAGE_SIZE = 25    # Records per page from database
BATCH_SIZE = 25   # Records processed per batch
```

**Celery Configuration**:
- Tasks run in parallel across available Celery workers
- Each page is processed independently
- Batches within a page are processed in parallel
- Automatic retries on failure (max 3 retries)

### Understanding the Output

#### Console Output

The script provides detailed progress information:

```
======================================================================
Starting DICOM and reports extraction process with Celery batch processing
======================================================================
EXTRACTION STATISTICS
======================================================================
Total records: 200
Total pages: 8
Page size: 25
Batch size: 25
Starting from page: 1
Using Celery: True
Count query time: 0:00:00 (0.19 seconds)
======================================================================

Dispatching 8 page tasks to Celery workers...
Waiting for page processing to complete (task group: abc123...)
Processed 8/8 pages successfully

EXTRACTION PHASE COMPLETE
======================================================================
Extraction time: 0:00:45 (45.23 seconds)

Starting GCS upload...
CSV uploaded successfully
DICOM files uploaded successfully
JPG images uploaded successfully

EXTRACTION AND UPLOAD COMPLETE
======================================================================
TIMING SUMMARY:
  Count query: 0:00:00 (0.19s)
  Extraction: 0:00:45 (45.23s)
  Upload: 0:00:12 (12.06s)
  TOTAL TIME: 0:00:57 (57.48 seconds)

STATISTICS SUMMARY:
  Total records: 200
  Processed files: 200
  Files downloaded: 200
  Files uploaded: 200
  CSV rows: 200
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

The CSV file contains the following columns:

- `study_id`: Unique study identifier
- `series_number`: Series number within the study
- `instance_id`: Unique instance identifier (used as filename)
- `instance_number`: Instance number within the series
- `file_path`: Original file path in the source system
- `file_url`: URL to download the original DICOM file
- `report_value`: Extracted report text (aggregated at study level)
- `field_created_at`: Timestamp when the report field was created
- `downloaded`: Boolean indicating if the file was successfully downloaded
- `local_file_path`: Local path where the file was stored during processing

### Idempotency and Progress Tracking

The pipeline is **idempotent** - it can be safely rerun without causing data duplication:

- **File Key Tracking**: Each file is tracked by a unique key `(study_id, series_id, instance_id, file_path)`
- **Progress Persistence**: Progress is saved to `extraction_progress.json`
- **Resume Capability**: If interrupted, the pipeline resumes from the last checkpoint
- **Duplicate Prevention**: Already processed files are skipped automatically

### Task Retries and Error Handling

All Celery tasks include automatic retry logic:

- **Max Retries**: 3 attempts per task
- **Retry Delay**: 60 seconds (exponential backoff)
- **Error Logging**: Detailed error messages logged for debugging
- **Graceful Degradation**: Failed tasks return error information without crashing the pipeline

## Development

### Running Locally

```bash
# Start services
make up

# Access worker bash
make bash

# View logs
make logs

# Run linting
make lint

# Auto-fix linting issues
make lint-fix

# Rebuild after code changes
make build
make up
```

### Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=etl --cov-report=html

# Run specific test file
pytest tests/test_batching.py

# Run smoke test
pytest tests/test_smoke.py -v
```

### Code Quality

The project includes comprehensive static analysis:

- **Unused Import Detection**: Autoflake checks for unused imports
- **Code Formatting**: Black enforces consistent code style
- **Import Sorting**: isort organizes imports
- **Type Checking**: mypy performs static type analysis
- **Unused Code Detection**: vulture identifies potentially unused code

Run checks:
```bash
make lint          # Check for issues
make lint-fix       # Auto-fix issues
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

### Task Metrics

Each Celery task returns detailed metrics:
- Batch ID for tracking
- Records processed count
- Files found/downloaded/uploaded counts
- Success/failure status
- Error messages (if failed)

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

#### Celery Tasks Not Running

- Check Redis connection: `docker exec celery_worker_intelligence redis-cli ping`
- Verify Celery worker is running: `docker ps | grep celery`
- Check logs: `make logs`
- Restart services: `make down && make up`

#### Task Failures

- Check Flower dashboard for task details
- Review Celery worker logs for error messages
- Verify task retries are working (check retry count in Flower)
- Check database connectivity from worker

### Debugging

Enable debug logging:

```env
LOGGING_LEVEL=DEBUG
```

View detailed task execution in Flower dashboard or check Celery logs.

## Performance Considerations

- **Parallel Processing**: Pages and batches run in parallel via Celery workers
- **Pagination**: Large datasets are processed in pages to avoid database overload
- **Batching**: Records are processed in batches for efficient memory usage
- **Progress Tracking**: Script can resume from last checkpoint if interrupted
- **Idempotency**: Safe to rerun without data duplication

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

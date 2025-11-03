# VLM Eden Dataset ETL System

A specialized ETL system built with Python and Celery that extracts chest DICOM images and reports from a source PostgreSQL database and uploads them to AWS S3 for Vision Language Model (VLM) dataset creation.

## Overview

This system extracts eligible chest DICOM studies based on specific criteria and uploads them to S3 with organized metadata, reports, and DICOM files.

### Key Features

- **Automated Discovery**: Identifies eligible chest studies based on doctor rankings and study criteria
- **S3 Integration**: Organized file storage in S3 with structured metadata
- **Parallel Processing**: Celery groups for parallel study processing
- **Error Handling**: Robust error handling and logging
- **Scheduled Tasks**: Automated discovery and processing via Celery Beat

## Architecture

### DICOM Extraction Pipeline

The DICOM extraction pipeline:

- Discovers eligible chest studies (CR/DX modalities) signed by doctors ranked 2-6
- Extracts DICOM image files and associated reports
- Uploads data to S3 bucket `s3-bucket-name` with organized structure
- Tracks processing status and errors

### Components

- **Celery Tasks**: Asynchronous task execution for DICOM extraction
- **DICOM Pipeline**: Core extraction and processing logic
- **S3 Service**: Handles file uploads to S3 with organized structure
- **Database Bridge**: Manages connections to source database
- **Query Layer**: SQL queries for discovering eligible studies

## Prerequisites

- Python 3.10+
- PostgreSQL database (source database with PACS data)
- Redis (for Celery broker and result backend)
- AWS S3 bucket access
- Docker and Docker Compose (for local development)

## Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd vlm-eden-dataset-etl
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Environment Configuration

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

#### AWS S3 Configuration
```env
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION_NAME=us-east-1
S3_BUCKET_NAME=s3-bucket-name
```

#### Other Configuration
```env
SENTRY_DNS=your_sentry_dsn  # Optional, for error tracking
FLOWER_USER=admin
FLOWER_PASSWORD=your_password
SECRET_KEY=your_secret_key
LOGGING_LEVEL=INFO
```

### 4. Run with Docker Compose

```bash
# Initialize and start all services
make up

# Or manually:
docker-compose up -d
```

This will start:
- Redis (broker)
- Celery Worker
- Celery Beat (scheduler)
- Flower (task monitoring)

## Usage

### DICOM Extraction Pipeline

#### Overview

The DICOM extraction pipeline identifies eligible chest studies based on:
- **Modalities**: CR and DX only
- **Body Part**: Chest (including variations: chest, torax, thorax, pecho, peito)
- **Doctors**: Ranked 2-6 by total signed chest studies
- **Status**: Studies with signed, active, non-deleted reports

#### Pipeline Flow

1. **Discovery Phase** (`discover_chest_dicom_studies` task):
   - Queries for doctors ranked by total signed chest studies
   - Filters to doctors ranked 2-6
   - Finds eligible studies matching criteria
   - Queues processing tasks for each study

2. **Processing Phase** (`process_study_to_s3` task):
   - Extracts study metadata
   - Retrieves DICOM image files
   - Retrieves report content
   - Uploads everything to S3 with organized structure

#### Manual Execution

##### Discover and Queue Studies

```python
from tasks import discover_chest_dicom_studies

# Discover all eligible studies and queue them
result = discover_chest_dicom_studies.delay()

# With limit (process only first 100 studies)
result = discover_chest_dicom_studies.delay(limit=100)
```

##### Process Individual Study

```python
from tasks import process_study_to_s3

# Process a specific study
result = process_study_to_s3.delay(
    study_id="study-uuid-here"
)
```

##### Process Batch of Studies

```python
from tasks import process_study_batch_to_s3

# Process multiple studies in parallel
study_ids = ["uuid1", "uuid2", "uuid3"]
result = process_study_batch_to_s3.delay(study_ids)
```

#### Scheduled Tasks

The following task runs automatically via Celery Beat:

- **DICOM Discovery**: Daily at 2 AM - discovers and queues chest DICOM studies

#### S3 Structure

Files are organized in S3 with the following structure:

```
s3-bucket-name/
└── {organization_id}/
    └── {study_id}/
        ├── metadata.json          # Study metadata
        ├── reports/
        │   └── {report_id}.txt    # Report content
        └── dicom_files/
            └── {series_id}/
                ├── {instance_number:04d}_{image_id}.dcm
                └── ...
```

## Monitoring

### Flower Dashboard

Access the Flower dashboard for task monitoring:

```
http://localhost:5555/flower
```

Login with credentials from your `.env` file.

### Logs

View logs using Docker Compose:

```bash
make logs

# Or directly:
docker-compose logs -f celery_worker
```

### Task Status

Check task results in Redis or via Flower dashboard.

## Project Structure

```
vlm-eden-dataset-etl/
├── celery_app.py              # Celery application configuration
├── celery_config.py           # Celery task scheduling
├── database.py                # Database connection utilities
├── run_worker.py              # Celery worker entry point
├── tasks.py                   # Celery task definitions
├── queries/                   # SQL queries
│   └── chest_dicom_studies.py # DICOM extraction queries
├── sync/                      # Core modules
│   ├── database_breach.py     # Database connection bridge
│   ├── dicom_pipeline.py      # DICOM extraction pipeline
│   └── s3_service.py          # S3 upload service
├── scripts/                   # Shell scripts
├── docker-compose.yml         # Docker Compose configuration
├── Dockerfile                 # Docker image definition
├── Makefile                   # Make commands
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Development

### Running Locally

```bash
# Start services
make up

# Access worker bash
make bash

# Format code
make format

# View logs
make logs
```

### Code Structure

- Tasks are defined in `tasks.py` and decorated with `@app.task`
- DICOM extraction logic is in `sync/dicom_pipeline.py`
- SQL queries for study discovery are in `queries/chest_dicom_studies.py`
- S3 upload logic is in `sync/s3_service.py`

## Troubleshooting

### Common Issues

#### Database Connection Errors

- Verify database credentials in `.env`
- Check network connectivity to database server
- Ensure PostgreSQL is accepting connections

#### S3 Upload Failures

- Verify AWS credentials in `.env`
- Check S3 bucket permissions
- Ensure bucket exists: `s3-bucket-name`

#### Celery Tasks Not Running

- Check Redis connection
- Verify Celery worker is running: `docker ps`
- Check logs: `make logs`

#### DICOM Files Not Found

The pipeline expects DICOM files to be accessible via the `file_path` field in the `pacs_images` table. If your system stores DICOM files differently:

1. Modify `sync/dicom_pipeline.py` in the `_upload_dicom_files` method
2. Implement custom file retrieval logic based on your storage system
3. Update the query in `queries/chest_dicom_studies.py` if needed

### Debugging

Enable debug logging:

```env
LOGGING_LEVEL=DEBUG
```

View task details in Flower or check Celery logs.

## DICOM Pipeline Details

### Study Eligibility Criteria

A study is eligible for extraction if it meets ALL of the following:

1. **Modality**: CR or DX
2. **Body Part**: Contains "chest", "torax", "thorax", "pecho", or "peito" (case-insensitive)
   - Matched in `pacs_body_part.name` or `pacs_body_part.identifier`
   - Or in `pacs_studies.dicom_description`
3. **Report Status**: 
   - `pr.is_active = TRUE`
   - `pr.deleted = FALSE`
   - `pr.status = 'SIGNED'`
4. **Doctor Rank**: Signed by a doctor ranked 2-6 (by total signed chest studies)
5. **Organization**: Non-demo organization (`po.is_demo = FALSE`)
6. **Study Status**: Not migrated (`ps.migrated = FALSE`)

### Doctor Ranking

Doctors are ranked based on their total count of signed chest studies. Only studies signed by doctors ranked 2-6 are processed.

The ranking query includes all doctors who have signed chest studies (CR/DX/PX modalities), ordered by total count descending.

### Processing Results

Each study processing returns a dictionary:

```python
{
    "study_id": "uuid",
    "organization_id": "uuid",
    "success": True/False,
    "dicom_files_uploaded": 5,
    "reports_uploaded": 1,
    "metadata_uploaded": True,
    "errors": []
}
```

### Error Handling

- Failed uploads are logged but don't stop batch processing
- Individual study failures don't affect other studies
- Errors are collected in the result dictionary
- Check Flower dashboard or logs for detailed error messages

## Configuration

### Scheduling

Modify `celery_config.py` to adjust task schedules:

```python
beat_schedule = {
    "discover_chest_dicom_studies": {
        "task": "tasks.discover_chest_dicom_studies",
        "schedule": crontab(minute="0", hour="2"),  # Daily at 2 AM
    },
}
```

### Batch Size

Adjust concurrency in `run_worker.py`:

```python
worker = app.Worker(
    concurrency=2,  # Number of parallel workers
    max_tasks_per_child=10,
)
```

## Contributing

1. Follow existing code patterns
2. Add appropriate logging
3. Include error handling
4. Update documentation
5. Test thoroughly

## License

[Your License Here]

## Support

For issues or questions:
- Check logs: `make logs`
- Review Flower dashboard
- Check AWS S3 bucket permissions
- Verify database connections

## Additional Notes

### DICOM File Storage

The current implementation assumes DICOM files are stored with file paths accessible to the ETL service. If your system uses:
- **Different storage location**: Modify `_upload_dicom_files` in `sync/dicom_pipeline.py`
- **Different table structure**: Update queries in `queries/chest_dicom_studies.py`
- **External API**: Implement API client in `sync/dicom_pipeline.py`

### Performance Considerations

- Large batches may take time to process
- Monitor S3 upload costs
- Consider rate limiting for very large batches
- Use Celery concurrency settings to control parallel processing

### Security

- Never commit `.env` files
- Use IAM roles for AWS access when possible
- Rotate AWS credentials regularly
- Limit S3 bucket access to necessary permissions only

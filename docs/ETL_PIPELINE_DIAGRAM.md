# ETL Pipeline Architecture Diagram

## High-Level Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    ETL Pipeline - Complete Flow                       │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────┐
│   Main CLI  │  extract_and_upload_dicom_reports.py
│  Entrypoint │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    ETLPipeline.run()                                  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 1. INITIALIZATION                                             │  │
│  │    ├─> QueryExecutor.get_total_count()                      │  │
│  │    ├─> Load progress from extraction_progress.json            │  │
│  │    └─> Calculate total pages                                  │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 2. EXTRACTION PHASE (Parallel via Celery)                    │  │
│  │                                                               │  │
│  │    ┌────────────────────────────────────────────────────┐    │  │
│  │    │ Dispatch Page Tasks (Celery Group)                │    │  │
│  │    │                                                    │    │  │
│  │    │  ┌──────────────┐  ┌──────────────┐  ┌─────────┐ │    │  │
│  │    │  │ Page Task 0  │  │ Page Task 1  │  │ Page N  │ │    │  │
│  │    │  │ (Worker 1)   │  │ (Worker 2)   │  │ (Worker)│ │    │  │
│  │    │  └──────┬───────┘  └──────┬───────┘  └────┬────┘ │    │  │
│  │    │         │                  │                │      │    │  │
│  │    │         ▼                  ▼                ▼      │    │  │
│  │    │    ┌─────────┐      ┌─────────┐      ┌─────────┐ │    │  │
│  │    │    │ Fetch    │      │ Fetch    │      │ Fetch    │ │    │  │
│  │    │    │ Page 0   │      │ Page 1   │      │ Page N   │ │    │  │
│  │    │    └────┬─────┘      └────┬─────┘      └────┬─────┘ │    │  │
│  │    │         │                  │                │      │    │  │
│  │    │         ▼                  ▼                ▼      │    │  │
│  │    │    ┌──────────────────────────────────────────┐   │    │  │
│  │    │    │ Dispatch Batch Tasks (Celery Group)      │   │    │  │
│  │    │    │                                          │   │    │  │
│  │    │    │  ┌──────┐  ┌──────┐  ┌──────┐          │   │    │  │
│  │    │    │  │Batch │  │Batch │  │Batch │          │   │    │  │
│  │    │    │  │Task 0│  │Task 1│  │Task N│          │   │    │  │
│  │    │    │  └───┬──┘  └───┬──┘  └───┬──┘          │   │    │  │
│  │    │    └──────┼──────────┼──────────┼────────────┘   │    │  │
│  │    │           │          │          │                │    │  │
│  │    │           ▼          ▼          ▼                │    │  │
│  │    │    ┌──────────────────────────────────────┐     │    │  │
│  │    │    │ process_batch_task()                 │     │    │  │
│  │    │    │  ├─> DataProcessor.process_batch()   │     │    │  │
│  │    │    │  ├─> DataProcessor.prepare_csv_rows()│     │    │  │
│  │    │    │  └─> DataFetcher.download_file()     │     │    │  │
│  │    │    └──────────────────────────────────────┘     │    │  │
│  │    └────────────────────────────────────────────────────┘    │  │
│  │                                                               │  │
│  │    ┌────────────────────────────────────────────────────┐    │  │
│  │    │ Aggregate Results from All Pages                   │    │  │
│  │    │  ├─> Collect CSV rows                              │    │  │
│  │    │  ├─> Update processed_file_keys                    │    │  │
│  │    │  └─> Calculate statistics                          │    │  │
│  │    └────────────────────────────────────────────────────┘    │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 3. LOADING PHASE                                             │  │
│  │    ├─> GCSUploader.upload_file() [CSV]                     │  │
│  │    ├─> GCSUploader.upload_directory_contents() [DICOM]      │  │
│  │    └─> GCSUploader.upload_directory_contents() [JPG]       │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 4. COMPLETION                                                │  │
│  │    ├─> Save final progress                                  │  │
│  │    ├─> Aggregate statistics                                 │  │
│  │    └─> Return summary                                       │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Data Flow Through Pipeline                    │
└─────────────────────────────────────────────────────────────────────┘

Database (PostgreSQL)
    │
    │ SQL Query
    ▼
┌─────────────────┐
│ QueryExecutor   │  Fetch pages of query results
└────────┬────────┘
         │
         │ List[Dict] (query results)
         ▼
┌─────────────────┐
│ BatchCreator    │  Split into batches
└────────┬────────┘
         │
         │ List[List[Dict]] (batches)
         ▼
┌─────────────────┐
│ DataProcessor   │  Process batch
│                 │  ├─> Group by file keys
│                 │  ├─> Aggregate report fields
│                 │  └─> Prepare CSV rows
└────────┬────────┘
         │
         │ Dict (file_data, report_fields, csv_rows)
         ▼
┌─────────────────┐
│ DataFetcher     │  Download DICOM files
└────────┬────────┘
         │
         │ Downloaded files
         ▼
┌─────────────────┐
│ CSV Writer     │  Save CSV rows to file
└────────┬────────┘
         │
         │ CSV file + DICOM files
         ▼
┌─────────────────┐
│ GCSUploader    │  Upload to GCS
│                 │  ├─> CSV file
│                 │  ├─> DICOM files
│                 │  └─> JPG images (if converted)
└────────┬────────┘
         │
         ▼
    GCS Bucket
```

## Celery Task Execution Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Celery Task Execution Flow                        │
└─────────────────────────────────────────────────────────────────────┘

Main Process
    │
    │ Dispatch group of page tasks
    ▼
┌─────────────────────────────────────────────────────────────┐
│ Celery Group (process_page_batch_task)                      │
│                                                              │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐           │
│  │ Worker 1   │  │ Worker 2   │  │ Worker N   │           │
│  │ Page 0     │  │ Page 1     │  │ Page N     │           │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘           │
│        │               │               │                   │
│        │ Dispatch batch group          │                   │
│        ▼               ▼               ▼                   │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐         │
│  │ Batch Group │ │ Batch Group │ │ Batch Group │         │
│  │ (Page 0)    │ │ (Page 1)    │ │ (Page N)    │         │
│  └─────┬───────┘ └─────┬───────┘ └─────┬───────┘         │
│        │               │               │                   │
│        │               │               │                   │
│        ▼               ▼               ▼                   │
│  ┌─────────┐     ┌─────────┐     ┌─────────┐             │
│  │ Batch 0 │     │ Batch 0 │     │ Batch 0 │             │
│  │ Batch 1 │     │ Batch 1 │     │ Batch 1 │             │
│  │ Batch N │     │ Batch N │     │ Batch N │             │
│  └─────────┘     └─────────┘     └─────────┘             │
│                                                              │
│  ┌──────────────────────────────────────────────┐          │
│  │ Aggregate Results                            │          │
│  │  ├─> Collect CSV rows from all batches      │          │
│  │  ├─> Update processed_file_keys             │          │
│  │  └─> Return page statistics                  │          │
│  └──────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────┘
    │
    │ All page results
    ▼
Main Process
    │
    │ Aggregate all page results
    ▼
Final Summary
```

## Idempotency Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Idempotency Mechanism                           │
└─────────────────────────────────────────────────────────────────────┘

Start Pipeline
    │
    ▼
┌─────────────────┐
│ Load Progress   │  extraction_progress.json
│                 │  ├─> current_page
│                 │  ├─> processed_file_keys
│                 │  └─> processed_file_count
└────────┬────────┘
         │
         │ Set of processed_file_keys
         ▼
┌─────────────────┐
│ Process Batch   │
│                 │  For each file:
│                 │    ├─> Generate file_key = (study_id, series_id,
│                 │    │                    instance_id, file_path)
│                 │    │
│                 │    ├─> Check if file_key in processed_file_keys
│                 │    │
│                 │    ├─> If YES: Skip (already processed)
│                 │    │
│                 │    └─> If NO: Process and add to processed_file_keys
└────────┬────────┘
         │
         │ Updated processed_file_keys
         ▼
┌─────────────────┐
│ Save Progress   │  Update extraction_progress.json
└─────────────────┘

Result: Safe to rerun - duplicates are automatically skipped
```

## Error Handling and Retries

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Error Handling and Retries                         │
└─────────────────────────────────────────────────────────────────────┘

Task Execution
    │
    ▼
┌─────────────────┐
│ Execute Task    │
└────────┬────────┘
         │
         ├─> SUCCESS ──────────────► Return result
         │
         └─> FAILURE
                │
                ▼
         ┌─────────────────┐
         │ Check Retries   │  retry_count < max_retries (3)?
         └────────┬────────┘
                  │
         ┌────────┴────────┐
         │                 │
         ▼                 ▼
    YES (retry)        NO (max retries exceeded)
         │                 │
         │                 ▼
         │         ┌─────────────────┐
         │         │ Return Error    │  With error details
         │         │ Result          │
         │         └─────────────────┘
         │
         ▼
┌─────────────────┐
│ Wait (backoff)  │  default_retry_delay * (2 ^ retry_count)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Retry Task      │  Return to "Execute Task"
└─────────────────┘
```

## Module Responsibilities

### Extract Module
- **QueryExecutor**: Executes SQL queries, handles pagination
- **DataFetcher**: Downloads files from URLs

### Transform Module
- **DataProcessor**: Processes query results, groups data, prepares CSV rows
- **DICOMConverter**: Converts DICOM files to JPG (optional, via dcm4che)

### Load Module
- **GCSUploader**: Uploads files to GCS bucket

### Batching Module
- **BatchCreator**: Splits data into manageable batches

### Tasks Module
- **process_batch_task**: Processes a single batch (extract, transform, download)
- **process_page_batch_task**: Processes a page by splitting into batches
- **download_and_convert_batch_task**: Downloads and converts DICOM files (optional)

### Pipeline Module
- **ETLPipeline**: Orchestrates the complete ETL process


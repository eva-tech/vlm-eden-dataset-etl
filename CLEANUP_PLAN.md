# Repository Cleanup Plan: Remove Intelligence-ETL Components

## Overview
This plan identifies and removes all SQL queries, migrations, and code related to the old `intelligence-etl` system, keeping only the components needed for the `vlm-eden-dataset-etl` DICOM extraction pipeline.

## Current System Analysis

### VLM Eden Dataset ETL (KEEP)
The DICOM extraction pipeline uses:
- `queries/chest_dicom_studies.py` - DICOM study queries
- `sync/dicom_pipeline.py` - DICOM extraction logic
- `sync/s3_service.py` - S3 upload service
- `sync/database_breach.py` - Database connection utility (standalone)
- `tasks.py` - DICOM tasks: `discover_chest_dicom_studies`, `process_study_to_s3`, `process_study_batch_to_s3`
- `celery_config.py` - Celery schedule (DICOM task scheduled)

### Intelligence-ETL (REMOVE)
The old intelligence data warehouse system uses:
- Dimension table sync modules and queries
- Fact table sync modules and queries
- Organization schema migrations
- Intelligence-specific cron tasks

---

## Files to Remove

### 1. Migrations Folder (`migrations/`)
**Status**: REMOVE ALL
- All 12 SQL migration files create tables for the intelligence data warehouse:
  - `dim_calendar`, `dim_facilities`, `dim_modalities`, `dim_practitioners`, `dim_technicians`
  - `fact_studies`
  - Various views and indexes
- **Reason**: These are for the intelligence data warehouse, not needed for DICOM extraction
- **Files**:
  - `20221214_01_9Jm6L.sql`
  - `20230104_01_r9HiK.sql`
  - `20230207_01_53Bud.sql`
  - `20230301_01_29RPQ.sql`
  - `20230426_01_BozYM.sql`
  - `20230510_01_V14Kk.sql`
  - `20230606_01_cY4nG.sql`
  - `20230627_01_81uau.sql`
  - `20241029_01_JnkId.sql`
  - `20241204_01_xSnrF.sql`
  - `20250103_01_AhiOW.sql`
  - `20250930_01_jmdvz.sql`

### 2. Queries Folder (`queries/`)
**Status**: REMOVE MOST, KEEP ONE
- **KEEP**: 
  - `chest_dicom_studies.py` ✅ (used by DICOM pipeline)
- **REMOVE**:
  - `dim_facitities.py` ❌ (used by `sync/facilities.py`)
  - `dim_modalities.py` ❌ (used by `sync/modalities.py`)
  - `dim_practitioners.py` ❌ (used by `sync/practitioners.py`)
  - `dim_technicians.py` ❌ (used by `sync/technicians.py`)
  - `fact_studies.py` ❌ (used by `sync/studies.py` and `sync/sync_validator.py`)
  - `schema_organizations.py` ❌ (used by `cron_tasks.py` for intelligence)
  - `sync_records.py` ❌ (used by `sync/sync_base.py`)

### 3. Sync Modules (`sync/`)
**Status**: REMOVE MOST, KEEP THREE
- **KEEP**:
  - `dicom_pipeline.py` ✅ (DICOM extraction)
  - `s3_service.py` ✅ (S3 uploads)
  - `database_breach.py` ✅ (standalone DB utility)
- **REMOVE**:
  - `facilities.py` ❌ (syncs dimension tables)
  - `modalities.py` ❌ (syncs dimension tables)
  - `practitioners.py` ❌ (syncs dimension tables)
  - `studies.py` ❌ (syncs fact tables)
  - `organizations.py` ❌ (syncs organizations for intelligence)
  - `technicians.py` ❌ (syncs dimension tables)
  - `sync_base.py` ❌ (base class for intelligence sync modules)
  - `sync_validator.py` ❌ (validates intelligence sync)
  - `constants.py` ❌ (only used by intelligence sync modules - verified)

### 4. Tasks File (`tasks.py`)
**Status**: MODIFY
- **KEEP**:
  - `discover_chest_dicom_studies` ✅
  - `process_study_to_s3` ✅
  - `process_study_batch_to_s3` ✅
- **REMOVE**:
  - `sync_data_from_by_organization` ❌
  - `sync_pending_data_by_organization` ❌
  - `sync_organizations` ❌

### 5. Cron Tasks (`cron_tasks.py`)
**Status**: ❌ DELETE ENTIRELY
- **Decision**: ✅ DELETE - All functions are intelligence-related
- `apply_migrations()` runs `general_migrations/` which we're removing
- All other tasks are intelligence ETL tasks
- DICOM tasks are scheduled directly in `celery_config.py`
- **Also update**:
  - `celery_app.py` - Remove `cron_tasks` from `CELERY_IMPORTS`
  - `run_worker.py` - Remove `apply_migrations()` call
  - `celery_config.py` - Remove intelligence tasks from `beat_schedule` (already identified)

### 6. General Migrations (`general_migrations/`)
**Status**: ❌ REMOVE
- `20240408_01_0CV2Y.sql` - Creates `organizations` table in DESTINATION database
- **Decision**: ✅ REMOVE - DICOM pipeline queries `pacs_organizations` from SOURCE database, not destination `organizations` table
- The destination `organizations` table is only used by intelligence ETL system

### 7. Configuration Files
**Status**: UPDATE
- `celery_config.py` - Remove intelligence ETL tasks from `beat_schedule`, keep only DICOM task
- `docker-compose.yml` - Update container names (remove "intelligence" references)
- `Makefile` - Update references (remove "intelligence" references)
- `Dockerfile` - Update WORKDIR (remove "intelligence" reference)

### 8. Other Files
**Status**: UPDATE OR REMOVE
- `utils.py` - ❌ REMOVE (not used by DICOM pipeline, verified)
- `README.md` - UPDATE to remove intelligence ETL references, focus on DICOM pipeline
- `yoyo.ini` / `yoyog.ini` - Review if still needed (likely only for intelligence migrations)

---

## Implementation Steps

### Phase 1: Backup & Verification
1. ✅ Create backup branch
2. ✅ Verify DICOM pipeline works before cleanup
3. ✅ Document current dependencies

### Phase 2: Remove Files
1. Delete all migration files in `migrations/` (12 files)
2. Delete general migration file in `general_migrations/` (1 file)
3. Delete intelligence query files in `queries/` - keep `chest_dicom_studies.py` (6 files)
4. Delete intelligence sync modules in `sync/` - keep `dicom_pipeline.py`, `s3_service.py`, `database_breach.py` (8 files)
5. Delete `utils.py` (not used by DICOM pipeline)
6. Delete `cron_tasks.py` (all intelligence-related)

### Phase 3: Update Code Files
1. Clean `tasks.py` - remove intelligence tasks and imports
2. Update `celery_app.py` - remove `cron_tasks` from CELERY_IMPORTS
3. Update `run_worker.py` - remove `apply_migrations()` import and call
4. Update `celery_config.py` - remove intelligence tasks from schedule
5. Update imports in remaining files

### Phase 4: Update Configuration
1. Update `docker-compose.yml` - rename containers
2. Update `Makefile` - update references
3. Update `Dockerfile` - update WORKDIR
4. Update `README.md` - focus on DICOM pipeline

### Phase 5: Testing
1. Verify DICOM pipeline still works
2. Test all Celery tasks
3. Check for any broken imports
4. Run linter

---

## Dependencies to Verify

Before removing, verify these are NOT used by DICOM pipeline:
- [x] `sync/constants.py` - ✅ Verified: Only used by intelligence sync modules
- [x] `utils.py` - ✅ Verified: Not used by DICOM pipeline (only used by intelligence tasks)
- [ ] `general_migrations/` - Check if `organizations` table is queried by DICOM pipeline
- [ ] `yoyo.ini` / `yoyog.ini` - Check if needed (likely only for intelligence migrations)

---

## Risk Assessment

### Low Risk
- Removing migration files (not executed by DICOM pipeline)
- Removing intelligence query files (not imported by DICOM code)
- Removing intelligence sync modules (not used by DICOM pipeline)

### Medium Risk
- Removing `cron_tasks.py` functions (verify no external dependencies)
- Updating configuration files (may break Docker setup temporarily)

### High Risk
- `general_migrations/` - May contain shared infrastructure
- `utils.py` - May contain shared utilities

---

## Questions Resolved ✅

1. **General Migrations**: ✅ REMOVE - DICOM pipeline queries `pacs_organizations` from SOURCE database, not destination `organizations` table
2. **Cron Tasks**: ✅ DELETE ENTIRELY - All functions are intelligence-related, `apply_migrations()` only runs `general_migrations/` which we're removing
3. **Utils**: ✅ REMOVE - Verified not used by DICOM pipeline
4. **Yoyo Config**: ⚠️ REVIEW - `yoyo.ini` and `yoyog.ini` are likely only needed for migrations, but keep for now if migrations might be re-added later

---

## Summary

**Files to Delete**: ~29 files
- 12 migration SQL files (`migrations/`)
- 1 general migration SQL file (`general_migrations/`)
- 6 query Python files (`queries/` - except `chest_dicom_studies.py`)
- 8 sync module Python files (`sync/` - except DICOM pipeline files)
- `utils.py` (not used by DICOM pipeline)
- `cron_tasks.py` (all intelligence-related, can be deleted entirely)

**Files to Modify**: ~8 files
- `tasks.py` (remove intelligence tasks)
- `celery_app.py` (remove `cron_tasks` from CELERY_IMPORTS)
- `run_worker.py` (remove `apply_migrations()` call)
- `celery_config.py` (remove intelligence tasks from schedule)
- `docker-compose.yml` (update container names)
- `Makefile` (update references)
- `Dockerfile` (update WORKDIR)
- `README.md` (focus on DICOM pipeline)

**Files to Keep**: Core DICOM pipeline
- `queries/chest_dicom_studies.py`
- `sync/dicom_pipeline.py`
- `sync/s3_service.py`
- `sync/database_breach.py`
- `tasks.py` (DICOM tasks only)
- `celery_config.py` (updated)
- `database.py`
- `celery_app.py`


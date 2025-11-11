# SQL Queries and Database Tables Reference

This document lists all SQL queries and database tables used in the VLM Eden Dataset ETL project.

## Database Tables

The following tables are used across all queries:

### Core Tables
1. **`pacs_reports`** (alias: `pr`)
   - Primary table for medical reports
   - Columns used: `id`, `study_id`, `signed_by_id`, `is_active`, `deleted`, `status`, `signed_at`, `created_at`, `updated_at`

2. **`pacs_studies`** (alias: `ps`)
   - Primary table for DICOM studies
   - Columns used: `id`, `organization_id`, `modalities`, `dicom_description`, `created_at`, `updated_at`, `dicom_date_time`

3. **`pacs_practitioners`** (alias: `pp`)
   - Table for medical practitioners/doctors
   - Columns used: `id`, `user_id`

4. **`pacs_users`** (alias: `pu`)
   - Table for user accounts
   - Columns used: `id`, `email`, `full_name`

5. **`pacs_organizations`** (alias: `po`)
   - Table for healthcare organizations
   - Columns used: `id`, `slug`

6. **`pacs_series`** (alias: `se`)
   - Table for DICOM series within studies
   - Columns used: `id`, `study_id`, `series_number`, `modality`, `body_part_id`, `created_at`

7. **`pacs_body_part`** (alias: `pbp`)
   - Table for body part classifications
   - Columns used: `id`, `name`, `identifier`

8. **`pacs_images`** (alias: `pi`)
   - Table for DICOM image files
   - Columns used: `id`, `series_id`, `instance_number`, `file_path`, `file_size`, `created_at`

---

## SQL Queries

### 1. `get_ranked_doctors`
**Purpose:** Get doctors ranked by total signed chest studies

**Location:** `queries/chest_dicom_studies.py`

**Tables Used:**
- `pacs_reports` (pr) - JOIN
- `pacs_practitioners` (pp) - JOIN
- `pacs_users` (pu) - JOIN
- `pacs_studies` (ps) - JOIN
- `pacs_organizations` (po) - LEFT JOIN
- `pacs_series` (se) - LEFT JOIN
- `pacs_body_part` (pbp) - LEFT JOIN

**Query:**
```sql
SELECT
  pu.id as user_id,
  pu.email,
  (array_agg(distinct pu.full_name))[1] as full_name,
  COUNT(DISTINCT ps.id) AS total_signed_chest_studies,
  ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT ps.id) DESC) as doctor_rank
FROM
  pacs_reports AS pr
JOIN
  pacs_practitioners AS pp ON pp.id = pr.signed_by_id
JOIN
  pacs_users AS pu ON pu.id = pp.user_id
JOIN
  pacs_studies AS ps ON ps.id = pr.study_id
LEFT JOIN
  pacs_organizations AS po ON po.id = ps.organization_id
LEFT JOIN
  pacs_series AS se ON se.study_id = ps.id
LEFT JOIN
  pacs_body_part AS pbp ON pbp.id = se.body_part_id
WHERE
  pr.is_active = TRUE
  AND pr.deleted = FALSE
  AND pr.status = 'SIGNED'
  AND (ps.modalities ILIKE '%DX%' OR ps.modalities ILIKE '%CR%')
  AND (
    pbp.name ILIKE '%torax%'
    OR pbp.name ILIKE '%thorax%'
    OR pbp.name ILIKE '%chest%'
    OR pbp.name ILIKE '%pecho%'
    OR pbp.name ILIKE '%peito%'
    OR pbp.identifier ILIKE '%torax%'
    OR pbp.identifier ILIKE '%thorax%'
    OR pbp.identifier ILIKE '%chest%'
    OR pbp.identifier ILIKE '%pecho%'
    OR pbp.identifier ILIKE '%peito%'
    OR ps.dicom_description ILIKE '%torax%'
    OR ps.dicom_description ILIKE '%thorax%'
    OR ps.dicom_description ILIKE '%chest%'
    OR ps.dicom_description ILIKE '%pecho%'
    OR ps.dicom_description ILIKE '%peito%'
    OR ps.dicom_description ILIKE '%tórax%'
  )
GROUP BY
  pu.id, pu.email
ORDER BY
  total_signed_chest_studies DESC
LIMIT 50;
```

**Filters:**
- Active, non-deleted, signed reports
- Modalities: CR or DX only
- Body part: chest-related terms (torax, thorax, chest, pecho, peito)
- Returns top 50 doctors ranked by total signed chest studies

---

### 2. `get_eligible_chest_studies`
**Purpose:** Get eligible chest DICOM studies signed by doctors ranked 2-6

**Location:** `queries/chest_dicom_studies.py`

**Tables Used:**
- `pacs_reports` (pr) - JOIN (in CTE and main query)
- `pacs_practitioners` (pp) - JOIN
- `pacs_users` (pu) - JOIN
- `pacs_studies` (ps) - JOIN
- `pacs_organizations` (po) - LEFT JOIN
- `pacs_series` (se) - LEFT JOIN
- `pacs_body_part` (pbp) - LEFT JOIN

**Query Structure:**
```sql
WITH ranked_doctors AS (
  -- Subquery: Same as get_ranked_doctors (lines 59-106)
  SELECT
    pu.id as user_id,
    pu.email,
    (array_agg(distinct pu.full_name))[1] as full_name,
    COUNT(DISTINCT ps.id) AS total_signed_chest_studies,
    ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT ps.id) DESC) as doctor_rank
  FROM
    pacs_reports AS pr
  JOIN pacs_practitioners AS pp ON pp.id = pr.signed_by_id
  JOIN pacs_users AS pu ON pu.id = pp.user_id
  JOIN pacs_studies AS ps ON ps.id = pr.study_id
  LEFT JOIN pacs_organizations AS po ON po.id = ps.organization_id
  LEFT JOIN pacs_series AS se ON se.study_id = ps.id
  LEFT JOIN pacs_body_part AS pbp ON pbp.id = se.body_part_id
  WHERE
    pr.is_active = TRUE
    AND pr.deleted = FALSE
    AND pr.status = 'SIGNED'
    AND (ps.modalities ILIKE '%DX%' OR ps.modalities ILIKE '%CR%')
    AND (body part filters...)
  GROUP BY pu.id, pu.email
  ORDER BY total_signed_chest_studies DESC
  LIMIT 50
),
eligible_doctors AS (
  SELECT user_id, email, full_name, doctor_rank
  FROM ranked_doctors
  WHERE doctor_rank BETWEEN 2 AND 6
)
SELECT DISTINCT
  ps.id as study_id,
  ps.organization_id,
  po.slug as organization_slug,
  ps.modalities,
  ps.dicom_description,
  ps.created_at,
  ps.updated_at,
  ps.dicom_date_time,
  pr.id as report_id,
  NULL as report_content,
  pr.signed_at,
  pr.created_at as report_created_at,
  pp.id as signed_by_practitioner_id,
  pu.id as signed_by_user_id,
  pu.full_name as signed_by_name,
  pu.email as signed_by_email,
  rd.doctor_rank,
  pbp.name as body_part_name,
  pbp.identifier as body_part_identifier
FROM
  pacs_reports AS pr
JOIN pacs_practitioners AS pp ON pp.id = pr.signed_by_id
JOIN pacs_users AS pu ON pu.id = pp.user_id
JOIN eligible_doctors AS ed ON ed.user_id = pu.id
JOIN ranked_doctors AS rd ON rd.user_id = pu.id
JOIN pacs_studies AS ps ON ps.id = pr.study_id
LEFT JOIN pacs_organizations AS po ON po.id = ps.organization_id
LEFT JOIN pacs_series AS se ON se.study_id = ps.id
LEFT JOIN pacs_body_part AS pbp ON pbp.id = se.body_part_id
WHERE
  pr.is_active = TRUE
  AND pr.deleted = FALSE
  AND pr.status = 'SIGNED'
  AND ps.modalities IN ('CR', 'DX')
  AND (
    pbp.name ILIKE '%chest%'
    OR ps.dicom_description ILIKE '%chest%'
    OR pbp.name ILIKE '%torax%'
    OR pbp.name ILIKE '%thorax%'
    OR pbp.name ILIKE '%pecho%'
    OR pbp.name ILIKE '%peito%'
  )
ORDER BY
  ps.created_at DESC;
```

**Filters:**
- Only doctors ranked 2-6 (excludes rank 1)
- Active, non-deleted, signed reports
- Modalities: CR or DX (exact match)
- Body part: chest-related terms
- Returns distinct studies ordered by creation date (newest first)

---

### 3. `get_dicom_images_for_study`
**Purpose:** Get DICOM images for a specific study

**Location:** `queries/chest_dicom_studies.py`

**Tables Used:**
- `pacs_images` (pi) - FROM
- `pacs_series` (se) - JOIN
- `pacs_body_part` (pbp) - LEFT JOIN

**Query:**
```sql
SELECT
  pi.id as image_id,
  pi.series_id,
  pi.instance_number,
  pi.file_path,
  pi.file_size,
  pi.created_at,
  se.series_number,
  se.modality,
  se.body_part_id,
  pbp.name as body_part_name
FROM
  pacs_images AS pi
JOIN
  pacs_series AS se ON se.id = pi.series_id
LEFT JOIN
  pacs_body_part AS pbp ON pbp.id = se.body_part_id
WHERE
  se.study_id = %(study_id)s::uuid
  AND se.modality IN ('CR', 'DX')
ORDER BY
  se.series_number, pi.instance_number;
```

**Parameters:**
- `study_id` (UUID) - Required parameter

**Filters:**
- Modalities: CR or DX only
- Ordered by series number and instance number

---

### 4. `get_series_for_study`
**Purpose:** Get series metadata for a study (alternative to get_dicom_images_for_study)

**Location:** `queries/chest_dicom_studies.py`

**Tables Used:**
- `pacs_series` (se) - FROM
- `pacs_body_part` (pbp) - LEFT JOIN

**Query:**
```sql
SELECT
  se.id as series_id,
  se.study_id,
  se.series_number,
  se.modality,
  se.body_part_id,
  pbp.name as body_part_name,
  pbp.identifier as body_part_identifier,
  se.created_at
FROM
  pacs_series AS se
LEFT JOIN
  pacs_body_part AS pbp ON pbp.id = se.body_part_id
WHERE
  se.study_id = %(study_id)s::uuid
  AND se.modality IN ('CR', 'DX')
ORDER BY
  se.series_number;
```

**Parameters:**
- `study_id` (UUID) - Required parameter

**Filters:**
- Modalities: CR or DX only
- Ordered by series number

---

### 5. `get_report_for_study`
**Purpose:** Get report content for a specific study

**Location:** `queries/chest_dicom_studies.py`

**Tables Used:**
- `pacs_reports` (pr) - FROM
- `pacs_practitioners` (pp) - JOIN
- `pacs_users` (pu) - JOIN

**Query:**
```sql
SELECT
  pr.id,
  pr.study_id,
  NULL as content,
  pr.status,
  pr.signed_at,
  pr.signed_by_id,
  pu.full_name as signed_by_name,
  pu.email as signed_by_email,
  pr.created_at,
  pr.updated_at
FROM
  pacs_reports AS pr
JOIN
  pacs_practitioners AS pp ON pp.id = pr.signed_by_id
JOIN
  pacs_users AS pu ON pu.id = pp.user_id
WHERE
  pr.study_id = %(study_id)s::uuid
  AND pr.is_active = TRUE
  AND pr.deleted = FALSE
  AND pr.status = 'SIGNED'
ORDER BY
  pr.created_at DESC
LIMIT 1;
```

**Parameters:**
- `study_id` (UUID) - Required parameter

**Filters:**
- Active, non-deleted, signed reports
- Returns most recent report (LIMIT 1)

---

## Table Relationships

```
pacs_reports (pr)
  ├─→ pacs_practitioners (pp) [pr.signed_by_id = pp.id]
  │     └─→ pacs_users (pu) [pp.user_id = pu.id]
  └─→ pacs_studies (ps) [pr.study_id = ps.id]
        ├─→ pacs_organizations (po) [ps.organization_id = po.id]
        └─→ pacs_series (se) [se.study_id = ps.id]
              ├─→ pacs_body_part (pbp) [se.body_part_id = pbp.id]
              └─→ pacs_images (pi) [pi.series_id = se.id]
                    └─→ pacs_body_part (pbp) [se.body_part_id = pbp.id]
```

---

## Query Usage in Code

### `sync/dicom_pipeline.py`

1. **StudyDiscovery.get_ranked_doctors()**
   - Uses: `get_ranked_doctors`

2. **StudyDiscovery.get_eligible_studies(limit=None)**
   - Uses: `get_eligible_chest_studies`
   - Applies LIMIT clause if provided

3. **StudyProcessor._upload_dicom_files()**
   - Uses: `get_dicom_images_for_study`
   - Falls back to: `get_series_for_study` if no images found

4. **StudyProcessor._upload_report()**
   - Uses: `get_report_for_study` (indirectly via study_data)

---

## Summary Statistics

- **Total Queries:** 5
- **Total Tables:** 8
- **Most Used Table:** `pacs_reports` (used in 4 queries)
- **Most Complex Query:** `get_eligible_chest_studies` (uses CTEs, multiple joins, subqueries)

---

## Notes

1. **Column Issues Fixed:**
   - `ps.migrated` - Removed (column doesn't exist)
   - `pr.content` - Changed to `NULL as report_content` (column doesn't exist)
   - `pp.full_name` - Changed to `pu.full_name` (correct table reference)

2. **Modality Filters:**
   - All queries now filter for CR and DX only (PX removed)

3. **Body Part Matching:**
   - Supports multiple languages: English (chest, thorax), Spanish (torax, pecho), Portuguese (peito)
   - Matches against: `pbp.name`, `pbp.identifier`, `ps.dicom_description`


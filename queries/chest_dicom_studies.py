"""This file contains queries related to chest DICOM studies extraction for specific doctors."""

# Query to get doctors ranked by total signed chest studies
get_ranked_doctors = """
SELECT
  pu.id as user_id,
  pu.email,
  (array_agg(distinct pp.full_name))[1] as full_name,
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
  AND po.is_demo = FALSE
  AND ps.migrated = FALSE
  AND (ps.modalities ILIKE '%DX%' OR ps.modalities ILIKE '%CR%' OR ps.modalities ILIKE '%PX%')
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
"""

# Query to get eligible studies signed by doctors ranked 2-6
# This query filters for CR/DX modalities, chest body parts, and specific ranked doctors
get_eligible_chest_studies = """
WITH ranked_doctors AS (
  SELECT
    pu.id as user_id,
    pu.email,
    (array_agg(distinct pp.full_name))[1] as full_name,
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
    AND po.is_demo = FALSE
    AND ps.migrated = FALSE
    AND (ps.modalities ILIKE '%DX%' OR ps.modalities ILIKE '%CR%' OR ps.modalities ILIKE '%PX%')
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
  pr.content as report_content,
  pr.signed_at,
  pr.created_at as report_created_at,
  pp.id as signed_by_practitioner_id,
  pu.id as signed_by_user_id,
  pp.full_name as signed_by_name,
  pu.email as signed_by_email,
  rd.doctor_rank,
  pbp.name as body_part_name,
  pbp.identifier as body_part_identifier
FROM
  pacs_reports AS pr
JOIN
  pacs_practitioners AS pp ON pp.id = pr.signed_by_id
JOIN
  pacs_users AS pu ON pu.id = pp.user_id
JOIN
  eligible_doctors AS ed ON ed.user_id = pu.id
JOIN
  ranked_doctors AS rd ON rd.user_id = pu.id
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
  AND po.is_demo = FALSE
  AND ps.migrated = FALSE
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
"""

# Query to get DICOM images for a specific study
# Note: This assumes pacs_images table exists. Adjust based on actual schema.
get_dicom_images_for_study = """
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
"""

# Alternative query if DICOM images are stored differently
# Query to get series metadata for a study (to construct file paths)
get_series_for_study = """
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
"""

# Query to get report content for a study
get_report_for_study = """
SELECT
  pr.id,
  pr.study_id,
  pr.content,
  pr.status,
  pr.signed_at,
  pr.signed_by_id,
  pp.full_name as signed_by_name,
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
"""

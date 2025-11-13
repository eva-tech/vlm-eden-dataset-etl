-- Production query to get all DICOM files and reports from CR/DX chest studies
-- signed by doctors ranked 2nd to 6th highest position
-- Following the pattern with CTE for doctor ranking and study filtering

WITH ranked_doctors AS (
  -- Get doctors ranked by total signed chest studies (CR/DX/PX)
  SELECT
    pu.id AS user_id,
    pu.email,
    (array_agg(distinct pp.full_name))[1] AS full_name,
    COUNT(DISTINCT ps.id) AS total_signed_chest_studies,
    ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT ps.id) DESC) AS doctor_rank
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
    pu.id
  ORDER BY
    total_signed_chest_studies DESC
  LIMIT 50
),
doctors_2_to_6 AS (
  -- Filter to doctors ranked 2nd to 6th
  SELECT user_id
  FROM ranked_doctors
  WHERE doctor_rank BETWEEN 2 AND 6
),
eligible_studies AS (
  -- Get studies signed by doctors ranked 2-6, with CR/DX modalities and chest body part
  SELECT DISTINCT ps.id
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
    AND ps.deleted = FALSE
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
    AND pu.id IN (SELECT user_id FROM doctors_2_to_6)
)
SELECT 
  ps.id AS study_id,
  ps.dicom_study_id,
  ps.dicom_description AS study_description,
  ps.modalities,
  ps.dicom_date_time AS study_date,
  
  pser.id AS series_id,
  pser.dicom_series_id,
  pser.dicom_description AS series_description,
  pser.dicom_number AS series_number,
  pser.modality_id,
  
  pbp.name AS body_part_name,
  pbp.identifier AS body_part_identifier,
  
  pi.id AS instance_id,
  pi.dicom_instance_id,
  pi.dicom_number AS instance_number,
  
  pif.id AS file_id,
  pif.original_file AS file_path,
  pif.image AS image_url,
  pif.type AS file_type,
  CASE 
    WHEN pif.original_file IS NOT NULL THEN CONCAT('https://files.dev-land.space/media/', pif.original_file)
    ELSE NULL
  END AS file_url,
  
  pr.id AS report_id,
  pr.status AS report_status,
  pr.template_id AS report_template_id,
  pr.signed_at AS report_signed_at,
  
  prf.id AS field_id,
  prf.template_field_id,
  prf.value AS field_value,
  prf.created_at AS field_created_at
  
FROM eligible_studies es
INNER JOIN pacs_studies ps ON ps.id = es.id
INNER JOIN pacs_series pser ON pser.study_id = ps.id AND pser.deleted = FALSE
LEFT JOIN pacs_body_part pbp ON pbp.id = pser.body_part_id
INNER JOIN pacs_instances pi ON pi.series_id = pser.id AND pi.deleted = FALSE
INNER JOIN pacs_instance_files pif ON pif.instance_id = pi.id AND pif.deleted = FALSE AND pif.original_file IS NOT NULL
INNER JOIN pacs_reports pr ON pr.study_id = ps.id AND pr.deleted = FALSE
LEFT JOIN pacs_report_fields prf ON prf.report_id = pr.id AND prf.deleted = FALSE
ORDER BY 
  ps.id,
  pser.dicom_number,
  pi.dicom_number,
  pif.original_file,
  prf.created_at;


# """Queries to get all DICOM files and reports from all studies in pacs_studies."""

# # Query following the pattern with randomly selected 3 studies from pacs_studies
# get_all_studies_data = """
# WITH all_studies AS (
#   -- Randomly select 3 studies from pacs_studies
#   SELECT id
#   FROM pacs_studies
#   WHERE deleted = FALSE
#   ORDER BY RANDOM()
#   LIMIT 3
# )
# SELECT 
#   ps.id AS study_id,
#   ps.dicom_study_id,
#   ps.dicom_description AS study_description,
#   ps.modalities,
#   ps.dicom_date_time AS study_date,
  
#   pser.id AS series_id,
#   pser.dicom_series_id,
#   pser.dicom_description AS series_description,
#   pser.dicom_number AS series_number,
#   pser.modality_id,
  
#   pbp.name AS body_part_name,
#   pbp.identifier AS body_part_identifier,
  
#   pi.id AS instance_id,
#   pi.dicom_instance_id,
#   pi.dicom_number AS instance_number,
  
#   pif.id AS file_id,
#   pif.file AS file_path,
#   pif.image AS image_url,
#   pif.type AS file_type,
#   CONCAT('https://files.dev-land.space/media/', pif.file) AS file_url,
  
#   pr.id AS report_id,
#   pr.status AS report_status,
#   pr.template_id AS report_template_id,
#   pr.signed_at AS report_signed_at,
  
#   prf.id AS field_id,
#   prf.template_field_id,
#   prf.value AS field_value,
#   prf.created_at AS field_created_at
  
# FROM all_studies rs
# INNER JOIN pacs_studies ps ON ps.id = rs.id
# INNER JOIN pacs_series pser ON pser.study_id = ps.id AND pser.deleted = FALSE
# LEFT JOIN pacs_body_part pbp ON pbp.id = pser.body_part_id
# INNER JOIN pacs_instances pi ON pi.series_id = pser.id AND pi.deleted = FALSE
# LEFT JOIN pacs_instance_files pif ON pif.instance_id = pi.id AND pif.deleted = FALSE
# INNER JOIN pacs_reports pr ON pr.study_id = ps.id AND pr.deleted = FALSE
# LEFT JOIN pacs_report_fields prf ON prf.report_id = pr.id AND prf.deleted = FALSE
# ORDER BY 
#   ps.id,
#   pser.dicom_number,
#   pi.dicom_number,
#   pif.original_file,
#   prf.created_at;
# """

"""Queries to get all DICOM files and reports from all studies in pacs_studies."""
# Query following the pattern with randomly selected 3 studies from pacs_studies
get_all_studies_data = """
WITH all_studies AS (
  -- Randomly select 3 studies from pacs_studies
  SELECT id
  FROM pacs_studies
  WHERE deleted = FALSE
  ORDER BY RANDOM()
  LIMIT 3
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
  
FROM all_studies rs
INNER JOIN pacs_studies ps ON ps.id = rs.id
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
"""

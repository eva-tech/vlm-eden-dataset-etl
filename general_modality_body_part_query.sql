-- General query: All modalities, all body parts
-- No modality filter, no chest filter, no is_demo filter, no migrated filter
-- This query returns all modality and body part combinations ordered by total signed studies
--
-- Filters applied:
--   - pr.is_active = TRUE
--   - pr.deleted = FALSE
--   - pr.status = 'SIGNED'
--
-- No filters for:
--   - Modality (includes all modalities: CR, DX, CT, MR, etc.)
--   - Body part (includes all body parts: Breast, Abdomen, Chest, etc.)
--   - is_demo (includes demo and non-demo organizations)
--   - migrated (includes migrated and non-migrated studies)
--
-- Expected results (from local database):
--   CR | Breast       | 3
--   CR | RADIOMETRIA. | 1
--   DX | Abdomen      | 1

SELECT
  COALESCE(ps.modalities, 'Unknown') AS modality,
  COALESCE(pbp.name, ps.dicom_description, 'Unknown') AS body_part,
  COUNT(DISTINCT ps.id) AS total_signed_studies
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
GROUP BY
  modality, body_part
ORDER BY
  total_signed_studies DESC;


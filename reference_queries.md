#CR and DX chest count
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
  AND po.is_demo = FALSE
  AND ps.migrated = FALSE
  AND ps.modalities IN ('CR', 'DX')
  AND (
        pbp.name ILIKE '%chest%'
        OR ps.dicom_description ILIKE '%chest%'
      )
GROUP BY
  modality, body_part
ORDER BY
  total_signed_studies DESC
LIMIT 20;

#top 50 doctors chest

SELECT
  pu.id,
  pu.email,
  (array_agg(distinct pp.full_name))[1] as full_name,
  COUNT(DISTINCT ps.id) AS total_signed_chest_studies
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
  and (ps.modalities ilike '%DX%' or ps.modalities ilike '%CR%' or ps.modalities ilike '%PX%')
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
LIMIT 50;
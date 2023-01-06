get_studies = """
    select distinct ps.id as external_id,
           ps.urgency_level,
           ps.status,
           pu.full_name as patient_full_name,
           pr.signed_at as sign_at,
           pr.status,
           ps.facility_id,
           ps.created_at,
           ps.updated_at,
           ps.dicom_date_time,
           string_agg(distinct pm.identifier, ',' order by pm.identifier) as identifier,
           ps.practitioner_id,
           ps.referring_practitioner_id,
           pr.signed_by_id,
           ps.patient_id,
           pu.gender,
           pu.birth_date,
           ps.deleted,
           ps.migrated
    from pacs_studies ps
             left join pacs_series p on ps.id = p.study_id and p.deleted = false
             left join pacs_modalities pm on p.modality_id = pm.id and p.deleted = false
             left join pacs_reports pr on ps.id = pr.study_id and pr.deleted=false and pr.is_active
             left join pacs_patients pu on ps.patient_id = pu.id
    where ps.organization_id=%(organization_id)s 
    and (ps.created_at > (%(date)s)::timestamptz or ps.updated_at > (%(date)s)::timestamptz
    or pr.updated_at > (%(date)s)::timestamptz)
    group by ps.id,
             pr.signed_at,
             ps.urgency_level,
             ps.status,
             ps.id,
             pr.status,
             pu.full_name,
             ps.facility_id,
             ps.created_at,
             ps.updated_at,
             ps.dicom_date_time,
             ps.referring_practitioner_id,
             pr.signed_by_id,
             pu.gender,
             pu.birth_date
    having count(p.id) > 0;
"""

insert_studies = """
INSERT INTO {schema}.fact_studies (
    external_id,
    status,
    sign_at,
    patient_full_name,
    urgency_level,
    created_at,
    updated_at,
    dicom_date_time, 
    patient_id,
    gender,
    birth_date,
    deleted,
    migrated,
    calendar_sign_at_id,
    calendar_birth_date_id,
    calendar_dicom_date_time_id,
    facility_id,
    modality_id,
    calendar_id,
    practitioner_id,
    referring_practitioner_id,
    signed_by_id
) VALUES %s
ON CONFLICT (external_id) 
DO UPDATE SET 
    status = excluded.status,
    sign_at = excluded.sign_at,
    dicom_date_time = excluded.dicom_date_time,
    urgency_level = excluded.urgency_level,
    updated_at = excluded.updated_at,
    patient_id = excluded.patient_id,
    gender = excluded.gender,
    birth_date = excluded.birth_date,
    deleted = excluded.deleted,
    migrated = excluded.migrated,
    calendar_sign_at_id = excluded.calendar_sign_at_id,
    calendar_birth_date_id = excluded.calendar_birth_date_id,
    calendar_dicom_date_time_id = excluded.calendar_dicom_date_time_id,
    practitioner_id = excluded.practitioner_id,
    referring_practitioner_id = excluded.referring_practitioner_id,
    signed_by_id = excluded.signed_by_id;
"""

insert_studies_template = """
    (%(external_id)s, 
    %(status)s, 
    %(sign_at)s, 
    %(patient_full_name)s, 
    %(urgency_level)s, 
    %(created_at)s, 
    %(updated_at)s, 
    %(dicom_date_time)s, 
    %(patient_id)s, 
    %(gender)s, 
    %(birth_date)s, 
    %(deleted)s, 
    %(migrated)s, 
    (select date_dim_id from {schema}.dim_calendar dc where date_actual = (%(sign_at)s)::date),
    (select date_dim_id from {schema}.dim_calendar dc where date_actual = (%(birth_date)s)::date),
    (select date_dim_id from {schema}.dim_calendar dc where date_actual = (%(dicom_date_time)s)::date),
    (select id from {schema}.dim_facilities where external_id = %(facility_id)s),
    (select id from {schema}.dim_modalities where identifier = %(identifier)s),
    (select date_dim_id from {schema}.dim_calendar dc where date_actual = (%(created_at)s)::date),
    (select id from {schema}.dim_practitioners where external_id = %(practitioner_id)s),
    (select id from {schema}.dim_practitioners where external_id = %(referring_practitioner_id)s),
    (select id from {schema}.dim_practitioners where external_id = %(signed_by_id)s))
"""

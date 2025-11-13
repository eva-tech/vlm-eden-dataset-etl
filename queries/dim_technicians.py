"""This file contains all the queries related to the dim_practitioners table."""

get_all_technicians = """
    select
        st.id as study_external_id, 
        string_agg(distinct nullif(trim(se.dicom_operators_name), ''), ',' order by nullif(trim(se.dicom_operators_name),'')) as name
    from pacs_studies as st
        join pacs_series as se on se.study_id = st.id
    where st.organization_id = (%(organization_id)s)::uuid and se.dicom_operators_name != '' and se.dicom_operators_name is not null
    and (se.created_at > (%(date)s)::timestamptz or se.updated_at > (%(date)s)::timestamptz)
    group by st.id 
"""

insert_technicians = """
    INSERT INTO {schema}.dim_technicians (
        fact_studies_id,
        study_external_id,
        name 
    )
    VALUES %s
"""

insert_technicians_template = """
    (
        (select id from {schema}.fact_studies where external_id = %(study_external_id)s),
        %(study_external_id)s,
        %(name)s 
    )
"""

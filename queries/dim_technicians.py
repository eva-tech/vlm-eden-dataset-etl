"""This file contains all the queries related to the dim_practitioners table."""

get_all_technicians = """
    select distinct string_agg(distinct nullif(trim(se.dicom_operators_name), ''), ',' order by nullif(trim(se.dicom_operators_name),'')) as name
    from pacs_studies as st
        join pacs_series as se on se.study_id = st.id
    where st.organization_id = %(organization_id)s and se.dicom_operators_name != '' and se.dicom_operators_name is not null
    and (se.created_at > (%(date)s)::timestamptz or se.updated_at > (%(date)s)::timestamptz)
    group by st.id 

"""

insert_technicians = """
    INSERT INTO {}.dim_technicians (
        name 
    )
    VALUES %s
"""

insert_technicians_template = """
    (
        %(name)s 
    )
"""

get_modalities_list = """
    SELECT 
        id,
        name,
        identifier,
        description,
        created_at,
        updated_at,
        name_es
    FROM pacs_modalities
    order by identifier
"""

get_modalities_from_studies = """
    select distinct string_agg(distinct pm.identifier, ',' order by pm.identifier) as modalities
    from pacs_studies ps
             left join pacs_series p on ps.id = p.study_id
             left join pacs_modalities pm on p.modality_id = pm.id
    where ps.organization_id = %(organization_id)s and pm.identifier is not null
    and (ps.created_at >= (%(date)s)::timestamptz or ps.updated_at >= (%(date)s)::timestamptz)
    group by ps.id
"""

insert_modalities = """
    INSERT INTO {schema}.dim_modalities (
        external_id, 
        name, 
        identifier, 
        description, 
        created_at, 
        updated_at,
        name_es
    )
    VALUES %s
    ON CONFLICT (identifier)   
    DO UPDATE SET
        name = excluded.name,
        description = excluded.description,
        created_at = excluded.created_at,
        updated_at = excluded.updated_at;
"""

insert_modalities_template = """
    (
        %(id)s,
        %(name)s,
        %(identifier)s,
        %(description)s,
        %(created_at)s,
        %(updated_at)s,
        %(name_es)s
    )
"""

"""Queries for the dim_modalities table."""
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
    select distinct ps.modalities as modalities
    from pacs_studies ps
    where ps.organization_id = %(organization_id)s
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
        updated_at = excluded.updated_at,
        name_es = excluded.name_es;
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

fix_names_template = """
    (
        %(external_id)s,
        %(name)s,
        %(identifier)s,
        %(description)s,
        %(created_at)s,
        %(updated_at)s,
        %(name_es)s
    )
"""

get_dim_modalities = """
    SELECT 
        external_id, 
        name, 
        identifier, 
        description, 
        created_at, 
        updated_at,
        name_es
    from {schema}.dim_modalities
"""

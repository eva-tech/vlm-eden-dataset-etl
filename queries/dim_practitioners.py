"""This file contains the queries for the dim_practitioners table."""

get_all_practitioners = """
    SELECT pp.id,
           pp.name,
           pp.first_surname,
           pp.last_surname,
           pp.full_name,
           pp.status,
           pp.gender,
           pp.created_at,
           pp.updated_at
    FROM pacs_practitioners pp
             WHERE pp.organization_id = %(organization_id)s
             and (pp.created_at > (%(date)s)::timestamptz or pp.updated_at > (%(date)s)::timestamptz)
"""

insert_practitioners = """
    INSERT INTO {}.dim_practitioners (
        external_id, 
        name, 
        first_surname, 
        last_surname, 
        full_name, 
        status, 
        gender, 
        created_at, 
        updated_at
    )
    VALUES %s
    ON CONFLICT (external_id)   
    DO UPDATE SET
        name = excluded.name,
        first_surname = excluded.first_surname,
        last_surname = excluded.last_surname,
        full_name = excluded.full_name,
        status = excluded.status,
        gender = excluded.gender,
        created_at = excluded.created_at,
        updated_at = excluded.updated_at;
"""

insert_practitioners_template = """
    (
        %(id)s, 
        %(name)s, 
        %(first_surname)s, 
        %(last_surname)s, 
        %(full_name)s,
        %(status)s,
        %(gender)s,
        %(created_at)s,
        %(updated_at)s
    )
"""

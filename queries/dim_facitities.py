"""This file contains all the queries related to the dim_facilities table."""
get_all_facilities = """
    SELECT distinct pf.id,
       pf.name,
       pf.address,
       pf.timezone,
       pf.created_at,
       pf.updated_at,
       ms.name as state,
       mc.name as city,
       pc.name as country
    FROM pacs_facilities as pf
         LEFT JOIN pacs_countries pc on pf.country_id = pc.id
         LEFT JOIN mgmt_states ms on pf.state_id = ms.id
         LEFT JOIN mgmt_cities mc on pf.city_id = mc.id
    WHERE pf.organization_id = %(organization_id)s
    and (pf.created_at > (%(date)s)::timestamptz or pf.updated_at > (%(date)s)::timestamptz)
"""

insert_facilities = """
    INSERT INTO {}.dim_facilities (
        external_id, 
        name, 
        address, 
        timezone, 
        country, 
        state, 
        city, 
        created_at, 
        updated_at
    )
    VALUES %s
    ON CONFLICT (external_id)   
    DO UPDATE SET
        name = excluded.name,
        address = excluded.name,
        timezone = excluded.timezone,
        country = excluded.country,
        state = excluded.state,
        city = excluded.city,
        created_at = excluded.created_at,
        updated_at = excluded.updated_at;
"""

insert_facilities_template = """
(
    %(id)s, 
    %(name)s, 
    %(address)s, 
    %(timezone)s, 
    %(country)s, 
    %(state)s, 
    %(city)s, 
    %(created_at)s, 
    %(updated_at)s
)
"""

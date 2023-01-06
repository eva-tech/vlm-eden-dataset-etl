organizations_with_product = """
    SELECT po.id,
           po.name,
           po.slug
    FROM pacs_organizations po
             JOIN pacs_product_organizations ppo on po.id = ppo.organization_id
             JOIN pacs_products pp on ppo.product_id = pp.id
    WHERE pp.slug = %s
    and ppo.deleted=false
    and po.deleted=false
    and ppo.has_access=true;
"""

create_schema = """ 
    CREATE SCHEMA IF NOT EXISTS {name};    
"""

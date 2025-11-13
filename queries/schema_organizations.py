"""This file contains all the queries related to the organizations schema."""

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

all_organizations = """
    SELECT *
    FROM pacs_organizations;
"""

insert_organizations = """
    INSERT INTO organizations (
        id, 
        deleted, 
        created_at, 
        updated_at, 
        name, 
        is_active, 
        created, 
        modified, 
        slug, 
        is_demo, 
        invoice_start_date, 
        include_global_assets,
        include_dental_viewer_database, 
        timezone, 
        report_email_subject, 
        default_permissions_behavior, 
        twilio_account_sid, 
        twilio_sender_number, 
        has_pending_payment, 
        suspended, 
        email_from, 
        invitation_template_html, 
        reset_password_template_html, 
        currency, 
        locale                     
    )
    VALUES %s
    ON CONFLICT (id)
    DO UPDATE SET
        deleted = excluded.deleted,
        created_at = excluded.created_at,
        updated_at = excluded.updated_at,
        name = excluded.name,
        is_active = excluded.is_active,
        created = excluded.created,
        modified = excluded.modified,
        slug = excluded.slug,
        is_demo = excluded.is_demo,
        invoice_start_date = excluded.invoice_start_date,
        include_global_assets = excluded.include_global_assets,
        include_dental_viewer_database = excluded.include_dental_viewer_database,
        timezone = excluded.timezone,
        report_email_subject = excluded.report_email_subject,
        default_permissions_behavior = excluded.default_permissions_behavior,
        twilio_account_sid = excluded.twilio_account_sid,
        twilio_sender_number = excluded.twilio_sender_number,
        has_pending_payment = excluded.has_pending_payment,
        suspended = excluded.suspended,
        email_from = excluded.email_from,
        invitation_template_html = excluded.invitation_template_html,
        reset_password_template_html = excluded.reset_password_template_html,
        currency = excluded.currency,
        locale = excluded.locale;
"""

insert_organizations_template = """
(
    %(id)s,
    %(deleted)s,
    %(created_at)s,
    %(updated_at)s,
    %(name)s,
    %(is_active)s,
    %(created)s,
    %(modified)s,
    %(slug)s,
    %(is_demo)s,
    %(invoice_start_date)s,
    %(include_global_assets)s,
    %(include_dental_viewer_database)s,
    %(timezone)s,
    %(report_email_subject)s,
    %(default_permissions_behavior)s,
    %(twilio_account_sid)s,
    %(twilio_sender_number)s,
    %(has_pending_payment)s,
    %(suspended)s,
    %(email_from)s,
    %(invitation_template_html)s,
    %(reset_password_template_html)s,
    %(currency)s,
    %(locale)s
)
"""

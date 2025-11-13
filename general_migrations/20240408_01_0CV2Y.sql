-- 
-- depends: 

create table organizations
(
    id                             uuid                     not null
        primary key,
    deleted                        boolean                  not null,
    created_at                     timestamp with time zone not null,
    updated_at                     timestamp with time zone not null,
    name                           varchar(200)             not null,
    is_active                      boolean                  not null,
    created                        timestamp with time zone not null,
    modified                       timestamp with time zone not null,
    slug                           varchar(200)             not null,
    is_demo                        boolean                  not null,
    invoice_start_date             timestamp with time zone,
    include_global_assets          boolean                  not null,
    include_dental_viewer_database boolean                  not null,
    timezone                       text,
    report_email_subject           text                     not null,
    default_permissions_behavior   boolean                  not null,
    twilio_account_sid             text,
    twilio_sender_number           text,
    has_pending_payment            boolean                  not null,
    suspended                      boolean,
    email_from                     text,
    invitation_template_html       text,
    reset_password_template_html   text,
    currency                       varchar(3),
    locale                         varchar(5)
);

create index pacs_organizations_slug_944ce2ff
    on organizations (slug);

create index pacs_organizations_slug_944ce2ff_like
    on organizations (slug varchar_pattern_ops);

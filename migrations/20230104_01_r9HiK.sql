-- 
-- depends: 20221214_01_9Jm6L

CREATE TABLE sync_records
(
    id             serial not null primary key,
    table_name     varchar(255),
    last_sync_date timestamptz,
    records_synced int,
    created_at     timestamptz
)

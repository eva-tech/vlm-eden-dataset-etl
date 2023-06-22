get_last_sync_date = """
    SELECT max(last_sync_date) FROM {schema}.sync_records where table_name=%(table_name)s;
"""

insert_last_sync_data = """
    INSERT INTO {schema}.sync_records (
        table_name,
        last_sync_date,
        records_synced,
        created_at
    ) VALUES (
        %(table_name)s,
        %(last_sync_date)s,
        %(records_synced)s,
        now()
    );
"""
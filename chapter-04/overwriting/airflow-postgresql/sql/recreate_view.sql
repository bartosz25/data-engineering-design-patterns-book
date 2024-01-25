SELECT table_name FROM information_schema.tables WHERE table_catalog = '{{ params.schema }}'
AND table_schema = 'dedp'
AND table_type = 'BASE TABLE'
AND table_name LIKE '{{ params.base_table_name }}_%'
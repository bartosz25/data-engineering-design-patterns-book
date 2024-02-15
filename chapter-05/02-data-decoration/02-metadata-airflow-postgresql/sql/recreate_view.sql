SELECT table_name FROM information_schema.tables WHERE table_catalog = 'dedp'
AND table_schema = 'dedp'
AND table_type = 'BASE TABLE'
AND table_name LIKE 'visits_weekly_%'
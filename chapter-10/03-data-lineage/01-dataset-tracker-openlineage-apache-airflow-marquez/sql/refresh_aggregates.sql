TRUNCATE TABLE devices_aggregates;


INSERT INTO devices_aggregates
    SELECT type, full_name, version, COUNT(*) AS all_occurrences FROM devices_raw GROUP BY type, full_name, version;
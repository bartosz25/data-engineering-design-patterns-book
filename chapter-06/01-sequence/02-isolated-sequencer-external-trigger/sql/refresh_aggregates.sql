{% set table_name = get_table_name(ds_nodash) %}

-- Just for the record. The following line works but it doesn't make the DAG idempotent!
-- Rerunning will lead to counting multiple times for the same {{ table_name }}
-- That's why we prefer here a more costly TRUNCATE + INSERT
--
--UPDATE dedp.devices SET
--    all_occurrences += new_devices.all_occurrences
--FROM (
--    SELECT type, full_name, version, COUNT(*) AS all_occurrences FROM {{ table_name }} GROUP BY type, full_name, version
--) AS new_devices
--WHERE type = new_devices.type AND full_name = new_devices.full_name AND version = new_devices.version;

TRUNCATE TABLE dedp.devices_aggregates;

INSERT INTO dedp.devices_aggregates (
    SELECT type, full_name, version, SUM(all_occurrences) AS all_occurrences
    FROM
        (SELECT type, full_name, version, COUNT(*) AS all_occurrences FROM dedp.devices_raw GROUP BY type, full_name, version
        UNION
        SELECT * FROM dedp.devices_aggregates) AS devices_union
    GROUP BY type, full_name, version
);
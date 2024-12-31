TRUNCATE TABLE dedp.devices;

INSERT INTO dedp.devices (id, brand_name, full_name, processor_brand)
    SELECT id, brand_name, full_name, processor_brand FROM (
     SELECT
        id, brand_name, full_name, processor_brand,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY execution_time DESC) AS position
     FROM dedp.devices_history
    ) AS to_load
    WHERE to_load.position = 1;
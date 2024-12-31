CREATE TEMPORARY TABLE temp_devices (LIKE dedp.devices);

COPY temp_devices FROM '/data_to_load/{{ ds_nodash}}.csv' CSV  DELIMITER ';' HEADER;

DELETE FROM dedp.devices_history WHERE execution_time = '{{ ts }}';

INSERT INTO dedp.devices_history (id, execution_time, brand_name, full_name, processor_brand)
SELECT id, '{{ ts }}' AS execution_time , brand_name, full_name, processor_brand FROM temp_devices;
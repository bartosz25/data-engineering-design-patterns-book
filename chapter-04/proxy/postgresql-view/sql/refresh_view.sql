{% set devices_internal_table = get_devices_table_name() %}

CREATE OR REPLACE VIEW dedp.devices AS SELECT * FROM {{ devices_internal_table }};
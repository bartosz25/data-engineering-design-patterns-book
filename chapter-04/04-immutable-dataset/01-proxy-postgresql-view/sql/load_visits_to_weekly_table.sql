{% set devices_internal_table = get_devices_table_name() %}

CREATE TABLE {{ devices_internal_table }} (
    type VARCHAR(10) NOT NULL,
    full_name TEXT NOT NULL,
    version VARCHAR(25) NOT NULL,
    PRIMARY KEY(type, full_name, version)
);

COPY {{ devices_internal_table }}  FROM '{{ get_input_csv_to_load() }}' CSV  DELIMITER ';' HEADER;
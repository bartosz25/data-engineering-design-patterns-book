DELETE FROM dedp.devices_raw WHERE load_id = '{{ ds_nodash }}';

CREATE TEMPORARY TABLE devices_staging_table (
    type VARCHAR(15) NOT NULL,
    full_name VARCHAR(50) NOT NULL,
    version VARCHAR(40) NOT NULL,
    PRIMARY KEY(type, full_name, version)
);

COPY devices_staging_table FROM '/data_to_load/dataset.csv' CSV  DELIMITER ';' HEADER;

INSERT INTO dedp.devices_raw (type, full_name, version, load_id)
    SELECT type, full_name, version, '{{ ds_nodash }}' FROM devices_staging_table
;
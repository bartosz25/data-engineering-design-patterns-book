{% set table_name = get_table_name(ds_nodash) %}

DROP VIEW IF EXISTS devices;
DROP TABLE IF EXISTS {{ table_name }};
CREATE TABLE {{ table_name }} (
    type VARCHAR(15) NOT NULL,
    full_name VARCHAR(50) NOT NULL,
    version VARCHAR(40) NOT NULL,
    PRIMARY KEY(full_name, version)
);

COPY {{ table_name }} FROM '/data_to_load/dataset.csv' CSV  DELIMITER ';' HEADER;

CREATE OR REPLACE VIEW devices AS SELECT * FROM {{ table_name }}